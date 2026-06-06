"""推理输入构建 + batch 切分 + 并发执行。

InferenceInputBuilder 从 ML-ready / DWD parquet 构建 InferenceSample；
run_batches_concurrent 用 ThreadPoolExecutor 并发跑 batch 并保持输出顺序。
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator

from robot_dh.inference.outputs import read_table_from_uri
from robot_dh.lake.store import create_lake_store
from robot_dh.lake.uri import join_uri, parse_uri
from robot_dh.models.schemas import InferencePrediction, InferenceSample

LOG = logging.getLogger(__name__)

# 按 split 选择的输入文件优先级（basename）。
_SPLIT_FILES = {
    "train": ["train.parquet"],
    "val": ["val.parquet"],
    "test": ["test.parquet"],
    "all": ["train.parquet", "val.parquet", "test.parquet"],
}
# ML-ready 缺失时回退的 DWD 特征文件名。
_FALLBACK_FILES = ["episode_feature.parquet", "episode_features.parquet", "features.parquet"]

# 从 row 提取到 metadata 的已知质量列（anomaly_scorer 等会用到）。
_METADATA_COLUMNS = ("quality_score", "max_velocity_mps", "null_rate", "label", "task")
_EPISODE_COLUMNS = ("episode_id", "episode_index", "episode")
_FRAME_COLUMNS = ("frame_id", "frame_index", "frame")
_TEXT_COLUMNS = ("input_text", "text", "caption", "language_instruction", "task")
_REF_COLUMNS = ("video_path", "video_uri", "image_uri", "image_path", "input_refs")


class InferenceInputError(ValueError):
    """输入构建失败（无数据 / 无法解析）。"""


@dataclass
class InferenceInputBuilder:
    """从 input_uri 构建推理样本。"""

    input_uri: str
    split: str = "all"
    limit: int | None = None
    dataset_id: str | None = None
    version: str | None = None

    def build(self) -> list[InferenceSample]:
        files = self._resolve_files()
        if not files:
            raise InferenceInputError(
                f"input_uri 下未找到可用 parquet（train/val/test 或 episode_feature）：{self.input_uri}"
            )
        dataset_id, version = self._resolve_dataset_version()
        samples: list[InferenceSample] = []
        row_index = 0
        for file_uri in files:
            try:
                table = read_table_from_uri(file_uri)
            except FileNotFoundError:
                continue
            for row in table.to_pylist():
                samples.append(self._row_to_sample(row, row_index, dataset_id, version, file_uri))
                row_index += 1
                if self.limit is not None and len(samples) >= self.limit:
                    return samples
        if not samples:
            raise InferenceInputError(f"input_uri 下 parquet 为空：{self.input_uri}")
        return samples

    def _resolve_files(self) -> list[str]:
        store = create_lake_store(self.input_uri)
        try:
            all_uris = store.list(self.input_uri)
        except Exception as err:  # list 失败（路径不存在等）给清晰错误
            raise InferenceInputError(f"无法列出 input_uri：{self.input_uri}（{err}）") from err
        by_name: dict[str, str] = {}
        for uri in all_uris:
            name = uri.rstrip("/").split("/")[-1]
            if name.endswith(".parquet"):
                by_name.setdefault(name, uri)

        wanted = _SPLIT_FILES.get(self.split, _SPLIT_FILES["all"])
        chosen = [by_name[name] for name in wanted if name in by_name]
        if chosen:
            return chosen
        for name in _FALLBACK_FILES:
            if name in by_name:
                return [by_name[name]]
        # 最后兜底：input_uri 本身是单个 parquet 文件，或目录内任意 parquet。
        if self.input_uri.endswith(".parquet"):
            return [parse_uri(self.input_uri).uri]
        return sorted(by_name.values())

    def _resolve_dataset_version(self) -> tuple[str | None, str | None]:
        if self.dataset_id and self.version:
            return self.dataset_id, self.version
        # 从 URI 末两段猜测（如 .../ml-ready/<dataset_id>/<version>）。
        parsed = parse_uri(self.input_uri)
        path = parsed.key if parsed.is_s3 else parsed.local_path
        segments = [s for s in path.rstrip("/").split("/") if s]
        guess_version = self.version
        guess_dataset = self.dataset_id
        if len(segments) >= 2:
            if guess_version is None:
                guess_version = segments[-1]
            if guess_dataset is None:
                guess_dataset = segments[-2]
        return guess_dataset, guess_version

    def _row_to_sample(
        self,
        row: dict[str, Any],
        row_index: int,
        dataset_id: str | None,
        version: str | None,
        file_uri: str,
    ) -> InferenceSample:
        ds = _first_present(row, ("dataset_id",)) or dataset_id
        ver = _first_present(row, ("version",)) or version
        episode_id = _first_present(row, _EPISODE_COLUMNS)
        episode_id = str(episode_id) if episode_id is not None else str(row_index)
        frame_val = _first_present(row, _FRAME_COLUMNS)
        frame_id = str(frame_val) if frame_val is not None else None
        # 若输入已带 sample_id（如 failed_samples.parquet 重试场景），沿用以保持样本身份。
        existing_sid = _first_present(row, ("sample_id",))
        sample_id = str(existing_sid) if existing_sid is not None else f"{ds or 'unknown'}:{ver or 'v1'}:{episode_id}:{row_index}"

        input_text = _first_present(row, _TEXT_COLUMNS)
        refs = _collect_refs(row)
        metadata = {k: row[k] for k in _METADATA_COLUMNS if k in row and row[k] is not None}

        return InferenceSample(
            sample_id=sample_id,
            dataset_id=ds,
            version=ver,
            episode_id=episode_id,
            frame_id=frame_id,
            input_uri=file_uri,
            input_text=str(input_text) if input_text is not None else None,
            input_refs=refs,
            metadata=metadata,
        )


def _first_present(row: dict[str, Any], keys: Iterable[str]) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def _collect_refs(row: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    for k in _REF_COLUMNS:
        v = row.get(k)
        if v is None:
            continue
        if isinstance(v, (list, tuple)):
            refs.extend(str(x) for x in v if x is not None)
        else:
            refs.append(str(v))
    return refs


def iter_batches(samples: list[InferenceSample], batch_size: int) -> Iterator[list[InferenceSample]]:
    """把样本切成 batch_size 大小的块。"""
    size = max(1, int(batch_size))
    for start in range(0, len(samples), size):
        yield samples[start : start + size]


def run_batches_concurrent(
    batches: list[list[InferenceSample]],
    fn: Callable[[list[InferenceSample]], list[InferencePrediction]],
    *,
    max_workers: int = 1,
) -> list[list[InferencePrediction]]:
    """并发执行各 batch，返回与 batches 等长、同序的结果列表。

    max_workers<=1 时退化为顺序执行（便于调试 / 确定性）。
    """
    if max_workers <= 1 or len(batches) <= 1:
        return [fn(b) for b in batches]
    results: list[list[InferencePrediction] | None] = [None] * len(batches)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_idx = {pool.submit(fn, batch): idx for idx, batch in enumerate(batches)}
        for future in future_to_idx:
            idx = future_to_idx[future]
            results[idx] = future.result()
    # 上面循环按 dict 插入序遍历，逐个 .result() 会阻塞到完成，顺序由 idx 保证。
    return [r if r is not None else [] for r in results]
