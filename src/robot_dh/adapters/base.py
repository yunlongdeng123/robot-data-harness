"""RobotDatasetAdapter 接口与共享数据结构。

每个具体 adapter（DROID / robomimic / bridge / universal）实现：
  - ``family``                 数据集家族；contract registry 的 key
  - ``detect(uri)``            根据 layout markers / dataset_id_prefix 判定是否处理
  - ``probe(uri, options)``    返回 ProbeResult（统计、列名、错误链）
  - ``list_episodes(uri)``     列 episode（DROID/bridge：parquet group；robomimic：HDF5 demo_*）
  - ``normalize_options(...)`` 推荐参数（include/exclude/skip_videos 等）
  - ``contract_runner()``      返回 ``(rules, metric_fn, contract_id)``，复用
                                ``robot_dh.qc.registry.get_contract_runner``

不强制每个 adapter 都自己跑 contract；v1.6 contract runner 已经接受 family
名称做路由，这里更多是给 CLI / API 提供"按 dataset_uri 推断 family"的能力。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from robot_dh.lake.uri import is_local_uri, is_s3_uri, parse_uri
from robot_dh.qc.base import Rule


@dataclass(slots=True)
class DetectionResult:
    family: str
    confidence: float          # 0..1；layout markers 命中 +0.6，dataset_id 前缀命中 +0.4
    matched_markers: list[str] = field(default_factory=list)
    matched_id_prefix: str | None = None
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EpisodeRef:
    episode_id: str
    file_uri: str
    rel_path: str
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ProbeResult:
    family: str
    dataset_uri: str
    status: str                # OK / WARN / FAIL
    files_count: int = 0
    bytes_total: int = 0
    parquet_files: int = 0
    hdf5_files: int = 0
    video_files: int = 0
    episodes_count: int = 0
    schema_summary: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    duration_sec: float = 0.0
    options: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class NormalizeResult:
    family: str
    dataset_uri: str
    output_uri: str
    status: str
    episodes_written: int = 0
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RobotDatasetAdapter(ABC):
    """抽象基类。每个 family 一个实例（无状态、可全局复用）。"""

    family: str = "universal"

    # -------- 探测 --------
    @abstractmethod
    def detect(
        self,
        dataset_uri: str,
        *,
        dataset_id: str | None = None,
        listings: list[str] | None = None,
    ) -> DetectionResult:
        """根据 URI / 已知 listing 判定是否能处理此数据集。

        ``listings`` 是相对 dataset_uri 的相对路径列表（adapter 自己 list 太重时
        允许调用方提供 cache）。
        """

    # -------- 探针 --------
    @abstractmethod
    def probe(
        self,
        dataset_uri: str,
        *,
        sample_limit: int = 32,
        options: dict[str, Any] | None = None,
    ) -> ProbeResult:
        """对数据集做轻量 schema / 文件计数；本地 file URI 走 fast path。"""

    # -------- episode 列表 --------
    def list_episodes(
        self,
        dataset_uri: str,
        *,
        limit: int | None = None,
    ) -> list[EpisodeRef]:
        """默认实现：根据 family 在 listing 里挑 parquet/hdf5。子类可覆盖。"""
        listings = self._list_relative_paths(dataset_uri)
        items: list[EpisodeRef] = []
        for i, rel in enumerate(listings):
            if limit is not None and len(items) >= limit:
                break
            file_uri = _join_uri(dataset_uri, rel)
            items.append(EpisodeRef(episode_id=f"{self.family}-{i:05d}", file_uri=file_uri, rel_path=rel))
        return items

    # -------- normalize 推荐参数 --------
    def normalize_options(self, dataset_uri: str) -> dict[str, Any]:
        return {}

    # -------- contract 路由 --------
    def contract_runner(self) -> tuple[list[Rule], Callable[[Any], dict[str, Any]], str]:
        from robot_dh.qc.registry import get_contract_runner

        return get_contract_runner(self.family)

    # -------- helpers --------
    @staticmethod
    def _list_relative_paths(dataset_uri: str) -> list[str]:
        """轻量 listing：只在本地路径下走 rglob；S3 不在 base class 实现以免引入 boto3。"""
        if is_s3_uri(dataset_uri):
            return []
        if not is_local_uri(dataset_uri):
            return []
        root = Path(parse_uri(dataset_uri).local_path)
        if not root.exists():
            return []
        if root.is_file():
            return [root.name]
        rels: list[str] = []
        for f in sorted(root.rglob("*")):
            if f.is_file():
                rels.append(f.relative_to(root).as_posix())
        return rels


def _join_uri(base: str, rel: str) -> str:
    """拼出 episode 的对外 URI；本地结果统一带 ``file://`` scheme。"""
    from robot_dh.lake.uri import is_local_uri, join_uri, to_file_uri

    combined = join_uri(base, rel)
    if is_local_uri(combined):
        return to_file_uri(combined)
    return combined
