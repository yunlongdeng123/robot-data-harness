"""LeRobot v2 dataset lazy profile（DROID / 其它 lerobot v2 系数据集通用）。

针对 droid_lerobot_scale30 0B archive log 现场（pod 在 import 之前就被 SIGKILL）做的
专属路径：

- 整 parquet **不下载**，纯 ``pyarrow + s3fs`` 读 footer；
- **完全不下载视频文件**——视频只走 list_objects_v2 拿 count + size，
  避免 32 mp4 × 30+ MiB 在 step pod 里串行 cv2 解码导致内存 / 时间炸；
- 只 download 三个小元数据：``meta/info.json`` / ``meta/stats.json`` / ``_manifest.json``，
  累计 < 100 KiB；
- ``data/chunk-*/file-*.parquet`` 抽样前 ``max_parquet`` 个并发读 footer
  （``ThreadPoolExecutor(max_workers=8)``），181 文件采样 8 个，单 step < 30s；
- 失败时 probe 字典里写 ``error_type / cause_type``，与 v1.6.5 bridge profile 同款，
  不再吞成 "Max Retries Exceeded"。

LeRobot v2 layout（``meta/info.json`` 存在即认为命中）::

    raw/<dataset>/v1/
    ├── _manifest.json
    ├── meta/info.json
    ├── meta/stats.json
    ├── meta/tasks.parquet
    ├── data/chunk-000/file-000.parquet ... file-180.parquet
    └── videos/...        # 可选

profile 入口签名与 ``profile_dataset`` 对齐，直接返回同款 ``AssetProfile``，下游
``droid_metrics`` 复用 ``profile.profile.parquet`` 字段。
"""

from __future__ import annotations

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import pyarrow.parquet as pq

from robot_dh.lake.s3_fs import get_s3fs, split_s3_uri
from robot_dh.lake.uri import is_s3_uri, parse_uri
from robot_dh.qc.base import AssetProfile

LOG = logging.getLogger(__name__)

# v2 layout 必备元数据；只要 meta/info.json 存在就认为是 lerobot v2 dataset。
_INFO_REL = "meta/info.json"
_STATS_REL = "meta/stats.json"
_MANIFEST_REL = "_manifest.json"

# 默认采样上限：181 个 parquet 抽 8 个 footer，单 step S3 GET 量 < 8 MiB。
DEFAULT_MAX_PARQUET_SAMPLES = 8
DEFAULT_PARQUET_PROBE_CONCURRENCY = 8


def _summarize_exception(err: BaseException) -> dict[str, Any]:
    """复用 parquet_probe 里的 cause 链解析（优先 __cause__，缺失 fallback __context__）。"""
    from robot_dh.qc.parquet_probe import _summarize_exception as _shared

    return _shared(err)


def _normalize_root(dataset_uri: str) -> str:
    """``s3://bucket/key/`` 或 ``s3://bucket/key`` 都规整成无尾 ``/`` 形式。"""
    return dataset_uri.rstrip("/")


def detect_lerobot_v2(dataset_uri: str) -> bool:
    """``meta/info.json`` 存在 → 认定为 lerobot v2。

    远端：用 s3fs.exists；本地：直接 stat。任何异常都视为不命中（保守 fallback）。
    """
    from pathlib import Path

    base = _normalize_root(dataset_uri)
    try:
        if is_s3_uri(base):
            fs = get_s3fs()
            bucket, key = split_s3_uri(f"{base}/{_INFO_REL}")
            return bool(fs.exists(f"{bucket}/{key}"))
        local_path = Path(parse_uri(base).local_path)
        return (local_path / _INFO_REL).is_file()
    except Exception as err:  # noqa: BLE001
        LOG.debug("lerobot v2 detect failed for %s: %s", dataset_uri, err)
        return False


def _read_s3_json(fs: Any, bucket: str, key: str) -> dict[str, Any] | None:
    """读取 S3 上的 JSON；缺失 / 损坏返回 None，不抛。"""
    try:
        with fs.open(f"{bucket}/{key}", "rb") as fobj:
            return json.load(fobj)
    except FileNotFoundError:
        return None
    except Exception as err:  # noqa: BLE001
        LOG.warning("lerobot v2 read json failed for s3://%s/%s: %s", bucket, key, err)
        return None


def _list_s3_prefix(fs: Any, bucket: str, prefix: str) -> list[tuple[str, int]]:
    """``fs.ls(bucket/prefix, detail=True)`` 包装，返回 ``[(s3 uri, size), ...]``。"""
    prefix = prefix.rstrip("/") + "/"
    try:
        entries = fs.ls(f"{bucket}/{prefix}", detail=True)
    except FileNotFoundError:
        return []
    except Exception as err:  # noqa: BLE001
        LOG.warning("lerobot v2 list failed for s3://%s/%s: %s", bucket, prefix, err)
        return []
    out: list[tuple[str, int]] = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        # s3fs 0.x/2.x 都会给 type=file/directory
        if entry.get("type") and entry["type"] != "file":
            continue
        name = entry.get("name") or entry.get("Key")
        if not name:
            continue
        size = int(entry.get("size") or entry.get("Size") or 0)
        # name 通常是 "bucket/prefix/file.ext"
        if "/" in name:
            uri = "s3://" + name
        else:
            uri = "s3://" + f"{bucket}/{name}"
        out.append((uri, size))
    return out


def _walk_s3_recursive(fs: Any, bucket: str, prefix: str) -> list[tuple[str, int]]:
    """对 ``videos/`` 这种多级目录递归列文件。结果按 path 排序、稳定。"""
    base = f"{bucket}/{prefix.rstrip('/')}"
    try:
        # find 会给出全部 file path
        names = fs.find(base, detail=True)
    except FileNotFoundError:
        return []
    except Exception as err:  # noqa: BLE001
        LOG.warning("lerobot v2 walk failed for s3://%s: %s", base, err)
        return []
    out: list[tuple[str, int]] = []
    if isinstance(names, dict):
        items = names.items()
    else:
        # fs.find(detail=False) 返回 list[str]，没 size
        items = [(name, {"size": 0}) for name in names or []]
    for name, info in items:
        size = int((info or {}).get("size") or 0)
        out.append(("s3://" + name, size))
    out.sort(key=lambda p: p[0])
    return out


def _probe_parquet_footer(s3_uri: str) -> dict[str, Any]:
    """只读 footer：schema / row_count / row_group_count / size。"""
    out: dict[str, Any] = {"uri": s3_uri, "readable": False}
    try:
        fs = get_s3fs()
        bucket, key = split_s3_uri(s3_uri)
        try:
            info = fs.info(f"{bucket}/{key}")
            out["size_bytes"] = int(info.get("size") or 0)
        except Exception:  # noqa: BLE001
            out["size_bytes"] = None
        with fs.open(f"{bucket}/{key}", "rb") as fobj:
            pf = pq.ParquetFile(fobj)
            schema = pf.schema_arrow
            out["readable"] = True
            out["row_count"] = int(pf.metadata.num_rows)
            out["num_row_groups"] = int(pf.num_row_groups)
            names = list(schema.names)
            out["schema_columns"] = names
            out["schema_hash"] = hashlib.sha256(
                "|".join(f"{n}:{schema.field(n).type}" for n in names).encode()
            ).hexdigest()
    except Exception as err:  # noqa: BLE001
        out.update(_summarize_exception(err))
    return out


def profile_lerobot_v2(
    *,
    dataset_uri: str,
    dataset_id: str | None = None,
    version: str | None = None,
    dataset_family: str | None = None,
    layer: str | None = None,
    max_parquet_samples: int = DEFAULT_MAX_PARQUET_SAMPLES,
    parquet_probe_concurrency: int = DEFAULT_PARQUET_PROBE_CONCURRENCY,
) -> AssetProfile:
    """profile lerobot v2 dataset，**不下载任何 parquet / 视频**。

    返回的 ``AssetProfile.profile`` 结构与通用 ``profile_dataset`` 对齐：

    - ``parquet``: list[dict]，每个 dict 至少含 ``readable / row_count / schema_columns
      / schema_hash``（与 droid_metrics 已消费的字段保持一致）；
    - ``files_overview``: ``{"parquet": N_total, "video": N_total, "meta": N_meta}``；
    - ``lerobot_v2``: 额外把 ``info.json`` / ``stats.json`` 关键字段冒泡到 profile 里，
      给 ``droid_metrics`` 直接消费 episodes_count / frames_count / fps。
    """
    base = _normalize_root(dataset_uri)
    if not is_s3_uri(base):
        # 本地暂不实现：lerobot v2 数据集都在 S3 上；调用方应走通用 profile_dataset。
        raise NotImplementedError("local lerobot v2 profile not supported; use profile_dataset")

    fs = get_s3fs()
    bucket, root_key = split_s3_uri(base)

    info = _read_s3_json(fs, bucket, f"{root_key}/{_INFO_REL}") or {}
    stats = _read_s3_json(fs, bucket, f"{root_key}/{_STATS_REL}") or {}
    manifest = _read_s3_json(fs, bucket, f"{root_key}/{_MANIFEST_REL}") or {}

    # 1) 列 data/ 下所有 chunk parquet（chunk-000/file-*.parquet ...）。
    data_files = _walk_s3_recursive(fs, bucket, f"{root_key}/data")
    parquet_uris = [(u, s) for u, s in data_files if u.lower().endswith(".parquet")]

    # 2) 视频只 count，不打开。
    video_files = _walk_s3_recursive(fs, bucket, f"{root_key}/videos")
    video_uris = [(u, s) for u, s in video_files if u.lower().endswith(".mp4")]

    # 3) 抽样 parquet footer，并发 lazy。
    sampled = parquet_uris[:max_parquet_samples]
    parquet_probes: list[dict[str, Any]] = []
    if sampled:
        with ThreadPoolExecutor(max_workers=max(1, parquet_probe_concurrency)) as ex:
            futures = {ex.submit(_probe_parquet_footer, u): u for u, _ in sampled}
            for fut in as_completed(futures):
                uri = futures[fut]
                try:
                    parquet_probes.append(fut.result())
                except Exception as err:  # noqa: BLE001
                    parquet_probes.append({"uri": uri, "readable": False, **_summarize_exception(err)})
    parquet_probes.sort(key=lambda p: p.get("uri", ""))

    # 4) 聚合统计。
    readable_parquet = sum(1 for p in parquet_probes if p.get("readable"))
    schema_hashes = [str(p["schema_hash"]) for p in parquet_probes if p.get("schema_hash")]
    schema_hash = (
        hashlib.sha256("|".join(sorted(set(schema_hashes))).encode()).hexdigest()
        if schema_hashes
        else None
    )
    bytes_total = sum(s for _, s in data_files) + sum(s for _, s in video_files)
    files_count = len(data_files) + len(video_files) + sum(
        1 for src in (info, stats, manifest) if src
    )

    episodes_count = int(info.get("total_episodes") or 0) or None
    frames_count = int(info.get("total_frames") or 0) or None
    fps = info.get("fps") or info.get("video_fps")

    probe_failure_count = sum(1 for p in parquet_probes if not p.get("readable"))
    status = "OK" if probe_failure_count == 0 and parquet_probes else (
        "WARN" if parquet_probes else "FAIL"
    )

    profile_id = (
        f"profile-{hashlib.sha256(base.encode()).hexdigest()[:12]}-"
        f"{int(datetime.now(timezone.utc).timestamp())}"
    )

    return AssetProfile(
        profile_id=profile_id,
        asset_uri=base,
        asset_format="lerobot_v2_parquet_video" if video_uris else "lerobot_v2_parquet",
        dataset_id=dataset_id,
        version=version,
        dataset_family=dataset_family,
        layer=layer,
        bytes=bytes_total or None,
        rows=sum(int(p.get("row_count") or 0) for p in parquet_probes) or None,
        files_count=files_count,
        episodes_count=episodes_count,
        videos_count=len(video_uris) or None,
        schema_hash=schema_hash,
        null_rate=None,
        profile={
            "parquet": parquet_probes,
            "hdf5": [],
            # 视频按 lerobot v2 路径只 list count；不打开，但向后兼容字段保留空 list。
            "video": [],
            "files_overview": {
                "parquet": len(parquet_uris),
                "hdf5": 0,
                "video": len(video_uris),
            },
            "probe_failure_count": probe_failure_count,
            "lerobot_v2": {
                "chunk_files_total": len(parquet_uris),
                "video_files_total": len(video_uris),
                "episodes_count": episodes_count,
                "frames_count": frames_count,
                "fps": fps,
                "stats_keys": sorted(list(stats.keys()))[:50] if isinstance(stats, dict) else [],
                "info_keys": sorted(list(info.keys()))[:50] if isinstance(info, dict) else [],
                "sampled_parquet_count": len(parquet_probes),
                "max_parquet_samples": max_parquet_samples,
            },
        },
        status=status,
    )
