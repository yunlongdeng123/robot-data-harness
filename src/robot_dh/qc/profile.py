"""dataset asset profile：列文件、对每种文件做探针、聚合输出 AssetProfile。

v1.6.5 起：

- parquet 走 `pq.ParquetFile(s3fs.open(...))` lazy 路径，只读 footer，不再 download 全文件；
- HDF5 / 视频沿用 materialize-first，但走带 retry/timeout 的共享 boto3 client，
  失败时 log 里带 ``cause=<具体类>`` 与 ``error_type=...``，停止吞成 "Max Retries Exceeded"；
- profile 出 ``status=FAILED`` 的探针条目时 contract_report 不再静默缺失（v1.6 修了
  ``parquet/hdf5/video`` 三类，AssetProfile.status 也会从 OK 退化到 WARN）。
"""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from concurrent.futures import ThreadPoolExecutor, as_completed

from robot_dh.lake.store import LakeStore, S3LakeStore, create_lake_store
from robot_dh.lake.uri import is_s3_uri, parse_uri
from robot_dh.qc.base import AssetProfile
from robot_dh.qc.hdf5_probe import probe_hdf5
from robot_dh.qc.lerobot_v2 import detect_lerobot_v2, profile_lerobot_v2
from robot_dh.qc.parquet_probe import (
    _summarize_exception,
    probe_parquet,
    probe_parquet_s3,
)
from robot_dh.qc.video_probe import probe_video

LOG = logging.getLogger(__name__)

# HDF5 / 视频探针都要先 download 到 /tmp 再解码，串行 26 个 HDF5 会撞 2h activeDeadline；
# 默认并发 4，可被 ROBOT_DH_QC_PROBE_CONCURRENCY 覆盖。
_DEFAULT_PROBE_CONCURRENCY = 4


def _probe_concurrency() -> int:
    """从环境变量解析并发度，缺省 4；非法值回退到 1（串行）。"""
    raw = os.environ.get("ROBOT_DH_QC_PROBE_CONCURRENCY")
    if not raw:
        return _DEFAULT_PROBE_CONCURRENCY
    try:
        return max(1, int(raw))
    except ValueError:
        return 1


def _list_files(dataset_uri: str) -> list[tuple[str, int]]:
    store = create_lake_store(dataset_uri)
    if is_s3_uri(dataset_uri):
        if not isinstance(store, S3LakeStore):
            raise RuntimeError("expected S3LakeStore for s3 uri")
        parsed = parse_uri(dataset_uri)
        prefix = parsed.key
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"
        out: list[tuple[str, int]] = []
        paginator = store.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=parsed.bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                out.append((f"s3://{parsed.bucket}/{obj['Key']}", int(obj.get("Size", 0))))
        return out
    root = Path(parse_uri(dataset_uri).local_path)
    if not root.exists():
        return []
    if root.is_file():
        return [(root.as_posix(), int(root.stat().st_size))]
    return [(f.as_posix(), int(f.stat().st_size)) for f in sorted(root.rglob("*")) if f.is_file()]


def _materialize_local(uri: str, target: Path, *, fast: bool = False) -> Path:
    """S3 -> 下载到本地（boto3 + retry/timeout 共享 client）；本地 -> 直接返回路径。

    v1.6.8（fvx5z F2 修复）：``fast=True`` 切到
    ``get_s3_boto_client_fast()``，单文件 worst-case 195s 而不是默认 3000s。HDF5
    probe 路径必须传 ``fast=True``，否则 robomimic 26 文件 × 默认 3000s/file 永远
    撞不到 contract aggregate 这一步。
    """
    if not is_s3_uri(uri):
        return Path(parse_uri(uri).local_path)
    from robot_dh.lake.s3_fs import (
        get_s3_boto_client,
        get_s3_boto_client_fast,
        split_s3_uri,
    )

    target.mkdir(parents=True, exist_ok=True)
    bucket, key = split_s3_uri(uri)
    local_file = target / key.rsplit("/", 1)[-1]
    client = get_s3_boto_client_fast() if fast else get_s3_boto_client()
    client.download_file(bucket, key, str(local_file))
    return local_file


def _log_probe_failure(kind: str, uri: str, probe: dict[str, Any]) -> None:
    """probe 返回 dict 里带 error_type 时，统一打出 cause= 字串，便于事后排障。

    v1.6.7：cause_type 为 None（典型场景：botocore RetriesExceededError）时把
    traceback 尾段也打到 WARNING，至少给排障人一条可追的堆栈。
    """
    if not probe.get("error_type"):
        return
    LOG.warning(
        "%s probe failed for %s: error_type=%s error=%s cause_type=%s cause=%s",
        kind, uri,
        probe.get("error_type"), probe.get("error"),
        probe.get("cause_type"), probe.get("cause"),
    )
    tb = probe.get("traceback")
    if probe.get("cause_type") is None and tb:
        LOG.warning("%s probe traceback for %s:\n%s", kind, uri, tb)


def _probe_parquet_uri(uri: str, tmp: Path) -> dict[str, Any]:
    """S3 parquet 走 lazy footer；本地 parquet 直接 probe。"""
    if is_s3_uri(uri):
        result = probe_parquet_s3(uri)
        if result.get("readable"):
            return result
        # lazy 路径失败（极少；常见原因是 SSL 中断/footer 截断）就回退一次 materialize-first，
        # 这一档至少能把 schema 给捞回来，避免 contract_report 完全空白。
        LOG.warning(
            "lazy parquet probe failed for %s (error_type=%s); falling back to materialize-first",
            uri, result.get("error_type"),
        )
        try:
            local = _materialize_local(uri, tmp / "pq_fallback")
            return probe_parquet(local)
        except Exception as err:  # noqa: BLE001
            return {"uri": uri, "readable": False, **_summarize_exception(err)}
    local = Path(parse_uri(uri).local_path)
    return probe_parquet(local)


def _probe_hdf5_uri(uri: str, tmp: Path) -> dict[str, Any]:
    """HDF5 始终 materialize-first（cloud-native HDF5 reader 当前 ROI 不够）。

    v1.6.8（fvx5z F2）：走 ``fast`` 档 boto3 client（read_timeout=60s × 3 attempts），
    避免默认档 300s × 10 把单文件单次 download 拖到 50min；并发 ~4 个 worker
    在 1800s step deadline 内能稳跑完 26 文件。
    """
    # v1.7：本地 file:// URI 时 ``_materialize_local`` 直接返回原 path，
    # 此时不能 unlink，否则 probe 会把 raw 数据本身删掉。仅 S3 下需要清理 tmp 副本。
    owns_local = is_s3_uri(uri)
    try:
        local = _materialize_local(uri, tmp / "hdf5", fast=True)
    except Exception as err:  # noqa: BLE001
        return {"uri": uri, "readable": False, **_summarize_exception(err)}
    out = probe_hdf5(local)
    out["uri"] = uri
    # 即下即删，控制 /tmp 占用：26 × 1.1 GiB 高峰只剩 4 路并发 × 1 个 = 4.4 GiB。
    if owns_local:
        try:
            local.unlink(missing_ok=True)
        except OSError:
            pass
    return out


def _probe_video_uri(uri: str, tmp: Path) -> dict[str, Any]:
    owns_local = is_s3_uri(uri)
    try:
        local = _materialize_local(uri, tmp / "mp4", fast=True)
    except Exception as err:  # noqa: BLE001
        return {"uri": uri, "readable": False, **_summarize_exception(err)}
    out = probe_video(local)
    out["uri"] = uri
    if owns_local:
        try:
            local.unlink(missing_ok=True)
        except OSError:
            pass
    return out


def profile_dataset(
    *,
    dataset_uri: str,
    dataset_id: str | None = None,
    version: str | None = None,
    dataset_family: str | None = None,
    layer: str | None = None,
    sample_limit: int = 32,
) -> AssetProfile:
    """对 dataset 全量列文件并按 ext 调用探针；为大目录限制 probe 数量。

    v1.6.6 起：

    - LeRobot v2 dataset（``meta/info.json`` 存在）走 lazy 专属路径
      （``profile_lerobot_v2``），整 parquet / 视频都不下载；
    - HDF5 / 视频探针并发化，串行 26 文件 ~2h 收敛到 ~15min；并发度由
      ``ROBOT_DH_QC_PROBE_CONCURRENCY`` 控制，缺省 4。
    """
    # 1) lerobot v2 走专属 lazy 路径，整 parquet/视频都不下载。
    if is_s3_uri(dataset_uri) and detect_lerobot_v2(dataset_uri):
        LOG.info("profile_dataset: detected lerobot v2 layout, using lazy footer path: %s", dataset_uri)
        return profile_lerobot_v2(
            dataset_uri=dataset_uri,
            dataset_id=dataset_id,
            version=version,
            dataset_family=dataset_family,
            layer=layer,
        )

    files = _list_files(dataset_uri)
    files_count = len(files)
    bytes_total = sum(s for _, s in files)
    parquet_files = [f for f in files if f[0].lower().endswith(".parquet")]
    hdf5_files = [f for f in files if f[0].lower().endswith((".hdf5", ".h5"))]
    video_files = [f for f in files if f[0].lower().endswith(".mp4")]

    parquet_probes: list[dict[str, Any]] = []
    hdf5_probes: list[dict[str, Any]] = []
    video_probes: list[dict[str, Any]] = []

    rows_total = 0
    null_rates: list[float] = []
    schema_hashes: list[str] = []
    probe_failure_count = 0

    concurrency = _probe_concurrency()

    with tempfile.TemporaryDirectory(prefix="robot-dh-profile-") as tmp_str:
        tmp = Path(tmp_str)

        # parquet：lazy 路径不下载，串行就够；偶发 SSL 中断 fallback materialize-first。
        for uri, _size in parquet_files[:sample_limit]:
            p = _probe_parquet_uri(uri, tmp)
            p["uri"] = uri
            parquet_probes.append(p)
            _log_probe_failure("parquet", uri, p)
            if not p.get("readable"):
                probe_failure_count += 1
                continue
            rows_total += int(p.get("row_count") or 0)
            if p.get("null_rate") is not None:
                null_rates.append(float(p["null_rate"]))
            if p.get("schema_hash"):
                schema_hashes.append(str(p["schema_hash"]))

        # HDF5 / 视频走 materialize-first，必须并发覆盖 download 主导耗时。
        hdf5_sample = hdf5_files[:sample_limit]
        if hdf5_sample:
            hdf5_probes.extend(_run_parallel_probes(
                hdf5_sample, lambda uri: _probe_hdf5_uri(uri, tmp), "hdf5", concurrency,
            ))
        video_sample = video_files[:sample_limit]
        if video_sample:
            video_probes.extend(_run_parallel_probes(
                video_sample, lambda uri: _probe_video_uri(uri, tmp), "video", concurrency,
            ))

        for p in hdf5_probes:
            if not p.get("readable"):
                probe_failure_count += 1
        for p in video_probes:
            if not p.get("readable"):
                probe_failure_count += 1

    schema_hash = hashlib.sha256("|".join(sorted(set(schema_hashes))).encode()).hexdigest() if schema_hashes else None
    null_rate = float(sum(null_rates) / len(null_rates)) if null_rates else None

    episodes_count = sum(int(p.get("demo_count") or 0) for p in hdf5_probes)
    if not episodes_count and parquet_probes:
        episodes_count = sum(1 for p in parquet_probes if p.get("readable"))

    profile_id = f"profile-{hashlib.sha256(dataset_uri.encode()).hexdigest()[:12]}-{int(datetime.now(timezone.utc).timestamp())}"

    status = "OK" if probe_failure_count == 0 else "WARN"

    return AssetProfile(
        profile_id=profile_id,
        asset_uri=dataset_uri,
        asset_format=_guess_format(parquet_files, hdf5_files, video_files),
        dataset_id=dataset_id,
        version=version,
        dataset_family=dataset_family,
        layer=layer,
        bytes=bytes_total,
        rows=rows_total or None,
        files_count=files_count,
        episodes_count=episodes_count or None,
        videos_count=len(video_files) or None,
        schema_hash=schema_hash,
        null_rate=null_rate,
        profile={
            "parquet": parquet_probes,
            "hdf5": hdf5_probes,
            "video": video_probes,
            "files_overview": {
                "parquet": len(parquet_files),
                "hdf5": len(hdf5_files),
                "video": len(video_files),
            },
            "probe_failure_count": probe_failure_count,
        },
        status=status,
    )


def _run_parallel_probes(
    items: list[tuple[str, int]],
    probe_fn,
    kind: str,
    concurrency: int,
) -> list[dict[str, Any]]:
    """对 (uri, size) 列表并发执行 probe_fn(uri)；结果按原顺序返回，失败也兜底。"""
    if concurrency <= 1 or len(items) <= 1:
        results: list[dict[str, Any]] = []
        for uri, _size in items:
            try:
                probe = probe_fn(uri)
            except Exception as err:  # noqa: BLE001
                probe = {"uri": uri, "readable": False, **_summarize_exception(err)}
            _log_probe_failure(kind, uri, probe)
            results.append(probe)
        return results

    indexed: list[tuple[int, dict[str, Any]]] = [None] * len(items)  # type: ignore[list-item]
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {ex.submit(probe_fn, uri): (idx, uri) for idx, (uri, _) in enumerate(items)}
        for fut in as_completed(futures):
            idx, uri = futures[fut]
            try:
                probe = fut.result()
            except Exception as err:  # noqa: BLE001
                probe = {"uri": uri, "readable": False, **_summarize_exception(err)}
            _log_probe_failure(kind, uri, probe)
            indexed[idx] = (idx, probe)
    return [item[1] for item in indexed if item is not None]


def _guess_format(parquet_files, hdf5_files, video_files) -> str | None:
    if hdf5_files and not parquet_files:
        return "hdf5"
    if parquet_files and video_files:
        return "lerobot_parquet_video"
    if parquet_files:
        return "parquet"
    if video_files:
        return "video"
    return None
