"""partition planner：嗅探数据集结构，按 target size 切 partition。

支持：
- LeRobot / DROID：按 data/*.parquet 切（每个 parquet ~= 1 partition，再按 size 合并）
- robomimic：按 *.hdf5 切（每个 hdf5 1 partition）
- BridgeData：按 parquet shard 切
- 单 demo / endpose.pt：返回单 partition

family detection：通过文件 ext + 目录结构启发式判断。
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import pyarrow.parquet as pq

from robot_dh.lake.store import LakeStore, S3LakeStore, create_lake_store
from robot_dh.lake.uri import is_s3_uri, join_uri, parse_uri
from robot_dh.partition.models import Partition, PartitionPlan, PartitionType

LOG = logging.getLogger(__name__)


_FAMILY_HINTS = {
    "robomimic": (".hdf5", ".h5"),
    "lerobot": (".parquet",),
    "droid": (".parquet",),
    "bridge": (".parquet",),
}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _list_files(store: LakeStore, dataset_uri: str) -> list[tuple[str, int]]:
    """返回 (uri, size_bytes) 列表。"""
    out: list[tuple[str, int]] = []
    if is_s3_uri(dataset_uri):
        if not isinstance(store, S3LakeStore):
            raise RuntimeError("expected S3LakeStore for s3 uri")
        parsed = parse_uri(dataset_uri)
        prefix = parsed.key
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"
        paginator = store.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=parsed.bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                size = int(obj.get("Size", 0))
                out.append((f"s3://{parsed.bucket}/{key}", size))
    else:
        root = Path(parse_uri(dataset_uri).local_path)
        if not root.exists():
            return out
        if root.is_file():
            out.append((root.as_posix(), int(root.stat().st_size)))
            return out
        for f in sorted(root.rglob("*")):
            if f.is_file():
                out.append((f.as_posix(), int(f.stat().st_size)))
    return out


def detect_dataset_family(
    *,
    dataset_uri: str,
    family_hint: str | None = None,
    files: list[tuple[str, int]] | None = None,
) -> str:
    """启发式：返回 'droid' / 'lerobot' / 'robomimic' / 'bridge' / 'universal' / 'demo'。"""
    if family_hint:
        return family_hint.lower()
    files = files if files is not None else []
    lname = dataset_uri.lower()
    if "robomimic" in lname:
        return "robomimic"
    if "droid" in lname:
        return "droid"
    if "lerobot" in lname:
        return "lerobot"
    if "bridge" in lname:
        return "bridge"
    has_parquet = any(p[0].lower().endswith(".parquet") for p in files)
    has_hdf5 = any(p[0].lower().endswith((".hdf5", ".h5")) for p in files)
    has_endpose = any(p[0].lower().endswith("/endpose.pt") for p in files)
    has_video = any(p[0].lower().endswith(".mp4") for p in files)
    if has_endpose:
        return "demo"
    if has_hdf5 and not has_video:
        return "robomimic"
    if has_parquet and has_video:
        return "lerobot"
    if has_parquet:
        return "bridge"
    return "universal"


def _partition_type_for_family(family: str) -> PartitionType:
    if family in ("droid", "lerobot"):
        return "parquet_file"
    if family == "robomimic":
        return "hdf5_file"
    if family == "bridge":
        return "parquet_file"
    return "single"


def _select_input_files(
    family: str, files: list[tuple[str, int]]
) -> list[tuple[str, int]]:
    if family in ("droid", "lerobot", "bridge"):
        return [f for f in files if f[0].lower().endswith(".parquet")]
    if family == "robomimic":
        return [f for f in files if f[0].lower().endswith((".hdf5", ".h5"))]
    return files


def _pack(
    partition_files: list[tuple[str, int]],
    target_bytes: int,
) -> list[list[tuple[str, int]]]:
    """First-Fit-Decreasing 装箱；保持文件顺序由调用方排序。"""
    sorted_files = sorted(partition_files, key=lambda p: p[1], reverse=True)
    groups: list[list[tuple[str, int]]] = []
    sizes: list[int] = []
    for f in sorted_files:
        placed = False
        for i in range(len(groups)):
            if sizes[i] + f[1] <= target_bytes:
                groups[i].append(f)
                sizes[i] += f[1]
                placed = True
                break
        if not placed:
            groups.append([f])
            sizes.append(f[1])
    # 内部按文件名稳定排序，便于复现
    for g in groups:
        g.sort(key=lambda p: p[0])
    return groups


def plan_dataset_partitions(
    *,
    dataset_uri: str,
    dataset_id: str,
    version: str,
    target_partition_size_gb: float = 2.0,
    family_hint: str | None = None,
    plan_id: str | None = None,
) -> PartitionPlan:
    """嗅探 dataset 文件 -> 产出 PartitionPlan。

    单文件 / endpose.pt demo 退化为 1 个 single partition。
    其余按 family 切 file_prefix / hdf5_file / parquet_file。
    """
    target_bytes = max(1, int(target_partition_size_gb * (1024**3)))
    plan_id = plan_id or f"part-{uuid.uuid4().hex[:12]}"
    store = create_lake_store(dataset_uri)
    files = _list_files(store, dataset_uri)
    total_bytes = sum(s for _, s in files)
    family = detect_dataset_family(
        dataset_uri=dataset_uri, family_hint=family_hint, files=files
    )
    partition_type = _partition_type_for_family(family)
    selected = _select_input_files(family, files)
    if not selected:
        # 没有匹配文件 -> 单 partition 兜底
        partition = Partition(
            partition_id=f"{plan_id}-p000",
            partition_index=0,
            dataset_uri=dataset_uri,
            partition_uri=dataset_uri,
            input_files=[uri for uri, _ in files],
            input_bytes=total_bytes,
            estimated_rows=0,
            metrics={"family": family},
        )
        return PartitionPlan(
            partition_plan_id=plan_id,
            dataset_id=dataset_id,
            version=version,
            dataset_uri=dataset_uri,
            dataset_family=family,
            partition_type="single",
            target_partition_size_bytes=target_bytes,
            total_input_bytes=total_bytes,
            partitions=[partition],
            created_at=_utcnow_iso(),
        )

    groups = _pack(selected, target_bytes)
    partitions: list[Partition] = []
    for idx, group in enumerate(groups):
        bytes_total = sum(s for _, s in group)
        rows_estimate = _estimate_rows(group, family)
        partitions.append(
            Partition(
                partition_id=f"{plan_id}-p{idx:03d}",
                partition_index=idx,
                dataset_uri=dataset_uri,
                partition_uri=group[0][0] if len(group) == 1 else dataset_uri,
                input_files=[uri for uri, _ in group],
                input_bytes=bytes_total,
                estimated_rows=rows_estimate,
                metrics={
                    "family": family,
                    "file_count": len(group),
                },
            )
        )
    return PartitionPlan(
        partition_plan_id=plan_id,
        dataset_id=dataset_id,
        version=version,
        dataset_uri=dataset_uri,
        dataset_family=family,
        partition_type=partition_type,
        target_partition_size_bytes=target_bytes,
        total_input_bytes=total_bytes,
        partitions=partitions,
        created_at=_utcnow_iso(),
    )


def _estimate_rows(group: list[tuple[str, int]], family: str) -> int:
    """估计 partition 内行数。

    v1.6.5 起 parquet 系（bridge / lerobot / droid）改读 parquet footer 的
    ``metadata.num_rows`` 真实计数，避免之前 ``bytes / 256`` 在含图像 bytes 的 shard 上
    误差 ~3000 倍（v1.6 bridgedata_v2 partition-plan 报 ``estimated_rows=931394`` vs
    实际 314 行）。footer 只读 ~50 KiB，对 GB 级 shard 也是毫秒级开销。

    读取失败时（远端 footer 截断、本地损坏等）回退到旧的 byte 启发式，确保 plan 不阻塞。
    """
    bytes_total = sum(s for _, s in group)
    if family in ("droid", "lerobot", "bridge"):
        precise = _precise_parquet_rows(group)
        if precise is not None:
            return precise
        return int(bytes_total / 256)
    if family == "robomimic":
        # hdf5 内 demo 行数无法不读出来精确，按 1MB ~ 1k frames 粗估
        return int(bytes_total / 1024)
    return 0


def _precise_parquet_rows(group: list[tuple[str, int]]) -> int | None:
    """对 group 中 parquet 文件挨个读 footer 求 num_rows 之和；任意失败返回 None。"""
    total = 0
    for uri, _size in group:
        if not uri.lower().endswith(".parquet"):
            return None
        try:
            if is_s3_uri(uri):
                from robot_dh.lake.s3_fs import get_s3fs, split_s3_uri

                fs = get_s3fs()
                bucket, key = split_s3_uri(uri)
                with fs.open(f"{bucket}/{key}", "rb") as fobj:
                    total += int(pq.ParquetFile(fobj).metadata.num_rows)
            else:
                local = Path(parse_uri(uri).local_path)
                total += int(pq.ParquetFile(str(local)).metadata.num_rows)
        except Exception as err:  # noqa: BLE001
            LOG.warning("parquet footer read failed for %s: %s; falling back to bytes heuristic", uri, err)
            return None
    return total
