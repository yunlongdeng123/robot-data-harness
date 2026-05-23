"""scale ETL planner：从 raw root 发现数据集 -> 按 size 装箱到 N 个 shard。

支持本地 + S3。发现策略：
  - 直接列举 <root>/raw/<dataset_id>/<version> 形态的 prefix。
  - 数据特征要求：含 endpose.pt / *.parquet / *.hdf5 / *.mp4 / meta.yaml 中至少一种。
  - 支持 include / exclude glob 列表（针对 dataset_id 进行匹配）。
  - input_bytes 估算：S3 用 list_objects_v2 + Size 累加；本地 stat。
"""

from __future__ import annotations

import fnmatch
import logging
from pathlib import Path
from typing import Iterable

from robot_dh.lake.store import S3LakeStore, create_lake_store
from robot_dh.lake.uri import is_s3_uri, parse_uri
from robot_dh.runtime.events import utcnow_iso
from robot_dh.runtime.ids import new_plan_id, new_shard_id
from robot_dh.sharding.models import EtlPlan, PlanDataset, PlanShard

LOG = logging.getLogger(__name__)


_DATA_FILE_EXTS = (".pt", ".parquet", ".hdf5", ".h5", ".mp4", ".yaml", ".yml", ".json", ".tar")


def _matches_any(name: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(name, pattern) for pattern in patterns)


def _looks_like_dataset(uris: Iterable[str]) -> bool:
    for uri in uris:
        if any(uri.lower().endswith(ext) for ext in _DATA_FILE_EXTS):
            return True
    return False


def _raw_prefix_from_root_key(root_key: str) -> str:
    """把 bucket 根或 raw 前缀统一成 S3 list_objects 可用前缀。"""
    base_key = root_key.strip("/")
    if not base_key:
        return "raw/"
    if base_key == "raw" or base_key.endswith("/raw"):
        return base_key + "/"
    return base_key + "/raw/"


def _discover_s3_dataset_prefixes(store: S3LakeStore, root_uri: str) -> list[tuple[str, str, str]]:
    """S3 下列出 raw/<id>/<ver>/... 前缀对应的 dataset。"""
    parsed = parse_uri(root_uri)
    prefix = _raw_prefix_from_root_key(parsed.key)

    seen: set[tuple[str, str]] = set()
    paginator = store.client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=parsed.bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            rel = key[len(prefix):] if key.startswith(prefix) else key
            parts = rel.split("/")
            if len(parts) < 3:
                continue
            ds = parts[0]
            ver = parts[1]
            if ds == "" or ver == "":
                continue
            seen.add((ds, ver))
    out: list[tuple[str, str, str]] = []
    for ds, ver in sorted(seen):
        dataset_uri = f"s3://{parsed.bucket}/{prefix}{ds}/{ver}/"
        out.append((ds, ver, dataset_uri))
    return out


def _discover_local_dataset_prefixes(root_uri: str) -> list[tuple[str, str, str]]:
    root_local = Path(parse_uri(root_uri).local_path)
    out: list[tuple[str, str, str]] = []
    if not root_local.exists():
        LOG.warning("planner: local root not found: %s", root_local)
        return out
    raw_dir = root_local / "raw"
    if not raw_dir.is_dir():
        # 仍支持直接传 dataset 目录
        for ds_dir in sorted(p for p in root_local.iterdir() if p.is_dir()):
            for ver_dir in sorted(p for p in ds_dir.iterdir() if p.is_dir()):
                files = [f.as_posix() for f in ver_dir.rglob("*") if f.is_file()]
                if _looks_like_dataset(files):
                    out.append((ds_dir.name, ver_dir.name, ver_dir.as_posix()))
        return out
    for ds_dir in sorted(p for p in raw_dir.iterdir() if p.is_dir()):
        for ver_dir in sorted(p for p in ds_dir.iterdir() if p.is_dir()):
            files = [f.as_posix() for f in ver_dir.rglob("*") if f.is_file()]
            if _looks_like_dataset(files):
                out.append((ds_dir.name, ver_dir.name, ver_dir.as_posix()))
    return out


def _estimate_s3_dataset_bytes(store: S3LakeStore, uri: str) -> int:
    parsed = parse_uri(uri)
    prefix = parsed.key
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    total = 0
    paginator = store.client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=parsed.bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            total += int(obj.get("Size", 0))
    return total


def _estimate_local_dataset_bytes(uri: str) -> int:
    path = Path(parse_uri(uri).local_path)
    if path.is_file():
        return int(path.stat().st_size)
    total = 0
    for f in path.rglob("*"):
        if f.is_file():
            total += int(f.stat().st_size)
    return total


def discover_datasets(root_uri: str) -> list[PlanDataset]:
    """从 root_uri/raw 下发现 dataset，返回未过滤的 PlanDataset 列表。"""
    if is_s3_uri(root_uri):
        store = create_lake_store(root_uri)
        if not isinstance(store, S3LakeStore):
            raise RuntimeError("expected S3LakeStore for s3 root_uri")
        items = _discover_s3_dataset_prefixes(store, root_uri)
        out: list[PlanDataset] = []
        for ds, ver, uri in items:
            input_bytes = _estimate_s3_dataset_bytes(store, uri)
            out.append(PlanDataset(dataset_id=ds, version=ver, dataset_uri=uri, input_bytes=input_bytes))
        return out
    items = _discover_local_dataset_prefixes(root_uri)
    out = []
    for ds, ver, uri in items:
        out.append(
            PlanDataset(
                dataset_id=ds,
                version=ver,
                dataset_uri=uri,
                input_bytes=_estimate_local_dataset_bytes(uri),
            )
        )
    return out


def _pack_into_shards(
    datasets: list[PlanDataset],
    target_bytes: int,
    max_shards: int,
    plan_id: str,
) -> list[PlanShard]:
    """First-Fit Decreasing 装箱；尽量使每个 shard 不超过 target_bytes，硬上限 max_shards。"""
    sorted_ds = sorted(datasets, key=lambda d: d.input_bytes, reverse=True)
    shards: list[PlanShard] = []
    for ds in sorted_ds:
        placed = False
        for shard in shards:
            if shard.total_bytes + ds.input_bytes <= target_bytes:
                shard.datasets.append(ds)
                shard.total_bytes += ds.input_bytes
                placed = True
                break
        if not placed:
            if len(shards) >= max_shards:
                # 满了之后追加到最小的 shard
                smallest = min(shards, key=lambda s: s.total_bytes)
                smallest.datasets.append(ds)
                smallest.total_bytes += ds.input_bytes
            else:
                idx = len(shards)
                shards.append(
                    PlanShard(
                        shard_id=new_shard_id(plan_id, idx),
                        shard_index=idx,
                        datasets=[ds],
                        total_bytes=ds.input_bytes,
                        status="PENDING",
                    )
                )
    # 至少有一个 shard（即便没有数据），便于下游统一处理
    if not shards:
        shards.append(
            PlanShard(
                shard_id=new_shard_id(plan_id, 0),
                shard_index=0,
                datasets=[],
                total_bytes=0,
                status="PENDING",
            )
        )
    return shards


def plan_etl(
    *,
    root_uri: str,
    lake_root: str,
    target_shard_size_gb: float = 5.0,
    max_shards: int = 16,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    plan_id: str | None = None,
) -> EtlPlan:
    """枚举 root_uri/raw 下数据集并装箱为 shard。"""
    target_bytes = int(target_shard_size_gb * (1024**3))
    plan_id = plan_id or new_plan_id()
    datasets = discover_datasets(root_uri)
    include = list(include_patterns or [])
    exclude = list(exclude_patterns or [])
    if include:
        datasets = [d for d in datasets if _matches_any(d.dataset_id, include) or _matches_any(d.dataset_uri, include)]
    if exclude:
        datasets = [
            d
            for d in datasets
            if not _matches_any(d.dataset_id, exclude) and not _matches_any(d.dataset_uri, exclude)
        ]

    shards = _pack_into_shards(datasets, target_bytes, max_shards, plan_id)
    total_bytes = sum(d.input_bytes for d in datasets)
    plan = EtlPlan(
        plan_id=plan_id,
        created_at=utcnow_iso(),
        root_uri=root_uri,
        lake_root=lake_root,
        target_shard_size_bytes=target_bytes,
        total_datasets=len(datasets),
        total_bytes=total_bytes,
        shards=shards,
        include_patterns=include,
        exclude_patterns=exclude,
    )
    return plan
