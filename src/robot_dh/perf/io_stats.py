"""统计 LakeStore URI 下的对象总字节数与 parquet 行数。"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq

from robot_dh.lake.store import LakeStore, S3LakeStore, create_lake_store
from robot_dh.lake.uri import is_s3_uri, parse_uri


def measure_local_dir_bytes(local_dir: Path) -> int:
    if not local_dir.exists():
        return 0
    total = 0
    for f in local_dir.rglob("*"):
        if f.is_file():
            total += int(f.stat().st_size)
    return total


def measure_local_parquet_rows(local_dir: Path) -> int:
    if not local_dir.exists():
        return 0
    rows = 0
    for f in local_dir.rglob("*.parquet"):
        try:
            rows += int(pq.ParquetFile(str(f)).metadata.num_rows)
        except Exception:
            continue
    return rows


def measure_uri_bytes(uri: str, *, store: LakeStore | None = None) -> int:
    """估算 uri 下所有对象总字节；本地走 stat，S3 走 list_objects_v2 + Size。"""
    if not uri:
        return 0
    if is_s3_uri(uri):
        store = store or create_lake_store(uri)
        if not isinstance(store, S3LakeStore):
            return 0
        parsed = parse_uri(uri)
        prefix = parsed.key
        if prefix and not prefix.endswith("/"):
            try:
                head = store.client.head_object(Bucket=parsed.bucket, Key=parsed.key)
                return int(head.get("ContentLength", 0))
            except Exception:
                prefix = prefix + "/"
        total = 0
        paginator = store.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=parsed.bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                total += int(obj.get("Size", 0))
        return total
    local_path = Path(parse_uri(uri).local_path)
    if local_path.is_file():
        return int(local_path.stat().st_size)
    return measure_local_dir_bytes(local_path)


def sum_file_sizes(files: Iterable[dict]) -> int:
    """已经 collect_file_stats 得到的列表的 size_bytes 求和。"""
    total = 0
    for info in files:
        try:
            total += int(info.get("size_bytes") or 0)
        except Exception:
            continue
    return total


def sum_file_rows(files: Iterable[dict]) -> int:
    total = 0
    for info in files:
        try:
            rc = info.get("row_count")
            if rc is not None:
                total += int(rc)
        except Exception:
            continue
    return total
