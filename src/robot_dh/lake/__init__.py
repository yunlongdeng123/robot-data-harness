"""robot-dh v1.4 数据湖层。

公开 API：
    LakeUri, parse_uri, join_uri, is_s3_uri, is_local_uri
    LakeStore, LocalLakeStore, S3LakeStore, create_lake_store
    ManifestBuilder, compute_file_sha256, collect_file_stats, write_manifest, read_manifest
    audit_lake
"""

from robot_dh.lake.uri import LakeUri, is_local_uri, is_s3_uri, join_uri, parse_uri
from robot_dh.lake.store import (
    LakeStore,
    LocalLakeStore,
    S3LakeStore,
    create_lake_store,
)
from robot_dh.lake.manifest import (
    ManifestBuilder,
    collect_file_stats,
    compute_file_sha256,
    read_manifest,
    write_manifest,
)
from robot_dh.lake.audit import audit_lake

__all__ = [
    "LakeUri",
    "parse_uri",
    "join_uri",
    "is_s3_uri",
    "is_local_uri",
    "LakeStore",
    "LocalLakeStore",
    "S3LakeStore",
    "create_lake_store",
    "ManifestBuilder",
    "compute_file_sha256",
    "collect_file_stats",
    "write_manifest",
    "read_manifest",
    "audit_lake",
]
