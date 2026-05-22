"""_manifest.json 构建与辅助函数。

ods/dwd/ads 各 slice 产出旁路 _manifest.json，字段见 docs/v1_4_handoff_inbox.md（Manifest 节）。
本模块为磁盘 manifest 结构的唯一约定来源。
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pyarrow.parquet as pq

from robot_dh.lake.store import LakeStore
from robot_dh.lake.uri import join_uri

MANIFEST_FILENAME = "_manifest.json"
MANIFEST_SCHEMA_VERSION = "1.4"


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compute_file_sha256(local_path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with local_path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _parquet_row_count(local_path: Path) -> int | None:
    if local_path.suffix.lower() != ".parquet":
        return None
    try:
        return int(pq.ParquetFile(str(local_path)).metadata.num_rows)
    except Exception:
        return None


def collect_file_stats(
    local_dir: Path,
    base_uri: str,
    files: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """扫描 local_dir 顶层（非递归）或白名单文件，统计 size/row_count/checksum，并用 join_uri 映射到 base_uri。"""

    local_dir = local_dir.expanduser().resolve()
    if not local_dir.is_dir():
        raise FileNotFoundError(f"manifest source directory not found: {local_dir}")

    selected: list[Path]
    if files is None:
        selected = [p for p in sorted(local_dir.iterdir()) if p.is_file() and p.name != MANIFEST_FILENAME]
    else:
        selected = []
        for name in files:
            p = local_dir / name
            if p.is_file():
                selected.append(p)

    out: list[dict[str, Any]] = []
    for path in selected:
        out.append(
            {
                "path": path.name,
                "uri": join_uri(base_uri, path.name),
                "format": path.suffix.lstrip(".") or "raw",
                "size_bytes": int(path.stat().st_size),
                "row_count": _parquet_row_count(path),
                "checksum_sha256": compute_file_sha256(path),
            }
        )
    return out


@dataclass(slots=True)
class JobInfo:
    job_id: str
    job_type: str
    started_at: str
    finished_at: str
    duration_sec: float


@dataclass(slots=True)
class CodeInfo:
    package_version: str
    git_commit: str | None = None


@dataclass(slots=True)
class ManifestBuilder:
    dataset_id: str
    version: str
    layer: str
    output_uri: str
    source_uris: list[str]
    files: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    job: JobInfo | None = None
    code: CodeInfo | None = None
    schema_version: str = MANIFEST_SCHEMA_VERSION
    created_at: str = field(default_factory=utcnow_iso)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "dataset_id": self.dataset_id,
            "version": self.version,
            "layer": self.layer,
            "created_at": self.created_at,
            "schema_version": self.schema_version,
            "source_uris": list(self.source_uris),
            "output_uri": self.output_uri,
            "files": list(self.files),
            "metrics": dict(self.metrics),
        }
        if self.job is not None:
            payload["job"] = asdict(self.job)
        if self.code is not None:
            payload["code"] = {
                k: v for k, v in asdict(self.code).items() if v is not None or k == "package_version"
            }
        return payload


def write_manifest(
    store: LakeStore,
    builder: ManifestBuilder,
) -> str:
    """序列化 manifest 并写入 output_uri/_manifest.json。"""
    manifest_uri = join_uri(builder.output_uri, MANIFEST_FILENAME)
    store.write_json(manifest_uri, builder.to_dict())
    return manifest_uri


def read_manifest(store: LakeStore, layer_uri: str) -> dict[str, Any]:
    """从 layer URI（目录或 manifest 文件本身）读取 _manifest.json。"""
    if layer_uri.endswith(MANIFEST_FILENAME):
        manifest_uri = layer_uri
    else:
        manifest_uri = join_uri(layer_uri, MANIFEST_FILENAME)
    return store.read_json(manifest_uri)
