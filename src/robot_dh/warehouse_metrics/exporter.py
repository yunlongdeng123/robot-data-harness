"""warehouse export：把 query 结果写成 parquet / csv / json，并落 _manifest.json。

输出 URI 支持：
    - 本地路径 / file://     直接落本地（_manifest 同目录）
    - s3://                  通过 fsspec/s3fs 写远端

format=parquet 需要 pyarrow；缺失时回退到 csv 并在 manifest.warnings 里说明。
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

LOG = logging.getLogger(__name__)


@dataclass
class ExportManifest:
    table: str
    dt: str
    format: str
    row_count: int
    output_uri: str
    created_at: str
    source_tables: list[str] = field(default_factory=list)
    checksum_sha256: str | None = None
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "dt": self.dt,
            "format": self.format,
            "row_count": self.row_count,
            "output_uri": self.output_uri,
            "created_at": self.created_at,
            "source_tables": list(self.source_tables),
            "checksum_sha256": self.checksum_sha256,
            "warnings": list(self.warnings),
        }


class WarehouseExporter:
    """warehouse 导出器。"""

    def __init__(self) -> None:
        self._pyarrow_available = _pyarrow_available()

    def export(
        self,
        *,
        rows: list[dict[str, Any]],
        table: str,
        dt: str,
        output_uri: str,
        format: str = "parquet",
        source_tables: list[str] | None = None,
    ) -> ExportManifest:
        fmt = format.lower()
        if fmt not in ("parquet", "csv", "json"):
            raise ValueError(f"unsupported export format '{format}'")
        warnings: list[str] = []
        if fmt == "parquet" and not self._pyarrow_available:
            warnings.append("pyarrow not available; falling back to csv")
            fmt = "csv"

        normalized_rows = [_normalize_row(r) for r in rows]
        payload_bytes = _serialize(normalized_rows, fmt)
        checksum = hashlib.sha256(payload_bytes).hexdigest() if payload_bytes else None

        data_uri = _ensure_data_uri(output_uri, table=table, fmt=fmt)
        _write_bytes(data_uri, payload_bytes)
        manifest_uri = _manifest_uri(data_uri)

        manifest = ExportManifest(
            table=table,
            dt=dt,
            format=fmt,
            row_count=len(normalized_rows),
            output_uri=data_uri,
            created_at=datetime.now(timezone.utc).isoformat(),
            source_tables=sorted(set(source_tables or [table])),
            checksum_sha256=checksum,
            warnings=warnings,
        )
        _write_bytes(manifest_uri, json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False).encode("utf-8"))
        return manifest


def _pyarrow_available() -> bool:
    try:
        import pyarrow  # noqa: F401
        import pyarrow.parquet  # noqa: F401
        return True
    except ImportError:
        return False


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {str(k): _normalize_value(v) for k, v in row.items()}


def _normalize_value(value: Any) -> Any:
    from datetime import date

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=str)
    return value


def _serialize(rows: list[dict[str, Any]], fmt: str) -> bytes:
    if not rows and fmt != "json":
        if fmt == "csv":
            return b""
        if fmt == "parquet":
            import pyarrow as pa
            import pyarrow.parquet as pq

            buf = io.BytesIO()
            pq.write_table(pa.table({}), buf)
            return buf.getvalue()
    if fmt == "json":
        return json.dumps(rows, indent=2, ensure_ascii=False).encode("utf-8")
    if fmt == "csv":
        keys: list[str] = []
        seen: set[str] = set()
        for r in rows:
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    keys.append(k)
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=keys)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in keys})
        return buf.getvalue().encode("utf-8")
    if fmt == "parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pylist(rows)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        return buf.getvalue()
    raise ValueError(f"unknown format '{fmt}'")


def _ensure_data_uri(output: str, *, table: str, fmt: str) -> str:
    """规整 output_uri：如果是目录就追加 ``{table}.{fmt}``；否则保留原 URI。"""
    suffix = {"parquet": ".parquet", "csv": ".csv", "json": ".json"}[fmt]
    if output.endswith("/"):
        return f"{output}{table}{suffix}"
    parsed = urlparse(output)
    base = parsed.path
    if base.endswith("/") or not Path(base).suffix:
        sep = "" if output.endswith("/") else "/"
        return f"{output}{sep}{table}{suffix}"
    if Path(base).suffix.lower() != suffix:
        return f"{output}{suffix}"
    return output


def _manifest_uri(data_uri: str) -> str:
    parsed = urlparse(data_uri)
    if parsed.scheme in ("", "file"):
        base = parsed.path if parsed.scheme == "file" else data_uri
        return str(Path(base).parent / "_manifest.json") if parsed.scheme == "" else f"file://{Path(parsed.path).parent.as_posix()}/_manifest.json"
    parent = data_uri.rsplit("/", 1)[0]
    return f"{parent}/_manifest.json"


def _write_bytes(uri: str, content: bytes) -> None:
    parsed = urlparse(uri)
    if parsed.scheme in ("", "file"):
        path = Path(parsed.path) if parsed.scheme == "file" else Path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return
    if parsed.scheme == "s3":
        try:
            import s3fs

            fs = s3fs.S3FileSystem()
            with fs.open(uri, "wb") as f:
                f.write(content)
            return
        except ImportError as err:
            raise RuntimeError(f"s3fs not available; cannot write {uri}: {err}") from err
    raise ValueError(f"unsupported URI scheme: {parsed.scheme!r}")
