"""lineage.json：raw -> ods -> dwd -> qc -> ml-ready。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_lineage(
    *,
    dataset_id: str,
    version: str,
    output_uri: str,
    input_root: str,
    quality_root: str | None,
    qc_root: str | None,
    train_uri: str,
    val_uri: str,
    test_uri: str,
) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "version": version,
        "output_uri": output_uri,
        "edges": [
            {"from": input_root, "to": output_uri, "kind": "ml_ready_export"},
            *([{"from": quality_root, "to": output_uri, "kind": "quality_filter"}] if quality_root else []),
            *([{"from": qc_root, "to": output_uri, "kind": "qc_filter"}] if qc_root else []),
            {"from": output_uri, "to": train_uri, "kind": "split.train"},
            {"from": output_uri, "to": val_uri, "kind": "split.val"},
            {"from": output_uri, "to": test_uri, "kind": "split.test"},
        ],
        "created_at": utcnow_iso(),
    }
