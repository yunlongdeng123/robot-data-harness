"""数据湖审计：检查 bucket / prefix / manifest / postgres-lake-tables 是否存在。

供 `robot-dh lake audit`（CLI）与 FastAPI 健康端点调用；返回结构化 dict 与汇总状态 PASS / WARN / FAIL。
"""

from __future__ import annotations

import os
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from robot_dh.lake.manifest import MANIFEST_FILENAME
from robot_dh.lake.store import LakeStore, S3LakeStore, create_lake_store
from robot_dh.lake.uri import is_s3_uri, join_uri, parse_uri

LAKE_LAYERS = ("raw", "ods", "dwd", "ads", "lineage", "tmp")

WAREHOUSE_TABLES = (
    "lake_assets",
    "etl_jobs",
    "lineage_edges",
    "dataset_versions",
    "quality_snapshots",
)


def _status_worst(*statuses: str) -> str:
    if "FAIL" in statuses:
        return "FAIL"
    if "WARN" in statuses:
        return "WARN"
    return "PASS"


def _check_bucket_visible(store: S3LakeStore, bucket: str) -> dict[str, Any]:
    try:
        store.client.head_bucket(Bucket=bucket)
        return {"name": "bucket", "bucket": bucket, "status": "PASS"}
    except (ClientError, BotoCoreError) as err:
        return {"name": "bucket", "bucket": bucket, "status": "FAIL", "error": str(err)}


def _check_layer_prefix(store: LakeStore, lake_root_uri: str, layer: str) -> dict[str, Any]:
    layer_uri = join_uri(lake_root_uri, layer)
    try:
        listed = store.list(layer_uri)
        return {
            "name": f"prefix:{layer}",
            "uri": layer_uri,
            "object_count": len(listed),
            "status": "PASS",
        }
    except (ClientError, BotoCoreError) as err:
        return {
            "name": f"prefix:{layer}",
            "uri": layer_uri,
            "status": "FAIL",
            "error": str(err),
        }


def _check_manifest_completeness(store: LakeStore, lake_root_uri: str) -> dict[str, Any]:
    """校验 ods/、dwd/ 下每个 (dataset_id, version) 的 _manifest.json 存在且含必填键。"""
    required_keys = {
        "dataset_id",
        "version",
        "layer",
        "created_at",
        "schema_version",
        "source_uris",
        "output_uri",
        "files",
    }
    missing_layers: list[str] = []
    incomplete: list[dict[str, Any]] = []
    layers_checked: dict[str, int] = {}

    for layer in ("ods", "dwd"):
        layer_uri = join_uri(lake_root_uri, layer)
        seen_pairs: set[tuple[str, str]] = set()
        try:
            objects = store.list(layer_uri)
        except (ClientError, BotoCoreError) as err:
            missing_layers.append(layer)
            incomplete.append({"layer": layer, "error": str(err)})
            continue
        prefix_marker = parse_uri(layer_uri).key if is_s3_uri(layer_uri) else layer_uri
        prefix_marker = prefix_marker.rstrip("/") + "/" if prefix_marker else ""
        for uri in objects:
            tail = parse_uri(uri).key if is_s3_uri(uri) else uri
            if prefix_marker and tail.startswith(prefix_marker):
                rel = tail[len(prefix_marker):]
            else:
                rel = tail
            parts = rel.split("/")
            if len(parts) < 3:
                continue
            ds, ver = parts[0], parts[1]
            seen_pairs.add((ds, ver))
        layers_checked[layer] = len(seen_pairs)
        for ds, ver in sorted(seen_pairs):
            slice_uri = join_uri(layer_uri, ds, ver)
            manifest_uri = join_uri(slice_uri, MANIFEST_FILENAME)
            ok = False
            err: str | None = None
            try:
                payload = store.read_json(manifest_uri)
                if isinstance(payload, dict) and required_keys.issubset(payload.keys()):
                    ok = True
                else:
                    err = "missing required keys"
            except (ClientError, BotoCoreError, FileNotFoundError, ValueError) as exc:
                err = str(exc)
            if not ok:
                incomplete.append(
                    {"layer": layer, "dataset_id": ds, "version": ver, "error": err}
                )

    status = "FAIL" if missing_layers else ("WARN" if incomplete else "PASS")
    return {
        "name": "manifest_completeness",
        "status": status,
        "layers_checked": layers_checked,
        "incomplete": incomplete,
    }


def _check_lake_tables() -> dict[str, Any]:
    """确认 registry DB 中 5 张 v1.4 数据湖元数据表已存在。"""
    from sqlalchemy import inspect

    from robot_dh.registry import get_engine, init_db
    from robot_dh.warehouse.models import ensure_lake_tables

    try:
        engine = get_engine()
        if engine.dialect.name == "sqlite":
            ensure_lake_tables(engine)
            init_db()
        inspector = inspect(engine)
        existing = set(inspector.get_table_names())
        missing = [t for t in WAREHOUSE_TABLES if t not in existing]
        if missing:
            return {
                "name": "lake_tables",
                "status": "FAIL",
                "missing": missing,
                "hint": (
                    "Apply the lake metadata migration on the cloud Postgres "
                    "(infra: ./scripts/21_pg_apply_lake_schema.sh) before retrying."
                ),
            }
        return {"name": "lake_tables", "status": "PASS", "tables": list(WAREHOUSE_TABLES)}
    except Exception as err:
        return {"name": "lake_tables", "status": "FAIL", "error": str(err)}


def audit_lake() -> dict[str, Any]:
    """执行全部数据湖检查，返回 status、lake_uri（s3:// 或 local）、checks 列表。"""
    lake_bucket = os.environ.get("ROBOT_DH_S3_LAKE_BUCKET", "robot-lake")
    endpoint = os.environ.get("ROBOT_DH_S3_ENDPOINT_URL")
    has_s3_creds = bool(endpoint and os.environ.get("ROBOT_DH_S3_ACCESS_KEY") and os.environ.get("ROBOT_DH_S3_SECRET_KEY"))

    checks: list[dict[str, Any]] = []

    if has_s3_creds:
        lake_root_uri = f"s3://{lake_bucket}/"
        store: Any = S3LakeStore.from_env()
        checks.append(_check_bucket_visible(store, lake_bucket))
    else:
        lake_root_uri = os.environ.get("ROBOT_DH_LOCAL_LAKE_ROOT", "runs/lake")
        store = create_lake_store(lake_root_uri)
        checks.append(
            {
                "name": "bucket",
                "status": "SKIP",
                "reason": "S3 env vars not configured; running local-lake audit",
                "lake_root_uri": lake_root_uri,
            }
        )

    for layer in LAKE_LAYERS:
        checks.append(_check_layer_prefix(store, lake_root_uri, layer))

    checks.append(_check_manifest_completeness(store, lake_root_uri))
    checks.append(_check_lake_tables())

    statuses = [c["status"] for c in checks if c["status"] != "SKIP"]
    overall = _status_worst(*statuses) if statuses else "PASS"

    return {
        "status": overall,
        "lake_uri": lake_root_uri,
        "checks": checks,
    }


def render_audit_human(payload: dict[str, Any]) -> str:
    lines = [f"lake audit: {payload['status']}  lake_uri={payload.get('lake_uri', '?')}"]
    for c in payload.get("checks", []):
        status = c.get("status", "?")
        name = c.get("name", "?")
        extra: list[str] = []
        for k in ("bucket", "uri", "object_count", "missing", "incomplete", "tables", "error", "reason", "hint", "layers_checked"):
            if k in c:
                v = c[k]
                if isinstance(v, list) and len(v) > 5:
                    extra.append(f"{k}=[{len(v)} items]")
                else:
                    extra.append(f"{k}={v}")
        lines.append(f"  [{status}] {name}  " + "  ".join(extra))
    return "\n".join(lines)
