"""`robot-dh lake {init,list,audit,manifest}` CLI 实现；参数解析在 robot_dh.cli，此处为业务逻辑。"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from robot_dh.lake.audit import audit_lake, render_audit_human
from robot_dh.lake.manifest import MANIFEST_FILENAME, read_manifest
from robot_dh.lake.store import S3LakeStore, create_lake_store
from robot_dh.lake.uri import is_s3_uri, join_uri, parse_uri

LAKE_LAYERS_DEFAULT = ("raw", "ods", "dwd", "ads", "lineage", "tmp")


def _default_lake_root_uri() -> str:
    bucket = os.environ.get("ROBOT_DH_S3_LAKE_BUCKET")
    has_s3 = (
        bool(bucket)
        and bool(os.environ.get("ROBOT_DH_S3_ENDPOINT_URL"))
        and bool(os.environ.get("ROBOT_DH_S3_ACCESS_KEY"))
        and bool(os.environ.get("ROBOT_DH_S3_SECRET_KEY"))
    )
    if has_s3:
        return f"s3://{bucket}/"
    return os.environ.get("ROBOT_DH_LOCAL_LAKE_ROOT", "runs/lake")


def lake_init(*, output: str = "human") -> dict[str, Any]:
    """探测 v1.4 所需的 bucket/prefix/postgres 表（只读，不创建资源）。"""
    payload = audit_lake()
    payload["action"] = "init"
    return payload


def lake_list(
    *,
    layer: str | None,
    lake_root_uri: str | None = None,
) -> dict[str, Any]:
    """枚举数据湖资产；layer 为 None 时列出全部标准分层。"""
    root = lake_root_uri or _default_lake_root_uri()
    layers = [layer] if layer else list(LAKE_LAYERS_DEFAULT)
    store = create_lake_store(root)
    results: list[dict[str, Any]] = []

    for ly in layers:
        layer_uri = join_uri(root, ly)
        items: list[str] = []
        slices: list[dict[str, Any]] = []
        try:
            items = store.list(layer_uri)
        except Exception as err:  # noqa: BLE001
            results.append(
                {"layer": ly, "uri": layer_uri, "error": str(err), "object_count": 0, "slices": []}
            )
            continue

        # ods/dwd 按 (dataset_id, version) 聚合 slice
        if ly in ("ods", "dwd"):
            seen: set[tuple[str, str]] = set()
            for obj in items:
                if is_s3_uri(obj):
                    key = parse_uri(obj).key
                else:
                    key = obj
                if is_s3_uri(layer_uri):
                    layer_key = parse_uri(layer_uri).key.rstrip("/") + "/"
                else:
                    layer_key = layer_uri.rstrip("/") + "/"
                if not key.startswith(layer_key):
                    continue
                rel = key[len(layer_key):]
                parts = rel.split("/")
                if len(parts) < 2:
                    continue
                seen.add((parts[0], parts[1]))
            for ds, ver in sorted(seen):
                slice_uri = join_uri(layer_uri, ds, ver)
                slices.append(
                    {
                        "dataset_id": ds,
                        "version": ver,
                        "uri": slice_uri,
                        "manifest_uri": join_uri(slice_uri, MANIFEST_FILENAME),
                    }
                )

        results.append(
            {
                "layer": ly,
                "uri": layer_uri,
                "object_count": len(items),
                "slices": slices,
            }
        )

    return {"lake_uri": root, "layers": results}


def lake_audit(*, output: str = "human") -> dict[str, Any]:
    return audit_lake()


def lake_manifest(*, uri: str) -> dict[str, Any]:
    """读取给定 layer URI（或直接 manifest 文件 URI）下的 _manifest.json。"""
    store = create_lake_store(uri)
    return read_manifest(store, uri)


def render_list_human(payload: dict[str, Any]) -> str:
    lines = [f"lake list: {payload.get('lake_uri', '?')}"]
    for layer in payload.get("layers", []):
        lines.append(
            f"  layer={layer['layer']:<8}  uri={layer['uri']:<60}  objects={layer.get('object_count', 0)}"
        )
        for s in layer.get("slices", []):
            lines.append(f"    - {s['dataset_id']}/{s['version']}  {s['uri']}")
        if "error" in layer:
            lines.append(f"    ERROR: {layer['error']}")
    return "\n".join(lines)


def render_init_human(payload: dict[str, Any]) -> str:
    return "lake init (probe-only, does not create resources)\n" + render_audit_human(payload)
