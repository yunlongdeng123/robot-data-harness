"""plan / summary 的 JSON 序列化 + 本地/S3 透明读写。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from robot_dh.lake.store import create_lake_store
from robot_dh.lake.uri import is_s3_uri


def write_json_uri(uri: str, payload: Any) -> str:
    if is_s3_uri(uri):
        store = create_lake_store(uri)
        return store.write_json(uri, payload)
    path = Path(uri).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    return path.as_posix()


def read_json_uri(uri: str) -> Any:
    if is_s3_uri(uri):
        store = create_lake_store(uri)
        return store.read_json(uri)
    path = Path(uri).expanduser()
    return json.loads(path.read_text())


def list_local_or_s3(uri: str) -> list[str]:
    if is_s3_uri(uri):
        store = create_lake_store(uri)
        return store.list(uri)
    path = Path(uri).expanduser()
    if not path.exists():
        return []
    return [p.as_posix() for p in sorted(path.rglob("*")) if p.is_file()]
