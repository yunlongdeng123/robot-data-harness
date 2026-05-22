from __future__ import annotations

import os
from pathlib import Path

from robot_dh.artifacts.base import ArtifactStore
from robot_dh.artifacts.local import LocalArtifactStore
from robot_dh.artifacts.s3 import S3ArtifactStore


def resolve_artifact_store_type(store_type: str | None = None) -> str:
    resolved = (store_type or os.environ.get("ROBOT_DH_ARTIFACT_STORE") or "local").strip().lower()
    if resolved not in {"local", "s3"}:
        raise ValueError(f"Unsupported artifact store: {resolved}")
    return resolved


def create_artifact_store(
    *,
    output_dir: Path,
    store_type: str | None = None,
) -> ArtifactStore:
    resolved = resolve_artifact_store_type(store_type)
    if resolved == "local":
        return LocalArtifactStore(output_dir)
    return S3ArtifactStore.from_env()