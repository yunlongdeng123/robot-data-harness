from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest

from robot_dh.artifacts.s3 import S3ArtifactStore


def _configure_s3_env(monkeypatch) -> None:
    endpoint = os.environ.get("ROBOT_DH_TEST_S3_ENDPOINT_URL")
    access_key = os.environ.get("ROBOT_DH_TEST_S3_ACCESS_KEY")
    secret_key = os.environ.get("ROBOT_DH_TEST_S3_SECRET_KEY")
    if not endpoint or not access_key or not secret_key:
        pytest.skip("S3 integration test environment is not configured")
    monkeypatch.setenv("ROBOT_DH_S3_ENDPOINT_URL", endpoint)
    monkeypatch.setenv("ROBOT_DH_S3_ACCESS_KEY", access_key)
    monkeypatch.setenv("ROBOT_DH_S3_SECRET_KEY", secret_key)
    monkeypatch.setenv(
        "ROBOT_DH_S3_ARTIFACT_BUCKET",
        os.environ.get("ROBOT_DH_TEST_S3_ARTIFACT_BUCKET", "robot-dh-artifacts"),
    )
    monkeypatch.setenv("ROBOT_DH_S3_REGION", os.environ.get("ROBOT_DH_TEST_S3_REGION", "us-east-1"))


def test_s3_artifact_store_optional(monkeypatch, tmp_path: Path) -> None:
    _configure_s3_env(monkeypatch)
    store = S3ArtifactStore.from_env()
    source = tmp_path / "artifact.txt"
    source.write_text("remote artifact\n", encoding="utf-8")
    artifact_key = f"tests/{uuid4().hex}/artifact.txt"

    artifact_uri = store.put_file(source, artifact_key)

    assert artifact_uri.startswith("s3://")
    assert store.exists(artifact_uri)

    downloaded = store.get_file(artifact_uri, tmp_path / "downloaded.txt")
    assert downloaded.read_text(encoding="utf-8") == "remote artifact\n"