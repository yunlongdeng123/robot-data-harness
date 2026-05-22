from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest

from robot_dh.artifacts.s3 import S3ArtifactStore
from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.pipeline import run_validation
from robot_dh.registry import RegistryService


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


def _write_config(path: Path, expected_presses: int) -> Path:
    path.write_text(
        f"""
dataset:
  min_samples: 10
validators:
  press_event:
    press_expected_min_count: {max(expected_presses - 2, 1)}
    press_expected_max_count: {expected_presses + 2}
    press_expected_count: {expected_presses}
""".strip(),
        encoding="utf-8",
    )
    return path


def test_validate_with_s3_optional(monkeypatch, tmp_path: Path) -> None:
    _configure_s3_env(monkeypatch)
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path / 'robot_dh.db'}")
    dataset_dir = generate_demo_dataset(
        output_dir=tmp_path / "samples" / "button_press_001",
        duration_sec=12.0,
        fps=12,
        num_buttons=5,
        num_presses=8,
    )
    run_id = f"s3-validate-{uuid4().hex[:8]}"
    report = run_validation(
        dataset_path=dataset_dir,
        config_path=_write_config(tmp_path / "config.yaml", expected_presses=8),
        output_dir=tmp_path / "runs" / run_id,
        run_id=run_id,
        record_to_registry=True,
        artifact_store_type="s3",
        artifact_prefix="runs/{run_id}",
    )

    assert report.artifacts["artifact_store"] == "s3"
    assert report.artifacts["report_json_uri"].startswith("s3://")
    assert report.artifacts["report_html_uri"].startswith("s3://")
    assert all(uri.startswith("s3://") for uri in report.artifacts["plots"].values())

    store = S3ArtifactStore.from_env()
    assert store.exists(report.artifacts["report_json_uri"])

    registry = RegistryService()
    detail = registry.get_run_detail(run_id)
    assert detail is not None
    assert any(artifact.artifact_uri.startswith("s3://") for artifact in detail.artifacts)