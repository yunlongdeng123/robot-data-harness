from __future__ import annotations

import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from robot_dh.registry import RegistryService, init_db


def test_registry_postgres_optional() -> None:
    db_uri = os.environ.get("ROBOT_DH_TEST_POSTGRES_URI")
    if not db_uri:
        pytest.skip("ROBOT_DH_TEST_POSTGRES_URI is not configured")

    init_db(db_uri)
    registry = RegistryService(db_uri=db_uri)
    suffix = uuid4().hex[:8]
    dataset_id = f"pg-dataset-{suffix}"
    run_id = f"pg-run-{suffix}"
    now = datetime.now(timezone.utc).isoformat()

    dataset = registry.upsert_dataset(
        dataset_id=dataset_id,
        version="v1",
        task_type="quality",
        robot_type="arm",
        pose_format="eexyzxyzw",
        storage_uri=f"s3://robot-datasets/{dataset_id}/v1",
        local_path=None,
        num_samples=128,
        duration_sec=6.4,
        fps=20.0,
    )
    run_db_id = registry.create_run(
        run_id=run_id,
        dataset_id=dataset.dataset_id,
        dataset_version=dataset.version,
        status="PASS",
        started_at=now,
        finished_at=now,
        duration_sec=0.2,
        config_path=None,
        output_dir="/tmp/output",
        report_json_path=None,
        report_html_path=None,
        metrics={"max_velocity_mps": 0.42},
        errors=[],
        warnings=[],
    )

    stored = registry.get_run(run_id)
    assert stored is not None
    assert stored.id == run_db_id
    assert stored.dataset_id == dataset_id