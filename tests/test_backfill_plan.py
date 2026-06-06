"""v1.8 backfill plan 测试。"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from robot_dh.quality_ops import (
    BackfillPlanner,
    generate_backfill_plan,
    show_backfill_status,
)
from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    AdsQualityDashboardRow,
    BackfillPlanRow,
    BackfillTaskRow,
    FactEtlRunRow,
    ensure_lake_tables,
)


@pytest.fixture()
def sqlite_db(monkeypatch, tmp_path):
    db_path = tmp_path / "bf.db"
    uri = f"sqlite:///{db_path}"
    monkeypatch.setenv("ROBOT_DH_DB_URI", uri)
    resolved = resolve_db_uri(uri)
    engine = get_engine(resolved)
    init_db(resolved)
    ensure_lake_tables(engine)
    yield engine


def test_plan_from_failed_fact_etl_runs(sqlite_db, tmp_path: Path) -> None:
    engine = sqlite_db
    dt1 = date(2026, 5, 25)
    with Session(engine, expire_on_commit=False, future=True) as session:
        session.add_all([
            FactEtlRunRow(
                run_key="k1", job_id="j1", run_id="r1",
                dataset_id="droid_lerobot_dev1g", version="v1", phase="normalize",
                status="FAILED", started_at=datetime.combine(dt1, datetime.min.time(), tzinfo=timezone.utc),
                finished_at=datetime.combine(dt1, datetime.min.time(), tzinfo=timezone.utc),
                dt=dt1, duration_sec=10.0, dataset_family="droid",
            ),
            FactEtlRunRow(
                run_key="k2", job_id="j2", run_id="r2",
                dataset_id="droid_lerobot_dev1g", version="v1", phase="normalize",
                status="OK", started_at=datetime.combine(dt1, datetime.min.time(), tzinfo=timezone.utc),
                dt=dt1, duration_sec=10.0,
            ),
        ])
        session.commit()

    plan = generate_backfill_plan(
        from_date="2026-05-25", to_date="2026-05-25",
        dataset_id="droid_lerobot_dev1g",
        phase="normalize",
        reason="test failed runs",
    )
    assert plan.status == "PERSISTED"
    assert len(plan.tasks) == 1
    t = plan.tasks[0]
    assert t.dataset_id == "droid_lerobot_dev1g"
    assert t.dt == "2026-05-25"
    assert "robot-dh etl run" in t.recommended_command
    assert "--phase normalize" in t.recommended_command
    assert "--resume" in t.recommended_command

    # 写库结果
    with Session(engine, expire_on_commit=False, future=True) as session:
        plans = session.query(BackfillPlanRow).all()
        assert len(plans) == 1
        tasks = session.query(BackfillTaskRow).all()
        assert len(tasks) == 1
        assert tasks[0].status == "PLANNED"


def test_plan_dry_run_does_not_persist(sqlite_db) -> None:
    plan = generate_backfill_plan(
        from_date="2026-05-25", to_date="2026-05-25",
        dataset_id="dataset_X", version="v1",
        phase="normalize",
        dry_run=True,
    )
    assert plan.dry_run is True
    assert any("dry_run" in w for w in plan.warnings)
    # 显式 dataset 时即使没历史失败也应生成 task（人工补数）
    assert len(plan.tasks) >= 1


def test_plan_outputs_json_and_md(sqlite_db, tmp_path: Path) -> None:
    out = tmp_path / "bf-out"
    plan = generate_backfill_plan(
        from_date="2026-05-25", to_date="2026-05-25",
        dataset_id="dataset_X", version="v1",
        phase="normalize",
        output_dir=out,
        dry_run=True,
    )
    assert plan.plan_path is not None and plan.plan_path.exists()
    assert plan.plan_md_path is not None and plan.plan_md_path.exists()
    payload = json.loads(plan.plan_path.read_text(encoding="utf-8"))
    assert payload["plan_id"] == plan.plan_id
    md = plan.plan_md_path.read_text(encoding="utf-8")
    assert "Backfill Plan" in md
    assert plan.plan_id in md


def test_recommend_command_routes_by_phase() -> None:
    # qc / ml_ready / sla 都有专属推荐命令
    qc_cmd = BackfillPlanner._recommend_command(dataset_id="a", version="v1", phase="qc")
    assert "robot-dh qc contract run" in qc_cmd
    mlr_cmd = BackfillPlanner._recommend_command(dataset_id="a", version="v1", phase="ml_ready")
    assert "robot-dh ml-ready export" in mlr_cmd
    sla_cmd = BackfillPlanner._recommend_command(dataset_id="a", version="v1", phase="sla")
    assert "robot-dh sla check" in sla_cmd


def test_status_returns_buckets(sqlite_db) -> None:
    plan = generate_backfill_plan(
        from_date="2026-05-25", to_date="2026-05-25",
        dataset_id="dataset_X", version="v1",
        phase="normalize",
    )
    status = show_backfill_status(plan_id=plan.plan_id)
    assert status["plan_id"] == plan.plan_id
    assert status["task_count"] >= 1
    assert "PLANNED" in status["status_buckets"]
