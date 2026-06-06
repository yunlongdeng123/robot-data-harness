"""v1.8 build_quality_summary 测试。"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from robot_dh.quality_ops import build_quality_summary
from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    AdsQualityDashboardRow,
    AdsWorkflowOpsDashboardRow,
    DwsRuleFailureDailyRow,
    DwsDatasetQualityDailyRow,
    FactWorkflowStepRow,
    ensure_lake_tables,
)


@pytest.fixture()
def sqlite_db(monkeypatch, tmp_path):
    db_path = tmp_path / "qs.db"
    uri = f"sqlite:///{db_path}"
    monkeypatch.setenv("ROBOT_DH_DB_URI", uri)
    resolved = resolve_db_uri(uri)
    engine = get_engine(resolved)
    init_db(resolved)
    ensure_lake_tables(engine)
    yield engine


def test_empty_db_summary_returns_zeros(sqlite_db) -> None:
    summary = build_quality_summary(date_="2026-05-25")
    d = summary.to_dict()
    assert d["dt"] == "2026-05-25"
    assert d["dataset_count"] == 0
    assert d["alert_level"] == "OK"
    assert d["top_failed_rules"] == []


def test_summary_with_ads_and_top_rules(sqlite_db) -> None:
    engine = sqlite_db
    target = date(2026, 5, 25)
    with Session(engine, expire_on_commit=False, future=True) as session:
        session.add_all([
            AdsQualityDashboardRow(
                dt=target, dataset_id="a", version="v1", dataset_family="droid",
                overall_status="PASS", quality_score=92.5,
                qc_pass_rate=0.96, etl_success_rate=0.98, workflow_success_rate=0.95,
                top_failed_rule=None, top_failed_rule_count=None,
                p95_duration_sec=10.0, ml_ready_rows=100, raw_bytes=1000, dwd_bytes=500,
                alert_level="OK", alert_reason=None,
            ),
            AdsQualityDashboardRow(
                dt=target, dataset_id="b", version="v1", dataset_family="lerobot",
                overall_status="WARN", quality_score=88.0,
                qc_pass_rate=0.92, etl_success_rate=0.95, workflow_success_rate=0.90,
                top_failed_rule="row_count_min", top_failed_rule_count=2,
                p95_duration_sec=20.0, ml_ready_rows=80, raw_bytes=2000, dwd_bytes=1000,
                alert_level="WARN", alert_reason="qc_pass_rate<0.95",
            ),
            DwsRuleFailureDailyRow(
                dt=target, dataset_family="droid", contract_id="droid_v1",
                rule_id="row_count_min", severity="warn",
                run_count=10, pass_count=8, warn_count=2, fail_count=2, fail_rate=0.2,
            ),
            DwsRuleFailureDailyRow(
                dt=target, dataset_family="lerobot", contract_id="lerobot_v1",
                rule_id="schema_match", severity="fail",
                run_count=5, pass_count=4, warn_count=0, fail_count=1, fail_rate=0.2,
            ),
            DwsDatasetQualityDailyRow(
                dt=target, dataset_id="a", version="v1", dataset_family="droid",
                qc_pass_rate=0.96, etl_success_rate=0.98, stale_heartbeat_count=2,
            ),
            FactWorkflowStepRow(
                step_key="k1", workflow_name="wf-1", step_name="s",
                phase="Succeeded", dt=target, duration_sec=5.0,
                archive_log_uri="s3://logs/wf-1/main.log",
            ),
            FactWorkflowStepRow(
                step_key="k2", workflow_name="wf-1", step_name="s2",
                phase="Succeeded", dt=target, duration_sec=15.0,
                archive_log_uri="s3://logs/wf-1/step2.log",
            ),
            AdsWorkflowOpsDashboardRow(
                dt=target, workflow_type="normalize",
                workflow_count=10, success_count=9, failed_count=1,
                success_rate=0.9, avg_duration_sec=12.0, p95_duration_sec=20.0,
                alert_level="WARN", alert_reason="success_rate<0.95",
            ),
        ])
        session.commit()

    summary = build_quality_summary(date_="2026-05-25")
    assert summary.dataset_count == 2
    assert summary.qc_pass_rate is not None and 0.93 <= summary.qc_pass_rate <= 0.97
    assert summary.alert_level in ("WARN", "OK")
    assert summary.top_failed_rules
    # top_failed_rules 应该按 fail_count DESC 排序
    rule_ids = [r.rule_id for r in summary.top_failed_rules]
    assert "row_count_min" in rule_ids
    assert summary.stale_heartbeat_count == 2
    assert summary.p95_step_duration_sec is not None
    assert "s3://logs/wf-1/main.log" in summary.archive_log_uris
    assert summary.workflow_ops and summary.workflow_ops[0]["workflow_type"] == "normalize"


def test_summary_alert_level_promotion(sqlite_db) -> None:
    engine = sqlite_db
    target = date(2026, 5, 25)
    with Session(engine, expire_on_commit=False, future=True) as session:
        session.add(
            AdsQualityDashboardRow(
                dt=target, dataset_id="a", version="v1", dataset_family="droid",
                overall_status="FAIL", quality_score=50.0,
                qc_pass_rate=0.5, etl_success_rate=0.6, workflow_success_rate=0.8,
                alert_level="CRITICAL", alert_reason="qc_pass_rate<0.8",
            )
        )
        session.commit()
    summary = build_quality_summary(date_="2026-05-25")
    assert summary.alert_level == "CRITICAL"
