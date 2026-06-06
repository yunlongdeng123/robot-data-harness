"""v1.8 SLA 测试：load_sla_policies + perform_sla_checks。"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from robot_dh.quality_ops import (
    SlaPolicy,
    SlaPolicyDoc,
    load_sla_policies,
    perform_sla_checks,
    render_sla_report,
)
from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    AdsQualityDashboardRow,
    DimDatasetRow,
    MlReadyDatasetRow,
    QcContractRunRow,
    SlaCheckRow,
    SlaPolicyRow,
    ensure_lake_tables,
)


@pytest.fixture()
def sqlite_db(monkeypatch, tmp_path):
    db_path = tmp_path / "sla.db"
    uri = f"sqlite:///{db_path}"
    monkeypatch.setenv("ROBOT_DH_DB_URI", uri)
    resolved = resolve_db_uri(uri)
    engine = get_engine(resolved)
    init_db(resolved)
    ensure_lake_tables(engine)
    yield engine


def test_load_sla_policies_from_default_yaml() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    doc = load_sla_policies(repo_root / "configs" / "sla_policies.yaml")
    assert len(doc.policies) >= 2
    ids = {p.policy_id for p in doc.policies}
    assert "devscale_daily_ready" in ids


def _seed_dataset(session: Session, *, dataset_id: str, version: str, family: str | None) -> None:
    session.add(
        DimDatasetRow(
            dataset_key=f"dataset:{dataset_id}:{version}",
            dataset_id=dataset_id, version=version, dataset_family=family,
        )
    )


def test_pass_when_metrics_above_threshold(sqlite_db) -> None:
    engine = sqlite_db
    dt = date(2026, 5, 25)
    started = datetime.combine(dt, datetime.min.time(), tzinfo=timezone.utc)
    with Session(engine, expire_on_commit=False, future=True) as session:
        _seed_dataset(session, dataset_id="droid_lerobot_dev1g", version="v1", family="droid")
        session.add_all([
            AdsQualityDashboardRow(
                dt=dt, dataset_id="droid_lerobot_dev1g", version="v1", dataset_family="droid",
                qc_pass_rate=0.95, etl_success_rate=0.95, workflow_success_rate=0.95,
                raw_bytes=1000, dwd_bytes=500, ml_ready_rows=100,
                alert_level="OK",
            ),
            QcContractRunRow(
                run_id="qrun", contract_id="droid_v1",
                dataset_id="droid_lerobot_dev1g", version="v1", dataset_family="droid",
                status="PASS", started_at=started, finished_at=started,
            ),
            MlReadyDatasetRow(
                dataset_id="droid_lerobot_dev1g", version="v1", dataset_family="droid",
                output_uri="file:///mlr/droid", num_train=80, num_val=10, num_test=10,
                status="OK", created_at=started,
            ),
        ])
        session.commit()

    policies = SlaPolicyDoc(policies=[
        SlaPolicy(
            policy_id="devscale_daily_ready",
            policy_name="Devscale Daily Ready",
            dataset_pattern="*dev*",
            dataset_family=None,
            deadline_hour=23,
            required_outputs=["qc_contract", "dwd", "ads", "ml_ready"],
            min_qc_pass_rate=0.8, min_etl_success_rate=0.8,
            max_failed_workflows=0,
        ),
    ])
    checks = perform_sla_checks(policies=policies, date_="2026-05-25")
    pass_checks = [c for c in checks if c.dataset_id == "droid_lerobot_dev1g"]
    assert pass_checks
    assert pass_checks[0].status == "PASS"
    assert not pass_checks[0].missing_outputs


def test_fail_when_below_threshold(sqlite_db) -> None:
    engine = sqlite_db
    dt = date(2026, 5, 25)
    with Session(engine, expire_on_commit=False, future=True) as session:
        _seed_dataset(session, dataset_id="droid_lerobot_dev1g", version="v1", family="droid")
        session.add(
            AdsQualityDashboardRow(
                dt=dt, dataset_id="droid_lerobot_dev1g", version="v1", dataset_family="droid",
                qc_pass_rate=0.5, etl_success_rate=0.6, workflow_success_rate=0.7,
                alert_level="CRITICAL",
            )
        )
        session.commit()

    policies = SlaPolicyDoc(policies=[
        SlaPolicy(
            policy_id="devscale_daily_ready", policy_name="x",
            dataset_pattern="*dev*", dataset_family=None,
            deadline_hour=23,
            required_outputs=["qc_contract", "ads"],
            min_qc_pass_rate=0.8, min_etl_success_rate=0.8,
            max_failed_workflows=0,
        ),
    ])
    checks = perform_sla_checks(policies=policies, date_="2026-05-25")
    matched = [c for c in checks if c.dataset_id == "droid_lerobot_dev1g"]
    assert matched
    assert matched[0].status == "FAIL"
    assert any("qc_pass_rate" in r for r in [matched[0].failed_reason or ""])
    # qc_contract / ml_ready 缺失 → missing_outputs 包含
    assert "qc_contract" in matched[0].missing_outputs


def test_warn_when_required_output_present_but_metric_null(sqlite_db) -> None:
    engine = sqlite_db
    dt = date(2026, 5, 25)
    started = datetime.combine(dt, datetime.min.time(), tzinfo=timezone.utc)
    with Session(engine, expire_on_commit=False, future=True) as session:
        _seed_dataset(session, dataset_id="dataset_dev_warn", version="v1", family=None)
        session.add(
            QcContractRunRow(
                run_id="qrun-w", contract_id="c",
                dataset_id="dataset_dev_warn", version="v1",
                status="WARN", started_at=started, finished_at=started,
            )
        )
        session.commit()

    policies = SlaPolicyDoc(policies=[
        SlaPolicy(
            policy_id="warn_policy", policy_name="warn-only",
            dataset_pattern="*dev*", dataset_family=None,
            deadline_hour=23,
            required_outputs=["qc_contract"],
            min_qc_pass_rate=0.5, min_etl_success_rate=None,
            max_failed_workflows=None,
        ),
    ])
    checks = perform_sla_checks(policies=policies, date_="2026-05-25")
    matched = [c for c in checks if c.dataset_id == "dataset_dev_warn"]
    assert matched
    assert matched[0].status == "WARN"


def test_sla_check_persists_policies_and_checks(sqlite_db) -> None:
    engine = sqlite_db
    dt = date(2026, 5, 25)
    with Session(engine, expire_on_commit=False, future=True) as session:
        _seed_dataset(session, dataset_id="a_dev", version="v1", family=None)
        session.add(
            AdsQualityDashboardRow(
                dt=dt, dataset_id="a_dev", version="v1",
                qc_pass_rate=0.95, etl_success_rate=0.95,
            )
        )
        session.commit()

    policies = SlaPolicyDoc(policies=[
        SlaPolicy(
            policy_id="persist_test", policy_name="persist",
            dataset_pattern="*dev*", dataset_family=None,
            deadline_hour=23, required_outputs=["ads"],
            min_qc_pass_rate=0.8, min_etl_success_rate=0.8,
            max_failed_workflows=None,
        ),
    ])
    perform_sla_checks(policies=policies, date_="2026-05-25", persist=True)
    with Session(engine, expire_on_commit=False, future=True) as session:
        ps = session.query(SlaPolicyRow).all()
        cs = session.query(SlaCheckRow).all()
        assert len(ps) == 1
        assert len(cs) == 1


def test_render_sla_report(sqlite_db, tmp_path: Path) -> None:
    policies = SlaPolicyDoc(policies=[])
    checks = perform_sla_checks(policies=policies, date_="2026-05-25")
    artifacts = render_sla_report(checks=checks, output_dir=tmp_path / "sla-report", date_="2026-05-25")
    assert Path(artifacts.sla_report_html).exists()
    assert Path(artifacts.sla_report_json).exists()
    assert Path(artifacts.sla_failed_datasets).exists()
