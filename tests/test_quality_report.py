"""v1.8 quality report 测试：HTML / JSON / CSV 都能生成（空数据也行）。"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from robot_dh.quality_ops import (
    QualityReportRenderer,
    build_quality_summary,
    render_quality_report,
)
from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    AdsQualityDashboardRow,
    DwsRuleFailureDailyRow,
    ensure_lake_tables,
)


@pytest.fixture()
def sqlite_db(monkeypatch, tmp_path):
    db_path = tmp_path / "qr.db"
    uri = f"sqlite:///{db_path}"
    monkeypatch.setenv("ROBOT_DH_DB_URI", uri)
    resolved = resolve_db_uri(uri)
    engine = get_engine(resolved)
    init_db(resolved)
    ensure_lake_tables(engine)
    yield engine


def test_render_empty_report(sqlite_db, tmp_path: Path) -> None:
    out = tmp_path / "report"
    artifacts = render_quality_report(date_="2026-05-25", output_dir=out)
    assert Path(artifacts.summary_json).exists()
    assert Path(artifacts.summary_html).exists()
    assert Path(artifacts.rule_failure_top10).exists()
    assert Path(artifacts.workflow_sla_summary).exists()
    assert Path(artifacts.abnormal_partitions).exists()
    assert Path(artifacts.archive_log_index).exists()
    # 必须是合法 JSON
    data = json.loads(Path(artifacts.summary_json).read_text(encoding="utf-8"))
    assert data["dt"] == "2026-05-25"
    assert data["dataset_count"] == 0


def test_render_with_data(sqlite_db, tmp_path: Path) -> None:
    engine = sqlite_db
    target = date(2026, 5, 25)
    with Session(engine, expire_on_commit=False, future=True) as session:
        session.add_all([
            AdsQualityDashboardRow(
                dt=target, dataset_id="d", version="v1", dataset_family="droid",
                overall_status="WARN", quality_score=80.0,
                qc_pass_rate=0.9, etl_success_rate=0.95, workflow_success_rate=0.9,
                alert_level="WARN", alert_reason="qc_pass_rate<0.95",
            ),
            DwsRuleFailureDailyRow(
                dt=target, dataset_family="droid", contract_id="c",
                rule_id="r1", severity="warn",
                run_count=5, pass_count=4, warn_count=1, fail_count=1, fail_rate=0.2,
            ),
        ])
        session.commit()

    out = tmp_path / "report-with-data"
    artifacts = render_quality_report(date_="2026-05-25", output_dir=out)
    html = Path(artifacts.summary_html).read_text(encoding="utf-8")
    assert "robot-dh v1.8 Quality Summary" in html
    assert "droid" in html or "d" in html

    csv_text = Path(artifacts.rule_failure_top10).read_text(encoding="utf-8")
    assert "rule_id" in csv_text.splitlines()[0]
    assert "r1" in csv_text


def test_renderer_format_helpers() -> None:
    r = QualityReportRenderer()
    assert r._fmt_rate(None) == "-"
    assert r._fmt_rate(0.85).endswith("%")
    assert r._fmt_seconds(0.5).endswith(" ms")
    assert r._fmt_seconds(45).endswith(" s")
    assert r._fmt_seconds(120).endswith(" min")
    assert r._fmt_bytes(0) == "-"
    assert r._fmt_bytes(2048).endswith("KiB") or r._fmt_bytes(2048).endswith("KB")
    assert r._fmt_int(1234567).count(",") >= 1
