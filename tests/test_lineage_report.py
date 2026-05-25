"""lineage report：基本字段 + write 落盘。"""

from __future__ import annotations

import json
from pathlib import Path

from robot_dh.lineage import build_lineage_report, write_lineage_report
from robot_dh.warehouse.robot_platform import PlatformWarehouse


def test_lineage_report_empty_db(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    # 触发建表
    PlatformWarehouse(soft=False)._get_engine()

    report = build_lineage_report(workflow_name="wf-test", workflow_namespace="robot-dh")
    assert report.workflow_name == "wf-test"
    assert report.workflow_status is None
    assert report.steps_total == 0
    out = tmp_path / "out" / "lineage_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    uri = write_lineage_report(report, out.as_posix())
    assert Path(uri).is_file()
    payload = json.loads(out.read_text())
    assert payload["workflow_name"] == "wf-test"
    assert "steps_total" in payload


def test_lineage_report_picks_up_workflow_run(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    wh = PlatformWarehouse(soft=False)
    wh.upsert_workflow_run(
        workflow_name="wf-pop",
        workflow_namespace="robot-dh",
        status="Succeeded",
    )
    wh.upsert_workflow_step(
        workflow_name="wf-pop",
        step_name="qc-run",
        workflow_namespace="robot-dh",
        phase="Succeeded",
    )

    report = build_lineage_report(workflow_name="wf-pop", workflow_namespace="robot-dh", warehouse=wh)
    assert report.workflow_status == "Succeeded"
    assert report.steps_total >= 1
