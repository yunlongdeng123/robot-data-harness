"""qc_contract_runs 落库行为守门（v1.8 修复）。

覆盖 4 个真 bug 的修复点：

1. ``started_at`` 兜底：CLI 没传时按 ``finished_at - duration_sec`` 反推。
2. ``failed_rules_json`` / ``warning_rules_json`` 落库必须是 JSON array，不是 ``{"items":[...]}``。
3. ``metrics_json._rule_results`` 必须携带全量规则（含 PASS）。
4. reader（``_contract_run_to_dict``）兼容历史 ``{"items":[...]}`` 旧库行。

测试走本地 SQLite，避免依赖远端 PG。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from robot_dh.warehouse.robot_platform import (
    PlatformWarehouse,
    _normalize_rules_array,
)


@pytest.fixture
def wh(tmp_path):
    db_uri = f"sqlite:///{tmp_path / 'platform.db'}"
    svc = PlatformWarehouse(soft=False, db_uri=db_uri)
    # 建表
    from robot_dh.warehouse.models import ensure_lake_tables
    from robot_dh.registry import get_engine

    ensure_lake_tables(get_engine(db_uri))
    return svc


def _make_rule(rule_id: str, status: str, severity: str = "warn") -> dict:
    return {
        "rule_id": rule_id,
        "severity": severity,
        "status": status,
        "metric": f"m_{rule_id}",
        "op": ">=",
        "threshold": 0.9,
        "actual": 0.95 if status == "PASS" else 0.4,
    }


def test_started_at_default_when_caller_omits(wh: PlatformWarehouse) -> None:
    """CLI 不传 started_at 时，service 必须用 finished_at - duration_sec 反推。"""
    finished = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc)
    duration = 30.0

    row_id = wh.record_qc_contract_run(
        run_id="run-1",
        contract_id="universal_v1",
        status="PASS",
        dataset_id="demo",
        version="v1",
        dataset_family="universal",
        finished_at=finished,
        duration_sec=duration,
    )
    assert row_id is not None

    rec = wh.get_qc_contract_run("run-1")
    assert rec is not None
    started_at = rec["started_at"]
    assert started_at is not None, "started_at 不能落 NULL，会被 fact 表 WHERE 过滤掉"
    parsed = datetime.fromisoformat(started_at)
    # SQLite 的 DateTime(timezone=True) 不真存 tz，统一按 UTC naive 比较
    expected = (finished - timedelta(seconds=duration)).replace(tzinfo=None)
    actual = parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
    assert actual == expected


def test_failed_and_warning_rules_persist_as_json_array(wh: PlatformWarehouse) -> None:
    """historical bug：写成 {"items":[...]} 会让 PG jsonb_array_elements 直接抛异常。"""
    failed = [_make_rule("r-fail", "FAIL", "fail")]
    warning = [_make_rule("r-warn", "WARN", "warn")]

    wh.record_qc_contract_run(
        run_id="run-2",
        contract_id="universal_v1",
        status="FAIL",
        dataset_id="demo",
        version="v1",
        duration_sec=1.5,
        failed_rules=failed,
        warning_rules=warning,
    )

    # 直接读 ORM 列内容（绕开 _normalize_rules_array），断言落库是 list 而非 dict。
    from robot_dh.warehouse.models import QcContractRunRow
    from sqlalchemy import select

    with wh._session() as session:  # type: ignore[attr-defined]
        row = session.scalar(select(QcContractRunRow).where(QcContractRunRow.run_id == "run-2"))
        assert isinstance(row.failed_rules_json, list), \
            f"failed_rules_json must be array, got {type(row.failed_rules_json).__name__}"
        assert isinstance(row.warning_rules_json, list)
        assert row.failed_rules_json == failed
        assert row.warning_rules_json == warning


def test_all_rules_embed_into_metrics_json_with_pass(wh: PlatformWarehouse) -> None:
    """全量规则（含 PASS）必须通过 metrics_json._rule_results 携带，否则 pass_rate 永远算不出。"""
    all_rules = [
        _make_rule("r-pass-1", "PASS"),
        _make_rule("r-pass-2", "PASS"),
        _make_rule("r-warn",   "WARN"),
        _make_rule("r-fail",   "FAIL", "fail"),
    ]

    wh.record_qc_contract_run(
        run_id="run-3",
        contract_id="universal_v1",
        status="WARN",
        dataset_id="demo",
        version="v1",
        duration_sec=2.0,
        metrics={"row_count": 100, "null_rate": 0.0},
        failed_rules=[r for r in all_rules if r["status"] == "FAIL"],
        warning_rules=[r for r in all_rules if r["status"] == "WARN"],
        all_rules=all_rules,
    )

    rec = wh.get_qc_contract_run("run-3")
    assert rec is not None
    metrics = rec["metrics_json"] or {}
    embedded = metrics.get("_rule_results")
    assert isinstance(embedded, list)
    statuses = sorted(r["status"] for r in embedded)
    assert statuses == ["FAIL", "PASS", "PASS", "WARN"]
    # 原 metrics 字段必须保留，不能被 _rule_results 覆盖
    assert metrics.get("row_count") == 100


def test_all_rules_falls_back_when_caller_only_passes_failed(wh: PlatformWarehouse) -> None:
    """旧 CLI 只传 failed/warning 时也要把它们塞进 _rule_results，避免下游 DML 拿 [] 算出 0 行。"""
    failed = [_make_rule("r-fail", "FAIL", "fail")]

    wh.record_qc_contract_run(
        run_id="run-4",
        contract_id="universal_v1",
        status="FAIL",
        dataset_id="demo",
        version="v1",
        duration_sec=0.5,
        failed_rules=failed,
    )

    rec = wh.get_qc_contract_run("run-4")
    embedded = (rec["metrics_json"] or {}).get("_rule_results", [])
    assert any(r["rule_id"] == "r-fail" for r in embedded)


def test_normalize_rules_array_handles_legacy_items_dict() -> None:
    """老库行用 {"items":[...]} 包过一层；reader 必须把它降回 array。"""
    legacy = {"items": [{"rule_id": "r1", "status": "FAIL"}]}
    out = _normalize_rules_array(legacy)
    assert out == [{"rule_id": "r1", "status": "FAIL"}]
    assert _normalize_rules_array(None) is None
    assert _normalize_rules_array([{"x": 1}]) == [{"x": 1}]
    assert _normalize_rules_array({"unrelated": True}) is None
