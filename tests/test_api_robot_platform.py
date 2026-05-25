"""v1.6 FastAPI：直接调用 endpoint 函数（避免 TestClient/httpx 依赖）。

DB 不可达 / 表缺失时应抛 HTTPException 503；表已建（SQLite + ensure_lake_tables）则返回空列表 / 404。
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import HTTPException

from robot_dh.api.main import (
    asset_profile_detail,
    asset_profiles,
    ml_ready_detail,
    ml_ready_list,
    qc_contract_detail,
    qc_contracts,
    qc_run_detail,
    qc_runs,
    workflow_detail,
    workflow_steps,
    workflows_list,
    workflows_submit_scale30,
    _WorkflowSubmitRequest,
)
from robot_dh.warehouse.robot_platform import PlatformWarehouse


def _setup_sqlite(monkeypatch, tmp_path):
    """SQLite + ensure_lake_tables 后所有 v1.6 表都存在，但内容空。"""
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    wh = PlatformWarehouse(soft=False)
    # 触发 _get_engine() -> ensure_lake_tables（SQLite 路径）
    wh._get_engine()


def test_qc_runs_returns_empty_when_no_data(monkeypatch, tmp_path) -> None:
    _setup_sqlite(monkeypatch, tmp_path)
    assert qc_runs() == []


def test_qc_run_detail_404(monkeypatch, tmp_path) -> None:
    _setup_sqlite(monkeypatch, tmp_path)
    with pytest.raises(HTTPException) as exc:
        qc_run_detail("missing")
    assert exc.value.status_code == 404


def test_qc_contracts_returns_empty(monkeypatch, tmp_path) -> None:
    _setup_sqlite(monkeypatch, tmp_path)
    assert qc_contracts() == []


def test_qc_contract_detail_404(monkeypatch, tmp_path) -> None:
    _setup_sqlite(monkeypatch, tmp_path)
    with pytest.raises(HTTPException) as exc:
        qc_contract_detail("missing")
    assert exc.value.status_code == 404


def test_ml_ready_endpoints(monkeypatch, tmp_path) -> None:
    _setup_sqlite(monkeypatch, tmp_path)
    assert ml_ready_list() == []
    with pytest.raises(HTTPException) as exc:
        ml_ready_detail("none", "v1")
    assert exc.value.status_code == 404


def test_workflows_endpoints(monkeypatch, tmp_path) -> None:
    _setup_sqlite(monkeypatch, tmp_path)
    assert workflows_list() == []
    with pytest.raises(HTTPException) as exc:
        workflow_detail("none")
    assert exc.value.status_code == 404
    assert workflow_steps("none") == []


def test_assets_profiles_endpoints(monkeypatch, tmp_path) -> None:
    _setup_sqlite(monkeypatch, tmp_path)
    assert asset_profiles() == []
    with pytest.raises(HTTPException) as exc:
        asset_profile_detail("none")
    assert exc.value.status_code == 404


def test_workflows_submit_returns_501() -> None:
    with pytest.raises(HTTPException) as exc:
        workflows_submit_scale30(_WorkflowSubmitRequest(workflow_name="x"))
    assert exc.value.status_code == 501
