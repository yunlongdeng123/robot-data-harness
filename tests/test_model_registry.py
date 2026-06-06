"""v1.9 ModelRegistry 测试：register/list/show + 本地 JSON 回退 + DB 不可用不失败。"""

from __future__ import annotations

import json

import pytest
from sqlalchemy.exc import OperationalError

from robot_dh.models import ModelRegistry, ModelSpec


def _spec(model_id: str, **kw) -> ModelSpec:
    return ModelSpec(
        model_id=model_id,
        model_name=kw.get("model_name", model_id),
        model_type=kw.get("model_type", "caption"),
        backend=kw.get("backend", "mock"),
        max_batch_size=kw.get("max_batch_size", 32),
    )


def test_register_list_show_sqlite(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path/'mr.db'}")
    reg = ModelRegistry()
    assert reg.backend_kind == "db"
    reg.register(_spec("mock-captioner-v1"))
    reg.register(_spec("mock-embedding-v1", model_type="embedding", backend="mock"))
    ids = sorted(s.model_id for s in reg.list_specs())
    assert ids == ["mock-captioner-v1", "mock-embedding-v1"]
    one = reg.get("mock-captioner-v1")
    assert one is not None and one.model_type == "caption"
    assert reg.get("nope") is None


def test_local_json_fallback(tmp_path) -> None:
    path = tmp_path / "model_registry.json"
    reg = ModelRegistry(local_path=path, local_only=True)
    assert reg.backend_kind == "local_json"
    reg.register(_spec("mock-anomaly-scorer-v1", model_type="anomaly_scorer"))
    assert path.exists()
    data = json.loads(path.read_text())
    assert "mock-anomaly-scorer-v1" in data["models"]
    # 新实例从同一 JSON 读出。
    reg2 = ModelRegistry(local_path=path, local_only=True)
    assert [s.model_id for s in reg2.list_specs()] == ["mock-anomaly-scorer-v1"]


def test_db_unavailable_falls_back_to_local(monkeypatch, tmp_path) -> None:
    """DB 探测抛异常时应回退本地 JSON 而非崩溃。"""
    monkeypatch.setenv("ROBOT_DH_DB_URI", "postgresql+psycopg://u:p@127.0.0.1:1/none")
    path = tmp_path / "fallback.json"

    def boom(*_a, **_k):
        raise OperationalError("connect", {}, Exception("refused"))

    monkeypatch.setattr("robot_dh.models.registry.get_engine", boom)
    reg = ModelRegistry(local_path=path)
    assert reg.backend_kind == "local_json"
    reg.register(_spec("mock-captioner-v1"))
    assert path.exists()
    assert [s.model_id for s in reg.list_specs()] == ["mock-captioner-v1"]


def test_register_from_config(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path/'cfg.db'}")
    cfg = tmp_path / "models.yaml"
    cfg.write_text(
        "models:\n"
        "  - model_id: mock-captioner-v1\n"
        "    model_name: Mock Captioner\n"
        "    model_type: caption\n"
        "    backend: mock\n"
        "  - model_id: local-rule-scorer-v1\n"
        "    model_name: Local Rule\n"
        "    model_type: anomaly_scorer\n"
        "    backend: local_cpu\n",
        encoding="utf-8",
    )
    reg = ModelRegistry()
    specs = reg.register_from_config(cfg)
    assert {s.model_id for s in specs} == {"mock-captioner-v1", "local-rule-scorer-v1"}


def test_invalid_backend_rejected() -> None:
    with pytest.raises(ValueError):
        ModelSpec(model_id="x", model_name="x", model_type="caption", backend="not_a_backend")
