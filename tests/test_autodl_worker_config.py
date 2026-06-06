"""v1.9 PromptC：AutoDL worker 配置校验测试（不依赖 GPU / vLLM）。"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types

import pytest

_WORKER_PATH = pathlib.Path(__file__).resolve().parents[1] / "workers" / "autodl_inference_worker" / "worker.py"


def _load_worker():
    spec = importlib.util.spec_from_file_location("autodl_inference_worker", _WORKER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # dataclass 装饰器需要模块在 sys.modules 中才能解析类型。
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


worker = _load_worker()


def _args(**kw) -> types.SimpleNamespace:
    base = dict(poll_interval_sec=10, max_jobs=1, dry_run=False, model_id=None)
    base.update(kw)
    return types.SimpleNamespace(**base)


def test_missing_db_uri_errors() -> None:
    with pytest.raises(worker.WorkerConfigError):
        worker.load_worker_config(_args(model_id="m"), env={})


def test_missing_model_id_errors() -> None:
    with pytest.raises(worker.WorkerConfigError):
        worker.load_worker_config(_args(model_id=None), env={"ROBOT_DH_DB_URI": "sqlite:///x.db"})


def test_non_dry_run_requires_endpoint() -> None:
    with pytest.raises(worker.WorkerConfigError):
        worker.load_worker_config(
            _args(model_id="m", dry_run=False),
            env={"ROBOT_DH_DB_URI": "sqlite:///x.db"},
        )


def test_dry_run_ok_without_endpoint() -> None:
    cfg = worker.load_worker_config(
        _args(model_id="m", dry_run=True),
        env={"ROBOT_DH_DB_URI": "sqlite:///x.db"},
    )
    assert cfg.model_id == "m"
    # to_safe_dict 不泄露密码。
    safe = cfg.to_safe_dict()
    assert "CHANGE_ME" not in str(safe)
    assert "password" not in str(safe).lower()


def test_config_reads_endpoint_from_env() -> None:
    cfg = worker.load_worker_config(
        _args(model_id="m", dry_run=False),
        env={
            "ROBOT_DH_DB_URI": "postgresql+psycopg://u:p@host:5432/db",
            "ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL": "http://127.0.0.1:8000/v1",
            "ROBOT_DH_OPENAI_COMPATIBLE_MODEL": "Qwen/Qwen2.5-0.5B-Instruct",
        },
    )
    assert cfg.openai_base_url == "http://127.0.0.1:8000/v1"
    assert cfg.openai_model == "Qwen/Qwen2.5-0.5B-Instruct"
    # db host 脱敏后不含密码
    assert "p@" not in cfg.to_safe_dict()["db_host"]
