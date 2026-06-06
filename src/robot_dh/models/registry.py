"""模型注册表：PostgreSQL 优先，DB 不可用时回退本地 JSON。

行为（见 v1_9_promptB 第三节）：
1. DB 可用：读写 model_registry 表。
2. DB 不可用：读写 .robot_dh/model_registry.json。
3. 支持从 configs/model_registry.yaml 批量初始化默认模型。
4. health：mock / local_cpu 直接 PASS；openai_compatible 尝试探测 endpoint。
5. 不打印任何 secret（endpoint_url 可打印，api_key 永不入库 / 不回显）。
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robot_dh.models.backends.base import get_backend
from robot_dh.models.schemas import BackendHealth, ModelSpec
from robot_dh.registry import get_engine, resolve_db_uri
from robot_dh.warehouse.models import ModelRegistryRow, ensure_lake_tables

LOG = logging.getLogger(__name__)

DEFAULT_LOCAL_REGISTRY = Path(".robot_dh/model_registry.json")


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"model registry config not found: {path}")
    import yaml

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"model registry config must be a mapping: {path}")
    return data


class ModelRegistry:
    """模型注册表读写入口。

    Args:
        db_uri: 显式 DB URI；空走 ROBOT_DH_DB_URI / SQLite 默认。
        local_path: 本地 JSON 路径，默认 .robot_dh/model_registry.json。
        local_only: 强制只用本地 JSON（测试 / 离线）。
    """

    def __init__(
        self,
        *,
        db_uri: str | None = None,
        local_path: Path | str | None = None,
        local_only: bool = False,
    ) -> None:
        self._db_uri = db_uri
        self._local_path = Path(local_path) if local_path else DEFAULT_LOCAL_REGISTRY
        self._local_only = local_only or os.environ.get("ROBOT_DH_MODEL_REGISTRY_LOCAL") == "1"

    # ---------- 后端选择 ----------

    def _engine_or_none(self) -> Engine | None:
        """返回可用 engine；本地模式或连接失败时返回 None（触发 JSON 回退）。"""
        if self._local_only:
            return None
        try:
            resolved = resolve_db_uri(self._db_uri)
            engine = get_engine(resolved)
            if engine.dialect.name == "sqlite":
                ensure_lake_tables(engine)
            else:
                # 远端 PG：探测一次连接，失败即回退本地。
                with engine.connect() as conn:  # noqa: F841 - 仅探测
                    pass
            return engine
        except SQLAlchemyError as err:
            LOG.warning("model registry: DB 不可用，回退本地 JSON：%s", err)
            return None
        except Exception as err:  # 连接/驱动层异常一律回退，不让 register 失败
            LOG.warning("model registry: DB 探测异常，回退本地 JSON：%s", err)
            return None

    @property
    def backend_kind(self) -> str:
        """当前实际使用的后端：'db' 或 'local_json'。"""
        return "local_json" if self._engine_or_none() is None else "db"

    # ---------- 本地 JSON ----------

    def _read_local(self) -> dict[str, dict[str, Any]]:
        if not self._local_path.exists():
            return {}
        try:
            data = json.loads(self._local_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        models = data.get("models") if isinstance(data, dict) else None
        return models if isinstance(models, dict) else {}

    def _write_local(self, models: dict[str, dict[str, Any]]) -> None:
        self._local_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"models": models, "updated_at": _utcnow_iso()}
        self._local_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    # ---------- 写 ----------

    def register(self, spec: ModelSpec) -> ModelSpec:
        """注册 / 更新一个模型；返回写入后的 spec。"""
        engine = self._engine_or_none()
        if engine is None:
            return self._register_local(spec)
        try:
            return self._register_db(engine, spec)
        except SQLAlchemyError as err:
            LOG.warning("model registry: DB 写入失败，回退本地 JSON：%s", err)
            return self._register_local(spec)

    def _register_db(self, engine: Engine, spec: ModelSpec) -> ModelSpec:
        now = _utcnow()
        with Session(engine, expire_on_commit=False, future=True) as session:
            row = session.get(ModelRegistryRow, spec.model_id)
            kwargs = spec.to_row_kwargs()
            if row is None:
                session.add(ModelRegistryRow(created_at=now, updated_at=now, **kwargs))
            else:
                for k, v in kwargs.items():
                    setattr(row, k, v)
                row.updated_at = now
            session.commit()
        return spec

    def _register_local(self, spec: ModelSpec) -> ModelSpec:
        models = self._read_local()
        now = _utcnow_iso()
        existing = models.get(spec.model_id) or {}
        spec.created_at = existing.get("created_at") or now
        spec.updated_at = now
        models[spec.model_id] = spec.to_dict()
        self._write_local(models)
        return spec

    def register_from_config(self, config_path: Path | str) -> list[ModelSpec]:
        """从 yaml 批量注册默认模型，返回注册结果列表。"""
        data = _load_yaml(Path(config_path))
        raw_models = data.get("models")
        if not isinstance(raw_models, list):
            raise ValueError("model registry config 缺少 models 列表")
        out: list[ModelSpec] = []
        for entry in raw_models:
            if not isinstance(entry, dict):
                continue
            spec = ModelSpec.from_dict(entry)
            # endpoint_url 支持从 env 注入（openai_compatible 默认读 base_url）。
            if not spec.endpoint_url and spec.backend == "openai_compatible":
                spec.endpoint_url = os.environ.get("ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL") or None
            out.append(self.register(spec))
        return out

    # ---------- 读 ----------

    def get(self, model_id: str) -> ModelSpec | None:
        engine = self._engine_or_none()
        if engine is None:
            models = self._read_local()
            entry = models.get(model_id)
            return ModelSpec.from_dict(entry) if entry else None
        try:
            with Session(engine, expire_on_commit=False, future=True) as session:
                row = session.get(ModelRegistryRow, model_id)
                return ModelSpec.from_row(row) if row else None
        except SQLAlchemyError as err:
            LOG.warning("model registry: DB 读取失败，回退本地 JSON：%s", err)
            entry = self._read_local().get(model_id)
            return ModelSpec.from_dict(entry) if entry else None

    def list_specs(self) -> list[ModelSpec]:
        engine = self._engine_or_none()
        if engine is None:
            return [ModelSpec.from_dict(v) for v in self._read_local().values()]
        try:
            with Session(engine, expire_on_commit=False, future=True) as session:
                rows = session.execute(
                    select(ModelRegistryRow).order_by(ModelRegistryRow.model_id)
                ).scalars().all()
                return [ModelSpec.from_row(r) for r in rows]
        except SQLAlchemyError as err:
            LOG.warning("model registry: DB 列举失败，回退本地 JSON：%s", err)
            return [ModelSpec.from_dict(v) for v in self._read_local().values()]

    # ---------- health ----------

    def health(self, model_id: str) -> BackendHealth:
        spec = self.get(model_id)
        if spec is None:
            return BackendHealth(
                status="FAIL",
                backend="unknown",
                model_id=model_id,
                detail="模型未注册",
                error=f"model_id={model_id} 不在注册表中",
            )
        backend = get_backend(spec)
        return backend.health(spec)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_iso() -> str:
    return _utcnow().replace(microsecond=0).isoformat().replace("+00:00", "Z")
