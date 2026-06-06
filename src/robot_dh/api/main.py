from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from robot_dh.infra import run_infra_doctor
from robot_dh.pipeline import run_validation
from robot_dh.registry import RegistryService
from robot_dh.warehouse.service import (
    LakeMetadataUnavailableError,
    WarehouseService,
)
from robot_dh.warehouse.robot_platform import PlatformWarehouse
from robot_dh.warehouse_metrics import (
    WAREHOUSE_TABLE_REGISTRY,
    WarehouseQueryService,
    WarehouseTableNotKnownError,
    load_warehouse_metrics_config,
)
from robot_dh.warehouse_metrics.query import QueryRequest
from robot_dh.quality_ops import build_quality_summary, show_backfill_status
from robot_dh.quality_ops.backfill import BackfillPlanner
from robot_dh.quality_ops.sla import load_sla_policies, perform_sla_checks
from sqlalchemy import inspect as sqla_inspect, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from robot_dh.warehouse.models import (
    BackfillPlanRow,
    SlaCheckRow,
)
from robot_dh.registry import get_engine, resolve_db_uri


class ValidateRequest(BaseModel):
    dataset_path: str
    config_path: str
    output_dir: str
    run_id: str
    record_to_registry: bool = False
    gate_policy_path: str | None = None
    artifact_store: str | None = None
    artifact_prefix: str | None = None


app = FastAPI(title="robot-data-harness", version="0.1.7")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/infra/health")
def infra_health() -> dict[str, object]:
    return run_infra_doctor()


@app.get("/datasets")
def list_datasets() -> list[dict[str, object]]:
    registry = RegistryService()
    return [asdict(record) for record in registry.list_datasets()]


@app.get("/runs")
def list_runs() -> list[dict[str, object]]:
    registry = RegistryService()
    return [asdict(record) for record in registry.list_runs()]


@app.post("/validate")
def validate(request: ValidateRequest) -> dict:
    report = run_validation(
        dataset_path=Path(request.dataset_path),
        config_path=Path(request.config_path),
        output_dir=Path(request.output_dir),
        run_id=request.run_id,
        record_to_registry=request.record_to_registry,
        gate_policy_path=Path(request.gate_policy_path) if request.gate_policy_path else None,
        artifact_store_type=request.artifact_store,
        artifact_prefix=request.artifact_prefix,
    )
    return report.to_dict()


@app.get("/runs/{run_id}")
def get_run(run_id: str) -> dict:
    registry = RegistryService()
    detail = registry.get_run_detail(run_id)
    if detail is not None:
        return asdict(detail)

    artifacts_root = Path(os.environ.get("ROBOT_DH_ARTIFACTS_DIR", "/artifacts"))
    if artifacts_root.exists():
        for report_path in artifacts_root.glob("**/report.json"):
            with report_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if payload.get("run_id") == run_id:
                return payload

    raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")


def _warehouse_strict() -> WarehouseService:
    """为 API 读路径构造 strict（出错即抛）的 WarehouseService。"""
    return WarehouseService(soft=False)


@app.get("/lake/assets")
def lake_assets(
    layer: str | None = None,
    dataset_id: str | None = None,
    version: str | None = None,
    limit: int = 200,
) -> list[dict]:
    try:
        return _warehouse_strict().list_lake_assets(
            layer=layer, dataset_id=dataset_id, version=version, limit=limit
        )
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


@app.get("/lake/lineage")
def lake_lineage(uri: str, limit: int = 200) -> dict:
    try:
        return _warehouse_strict().list_lineage(uri=uri, limit=limit)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


@app.get("/etl/jobs")
def etl_jobs(limit: int = 50) -> list[dict]:
    try:
        return _warehouse_strict().list_etl_jobs(limit=limit)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


@app.get("/etl/jobs/{job_id}")
def etl_job_detail(job_id: str) -> dict:
    try:
        payload = _warehouse_strict().get_etl_job(job_id)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))
    if payload is None:
        raise HTTPException(status_code=404, detail=f"etl job not found: {job_id}")
    return payload


@app.get("/quality/summary")
def quality_summary(
    date: str | None = None,
    limit: int = 50,
) -> object:
    """quality summary。

    传 ``date=YYYY-MM-DD`` 时走 v1.8 ads_quality_dashboard 聚合；
    不传 date 时回退到 v1.4 旧的 quality_snapshots 列表（保持向后兼容）。
    """
    if date is not None:
        try:
            return build_quality_summary(date_=date).to_dict()
        except SQLAlchemyError as err:
            raise HTTPException(status_code=503, detail=f"db unavailable: {err}")
    try:
        return _warehouse_strict().latest_quality_summary(limit=limit)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


# v1.5 只读接口


@app.get("/etl/perf")
def etl_perf(
    dataset_id: str | None = None,
    version: str | None = None,
    phase: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[dict]:
    try:
        return _warehouse_strict().list_etl_perf_runs(
            dataset_id=dataset_id,
            version=version,
            phase=phase,
            status=status,
            limit=limit,
        )
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


@app.get("/etl/shards")
def etl_shards(
    plan_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[dict]:
    try:
        return _warehouse_strict().list_etl_shards(plan_id=plan_id, status=status, limit=limit)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


@app.get("/benchmark/runs")
def benchmark_runs(limit: int = 100) -> list[dict]:
    try:
        return _warehouse_strict().list_benchmark_runs(limit=limit)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


@app.get("/benchmark/runs/{benchmark_id}")
def benchmark_run_detail(benchmark_id: str) -> dict:
    try:
        payload = _warehouse_strict().get_benchmark_run(benchmark_id)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))
    if payload is None:
        raise HTTPException(status_code=404, detail=f"benchmark not found: {benchmark_id}")
    return payload


@app.get("/events")
def runtime_events(
    event_type: str | None = None,
    run_id: str | None = None,
    job_id: str | None = None,
    limit: int = 100,
) -> list[dict]:
    try:
        return _warehouse_strict().list_runtime_events(
            event_type=event_type,
            run_id=run_id,
            job_id=job_id,
            limit=limit,
        )
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


# v1.6 只读接口


def _platform_warehouse_strict() -> PlatformWarehouse:
    return PlatformWarehouse(soft=False)


@app.get("/qc/contracts")
def qc_contracts(limit: int = 100) -> list[dict]:
    try:
        return _platform_warehouse_strict().list_qc_contracts(limit=limit)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


@app.get("/qc/contracts/{contract_id}")
def qc_contract_detail(contract_id: str) -> dict:
    try:
        rows = _platform_warehouse_strict().list_qc_contracts(limit=1000)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))
    for r in rows:
        if r["contract_id"] == contract_id:
            return r
    raise HTTPException(status_code=404, detail=f"contract not found: {contract_id}")


@app.get("/qc/runs")
def qc_runs(
    contract_id: str | None = None,
    dataset_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[dict]:
    try:
        return _platform_warehouse_strict().list_qc_contract_runs(
            contract_id=contract_id, dataset_id=dataset_id, status=status, limit=limit,
        )
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


@app.get("/qc/runs/{run_id}")
def qc_run_detail(run_id: str) -> dict:
    try:
        payload = _platform_warehouse_strict().get_qc_contract_run(run_id)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))
    if payload is None:
        raise HTTPException(status_code=404, detail=f"qc run not found: {run_id}")
    return payload


@app.get("/assets/profiles")
def asset_profiles(
    dataset_id: str | None = None,
    version: str | None = None,
    dataset_family: str | None = None,
    limit: int = 200,
) -> list[dict]:
    try:
        return _platform_warehouse_strict().list_asset_profiles(
            dataset_id=dataset_id, version=version, dataset_family=dataset_family, limit=limit,
        )
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


@app.get("/assets/profiles/{profile_id}")
def asset_profile_detail(profile_id: str) -> dict:
    try:
        payload = _platform_warehouse_strict().get_asset_profile(profile_id)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))
    if payload is None:
        raise HTTPException(status_code=404, detail=f"profile not found: {profile_id}")
    return payload



# v1.6.3 ml-ready / workflows endpoints


@app.get("/ml-ready")
def ml_ready_list(limit: int = 100) -> list[dict]:
    try:
        return _platform_warehouse_strict().list_ml_ready_datasets(limit=limit)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


@app.get("/ml-ready/{dataset_id}/{version}")
def ml_ready_detail(dataset_id: str, version: str) -> dict:
    try:
        payload = _platform_warehouse_strict().get_ml_ready_dataset(dataset_id=dataset_id, version=version)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))
    if payload is None:
        raise HTTPException(status_code=404, detail=f"ml-ready dataset not found: {dataset_id}/{version}")
    return payload


@app.get("/workflows")
def workflows_list(status: str | None = None, limit: int = 100) -> list[dict]:
    try:
        return _platform_warehouse_strict().list_workflow_runs(status=status, limit=limit)
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


@app.get("/workflows/{workflow_name}")
def workflow_detail(workflow_name: str, namespace: str | None = None) -> dict:
    try:
        payload = _platform_warehouse_strict().get_workflow_run(
            workflow_name=workflow_name, workflow_namespace=namespace,
        )
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))
    if payload is None:
        raise HTTPException(status_code=404, detail=f"workflow not found: {workflow_name}")
    return payload


@app.get("/workflows/{workflow_name}/steps")
def workflow_steps(workflow_name: str, namespace: str | None = None, limit: int = 200) -> list[dict]:
    try:
        return _platform_warehouse_strict().list_workflow_steps(
            workflow_name=workflow_name, workflow_namespace=namespace, limit=limit,
        )
    except LakeMetadataUnavailableError as err:
        raise HTTPException(status_code=503, detail=str(err))


class _WorkflowSubmitRequest(BaseModel):
    workflow_name: str | None = None
    parameters: dict | None = None


@app.post("/workflows/scale30")
def workflows_submit_scale30(req: _WorkflowSubmitRequest) -> dict:
    """v1.6.3 控制面：当前不直接 submit 重 ETL，仅返回 501 + 提示走 Argo CLI。"""
    raise HTTPException(
        status_code=501,
        detail={
            "reason": "server-side argo submit not enabled in this build",
            "hint": "use  or  instead",
            "requested": req.dict(),
        },
    )


# v1.8 warehouse / quality / backfill / sla 只读接口
#
# 设计要点：
# - DB 不可用 → 503
# - 仅暴露白名单表 / 简单 WHERE，避免任意 SQL 入口
# - 不在 API 中触发重 build / backfill run；保留 CLI 入口


def _warehouse_query_service() -> WarehouseQueryService:
    return WarehouseQueryService(config=load_warehouse_metrics_config())


@app.get("/warehouse/tables")
def warehouse_tables() -> list[dict[str, object]]:
    """列出 v1.8 注册的所有表 + 是否在 DB 中存在。"""
    try:
        return _warehouse_query_service().list_tables()
    except SQLAlchemyError as err:
        raise HTTPException(status_code=503, detail=f"warehouse db unavailable: {err}")


@app.get("/warehouse/query")
def warehouse_query(
    table: str,
    limit: int = 50,
    where: str | None = None,
    order_by: str | None = None,
) -> list[dict[str, object]]:
    """查询 v1.8 表。

    - ``table`` 必须在 ``WAREHOUSE_TABLE_REGISTRY`` 白名单内
    - ``where`` 仅支持 ``col = 'value'`` / ``col IN ('a','b')``（用 AND 连接）
    - ``order_by`` 仅支持 ``col`` / ``col ASC`` / ``col DESC``
    """
    if table not in WAREHOUSE_TABLE_REGISTRY:
        raise HTTPException(
            status_code=400,
            detail=f"unknown warehouse table '{table}'; known={sorted(WAREHOUSE_TABLE_REGISTRY)}",
        )
    try:
        return _warehouse_query_service().query(QueryRequest(
            table=table, limit=limit, where=where, order_by=order_by,
        ))
    except WarehouseTableNotKnownError as err:
        raise HTTPException(status_code=400, detail=str(err))
    except SQLAlchemyError as err:
        raise HTTPException(status_code=503, detail=f"warehouse db unavailable: {err}")


@app.get("/quality/report/latest")
def quality_report_latest() -> dict[str, object]:
    """返回最近一日（按 UTC）的 quality summary。"""
    try:
        return build_quality_summary().to_dict()
    except SQLAlchemyError as err:
        raise HTTPException(status_code=503, detail=f"db unavailable: {err}")


@app.get("/backfill/plans")
def backfill_plans_list(limit: int = 50) -> list[dict[str, object]]:
    """最近的 backfill_plans 列表（按 created_at DESC）。"""
    engine = get_engine(resolve_db_uri(None))
    existing = set(sqla_inspect(engine).get_table_names())
    if "backfill_plans" not in existing:
        raise HTTPException(status_code=503, detail="backfill_plans 表不存在；请先 apply v1.8 schema")
    try:
        with Session(engine, expire_on_commit=False, future=True) as session:
            rows = session.execute(
                select(BackfillPlanRow).order_by(BackfillPlanRow.created_at.desc()).limit(limit)
            ).scalars().all()
            return [
                {
                    "plan_id": r.plan_id,
                    "status": r.status,
                    "from_date": r.from_date.isoformat() if r.from_date else None,
                    "to_date": r.to_date.isoformat() if r.to_date else None,
                    "dataset_id": r.dataset_id,
                    "version": r.version,
                    "phase": r.phase,
                    "reason": r.reason,
                    "task_count": r.task_count,
                    "created_by": r.created_by,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
                for r in rows
            ]
    except SQLAlchemyError as err:
        raise HTTPException(status_code=503, detail=f"db unavailable: {err}")


@app.get("/backfill/plans/{plan_id}")
def backfill_plan_detail(plan_id: str) -> dict[str, object]:
    try:
        status = show_backfill_status(plan_id=plan_id)
    except SQLAlchemyError as err:
        raise HTTPException(status_code=503, detail=f"db unavailable: {err}")
    if status.get("plan") is None and "missing_tables" not in status:
        raise HTTPException(status_code=404, detail=f"plan not found: {plan_id}")
    return status


@app.get("/sla/checks")
def sla_checks_list(
    date: str | None = None,
    limit: int = 200,
) -> list[dict[str, object]]:
    """按日期返回 sla_checks 列表。"""
    engine = get_engine(resolve_db_uri(None))
    existing = set(sqla_inspect(engine).get_table_names())
    if "sla_checks" not in existing:
        raise HTTPException(status_code=503, detail="sla_checks 表不存在；请先 apply v1.8 schema")
    try:
        with Session(engine, expire_on_commit=False, future=True) as session:
            query = select(SlaCheckRow).order_by(SlaCheckRow.checked_at.desc()).limit(limit)
            if date is not None:
                from datetime import date as _date
                try:
                    target = _date.fromisoformat(date)
                except ValueError as err:
                    raise HTTPException(status_code=400, detail=f"invalid date: {err}")
                query = select(SlaCheckRow).where(SlaCheckRow.dt == target).limit(limit)
            rows = session.execute(query).scalars().all()
            return [
                {
                    "check_id": r.check_id,
                    "policy_id": r.policy_id,
                    "dt": r.dt.isoformat() if r.dt else None,
                    "dataset_id": r.dataset_id,
                    "version": r.version,
                    "status": r.status,
                    "qc_pass_rate": r.qc_pass_rate,
                    "etl_success_rate": r.etl_success_rate,
                    "workflow_success_rate": r.workflow_success_rate,
                    "missing_outputs_json": r.missing_outputs_json,
                    "failed_reason": r.failed_reason,
                    "metrics_json": r.metrics_json,
                    "checked_at": r.checked_at.isoformat() if r.checked_at else None,
                }
                for r in rows
            ]
    except SQLAlchemyError as err:
        raise HTTPException(status_code=503, detail=f"db unavailable: {err}")


# ============================================================
# v1.9 AI Inference Data Plane 只读接口（+ 轻量 POST）
#
# 设计：复用 WarehouseQueryService 白名单查询；DB 不可用 -> 503；不暴露 secret；
# POST /inference/jobs 仅创建 job record，不在 API 进程内跑推理。
# ============================================================


def _inference_query(table: str, *, where: str | None = None, limit: int = 100, order_by: str | None = None) -> list[dict]:
    try:
        return _warehouse_query_service().query(QueryRequest(
            table=table, limit=limit, where=where, order_by=order_by,
        ))
    except WarehouseTableNotKnownError as err:
        raise HTTPException(status_code=400, detail=str(err))
    except SQLAlchemyError as err:
        raise HTTPException(status_code=503, detail=f"inference db unavailable: {err}")


@app.get("/models")
def models_list(model_type: str | None = None, backend: str | None = None, limit: int = 200) -> list[dict]:
    where = None
    if model_type:
        where = f"model_type = '{model_type}'"
    elif backend:
        where = f"backend = '{backend}'"
    return _inference_query("model_registry", where=where, limit=limit)


@app.get("/models/{model_id}")
def model_detail(model_id: str) -> dict:
    rows = _inference_query("model_registry", where=f"model_id = '{model_id}'", limit=1)
    if not rows:
        raise HTTPException(status_code=404, detail=f"model not found: {model_id}")
    return rows[0]


@app.get("/inference/jobs")
def inference_jobs_list(status: str | None = None, limit: int = 50) -> list[dict]:
    where = f"status = '{status}'" if status else None
    return _inference_query("inference_jobs", where=where, limit=limit, order_by="created_at DESC")


@app.get("/inference/jobs/{job_id}")
def inference_job_detail(job_id: str) -> dict:
    rows = _inference_query("inference_jobs", where=f"job_id = '{job_id}'", limit=1)
    if not rows:
        raise HTTPException(status_code=404, detail=f"inference job not found: {job_id}")
    return rows[0]


@app.get("/inference/jobs/{job_id}/outputs")
def inference_job_outputs(job_id: str, limit: int = 200) -> list[dict]:
    return _inference_query("inference_outputs", where=f"job_id = '{job_id}'", limit=limit)


@app.get("/inference/benchmarks")
def inference_benchmarks_list(model_id: str | None = None, limit: int = 100) -> list[dict]:
    where = f"model_id = '{model_id}'" if model_id else None
    return _inference_query("inference_benchmark_runs", where=where, limit=limit, order_by="created_at DESC")


@app.get("/distillation/datasets")
def distillation_datasets_list(status: str | None = None, limit: int = 100) -> list[dict]:
    where = f"status = '{status}'" if status else None
    return _inference_query("distillation_datasets", where=where, limit=limit)


@app.get("/distillation/datasets/{distill_id}")
def distillation_dataset_detail(distill_id: str) -> dict:
    rows = _inference_query("distillation_datasets", where=f"distill_id = '{distill_id}'", limit=1)
    if not rows:
        raise HTTPException(status_code=404, detail=f"distillation dataset not found: {distill_id}")
    return rows[0]


class _InferenceJobCreateRequest(BaseModel):
    model_id: str
    input_uri: str
    output_uri: str
    task_type: str
    job_name: str | None = None
    dataset_id: str | None = None
    version: str | None = None
    batch_size: int | None = None
    max_workers: int | None = None


@app.post("/inference/jobs")
def inference_job_create(req: _InferenceJobCreateRequest) -> dict:
    """轻量创建一条 inference_jobs 记录（status=QUEUED），不在 API 进程内执行推理。

    实际执行交给 CLI `robot-dh infer run` 或 AutoDL pull-based worker。DB 不可用返回 503。
    """
    from robot_dh.ai_tasks.state import JOB_QUEUED
    from robot_dh.ai_tasks.store import resolve_optional_engine
    from robot_dh.inference.job import InferenceJob, new_job_id, write_job_pg

    engine = resolve_optional_engine(None)
    if engine is None:
        raise HTTPException(status_code=503, detail="db unavailable: inference job 创建需要 PostgreSQL / SQLite")
    job = InferenceJob(
        job_id=new_job_id(),
        model_id=req.model_id,
        input_uri=req.input_uri,
        output_uri=req.output_uri,
        task_type=req.task_type,
        status=JOB_QUEUED,
        job_name=req.job_name,
        dataset_id=req.dataset_id,
        version=req.version,
        batch_size=req.batch_size,
        max_workers=req.max_workers,
    )
    if not write_job_pg(engine, job):
        raise HTTPException(status_code=503, detail="db unavailable: 写 inference_jobs 失败")
    return job.to_dict()
