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


class ValidateRequest(BaseModel):
    dataset_path: str
    config_path: str
    output_dir: str
    run_id: str
    record_to_registry: bool = False
    gate_policy_path: str | None = None
    artifact_store: str | None = None
    artifact_prefix: str | None = None


app = FastAPI(title="robot-data-harness", version="0.1.5")


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
def quality_summary(limit: int = 50) -> list[dict]:
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
