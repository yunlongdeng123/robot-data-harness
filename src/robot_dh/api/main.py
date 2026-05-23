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