"""ETL Runner：编排 raw -> ods -> dwd（-> ads），支持单数据集或批量扫描。

v1.6 新增：
- phase-level run：normalize | features | ads | all
- normalize 透传 resume / force / heartbeat / progress 参数

公开 API：
    etl_run(...)  : 单数据集流水线（normalize + build-features [+ build-ads]）
    etl_scan(...) : 扫描 raw 根目录，对每个数据集调用 etl_run
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from robot_dh.etl.ads import AdsResult, build_ads
from robot_dh.etl.features import FeatureResult, build_features
from robot_dh.etl.lineage import LineageEvent, write_lineage_events
from robot_dh.etl.normalize import NormalizeResult, normalize_dataset
from robot_dh.lake.hf_adapter import is_huggingface_dataset_dir
from robot_dh.lake.manifest import (
    MANIFEST_FILENAME,
    utcnow_iso,
)
from robot_dh.lake.store import create_lake_store
from robot_dh.lake.uri import is_s3_uri, join_uri, parse_uri
from robot_dh.warehouse.service import WarehouseService

LOG = logging.getLogger(__name__)


VALID_PHASES = ("normalize", "features", "ads", "all")


@dataclass(slots=True)
class EtlRunResult:
    dataset_id: str
    version: str
    raw_uri: str
    ods_uri: str
    dwd_uri: str
    ads_uri: str | None
    job_id: str
    status: str
    duration_sec: float
    normalize: NormalizeResult | None
    features: FeatureResult | None
    ads: AdsResult | None
    error: str | None = None
    summary_uri: str | None = None
    phase: str = "all"

    def to_dict(self) -> dict[str, Any]:
        def _r(r: Any) -> Any:
            if r is None:
                return None
            d = asdict(r)
            return d

        return {
            "dataset_id": self.dataset_id,
            "version": self.version,
            "raw_uri": self.raw_uri,
            "ods_uri": self.ods_uri,
            "dwd_uri": self.dwd_uri,
            "ads_uri": self.ads_uri,
            "job_id": self.job_id,
            "status": self.status,
            "duration_sec": self.duration_sec,
            "error": self.error,
            "normalize": _r(self.normalize),
            "features": _r(self.features),
            "ads": _r(self.ads),
            "summary_uri": self.summary_uri,
            "phase": self.phase,
        }


def _resolve_lake_layout(lake_root_uri: str, dataset_id: str, version: str) -> tuple[str, str, str]:
    ods_uri = join_uri(lake_root_uri, "ods", dataset_id, version)
    dwd_uri = join_uri(lake_root_uri, "dwd", dataset_id, version)
    ads_uri = join_uri(lake_root_uri, "ads", "quality")
    return ods_uri, dwd_uri, ads_uri


def _infer_dataset_identity(dataset_uri: str) -> tuple[str | None, str | None]:
    if is_s3_uri(dataset_uri):
        key = parse_uri(dataset_uri).key
    else:
        key = parse_uri(dataset_uri).local_path
    parts = [p for p in key.split("/") if p]
    if "raw" in parts:
        try:
            i = parts.index("raw")
            ds = parts[i + 1] if i + 1 < len(parts) else None
            ver = parts[i + 2] if i + 2 < len(parts) else None
            return ds, ver
        except IndexError:
            pass
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return None, None


def etl_run(
    *,
    dataset_uri: str,
    dataset_id: str | None,
    version: str | None,
    lake_root_uri: str,
    build_ads_layer: bool = False,
    features_config_path: Path | None = None,
    ads_config_path: Path | None = None,
    job_id: str | None = None,
    db_uri: str | None = None,
    warehouse: WarehouseService | None = None,
    summary_dir: Path | None = None,
    phase: str = "all",
    resume: bool = True,
    force: bool = False,
    heartbeat_interval_sec: float = 30.0,
    progress_log_interval_sec: float = 30.0,
    workflow_name: str | None = None,
    perf_dir: Path | None = None,
    warehouse_v16: Any | None = None,
) -> EtlRunResult:
    """对单个数据集执行 normalize + build-features（可选 build-ads）。

    phase: normalize | features | ads | all
        - normalize: 仅 raw -> ods
        - features:  从已有 ods 构建 dwd
        - ads:       从已有 dwd 写 ads
        - all:       原 v1.5 行为
    """
    if phase not in VALID_PHASES:
        raise ValueError(f"etl_run: unsupported phase={phase!r}; expected one of {VALID_PHASES}")

    if warehouse is None:
        warehouse = WarehouseService(soft=True, db_uri=db_uri)

    inferred_ds, inferred_ver = _infer_dataset_identity(dataset_uri)
    ds = dataset_id or inferred_ds
    ver = version or inferred_ver or "v1"
    if not ds:
        raise ValueError(
            f"etl_run: cannot infer dataset_id from {dataset_uri}; please pass --dataset-id"
        )

    job_id = job_id or f"etl-run-{ds}-{ver}-{uuid.uuid4().hex[:8]}"
    started = time.time()
    started_iso = utcnow_iso()

    LOG.info(
        "etl_run START: job_id=%s dataset_uri=%s -> %s/%s phase=%s resume=%s force=%s",
        job_id, dataset_uri, ds, ver, phase, resume, force,
    )

    warehouse.record_etl_job_start(
        job_id=job_id,
        job_type=f"etl_run.{phase}",
        input_uri=dataset_uri,
        output_uri=lake_root_uri,
    )

    ods_uri, dwd_uri, ads_uri = _resolve_lake_layout(lake_root_uri, ds, ver)

    result = EtlRunResult(
        dataset_id=ds,
        version=ver,
        raw_uri=dataset_uri,
        ods_uri=ods_uri,
        dwd_uri=dwd_uri,
        ads_uri=ads_uri if (build_ads_layer or phase == "ads") else None,
        job_id=job_id,
        status="RUNNING",
        duration_sec=0.0,
        normalize=None,
        features=None,
        ads=None,
        phase=phase,
    )

    try:
        warehouse.upsert_dataset_version(
            dataset_id=ds,
            version=ver,
            raw_uri=dataset_uri,
            status="discovered",
        )

        run_normalize = phase in ("normalize", "all")
        run_features = phase in ("features", "all")
        run_ads = (phase == "ads") or (phase == "all" and build_ads_layer)

        if run_normalize:
            LOG.info("  [normalize] %s -> %s", dataset_uri, ods_uri)
            result.normalize = normalize_dataset(
                dataset_uri=dataset_uri,
                output_uri=ods_uri,
                dataset_id=ds,
                version=ver,
                job_id=f"{job_id}::normalize",
                warehouse=warehouse,
                lake_root_uri=lake_root_uri,
                resume=resume,
                force=force,
                heartbeat_interval_sec=heartbeat_interval_sec,
                progress_log_interval_sec=progress_log_interval_sec,
                workflow_name=workflow_name,
                step_name=f"{ds}-{ver}-normalize",
                perf_dir=perf_dir,
                warehouse_v16=warehouse_v16,
            )

        if run_features:
            features_input = ods_uri
            LOG.info("  [build-features] %s -> %s", features_input, dwd_uri)
            result.features = build_features(
                input_uri=features_input,
                output_uri=dwd_uri,
                config_path=features_config_path,
                job_id=f"{job_id}::features",
                warehouse=warehouse,
                lake_root_uri=lake_root_uri,
            )

        if run_ads:
            LOG.info("  [build-ads] %s -> %s", join_uri(lake_root_uri, "dwd"), ads_uri)
            result.ads = build_ads(
                input_root_uri=join_uri(lake_root_uri, "dwd"),
                output_uri=ads_uri,
                config_path=ads_config_path,
                job_id=f"{job_id}::ads",
                db_uri=db_uri,
                warehouse=warehouse,
                lake_root_uri=lake_root_uri,
            )

        result.status = "OK"
        if result.features and result.features.job_status == "WARN":
            result.status = "WARN"

        elapsed = time.time() - started
        result.duration_sec = elapsed
        warehouse.record_etl_job_finish(
            job_id=job_id,
            status=result.status,
            metrics={
                "normalize_rows": result.normalize.num_samples if result.normalize else 0,
                "features_press_count": result.features.num_press_events if result.features else 0,
                "duration_ms": int(elapsed * 1000),
                "phase": phase,
            },
        )

        # 顶层血缘边 raw -> dwd（仅 features/all 跑过时）
        if run_features:
            # 顶层血缘边的 job_type 保持 v1.5 "etl_run" 以兼容下游查询；phase!=all 时附带后缀。
            edge_job_type = "etl_run" if phase == "all" else f"etl_run.{phase}"
            warehouse.record_lineage_edge(
                source_uri=dataset_uri,
                target_uri=dwd_uri,
                job_id=job_id,
                job_type=edge_job_type,
            )

    except Exception as err:  # noqa: BLE001
        elapsed = time.time() - started
        result.status = "FAIL"
        result.duration_sec = elapsed
        result.error = str(err)
        warehouse.record_etl_job_finish(job_id=job_id, status="FAIL", error_message=str(err))
        LOG.error("etl_run FAIL: %s", err)

    if summary_dir is not None:
        summary_dir = summary_dir.expanduser().resolve()
        summary_dir.mkdir(parents=True, exist_ok=True)
        summary_path = summary_dir / "etl_summary.json"
        summary_path.write_text(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        result.summary_uri = summary_path.as_posix()

    LOG.info(
        "etl_run END:   job_id=%s status=%s duration=%.2fs", job_id, result.status, result.duration_sec
    )
    return result


@dataclass(slots=True)
class EtlScanResult:
    root_uri: str
    lake_root_uri: str
    scan_id: str
    total: int
    succeeded: int
    failed: int
    skipped: int
    duration_sec: float
    runs: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "root_uri": self.root_uri,
            "lake_root_uri": self.lake_root_uri,
            "scan_id": self.scan_id,
            "total": self.total,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "skipped": self.skipped,
            "duration_sec": self.duration_sec,
            "runs": self.runs,
        }


def _discover_raw_datasets(root_uri: str) -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = []
    store = create_lake_store(root_uri)
    if is_s3_uri(root_uri):
        raw_uri = join_uri(root_uri, "raw")
        seen: set[tuple[str, str]] = set()
        for obj_uri in store.list(raw_uri):
            key = parse_uri(obj_uri).key
            parts = key.split("/")
            if len(parts) < 4 or parts[0] != "raw":
                continue
            ds, ver = parts[1], parts[2]
            seen.add((ds, ver))
        for ds, ver in sorted(seen):
            out.append((ds, ver, join_uri(root_uri, "raw", ds, ver)))
    else:
        root = Path(parse_uri(root_uri).local_path)
        if not root.exists():
            return out
        candidates: set[Path] = set()
        for endpose in sorted(root.rglob("endpose.pt")):
            candidates.add(endpose.parent)
        raw_root = root / "raw"
        if raw_root.is_dir():
            for ds_dir in sorted(p for p in raw_root.iterdir() if p.is_dir()):
                for version_dir in sorted(p for p in ds_dir.iterdir() if p.is_dir()):
                    if is_huggingface_dataset_dir(version_dir):
                        candidates.add(version_dir)

        for dataset_dir in sorted(candidates):
            ds_id_from_path: str | None = None
            ver_from_path: str | None = None
            ancestors = list(dataset_dir.parents)
            for i, anc in enumerate(ancestors):
                if anc.name == "raw":
                    rel_parts = dataset_dir.relative_to(anc).parts
                    if len(rel_parts) >= 2:
                        ds_id_from_path = rel_parts[0]
                        ver_from_path = "/".join(rel_parts[1:])
                    elif len(rel_parts) == 1:
                        ds_id_from_path = rel_parts[0]
                    break
            ds_id = ds_id_from_path or dataset_dir.name
            ver = ver_from_path or "v1"
            if ds_id_from_path is None or ver_from_path is None:
                meta_path = dataset_dir / "meta.yaml"
                if meta_path.is_file():
                    try:
                        import yaml as _yaml

                        raw = _yaml.safe_load(meta_path.read_text()) or {}
                        meta_ds = raw.get("dataset_id")
                        meta_ver = raw.get("version") or raw.get("dataset_version")
                        if meta_ds and ds_id_from_path is None:
                            ds_id = str(meta_ds)
                        if meta_ver and ver_from_path is None:
                            ver = str(meta_ver)
                    except Exception:
                        pass
            out.append((ds_id, ver, dataset_dir.as_posix()))
    return out


def _existing_layers(lake_root_uri: str, ds: str, ver: str) -> dict[str, bool]:
    store = create_lake_store(lake_root_uri)
    return {
        "ods": store.exists(join_uri(lake_root_uri, "ods", ds, ver, MANIFEST_FILENAME)),
        "dwd": store.exists(join_uri(lake_root_uri, "dwd", ds, ver, MANIFEST_FILENAME)),
    }


def _filter_discovered(
    discovered: list[tuple[str, str, str]],
    include_patterns: list[str] | None,
    exclude_patterns: list[str] | None,
) -> list[tuple[str, str, str]]:
    import fnmatch

    out = list(discovered)
    if include_patterns:
        out = [
            d
            for d in out
            if any(fnmatch.fnmatch(d[0], pat) or fnmatch.fnmatch(d[2], pat) for pat in include_patterns)
        ]
    if exclude_patterns:
        out = [
            d
            for d in out
            if not any(fnmatch.fnmatch(d[0], pat) or fnmatch.fnmatch(d[2], pat) for pat in exclude_patterns)
        ]
    return out


def etl_scan(
    *,
    root_uri: str,
    lake_root_uri: str,
    limit: int | None = None,
    build_ads_layer: bool = False,
    force: bool = False,
    features_config_path: Path | None = None,
    ads_config_path: Path | None = None,
    db_uri: str | None = None,
    summary_dir: Path | None = None,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
) -> EtlScanResult:
    scan_id = f"etl-scan-{uuid.uuid4().hex[:12]}"
    started = time.time()
    LOG.info("etl_scan START: scan_id=%s root=%s lake=%s", scan_id, root_uri, lake_root_uri)

    discovered = _discover_raw_datasets(root_uri)
    discovered = _filter_discovered(discovered, include_patterns, exclude_patterns)
    if limit is not None and limit > 0:
        discovered = discovered[:limit]

    runs: list[dict[str, Any]] = []
    succeeded = 0
    failed = 0
    skipped = 0

    warehouse = WarehouseService(soft=True, db_uri=db_uri)
    warehouse.record_etl_job_start(
        job_id=scan_id,
        job_type="etl_scan",
        input_uri=root_uri,
        output_uri=lake_root_uri,
    )

    for ds, ver, dataset_uri in discovered:
        if not force:
            layers = _existing_layers(lake_root_uri, ds, ver)
            if layers["ods"] and layers["dwd"]:
                skipped += 1
                runs.append(
                    {
                        "dataset_id": ds,
                        "version": ver,
                        "raw_uri": dataset_uri,
                        "status": "SKIPPED",
                        "reason": "ods+dwd manifests exist; pass --force to rebuild",
                    }
                )
                continue
        try:
            result = etl_run(
                dataset_uri=dataset_uri,
                dataset_id=ds,
                version=ver,
                lake_root_uri=lake_root_uri,
                build_ads_layer=False,
                features_config_path=features_config_path,
                ads_config_path=ads_config_path,
                job_id=f"{scan_id}-{ds}-{ver}",
                db_uri=db_uri,
                warehouse=warehouse,
                force=force,
            )
            if result.status in {"OK", "WARN"}:
                succeeded += 1
            else:
                failed += 1
            runs.append(result.to_dict())
        except Exception as err:  # noqa: BLE001
            failed += 1
            runs.append(
                {
                    "dataset_id": ds,
                    "version": ver,
                    "raw_uri": dataset_uri,
                    "status": "FAIL",
                    "error": str(err),
                }
            )

    if build_ads_layer:
        try:
            ads_uri = join_uri(lake_root_uri, "ads", "quality")
            ads_result = build_ads(
                input_root_uri=join_uri(lake_root_uri, "dwd"),
                output_uri=ads_uri,
                config_path=ads_config_path,
                job_id=f"{scan_id}::ads",
                db_uri=db_uri,
                warehouse=warehouse,
                lake_root_uri=lake_root_uri,
            )
            runs.append({"ads": asdict(ads_result)})
        except Exception as err:  # noqa: BLE001
            failed += 1
            runs.append({"ads": {"status": "FAIL", "error": str(err)}})

    elapsed = time.time() - started
    warehouse.record_etl_job_finish(
        job_id=scan_id,
        status="OK" if failed == 0 else ("FAIL" if succeeded == 0 else "WARN"),
        metrics={
            "total": len(discovered),
            "succeeded": succeeded,
            "failed": failed,
            "skipped": skipped,
            "duration_ms": int(elapsed * 1000),
        },
    )

    summary = EtlScanResult(
        root_uri=root_uri,
        lake_root_uri=lake_root_uri,
        scan_id=scan_id,
        total=len(discovered),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        duration_sec=elapsed,
        runs=runs,
    )

    if summary_dir is not None:
        summary_dir = summary_dir.expanduser().resolve()
        summary_dir.mkdir(parents=True, exist_ok=True)
        out = summary_dir / "etl_scan_summary.json"
        out.write_text(json.dumps(summary.to_dict(), indent=2, ensure_ascii=False))

    LOG.info(
        "etl_scan END:   scan_id=%s total=%d ok=%d warn-fail=%d skipped=%d duration=%.2fs",
        scan_id, summary.total, succeeded, failed, skipped, elapsed,
    )
    return summary
