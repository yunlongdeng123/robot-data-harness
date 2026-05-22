from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from robot_dh.logging_utils import log_event
from robot_dh.pipeline import run_validation
from robot_dh.registry import RegistryService


def _peek_dataset_identity(dataset_dir: Path) -> tuple[str, str]:
    meta_path = dataset_dir / "meta.yaml"
    if meta_path.exists():
        with meta_path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        if isinstance(payload, dict):
            dataset_id = str(payload.get("dataset_id", dataset_dir.name))
            version = str(payload.get("version", payload.get("dataset_version", "v1")))
            return dataset_id, version
    return dataset_dir.name, "v1"


def discover_datasets(root: Path) -> list[Path]:
    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"Scan root not found: {root}")
    return sorted(
        child for child in root.iterdir() if child.is_dir() and (child / "endpose.pt").exists()
    )


def scan_datasets(
    *,
    root: Path,
    config_path: Path,
    output_root: Path,
    use_registry: bool,
    only_new: bool,
    gate_policy_path: Path | None = None,
    artifact_store_type: str | None = None,
    artifact_prefix: str | None = None,
    db_uri: str | None = None,
) -> dict[str, Any]:
    datasets = discover_datasets(root)
    output_root = output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    registry_service = RegistryService(db_uri=db_uri) if use_registry or only_new else None
    summary_runs: list[dict[str, Any]] = []
    succeeded = 0
    failed = 0
    skipped = 0
    scan_id = datetime.now(timezone.utc).strftime("scan-%Y%m%d-%H%M%S")
    started_at = datetime.now(timezone.utc).isoformat()

    for dataset_dir in datasets:
        dataset_id, version = _peek_dataset_identity(dataset_dir)
        if only_new and registry_service is not None and registry_service.has_successful_run(dataset_id, version):
            skipped += 1
            summary_runs.append(
                {
                    "dataset_id": dataset_id,
                    "version": version,
                    "status": "SKIPPED",
                    "dataset_path": str(dataset_dir.resolve()),
                    "reason": "existing successful run",
                }
            )
            continue

        run_id = f"scan-{dataset_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        dataset_output_dir = output_root / dataset_dir.name
        log_event(
            "scan_dataset_started",
            dataset_id=dataset_id,
            version=version,
            run_id=run_id,
            output_dir=str(dataset_output_dir),
        )
        report = run_validation(
            dataset_path=dataset_dir,
            config_path=config_path,
            output_dir=dataset_output_dir,
            run_id=run_id,
            record_to_registry=use_registry,
            gate_policy_path=gate_policy_path,
            artifact_store_type=artifact_store_type,
            artifact_prefix=artifact_prefix,
            db_uri=db_uri,
        )
        outcome = report.status
        if report.gate.get("status") == "FAIL":
            outcome = "FAIL"
        elif report.gate.get("status") == "WARN" and outcome != "FAIL":
            outcome = "WARN"
        if outcome == "FAIL":
            failed += 1
        else:
            succeeded += 1
        summary_runs.append(
            {
                "dataset_id": dataset_id,
                "version": version,
                "run_id": report.run_id,
                "status": outcome,
                "report_json": report.artifacts.get("report_json"),
                "output_dir": str(dataset_output_dir),
            }
        )
        log_event(
            "scan_dataset_finished",
            dataset_id=dataset_id,
            version=version,
            run_id=report.run_id,
            status=outcome,
        )

    summary = {
        "scan_id": scan_id,
        "total": len(datasets),
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "runs": summary_runs,
    }
    summary_path = output_root / "scan_summary.json"
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)
    if registry_service is not None:
        status = "FAIL" if failed > 0 else ("WARN" if skipped > 0 and succeeded == 0 else "PASS")
        registry_service.record_scan_job(
            scan_id=scan_id,
            root_uri=root.resolve().as_uri(),
            output_root=str(output_root),
            status=status,
            total=summary["total"],
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc).isoformat(),
            summary=summary,
        )
    return summary