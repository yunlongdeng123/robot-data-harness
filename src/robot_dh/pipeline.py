from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

from robot_dh.artifacts import create_artifact_store, resolve_artifact_store_type
from robot_dh.config import load_config
from robot_dh.data.loaders import DatasetLoader
from robot_dh.gate import evaluate_gate, load_gate_policy, write_gate_report
from robot_dh.logging_utils import log_event
from robot_dh.registry import RegistryService, get_db_backend
from robot_dh.reports.models import QualityReport
from robot_dh.reports.plots import write_plot_artifacts
from robot_dh.reports.writer import print_console_summary, write_report_outputs
from robot_dh.validators import (
    EulerStabilityValidator,
    PressEventValidator,
    QuaternionValidator,
    SchemaValidator,
    VelocityJumpValidator,
    WorkspaceBBoxValidator,
    XYClusterValidator,
)
from robot_dh.validators.base import BaseValidator, ValidationResult, ValidationStatus


def _default_validators() -> list[BaseValidator]:
    return [
        SchemaValidator(),
        QuaternionValidator(),
        EulerStabilityValidator(),
        VelocityJumpValidator(),
        PressEventValidator(),
        XYClusterValidator(),
        WorkspaceBBoxValidator(),
    ]


def _compute_report_status(results: list[ValidationResult]) -> str:
    statuses = {result.status for result in results}
    if ValidationStatus.FAIL in statuses:
        return ValidationStatus.FAIL.value
    if ValidationStatus.WARN in statuses:
        return ValidationStatus.WARN.value
    return ValidationStatus.PASS.value


def _aggregate_metrics(results: list[ValidationResult]) -> dict[str, Any]:
    by_name = {result.name: result for result in results}
    return {
        "quat_max_norm_error": by_name["quaternion"].metrics.get("quat_max_norm_error"),
        "max_velocity_mps": by_name["velocity_jump"].metrics.get("max_velocity_mps"),
        "detected_press_count": by_name["press_event"].metrics.get("detected_press_count"),
        "cluster_silhouette": by_name["xy_cluster"].metrics.get("silhouette_score"),
        "max_cluster_radius": by_name["xy_cluster"].metrics.get("max_cluster_radius"),
        "outside_ratio": by_name["workspace_bbox"].metrics.get("outside_ratio"),
    }


def _resolve_artifact_prefix(
    prefix_template: str | None,
    *,
    run_id: str,
    dataset_id: str,
    store_type: str,
) -> str:
    template = prefix_template
    if template is None:
        template = "runs/{run_id}" if store_type == "s3" else ""
    return template.format(run_id=run_id, dataset_id=dataset_id).strip("/")


def _artifact_path(prefix: str, relative_path: str) -> str:
    relative = relative_path.lstrip("/")
    return f"{prefix}/{relative}" if prefix else relative


def _upload_plot_artifacts(
    output_dir: Path,
    plots: dict[str, str],
    *,
    artifact_store,
    artifact_prefix: str,
) -> dict[str, str]:
    uploaded: dict[str, str] = {}
    for plot_name, relative_path in plots.items():
        uploaded[plot_name] = artifact_store.put_file(
            output_dir / relative_path,
            _artifact_path(artifact_prefix, relative_path),
        )
    return uploaded


def run_validation(
    dataset_path: Path,
    config_path: Path | None,
    output_dir: Path,
    run_id: str | None,
    *,
    record_to_registry: bool = False,
    gate_policy_path: Path | None = None,
    artifact_store_type: str | None = None,
    artifact_prefix: str | None = None,
    db_uri: str | None = None,
) -> QualityReport:
    started = perf_counter()
    started_at = datetime.now(timezone.utc).isoformat()
    config = load_config(config_path)
    loader = DatasetLoader(config)
    dataset = loader.load(dataset_path)
    dataset_version = str(
        dataset.meta.get("version", dataset.meta.get("dataset_version", "v1"))
    )
    state: dict[str, Any] = {}
    validators = _default_validators()
    results: list[ValidationResult] = []
    resolved_run_id = run_id or datetime.now(timezone.utc).strftime("run-%Y%m%d-%H%M%S")
    for validator in validators:
        validator_started = perf_counter()
        result = validator.validate(dataset, state, config)
        result.details["duration_ms"] = round((perf_counter() - validator_started) * 1000.0, 3)
        results.append(result)
        log_event(
            "validator_finished",
            run_id=resolved_run_id,
            dataset_id=dataset.dataset_id,
            validator=validator.name,
            status=result.status.value,
            duration_ms=result.details["duration_ms"],
            metrics=result.metrics,
        )

    output_dir = output_dir.expanduser().resolve()
    report_cfg = config.get("reports", {})
    write_json = bool(report_cfg.get("json", True))
    write_html = bool(report_cfg.get("html", True))
    plots = write_plot_artifacts(dataset, state, output_dir) if report_cfg.get("save_plots", True) else {}
    resolved_store_type = resolve_artifact_store_type(artifact_store_type)
    resolved_artifact_prefix = _resolve_artifact_prefix(
        artifact_prefix,
        run_id=resolved_run_id,
        dataset_id=dataset.dataset_id,
        store_type=resolved_store_type,
    )
    status = _compute_report_status(results)
    warnings = list(dataset.warnings)
    errors: list[str] = []
    for result in results:
        if result.status == ValidationStatus.WARN:
            warnings.extend(result.messages)
        if result.status == ValidationStatus.FAIL:
            errors.extend(result.messages)

    report = QualityReport(
        run_id=resolved_run_id,
        dataset_id=dataset.dataset_id,
        status=status,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc).isoformat(),
        duration_sec=perf_counter() - started,
        config=config,
        dataset_meta={
            "dataset_path": str(dataset.dataset_path),
            "endpose_path": str(dataset.endpose_path),
            "video_path": str(dataset.video_path) if dataset.video_path else None,
            "meta_path": str(dataset.meta_path) if dataset.meta_path else None,
            "video_meta": {
                "fps": dataset.video_meta.fps,
                "frame_count": dataset.video_meta.frame_count,
                "duration_sec": dataset.video_meta.duration_sec,
                "source": dataset.video_meta.source,
            },
            "meta": dataset.meta,
        },
        metrics=_aggregate_metrics(results),
        validators=results,
        artifacts={"plots": plots},
        warnings=warnings,
        errors=errors,
        registry={
            "dataset_id": dataset.dataset_id,
            "version": dataset_version,
            "run_db_id": None,
            "db_backend": get_db_backend(db_uri),
        },
        gate={"status": "SKIP", "results": [], "failed_rules": [], "warning_rules": []},
    )
    report.artifacts["artifact_store"] = resolved_store_type
    if resolved_artifact_prefix:
        report.artifacts["artifact_prefix"] = resolved_artifact_prefix

    if gate_policy_path is not None:
        gate_policy = load_gate_policy(gate_policy_path)
        report.gate = {
            **evaluate_gate(report.metrics, gate_policy),
            "policy_path": str(gate_policy_path.resolve()),
        }

    if write_json:
        report.artifacts["report_json"] = str(output_dir / "report.json")
    if write_html:
        report.artifacts["report_html"] = str(output_dir / "report.html")
    artifact_store = create_artifact_store(output_dir=output_dir, store_type=resolved_store_type)
    if gate_policy_path is not None:
        gate_report_path = output_dir / "gate_report.json"
        write_gate_report(report.gate, gate_report_path)
        report.artifacts["gate_report"] = str(gate_report_path)

    write_report_outputs(report, output_dir, write_json=write_json, write_html=write_html)
    report.artifacts["plots"] = _upload_plot_artifacts(
        output_dir,
        plots,
        artifact_store=artifact_store,
        artifact_prefix=resolved_artifact_prefix,
    )
    write_report_outputs(report, output_dir, write_json=write_json, write_html=write_html)
    if gate_policy_path is not None:
        write_gate_report(report.gate, output_dir / "gate_report.json")

    if write_json:
        report.artifacts["report_json_uri"] = artifact_store.put_file(
            output_dir / "report.json",
            _artifact_path(resolved_artifact_prefix, "report.json"),
        )
    if write_html:
        report.artifacts["report_html_uri"] = artifact_store.put_file(
            output_dir / "report.html",
            _artifact_path(resolved_artifact_prefix, "report.html"),
        )
    if gate_policy_path is not None:
        report.artifacts["gate_report_uri"] = artifact_store.put_file(
            output_dir / "gate_report.json",
            _artifact_path(resolved_artifact_prefix, "gate_report.json"),
        )

    if record_to_registry:
        registry_service = RegistryService(db_uri=db_uri)
        registry_service.upsert_dataset(
            dataset_id=dataset.dataset_id,
            version=dataset_version,
            task_type=dataset.meta.get("task_type"),
            robot_type=dataset.meta.get("robot_type"),
            pose_format=dataset.meta.get("pose_format", "eexyzxyzw"),
            storage_uri=dataset.meta.get("storage_uri", dataset.dataset_path.resolve().as_uri()),
            local_path=str(dataset.dataset_path),
            num_samples=int(dataset.pose.shape[0]),
            duration_sec=float(dataset.video_meta.duration_sec),
            fps=float(dataset.video_meta.fps),
            last_status=report.status,
            last_run_id=report.run_id,
        )
        report.registry["run_db_id"] = registry_service.record_validation_run(
            report=report,
            dataset_version=dataset_version,
            config_path=config_path,
            output_dir=output_dir,
        )

    write_report_outputs(report, output_dir, write_json=write_json, write_html=write_html)
    if gate_policy_path is not None:
        write_gate_report(report.gate, output_dir / "gate_report.json")
    if write_json and report.artifacts.get("report_json_uri"):
        report.artifacts["report_json_uri"] = artifact_store.put_file(
            output_dir / "report.json",
            _artifact_path(resolved_artifact_prefix, "report.json"),
        )
    if write_html and report.artifacts.get("report_html_uri"):
        report.artifacts["report_html_uri"] = artifact_store.put_file(
            output_dir / "report.html",
            _artifact_path(resolved_artifact_prefix, "report.html"),
        )
    print_console_summary(report)
    return report


COMPARE_RULES = {
    "cluster_silhouette": {"direction": "higher", "warn_ratio": 0.03, "fail_ratio": 0.10},
    "max_velocity_mps": {"direction": "lower", "warn_ratio": 0.10, "fail_ratio": 0.25},
    "quat_max_norm_error": {"direction": "lower", "warn_ratio": 0.10, "fail_ratio": 0.50},
    "max_cluster_radius": {"direction": "lower", "warn_ratio": 0.10, "fail_ratio": 0.25},
    "outside_ratio": {"direction": "lower", "warn_ratio": 0.10, "fail_ratio": 0.25},
}


def _load_report_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _compare_metric(metric: str, baseline: Any, candidate: Any) -> str:
    if baseline is None or candidate is None:
        return "WARN"
    if metric not in COMPARE_RULES:
        return "PASS" if baseline == candidate else "WARN"

    baseline_value = float(baseline)
    candidate_value = float(candidate)
    if abs(baseline_value) <= 1.0e-12:
        delta_ratio = float("inf") if candidate_value != baseline_value else 0.0
    else:
        delta_ratio = abs(candidate_value - baseline_value) / abs(baseline_value)

    direction = COMPARE_RULES[metric]["direction"]
    degraded = candidate_value < baseline_value if direction == "higher" else candidate_value > baseline_value
    if not degraded:
        return "PASS"
    if delta_ratio >= COMPARE_RULES[metric]["fail_ratio"]:
        return "FAIL"
    if delta_ratio >= COMPARE_RULES[metric]["warn_ratio"]:
        return "WARN"
    return "PASS"


def compare_reports(baseline_path: Path, candidate_path: Path) -> dict[str, Any]:
    baseline = _load_report_json(baseline_path)
    candidate = _load_report_json(candidate_path)
    lines: list[str] = []
    has_failures = False
    metrics = [
        "cluster_silhouette",
        "max_velocity_mps",
        "quat_max_norm_error",
        "detected_press_count",
        "max_cluster_radius",
        "outside_ratio",
    ]
    for metric in metrics:
        baseline_value = baseline.get("metrics", {}).get(metric)
        candidate_value = candidate.get("metrics", {}).get(metric)
        verdict = _compare_metric(metric, baseline_value, candidate_value)
        lines.append(f"{metric}: {baseline_value} -> {candidate_value} {verdict}")
        if verdict == "FAIL":
            has_failures = True
    lines.append(f"status: {baseline.get('status')} -> {candidate.get('status')}")
    if candidate.get("status") == "FAIL":
        has_failures = True
    return {"lines": lines, "has_failures": has_failures}
