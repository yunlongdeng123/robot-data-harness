"""benchmark runner：编排 mutation -> validate -> 状态比较。"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from robot_dh.benchmark.models import (
    BenchmarkCase,
    BenchmarkCaseResult,
    BenchmarkReport,
    BenchmarkSuite,
)
from robot_dh.benchmark.mutations import MutationError, apply_mutation
from robot_dh.benchmark.report import (
    render_html_report,
    render_markdown_report,
    write_benchmark_artifacts,
)
from robot_dh.pipeline import run_validation
from robot_dh.runtime.events import RuntimeEventLogger, utcnow_iso
from robot_dh.runtime.ids import new_run_id
from robot_dh.warehouse.service import WarehouseService

LOG = logging.getLogger(__name__)


def _resolve_failed_validators(report_payload: dict[str, Any]) -> list[str]:
    """从 report.json payload 抽取 FAIL 的 validator 名字。"""
    failed: list[str] = []
    for validator in report_payload.get("validators") or []:
        if isinstance(validator, dict):
            name = validator.get("name")
            status = validator.get("status")
        else:
            name = getattr(validator, "name", None)
            status = getattr(validator, "status", None)
            if hasattr(status, "value"):
                status = status.value
        if name and status == "FAIL":
            failed.append(str(name))
    return failed


def _report_to_payload(report: Any) -> dict[str, Any]:
    if hasattr(report, "to_dict"):
        return report.to_dict()
    return dict(report)


def _run_single_case(
    case: BenchmarkCase,
    *,
    output_dir: Path,
    mutated_dir: Path,
    default_config_path: Path | None,
    gate_policy_path: Path | None,
    events: RuntimeEventLogger,
    benchmark_id: str,
) -> BenchmarkCaseResult:
    case_started = time.time()
    actual_status = "ERROR"
    actual_failed: list[str] = []
    error_message: str | None = None
    mutated_path: str | None = None
    dataset_path: str | None = None
    report_uri: str | None = None
    metrics: dict[str, Any] = {}

    case_output_dir = output_dir / "cases" / case.case_id
    case_output_dir.mkdir(parents=True, exist_ok=True)

    try:
        if case.mutation:
            if not case.source_dataset:
                raise MutationError(f"case {case.case_id} requires source_dataset for mutation")
            src = Path(case.source_dataset).expanduser().resolve()
            target = mutated_dir / f"{case.case_id}"
            apply_mutation(source_dataset=src, output_dataset=target, mutation=case.mutation)
            dataset = target
            mutated_path = target.as_posix()
            events.emit(
                "mutation_applied",
                payload={"case_id": case.case_id, "mutation": case.mutation, "source": src.as_posix(), "target": target.as_posix()},
                run_id=benchmark_id,
            )
        else:
            if not case.dataset:
                raise ValueError(f"case {case.case_id} missing dataset path (and no mutation)")
            dataset = Path(case.dataset).expanduser().resolve()
        dataset_path = dataset.as_posix()

        cfg_path: Path | None = None
        if case.config_path:
            cfg_path = Path(case.config_path).expanduser()
        elif default_config_path is not None:
            cfg_path = default_config_path

        gp: Path | None = None
        if case.gate_policy_path:
            gp = Path(case.gate_policy_path).expanduser()
        elif gate_policy_path is not None:
            gp = gate_policy_path

        try:
            report = run_validation(
                dataset_path=dataset,
                config_path=cfg_path,
                output_dir=case_output_dir,
                run_id=f"{benchmark_id}::{case.case_id}",
                gate_policy_path=gp,
            )
            payload = _report_to_payload(report)
            actual_status = str(payload.get("status", "PASS"))
            actual_failed = _resolve_failed_validators(payload)
            metrics = dict(payload.get("metrics") or {})
            artifacts = payload.get("artifacts") or {}
            report_uri = artifacts.get("report_json") or (case_output_dir / "report.json").as_posix()
        except Exception as err:  # noqa: BLE001
            actual_status = "FAIL"
            actual_failed = ["pipeline_error"]
            error_message = f"{type(err).__name__}: {err}"
            LOG.warning("case %s pipeline failure: %s", case.case_id, err)

        expected_set = set(case.expected_failed_validators or [])
        actual_set = set(actual_failed)
        validator_match = (not expected_set) or expected_set.issubset(actual_set)
        status_match = case.expected_status.upper() == actual_status.upper()
        match = status_match and validator_match
    except Exception as err:
        actual_status = "ERROR"
        error_message = f"{type(err).__name__}: {err}"
        match = False
        LOG.exception("case %s failed: %s", case.case_id, err)

    duration_sec = time.time() - case_started
    return BenchmarkCaseResult(
        case_id=case.case_id,
        mutation=case.mutation,
        expected_status=case.expected_status,
        actual_status=actual_status,
        expected_failed_validators=list(case.expected_failed_validators or []),
        actual_failed_validators=actual_failed,
        match=match,
        duration_sec=duration_sec,
        error_message=error_message,
        dataset_path=dataset_path,
        mutated_dataset_path=mutated_path,
        report_uri=report_uri,
        metrics=metrics,
    )


def run_benchmark(
    *,
    suite_path: Path,
    output_dir: Path,
    record_to_registry: bool = False,
    default_config_path: Path | None = None,
    gate_policy_path: Path | None = None,
    warehouse: WarehouseService | None = None,
    events: RuntimeEventLogger | None = None,
) -> BenchmarkReport:
    suite_path = suite_path.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    mutated_dir = output_dir / "mutated"
    mutated_dir.mkdir(parents=True, exist_ok=True)

    suite = BenchmarkSuite.from_yaml(suite_path)
    warehouse = warehouse or WarehouseService(soft=True)
    events = events or RuntimeEventLogger(warehouse=warehouse)

    benchmark_id = new_run_id(prefix="bench")
    started = time.time()
    started_iso = utcnow_iso()

    events.emit(
        "benchmark_started",
        payload={"suite_name": suite.suite_name, "suite_path": suite_path.as_posix(), "total_cases": len(suite.cases)},
        run_id=benchmark_id,
    )

    case_results: list[BenchmarkCaseResult] = []
    for case in suite.cases:
        result = _run_single_case(
            case,
            output_dir=output_dir,
            mutated_dir=mutated_dir,
            default_config_path=default_config_path,
            gate_policy_path=gate_policy_path,
            events=events,
            benchmark_id=benchmark_id,
        )
        case_results.append(result)
        events.emit(
            "benchmark_case_finished",
            payload={
                "case_id": case.case_id,
                "mutation": case.mutation,
                "expected_status": case.expected_status,
                "actual_status": result.actual_status,
                "expected_failed_validators": list(case.expected_failed_validators or []),
                "actual_failed_validators": list(result.actual_failed_validators),
                "match": result.match,
                "duration_sec": result.duration_sec,
            },
            run_id=benchmark_id,
        )
        warehouse.record_benchmark_case(
            benchmark_id=benchmark_id,
            case_id=case.case_id,
            mutation=case.mutation,
            expected_status=case.expected_status,
            actual_status=result.actual_status,
            expected_failed_validators=list(case.expected_failed_validators or []),
            actual_failed_validators=list(result.actual_failed_validators),
            match=result.match,
            duration_sec=result.duration_sec,
            error_message=result.error_message,
            metrics=result.metrics,
            dataset_uri=result.dataset_path,
            artifacts_uri=result.report_uri,
        )

    total = len(case_results)
    passed = sum(1 for c in case_results if c.match)
    failed = total - passed
    mismatched = sum(1 for c in case_results if not c.match)
    duration_sec = time.time() - started
    finished_iso = utcnow_iso()
    status = "PASS" if failed == 0 else "FAIL"

    report = BenchmarkReport(
        benchmark_id=benchmark_id,
        suite_name=suite.suite_name,
        suite_path=suite_path.as_posix(),
        status=status,
        total=total,
        passed=passed,
        failed=failed,
        mismatched=mismatched,
        duration_sec=duration_sec,
        started_at=started_iso,
        finished_at=finished_iso,
        cases=case_results,
        output_dir=output_dir.as_posix(),
    )

    write_benchmark_artifacts(report, output_dir)

    warehouse.record_benchmark_run(
        benchmark_id=benchmark_id,
        suite_name=suite.suite_name,
        suite_path=suite_path.as_posix(),
        total_cases=total,
        passed=passed,
        failed=failed,
        mismatched=mismatched,
        status=status,
        started_at=datetime.fromisoformat(started_iso.replace("Z", "+00:00")),
        finished_at=datetime.fromisoformat(finished_iso.replace("Z", "+00:00")),
        duration_sec=duration_sec,
        report_uri=(output_dir / "benchmark_report.json").as_posix(),
        metrics={"record_to_registry": bool(record_to_registry)},
    )

    events.emit(
        "benchmark_finished",
        payload={
            "benchmark_id": benchmark_id,
            "status": status,
            "total": total,
            "passed": passed,
            "failed": failed,
        },
        run_id=benchmark_id,
    )

    return report
