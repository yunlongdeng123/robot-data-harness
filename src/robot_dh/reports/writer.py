from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from robot_dh.logging_utils import get_log_format, get_logger, log_event
from robot_dh.reports.models import QualityReport


def _template_environment() -> Environment:
    template_dir = Path(__file__).resolve().parent / "templates"
    return Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )


def _validator_lookup(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {validator["name"]: validator for validator in report.get("validators", [])}


def _cluster_center_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    validator = _validator_lookup(report).get("xy_cluster", {})
    metrics = validator.get("metrics", {})
    centers = metrics.get("cluster_centers") or []
    counts = metrics.get("cluster_counts") or []
    radii = metrics.get("cluster_radii") or []
    rows: list[dict[str, Any]] = []
    for index, center in enumerate(centers, start=1):
        rows.append(
            {
                "index": index,
                "center_x": f"{center[0]:.4f}",
                "center_y": f"{center[1]:.4f}",
                "count": counts[index - 1] if index - 1 < len(counts) else "-",
                "radius": f"{radii[index - 1]:.4f}" if index - 1 < len(radii) else "-",
            }
        )
    return rows


def _jump_point_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    validator = _validator_lookup(report).get("velocity_jump", {})
    metrics = validator.get("metrics", {})
    indices = metrics.get("jump_indices") or []
    times = metrics.get("jump_times") or []
    rows: list[dict[str, Any]] = []
    for index, time_sec in zip(indices, times, strict=False):
        rows.append({"index": index, "time_sec": f"{float(time_sec):.3f}"})
    return rows


def _plot_cards(report: dict[str, Any]) -> list[tuple[str, str]]:
    plots = report.get("artifacts", {}).get("plots", {})
    titles = {
        "z_press_events": "Z press events",
        "xy_clusters": "XY clusters",
        "euler_angles": "Euler angles",
        "velocity_profile": "Velocity profile",
    }
    return [(titles[key], value) for key, value in plots.items() if key in titles]


def write_report_outputs(
    report: QualityReport,
    output_dir: Path,
    *,
    write_json: bool = True,
    write_html: bool = True,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dict = report.to_dict()
    json_path = output_dir / "report.json"
    html_path = output_dir / "report.html"
    written: dict[str, str] = {}

    if write_json:
        with json_path.open("w", encoding="utf-8") as handle:
            json.dump(report_dict, handle, indent=2, ensure_ascii=False)
        written["json"] = str(json_path)

    if write_html:
        template = _template_environment().get_template("report.html.j2")
        html = template.render(
            report=report_dict,
            cluster_centers=_cluster_center_rows(report_dict),
            jump_points=_jump_point_rows(report_dict),
            plot_cards=_plot_cards(report_dict),
        )
        with html_path.open("w", encoding="utf-8") as handle:
            handle.write(html)
        written["html"] = str(html_path)
    return written


def print_console_summary(report: QualityReport) -> None:
    if get_log_format() == "json":
        for validator in report.validators:
            log_event(
                "validator_summary",
                run_id=report.run_id,
                dataset_id=report.dataset_id,
                validator=validator.name,
                status=validator.status.value,
                metrics=validator.metrics,
            )
        if report.gate:
            log_event(
                "gate_finished",
                run_id=report.run_id,
                dataset_id=report.dataset_id,
                status=report.gate.get("status"),
                failed_rules=report.gate.get("failed_rules", []),
                warning_rules=report.gate.get("warning_rules", []),
            )
        log_event(
            "report_generated",
            run_id=report.run_id,
            dataset_id=report.dataset_id,
            status=report.status,
            artifacts=report.artifacts,
        )
        return

    logger = get_logger()
    for validator in report.validators:
        if validator.name == "schema":
            message = f"shape=({validator.metrics['n_samples']}, {validator.metrics['n_dims']})"
        elif validator.name == "quaternion":
            message = f"max_norm_error={validator.metrics['quat_max_norm_error']:.6f}"
        elif validator.name == "velocity_jump":
            message = f"max_velocity={validator.metrics['max_velocity_mps']:.2f} m/s"
        elif validator.name == "press_event":
            message = f"detected={validator.metrics['detected_press_count']}"
        elif validator.name == "xy_cluster":
            silhouette = validator.metrics.get("silhouette_score")
            if silhouette is None:
                message = f"k={validator.metrics['num_clusters']}, silhouette=n/a"
            else:
                message = f"k={validator.metrics['num_clusters']}, silhouette={silhouette:.2f}"
        elif validator.name == "workspace_bbox":
            message = f"outside_ratio={validator.metrics['outside_ratio']:.3f}"
        else:
            message = "; ".join(validator.messages)
        logger.info("[%s] %s: %s", validator.status.value, validator.name, message)

    html_path = report.artifacts.get("report_html") or report.artifacts.get("html")
    if html_path is None:
        html_path = report.artifacts.get("report_json") or report.artifacts.get("json")
    if html_path is None:
        logger.info("[%s] report outputs disabled by configuration", report.status)
        return
    if report.gate:
        gate_status = report.gate.get("status", "SKIP")
        failed_rules = report.gate.get("failed_rules", [])
        warning_rules = report.gate.get("warning_rules", [])
        if gate_status == "FAIL":
            logger.info("[FAIL] quality gate failed: %s", ", ".join(failed_rules))
        elif gate_status == "WARN":
            logger.info("[WARN] quality gate warnings: %s", ", ".join(warning_rules))
        elif gate_status == "PASS":
            logger.info("[PASS] quality gate passed")
    logger.info("[%s] report generated: %s", report.status, html_path)
