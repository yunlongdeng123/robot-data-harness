from __future__ import annotations

import json
from pathlib import Path

from robot_dh.gate import evaluate_gate, evaluate_report, load_gate_policy, write_gate_report


def write_policy(path: Path) -> Path:
    path.write_text(
        """
rules:
  - name: velocity
    metric: max_velocity_mps
    op: "<="
    value: 2.0
    severity: fail
  - name: presses
    metric: detected_press_count
    op: "between"
    value: [8, 12]
    severity: warn
  - name: silhouette
    metric: cluster_silhouette
    op: ">="
    value: 0.7
    severity: fail
""".strip(),
        encoding="utf-8",
    )
    return path


def test_gate_pass_warn_fail(tmp_path: Path) -> None:
    policy = load_gate_policy(write_policy(tmp_path / "gate.yaml"))

    passed = evaluate_gate(
        {
            "max_velocity_mps": 1.0,
            "detected_press_count": 10,
            "cluster_silhouette": 0.9,
        },
        policy,
    )
    assert passed["status"] == "PASS"

    warned = evaluate_gate(
        {
            "max_velocity_mps": 1.0,
            "detected_press_count": 3,
            "cluster_silhouette": 0.9,
        },
        policy,
    )
    assert warned["status"] == "WARN"
    assert warned["warning_rules"] == ["presses"]

    failed = evaluate_gate(
        {
            "max_velocity_mps": 4.0,
            "detected_press_count": 10,
            "cluster_silhouette": 0.4,
        },
        policy,
    )
    assert failed["status"] == "FAIL"
    assert failed["failed_rules"] == ["velocity", "silhouette"]


def test_gate_report_roundtrip(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"
    report_path.write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "dataset_id": "dataset-1",
                "metrics": {
                    "max_velocity_mps": 1.2,
                    "detected_press_count": 10,
                    "cluster_silhouette": 0.8,
                },
            }
        ),
        encoding="utf-8",
    )
    gate_report = evaluate_report(report_path, write_policy(tmp_path / "gate.yaml"))
    output_path = write_gate_report(gate_report, tmp_path / "gate_report.json")

    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"
    assert payload["run_id"] == "run-1"
