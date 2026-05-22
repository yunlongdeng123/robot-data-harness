from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from robot_dh.gate.policy import GatePolicy, GateRule, load_gate_policy


def _evaluate_rule(actual_value: Any, rule: GateRule) -> bool:
    if actual_value is None:
        return False
    if rule.op == "<=":
        return float(actual_value) <= float(rule.value)
    if rule.op == "<":
        return float(actual_value) < float(rule.value)
    if rule.op == ">=":
        return float(actual_value) >= float(rule.value)
    if rule.op == ">":
        return float(actual_value) > float(rule.value)
    if rule.op == "==":
        return actual_value == rule.value
    if rule.op == "!=":
        return actual_value != rule.value
    if rule.op == "between":
        lower_bound, upper_bound = rule.value
        return float(lower_bound) <= float(actual_value) <= float(upper_bound)
    if rule.op == "in":
        return actual_value in rule.value
    raise ValueError(f"Unsupported gate operator: {rule.op}")


def evaluate_gate(metrics: Mapping[str, Any], policy: GatePolicy) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    failed_rules: list[str] = []
    warning_rules: list[str] = []
    for rule in policy.rules:
        actual_value = metrics.get(rule.metric)
        passed = _evaluate_rule(actual_value, rule)
        results.append(
            {
                "name": rule.name,
                "metric": rule.metric,
                "op": rule.op,
                "expected": rule.value,
                "actual": actual_value,
                "severity": rule.severity,
                "passed": passed,
            }
        )
        if passed:
            continue
        if rule.severity == "fail":
            failed_rules.append(rule.name)
        else:
            warning_rules.append(rule.name)

    status = "PASS"
    if failed_rules:
        status = "FAIL"
    elif warning_rules:
        status = "WARN"
    return {
        "status": status,
        "results": results,
        "failed_rules": failed_rules,
        "warning_rules": warning_rules,
    }


def evaluate_report(report_path: Path, policy_path: Path) -> dict[str, Any]:
    with report_path.open("r", encoding="utf-8") as handle:
        report = json.load(handle)
    gate = evaluate_gate(report.get("metrics", {}), load_gate_policy(policy_path))
    gate["report_path"] = str(report_path.resolve())
    gate["policy_path"] = str(policy_path.resolve())
    gate["run_id"] = report.get("run_id")
    gate["dataset_id"] = report.get("dataset_id")
    return gate


def write_gate_report(gate_report: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(dict(gate_report), handle, indent=2, ensure_ascii=False)
    return output_path
