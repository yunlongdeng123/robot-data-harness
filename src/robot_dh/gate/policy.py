from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

SUPPORTED_OPERATORS = {"<=", "<", ">=", ">", "==", "!=", "between", "in"}
SUPPORTED_SEVERITIES = {"fail", "warn"}


@dataclass(slots=True)
class GateRule:
    name: str
    metric: str
    op: str
    value: Any
    severity: str


@dataclass(slots=True)
class GatePolicy:
    rules: list[GateRule]


def load_gate_policy(path: Path) -> GatePolicy:
    if not path.exists():
        raise FileNotFoundError(f"Gate policy file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    raw_rules = payload.get("rules")
    if not isinstance(raw_rules, list):
        raise ValueError(f"Gate policy must contain a 'rules' list: {path}")

    rules: list[GateRule] = []
    for raw_rule in raw_rules:
        if not isinstance(raw_rule, dict):
            raise ValueError(f"Gate rule must be a mapping: {raw_rule}")
        rule = GateRule(
            name=str(raw_rule["name"]),
            metric=str(raw_rule["metric"]),
            op=str(raw_rule["op"]),
            value=raw_rule["value"],
            severity=str(raw_rule["severity"]).lower(),
        )
        if rule.op not in SUPPORTED_OPERATORS:
            raise ValueError(f"Unsupported gate operator '{rule.op}' in rule '{rule.name}'")
        if rule.severity not in SUPPORTED_SEVERITIES:
            raise ValueError(f"Unsupported gate severity '{rule.severity}' in rule '{rule.name}'")
        rules.append(rule)
    return GatePolicy(rules=rules)
