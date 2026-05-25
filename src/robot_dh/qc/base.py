"""QC contract 基础类型。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

Severity = Literal["info", "warn", "fail"]


@dataclass(slots=True)
class Rule:
    """contract 内单条规则定义；不依赖具体 dataset。"""

    rule_id: str
    metric: str
    op: str  # ">=", "<=", "==", "!=", ">", "<", "in", "exists"
    threshold: Any
    severity: Severity = "warn"
    description: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RuleResult:
    """单条规则执行结果。"""

    rule_id: str
    metric: str
    op: str
    threshold: Any
    actual: Any
    severity: Severity
    status: str  # PASS / WARN / FAIL / SKIP

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AssetProfile:
    """asset 级画像。schema 与 PG asset_profiles 表对齐。"""

    profile_id: str
    asset_uri: str
    asset_format: str | None = None
    dataset_id: str | None = None
    version: str | None = None
    dataset_family: str | None = None
    layer: str | None = None
    bytes: int | None = None
    rows: int | None = None
    files_count: int | None = None
    episodes_count: int | None = None
    videos_count: int | None = None
    schema_hash: str | None = None
    null_rate: float | None = None
    profile: dict[str, Any] = field(default_factory=dict)
    status: str = "OK"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ContractReport:
    """单次 contract 执行的统一输出。"""

    contract_id: str
    dataset_family: str
    dataset_id: str
    version: str
    dataset_uri: str
    status: str  # PASS / WARN / FAIL
    started_at: str
    finished_at: str
    duration_sec: float
    metrics: dict[str, Any] = field(default_factory=dict)
    rules: list[RuleResult] = field(default_factory=list)
    failed_rules: list[dict[str, Any]] = field(default_factory=list)
    warning_rules: list[dict[str, Any]] = field(default_factory=list)
    artifacts: dict[str, Any] = field(default_factory=dict)
    run_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["rules"] = [r.to_dict() for r in self.rules]
        return payload


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def evaluate_rule(rule: Rule, actual: Any) -> RuleResult:
    """根据 rule.op 比较 actual 与 threshold；不匹配按 severity 决定 WARN/FAIL。"""

    def _ok(actual: Any) -> bool:
        op = rule.op
        thr = rule.threshold
        if actual is None:
            # actual 缺失 -> 视为不通过（除非 op 是 exists 的反向）
            return op == "missing-ok"
        try:
            if op == ">=":
                return actual >= thr
            if op == "<=":
                return actual <= thr
            if op == ">":
                return actual > thr
            if op == "<":
                return actual < thr
            if op == "==":
                return actual == thr
            if op == "!=":
                return actual != thr
            if op == "in":
                return actual in thr
            if op == "exists":
                return actual is not None and actual != ""
        except TypeError:
            return False
        return False

    passed = _ok(actual)
    status = "PASS" if passed else ("WARN" if rule.severity == "warn" else "FAIL" if rule.severity == "fail" else "PASS")
    if rule.severity == "info" and not passed:
        status = "PASS"
    return RuleResult(
        rule_id=rule.rule_id,
        metric=rule.metric,
        op=rule.op,
        threshold=rule.threshold,
        actual=actual,
        severity=rule.severity,
        status=status,
    )


def aggregate_status(results: list[RuleResult]) -> str:
    if any(r.status == "FAIL" for r in results):
        return "FAIL"
    if any(r.status == "WARN" for r in results):
        return "WARN"
    return "PASS"
