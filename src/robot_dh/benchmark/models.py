"""benchmark suite / case / report 数据模型。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class BenchmarkCase:
    case_id: str
    dataset: str | None = None
    source_dataset: str | None = None
    mutation: str | None = None
    expected_status: str = "PASS"
    expected_failed_validators: list[str] = field(default_factory=list)
    config_path: str | None = None
    gate_policy_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BenchmarkSuite:
    suite_name: str
    cases: list[BenchmarkCase] = field(default_factory=list)
    description: str | None = None

    @classmethod
    def from_yaml(cls, path: Path) -> "BenchmarkSuite":
        if not path.is_file():
            raise FileNotFoundError(f"benchmark suite not found: {path}")
        raw = yaml.safe_load(path.read_text()) or {}
        cases_raw = raw.get("cases") or []
        cases: list[BenchmarkCase] = []
        for item in cases_raw:
            cases.append(
                BenchmarkCase(
                    case_id=str(item["case_id"]),
                    dataset=item.get("dataset"),
                    source_dataset=item.get("source_dataset"),
                    mutation=item.get("mutation"),
                    expected_status=str(item.get("expected_status", "PASS")),
                    expected_failed_validators=list(item.get("expected_failed_validators") or []),
                    config_path=item.get("config_path"),
                    gate_policy_path=item.get("gate_policy_path"),
                )
            )
        return cls(
            suite_name=str(raw.get("suite_name", path.stem)),
            cases=cases,
            description=raw.get("description"),
        )


@dataclass
class BenchmarkCaseResult:
    case_id: str
    mutation: str | None
    expected_status: str
    actual_status: str
    expected_failed_validators: list[str]
    actual_failed_validators: list[str]
    match: bool
    duration_sec: float
    error_message: str | None = None
    dataset_path: str | None = None
    mutated_dataset_path: str | None = None
    report_uri: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BenchmarkReport:
    benchmark_id: str
    suite_name: str
    suite_path: str
    status: str
    total: int
    passed: int
    failed: int
    mismatched: int
    duration_sec: float
    started_at: str
    finished_at: str
    cases: list[BenchmarkCaseResult] = field(default_factory=list)
    output_dir: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "benchmark_id": self.benchmark_id,
            "suite_name": self.suite_name,
            "suite_path": self.suite_path,
            "status": self.status,
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "mismatched": self.mismatched,
            "duration_sec": self.duration_sec,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "output_dir": self.output_dir,
            "cases": [c.to_dict() for c in self.cases],
        }
