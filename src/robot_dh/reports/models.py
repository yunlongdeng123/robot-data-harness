from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any

import numpy as np

from robot_dh.validators.base import ValidationResult


def to_serializable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.ndarray):
        return [to_serializable(item) for item in value.tolist()]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        if np.isnan(value) or np.isinf(value):
            return None
        return float(value)
    if is_dataclass(value):
        return to_serializable(asdict(value))
    if isinstance(value, dict):
        return {str(key): to_serializable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_serializable(item) for item in value]
    return value


@dataclass(slots=True)
class QualityReport:
    run_id: str
    dataset_id: str
    status: str
    started_at: str
    finished_at: str
    duration_sec: float
    config: dict[str, Any]
    dataset_meta: dict[str, Any]
    metrics: dict[str, Any]
    validators: list[ValidationResult]
    artifacts: dict[str, Any]
    warnings: list[str]
    errors: list[str]
    registry: dict[str, Any] = field(default_factory=dict)
    gate: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return to_serializable(asdict(self))
