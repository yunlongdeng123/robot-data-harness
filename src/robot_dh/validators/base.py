from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

from robot_dh.data.dataset import DatasetBundle

ValidatorState = dict[str, Any]


class ValidationStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass(slots=True)
class ValidationResult:
    name: str
    status: ValidationStatus
    metrics: dict[str, Any] = field(default_factory=dict)
    messages: list[str] = field(default_factory=list)
    artifacts: dict[str, Any] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)


class BaseValidator(ABC):
    name: str

    @abstractmethod
    def validate(
        self,
        dataset: DatasetBundle,
        state: ValidatorState,
        config: Mapping[str, Any],
    ) -> ValidationResult:
        raise NotImplementedError
