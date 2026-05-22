from __future__ import annotations

from typing import Any, Mapping

import numpy as np

from robot_dh.data.dataset import DatasetBundle
from robot_dh.validators.base import BaseValidator, ValidationResult, ValidationStatus, ValidatorState


def normalize_quaternions(quaternions_xyzw: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(quaternions_xyzw, axis=1, keepdims=True)
    norms = np.clip(norms, 1.0e-12, None)
    return quaternions_xyzw / norms


def make_quaternions_continuous(quaternions_xyzw: np.ndarray) -> np.ndarray:
    continuous = quaternions_xyzw.astype(np.float64, copy=True)
    for index in range(1, continuous.shape[0]):
        if np.dot(continuous[index - 1], continuous[index]) < 0.0:
            continuous[index] *= -1.0
    return continuous


class QuaternionValidator(BaseValidator):
    name = "quaternion"

    def validate(
        self,
        dataset: DatasetBundle,
        state: ValidatorState,
        config: Mapping[str, Any],
    ) -> ValidationResult:
        validator_cfg = config.get("validators", {}).get("quaternion", {})
        tolerance = float(validator_cfg.get("quat_norm_tol", 1.0e-5))
        quaternions = dataset.quaternions
        norms = np.linalg.norm(quaternions, axis=1)
        norm_errors = np.abs(norms - 1.0)
        bad_indices = np.flatnonzero(norm_errors > tolerance)
        normalized = make_quaternions_continuous(normalize_quaternions(quaternions))
        state["normalized_quaternions"] = normalized

        metrics = {
            "quat_norm_min": float(np.min(norms)),
            "quat_norm_max": float(np.max(norms)),
            "quat_norm_mean": float(np.mean(norms)),
            "quat_max_norm_error": float(np.max(norm_errors)),
            "num_bad_quaternions": int(len(bad_indices)),
        }

        status = ValidationStatus.PASS if len(bad_indices) == 0 else ValidationStatus.FAIL
        messages = [f"max_norm_error={metrics['quat_max_norm_error']:.6f}"]
        details = {"bad_indices": bad_indices.astype(int).tolist()}
        return ValidationResult(
            name=self.name,
            status=status,
            metrics=metrics,
            messages=messages,
            details=details,
        )
