from __future__ import annotations

import warnings
from typing import Any, Mapping

import numpy as np
from scipy.spatial.transform import Rotation

from robot_dh.data.dataset import DatasetBundle
from robot_dh.validators.base import BaseValidator, ValidationResult, ValidationStatus, ValidatorState
from robot_dh.validators.quaternion import make_quaternions_continuous, normalize_quaternions


def quaternion_to_euler(quaternions_xyzw: np.ndarray) -> np.ndarray:
    continuous = make_quaternions_continuous(normalize_quaternions(quaternions_xyzw))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        euler_rad = Rotation.from_quat(continuous).as_euler("xyz", degrees=False)
    return np.unwrap(euler_rad, axis=0)


class EulerStabilityValidator(BaseValidator):
    name = "euler_stability"

    def validate(
        self,
        dataset: DatasetBundle,
        state: ValidatorState,
        config: Mapping[str, Any],
    ) -> ValidationResult:
        validator_cfg = config.get("validators", {}).get("euler_stability", {})
        max_roll_var = float(validator_cfg.get("max_roll_var", 1.0e-3))
        max_pitch_var = float(validator_cfg.get("max_pitch_var", 1.0e-3))
        quaternions = state.get("normalized_quaternions", dataset.quaternions)
        euler_rad = quaternion_to_euler(quaternions)
        euler_deg = np.rad2deg(euler_rad)
        state["euler_rad"] = euler_rad
        state["euler_deg"] = euler_deg

        roll = euler_rad[:, 0]
        pitch = euler_rad[:, 1]
        yaw = euler_rad[:, 2]
        metrics = {
            "roll_var": float(np.var(roll)),
            "pitch_var": float(np.var(pitch)),
            "yaw_var": float(np.var(yaw)),
            "roll_range": float(np.ptp(roll)),
            "pitch_range": float(np.ptp(pitch)),
            "yaw_range": float(np.ptp(yaw)),
        }
        status = ValidationStatus.PASS
        if metrics["roll_var"] > max_roll_var or metrics["pitch_var"] > max_pitch_var:
            status = ValidationStatus.FAIL
        messages = [
            f"roll_var={metrics['roll_var']:.6e}",
            f"pitch_var={metrics['pitch_var']:.6e}",
        ]
        return ValidationResult(name=self.name, status=status, metrics=metrics, messages=messages)
