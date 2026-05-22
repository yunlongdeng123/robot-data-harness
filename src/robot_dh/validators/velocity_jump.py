from __future__ import annotations

from typing import Any, Mapping

import numpy as np

from robot_dh.data.dataset import DatasetBundle
from robot_dh.validators.base import BaseValidator, ValidationResult, ValidationStatus, ValidatorState


class VelocityJumpValidator(BaseValidator):
    name = "velocity_jump"

    def validate(
        self,
        dataset: DatasetBundle,
        state: ValidatorState,
        config: Mapping[str, Any],
    ) -> ValidationResult:
        validator_cfg = config.get("validators", {}).get("velocity_jump", {})
        threshold = float(validator_cfg.get("velocity_threshold_mps", 2.0))
        if dataset.pose.shape[0] < 2:
            velocities = np.array([], dtype=np.float64)
            jump_indices = np.array([], dtype=np.int64)
        else:
            delta_xyz = np.diff(dataset.xyz, axis=0)
            delta_d = np.linalg.norm(delta_xyz, axis=1)
            velocities = delta_d / max(dataset.dt, 1.0e-12)
            jump_indices = np.flatnonzero(velocities > threshold) + 1

        state["velocities"] = velocities
        state["velocity_threshold_mps"] = threshold
        state["jump_indices"] = jump_indices
        metrics = {
            "max_velocity_mps": float(np.max(velocities)) if velocities.size else 0.0,
            "mean_velocity_mps": float(np.mean(velocities)) if velocities.size else 0.0,
            "p95_velocity_mps": float(np.percentile(velocities, 95)) if velocities.size else 0.0,
            "num_jump_points": int(len(jump_indices)),
            "jump_indices": jump_indices.astype(int).tolist(),
            "jump_times": dataset.timestamps[jump_indices].astype(float).tolist() if len(jump_indices) else [],
        }
        status = ValidationStatus.PASS if metrics["max_velocity_mps"] <= threshold else ValidationStatus.FAIL
        messages = [f"max_velocity={metrics['max_velocity_mps']:.3f} m/s"]
        return ValidationResult(name=self.name, status=status, metrics=metrics, messages=messages)
