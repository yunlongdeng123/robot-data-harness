from __future__ import annotations

from typing import Any, Mapping

import numpy as np

from robot_dh.data.dataset import DatasetBundle
from robot_dh.validators.base import BaseValidator, ValidationResult, ValidationStatus, ValidatorState


class SchemaValidator(BaseValidator):
    name = "schema"

    def validate(
        self,
        dataset: DatasetBundle,
        state: ValidatorState,
        config: Mapping[str, Any],
    ) -> ValidationResult:
        dataset_cfg = config.get("dataset", {})
        expected_dim = int(dataset_cfg.get("expected_pose_dim", 7))
        min_samples = int(dataset_cfg.get("min_samples", 10))

        pose = dataset.pose
        xyz = pose[:, :3]
        quaternions = pose[:, 3:7]
        messages: list[str] = []
        status = ValidationStatus.PASS

        if pose.ndim != 2 or pose.shape[1] != expected_dim:
            status = ValidationStatus.FAIL
            messages.append(f"Expected shape [N, {expected_dim}], got {tuple(pose.shape)}")

        if pose.shape[0] < min_samples:
            status = ValidationStatus.FAIL
            messages.append(f"Expected at least {min_samples} samples, got {pose.shape[0]}")

        if not np.isfinite(pose).all():
            status = ValidationStatus.FAIL
            messages.append("Pose array contains NaN or Inf values")

        metrics = {
            "n_samples": int(pose.shape[0]),
            "n_dims": int(pose.shape[1]),
            "xyz_min": xyz.min(axis=0).astype(float).tolist(),
            "xyz_max": xyz.max(axis=0).astype(float).tolist(),
            "xyz_mean": xyz.mean(axis=0).astype(float).tolist(),
            "q_min": quaternions.min(axis=0).astype(float).tolist(),
            "q_max": quaternions.max(axis=0).astype(float).tolist(),
            "q_mean": quaternions.mean(axis=0).astype(float).tolist(),
        }
        if not messages:
            messages.append(f"shape=({pose.shape[0]}, {pose.shape[1]})")
        state["schema"] = metrics
        return ValidationResult(name=self.name, status=status, metrics=metrics, messages=messages)
