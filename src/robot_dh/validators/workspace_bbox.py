from __future__ import annotations

from typing import Any, Mapping

import numpy as np

from robot_dh.data.dataset import DatasetBundle
from robot_dh.validators.base import BaseValidator, ValidationResult, ValidationStatus, ValidatorState


class WorkspaceBBoxValidator(BaseValidator):
    name = "workspace_bbox"

    def validate(
        self,
        dataset: DatasetBundle,
        state: ValidatorState,
        config: Mapping[str, Any],
    ) -> ValidationResult:
        validator_cfg = config.get("validators", {}).get("workspace_bbox", {})
        bbox_margin_ratio = float(validator_cfg.get("bbox_margin_ratio", 0.5))
        max_outside_ratio = float(validator_cfg.get("max_outside_ratio", 0.10))
        fail_on_excessive_outside = bool(validator_cfg.get("fail_on_excessive_outside", False))

        centers = np.asarray(state.get("cluster_centers", []), dtype=np.float64)
        if centers.size == 0:
            return ValidationResult(
                name=self.name,
                status=ValidationStatus.FAIL,
                metrics={
                    "trajectory_bbox": dataset.xyz[:, :2].astype(float).tolist() if len(dataset.xyz) == 1 else [],
                    "button_bbox": [],
                    "expanded_button_bbox": [],
                    "outside_ratio": 1.0,
                },
                messages=["Cluster centers are required before workspace bbox validation"],
            )

        trajectory_xy = dataset.xyz[:, :2]
        trajectory_bbox = [
            float(np.min(trajectory_xy[:, 0])),
            float(np.min(trajectory_xy[:, 1])),
            float(np.max(trajectory_xy[:, 0])),
            float(np.max(trajectory_xy[:, 1])),
        ]
        button_bbox = [
            float(np.min(centers[:, 0])),
            float(np.min(centers[:, 1])),
            float(np.max(centers[:, 0])),
            float(np.max(centers[:, 1])),
        ]
        span_x = button_bbox[2] - button_bbox[0]
        span_y = button_bbox[3] - button_bbox[1]
        margin = max(max(span_x, span_y) * bbox_margin_ratio, 1.0e-6)
        expanded_button_bbox = [
            button_bbox[0] - margin,
            button_bbox[1] - margin,
            button_bbox[2] + margin,
            button_bbox[3] + margin,
        ]

        outside_mask = (
            (trajectory_xy[:, 0] < expanded_button_bbox[0])
            | (trajectory_xy[:, 0] > expanded_button_bbox[2])
            | (trajectory_xy[:, 1] < expanded_button_bbox[1])
            | (trajectory_xy[:, 1] > expanded_button_bbox[3])
        )
        outside_ratio = float(np.mean(outside_mask)) if len(outside_mask) else 0.0
        state["trajectory_bbox"] = trajectory_bbox
        state["button_bbox"] = button_bbox
        state["expanded_button_bbox"] = expanded_button_bbox

        metrics = {
            "trajectory_bbox": trajectory_bbox,
            "button_bbox": button_bbox,
            "expanded_button_bbox": expanded_button_bbox,
            "outside_ratio": outside_ratio,
        }
        status = ValidationStatus.PASS
        messages = [f"outside_ratio={outside_ratio:.3f}"]
        if outside_ratio > max_outside_ratio:
            status = ValidationStatus.FAIL if fail_on_excessive_outside else ValidationStatus.WARN
            messages.append(
                f"outside_ratio={outside_ratio:.3f} exceeded threshold {max_outside_ratio:.3f}"
            )
        return ValidationResult(name=self.name, status=status, metrics=metrics, messages=messages)
