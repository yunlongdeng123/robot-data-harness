from __future__ import annotations

from typing import Any, Mapping

import numpy as np
from scipy.signal import find_peaks, savgol_filter

from robot_dh.data.dataset import DatasetBundle
from robot_dh.validators.base import BaseValidator, ValidationResult, ValidationStatus, ValidatorState


def _resolve_window(length: int, preferred: int, polyorder: int) -> int:
    window = min(length, preferred)
    if window % 2 == 0:
        window -= 1
    if window <= polyorder:
        window = polyorder + 2 if (polyorder + 2) % 2 == 1 else polyorder + 3
    return window if window >= 5 and window <= length else 0


class PressEventValidator(BaseValidator):
    name = "press_event"

    def validate(
        self,
        dataset: DatasetBundle,
        state: ValidatorState,
        config: Mapping[str, Any],
    ) -> ValidationResult:
        validator_cfg = config.get("validators", {}).get("press_event", {})
        z_values = dataset.xyz[:, 2].astype(np.float64)
        smooth_window = int(validator_cfg.get("smooth_window", 21))
        smooth_polyorder = int(validator_cfg.get("smooth_polyorder", 3))
        min_distance_sec = float(validator_cfg.get("press_min_distance_sec", 0.8))
        expected_min_count = int(validator_cfg.get("press_expected_min_count", 5))
        expected_max_count = int(validator_cfg.get("press_expected_max_count", 100))
        exact_expected = validator_cfg.get("press_expected_count", dataset.meta.get("num_presses"))

        window = _resolve_window(len(z_values), smooth_window, smooth_polyorder)
        if window:
            z_smooth = savgol_filter(z_values, window_length=window, polyorder=smooth_polyorder)
        else:
            z_smooth = z_values.copy()

        z_range = float(np.ptp(z_smooth)) if len(z_smooth) else 0.0
        configured_prominence = validator_cfg.get("press_prominence")
        prominence = (
            float(configured_prominence)
            if configured_prominence is not None
            else max(z_range * 0.05, 1.0e-6)
        )
        min_distance_frames = max(1, int(round(min_distance_sec / max(dataset.dt, 1.0e-12))))
        press_indices, _ = find_peaks(-z_smooth, prominence=prominence, distance=min_distance_frames)

        low_z_cutoff = float(np.percentile(z_smooth, 60)) if len(z_smooth) else 0.0
        press_indices = press_indices[z_smooth[press_indices] <= low_z_cutoff]

        state["z_smooth"] = z_smooth
        state["press_indices"] = press_indices
        state["press_prominence"] = prominence

        press_count = int(len(press_indices))
        metrics = {
            "press_indices": press_indices.astype(int).tolist(),
            "press_times": dataset.timestamps[press_indices].astype(float).tolist(),
            "press_z_values": z_values[press_indices].astype(float).tolist(),
            "detected_press_count": press_count,
            "z_min": float(np.min(z_values)),
            "z_max": float(np.max(z_values)),
            "z_range": float(np.ptp(z_values)),
            "press_prominence": float(prominence),
        }

        status = ValidationStatus.PASS
        messages = [f"detected={press_count}"]
        if press_count < expected_min_count or press_count > expected_max_count:
            status = ValidationStatus.FAIL
            messages.append(
                f"Detected {press_count} presses outside expected range [{expected_min_count}, {expected_max_count}]"
            )
        elif exact_expected is not None and press_count != int(exact_expected):
            status = ValidationStatus.WARN
            messages.append(f"Detected {press_count} presses, expected {int(exact_expected)}")
        return ValidationResult(name=self.name, status=status, metrics=metrics, messages=messages)
