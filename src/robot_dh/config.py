from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

import yaml


DEFAULT_CONFIG: dict[str, Any] = {
    "dataset": {
        "expected_pose_dim": 7,
        "default_fps": 30,
        "min_samples": 10,
    },
    "quality_gate": {
        "fail_on_warning": False,
    },
    "validators": {
        "quaternion": {
            "quat_norm_tol": 1.0e-5,
        },
        "euler_stability": {
            "max_roll_var": 1.0e-3,
            "max_pitch_var": 1.0e-3,
        },
        "velocity_jump": {
            "velocity_threshold_mps": 2.0,
        },
        "press_event": {
            "smooth_window": 21,
            "smooth_polyorder": 3,
            "press_min_distance_sec": 0.8,
            "press_prominence": None,
            "press_expected_min_count": 5,
            "press_expected_max_count": 100,
        },
        "xy_cluster": {
            "method": "kmeans",
            "expected_num_buttons": 5,
            "min_cluster_silhouette": 0.70,
            "min_points_per_cluster": 2,
            "random_state": 42,
        },
        "workspace_bbox": {
            "bbox_margin_ratio": 0.5,
            "max_outside_ratio": 0.10,
            "fail_on_excessive_outside": False,
        },
    },
    "reports": {
        "save_plots": True,
        "html": True,
        "json": True,
    },
}


def _deep_merge_dicts(base: dict[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dicts(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def load_yaml(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Config file must contain a YAML mapping: {path}")
    return payload


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    return _deep_merge_dicts(DEFAULT_CONFIG, load_yaml(config_path))


def validator_config(config: Mapping[str, Any], name: str) -> dict[str, Any]:
    validators = config.get("validators", {})
    if not isinstance(validators, Mapping):
        return {}
    settings = validators.get(name, {})
    return dict(settings) if isinstance(settings, Mapping) else {}
