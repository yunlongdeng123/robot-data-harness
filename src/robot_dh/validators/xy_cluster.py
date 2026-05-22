from __future__ import annotations

from typing import Any, Mapping

import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from robot_dh.data.dataset import DatasetBundle
from robot_dh.validators.base import BaseValidator, ValidationResult, ValidationStatus, ValidatorState


class XYClusterValidator(BaseValidator):
    name = "xy_cluster"

    def validate(
        self,
        dataset: DatasetBundle,
        state: ValidatorState,
        config: Mapping[str, Any],
    ) -> ValidationResult:
        validator_cfg = config.get("validators", {}).get("xy_cluster", {})
        expected_num_buttons = int(validator_cfg.get("expected_num_buttons", 5))
        min_silhouette = float(validator_cfg.get("min_cluster_silhouette", 0.70))
        min_points_per_cluster = int(validator_cfg.get("min_points_per_cluster", 2))
        random_state = int(validator_cfg.get("random_state", 42))
        press_indices = np.asarray(state.get("press_indices", []), dtype=np.int64)
        if len(press_indices) < expected_num_buttons:
            return ValidationResult(
                name=self.name,
                status=ValidationStatus.FAIL,
                metrics={
                    "num_clusters": expected_num_buttons,
                    "cluster_centers": [],
                    "cluster_counts": [],
                    "cluster_radii": [],
                    "max_cluster_radius": 0.0,
                    "mean_cluster_radius": 0.0,
                    "silhouette_score": None,
                    "labels": [],
                },
                messages=[
                    f"Not enough press points for clustering: {len(press_indices)} < {expected_num_buttons}"
                ],
            )

        press_xy = dataset.xyz[press_indices, :2]
        model = KMeans(n_clusters=expected_num_buttons, n_init=20, random_state=random_state)
        labels = model.fit_predict(press_xy)
        centers = model.cluster_centers_
        counts = np.bincount(labels, minlength=expected_num_buttons)
        cluster_radii: list[float] = []
        for cluster_index in range(expected_num_buttons):
            cluster_points = press_xy[labels == cluster_index]
            distances = np.linalg.norm(cluster_points - centers[cluster_index], axis=1)
            cluster_radii.append(float(np.max(distances)) if len(distances) else 0.0)

        silhouette_value: float | None = None
        silhouette_message: str | None = None
        try:
            if len(press_xy) > expected_num_buttons and len(np.unique(labels)) > 1:
                silhouette_value = float(silhouette_score(press_xy, labels))
            else:
                silhouette_message = "Silhouette score skipped because press samples are too few"
        except Exception as exc:
            silhouette_message = f"Silhouette score failed: {exc}"

        state["press_xy"] = press_xy
        state["cluster_labels"] = labels
        state["cluster_centers"] = centers
        state["cluster_counts"] = counts
        state["cluster_radii"] = np.asarray(cluster_radii, dtype=np.float64)

        metrics = {
            "num_clusters": int(expected_num_buttons),
            "cluster_centers": centers.astype(float).tolist(),
            "cluster_counts": counts.astype(int).tolist(),
            "cluster_radii": cluster_radii,
            "max_cluster_radius": float(max(cluster_radii) if cluster_radii else 0.0),
            "mean_cluster_radius": float(np.mean(cluster_radii) if cluster_radii else 0.0),
            "silhouette_score": silhouette_value,
            "labels": labels.astype(int).tolist(),
        }

        status = ValidationStatus.PASS
        messages = [f"k={expected_num_buttons}"]
        if int(np.min(counts)) < min_points_per_cluster:
            status = ValidationStatus.FAIL
            messages.append(
                f"Each cluster requires at least {min_points_per_cluster} points, got counts {counts.tolist()}"
            )
        elif silhouette_value is None:
            status = ValidationStatus.WARN
            if silhouette_message is not None:
                messages.append(silhouette_message)
        elif silhouette_value < min_silhouette:
            status = ValidationStatus.FAIL
            messages.append(
                f"silhouette={silhouette_value:.3f} is below threshold {min_silhouette:.3f}"
            )
        else:
            messages.append(f"silhouette={silhouette_value:.3f}")
        return ValidationResult(name=self.name, status=status, metrics=metrics, messages=messages)
