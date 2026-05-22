from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Rectangle

from robot_dh.data.dataset import DatasetBundle


def plot_z_press_events(dataset: DatasetBundle, state: Mapping[str, Any], output_path: Path) -> None:
    z_values = dataset.xyz[:, 2]
    z_smooth = np.asarray(state.get("z_smooth", z_values), dtype=np.float64)
    press_indices = np.asarray(state.get("press_indices", []), dtype=np.int64)

    fig, axis = plt.subplots(figsize=(12, 4.5))
    axis.plot(dataset.timestamps, z_values, color="#6c7a89", linewidth=1.2, label="Raw Z")
    axis.plot(dataset.timestamps, z_smooth, color="#0f4c5c", linewidth=2.0, label="Smoothed Z")
    if len(press_indices):
        axis.scatter(
            dataset.timestamps[press_indices],
            z_values[press_indices],
            color="#c1121f",
            s=36,
            zorder=5,
            label="Detected press events",
        )
    axis.set_title("Z-axis press event detection")
    axis.set_xlabel("Time (s)")
    axis.set_ylabel("Z")
    axis.grid(alpha=0.25)
    axis.legend(loc="best")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def plot_xy_clusters(dataset: DatasetBundle, state: Mapping[str, Any], output_path: Path) -> None:
    trajectory_xy = dataset.xyz[:, :2]
    press_xy = np.asarray(state.get("press_xy", []), dtype=np.float64)
    labels = np.asarray(state.get("cluster_labels", []), dtype=np.int64)
    centers = np.asarray(state.get("cluster_centers", []), dtype=np.float64)
    button_bbox = state.get("button_bbox")
    expanded_button_bbox = state.get("expanded_button_bbox")
    colors = ["#c1121f", "#003049", "#2a9d8f", "#f77f00", "#588157", "#6d597a"]

    fig, axis = plt.subplots(figsize=(7.8, 7.8))
    axis.scatter(
        trajectory_xy[:, 0],
        trajectory_xy[:, 1],
        s=8,
        color="#c7d3dd",
        alpha=0.65,
        label="Full XY trajectory",
    )
    for cluster_index in np.unique(labels):
        cluster_points = press_xy[labels == cluster_index]
        axis.scatter(
            cluster_points[:, 0],
            cluster_points[:, 1],
            s=34,
            color=colors[int(cluster_index) % len(colors)],
            label=f"Cluster {int(cluster_index) + 1}",
        )
    if len(centers):
        axis.scatter(
            centers[:, 0],
            centers[:, 1],
            s=160,
            marker="X",
            color="#1b1b1b",
            label="Cluster centers",
        )
    if button_bbox:
        axis.add_patch(
            Rectangle(
                (button_bbox[0], button_bbox[1]),
                button_bbox[2] - button_bbox[0],
                button_bbox[3] - button_bbox[1],
                fill=False,
                edgecolor="#0f4c5c",
                linewidth=2.0,
                linestyle="-",
                label="Button bbox",
            )
        )
    if expanded_button_bbox:
        axis.add_patch(
            Rectangle(
                (expanded_button_bbox[0], expanded_button_bbox[1]),
                expanded_button_bbox[2] - expanded_button_bbox[0],
                expanded_button_bbox[3] - expanded_button_bbox[1],
                fill=False,
                edgecolor="#f77f00",
                linewidth=2.0,
                linestyle="--",
                label="Expanded bbox",
            )
        )
    axis.set_title("XY trajectory and button clusters")
    axis.set_xlabel("X")
    axis.set_ylabel("Y")
    axis.axis("equal")
    axis.grid(alpha=0.2)
    handles, labels_text = axis.get_legend_handles_labels()
    seen: set[str] = set()
    filtered_handles = []
    filtered_labels = []
    for handle, label in zip(handles, labels_text, strict=False):
        if label in seen:
            continue
        seen.add(label)
        filtered_handles.append(handle)
        filtered_labels.append(label)
    axis.legend(filtered_handles, filtered_labels, loc="best", fontsize=8.5)
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def plot_euler_angles(dataset: DatasetBundle, state: Mapping[str, Any], output_path: Path) -> None:
    euler_deg = np.asarray(state.get("euler_deg", np.zeros((dataset.pose.shape[0], 3))), dtype=np.float64)
    fig, axes = plt.subplots(3, 1, figsize=(12, 8.5), sharex=True)
    series = (
        (euler_deg[:, 0], "Roll (deg)", "#003049"),
        (euler_deg[:, 1], "Pitch (deg)", "#2a9d8f"),
        (euler_deg[:, 2], "Yaw (deg)", "#c1121f"),
    )
    for axis, (values, ylabel, color) in zip(axes, series, strict=False):
        axis.plot(dataset.timestamps, values, color=color, linewidth=1.4)
        axis.set_ylabel(ylabel)
        axis.grid(alpha=0.25)
    axes[0].set_title("Euler angle stability")
    axes[-1].set_xlabel("Time (s)")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def plot_velocity_profile(dataset: DatasetBundle, state: Mapping[str, Any], output_path: Path) -> None:
    velocities = np.asarray(state.get("velocities", []), dtype=np.float64)
    jump_indices = np.asarray(state.get("jump_indices", []), dtype=np.int64)
    threshold = float(state.get("velocity_threshold_mps", 0.0))
    velocity_times = dataset.timestamps[1:] if len(dataset.timestamps) > 1 else np.array([], dtype=np.float64)

    fig, axis = plt.subplots(figsize=(12, 4.5))
    axis.plot(velocity_times, velocities, color="#003049", linewidth=1.6, label="Velocity")
    axis.axhline(threshold, color="#c1121f", linestyle="--", linewidth=1.4, label="Threshold")
    if len(jump_indices):
        axis.scatter(
            dataset.timestamps[jump_indices],
            velocities[jump_indices - 1],
            color="#f77f00",
            s=38,
            label="Jump points",
        )
    axis.set_title("Velocity profile")
    axis.set_xlabel("Time (s)")
    axis.set_ylabel("Velocity (m/s)")
    axis.grid(alpha=0.25)
    axis.legend(loc="best")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def write_plot_artifacts(
    dataset: DatasetBundle,
    state: Mapping[str, Any],
    output_dir: Path,
) -> dict[str, str]:
    plot_dir = output_dir / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    plot_z_press_events(dataset, state, plot_dir / "z_press_events.png")
    plot_xy_clusters(dataset, state, plot_dir / "xy_clusters.png")
    plot_euler_angles(dataset, state, plot_dir / "euler_angles.png")
    plot_velocity_profile(dataset, state, plot_dir / "velocity_profile.png")
    return {
        "z_press_events": "plots/z_press_events.png",
        "xy_clusters": "plots/xy_clusters.png",
        "euler_angles": "plots/euler_angles.png",
        "velocity_profile": "plots/velocity_profile.png",
    }
