from __future__ import annotations

from pathlib import Path
from typing import Any

import cv2
import numpy as np
import torch
import yaml
from scipy.spatial.transform import Rotation


BUTTON_COLORS_BGR = [
    (32, 34, 218),
    (48, 142, 242),
    (87, 193, 77),
    (240, 196, 56),
    (164, 89, 206),
]


def _button_centers(num_buttons: int) -> np.ndarray:
    if num_buttons == 5:
        return np.array(
            [
                [-0.16, -0.10],
                [0.00, -0.11],
                [0.16, -0.09],
                [-0.11, 0.10],
                [0.12, 0.11],
            ],
            dtype=np.float64,
        )
    angles = np.linspace(0.0, 2.0 * np.pi, num_buttons, endpoint=False)
    radius = 0.16
    return np.stack([radius * np.cos(angles), radius * np.sin(angles)], axis=1)


def _smoothstep(values: np.ndarray) -> np.ndarray:
    values = np.clip(values, 0.0, 1.0)
    return values * values * (3.0 - 2.0 * values)


def _press_sequence(num_buttons: int, num_presses: int) -> np.ndarray:
    sequence = []
    cursor = 0
    while len(sequence) < num_presses:
        order = np.roll(np.arange(num_buttons), cursor % num_buttons)
        sequence.extend(order.tolist())
        cursor += 2
    return np.asarray(sequence[:num_presses], dtype=np.int64)


def generate_demo_pose(
    duration_sec: float,
    fps: int,
    num_buttons: int,
    num_presses: int,
) -> dict[str, Any]:
    num_frames = max(2, int(round(duration_sec * fps)))
    timestamps = np.linspace(0.0, duration_sec, num_frames, dtype=np.float64)
    button_centers = _button_centers(num_buttons)
    button_sequence = _press_sequence(num_buttons, num_presses)
    segment_edges = np.linspace(0.8, duration_sec - 0.8, num_presses + 1, dtype=np.float64)
    travel_ratio = 0.63
    hover_height = 0.205
    press_depth = 0.066

    xyz = np.zeros((num_frames, 3), dtype=np.float64)
    xyz[:, 2] = hover_height
    press_indices: list[int] = []
    current_xy = np.array([0.0, 0.0], dtype=np.float64)

    for segment_index, button_index in enumerate(button_sequence):
        start_time = segment_edges[segment_index]
        end_time = segment_edges[segment_index + 1]
        target_xy = button_centers[button_index].copy()
        target_xy += 0.0025 * np.array(
            [np.cos(segment_index * 0.9), np.sin(segment_index * 0.7)], dtype=np.float64
        )
        move_end = start_time + (end_time - start_time) * travel_ratio
        press_center = start_time + (end_time - start_time) * 0.82
        press_sigma = max((end_time - start_time) * 0.06, 0.04)
        segment_mask = (timestamps >= start_time) & (timestamps <= end_time)
        segment_times = timestamps[segment_mask]
        if len(segment_times) == 0:
            current_xy = target_xy
            continue

        blend = _smoothstep((segment_times - start_time) / max(move_end - start_time, 1.0e-6))
        xy_segment = current_xy[None, :] + (target_xy - current_xy)[None, :] * blend[:, None]
        xy_segment[segment_times >= move_end] = target_xy
        xy_segment += 0.0012 * np.column_stack(
            [
                np.sin(0.7 * segment_times + button_index),
                np.cos(0.5 * segment_times - button_index),
            ]
        )
        dip = press_depth * np.exp(-0.5 * ((segment_times - press_center) / press_sigma) ** 2)
        z_segment = hover_height + 0.003 * np.sin(0.35 * segment_times) - dip
        xyz[segment_mask, :2] = xy_segment
        xyz[segment_mask, 2] = z_segment

        press_index = int(np.argmin(np.abs(timestamps - press_center)))
        press_indices.append(press_index)
        current_xy = target_xy

    pre_mask = timestamps < segment_edges[0]
    if np.any(pre_mask):
        first_index = int(np.flatnonzero(~pre_mask)[0])
        first_xy = xyz[first_index, :2]
        pre_blend = _smoothstep(timestamps[pre_mask] / max(segment_edges[0], 1.0e-6))
        xyz[pre_mask, :2] = first_xy[None, :] * pre_blend[:, None]
        xyz[pre_mask, 2] = hover_height

    post_mask = timestamps > segment_edges[-1]
    if np.any(post_mask):
        last_index = int(np.flatnonzero(~post_mask)[-1])
        xyz[post_mask, :2] = xyz[last_index, :2]
        xyz[post_mask, 2] = hover_height + 0.0015 * np.sin(0.25 * timestamps[post_mask])

    roll = 0.005 * np.sin(0.25 * timestamps)
    pitch = 0.004 * np.cos(0.21 * timestamps)
    yaw = 0.08 * np.sin(0.18 * timestamps)
    euler = np.column_stack([roll, pitch, yaw])
    quaternions = Rotation.from_euler("xyz", euler, degrees=False).as_quat()
    pose = np.concatenate([xyz, quaternions], axis=1)
    return {
        "pose": pose,
        "timestamps": timestamps,
        "button_centers": button_centers,
        "press_indices": press_indices,
        "button_sequence": button_sequence.tolist(),
    }


def _xy_to_pixels(points_xy: np.ndarray, width: int, height: int) -> np.ndarray:
    margin = 52
    x_min = float(np.min(points_xy[:, 0])) - 0.06
    x_max = float(np.max(points_xy[:, 0])) + 0.06
    y_min = float(np.min(points_xy[:, 1])) - 0.06
    y_max = float(np.max(points_xy[:, 1])) + 0.06
    scale_x = (width - 2 * margin) / max(x_max - x_min, 1.0e-6)
    scale_y = (height - 2 * margin) / max(y_max - y_min, 1.0e-6)
    scale = min(scale_x, scale_y)
    pixels = np.empty_like(points_xy)
    pixels[:, 0] = margin + (points_xy[:, 0] - x_min) * scale
    pixels[:, 1] = height - margin - (points_xy[:, 1] - y_min) * scale
    return pixels


def write_demo_video(
    output_path: Path,
    fps: int,
    trajectory_xy: np.ndarray,
    button_centers_xy: np.ndarray,
    press_indices: list[int],
) -> None:
    width, height = 640, 640
    points_xy = np.vstack([trajectory_xy, button_centers_xy])
    trajectory_px = _xy_to_pixels(trajectory_xy, width, height)
    buttons_px = _xy_to_pixels(button_centers_xy, width, height)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(output_path), fourcc, float(fps), (width, height))
    if not writer.isOpened():
        raise RuntimeError(f"Failed to open video writer for {output_path}")

    press_lookup = set(press_indices)
    for frame_index, point in enumerate(trajectory_px):
        frame = np.full((height, width, 3), 244, dtype=np.uint8)
        frame[:] = (237, 242, 245)
        for grid in range(40, width, 80):
            cv2.line(frame, (grid, 30), (grid, height - 30), (220, 226, 230), 1)
            cv2.line(frame, (30, grid), (width - 30, grid), (220, 226, 230), 1)

        cv2.rectangle(frame, (28, 28), (width - 28, height - 28), (188, 200, 210), 2)
        for button_index, button_point in enumerate(buttons_px):
            color = BUTTON_COLORS_BGR[button_index % len(BUTTON_COLORS_BGR)]
            center = tuple(np.round(button_point).astype(int).tolist())
            cv2.circle(frame, center, 28, color, -1)
            cv2.circle(frame, center, 32, (33, 40, 48), 2)
            cv2.putText(
                frame,
                str(button_index + 1),
                (center[0] - 8, center[1] + 8),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.8,
                (255, 255, 255),
                2,
                cv2.LINE_AA,
            )

        trail_start = max(0, frame_index - 90)
        trail = trajectory_px[trail_start : frame_index + 1]
        for trail_point in trail:
            center = tuple(np.round(trail_point).astype(int).tolist())
            cv2.circle(frame, center, 2, (120, 132, 145), -1)

        current_center = tuple(np.round(point).astype(int).tolist())
        end_effector_color = (34, 87, 122) if frame_index not in press_lookup else (17, 138, 178)
        cv2.circle(frame, current_center, 11, end_effector_color, -1)
        cv2.circle(frame, current_center, 15, (10, 24, 35), 2)
        cv2.putText(
            frame,
            f"frame {frame_index:04d}",
            (36, 602),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.6,
            (52, 64, 76),
            2,
            cv2.LINE_AA,
        )
        writer.write(frame)

    writer.release()


def generate_demo_dataset(
    output_dir: Path,
    duration_sec: float,
    fps: int,
    num_buttons: int,
    num_presses: int,
) -> Path:
    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    demo = generate_demo_pose(
        duration_sec=duration_sec,
        fps=fps,
        num_buttons=num_buttons,
        num_presses=num_presses,
    )
    pose = demo["pose"]
    torch.save(torch.tensor(pose, dtype=torch.float32), output_dir / "endpose.pt")
    write_demo_video(
        output_path=output_dir / "video.mp4",
        fps=fps,
        trajectory_xy=pose[:, :2],
        button_centers_xy=demo["button_centers"],
        press_indices=demo["press_indices"],
    )
    meta = {
        "dataset_id": output_dir.name,
        "duration_sec": float(duration_sec),
        "fps": int(fps),
        "num_buttons": int(num_buttons),
        "num_presses": int(num_presses),
        "button_centers": np.asarray(demo["button_centers"], dtype=np.float64).round(6).tolist(),
        "press_indices": [int(index) for index in demo["press_indices"]],
        "button_sequence": [int(index) for index in demo["button_sequence"]],
    }
    with (output_dir / "meta.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump(meta, handle, sort_keys=False)
    return output_dir
