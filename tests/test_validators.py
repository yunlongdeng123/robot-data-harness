from __future__ import annotations

from pathlib import Path

import numpy as np

from robot_dh.config import load_config
from robot_dh.data.dataset import DatasetBundle, VideoMetadata
from robot_dh.validators.euler_stability import EulerStabilityValidator
from robot_dh.validators.press_event import PressEventValidator
from robot_dh.validators.quaternion import QuaternionValidator
from robot_dh.validators.velocity_jump import VelocityJumpValidator
from robot_dh.validators.xy_cluster import XYClusterValidator


def make_bundle(
    pose: np.ndarray,
    *,
    fps: float = 30.0,
    dataset_id: str = "validator-test",
    meta: dict | None = None,
) -> DatasetBundle:
    timestamps = np.arange(pose.shape[0], dtype=np.float64) / fps
    duration_sec = float(timestamps[-1]) if len(timestamps) else 0.0
    return DatasetBundle(
        dataset_id=dataset_id,
        dataset_path=Path("/tmp") / dataset_id,
        endpose_path=Path("/tmp") / dataset_id / "endpose.pt",
        pose=pose,
        timestamps=timestamps,
        dt=1.0 / fps,
        video_meta=VideoMetadata(
            fps=fps,
            frame_count=pose.shape[0],
            duration_sec=duration_sec,
            source="fps",
        ),
        meta=meta or {},
    )


def identity_quaternions(num_samples: int) -> np.ndarray:
    return np.tile(np.array([[0.0, 0.0, 0.0, 1.0]], dtype=np.float64), (num_samples, 1))


def test_quaternion_validator_passes_for_unit_quaternions() -> None:
    num_samples = 20
    xyz = np.zeros((num_samples, 3), dtype=np.float64)
    pose = np.concatenate([xyz, identity_quaternions(num_samples)], axis=1)
    result = QuaternionValidator().validate(make_bundle(pose), {}, load_config())
    assert result.status.value == "PASS"
    assert result.metrics["num_bad_quaternions"] == 0


def test_quaternion_validator_fails_for_bad_norms() -> None:
    num_samples = 20
    xyz = np.zeros((num_samples, 3), dtype=np.float64)
    quaternions = identity_quaternions(num_samples)
    quaternions[5] = np.array([0.0, 0.0, 0.0, 2.0])
    pose = np.concatenate([xyz, quaternions], axis=1)
    result = QuaternionValidator().validate(make_bundle(pose), {}, load_config())
    assert result.status.value == "FAIL"
    assert result.metrics["num_bad_quaternions"] == 1


def test_velocity_jump_validator_detects_jumps() -> None:
    xyz = np.zeros((12, 3), dtype=np.float64)
    xyz[:, 0] = np.linspace(0.0, 0.2, 12)
    xyz[7, 0] = 5.0
    pose = np.concatenate([xyz, identity_quaternions(12)], axis=1)
    result = VelocityJumpValidator().validate(make_bundle(pose), {}, load_config())
    assert result.status.value == "FAIL"
    assert result.metrics["num_jump_points"] >= 1


def test_velocity_jump_validator_passes_smooth_motion() -> None:
    xyz = np.zeros((24, 3), dtype=np.float64)
    xyz[:, 0] = np.linspace(0.0, 0.3, 24)
    pose = np.concatenate([xyz, identity_quaternions(24)], axis=1)
    result = VelocityJumpValidator().validate(make_bundle(pose), {}, load_config())
    assert result.status.value == "PASS"


def test_press_event_validator_detects_local_minima() -> None:
    fps = 20.0
    num_samples = 240
    timestamps = np.arange(num_samples, dtype=np.float64) / fps
    xyz = np.zeros((num_samples, 3), dtype=np.float64)
    xyz[:, 2] = 0.22 + 0.004 * np.sin(0.2 * timestamps)
    for center in (40, 90, 140, 190, 220):
        xyz[:, 2] -= 0.06 * np.exp(-0.5 * ((np.arange(num_samples) - center) / 3.0) ** 2)
    pose = np.concatenate([xyz, identity_quaternions(num_samples)], axis=1)
    bundle = make_bundle(pose, fps=fps, meta={"num_presses": 5})
    result = PressEventValidator().validate(bundle, {}, load_config())
    assert result.status.value == "PASS"
    assert result.metrics["detected_press_count"] == 5


def test_xy_cluster_validator_finds_five_clusters() -> None:
    rng = np.random.default_rng(7)
    centers = np.array(
        [
            [-0.2, -0.1],
            [0.0, -0.1],
            [0.2, -0.1],
            [-0.1, 0.12],
            [0.12, 0.14],
        ],
        dtype=np.float64,
    )
    press_xy = np.vstack([center + 0.005 * rng.normal(size=(3, 2)) for center in centers])
    xyz = np.zeros((press_xy.shape[0] + 20, 3), dtype=np.float64)
    xyz[: press_xy.shape[0], :2] = press_xy
    xyz[press_xy.shape[0] :, :2] = np.linspace(centers.min(axis=0), centers.max(axis=0), 20)
    pose = np.concatenate([xyz, identity_quaternions(xyz.shape[0])], axis=1)
    state = {"press_indices": np.arange(press_xy.shape[0], dtype=np.int64)}
    result = XYClusterValidator().validate(make_bundle(pose), state, load_config())
    assert result.status.value == "PASS"
    assert result.metrics["num_clusters"] == 5
    assert len(result.metrics["cluster_centers"]) == 5


def test_euler_stability_validator_passes_small_variance() -> None:
    from scipy.spatial.transform import Rotation

    num_samples = 100
    xyz = np.zeros((num_samples, 3), dtype=np.float64)
    angles = np.column_stack(
        [
            0.003 * np.sin(np.linspace(0.0, 1.0, num_samples)),
            0.003 * np.cos(np.linspace(0.0, 1.0, num_samples)),
            0.08 * np.sin(np.linspace(0.0, 2.0, num_samples)),
        ]
    )
    quaternions = Rotation.from_euler("xyz", angles, degrees=False).as_quat()
    pose = np.concatenate([xyz, quaternions], axis=1)
    result = EulerStabilityValidator().validate(make_bundle(pose), {}, load_config())
    assert result.status.value == "PASS"
