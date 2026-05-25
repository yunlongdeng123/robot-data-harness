"""robomimic contract：fake hdf5。"""

from __future__ import annotations

from pathlib import Path

import h5py

from robot_dh.qc.contracts import run_contract


def _write_robomimic_hdf5(path: Path, n_demos: int = 3, with_actions: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as f:
        for i in range(n_demos):
            g = f.create_group(f"data/demo_{i}")
            if with_actions:
                g.create_dataset("actions", data=[[0.1, 0.2]] * 20)
            g.create_dataset("rewards", data=[1.0] * 20)
            g.create_dataset("dones", data=[0] * 20)


def test_robomimic_pass(tmp_path: Path) -> None:
    root = tmp_path / "robomimic/v1"
    _write_robomimic_hdf5(root / "low_dim.hdf5")
    _write_robomimic_hdf5(root / "image.hdf5")
    report, _ = run_contract(
        dataset_uri=root.as_posix(),
        dataset_family="robomimic",
        dataset_id="robomimic",
        version="v1",
    )
    # demo_count > 0 + actions present
    assert report.metrics["demo_count"] >= 6
    assert report.metrics["action_present_rate"] == 1.0


def test_robomimic_fail_when_actions_missing(tmp_path: Path) -> None:
    root = tmp_path / "robomimic/v1"
    _write_robomimic_hdf5(root / "no_actions.hdf5", with_actions=False)
    report, _ = run_contract(
        dataset_uri=root.as_posix(),
        dataset_family="robomimic",
        dataset_id="robomimic",
        version="v1",
    )
    # action_present_rate=0 -> warn-level 规则 -> WARN
    assert report.status in ("WARN", "FAIL")
    assert report.metrics["action_present_rate"] == 0.0
