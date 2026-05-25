"""HDF5 探针：读取 group / dataset 结构与基本字段（针对 robomimic）。

v1.6.5 起出错时把 ``error_type / cause_type`` 一并写到返回字典，停止吞成 "Max Retries
Exceeded" 这种没法排障的字串。HDF5 没有靠谱的 cloud-native reader（fsspec 太慢、ROS3
要 conda 装、h5coro 是 TB 级用的），所以这里只处理本地路径；S3 上的 HDF5 由
``profile.py`` 那一层用 boto3 + materialize-first 下载到 /tmp 再调本函数。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import h5py

LOG = logging.getLogger(__name__)


def _summarize_exception(err: BaseException) -> dict[str, Any]:
    """复用 parquet_probe 里的 cause 链解析（优先 __cause__，缺失 fallback __context__）。"""
    from robot_dh.qc.parquet_probe import _summarize_exception as _shared

    return _shared(err)


def probe_hdf5(path: Path) -> dict[str, Any]:
    """对 robomimic HDF5 取 demo / action shape；v1.6.6 起补 episode_lens 列表。

    每个 ``data/demo_X`` group 的 ``actions.shape[0]`` 就是该 demo 的 episode_len。
    只读 h5py dataset 的 ``shape`` metadata，不实际读 byte，单文件耗时 < 50ms。
    """
    out: dict[str, Any] = {
        "uri": path.as_posix(),
        "size_bytes": int(path.stat().st_size),
        "readable": False,
        "demo_count": 0,
        "demo_keys": [],
        "has_actions": False,
        "has_obs": False,
        "has_next_obs": False,
        "has_rewards": False,
        "has_dones": False,
        "episode_lens": [],
    }
    try:
        with h5py.File(path, "r") as f:
            out["readable"] = True
            data = f.get("data")
            if data is not None and isinstance(data, h5py.Group):
                demos = sorted(k for k in data.keys() if k.startswith("demo_"))
                out["demo_count"] = len(demos)
                out["demo_keys"] = demos[:10]
                if demos:
                    head = data[demos[0]]
                    out["has_actions"] = "actions" in head
                    out["has_obs"] = "obs" in head
                    out["has_next_obs"] = "next_obs" in head
                    out["has_rewards"] = "rewards" in head
                    out["has_dones"] = "dones" in head
                    head_actions = head.get("actions") if out["has_actions"] else None
                    if head_actions is not None and hasattr(head_actions, "shape") and len(head_actions.shape) >= 1:
                        out["actions_shape"] = list(head_actions.shape)
                    # 遍历所有 demo group 取 episode_len（actions.shape[0]）。
                    if out["has_actions"]:
                        lens: list[int] = []
                        for name in demos:
                            g = data.get(name)
                            if not isinstance(g, h5py.Group):
                                continue
                            actions = g.get("actions")
                            if actions is None or not hasattr(actions, "shape") or len(actions.shape) < 1:
                                continue
                            lens.append(int(actions.shape[0]))
                        out["episode_lens"] = lens
            else:
                # 非 robomimic 的 HDF5：至少返回 top-level keys 给上层做粗判
                out["top_level_groups"] = list(f.keys())
    except Exception as err:  # noqa: BLE001
        out.update(_summarize_exception(err))
    return out
