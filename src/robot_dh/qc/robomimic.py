"""robomimic contract：HDF5 group 结构。

v1.6.6 起 ``episode_len_p50/p95`` 直接消费 ``probe_hdf5`` 写入的 ``episode_lens`` 列表
（每个 demo 的 ``actions.shape[0]``）；多 HDF5 文件做 concat 后再算 percentile，
避免老版"对每文件 p50 再 p50"的统计错位。
"""

from __future__ import annotations

from typing import Any

from robot_dh.qc.base import Rule
from robot_dh.qc.profile import AssetProfile

ROBOMIMIC_RULES = [
    Rule(rule_id="hdf5_valid_rate", metric="hdf5_valid_rate", op=">=", threshold=0.95, severity="fail"),
    Rule(rule_id="demo_count_min", metric="demo_count", op=">=", threshold=1, severity="fail"),
    Rule(rule_id="action_present_rate", metric="action_present_rate", op=">=", threshold=0.95, severity="warn"),
    Rule(rule_id="obs_next_obs_mismatch_rate", metric="obs_next_obs_mismatch_rate", op="<=", threshold=0.05, severity="warn"),
    Rule(rule_id="reward_done_length_mismatch_rate", metric="reward_done_length_mismatch_rate", op="<=", threshold=0.05, severity="warn"),
    Rule(rule_id="episode_len_p50_min", metric="episode_len_p50", op=">=", threshold=5, severity="warn",
         description="单 demo episode_len 中位数应 >=5"),
    Rule(rule_id="episode_len_p95_max", metric="episode_len_p95", op="<=", threshold=2000, severity="warn",
         description="单 demo episode_len p95 应 <=2000，防 outlier"),
]


def _percentile(values: list[int], pct: float) -> int:
    """简易 percentile：对已排序列表线性插值；空列表返回 0。"""
    if not values:
        return 0
    sorted_vals = sorted(int(v) for v in values)
    if pct <= 0:
        return sorted_vals[0]
    if pct >= 100:
        return sorted_vals[-1]
    rank = (len(sorted_vals) - 1) * (pct / 100.0)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = rank - lo
    return int(round(sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac))


def robomimic_metrics(profile: AssetProfile) -> dict[str, Any]:
    hdf5 = profile.profile.get("hdf5") or []
    valid = [h for h in hdf5 if h.get("readable")]
    n_total = len(hdf5) or 1
    demo_count_total = sum(int(h.get("demo_count") or 0) for h in valid)
    action_present_files = sum(1 for h in valid if h.get("has_actions"))
    obs_present = sum(1 for h in valid if h.get("has_obs"))
    next_obs_present = sum(1 for h in valid if h.get("has_next_obs"))
    rewards_present = sum(1 for h in valid if h.get("has_rewards"))
    dones_present = sum(1 for h in valid if h.get("has_dones"))

    n_valid = len(valid) or 1

    # episode_lens 全部 concat 再算 percentile；先按文件 percentile 再聚合会丢精度。
    all_lens: list[int] = []
    for h in valid:
        all_lens.extend(int(x) for x in (h.get("episode_lens") or []) if int(x) > 0)
    episode_len_p50 = _percentile(all_lens, 50)
    episode_len_p95 = _percentile(all_lens, 95)

    return {
        "hdf5_valid_rate": float(len(valid)) / n_total,
        "demo_count": int(demo_count_total),
        "num_hdf5_files": int(len(hdf5)),
        "action_present_rate": float(action_present_files) / n_valid,
        "obs_next_obs_mismatch_rate": float(abs(obs_present - next_obs_present)) / n_valid,
        "reward_done_length_mismatch_rate": float(abs(rewards_present - dones_present)) / n_valid,
        "action_range_violation_rate": 0.0,
        "episode_len_p50": episode_len_p50,
        "episode_len_p95": episode_len_p95,
        "episode_len_min": min(all_lens) if all_lens else 0,
        "episode_len_max": max(all_lens) if all_lens else 0,
        "episode_count_total": len(all_lens),
    }
