"""BridgeData V2 contract：parquet shard。

v1.6.5 起穿透嵌套 schema：

- ``traj_len_p50/p95`` 优先用 probe 提供的 ``per_episode_lengths``（按 ``episode_idx``
  切分）；没有时回退到原先的 ``row_count`` 口径，保留与 LeRobot v1 转换的兼容性。
- ``language_missing_rate`` 优先用 probe 写入的 ``language_missing_rate`` 字段
  （顶层文本列 / nested ``state.language_embedding`` 二选一），否则按列名启发式兜底。
"""

from __future__ import annotations

from typing import Any

from robot_dh.qc.base import Rule
from robot_dh.qc.profile import AssetProfile

BRIDGE_RULES = [
    Rule(rule_id="parquet_valid_rate", metric="parquet_valid_rate", op=">=", threshold=0.95, severity="warn"),
    Rule(rule_id="num_parquet_files_min", metric="num_parquet_files", op=">=", threshold=1, severity="fail"),
    Rule(rule_id="action_column_coverage", metric="action_column_coverage", op=">", threshold=0.0, severity="warn"),
    Rule(rule_id="language_missing_rate", metric="language_missing_rate", op="<=", threshold=0.5, severity="warn"),
    # v1.6.7：bridge enrich 失败时 episode_count 必然 0；此条 fail rule 让 contract gate
    # 直接 FAIL，杜绝"_fill_bridge_metrics 静默失败 → traj_p50=row_count=314 失真 PASS"。
    Rule(rule_id="episode_count_min", metric="episode_count", op=">=", threshold=1, severity="fail"),
]

ACTION_COL_HINTS = ("action", "actions", "delta_action")
LANGUAGE_COL_HINTS = ("language", "language_instruction", "instruction", "task_description", "language_embedding")
IMAGE_COL_HINTS = ("image", "rgb", "primary_image")
ENV_COL_HINTS = ("environment", "env", "scene")
SKILL_COL_HINTS = ("skill", "task")


def bridge_metrics(profile: AssetProfile) -> dict[str, Any]:
    parquet = profile.profile.get("parquet") or []
    n = len(parquet) or 1
    readable = sum(1 for p in parquet if p.get("readable"))

    # 1) 合并所有 column path（顶层 + 嵌套）做 hints 匹配。
    all_columns: list[str] = []
    for p in parquet:
        all_columns.extend(p.get("schema_columns") or [])
        all_columns.extend(p.get("nested_columns") or [])
    columns_lower = [c.lower() for c in all_columns]

    has_action = any(any(h in c for h in ACTION_COL_HINTS) for c in columns_lower)
    has_language = any(any(h in c for h in LANGUAGE_COL_HINTS) for c in columns_lower)
    has_image = any(any(h in c for h in IMAGE_COL_HINTS) for c in columns_lower)
    has_env = any(any(h in c for h in ENV_COL_HINTS) for c in columns_lower)
    has_skill = any(any(h in c for h in SKILL_COL_HINTS) for c in columns_lower)

    # 2) traj_len：只用 per_episode_lengths；enrich 失败时不再回退到 row_count。
    # v1.6.7：之前回退到 ``row_count`` 在 _fill_bridge_metrics 静默失败时把整 parquet
    # 当作 1 traj，导致 traj_p50=traj_p95=num_rows、episode_count=0 但 status PASS 的
    # dls4z 失真。core metric 算不出来必须以 ``traj_len_p50=0 + episode_count=0`` 显式
    # 暴露，让 episode_count_min rule (>=1) 把 contract 推到 FAIL。
    per_ep_lengths: list[int] = []
    enrich_failures: list[dict[str, Any]] = []
    for p in parquet:
        lens = p.get("per_episode_lengths")
        if lens:
            per_ep_lengths.extend(int(x) for x in lens if int(x) > 0)
        elif p.get("readable"):
            enrich_failures.append(
                {
                    "uri": p.get("uri"),
                    "error_type": p.get("bridge_metrics_error_type"),
                    "cause_type": p.get("bridge_metrics_cause_type"),
                    "error": p.get("bridge_metrics_error"),
                }
            )
    per_ep_lengths.sort()
    if per_ep_lengths:
        p50 = per_ep_lengths[len(per_ep_lengths) // 2]
        p95_idx = min(len(per_ep_lengths) - 1, int(len(per_ep_lengths) * 0.95))
        p95 = per_ep_lengths[p95_idx]
    else:
        p50 = p95 = 0

    # 3) language_missing_rate：优先 probe 的精确值（顶层文本 / nested embedding）。
    lang_rates = [
        float(p["language_missing_rate"])
        for p in parquet
        if p.get("language_missing_rate") is not None
    ]
    if lang_rates:
        language_missing_rate = float(sum(lang_rates) / len(lang_rates))
    else:
        language_missing_rate = 0.0 if has_language else 1.0

    out: dict[str, Any] = {
        "num_parquet_files": len(parquet),
        "parquet_valid_rate": float(readable) / n,
        "traj_len_p50": int(p50),
        "traj_len_p95": int(p95),
        "language_missing_rate": float(language_missing_rate),
        "image_ref_missing_rate": 0.0 if has_image else 1.0,
        "action_column_coverage": 1.0 if has_action else 0.0,
        "environment_count": 1 if has_env else 0,
        "skill_count": 1 if has_skill else 0,
        "episode_count": len(per_ep_lengths),
    }
    if enrich_failures:
        out["bridge_enrich_failure_count"] = len(enrich_failures)
        out["bridge_enrich_failures"] = enrich_failures[:5]
    return out
