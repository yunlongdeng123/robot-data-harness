"""dataset_card：训练侧需要的人类可读 + 机读元数据。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_dataset_card_json(
    *,
    dataset_id: str,
    version: str,
    source_roots: dict[str, str | None],
    output_uri: str,
    num_train: int,
    num_val: int,
    num_test: int,
    dataset_families: list[str],
    quality_policy: dict[str, Any],
    lineage_uri: str,
    schema_uri: str,
    known_limitations: list[str] | None = None,
    generated_by: str = "robot-data-harness v1.6.3",
) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "version": version,
        "created_at": utcnow_iso(),
        "source_roots": source_roots,
        "output_uri": output_uri,
        "num_train": int(num_train),
        "num_val": int(num_val),
        "num_test": int(num_test),
        "dataset_families": list(dataset_families),
        "quality_policy": dict(quality_policy),
        "known_limitations": list(known_limitations or []),
        "lineage_uri": lineage_uri,
        "schema_uri": schema_uri,
        "generated_by": generated_by,
    }


def build_dataset_card_md(card: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"# Dataset card — {card['dataset_id']} {card['version']}")
    lines.append("")
    lines.append(f"- created_at: `{card['created_at']}`")
    lines.append(f"- output_uri: `{card['output_uri']}`")
    lines.append(f"- generated_by: `{card['generated_by']}`")
    lines.append("")
    lines.append("## 数量")
    lines.append("")
    lines.append(f"- train: **{card['num_train']}**")
    lines.append(f"- val:   **{card['num_val']}**")
    lines.append(f"- test:  **{card['num_test']}**")
    lines.append("")
    lines.append("## 数据来源")
    lines.append("")
    for k, v in (card.get("source_roots") or {}).items():
        lines.append(f"- {k}: `{v}`")
    lines.append("")
    lines.append("## 质量策略")
    lines.append("")
    qp = card.get("quality_policy") or {}
    lines.append(f"- quality_threshold: **{qp.get('quality_threshold')}**")
    lines.append(f"- excluded_status: `{qp.get('excluded_status')}`")
    if qp.get("family_filter"):
        lines.append(f"- family_filter: `{qp.get('family_filter')}`")
    if qp.get("min_episode_length") is not None:
        lines.append(f"- min_episode_length: {qp.get('min_episode_length')}")
    lines.append("")
    lines.append("## dataset_families")
    lines.append("")
    for f in card.get("dataset_families") or []:
        lines.append(f"- {f}")
    if card.get("known_limitations"):
        lines.append("")
        lines.append("## 已知限制")
        lines.append("")
        for it in card["known_limitations"]:
            lines.append(f"- {it}")
    lines.append("")
    lines.append("## Lineage / Schema")
    lines.append("")
    lines.append(f"- lineage_uri: `{card.get('lineage_uri')}`")
    lines.append(f"- schema_uri: `{card.get('schema_uri')}`")
    lines.append("")
    return "\n".join(lines)
