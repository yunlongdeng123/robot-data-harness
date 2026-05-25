"""dataset_card：字段齐全 + md 渲染合法。"""

from __future__ import annotations

from robot_dh.ml_ready.dataset_card import build_dataset_card_json, build_dataset_card_md


def test_dataset_card_fields() -> None:
    qp = {
        "quality_threshold": 80.0,
        "excluded_status": ["FAIL"],
        "split": {"train": 0.8, "val": 0.1, "test": 0.1},
        "family_filter": [],
        "min_episode_length": None,
        "exclude_failed_contract": True,
        "rules": [],
    }
    card = build_dataset_card_json(
        dataset_id="demo",
        version="v1",
        source_roots={"input_root": "runs/lake/dwd"},
        output_uri="runs/lake/ml-ready/demo/v1",
        num_train=80,
        num_val=10,
        num_test=10,
        dataset_families=["button_press"],
        quality_policy=qp,
        lineage_uri="runs/lake/ml-ready/demo/v1/lineage.json",
        schema_uri="runs/lake/ml-ready/demo/v1/feature_schema.json",
        known_limitations=["仅含 demo 数据"],
    )
    for k in (
        "dataset_id", "version", "created_at", "source_roots", "output_uri",
        "num_train", "num_val", "num_test", "dataset_families", "quality_policy",
        "lineage_uri", "schema_uri", "generated_by", "known_limitations",
    ):
        assert k in card
    md = build_dataset_card_md(card)
    assert "# Dataset card — demo v1" in md
    assert "**80**" in md
    assert "仅含 demo 数据" in md
