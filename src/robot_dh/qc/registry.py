"""contract registry：family -> (rules, metric_fn)。"""

from __future__ import annotations

from typing import Callable

from robot_dh.qc.base import Rule
from robot_dh.qc.bridge import BRIDGE_RULES, bridge_metrics
from robot_dh.qc.droid import DROID_RULES, droid_metrics
from robot_dh.qc.profile import AssetProfile
from robot_dh.qc.robomimic import ROBOMIMIC_RULES, robomimic_metrics
from robot_dh.qc.universal import UNIVERSAL_RULES, universal_metrics


MetricFn = Callable[[AssetProfile], dict]

_RUNNERS: dict[str, tuple[list[Rule], MetricFn, str]] = {
    "universal": (UNIVERSAL_RULES, universal_metrics, "universal_v1"),
    "droid": (DROID_RULES, droid_metrics, "droid_multimodal_v1"),
    "lerobot": (DROID_RULES, droid_metrics, "lerobot_multimodal_v1"),
    "robomimic": (ROBOMIMIC_RULES, robomimic_metrics, "robomimic_hdf5_v1"),
    "bridge": (BRIDGE_RULES, bridge_metrics, "bridgedata_v2_v1"),
}


def list_contracts() -> list[dict]:
    return [
        {"contract_id": cid, "dataset_family": fam, "rules": [r.to_dict() for r in rules]}
        for fam, (rules, _fn, cid) in _RUNNERS.items()
    ]


def get_contract_runner(family: str) -> tuple[list[Rule], MetricFn, str]:
    family_lower = family.lower()
    if family_lower not in _RUNNERS:
        # 兜底：universal
        return _RUNNERS["universal"]
    return _RUNNERS[family_lower]
