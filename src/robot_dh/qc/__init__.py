"""v1.6.2 QC Contract Layer。

公开 API：
    run_contract       : 执行 contract -> ContractReport
    profile_dataset    : 仅扫描 asset profile
    list_contracts     : 列出已注册 contract 定义
    write_report       : 落地 contract_report.json / .html / asset_profile.json

不重写 v1.5 validator pipeline。本模块只做多源数据的 dataset-specific schema/temporal QC。
"""

from robot_dh.qc.base import (
    Rule,
    RuleResult,
    ContractReport,
    AssetProfile,
    Severity,
)
from robot_dh.qc.registry import list_contracts, get_contract_runner
from robot_dh.qc.contracts import run_contract, write_report
from robot_dh.qc.profile import profile_dataset

__all__ = [
    "Rule",
    "RuleResult",
    "ContractReport",
    "AssetProfile",
    "Severity",
    "list_contracts",
    "get_contract_runner",
    "run_contract",
    "write_report",
    "profile_dataset",
]
