"""v1.6 partition：把单个大 dataset 切成多个 partition 以便 partial resume。

公开 API：
    plan_dataset_partitions(...)  : 给定 dataset URI -> PartitionPlan
    PartitionPlan / Partition     : 数据结构
    detect_dataset_family(...)    : 嗅探数据集类型
"""

from robot_dh.partition.models import (
    Partition,
    PartitionPlan,
    PartitionType,
)
from robot_dh.partition.planner import (
    detect_dataset_family,
    plan_dataset_partitions,
)

__all__ = [
    "Partition",
    "PartitionPlan",
    "PartitionType",
    "detect_dataset_family",
    "plan_dataset_partitions",
]
