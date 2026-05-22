"""robot-dh v1.4 warehouse 层。

PostgreSQL（测试可为 SQLite）登记数据湖元数据：
    lake_assets、etl_jobs、lineage_edges、dataset_versions、quality_snapshots

复用 v1.3 SQLAlchemy 引擎工厂；v1.4 在 warehouse.models.WarehouseBase 上增 5 张表。
"""

from robot_dh.warehouse.models import (
    DatasetVersionRow,
    EtlJobRow,
    LakeAssetRow,
    LineageEdgeRow,
    QualitySnapshotRow,
    WarehouseBase,
    ensure_lake_tables,
)
from robot_dh.warehouse.service import (
    WarehouseService,
    LakeMetadataUnavailableError,
)

__all__ = [
    "DatasetVersionRow",
    "EtlJobRow",
    "LakeAssetRow",
    "LineageEdgeRow",
    "QualitySnapshotRow",
    "WarehouseBase",
    "ensure_lake_tables",
    "WarehouseService",
    "LakeMetadataUnavailableError",
]
