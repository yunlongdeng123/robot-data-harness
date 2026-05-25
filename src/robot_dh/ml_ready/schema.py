"""feature_schema.json：导出 ml-ready dataset 列定义。"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pyarrow as pa


def build_feature_schema(df: pd.DataFrame) -> dict[str, Any]:
    """从 DataFrame 推断出 dtype 列表 + 简单语义说明。"""
    fields: list[dict[str, Any]] = []
    for name, dtype in df.dtypes.items():
        try:
            arrow_type = str(pa.from_numpy_dtype(dtype))
        except Exception:
            arrow_type = str(dtype)
        fields.append(
            {
                "name": name,
                "dtype": arrow_type,
                "nullable": True,
            }
        )
    return {
        "schema_version": "v1.6.3",
        "fields": fields,
        "row_count": int(len(df)),
    }
