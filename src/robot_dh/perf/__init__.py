"""v1.5 性能 profiler：包装 normalize / build-features / build-ads / etl run / etl scan 等阶段。"""

from robot_dh.perf.profiler import EtlProfiler, PerfRecord
from robot_dh.perf.writer import (
    emit_perf_records,
    perf_records_from_etl_run,
    write_perf_json,
    write_perf_record_to_db,
)

__all__ = [
    "EtlProfiler",
    "PerfRecord",
    "emit_perf_records",
    "perf_records_from_etl_run",
    "write_perf_json",
    "write_perf_record_to_db",
]
