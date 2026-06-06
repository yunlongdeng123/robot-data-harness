"""v1.8 promptC：SparkSQL local mode 离线宽表测试。

策略：
- pyspark 未安装时整文件 skip；不报错。
- 构造一份最小 parquet 输入（用 pandas + pyarrow，仓库默认依赖里都有）：
    fact_etl_run：1 个 SUCCEEDED + 1 个 FAILED 行
    fact_qc_rule_result：2 PASS + 1 WARN + 1 FAIL
    fact_workflow_step：1 SUCCEEDED + 1 FAILED
    dim_dataset：1 行
- 跑 build_quality_ads；
- 断言：
    1. DWS / ADS parquet 存在；
    2. DWS 行数 == 1（按 dataset_id, version 聚合）；
    3. ADS 行数 == 1；
    4. ads quality_score 在 [0, 100]；
    5. _manifest.json 字段齐；
- 如果 pyspark 已装但 JVM 启不来（比如缺 JDK），也 skip。
"""

from __future__ import annotations

import datetime as _dt
import json
import os
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark 未安装，跳过 SparkSQL optional 测试")

import pandas as pd  # noqa: E402  仓库默认依赖
import pyarrow as pa  # noqa: E402  仓库默认依赖
import pyarrow.parquet as pq  # noqa: E402


def _write_parquet(target_dir: Path, df: pd.DataFrame) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    out = target_dir / "part-0000.parquet"
    pq.write_table(pa.Table.from_pandas(df), str(out))


@pytest.fixture(scope="module")
def fake_warehouse_export(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("wh_export_for_spark")
    dt = _dt.date(2026, 5, 25)

    _write_parquet(
        root / "fact_etl_run",
        pd.DataFrame(
            {
                "run_key": ["r1", "r2"],
                "job_id": ["j1", "j2"],
                "run_id": ["run-1", "run-2"],
                "dataset_id": ["ds_a", "ds_a"],
                "version": ["v1", "v1"],
                "dataset_family": ["bridge", "bridge"],
                "phase": ["normalize", "qc"],
                "status": ["SUCCEEDED", "FAILED"],
                "duration_sec": [120.0, 60.0],
                "input_bytes": [1024, 2048],
                "output_bytes": [800, 0],
                "input_rows": [100, 100],
                "output_rows": [100, 0],
                "peak_memory_mb": [256.0, 256.0],
                "error_message": [None, "boom"],
                "archive_log_uri": [None, None],
                "dt": [dt, dt],
            }
        ),
    )

    _write_parquet(
        root / "fact_qc_rule_result",
        pd.DataFrame(
            {
                "rule_result_key": ["q1", "q2", "q3", "q4"],
                "run_id": ["run-1"] * 4,
                "contract_id": ["bridge_contract"] * 4,
                "dataset_id": ["ds_a"] * 4,
                "version": ["v1"] * 4,
                "dataset_family": ["bridge"] * 4,
                "rule_id": ["rule_a", "rule_b", "rule_c", "rule_a"],
                "severity": ["error"] * 4,
                "status": ["PASS", "PASS", "WARN", "FAIL"],
                "metric": ["null_rate"] * 4,
                "op": ["<="] * 4,
                "threshold_value": ["0.1"] * 4,
                "actual_value": ["0.05", "0.02", "0.15", "0.30"],
                "dt": [dt] * 4,
            }
        ),
    )

    _write_parquet(
        root / "fact_workflow_step",
        pd.DataFrame(
            {
                "step_key": ["s1", "s2"],
                "workflow_name": ["wf-1", "wf-1"],
                "workflow_namespace": ["robot-dh", "robot-dh"],
                "workflow_type": ["local-devscale", "local-devscale"],
                "step_name": ["normalize", "qc"],
                "template_name": ["normalize-template", "qc-template"],
                "pod_name": ["wf-1-1", "wf-1-2"],
                "phase": ["Succeeded", "Failed"],
                "dataset_id": ["ds_a", "ds_a"],
                "version": ["v1", "v1"],
                "dataset_family": ["bridge", "bridge"],
                "duration_sec": [12.0, 8.0],
                "exit_code": [0, 1],
                "container_reason": [None, "Error"],
                "archive_log_uri": [None, None],
                "archive_log_url": [None, None],
                "dt": [dt, dt],
            }
        ),
    )

    _write_parquet(
        root / "dim_dataset",
        pd.DataFrame(
            {
                "dataset_key": ["dataset:ds_a:v1"],
                "dataset_id": ["ds_a"],
                "version": ["v1"],
                "dataset_family": ["bridge"],
                "raw_uri": ["file:///tmp/raw"],
                "ods_uri": ["file:///tmp/ods"],
                "dwd_uri": ["file:///tmp/dwd"],
                "ads_uri": [None],
                "ml_ready_uri": [None],
                "latest_status": ["PASS"],
                "latest_quality_score": [88.0],
                "is_active": [True],
            }
        ),
    )

    return root


def _spark_runtime_ok() -> bool:
    """启一个最小 SparkSession 探测；失败（如缺 JDK）就让全文件 skip。"""
    try:
        from robot_dh.spark_jobs.session import build_local_spark_session

        spark = build_local_spark_session(app_name="robot-dh-spark-probe")
        spark.stop()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def _runtime_check() -> None:
    if not _spark_runtime_ok():
        pytest.skip("Spark runtime 不可用（可能缺 JDK / JVM 启动失败）")


def test_build_quality_ads_smoke(
    fake_warehouse_export: Path, tmp_path: Path, _runtime_check: None
) -> None:
    from robot_dh.spark_jobs.quality_ads import build_quality_ads

    output = tmp_path / "spark_ads"
    result = build_quality_ads(
        input_uri=f"file://{fake_warehouse_export}",
        output_uri=f"file://{output}",
        dt="2026-05-25",
    )

    res_dict = result.to_dict()
    assert res_dict["dt"] == "2026-05-25"
    assert res_dict["dws_row_count"] == 1
    assert res_dict["ads_row_count"] == 1
    for src in ("fact_etl_run", "fact_qc_rule_result", "fact_workflow_step", "dim_dataset"):
        assert res_dict["sources_present"][src] is True, f"{src} 应该被 Spark 成功加载"

    dws_dir = Path(res_dict["dws_path"])
    ads_dir = Path(res_dict["ads_path"])
    assert dws_dir.exists() and any(dws_dir.iterdir())
    assert ads_dir.exists() and any(ads_dir.iterdir())

    manifest = json.loads(Path(res_dict["manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["dt"] == "2026-05-25"
    assert manifest["sources_present"]["fact_etl_run"] is True
    assert len(manifest["outputs"]) == 2

    # 验证 ADS quality_score 落在 [0, 100]，alert_level 是合法字面量。
    # 注意：parquet 在 dt=YYYY-MM-DD 父目录下时，pq.read_table 会触发分区推断与 parquet 内
    # 列名冲突；用 ParquetFile.read() 直接读单文件 schema 避开 dataset 探测。
    parquet_path = next(ads_dir.glob("*.parquet"))
    table = pq.ParquetFile(str(parquet_path)).read()
    df = table.to_pandas()
    assert len(df) == 1
    qs = float(df["quality_score"].iloc[0])
    assert 0.0 <= qs <= 100.0
    assert df["alert_level"].iloc[0] in ("OK", "WARNING", "CRITICAL")
    # 这份 fixture 一定会有 fail（status=FAILED + qc=FAIL + wf=Failed），所以 alert 必 CRITICAL
    assert df["alert_level"].iloc[0] == "CRITICAL"


def test_build_quality_ads_empty_input(tmp_path: Path, _runtime_check: None) -> None:
    """全部 source 缺失时不抛错，输出仍为空表 parquet。"""
    from robot_dh.spark_jobs.quality_ads import build_quality_ads

    empty_root = tmp_path / "empty_input"
    empty_root.mkdir()
    output = tmp_path / "spark_ads_empty"
    result = build_quality_ads(
        input_uri=f"file://{empty_root}",
        output_uri=f"file://{output}",
        dt="2026-05-25",
    )
    res = result.to_dict()
    assert res["dws_row_count"] == 0
    assert res["ads_row_count"] == 0
    assert all(v is False for v in res["sources_present"].values())


def test_render_sql_rejects_unsafe_value() -> None:
    from robot_dh.spark_jobs.quality_ads import _render_sql

    with pytest.raises(ValueError):
        _render_sql("WHERE dt = '{{ dt }}'", {"dt": "2026-05-25'; DROP TABLE x; --"})


def test_render_sql_missing_placeholder_raises() -> None:
    from robot_dh.spark_jobs.quality_ads import _render_sql

    with pytest.raises(KeyError):
        _render_sql("WHERE dt = '{{ dt }}'", {"other": "x"})


def test_unsupported_scheme_raises() -> None:
    from robot_dh.spark_jobs.quality_ads import _normalize_uri_to_path

    with pytest.raises(ValueError):
        _normalize_uri_to_path("s3://bucket/key")


def test_etl_status_ok_warn_counted_as_success(
    tmp_path: Path, _runtime_check: None
) -> None:
    """守门：Spark DWS 必须把 OK / WARN 视为 success，与 runner.py / PG DML 对齐。

    历史漂移：Spark SQL 早期版本只认 status='SUCCEEDED'，但仓库内 ETL 实际写入
    'OK' / 'WARN'（cli.py: `return 0 if status in {OK, WARN} else 1`），
    导致 features 单步 WARN 把 etl_success_rate 拖到 0/67%，假阳性触发 CRITICAL。
    """
    from robot_dh.spark_jobs.quality_ads import build_quality_ads

    root = tmp_path / "wh_export_ok_warn"
    dt = _dt.date(2026, 5, 25)
    _write_parquet(
        root / "fact_etl_run",
        pd.DataFrame(
            {
                "run_key": ["r1", "r2", "r3"],
                "job_id": ["j1", "j2", "j3"],
                "run_id": ["run-1"] * 3,
                "dataset_id": ["ds_warn"] * 3,
                "version": ["v1"] * 3,
                "dataset_family": ["bridge"] * 3,
                "phase": ["normalize", "build_features", "build_ads"],
                "status": ["OK", "WARN", "OK"],
                "duration_sec": [1.0, 2.0, 3.0],
                "input_bytes": [10, 20, 30],
                "output_bytes": [10, 20, 30],
                "input_rows": [1, 1, 1],
                "output_rows": [1, 1, 1],
                "peak_memory_mb": [1.0, 1.0, 1.0],
                "error_message": [None, None, None],
                "archive_log_uri": [None, None, None],
                "dt": [dt, dt, dt],
            }
        ),
    )
    _write_parquet(
        root / "fact_qc_rule_result",
        pd.DataFrame(
            {
                "rule_result_key": ["q1"],
                "run_id": ["run-1"],
                "contract_id": ["c"],
                "dataset_id": ["ds_warn"],
                "version": ["v1"],
                "dataset_family": ["bridge"],
                "rule_id": ["contract_status"],
                "severity": ["error"],
                "status": ["PASS"],
                "metric": [None],
                "op": [None],
                "threshold_value": [None],
                "actual_value": [None],
                "dt": [dt],
            }
        ),
    )
    _write_parquet(
        root / "dim_dataset",
        pd.DataFrame(
            {
                "dataset_key": ["dataset:ds_warn:v1"],
                "dataset_id": ["ds_warn"],
                "version": ["v1"],
                "dataset_family": ["bridge"],
                "raw_uri": ["file:///tmp/raw"],
                "ods_uri": ["file:///tmp/ods"],
                "dwd_uri": ["file:///tmp/dwd"],
                "ads_uri": [None],
                "ml_ready_uri": [None],
                "latest_status": ["PASS"],
                "latest_quality_score": [80.0],
                "is_active": [True],
            }
        ),
    )

    output = tmp_path / "spark_ads_ok_warn"
    result = build_quality_ads(
        input_uri=f"file://{root}",
        output_uri=f"file://{output}",
        dt="2026-05-25",
    )
    res = result.to_dict()
    dws_dir = Path(res["dws_path"])
    parquet_path = next(dws_dir.glob("*.parquet"))
    df = pq.ParquetFile(str(parquet_path)).read().to_pandas()
    assert len(df) == 1
    row = df.iloc[0]
    assert int(row["etl_run_count"]) == 3
    assert int(row["etl_success_count"]) == 3, (
        "WARN 必须计入 etl_success_count；status=OK/WARN/OK 应该全 success"
    )
    assert float(row["etl_success_rate"]) == 1.0


def test_etl_running_state_excluded_from_denominator(
    tmp_path: Path, _runtime_check: None
) -> None:
    """守门：Spark DWS 必须把 RUNNING / PENDING / STARTED 从分母里剔除。

    历史漂移：normalize 早期版本会先写 status='RUNNING' 再 update，孤儿留在 PG。
    如果 Spark / PG 都把 RUNNING 算分母，3 OK + 1 RUNNING = 75% 触发 WARN/CRITICAL。
    """
    from robot_dh.spark_jobs.quality_ads import build_quality_ads

    root = tmp_path / "wh_export_running"
    dt = _dt.date(2026, 5, 25)
    _write_parquet(
        root / "fact_etl_run",
        pd.DataFrame(
            {
                "run_key": ["r1", "r2", "r3", "r4"],
                "job_id": ["j1", "j2", "j3", "j4"],
                "run_id": ["run-1"] * 4,
                "dataset_id": ["ds_run"] * 4,
                "version": ["v1"] * 4,
                "dataset_family": ["bridge"] * 4,
                "phase": ["normalize", "build_features", "build_ads", "normalize"],
                "status": ["OK", "OK", "OK", "RUNNING"],
                "duration_sec": [1.0, 2.0, 3.0, 0.0],
                "input_bytes": [10, 20, 30, 0],
                "output_bytes": [10, 20, 30, 0],
                "input_rows": [1, 1, 1, 0],
                "output_rows": [1, 1, 1, 0],
                "peak_memory_mb": [1.0, 1.0, 1.0, 0.0],
                "error_message": [None] * 4,
                "archive_log_uri": [None] * 4,
                "dt": [dt] * 4,
            }
        ),
    )
    _write_parquet(
        root / "fact_qc_rule_result",
        pd.DataFrame(
            {
                "rule_result_key": ["q1"],
                "run_id": ["run-1"],
                "contract_id": ["c"],
                "dataset_id": ["ds_run"],
                "version": ["v1"],
                "dataset_family": ["bridge"],
                "rule_id": ["contract_status"],
                "severity": ["error"],
                "status": ["PASS"],
                "metric": [None],
                "op": [None],
                "threshold_value": [None],
                "actual_value": [None],
                "dt": [dt],
            }
        ),
    )
    _write_parquet(
        root / "dim_dataset",
        pd.DataFrame(
            {
                "dataset_key": ["dataset:ds_run:v1"],
                "dataset_id": ["ds_run"],
                "version": ["v1"],
                "dataset_family": ["bridge"],
                "raw_uri": ["file:///tmp/raw"],
                "ods_uri": ["file:///tmp/ods"],
                "dwd_uri": ["file:///tmp/dwd"],
                "ads_uri": [None],
                "ml_ready_uri": [None],
                "latest_status": ["PASS"],
                "latest_quality_score": [80.0],
                "is_active": [True],
            }
        ),
    )

    output = tmp_path / "spark_ads_running"
    build_quality_ads(
        input_uri=f"file://{root}",
        output_uri=f"file://{output}",
        dt="2026-05-25",
    )
    dws_dir = output / "dws_dataset_quality_daily" / "dt=2026-05-25"
    parquet_path = next(dws_dir.glob("*.parquet"))
    df = pq.ParquetFile(str(parquet_path)).read().to_pandas()
    assert len(df) == 1
    row = df.iloc[0]
    assert int(row["etl_run_count"]) == 3, (
        "RUNNING/PENDING/STARTED 必须被剔除；3 OK + 1 RUNNING 应该只算 3 个 run"
    )
    assert int(row["etl_success_count"]) == 3
    assert float(row["etl_success_rate"]) == 1.0


def test_dt_partition_dir_does_not_override_row_dt(
    tmp_path: Path, _runtime_check: None
) -> None:
    """守门：warehouse export 的 `dt=<export_date>/` 子目录不得被 Spark 当 partition column。

    warehouse export 用 `dt=YYYY-MM-DD/` 表示"导出于哪一天"——历史 fact 行的 dt 列
    可能与子目录名不一致（bridgedata_v2_scale30 真实 dt=2026-05-25，但被昨天的 export
    任务写到了 dt=2026-05-26/ 下）。默认 partition discovery 会把目录名覆盖行级 dt 列，
    导致跨日数据被错误归类。`_try_load_parquet` 用 recursiveFileLookup=true 关闭这一行为。
    """
    from robot_dh.spark_jobs.quality_ads import build_quality_ads

    root = tmp_path / "wh_export_dt_dir"
    row_dt = _dt.date(2026, 5, 25)
    target_dt = "2026-05-26"

    _write_parquet(
        root / "fact_etl_run" / f"dt={target_dt}",
        pd.DataFrame(
            {
                "run_key": ["r1"],
                "job_id": ["j1"],
                "run_id": ["run-1"],
                "dataset_id": ["ds_old"],
                "version": ["v1"],
                "dataset_family": ["bridge"],
                "phase": ["normalize"],
                "status": ["OK"],
                "duration_sec": [1.0],
                "input_bytes": [10],
                "output_bytes": [10],
                "input_rows": [1],
                "output_rows": [1],
                "peak_memory_mb": [1.0],
                "error_message": [None],
                "archive_log_uri": [None],
                "dt": [row_dt],
            }
        ),
    )
    _write_parquet(
        root / "fact_qc_rule_result" / f"dt={target_dt}",
        pd.DataFrame(
            {
                "rule_result_key": ["q1"],
                "run_id": ["run-1"],
                "contract_id": ["c"],
                "dataset_id": ["ds_old"],
                "version": ["v1"],
                "dataset_family": ["bridge"],
                "rule_id": ["contract_status"],
                "severity": ["error"],
                "status": ["PASS"],
                "metric": [None],
                "op": [None],
                "threshold_value": [None],
                "actual_value": [None],
                "dt": [row_dt],
            }
        ),
    )
    _write_parquet(
        root / "dim_dataset" / f"dt={target_dt}",
        pd.DataFrame(
            {
                "dataset_key": ["dataset:ds_old:v1"],
                "dataset_id": ["ds_old"],
                "version": ["v1"],
                "dataset_family": ["bridge"],
                "raw_uri": ["file:///tmp/raw"],
                "ods_uri": ["file:///tmp/ods"],
                "dwd_uri": ["file:///tmp/dwd"],
                "ads_uri": [None],
                "ml_ready_uri": [None],
                "latest_status": ["PASS"],
                "latest_quality_score": [80.0],
                "is_active": [True],
            }
        ),
    )

    output = tmp_path / "spark_ads_dt_dir"
    result = build_quality_ads(
        input_uri=f"file://{root}",
        output_uri=f"file://{output}",
        dt=target_dt,
    )
    res = result.to_dict()
    assert res["dws_row_count"] == 0, (
        f"目录名 dt={target_dt} 不能覆盖行级 dt={row_dt}；昨日数据不应被算到今天的 DWS"
    )
