# v1.8 Warehouse Metrics 设计文档

> 本文档解释 v1.8 数仓分层 DIM / FACT / DWS / ADS 的表结构、SQL build 顺序、指标口径与本地 SQLite 简化路径。
> 真源 schema：`postgres/migrations/006_v1_8_warehouse_quality_ops.sql`（远端 PostgreSQL）。
> 本地 SQLite 由 `WarehouseBase` 自动建表（`robot-dh warehouse init --apply-ddl`）。

## 1. 分层定位

| 层 | 角色 | 写入方 | 读取方 |
|---|---|---|---|
| DIM | 维度宽表，跨 dt 维护 dataset 最新画像 | `robot-dh warehouse build`（增量 UPSERT） | quality summary / sla check / ml-ready 入口 |
| FACT | 单次事件事实表，按 dt 物化 | `robot-dh warehouse build` 从 v1.5/v1.6 业务表增量物化 | DWS 聚合源；FastAPI 直查 |
| DWS | 日度宽表，按主键 (dt, ...) 聚合 | warehouse build dws 层 | ADS / quality summary / sla check |
| ADS | 应用层，含阈值判定 + alert_level | warehouse build ads 层 | quality report / FastAPI 看板 |

## 2. 15 张表速查

### 2.1 DIM 层（1 表）

`dim_dataset` —— dataset 维度宽表。

| 字段 | 类型 | 说明 |
|---|---|---|
| dataset_key | text PK | `'dataset:<dataset_id>:<version>'` |
| dataset_id / version | text | 与 v1.4 `dataset_versions` 对齐 |
| dataset_family | text | 取自最近一次 `quality_snapshots.metrics_json.dataset_family` |
| raw_uri / ods_uri / dwd_uri / ads_uri / ml_ready_uri | text | 各层根 URI；缺位为 NULL |
| latest_status | text | `quality_snapshots.quality_status` 或 `dataset_versions.status` |
| latest_quality_score | double | 最近一次 `quality_snapshots.quality_score` |

### 2.2 FACT 层（4 表）

| 表 | 来源 | 主键 | 关键字段 |
|---|---|---|---|
| `fact_etl_run` | `etl_perf_runs` + `etl_jobs` | `run_key = md5(job_id|run_id|phase|dataset_id|version)` | phase / status / duration_sec / input/output_bytes / peak_memory_mb |
| `fact_qc_rule_result` | `qc_contract_runs.failed_rules_json` / `warning_rules_json` 展开 + `contract_status` summary | `rule_result_key = md5(run_id|severity|rule_id|metric)` | rule_id / severity / status / metric / op / threshold / actual |
| `fact_workflow_step` | `workflow_steps` + `workflow_runs.workflow_type` | `step_key = md5(ns|workflow|step|pod)` | phase / duration / exit_code / container_reason / archive_log_uri |
| `fact_asset_profile` | `asset_profiles` | `asset_profile_key = md5(profile_id)` | rows / bytes / files_count / episodes_count / videos_count / null_rate |

**dt 列冗余存储**：所有 FACT 表都有 `dt date`（由 `started_at AT TIME ZONE 'UTC'::date` 推出），方便按天分区查询；事实真源是 `started_at / finished_at`。

### 2.3 DWS 层（3 表）

| 表 | 主键 | 指标 |
|---|---|---|
| `dws_dataset_quality_daily` | `(dt, dataset_id, version)` | qc_run_count / qc_pass_count / qc_warn_count / qc_fail_count / **qc_pass_rate**, etl_run_count / etl_success_count / **etl_success_rate**, workflow_count / workflow_success_count / **workflow_success_rate**, avg_quality_score, ml_ready_rows, total_input_bytes / total_output_bytes, **p95_etl_duration_sec / p95_workflow_step_duration_sec**, stale_heartbeat_count |
| `dws_rule_failure_daily` | `(dt, dataset_family, contract_id, rule_id, severity)` | run_count / pass_count / warn_count / fail_count / **fail_rate** |
| `dws_workflow_ops_daily` | `(dt, workflow_type)` | workflow_count / success_count / failed_count / running_count / **success_rate**, avg_duration_sec / **p95_duration_sec**, deadline_exceeded_count, oom_count, nonzero_exit_count |

### 2.4 ADS 层（2 表）

| 表 | 主键 | 字段 |
|---|---|---|
| `ads_quality_dashboard` | `(dt, dataset_id, version)` | **overall_status / quality_score**, qc_pass_rate / etl_success_rate / workflow_success_rate, top_failed_rule / top_failed_rule_count, p95_duration_sec, ml_ready_rows, raw_bytes / dwd_bytes, **alert_level / alert_reason** |
| `ads_workflow_ops_dashboard` | `(dt, workflow_type)` | 同 DWS + alert_level / alert_reason |

**口径**（在 `build_ads_quality_dashboard.sql` 与 builder.py 中实现）：

```text
overall_status = FAIL  if qc_pass_rate<0.8  OR etl_success_rate<0.8
                 WARN  if qc_pass_rate<0.95
                 PASS  otherwise

quality_score  = 100 * qc_pass_rate          * 0.5
              + 100 * etl_success_rate       * 0.3
              + 100 * workflow_success_rate  * 0.2

alert_level    = CRITICAL / WARN / OK（与 overall_status 同档）
alert_reason   = 'qc_pass_rate<0.8' / 'etl_success_rate<0.8' / 'qc_pass_rate<0.95' / NULL
```

### 2.5 Backfill / SLA（5 表）

| 表 | 主键 | 用途 |
|---|---|---|
| `backfill_plans` | `plan_id` | `robot-dh backfill plan` 写入；含 plan_json |
| `backfill_tasks` | `task_id` | 每个 (dataset, version, dt, phase) 一行；含 `recommended_command` |
| `sla_policies` | `policy_id` | `robot-dh sla check` 读 yaml 后 upsert |
| `sla_checks` | `check_id` | 每次 SLA 校验产物；含 missing_outputs_json |
| `dataset_partition_readiness` | `readiness_key` | "今日 dataset 是否齐"；与 `dataset_partitions` 互补（前者关注"到岗"，后者关注分片拆分） |

## 3. SQL build 顺序

`robot-dh warehouse build` 按下面顺序串接执行（10 个 DML 文件）：

```text
dim → fact → dws → ads
  1) build_dim_dataset.sql
  2) build_fact_etl_run.sql
  3) build_fact_qc_rule_result.sql
  4) build_fact_workflow_step.sql
  5) build_fact_asset_profile.sql
  6) build_dws_dataset_quality_daily.sql
  7) build_dws_rule_failure_daily.sql
  8) build_dws_workflow_ops_daily.sql
  9) build_ads_quality_dashboard.sql
 10) build_ads_workflow_ops_dashboard.sql
```

每个 SQL 都用 PostgreSQL `INSERT … ON CONFLICT (…) DO UPDATE` 做 UPSERT，单日重跑幂等。

## 4. 两套后端

| 后端 | 用途 | build 路径 |
|---|---|---|
| PostgreSQL | 远端生产 | 直接执行 `warehouse/sql/dml/*.sql` |
| SQLite | `make test` / 离线 demo | Python 端聚合（`WarehouseBuilder._sqlite_build_*`）；与 PostgreSQL SQL 同口径 |

> SQLite 简化路径只保证 **promptB 第十一节列出的"空数据也能跑通 + 关键指标算对"**；高级聚合（`PERCENTILE_CONT` / `jsonb_array_elements` / `LATERAL`）在 Python 端实现，单测可断言确定输出。

## 5. SQL 模板参数

所有 build SQL 接收 3 个参数（由 `SqlTemplateRunner.render` 渲染）：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `{{ schema }}` | `public`（可被 `ROBOT_DH_WAREHOUSE_SCHEMA` 覆盖） | 限定为 schema 前缀 |
| `{{ start_date }}` / `{{ end_date }}` | 单日 build 时同值；区间 build 时 from/to | 控制 dt 过滤区间 |

参数值会经过**危险字符校验**（`SqlExecutionError`：拒绝包含 `;`、单/双引号、`--`、`/*` 的值），防止 SQL injection。

## 6. 常见操作

```bash
# 远端 build
robot-dh warehouse build --date 2026-05-25 --layers dim,fact,dws,ads

# 仅 dws+ads 重跑（fact 已物化好的情况）
robot-dh warehouse build --date 2026-05-25 --layers dws,ads

# 区间 build（适合补数）
robot-dh warehouse build --from-date 2026-05-01 --to-date 2026-05-25

# dry-run 看渲染结果不写库
robot-dh warehouse build --date 2026-05-25 --dry-run

# 单 SQL 调试
robot-dh warehouse sql run --file warehouse/sql/dml/build_dim_dataset.sql --dt 2026-05-25 --dry-run
```

## 7. 与 v1.7 的边界

- v1.8 **只读** v1.7 业务表（`etl_perf_runs` / `qc_contract_runs` / `workflow_steps` / `asset_profiles` / `ml_ready_datasets` / `dataset_versions` / `quality_snapshots`）；不会修改 v1.7 已有写入路径。
- 当 v1.7 表缺失（如新部署的 infra 还没跑 005 迁移）时，`warehouse build` 走 PostgreSQL 路径会报 `relation "etl_perf_runs" does not exist`；正确处理：先 `./scripts/27_pg_apply_robot_platform_schema.sh`（v1.6 schema）再跑 v1.8。
- v1.8 写入的所有表均使用 **text 主键 + 复合主键 UPSERT**，没有 bigserial / sequence，不需要 GRANT USAGE/SELECT ON SEQUENCE。
