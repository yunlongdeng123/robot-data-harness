# robot-dh-infra v1.6 元数据 schema 设计

本文档说明 `postgres/migrations/005_v1_6_robot_platform.sql` 中新增的 9 张表用途、字段语义、与 v1.4 / v1.5 既有表的关系，以及由哪一类组件写入 / 读取。

## 0. 总览

| 表 | 用途 | 写入端 | 读取端 |
|----|------|--------|--------|
| `qc_contracts` | 数据集族级别的 QC 规则定义 | 主项目 Python CLI / FastAPI | exporter / dashboard |
| `qc_contract_runs` | 单次 QC contract 执行结果 | 主项目 ETL / FastAPI | exporter / dashboard |
| `workflow_runs` | v1.6 通用 workflow run 元数据 | Argo sync 脚本 / 主项目 CLI | exporter / report |
| `workflow_steps` | workflow 内 step 级状态 | Argo sync 脚本 | exporter / report |
| `asset_profiles` | 单个 asset 的画像 | 主项目 ETL（profile_asset task） | exporter |
| `ml_ready_datasets` | 训练就绪 dataset 元数据 | 主项目 ETL（build_ml_ready） | training pipeline / exporter |
| `dataset_partitions` | 按 episode / time / size 的分片登记 | 主项目 ETL（plan_partition） | normalize / resume worker |
| `task_heartbeats` | 长任务运行时心跳 | 主项目 worker（normalize / feature / contract） | report / monitoring |
| `openlineage_events` | OpenLineage 风格统一事件表 | 主项目 ETL 通过 OpenLineage emitter | exporter / external lineage tool |

> 本仓库（`robot-dh-infra`）只负责建表、授权与基础设施 smoke。所有业务写入由 `robot-data-harness` 主项目完成；exporter / report 既可以是主项目内置的 FastAPI，也可以是后续添加的 Go exporter。

## 1. 与 v1.4 / v1.5 既有表的关系

```
v1.4 (lake 基础)
├── lake_assets                    └── 与 asset_profiles 通过 (asset_uri) 关联
├── etl_jobs                       └── 与 workflow_runs 通过 (job_id ↔ parameters_json) 关联
├── lineage_edges                  └── 被 openlineage_events 取代为主线，保留兼容
├── dataset_versions               └── 与 ml_ready_datasets (dataset_id, version) 关联
└── quality_snapshots              └── 与 qc_contract_runs (dataset_id, version) 关联

v1.5 (scale / benchmark / Argo)
├── etl_perf_runs                  └── 与 workflow_steps (dataset_id, version, phase) 互补
├── etl_shards                     └── 与 dataset_partitions 互补（shards = 资源调度视角，partitions = 数据划分视角）
├── benchmark_runs / benchmark_cases └── 由 v1.6 workflow_runs / workflow_steps 提供更细粒度状态
├── argo_workflow_runs             └── workflow_runs 是更通用的对应物；两者并存，由 sync 同步
└── runtime_events                 └── openlineage_events 是更标准化的事件流；runtime_events 保留兼容

v1.6 (robot platform)
├── qc_contracts / qc_contract_runs
├── workflow_runs / workflow_steps
├── asset_profiles
├── ml_ready_datasets
├── dataset_partitions
├── task_heartbeats
└── openlineage_events
```

## 2. 表字段语义

### 2.1 `qc_contracts`

| 字段 | 类型 | 语义 |
|------|------|------|
| `contract_id` | text UNIQUE | 业务唯一 ID，建议 `qc-<dataset_family>-<short_hash>` |
| `dataset_family` | text | 数据集族（`droid_lerobot` / `robomimic` / `bridgedata_v2` / ...） |
| `version` | text | contract 自身的版本，便于回滚 |
| `description` | text | 自然语言描述 |
| `rules_json` | jsonb | 规则定义，结构由主项目约定（如 `{rules:[{name,kind,expected}]}`） |
| `enabled` | boolean | 是否启用；停用后保留历史，不删除 |
| `updated_at` | timestamptz | 触发器自动更新，便于 contract diff |

**写入端**：主项目 Python CLI（`robot-dh contract register ...`）。
**读取端**：QC runner / exporter / dashboard。

### 2.2 `qc_contract_runs`

| 字段 | 语义 |
|------|------|
| `run_id` | UNIQUE，建议 `qcrun-<ts>-<short_hash>` |
| `contract_id` | 引用 `qc_contracts.contract_id`（弱引用，不做 FK，便于跨环境同步） |
| `dataset_id / version / dataset_family` | 实际跑的数据集元信息 |
| `dataset_uri` | 输入数据的 S3 URI |
| `status` | `pass / warn / fail / error`（脚本归一时还容忍 `passed / failed / success`） |
| `metrics_json` | 详细 metrics（行数、命中率等） |
| `failed_rules_json / warning_rules_json` | 命中的规则列表 |
| `artifacts_uri` | 报告产物 prefix，建议 `s3://robot-lake/qc/<contract_id>/<run_id>/` |

**写入端**：主项目 QC runner（独立 ETL task 或 Argo step）。
**读取端**：`./scripts/39_qc_contract_report.sh`、exporter。

### 2.3 `workflow_runs`

| 字段 | 语义 |
|------|------|
| `workflow_name` | 必填；与 `workflow_namespace` 组成 UNIQUE |
| `workflow_uid` | Argo Workflow UID（可空，非 Argo 来源时为空） |
| `workflow_template` | 模板名 |
| `workflow_type` | `argo / cli / fastapi / batch` 等 |
| `parameters_json` | 启动参数 |
| `metrics_json` | 终态聚合 metrics |
| `workflow_json` | 完整 Argo Workflow JSON 快照（便于 replay） |

**与 v1.5 `argo_workflow_runs` 的差别**：

- `argo_workflow_runs` 是 Argo 强相关的镜像快照（`workflow_uid` 必填、`entrypoint` 来自 spec）。
- `workflow_runs` 是更通用的视图，可以收纳本地 CLI / FastAPI 起的非 Argo workflow。
- 两表并存：Argo sync 脚本同时写两边；非 Argo 来源只写 `workflow_runs`。

### 2.4 `workflow_steps`

| 字段 | 语义 |
|------|------|
| `workflow_name / workflow_namespace / step_name` | UNIQUE 三元组 |
| `template_name` | 来自 Argo 模板 |
| `pod_name` | 实际 Pod 名 |
| `phase` | `Pending / Running / Succeeded / Failed / Error / Aborted / Timeout` |
| `dataset_id / version / dataset_family` | step 处理的数据集元信息（v1.6 多源核心） |
| `input_uri / output_uri` | 该 step 的输入输出 |
| `metrics_json` | step 内部 metrics（如 rows_in / rows_out） |
| `message` | 错误 / 状态消息（避免和 `error_message` 命名冲突 v1.5 表） |

**写入端**：Argo sync 脚本（轮询 Argo API 后写）。
**读取端**：`./scripts/38_workflow_metadata_report.sh`、Go exporter。

### 2.5 `asset_profiles`

| 字段 | 语义 |
|------|------|
| `profile_id` | UNIQUE，建议 `profile-<sha256[:12]>-<ts>` |
| `asset_uri` | 对应物理对象 URI（与 v1.4 `lake_assets.uri` 可关联） |
| `asset_format` | `parquet / mp4 / hdf5 / jsonl / ...` |
| `layer` | `raw / ods / dwd / ads / ml-ready` |
| `bytes / rows / files_count / episodes_count / videos_count` | 统计计数 |
| `schema_hash` | parquet schema hash（便于 schema drift 探测） |
| `null_rate` | 关键列空率聚合 |
| `profile_json` | 详细 profile（建议存 polars / pandas describe 输出 + 分类列 distinct） |
| `status` | `success / failed / partial` |

**写入端**：主项目 ETL 的 `profile_asset` task（在 normalize 完成后跑）。
**读取端**：exporter；可被 v1.4 `lake_assets` 引用。

### 2.6 `ml_ready_datasets`

| 字段 | 语义 |
|------|------|
| `output_uri` | UNIQUE，建议 `s3://robot-lake/ml-ready/<dataset_id>/<version>/` |
| `train_uri / val_uri / test_uri` | 三个 split 的 URI |
| `dataset_card_uri` | `DATASET_CARD.md` 路径 |
| `feature_schema_uri` | 特征 schema JSON |
| `quality_filter_uri` | 训练前应用的 quality 过滤策略 JSON |
| `lineage_uri` | OpenLineage 事件归档 |
| `quality_threshold` | 训练样本最小 quality score |
| `num_train / num_val / num_test` | 三 split 的样本数 |
| `status` | `building / ready / archived / failed` |

**写入端**：主项目 ETL `build_ml_ready` task。
**读取端**：training pipeline、`./scripts/41_export_v1_6_client_env.sh` 暴露的 `ROBOT_DH_ML_READY_ROOT`。

### 2.7 `dataset_partitions`

| 字段 | 语义 |
|------|------|
| `partition_id` | UNIQUE |
| `partition_type` | `episode / time / size / family / shard` 等 |
| `partition_index` | 0-based |
| `partition_uri` | partition 的 S3 URI |
| `input_bytes / estimated_rows` | 该 partition 的预估输入规模 |
| `status` | `pending / running / success / failed / skipped` |
| `metrics_json` | 详细 metrics（实际处理量、耗时） |

**与 v1.5 `etl_shards` 的差别**：

- `etl_shards` 描述 Argo 任务调度视角的分片（plan / worker 视角）
- `dataset_partitions` 描述数据本身的分区（episode / time 视角）
- 二者通常一对多：一个 `etl_shard` 处理多个 `dataset_partition`

**写入端**：主项目 ETL `plan_partition` task。
**读取端**：normalize / partial resume worker。

### 2.8 `task_heartbeats`

| 字段 | 语义 |
|------|------|
| `task_id` | 任务 ID（与 partition / shard 解耦） |
| `phase` | `normalize / feature / contract / benchmark / ...` |
| `progress_current / progress_total / progress_unit` | 进度三元组（`unit` 例：`episode / frame / row / mb`） |
| `message` | 心跳附带消息 |
| `updated_at` | 每次心跳被更新（worker 自行频繁 INSERT；查询时按 `(task_id, updated_at DESC)` 取 latest） |

**写入端**：主项目长任务 worker（每 10–60s 一次）。
**读取端**：监控 / dashboard / `./scripts/38_workflow_metadata_report.sh` 的扩展。

> 不放 `UNIQUE(task_id)` 是有意的：保留每次心跳记录便于排查 deadline 失败时 worker 卡在哪一步。生产环境需要定期清理；建议主项目侧实现 `DELETE FROM task_heartbeats WHERE updated_at < now() - interval '30 days'`。

### 2.9 `openlineage_events`

| 字段 | 语义 |
|------|------|
| `event_id` | UNIQUE，由 producer 生成 |
| `event_type` | OpenLineage spec：`START / RUNNING / COMPLETE / ABORT / FAIL / OTHER` |
| `event_time` | 事件发生时间，独立于 `created_at` |
| `job_namespace / job_name` | OpenLineage Job 标识 |
| `run_id` | OpenLineage Run ID |
| `inputs_json / outputs_json` | OpenLineage 标准 input / output dataset 列表 |
| `facets_json` | 标准 facets（schema / quality / lineage / ...） |
| `raw_event_json` | 原始 OpenLineage 事件 |

**与 v1.5 `runtime_events` 的差别**：

- `runtime_events` 是项目自定义 event_type（`smoke.event / etl.normalize.start` 等）
- `openlineage_events` 严格遵循 OpenLineage spec，便于对接 Marquez / DataHub / OpenLineage Java exporter

## 3. 与外部 exporter 的接口

| 数据出口 | 推荐查询 |
|----------|----------|
| Prometheus / Grafana 面板 | `qc_contract_runs` 按 `status, dataset_family` 聚合；`task_heartbeats` 按 `phase` 取最新 |
| OpenLineage 兼容工具 | 直接 SELECT `openlineage_events`，按 `event_time` 滚动游标 |
| Go exporter | 读 `workflow_runs / workflow_steps`、`benchmark_runs / benchmark_cases`（v1.5）、`qc_contract_runs`（v1.6） |
| Argo 同步进程 | 写 `workflow_runs / workflow_steps`，同时按需写 `argo_workflow_runs` |

## 4. 索引说明

| 索引 | 目的 |
|------|------|
| `idx_qc_contract_runs_contract_status_created_at` | dashboard 按 contract 聚合最近窗口 |
| `idx_qc_contract_runs_dataset_version_status` | exporter 按数据集查最新 status |
| `idx_workflow_runs_status_created_at` | 监控 "最近运行 / 失败" |
| `idx_workflow_steps_workflow_phase` | report 计算失败 step 分布 |
| `idx_workflow_steps_dataset_version` | 多源 step 分布查询 |
| `idx_asset_profiles_dataset_version_family` | 单数据集 asset 画像 |
| `idx_asset_profiles_format_status` | 按格式 / 状态过滤 |
| `idx_ml_ready_datasets_dataset_version_status` | training pipeline 找 ready 数据 |
| `idx_dataset_partitions_dataset_version_type` | normalize resume 时按 type 取 partition |
| `idx_task_heartbeats_task_id` | 取单 task 最新 heartbeat |
| `idx_task_heartbeats_workflow_step` | 按 workflow / step 聚合心跳 |
| `idx_openlineage_events_type_time` | 按 event_type 滚动游标 |
