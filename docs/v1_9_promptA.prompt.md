你是资深 AI 数据系统工程师、PostgreSQL 数据基建工程师、DevOps/SRE 工程师。当前项目是 robot-dh-infra，运行在腾讯云 Ubuntu 服务器上，为 robot-data-harness 提供远端 PostgreSQL / MinIO / Redis 基础设施。

请在现有 v1.8 infra 基础上升级到 v1.9。不要删除已有数据，不要 drop 已有表，不要自动格式化磁盘，不要暴露真实密码到日志。

============================================================
一、完整背景
============================================================

主项目：
  robot-data-harness

当前主项目 v1.8 已完成：
  - Local-First Robot Data Platform Runtime
  - DROID / LeRobot、robomimic、BridgeData adapter
  - QC Contract
  - Argo Local devscale DAG
  - raw / ods / dwd / ads / ML-ready 数据湖
  - PostgreSQL metadata
  - Warehouse Metrics & Quality Ops
  - DIM / FACT / DWS / ADS 数仓指标层
  - Quality Report
  - Backfill / SLA
  - SparkSQL local mode 可选
  - Go exporter
  - FastAPI 查询接口

v1.9 目标：
  AI Inference Data Plane Lite

核心目标：
  1. 新增模型注册表。
  2. 新增推理任务表。
  3. 新增推理输出表。
  4. 新增蒸馏数据集表。
  5. 新增推理 benchmark 表。
  6. 新增事件 / 失败 / 死信任务支持，为后续 Kafka / Redis Streams / AutoDL GPU worker 做准备。
  7. 不引入真实大模型依赖。
  8. 不强依赖 GPU。
  9. 不实现完整训练平台。

============================================================
二、本次交付物
============================================================

新增或修改：

postgres/
  migrations/
    007_v1_9_inference_data_plane.sql

scripts/
  45_pg_apply_v1_9_schema.sh
  46_pg_v1_9_smoke_test.sh
  47_inference_ops_report.sh
  48_distill_dataset_report.sh
  49_export_v1_9_client_env.sh

client/
  robot-dh-v1-9.env.example
  k8s-v1-9-secret.example.yaml
  k8s-create-v1-9-secret.example.sh

docs/
  v1_9_inference_schema.md
  v1_9_inference_ops_runbook.md
  v1_9_autodl_worker_notes.md

README.md 更新 v1.9 章节。

============================================================
三、PostgreSQL v1.9 schema
============================================================

新增 migration：

postgres/migrations/007_v1_9_inference_data_plane.sql

只允许：
  CREATE TABLE IF NOT EXISTS
  CREATE INDEX IF NOT EXISTS
  CREATE VIEW IF NOT EXISTS
  GRANT

禁止：
  DROP TABLE
  TRUNCATE
  destructive ALTER
  删除已有数据

------------------------------------------------------------
1. model_registry
------------------------------------------------------------

model_registry:
  model_id text primary key
  model_name text not null
  model_type text not null
  backend text not null
  endpoint_url text
  input_schema_json jsonb
  output_schema_json jsonb
  max_batch_size int
  timeout_sec int
  status text not null default 'ACTIVE'
  tags_json jsonb
  created_at timestamptz default now()
  updated_at timestamptz default now()

索引：
  model_registry(model_type)
  model_registry(backend)
  model_registry(status)

model_type 示例：
  caption
  embedding
  anomaly_scorer
  vlm
  llm
  mock

backend 示例：
  mock
  local_cpu
  openai_compatible
  autodl_worker
  http_json

------------------------------------------------------------
2. inference_jobs
------------------------------------------------------------

inference_jobs:
  job_id text primary key
  job_name text
  model_id text not null
  input_uri text not null
  output_uri text not null
  input_format text
  output_format text
  dataset_id text
  version text
  dataset_family text
  task_type text not null
  status text not null
  priority int default 0
  batch_size int
  max_workers int
  total_samples bigint default 0
  processed_samples bigint default 0
  failed_samples bigint default 0
  started_at timestamptz
  finished_at timestamptz
  duration_sec double precision
  error_message text
  config_json jsonb
  metrics_json jsonb
  created_at timestamptz default now()
  updated_at timestamptz default now()

索引：
  inference_jobs(status, created_at)
  inference_jobs(model_id, status)
  inference_jobs(dataset_id, version)
  inference_jobs(task_type, status)

状态：
  CREATED
  QUEUED
  RUNNING
  SUCCEEDED
  FAILED
  RETRYING
  CANCELLED
  DEAD_LETTER

------------------------------------------------------------
3. inference_outputs
------------------------------------------------------------

inference_outputs:
  output_id text primary key
  job_id text not null
  model_id text not null
  sample_id text
  dataset_id text
  version text
  episode_id text
  frame_id text
  input_uri text
  output_uri text
  prediction_type text
  prediction_json jsonb
  confidence double precision
  latency_ms double precision
  token_count int
  status text
  error_message text
  created_at timestamptz default now()

索引：
  inference_outputs(job_id)
  inference_outputs(model_id, created_at)
  inference_outputs(dataset_id, version)
  inference_outputs(prediction_type, status)

------------------------------------------------------------
4. inference_failures
------------------------------------------------------------

inference_failures:
  failure_id text primary key
  job_id text not null
  model_id text
  sample_id text
  input_uri text
  error_type text
  error_message text
  retryable boolean default true
  attempt int default 1
  created_at timestamptz default now()

索引：
  inference_failures(job_id)
  inference_failures(error_type)
  inference_failures(retryable)

------------------------------------------------------------
5. distillation_datasets
------------------------------------------------------------

distillation_datasets:
  distill_id text primary key
  dataset_id text
  version text
  source_inference_job_id text
  teacher_model_id text
  distill_format text not null
  output_uri text not null
  train_uri text
  val_uri text
  test_uri text
  dataset_card_uri text
  num_train bigint default 0
  num_val bigint default 0
  num_test bigint default 0
  status text not null
  metrics_json jsonb
  created_at timestamptz default now()
  updated_at timestamptz default now()

索引：
  distillation_datasets(dataset_id, version)
  distillation_datasets(teacher_model_id)
  distillation_datasets(status)

distill_format 示例：
  instruction_tuning
  caption_sft
  embedding_pairs
  anomaly_detection

------------------------------------------------------------
6. inference_benchmark_runs
------------------------------------------------------------

inference_benchmark_runs:
  benchmark_id text primary key
  model_id text
  backend text
  workload_name text
  input_uri text
  status text
  concurrency int
  batch_size int
  duration_sec double precision
  total_samples bigint
  succeeded_samples bigint
  failed_samples bigint
  samples_per_sec double precision
  p50_latency_ms double precision
  p95_latency_ms double precision
  p99_latency_ms double precision
  error_rate double precision
  cost_estimate_json jsonb
  metrics_json jsonb
  started_at timestamptz
  finished_at timestamptz
  created_at timestamptz default now()

索引：
  inference_benchmark_runs(model_id, created_at)
  inference_benchmark_runs(backend, status)
  inference_benchmark_runs(workload_name)

------------------------------------------------------------
7. ai_task_events
------------------------------------------------------------

ai_task_events:
  event_id text primary key
  event_type text not null
  task_id text
  job_id text
  model_id text
  dataset_id text
  version text
  payload_json jsonb
  created_at timestamptz default now()

索引：
  ai_task_events(event_type, created_at)
  ai_task_events(task_id)
  ai_task_events(job_id)

------------------------------------------------------------
8. dead_letter_tasks
------------------------------------------------------------

dead_letter_tasks:
  dead_letter_id text primary key
  task_type text not null
  task_id text
  job_id text
  reason text
  payload_json jsonb
  retry_count int default 0
  created_at timestamptz default now()

索引：
  dead_letter_tasks(task_type, created_at)
  dead_letter_tasks(job_id)

------------------------------------------------------------
9. ADS / DWS 推理运营表
------------------------------------------------------------

dws_inference_job_daily:
  dt date not null
  model_id text not null
  backend text
  task_type text
  job_count int default 0
  success_count int default 0
  fail_count int default 0
  success_rate double precision
  total_samples bigint default 0
  samples_per_sec double precision
  avg_latency_ms double precision
  p95_latency_ms double precision
  error_rate double precision
  updated_at timestamptz default now()
  primary key (dt, model_id, task_type)

ads_inference_dashboard:
  dt date not null
  model_id text not null
  backend text
  task_type text
  overall_status text
  job_count int
  success_rate double precision
  total_samples bigint
  samples_per_sec double precision
  p95_latency_ms double precision
  error_rate double precision
  top_error_type text
  alert_level text
  alert_reason text
  updated_at timestamptz default now()
  primary key (dt, model_id, task_type)

============================================================
四、脚本要求
============================================================

scripts/45_pg_apply_v1_9_schema.sh:
  - 幂等执行 007_v1_9_inference_data_plane.sql。
  - 不 drop、不 truncate。
  - 应用后列出 v1.9 新表。
  - 退出码非 0 表示失败。

scripts/46_pg_v1_9_smoke_test.sh:
  - 使用 app user 测试读写权限。
  - 覆盖 model_registry、inference_jobs、inference_outputs、distillation_datasets、inference_benchmark_runs、ai_task_events。
  - 插入 smoke 数据后清理。
  - 不影响真实数据。

scripts/47_inference_ops_report.sh:
  - 查询 inference_jobs、inference_outputs、inference_failures、inference_benchmark_runs。
  - 输出 Markdown 和 JSON：
      /data/robot-dh/logs/v1_9_inference_ops_YYYYmmdd_HHMMSS.md
      /data/robot-dh/logs/v1_9_inference_ops_YYYYmmdd_HHMMSS.json
  - 表为空时不失败。

scripts/48_distill_dataset_report.sh:
  - 查询 distillation_datasets。
  - 输出蒸馏数据集统计报告。
  - 表为空时不失败。

scripts/49_export_v1_9_client_env.sh:
  - 生成 client/robot-dh-v1-9.env.example 或真实 env。
  - 默认脱敏。
  - 传 --show-secrets 才输出真实文件。
  - 增加：
      ROBOT_DH_PLATFORM_VERSION=1.9
      ROBOT_DH_INFER_OUTPUT_ROOT=s3://robot-lake/infer
      ROBOT_DH_DISTILL_OUTPUT_ROOT=s3://robot-lake/distill
      ROBOT_DH_DEFAULT_INFER_BACKEND=mock
      ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL=
      ROBOT_DH_OPENAI_COMPATIBLE_API_KEY=
  - chmod 600。
  - 不打印密码。

============================================================
五、文档要求
============================================================

docs/v1_9_inference_schema.md：
  - 解释 model_registry / inference_jobs / inference_outputs / distillation_datasets。
  - 解释 mock / local_cpu / openai_compatible / autodl_worker backend。
  - 解释如何从 ML-ready dataset 生成 inference outputs。

docs/v1_9_inference_ops_runbook.md：
  - 如何 apply schema。
  - 如何 smoke test。
  - 如何看推理任务状态。
  - 如何看 benchmark。
  - 如何看 distillation dataset。

docs/v1_9_autodl_worker_notes.md：
  - AutoDL 只作为 GPU worker，不作为 K8s / DB / MinIO 节点。
  - 推荐 pull-based worker。
  - 不要把核心数据放在 AutoDL 临时盘。
  - 后续接 vLLM / OpenAI-compatible endpoint 的 env。

README.md：
  - 新增 v1.9 AI Inference Data Plane Lite 章节。
  - 新增验收命令。

============================================================
六、验收命令
============================================================

用户手动执行：

cd /opt/robot-dh-infra

./scripts/06_healthcheck.sh
./scripts/45_pg_apply_v1_9_schema.sh
./scripts/46_pg_v1_9_smoke_test.sh
./scripts/47_inference_ops_report.sh
./scripts/48_distill_dataset_report.sh
./scripts/49_export_v1_9_client_env.sh

验收标准：
  - 所有脚本可重复执行。
  - 不删除已有数据。
  - 不 drop / truncate 已有表。
  - 不暴露密码。
  - v1.9 新表存在。
  - app user 可读写新表。
  - 报告脚本在空表和有数据时都能运行。

请开始实现。所有 shell 脚本使用 set -euo pipefail。不要留 TODO，不要写伪代码。