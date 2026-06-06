-- v1.9 AI inference data plane lite schema.
-- 模型注册表 / 推理任务 / 推理输出 / 推理失败 / 蒸馏数据集 / 推理 benchmark /
-- AI 任务事件 / 死信任务 + 推理运营 DWS / ADS 指标表。
-- 为后续 Kafka / Redis Streams / AutoDL GPU worker（pull-based）预留事件与死信通道。
--
-- 仅做：
--   CREATE TABLE IF NOT EXISTS
--   CREATE INDEX IF NOT EXISTS
--   GRANT
--
-- 禁止：
--   DROP TABLE / TRUNCATE / destructive ALTER / 删除已有数据
--
-- 与 001/002/003/004/005/006 迁移完全并存，可重复执行。
--
-- 设计要点：
--   1. 全部新表主键使用 text（业务侧生成 key），不引入 bigserial，因此不需要 SEQUENCE GRANT。
--   2. dws_/ads_ 推理运营表使用复合主键 (dt, model_id, task_type)，配合 ON CONFLICT DO UPDATE 做 UPSERT。
--   3. *_json 列统一 jsonb，承载非结构化 schema / 指标 / 配置 / 事件 payload。
--   4. model_registry / inference_jobs / inference_outputs 由主项目 robot-data-harness 在
--      model register / infer / distill 阶段写入，infra 仓库只建表不写入业务数据。
--   5. 模型推理与现有数据湖解耦：input_uri 通常指向 ML-ready / dwd parquet，output_uri 指向
--      s3://robot-lake/infer 或 s3://robot-lake/distill。

BEGIN;

-- ============================================================
-- 1. model_registry：模型注册表
-- ============================================================
--
-- 一行一个可调用模型版本。model_type 取值示例：caption / embedding / anomaly_scorer / vlm / llm / mock。
-- backend 取值示例：mock / local_cpu / openai_compatible / autodl_worker / http_json。
-- endpoint_url 仅 http 类 backend 需要；mock / local_cpu 可为空。
-- input_schema_json / output_schema_json 描述模型 I/O 契约，供 infer job 校验与下游消费。
CREATE TABLE IF NOT EXISTS model_registry (
  model_id text PRIMARY KEY,
  model_name text NOT NULL,
  model_type text NOT NULL,
  backend text NOT NULL,
  endpoint_url text,
  input_schema_json jsonb,
  output_schema_json jsonb,
  max_batch_size int,
  timeout_sec int,
  status text NOT NULL DEFAULT 'ACTIVE',
  tags_json jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_model_registry_model_type
  ON model_registry (model_type);
CREATE INDEX IF NOT EXISTS idx_model_registry_backend
  ON model_registry (backend);
CREATE INDEX IF NOT EXISTS idx_model_registry_status
  ON model_registry (status);

-- ============================================================
-- 2. inference_jobs：批量推理任务
-- ============================================================
--
-- 一行一个批量推理任务（对一份输入数据集做 caption / embedding / 打分等）。
-- status 取值：CREATED / QUEUED / RUNNING / SUCCEEDED / FAILED / RETRYING / CANCELLED / DEAD_LETTER。
-- total_samples / processed_samples / failed_samples 由 worker 增量回写，做进度与成功率统计。
-- config_json 存批处理参数（backend 覆盖、prompt 模板等），metrics_json 存收尾态聚合指标。
CREATE TABLE IF NOT EXISTS inference_jobs (
  job_id text PRIMARY KEY,
  job_name text,
  model_id text NOT NULL,
  input_uri text NOT NULL,
  output_uri text NOT NULL,
  input_format text,
  output_format text,
  dataset_id text,
  version text,
  dataset_family text,
  task_type text NOT NULL,
  status text NOT NULL,
  priority int DEFAULT 0,
  batch_size int,
  max_workers int,
  total_samples bigint DEFAULT 0,
  processed_samples bigint DEFAULT 0,
  failed_samples bigint DEFAULT 0,
  started_at timestamptz,
  finished_at timestamptz,
  duration_sec double precision,
  error_message text,
  config_json jsonb,
  metrics_json jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_inference_jobs_status_created_at
  ON inference_jobs (status, created_at);
CREATE INDEX IF NOT EXISTS idx_inference_jobs_model_status
  ON inference_jobs (model_id, status);
CREATE INDEX IF NOT EXISTS idx_inference_jobs_dataset_version
  ON inference_jobs (dataset_id, version);
CREATE INDEX IF NOT EXISTS idx_inference_jobs_task_type_status
  ON inference_jobs (task_type, status);

-- ============================================================
-- 3. inference_outputs：单样本推理输出
-- ============================================================
--
-- 一行一个样本（sample / frame）的模型预测结果。prediction_json 存结构化预测
-- （caption 文本 / embedding 向量引用 / anomaly score 等），output_uri 指向落盘的大对象。
-- 大向量不入库：prediction_json 只放摘要或外链，原始 embedding 写 s3://robot-lake/infer。
CREATE TABLE IF NOT EXISTS inference_outputs (
  output_id text PRIMARY KEY,
  job_id text NOT NULL,
  model_id text NOT NULL,
  sample_id text,
  dataset_id text,
  version text,
  episode_id text,
  frame_id text,
  input_uri text,
  output_uri text,
  prediction_type text,
  prediction_json jsonb,
  confidence double precision,
  latency_ms double precision,
  token_count int,
  status text,
  error_message text,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_inference_outputs_job_id
  ON inference_outputs (job_id);
CREATE INDEX IF NOT EXISTS idx_inference_outputs_model_created_at
  ON inference_outputs (model_id, created_at);
CREATE INDEX IF NOT EXISTS idx_inference_outputs_dataset_version
  ON inference_outputs (dataset_id, version);
CREATE INDEX IF NOT EXISTS idx_inference_outputs_prediction_type_status
  ON inference_outputs (prediction_type, status);

-- ============================================================
-- 4. inference_failures：推理失败 / 可重试样本
-- ============================================================
--
-- 一行一个失败样本，供 retry / dead-letter 流程消费。retryable=false 的样本会被搬到
-- dead_letter_tasks。attempt 记录已重试次数，error_type 用于聚合根因（timeout / oom / schema 等）。
CREATE TABLE IF NOT EXISTS inference_failures (
  failure_id text PRIMARY KEY,
  job_id text NOT NULL,
  model_id text,
  sample_id text,
  input_uri text,
  error_type text,
  error_message text,
  retryable boolean DEFAULT TRUE,
  attempt int DEFAULT 1,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_inference_failures_job_id
  ON inference_failures (job_id);
CREATE INDEX IF NOT EXISTS idx_inference_failures_error_type
  ON inference_failures (error_type);
CREATE INDEX IF NOT EXISTS idx_inference_failures_retryable
  ON inference_failures (retryable);

-- ============================================================
-- 5. distillation_datasets：蒸馏数据集
-- ============================================================
--
-- 一行一个由 teacher 模型推理结果蒸馏出的训练数据集。distill_format 示例：
-- instruction_tuning / caption_sft / embedding_pairs / anomaly_detection。
-- source_inference_job_id 指向产出 pseudo label 的 inference_jobs.job_id，做血缘回溯。
-- output_uri 指向 s3://robot-lake/distill，train/val/test_uri 为切分后的分片根。
CREATE TABLE IF NOT EXISTS distillation_datasets (
  distill_id text PRIMARY KEY,
  dataset_id text,
  version text,
  source_inference_job_id text,
  teacher_model_id text,
  distill_format text NOT NULL,
  output_uri text NOT NULL,
  train_uri text,
  val_uri text,
  test_uri text,
  dataset_card_uri text,
  num_train bigint DEFAULT 0,
  num_val bigint DEFAULT 0,
  num_test bigint DEFAULT 0,
  status text NOT NULL,
  metrics_json jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_distillation_datasets_dataset_version
  ON distillation_datasets (dataset_id, version);
CREATE INDEX IF NOT EXISTS idx_distillation_datasets_teacher_model
  ON distillation_datasets (teacher_model_id);
CREATE INDEX IF NOT EXISTS idx_distillation_datasets_status
  ON distillation_datasets (status);

-- ============================================================
-- 6. inference_benchmark_runs：推理 benchmark
-- ============================================================
--
-- 一行一次推理后端压测（固定 workload + concurrency + batch_size）。
-- samples_per_sec / p50/p95/p99_latency_ms / error_rate 是核心吞吐与时延指标。
-- cost_estimate_json 预留 GPU 时长 / token 成本估算（AutoDL / OpenAI-compatible 接入后填）。
CREATE TABLE IF NOT EXISTS inference_benchmark_runs (
  benchmark_id text PRIMARY KEY,
  model_id text,
  backend text,
  workload_name text,
  input_uri text,
  status text,
  concurrency int,
  batch_size int,
  duration_sec double precision,
  total_samples bigint,
  succeeded_samples bigint,
  failed_samples bigint,
  samples_per_sec double precision,
  p50_latency_ms double precision,
  p95_latency_ms double precision,
  p99_latency_ms double precision,
  error_rate double precision,
  cost_estimate_json jsonb,
  metrics_json jsonb,
  started_at timestamptz,
  finished_at timestamptz,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_inference_benchmark_runs_model_created_at
  ON inference_benchmark_runs (model_id, created_at);
CREATE INDEX IF NOT EXISTS idx_inference_benchmark_runs_backend_status
  ON inference_benchmark_runs (backend, status);
CREATE INDEX IF NOT EXISTS idx_inference_benchmark_runs_workload
  ON inference_benchmark_runs (workload_name);

-- ============================================================
-- 7. ai_task_events：AI 任务事件流
-- ============================================================
--
-- 统一事件表，为后续 Kafka / Redis Streams 落地做准备。event_type 示例：
-- JOB_CREATED / JOB_STARTED / SAMPLE_DONE / JOB_SUCCEEDED / JOB_FAILED / DISTILL_BUILT。
-- payload_json 存事件原文；task_id / job_id 做关联查询。
CREATE TABLE IF NOT EXISTS ai_task_events (
  event_id text PRIMARY KEY,
  event_type text NOT NULL,
  task_id text,
  job_id text,
  model_id text,
  dataset_id text,
  version text,
  payload_json jsonb,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ai_task_events_type_created_at
  ON ai_task_events (event_type, created_at);
CREATE INDEX IF NOT EXISTS idx_ai_task_events_task_id
  ON ai_task_events (task_id);
CREATE INDEX IF NOT EXISTS idx_ai_task_events_job_id
  ON ai_task_events (job_id);

-- ============================================================
-- 8. dead_letter_tasks：死信任务
-- ============================================================
--
-- 多次重试仍失败、或被显式判定不可重试的任务落到此处，供人工排查 / 离线补偿。
-- task_type 示例：inference_job / inference_sample / distill_build。
CREATE TABLE IF NOT EXISTS dead_letter_tasks (
  dead_letter_id text PRIMARY KEY,
  task_type text NOT NULL,
  task_id text,
  job_id text,
  reason text,
  payload_json jsonb,
  retry_count int DEFAULT 0,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_tasks_type_created_at
  ON dead_letter_tasks (task_type, created_at);
CREATE INDEX IF NOT EXISTS idx_dead_letter_tasks_job_id
  ON dead_letter_tasks (job_id);

-- ============================================================
-- 9. dws_inference_job_daily：推理任务按天聚合（DWS）
-- ============================================================
--
-- 由主项目 warehouse builder 从 inference_jobs / inference_outputs 聚合而来。
-- 复合主键 (dt, model_id, task_type)，配合 ON CONFLICT DO UPDATE 幂等 UPSERT。
CREATE TABLE IF NOT EXISTS dws_inference_job_daily (
  dt date NOT NULL,
  model_id text NOT NULL,
  backend text,
  task_type text,
  job_count int DEFAULT 0,
  success_count int DEFAULT 0,
  fail_count int DEFAULT 0,
  success_rate double precision,
  total_samples bigint DEFAULT 0,
  samples_per_sec double precision,
  avg_latency_ms double precision,
  p95_latency_ms double precision,
  error_rate double precision,
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (dt, model_id, task_type)
);

CREATE INDEX IF NOT EXISTS idx_dws_inference_job_daily_model_dt
  ON dws_inference_job_daily (model_id, dt);
CREATE INDEX IF NOT EXISTS idx_dws_inference_job_daily_backend_dt
  ON dws_inference_job_daily (backend, dt);

-- ============================================================
-- 10. ads_inference_dashboard：推理运营看板（ADS）
-- ============================================================
--
-- 面向看板 / 告警的最终聚合层。overall_status / alert_level / alert_reason 由主项目按阈值判定。
-- 复合主键 (dt, model_id, task_type)，UPSERT 覆盖当天最新画像。
CREATE TABLE IF NOT EXISTS ads_inference_dashboard (
  dt date NOT NULL,
  model_id text NOT NULL,
  backend text,
  task_type text,
  overall_status text,
  job_count int,
  success_rate double precision,
  total_samples bigint,
  samples_per_sec double precision,
  p95_latency_ms double precision,
  error_rate double precision,
  top_error_type text,
  alert_level text,
  alert_reason text,
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (dt, model_id, task_type)
);

CREATE INDEX IF NOT EXISTS idx_ads_inference_dashboard_alert_level_dt
  ON ads_inference_dashboard (alert_level, dt);
CREATE INDEX IF NOT EXISTS idx_ads_inference_dashboard_model_dt
  ON ads_inference_dashboard (model_id, dt);

-- ============================================================
-- 11. GRANT 给应用账号 robot_dh_app
-- ============================================================
--
-- 全部 v1.9 表均使用 text / 复合主键，不存在 bigserial / SEQUENCE，
-- 因此本次 migration 不需要 GRANT USAGE/SELECT ON SEQUENCE。

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
  model_registry,
  inference_jobs, inference_outputs, inference_failures,
  distillation_datasets, inference_benchmark_runs,
  ai_task_events, dead_letter_tasks,
  dws_inference_job_daily, ads_inference_dashboard
TO robot_dh_app;

COMMIT;
