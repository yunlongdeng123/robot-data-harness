-- v1.6 robot platform metadata schema.
-- 多源机器人数据 QC contract / workflow metadata / asset profile / ml-ready / OpenLineage 事件。
--
-- 仅做：
--   CREATE TABLE IF NOT EXISTS
--   CREATE INDEX IF NOT EXISTS
--   GRANT
--
-- 禁止：
--   DROP TABLE / TRUNCATE / destructive ALTER / 删除已有数据
--
-- 与 001/002/003/004 迁移完全并存，可重复执行。

BEGIN;

-- qc_contracts：数据集族级别的 QC 规则定义（rules_json 描述 schema / range / domain check）。
CREATE TABLE IF NOT EXISTS qc_contracts (
  id bigserial PRIMARY KEY,
  contract_id text NOT NULL UNIQUE,
  dataset_family text NOT NULL,
  version text NOT NULL,
  description text,
  rules_json jsonb NOT NULL,
  enabled boolean NOT NULL DEFAULT TRUE,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- qc_contract_runs：单次 QC contract 执行结果（每次 normalize / build_features / build_ads 完成都会落一条）。
CREATE TABLE IF NOT EXISTS qc_contract_runs (
  id bigserial PRIMARY KEY,
  run_id text NOT NULL UNIQUE,
  contract_id text NOT NULL,
  dataset_id text,
  version text,
  dataset_family text,
  dataset_uri text,
  status text NOT NULL,
  started_at timestamptz,
  finished_at timestamptz,
  duration_sec double precision,
  metrics_json jsonb,
  failed_rules_json jsonb,
  warning_rules_json jsonb,
  artifacts_uri text,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- workflow_runs：v1.6 通用 workflow run 元数据。
-- 与 v1.5 argo_workflow_runs 并存：argo_workflow_runs 是 Argo 同步快照，
-- workflow_runs 由主项目 CLI / sync 脚本写入，支持非 Argo workflow（FastAPI / 本地 CLI）。
CREATE TABLE IF NOT EXISTS workflow_runs (
  id bigserial PRIMARY KEY,
  workflow_name text NOT NULL,
  workflow_uid text,
  workflow_namespace text,
  workflow_template text,
  workflow_type text,
  status text,
  started_at timestamptz,
  finished_at timestamptz,
  duration_sec double precision,
  parameters_json jsonb,
  metrics_json jsonb,
  workflow_json jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (workflow_namespace, workflow_name)
);

-- workflow_steps：workflow 内单个 step / template 的细粒度状态。
-- 用于排查 v1.5 normalize 阶段 deadline 失败（没有 step 级时序与 dataset_id 维度）。
CREATE TABLE IF NOT EXISTS workflow_steps (
  id bigserial PRIMARY KEY,
  workflow_name text NOT NULL,
  workflow_namespace text,
  step_name text NOT NULL,
  template_name text,
  pod_name text,
  phase text,
  started_at timestamptz,
  finished_at timestamptz,
  duration_sec double precision,
  dataset_id text,
  version text,
  dataset_family text,
  input_uri text,
  output_uri text,
  metrics_json jsonb,
  message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (workflow_namespace, workflow_name, step_name)
);

-- asset_profiles：单个 asset（parquet / mp4 / hdf5 / jsonl）的画像。
-- profile_id 由 Python 端生成（建议 'profile-<sha256[:12]>-<ts>'），不强制语义。
CREATE TABLE IF NOT EXISTS asset_profiles (
  id bigserial PRIMARY KEY,
  profile_id text NOT NULL UNIQUE,
  dataset_id text,
  version text,
  dataset_family text,
  asset_uri text NOT NULL,
  asset_format text,
  layer text,
  bytes bigint,
  rows bigint,
  files_count int,
  episodes_count int,
  videos_count int,
  schema_hash text,
  null_rate double precision,
  profile_json jsonb,
  status text,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- ml_ready_datasets：训练就绪数据集元数据（train/val/test + dataset card + feature schema）。
-- output_uri 唯一：同一份 ml-ready 输出只允许登记一次；要更新走 UPDATE，不要重复插入。
CREATE TABLE IF NOT EXISTS ml_ready_datasets (
  id bigserial PRIMARY KEY,
  dataset_id text NOT NULL,
  version text NOT NULL,
  dataset_family text,
  output_uri text NOT NULL UNIQUE,
  train_uri text,
  val_uri text,
  test_uri text,
  dataset_card_uri text,
  feature_schema_uri text,
  quality_filter_uri text,
  lineage_uri text,
  quality_threshold double precision,
  num_train bigint,
  num_val bigint,
  num_test bigint,
  status text,
  metrics_json jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- dataset_partitions：数据集按 episode / time / size 等维度的分片登记。
-- 用于 v1.6 partial resume：normalize 中途失败时，按 partition 重跑而不是整 dataset。
CREATE TABLE IF NOT EXISTS dataset_partitions (
  id bigserial PRIMARY KEY,
  partition_id text NOT NULL UNIQUE,
  dataset_id text NOT NULL,
  version text NOT NULL,
  dataset_family text,
  dataset_uri text NOT NULL,
  partition_type text NOT NULL,
  partition_index int NOT NULL,
  partition_uri text,
  input_bytes bigint,
  estimated_rows bigint,
  status text,
  metrics_json jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- task_heartbeats：normalize / feature / contract / benchmark 等长任务的运行时心跳。
-- 同一 task_id 会被频繁 UPSERT；查询时按 updated_at 取最新一行。
-- 注意：本表故意不写 UNIQUE(task_id)，允许保留历史 heartbeat 用于排查；
-- 应用层按 (task_id, updated_at DESC) 取 latest。
CREATE TABLE IF NOT EXISTS task_heartbeats (
  id bigserial PRIMARY KEY,
  task_id text NOT NULL,
  workflow_name text,
  step_name text,
  dataset_id text,
  version text,
  phase text,
  progress_current bigint,
  progress_total bigint,
  progress_unit text,
  message text,
  metrics_json jsonb,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- openlineage_events：OpenLineage 风格的统一事件表。
-- 与 v1.5 runtime_events 不同：runtime_events 是项目自定义 event_type，
-- openlineage_events 严格遵循 OpenLineage spec（START / COMPLETE / FAIL / ABORT）。
-- 同一 run 通常会写多条（每个状态切换一条），event_id 由 producer 生成保证唯一。
CREATE TABLE IF NOT EXISTS openlineage_events (
  id bigserial PRIMARY KEY,
  event_id text NOT NULL UNIQUE,
  event_type text NOT NULL,
  event_time timestamptz NOT NULL,
  job_namespace text,
  job_name text,
  run_id text,
  inputs_json jsonb,
  outputs_json jsonb,
  facets_json jsonb,
  raw_event_json jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_qc_contract_runs_contract_status_created_at
  ON qc_contract_runs (contract_id, status, created_at);

CREATE INDEX IF NOT EXISTS idx_qc_contract_runs_dataset_version_status
  ON qc_contract_runs (dataset_id, version, status);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_status_created_at
  ON workflow_runs (status, created_at);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_workflow_phase
  ON workflow_steps (workflow_name, phase);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_dataset_version
  ON workflow_steps (dataset_id, version);

CREATE INDEX IF NOT EXISTS idx_asset_profiles_dataset_version_family
  ON asset_profiles (dataset_id, version, dataset_family);

CREATE INDEX IF NOT EXISTS idx_asset_profiles_format_status
  ON asset_profiles (asset_format, status);

CREATE INDEX IF NOT EXISTS idx_ml_ready_datasets_dataset_version_status
  ON ml_ready_datasets (dataset_id, version, status);

CREATE INDEX IF NOT EXISTS idx_dataset_partitions_dataset_version_type
  ON dataset_partitions (dataset_id, version, partition_type);

CREATE INDEX IF NOT EXISTS idx_task_heartbeats_task_id
  ON task_heartbeats (task_id);

CREATE INDEX IF NOT EXISTS idx_task_heartbeats_workflow_step
  ON task_heartbeats (workflow_name, step_name);

CREATE INDEX IF NOT EXISTS idx_openlineage_events_type_time
  ON openlineage_events (event_type, event_time);

-- 把新表权限授予固定应用账号 robot_dh_app。
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
  qc_contracts, qc_contract_runs,
  workflow_runs, workflow_steps,
  asset_profiles, ml_ready_datasets,
  dataset_partitions, task_heartbeats,
  openlineage_events
TO robot_dh_app;

GRANT USAGE, SELECT ON SEQUENCE
  qc_contracts_id_seq, qc_contract_runs_id_seq,
  workflow_runs_id_seq, workflow_steps_id_seq,
  asset_profiles_id_seq, ml_ready_datasets_id_seq,
  dataset_partitions_id_seq, task_heartbeats_id_seq,
  openlineage_events_id_seq
TO robot_dh_app;

COMMIT;
