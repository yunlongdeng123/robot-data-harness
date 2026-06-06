-- v1.8 DIM 层：dataset 维度宽表。
-- dataset_key 由主项目生成，推荐 'dataset:<dataset_id>:<version>'。
-- 仅 CREATE TABLE / INDEX IF NOT EXISTS，幂等。
-- 与 postgres/migrations/006_v1_8_warehouse_quality_ops.sql 保持字段一致（远端 schema 真源）。

CREATE TABLE IF NOT EXISTS dim_dataset (
  dataset_key text PRIMARY KEY,
  dataset_id text NOT NULL,
  version text NOT NULL,
  dataset_family text,
  source_uri text,
  raw_uri text,
  ods_uri text,
  dwd_uri text,
  ads_uri text,
  ml_ready_uri text,
  first_seen_at timestamptz,
  latest_status text,
  latest_quality_score double precision,
  is_active boolean DEFAULT TRUE,
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dim_dataset_dataset_id_version
  ON dim_dataset (dataset_id, version);
CREATE INDEX IF NOT EXISTS idx_dim_dataset_dataset_family
  ON dim_dataset (dataset_family);
CREATE INDEX IF NOT EXISTS idx_dim_dataset_latest_status
  ON dim_dataset (latest_status);
