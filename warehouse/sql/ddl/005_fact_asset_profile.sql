-- v1.8 FACT：asset 画像事实表。
-- 与 v1.6 asset_profiles 差别：本表强制 layer + dt，方便按天聚合到 dws/ads。

CREATE TABLE IF NOT EXISTS fact_asset_profile (
  asset_profile_key text PRIMARY KEY,
  profile_id text,
  dataset_id text,
  version text,
  dataset_family text,
  asset_uri text,
  asset_format text,
  layer text,
  bytes bigint,
  rows bigint,
  files_count int,
  episodes_count int,
  videos_count int,
  schema_hash text,
  null_rate double precision,
  status text,
  dt date,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_asset_profile_dt_dataset_layer
  ON fact_asset_profile (dt, dataset_id, layer);
CREATE INDEX IF NOT EXISTS idx_fact_asset_profile_format_status
  ON fact_asset_profile (asset_format, status);
CREATE INDEX IF NOT EXISTS idx_fact_asset_profile_family_dt
  ON fact_asset_profile (dataset_family, dt);
