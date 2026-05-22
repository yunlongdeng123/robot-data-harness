-- 001_lake_metadata.reconstructed.sql
--
-- This file is a RECEIVER-SIDE RECONSTRUCTION of the v1.4 lake metadata schema
-- that was actually deployed on the cloud `robot_dh` database. It was produced
-- by reading information_schema + pg_indexes + pg_constraint directly against
-- 82.156.129.81:5432 from this WSL, because SSH to the infra host is currently
-- blocked by the local proxy TUN. Once SSH is restored, this file should be
-- replaced with (or diffed against) the authoritative
--   /opt/robot-dh-infra/postgres/migrations/001_lake_metadata.sql
--
-- Purpose for the main project (robot-data-harness):
--   * Source of truth for the SQLAlchemy models in src/robot_dh/warehouse/models.py
--   * Reference only - DO NOT execute this on the cloud robot_dh database.
--     Tables already exist (owner: robot_dh_admin); applying again will fail.
--
-- Discovered on:   2026-05-21T21:12 UTC
-- Server version:  PostgreSQL 16.14 (Alpine)
-- Database:        robot_dh
-- Schema:          public
-- Owner of v1.4 tables: robot_dh_admin
-- Application role queried with: robot_dh_app

BEGIN;

--------------------------------------------------------------------------------
-- 1. lake_assets
--    One row per object/dataset slice that ETL persisted into the lake.
--    `uri` is GLOBALLY UNIQUE - re-registering the same URI raises 23505.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.lake_assets (
    id          BIGSERIAL    PRIMARY KEY,
    dataset_id  TEXT         NOT NULL,
    version     TEXT         NOT NULL,
    layer       TEXT         NOT NULL,                  -- raw | ods | dwd | ads | lineage | tmp
    asset_type  TEXT         NOT NULL,                  -- free-form: e.g. "pose_parquet", "video", "manifest"
    uri         TEXT         NOT NULL UNIQUE,
    format      TEXT         NULL,
    size_bytes  BIGINT       NULL,
    row_count   BIGINT       NULL,
    checksum    TEXT         NULL,                      -- sha256 hex recommended
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_lake_assets_dataset_version_layer
    ON public.lake_assets (dataset_id, version, layer);

--------------------------------------------------------------------------------
-- 2. etl_jobs
--    One row per ETL run (normalize / build-features / build-ads / etl-run).
--    `job_id` is the caller-provided unique identifier; reusing it raises 23505.
--    `status` is NOT NULL with no default - caller must set RUNNING/OK/WARN/FAIL.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.etl_jobs (
    id            BIGSERIAL    PRIMARY KEY,
    job_id        TEXT         NOT NULL UNIQUE,
    job_type      TEXT         NOT NULL,                -- normalize | build_features | build_ads | etl_run | etl_scan
    input_uri     TEXT         NULL,
    output_uri    TEXT         NULL,
    status        TEXT         NOT NULL,                -- RUNNING | OK | WARN | FAIL
    started_at    TIMESTAMPTZ  NULL,
    finished_at   TIMESTAMPTZ  NULL,
    duration_sec  DOUBLE PRECISION NULL,
    error_message TEXT         NULL,
    metrics_json  JSONB        NULL,                    -- free-form; schema agreed in docs/v1_4_handoff_inbox.md
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_etl_jobs_job_type_status_created_at
    ON public.etl_jobs (job_type, status, created_at);

--------------------------------------------------------------------------------
-- 3. lineage_edges
--    Directed edge: source_uri -> target_uri produced by job_id (job_type).
--    NOTE: no uniqueness constraint - the same edge may legitimately appear
--    multiple times across reruns. ETL should de-duplicate at write time if
--    desired (recommended: ON CONFLICT DO NOTHING via composite unique upgrade
--    is NOT present in current schema; just check before inserting).
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.lineage_edges (
    id          BIGSERIAL    PRIMARY KEY,
    source_uri  TEXT         NOT NULL,
    target_uri  TEXT         NOT NULL,
    job_id      TEXT         NOT NULL,                  -- FK-style (text) to etl_jobs.job_id, no enforced FK
    job_type    TEXT         NOT NULL,
    run_id      TEXT         NULL,                      -- optional, ties back to v1.3 runs.run_id when applicable
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_source_uri
    ON public.lineage_edges (source_uri);
CREATE INDEX IF NOT EXISTS idx_lineage_edges_target_uri
    ON public.lineage_edges (target_uri);

--------------------------------------------------------------------------------
-- 4. dataset_versions
--    One row per (dataset_id, version) tuple; UNIQUE on both columns.
--    NOTE: only raw/ods/dwd URI columns exist. There is NO ads_uri column
--    (ads layer is shared across datasets at lake://ads/quality/).
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.dataset_versions (
    id          BIGSERIAL    PRIMARY KEY,
    dataset_id  TEXT         NOT NULL,
    version     TEXT         NOT NULL,
    raw_uri     TEXT         NULL,
    ods_uri     TEXT         NULL,
    dwd_uri     TEXT         NULL,
    status      TEXT         NULL,                      -- e.g. discovered | normalized | featurized | scored | failed
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (dataset_id, version)
);

--------------------------------------------------------------------------------
-- 5. quality_snapshots
--    One row per quality-gate evaluation snapshot.
--    NOTE: NO uniqueness on (dataset_id, version, run_id); ETL may write
--    multiple snapshots for the same (dataset, version) over time. Latest
--    snapshot is the one with the largest created_at.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.quality_snapshots (
    id              BIGSERIAL    PRIMARY KEY,
    dataset_id      TEXT         NOT NULL,
    version         TEXT         NOT NULL,
    run_id          TEXT         NULL,                  -- ties to etl_jobs.job_id or v1.3 runs.run_id
    quality_status  TEXT         NULL,                  -- e.g. PASS | WARN | FAIL
    quality_score   DOUBLE PRECISION NULL,              -- 0..100; see ads layer scoring
    metrics_json    JSONB        NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_quality_snapshots_dataset_version_created_at
    ON public.quality_snapshots (dataset_id, version, created_at);

COMMIT;

-- End of reconstructed v1.4 lake metadata schema.
