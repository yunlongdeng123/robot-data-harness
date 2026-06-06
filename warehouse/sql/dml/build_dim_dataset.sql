-- 物化 dim_dataset：从 dataset_versions / quality_snapshots / ml_ready_datasets 聚合，按 (dataset_id, version) 维度 UPSERT。
-- 参数：
--   {{ schema }}    -- 目标 schema，默认 public
--   {{ start_date }} / {{ end_date }} -- 在 dim 层不强制使用，但保留以便统一接口（dim 表是全量画像，未按日切片）

INSERT INTO {{ schema }}.dim_dataset (
    dataset_key,
    dataset_id,
    version,
    dataset_family,
    source_uri,
    raw_uri,
    ods_uri,
    dwd_uri,
    ads_uri,
    ml_ready_uri,
    first_seen_at,
    latest_status,
    latest_quality_score,
    is_active,
    updated_at
)
SELECT
    'dataset:' || dv.dataset_id || ':' || dv.version              AS dataset_key,
    dv.dataset_id,
    dv.version,
    qs.dataset_family                                              AS dataset_family,
    dv.raw_uri                                                     AS source_uri,
    dv.raw_uri,
    dv.ods_uri,
    dv.dwd_uri,
    NULL                                                           AS ads_uri,
    mlr.output_uri                                                 AS ml_ready_uri,
    dv.created_at                                                  AS first_seen_at,
    COALESCE(qs.quality_status, dv.status)                         AS latest_status,
    qs.quality_score                                               AS latest_quality_score,
    TRUE                                                           AS is_active,
    now()                                                          AS updated_at
FROM {{ schema }}.dataset_versions dv
LEFT JOIN LATERAL (
    SELECT
        quality_status,
        quality_score,
        (metrics_json ->> 'dataset_family')                        AS dataset_family
    FROM {{ schema }}.quality_snapshots
    WHERE dataset_id = dv.dataset_id
      AND version    = dv.version
    ORDER BY created_at DESC
    LIMIT 1
) qs ON TRUE
LEFT JOIN LATERAL (
    SELECT output_uri
    FROM {{ schema }}.ml_ready_datasets
    WHERE dataset_id = dv.dataset_id
      AND version    = dv.version
    ORDER BY created_at DESC
    LIMIT 1
) mlr ON TRUE
ON CONFLICT (dataset_key) DO UPDATE SET
    dataset_id            = EXCLUDED.dataset_id,
    version               = EXCLUDED.version,
    dataset_family        = COALESCE(EXCLUDED.dataset_family, dim_dataset.dataset_family),
    source_uri            = COALESCE(EXCLUDED.source_uri, dim_dataset.source_uri),
    raw_uri               = COALESCE(EXCLUDED.raw_uri, dim_dataset.raw_uri),
    ods_uri               = COALESCE(EXCLUDED.ods_uri, dim_dataset.ods_uri),
    dwd_uri               = COALESCE(EXCLUDED.dwd_uri, dim_dataset.dwd_uri),
    ads_uri               = COALESCE(EXCLUDED.ads_uri, dim_dataset.ads_uri),
    ml_ready_uri          = COALESCE(EXCLUDED.ml_ready_uri, dim_dataset.ml_ready_uri),
    latest_status         = EXCLUDED.latest_status,
    latest_quality_score  = EXCLUDED.latest_quality_score,
    updated_at            = now();
