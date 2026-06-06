-- 物化 fact_asset_profile：从 asset_profiles 拉行。
-- asset_profile_key = md5(profile_id)。
-- 参数：{{ schema }} / {{ start_date }} / {{ end_date }}

INSERT INTO {{ schema }}.fact_asset_profile (
    asset_profile_key,
    profile_id,
    dataset_id,
    version,
    dataset_family,
    asset_uri,
    asset_format,
    layer,
    bytes,
    rows,
    files_count,
    episodes_count,
    videos_count,
    schema_hash,
    null_rate,
    status,
    dt,
    created_at
)
SELECT
    md5(ap.profile_id)                                                     AS asset_profile_key,
    ap.profile_id,
    ap.dataset_id,
    ap.version,
    ap.dataset_family,
    ap.asset_uri,
    ap.asset_format,
    ap.layer,
    ap.bytes,
    ap.rows,
    ap.files_count,
    ap.episodes_count,
    ap.videos_count,
    ap.schema_hash,
    ap.null_rate,
    ap.status,
    (ap.created_at AT TIME ZONE 'UTC')::date                               AS dt,
    now()                                                                  AS created_at
FROM {{ schema }}.asset_profiles ap
WHERE (ap.created_at AT TIME ZONE 'UTC')::date BETWEEN
        CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
ON CONFLICT (asset_profile_key) DO UPDATE SET
    bytes             = EXCLUDED.bytes,
    rows              = EXCLUDED.rows,
    files_count       = EXCLUDED.files_count,
    episodes_count    = EXCLUDED.episodes_count,
    videos_count      = EXCLUDED.videos_count,
    schema_hash       = EXCLUDED.schema_hash,
    null_rate         = EXCLUDED.null_rate,
    status            = EXCLUDED.status,
    created_at        = now();
