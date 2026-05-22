# robot-data-harness v1.4 数据湖布局

本仓库将 v1.4 数据湖资产写入四层布局，同时保持 v1.3 的校验、注册表与 artifact store 行为不变。

## 分层

```text
raw/{dataset_id}/{version}/
ods/{dataset_id}/{version}/
dwd/{dataset_id}/{version}/
ads/quality/
lineage/events/{yyyy}/{mm}/{dd}/
tmp/{run_id}/
```

`raw/` 仅追加（append-only），由采集或基础设施任务写入。ETL 从 raw 数据 bucket 或本地 raw 目录读取，只写下游层。

## Raw 输入

支持两种 raw 布局：

- v1.3 demo：`endpose.pt`，可选 `video.mp4`、可选 `meta.yaml`。
- HuggingFace 风格快照：`data/` 下 parquet，以及 robomimic 风格 HDF5。通用适配器抽取常见 7 维位姿向量；存在 `episode_id` 或 `episode_index` 列时保留 episode 身份。

当前远端 bucket 中已发现的数据集记录在 `configs/datasets.yaml`；`etl scan` 不要求硬编码 dataset ID。

## ODS 输出

`ods/{dataset_id}/{version}/` 包含：

- `pose.parquet`
- `video_meta.parquet`
- `episode_meta.parquet`
- `_manifest.json`

`pose.parquet` 为规范化位姿主表，可含多个 episode；`frame_idx` 在单个 episode 内局部编号。

## DWD 输出

`dwd/{dataset_id}/{version}/` 包含：

- `pose_feature.parquet`
- `press_event.parquet`
- `trajectory_segment.parquet`
- `episode_feature.parquet`
- `_manifest.json`

特征按 episode 抽取后，再写入该 dataset/version 切片的合并 parquet。

## ADS 输出

`ads/quality/` 包含：

- `dataset_quality_summary.parquet`
- `validator_failure_stats.parquet`
- `episode_quality_score.parquet`
- `_manifest.json`

ADS 跨 dataset 与 version 共享。

## Manifest 契约

每个 ETL 输出层写入 `_manifest.json`，字段包括：

- `dataset_id`、`version`、`layer`
- `created_at`、`schema_version`
- `source_uris`、`output_uri`
- `files[]`：`path`、`uri`、`format`、`size_bytes`、`row_count`、`checksum_sha256`
- `metrics`
- `job`
- `code`

用 `robot-dh lake manifest --uri <layer-uri>` 查看 manifest；用 `robot-dh lake audit` 检查 manifest 完整性。
