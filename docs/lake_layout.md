# robot-dh v1.4 数据湖布局规范

本文档描述 `robot-dh-infra` 在 v1.4 阶段为 `robot-data-harness` 数据湖 / ETL 层准备的 bucket、prefix 和元数据布局。基础设施只负责"承载"这些约定，不负责执行业务 ETL；具体 Python ETL 仍由 WSL 主项目 `robot-data-harness` 实现。

## 1. 分层模型

数据湖采用经典四层 + 血缘 + 临时区结构：

| 层级 | 定位 | 主要输入 | 主要输出 |
|------|------|----------|----------|
| `raw` | 原始机器人数据资产，保持上游原貌 | 上游数据集（HuggingFace、OpenXLab、自采集等） | `endpose.pt`、`video.mp4`、`meta.yaml`、`_manifest.json` |
| `ods` | 标准化明细层，把异构原始数据转成统一 schema 的列存 | `raw/{dataset_id}/{version}/` | `pose.parquet`、`video_meta.parquet`、`episode_meta.parquet`、`_manifest.json` |
| `dwd` | 清洗 + 特征层，按 episode / segment 聚合，写入业务特征 | `ods/{dataset_id}/{version}/` | `pose_feature.parquet`、`press_event.parquet`、`trajectory_segment.parquet`、`episode_feature.parquet`、`_manifest.json` |
| `ads` | 应用指标层，按报告/看板维度汇总，给 quality gate / dashboard 使用 | `dwd/{dataset_id}/{version}/`、`quality_snapshots` | `dataset_quality_summary.parquet`、`validator_failure_stats.parquet`、`episode_quality_score.parquet` |
| `lineage` | 事件型血缘日志，按日分区写入 | ETL 作业事件 | `events/yyyy/mm/dd/*.jsonl` |
| `tmp` | 作业临时区，按 `run_id` 隔离，可随时清理 | ETL 作业中间产物 | `tmp/{run_id}/` |

约束：

- 任何下游层都禁止写回上游层。
- `raw` 是只追加层，不删除、不覆盖，依赖 MinIO bucket versioning 兜底。
- `ods` / `dwd` / `ads` 允许通过新版本号 `version` 重算覆盖，旧版本通过 MinIO versioning 保留。
- `tmp` 内对象可由 ETL 自行清理；基础设施不保证保留。

## 2. Bucket 和 prefix 规范

v1.4 数据湖统一使用 bucket `robot-lake`（在 `.env` 中由 `ROBOT_DH_LAKE_BUCKET` 配置）。`robot-datasets` 继续作为 v1.3 原始数据集集中地，不会被替换或迁移。

```text
robot-lake/
  raw/
    {dataset_id}/{version}/
      endpose.pt
      video.mp4
      meta.yaml
      _manifest.json
  ods/
    {dataset_id}/{version}/
      pose.parquet
      video_meta.parquet
      episode_meta.parquet
      _manifest.json
  dwd/
    {dataset_id}/{version}/
      pose_feature.parquet
      press_event.parquet
      trajectory_segment.parquet
      episode_feature.parquet
      _manifest.json
  ads/
    quality/
      dataset_quality_summary.parquet
      validator_failure_stats.parquet
      episode_quality_score.parquet
  lineage/
    events/
      yyyy/mm/dd/*.jsonl
  tmp/
    {run_id}/
```

命名约束：

- `{dataset_id}` 推荐使用 lower kebab-case（例：`droid`、`bridgedata-v2`、`robomimic`）。
- `{version}` 推荐使用 `vYYYYMMDD` 或 `vYYYYMMDD-N`，避免空格、冒号和反斜杠。
- `{run_id}` 推荐携带 ETL 作业类型和时间戳，例：`ods_pose_20260522_041500_ab12`。
- 所有 prefix 都以 `.keep` 占位对象兜底，避免 MinIO 控制台出现空目录被回收的错觉。

## 3. PostgreSQL 元数据模式

`robot-lake` 中的对象不做主索引；所有可被检索、回放、归档的资产都必须在 PostgreSQL 中登记。

`postgres/migrations/001_lake_metadata.sql` 增量创建以下表（不破坏 v1.3 已有业务表）：

| 表 | 作用 |
|----|------|
| `lake_assets` | 单个对象级元数据（uri / size / row_count / checksum 等） |
| `etl_jobs` | ETL 作业运行记录（job_type / status / metrics_json） |
| `lineage_edges` | 资产之间的血缘边（source_uri → target_uri） |
| `dataset_versions` | 数据集版本聚合（raw / ods / dwd uri 一行） |
| `quality_snapshots` | quality gate 结果快照（quality_status / score / metrics_json） |

约定：

- 所有时间字段使用 `timestamptz`，存 UTC。
- `metrics_json` 是 `jsonb`，结构由 ETL 自行约定，不强制 schema。
- `lake_assets.uri` 是唯一索引；同一个 uri 不要重复登记。
- `dataset_versions` 在 `(dataset_id, version)` 上唯一，便于按版本聚合查询。

## 4. `_manifest.json` 建议字段

每个 `raw` / `ods` / `dwd` 资产目录都建议落一个 `_manifest.json`，让数据湖在没有 PostgreSQL 的情况下也能自描述。

推荐字段：

```jsonc
{
  "dataset_id": "droid",
  "version": "v20260521",
  "layer": "ods",
  "created_at": "2026-05-22T04:15:00Z",
  "source_uris": [
    "s3://robot-lake/raw/droid/v20260521/"
  ],
  "row_counts": {
    "pose.parquet": 123456,
    "video_meta.parquet": 1024,
    "episode_meta.parquet": 256
  },
  "checksums": {
    "pose.parquet": "sha256:...",
    "video_meta.parquet": "sha256:...",
    "episode_meta.parquet": "sha256:..."
  },
  "schema_version": "ods.v1"
}
```

注意：

- `schema_version` 必须存在，并和主项目 `robot-data-harness` 中的 schema 注册表对齐。
- `source_uris` 至少包含直接上游 uri；多上游 join 时全部列出，便于和 `lineage_edges` 对账。
- 任何字段缺失都不阻塞写入，但下游 ETL / quality gate 应该有规则告警。

## 5. 访问控制

MinIO 应用账号 `MINIO_APP_ACCESS_KEY` 通过两条 policy 组合获得权限：

- `robot-dh-readwrite`：覆盖 `robot-datasets` / `robot-dh-artifacts` / `robot-dh-backups`，由 v1.3 已有 policy 保留。
- `robot-dh-lake-readwrite`：覆盖 `robot-lake`，由 v1.4 新增。具体定义见 `minio/policies/robot_dh_lake_readwrite.json`。

PostgreSQL 应用账号 `robot_dh_app` 的库级 / schema 级权限由 v1.3 已有 init 脚本授予；v1.4 新表创建在 `public` schema，应用账号默认就可以读写。

## 6. 与脚本的对应关系

| 脚本 | 关注的 prefix / 表 |
|------|--------------------|
| `scripts/18_setup_lake_buckets.sh` | 创建 `robot-lake` bucket、开启 versioning、写入 `raw/.keep` 等占位对象、应用 lake policy |
| `scripts/19_audit_lake_layout.sh` | 检查 bucket、占位对象、versioning 状态、PostgreSQL 元数据表、`/data/robot-dh` 磁盘 |
| `scripts/20_list_remote_assets.sh` | 扫描 `robot-datasets/raw/` 和 `robot-lake/raw/`，输出候选 dataset 目录 + endpose/video/meta 检查 |
| `scripts/21_pg_apply_lake_schema.sh` | 应用 `001_lake_metadata.sql`，幂等增量 |
| `scripts/22_pg_lake_smoke_test.sh` | 用应用账号在五张元数据表上插入 + 删除一条 smoke 记录 |
| `scripts/23_minio_lake_smoke_test.sh` | 用应用 access key 在 `tmp/` 下写入再删除一个 smoke 对象，确认 lake policy 生效 |
| `scripts/24_export_lake_client_env.sh` | 渲染 lake 版客户端 env（脱敏默认，需 `--show-secrets` 写真实文件） |

更多操作步骤见 `docs/v1_4_infra_runbook.md`。
