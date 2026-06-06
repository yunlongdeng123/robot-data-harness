# robot-data-harness v1.4 交接（接收侧档案）

> 本文档是 **WSL 主项目侧** 的交接备忘。云端 `robot-dh-infra` 已完成 v1.4 基础设施（PostgreSQL 元数据表、MinIO `robot-lake` bucket、Redis、相关密钥与脚本），本仓库负责按"环境变量契约 + prefix 契约 + 元数据表契约"对接，**不负责基础设施**。
>
> 真正的设计文档（lake 分层、`_manifest.json`、回滚、健康检查）以 `docs/lake_layout.md`、`docs/v1_4_infra_runbook.md` 为准 —— 这两个文件在交接完成后会出现在 `docs/` 下。

## 0. 资料接收路径

| 类别 | 仓库内路径 | 是否入 git |
|---|---|---|
| env 模板 | `client/robot-dh-lake.env.example` | ✅ |
| env 真实密码版 | `~/.config/robot-dh/robot-dh-lake.env`（0600） | ❌ **绝不入 git** |
| 客户端 tunnel/doctor/checklist 脚本 | `client/wsl-*.sh`、`client/wsl-*.md` | ✅（仅模板/无密码版本） |
| K8s lake Secret 模板 | `client/k8s-lake-secret.example.yaml` | ✅ |
| 数据湖设计文档 | `docs/lake_layout.md` | ✅ |
| v1.4 运行手册 | `docs/v1_4_infra_runbook.md` | ✅ |
| PostgreSQL 元数据迁移（参考） | `postgres/migrations/001_lake_metadata.sql` | ✅ |
| MinIO 应用账号策略（参考） | `minio/policies/robot_dh_lake_readwrite.json` | ✅ |
| 当前云端资产快照 | `docs/remote_assets_*.json` | ❌（含内部对象列表，已在 `.gitignore` 中排除） |

> **本次交接的实际执行方式**：因 WSL → 云端 SSH 22 被本地代理 TUN 拦截（HTTP/HTTPS/PG/MinIO/Redis 端口全通），交接走了"**直连反查**"路径：
> - 真实密码 env 由 `client/robot-dh-public.env`（v1.3 公网模式，密码 v1.4 未轮换）+ 补 2 个 v1.4 变量手工合成 → `~/.config/robot-dh/robot-dh-lake.env`
> - 5 张元数据表用 `psql` 反查 `information_schema` + `pg_indexes` + `pg_constraint` → `postgres/migrations/001_lake_metadata.reconstructed.sql`
> - MinIO 策略从可见 bucket 列表 + 交接清单文字反推 → `minio/policies/robot_dh_lake_readwrite.reconstructed.json`
> - 资产清单用 `scripts/discover_remote_assets.py` (boto3) 重建 → `docs/remote_assets_<ts>.local-discovered.json`
> - 仍待 SSH 拷贝的纯设计文档：`docs/lake_layout.md`、`docs/v1_4_infra_runbook.md`、`client/wsl-access-checklist.md`

接收一键脚本：

```bash
scripts/fetch_v1_4_handoff.sh
scripts/verify_v1_4_handoff.sh
```

## 1. 环境变量契约（9 个，名字锁定）

主项目代码从环境读以下 9 个变量，**不允许在主项目内改名**：

| 变量 | 用途 | 备注 |
|---|---|---|
| `ROBOT_DH_DB_URI` | PostgreSQL 应用账号 DSN | 用户名固定 `robot_dh_app`，数据库固定 `robot_dh` |
| `ROBOT_DH_ARTIFACT_STORE` | artifact 后端类型 | 远端模式固定 `s3` |
| `ROBOT_DH_S3_ENDPOINT_URL` | MinIO S3 API endpoint | tunnel 模式 `http://127.0.0.1:19000`，公网模式 `http://82.156.129.81:9000` |
| `ROBOT_DH_S3_ACCESS_KEY` | MinIO 应用 access key | |
| `ROBOT_DH_S3_SECRET_KEY` | MinIO 应用 secret | |
| `ROBOT_DH_S3_DATA_BUCKET` | v1.3 数据 bucket | `robot-datasets` |
| `ROBOT_DH_S3_ARTIFACT_BUCKET` | v1.3 产物 bucket | `robot-dh-artifacts` |
| `ROBOT_DH_S3_LAKE_BUCKET` | **v1.4 新增** 数据湖 bucket | `robot-lake` |
| `ROBOT_DH_REDIS_URL` | Redis DSN | `redis://:password@host:port/0` |

> 主项目仍兼容 v1.3 的 7 变量（少了 `ROBOT_DH_S3_LAKE_BUCKET` 和 `ROBOT_DH_ARTIFACT_STORE` 也能跑本地 SQLite + local artifact 模式）；只要使用数据湖，必须凑齐 9 个。

## 2. 数据湖 prefix 契约（`robot-lake` bucket）

ETL 必须按这套 prefix 写入（详情见 `docs/lake_layout.md`）：

```text
robot-lake/raw/{dataset_id}/{version}/         # 上游原始资产（只追加）
robot-lake/ods/{dataset_id}/{version}/         # 标准化明细 parquet
robot-lake/dwd/{dataset_id}/{version}/         # 清洗 + 特征
robot-lake/ads/quality/                        # 应用指标
robot-lake/lineage/events/yyyy/mm/dd/*.jsonl   # 血缘事件
robot-lake/tmp/{run_id}/                       # ETL 临时区（ETL 自清理）
```

命名约束：

- `{dataset_id}`：**lower kebab-case**（例：`bridgedata-v2`）
- `{version}`：`vYYYYMMDD` 或 `vYYYYMMDD-N`

## 3. PostgreSQL 元数据表契约（接收侧反查实证）

云端已经创建并通过 smoke test，主项目直接 INSERT / SELECT，**不要再做迁移**。完整定义见 `postgres/migrations/001_lake_metadata.reconstructed.sql`（反查重建版，SSH 修好后用云端权威版覆盖）。

| 表 | 主要用途 | 唯一约束（实证） |
|---|---|---|
| `lake_assets` | 单对象元数据（uri/size/row_count/checksum/asset_type） | `uri` UNIQUE |
| `etl_jobs` | ETL 作业运行（含 `metrics_json` jsonb） | `job_id` UNIQUE |
| `lineage_edges` | `source_uri → target_uri` 血缘边 | **无唯一约束**（同一边可重复写入；ETL 自行去重） |
| `dataset_versions` | dataset 版本聚合（仅 raw/ods/dwd URI，**无 ads_uri**） | `(dataset_id, version)` UNIQUE |
| `quality_snapshots` | quality gate 结果快照（含 `metrics_json` jsonb） | **无唯一约束**（同 dataset+version 允许多次快照，按 created_at desc 取最新） |

实证发现的细节差异（与原始交接清单的口头描述对比）：

- `lake_assets.asset_type` 是 **NOT NULL**，但原清单未提及 → 主项目 ETL 必须显式赋值（建议字段值：`pose_parquet` / `video` / `manifest` / `feature` / `summary` / `raw_object`）
- `etl_jobs.status` 是 **NOT NULL** 且无默认值 → 调用方必须显式设置 `RUNNING / OK / WARN / FAIL`
- `dataset_versions` **没有** `ads_uri` 字段 → ads 是跨 dataset 共享的（`robot-lake/ads/quality/`），不在版本表里记
- `quality_snapshots` 没有 unique 约束 → 主项目查询时必须用 `ORDER BY created_at DESC LIMIT 1` 取最新

类型约定：

- 所有时间字段：`timestamptz` UTC
- `metrics_json`：`jsonb`，结构由 ETL 自行约定（见第 8 节 "待协商项 #3"）
- 整数主键：`bigserial`

## 4. 当前云端已有资产（接收侧实测）

由 `scripts/discover_remote_assets.py` 直连 MinIO 自动发现，结果落到 `docs/remote_assets_<ts>.local-discovered.json`（不入 git）。

| 来源 | 数据集 | 版本 | 对象数 |
|---|---|---|---|
| `robot-datasets/raw` | `bridgedata_v2` | `sample` | 5 |
| `robot-datasets/raw` | `droid` | `calibration` | 5 |
| `robot-datasets/raw` | `droid` | `lerobot_sample` | 24 |
| `robot-datasets/raw` | `robomimic` | `sample` | 9 |

> 与原始交接清单（说"3 套"）对比，实测**多发现 1 套** `droid/calibration`。

**所有可见 bucket**：`robot-datasets`、`robot-dh-artifacts`、`robot-dh-backups`、`robot-lake`

**`robot-lake/` 现状**：6 层 prefix 都已建好（`raw/ ods/ dwd/ ads/quality/ lineage/events/ tmp/`）。已经能看到 2 个 smoke 测试占位目录 `tmp/smoke_20260521_*`。

**重要提醒**：4 套资产都是 HuggingFace 风格（含 `.cache/huggingface/`、`*.parquet`、`*.hdf5`），**没有** v1.3 假设的 `endpose.pt / video.mp4 / meta.yaml` 三件套。主项目 ETL 在做 `raw → ods` 时要按 HuggingFace dataset 的 schema 解析（待协商项 #2）；同时 `endpose.pt + video.mp4 + meta.yaml` 走的是**本地 demo**（`samples/button_press_001` 由 `make demo-data` 生成）路径，两条 raw 风格主项目都得支持。

**dataset_id 命名实测**：云端是 **下划线风格**（`bridgedata_v2`、`droid`、`robomimic`），与原清单建议的"lower kebab-case"不一致。结论：主项目 ETL **以下划线风格为既成事实**，但在主项目 `configs/datasets.yaml` 里维护一份归一映射表（待协商项 #5）。

## 5. 接入模式

| 场景 | 模式 | host | 端口 |
|---|---|---|---|
| WSL 本地 CLI / FastAPI 调试 | SSH tunnel（默认推荐） | `127.0.0.1` | 15432 / 19000 / 16379 |
| kind 内 Pod / K8s Job | 公网白名单直连 | `82.156.129.81` 或公网 DNS | 5432 / 9000 / 6379 |

打通 tunnel：

```bash
./client/wsl-open-tunnels.sh   # 一键 4 端口（15432 / 19000 / 19001 / 16379）
```

切换公网模式的前置（在云端做，主项目无需操作）：

1. 云端 `.env` 改 `BIND_ADDR=0.0.0.0`、设 `TRUSTED_CIDR`、`POSTGRES_APP_TRUSTED_CIDRS`、`SSH_TRUSTED_CIDR`
2. `./scripts/04_up.sh`（同步 `pg_hba`）
3. `./scripts/12_firewall_plan.sh --apply`（UFW）
4. 云安全组白名单
5. `./scripts/24_export_lake_client_env.sh --mode public --show-secrets`

## 6. 运维硬约束（必须遵守）

- ❌ **禁止删除** `raw/` 层任何对象（只追加，覆盖通过 MinIO versioning 兜底）
- ❌ **禁止跨层反向写**（下游不写上游：dwd 不能写 ods，ads 不能写 dwd…）
- ❌ **禁止** `mc rb --force local/robot-lake`（会清空所有数据）
- ❌ **禁止** `DROP DATABASE robot_dh`（会破坏 v1.3 registry）
- ❌ 同一个 `lake_assets.uri` **不要重复登记**（唯一索引会报错）
- ✅ `tmp/{run_id}/` 由 ETL 自行清理（基础设施不保证保留）
- ✅ 真实密码 env 文件权限 `0600`，**绝对不入 git**（已在 `.gitignore` 兜底）

## 7. 健康检查 / 排障入口

主项目排障第一时间在云端跑（需 SSH）：

```bash
ssh ubuntu@82.156.129.81 '
  cd /opt/robot-dh-infra
  ./scripts/06_healthcheck.sh         # v1.3 容器/服务级
  ./scripts/19_audit_lake_layout.sh   # v1.4 数据湖布局级
  ./scripts/20_list_remote_assets.sh  # 当前云端有什么数据
'
```

WSL 侧排障：

```bash
source ~/.config/robot-dh/robot-dh-lake.env
./client/wsl-remote-doctor.sh         # tunnel + 三服务可达性 + 9 个环境变量
```

## 8. 待协商的开放项（与基础设施团队对齐后再写代码）

| # | 议题 | 当前状态 | 我方建议 |
|---|---|---|---|
| 1 | `_manifest.json` 字段是否强约束 | `docs/lake_layout.md` 是"建议"级 | 主项目 ETL 先定 `schema_version` + `checksums` 为硬要求；其余字段允许扩展 |
| 2 | HuggingFace 数据集 schema 映射 | 现有 3 套样本不符合 v1.3 三件套假设 | 由 ETL 团队定义 `raw → ods` 的字段映射规则，主项目要写一个 `HFDatasetAdapter` |
| 3 | `metrics_json` jsonb 结构 | 完全自由 | 至少先约定 `etl_jobs.metrics_json` 含 `{rows_in, rows_out, bytes_in, bytes_out, duration_ms}`；`quality_snapshots.metrics_json` 含 `{score, gate_passed, failed_checks: []}` |
| 4 | 血缘事件 schema | jsonl 每行字段未定 | 优先用 OpenLineage 子集，便于后续接 Marquez |
| 5 | `dataset_id` 命名归一 | `bridgedata_v2` vs `bridgedata-v2` 不一致 | 主项目维护权威 dataset 名单（`configs/datasets.yaml`），ETL 写入前归一化 |

## 9. 主项目代码改造清单（v1.4 适配）

下面这些改动 **要在拿到云端实际文件后做**，不要凭猜先动：

- [ ] `src/robot_dh/infra/` 增加 `ROBOT_DH_S3_LAKE_BUCKET` 配置项与 `infra doctor` 检查
- [ ] `src/robot_dh/registry/` 增加 SQLAlchemy 模型映射到 5 张元数据表（`lake_assets` 等）
- [ ] `src/robot_dh/lake/`（新模块）：实现 prefix 工具、URI 构造、HuggingFace adapter
- [ ] `k8s/secret.example.yaml` 增加 lake bucket key
- [ ] `client/wsl-remote-doctor.sh` 已有 9 变量列表，需对齐 `ROBOT_DH_S3_LAKE_BUCKET` 检查（云端版本已含，覆盖即可）
- [ ] `README.md`：升级到 v1.4 章节
- [ ] `tests/`：增加 lake URI 构造、prefix 合法性、HF adapter 单测

## 10. 一句话交接（备查）

> 9 个 `ROBOT_DH_*` 变量定契约；`robot-lake` 按 `raw/ods/dwd/ads/lineage/tmp` 分层；5 张元数据表只用不迁移；调试走 SSH tunnel、生产走公网白名单；raw 层只追不删；真实密码 env 永远在 `~/.config/robot-dh/` 且 0600。
