# v1.5 `etl_shards` 对齐交接文档（已闭环 / 历史记录）

> 状态：**已 apply，闭环**（远端 `robot-dh-infra` 已经跑完 `33_pg_apply_etl_shards_align.sh` + `30_pg_v1_5_smoke_test.sh`；主项目模型已对齐并通过 `tests/test_warehouse_service.py`）。保留本文档作为字段约定与历史参考，**不需要再次执行**。
> 面向：本地 WSL 上的主项目 `robot-data-harness`
> 关联远端仓库：`robot-dh-infra`（已在远端服务器 `/opt/robot-dh-infra` 部署）
> 关联 Argo 工作流：`robot-dh-scale30-etl-g29nh`（命名空间 `robot-dh`）
> 编写时间：2026-05-23

## 1. 背景

在 scale30 ETL workflow `robot-dh-scale30-etl-g29nh` 跑测过程中，主项目通过 SQLAlchemy 写入远端 PostgreSQL 时，反复出现 soft-mode 警告：

```text
warehouse record_etl_shard failed (continuing in soft mode):
(psycopg.errors.UndefinedColumn) column etl_shards.shard_index does not exist
```

定位结论：远端 `etl_shards` 表的 schema 与主项目的 SQLAlchemy 模型存在结构性漂移。漂移的源头在 `robot-dh-infra` 仓库的 `postgres/migrations/002_v1_5_scale_benchmark.sql`，本次本仓库已经做了对齐修复。

本文档面向主项目（WSL 端）说明：

- 远端 `etl_shards` 当前的字段约定
- 主项目侧需要做 / 不需要做的事
- 三个相邻但责任不同的修复点的边界

## 2. 远端 `etl_shards` 字段约定（当前已对齐）

```sql
CREATE TABLE etl_shards (
  id            bigserial PRIMARY KEY,
  plan_id       text NOT NULL,
  shard_id      text NOT NULL,        -- ★ 'plan-<ts>-<hash>::shard-<idx>' 复合字符串
  shard_index   int,                  -- ★ 0-based 序号
  shard_uri     text,                 -- 兼容字段，主项目不写不读
  dataset_count int,
  input_bytes   bigint,
  status        text NOT NULL,
  assigned_worker text,               -- 兼容字段，主项目不写不读
  started_at    timestamptz,
  finished_at   timestamptz,
  duration_sec  double precision,     -- ★
  succeeded     int,                  -- ★
  failed        int,                  -- ★
  skipped       int,                  -- ★
  summary_uri   text,                 -- ★ shard_summary.json 的 s3 uri
  error_message text,                 -- ★ FAIL 时填错误摘要
  metrics_json  jsonb,
  created_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (plan_id, shard_id)
);
```

打 ★ 的 7 个字段是本次新增 / 类型修正的列。

兼容字段 `shard_uri` / `assigned_worker` 仍然保留：

- 主项目当前 **不写不读**，可以安全忽略
- 数据库层为了不破坏可能存在的旧 INSERT / 旧脚本而保留
- 主项目侧不需要在 SQLAlchemy 模型里映射这两列；如果 SQLAlchemy 模型已经显式声明了这两列，可以保留映射或删除映射，都不影响

## 3. 主项目（WSL 端）需要做的事

### 3.1 必须做：等远端跑完 schema 对齐脚本

远端管理员会执行：

```bash
cd /opt/robot-dh-infra
./scripts/33_pg_apply_etl_shards_align.sh
```

执行完成后 `etl_shards` 即对齐到 §2 的字段集合。本地无需做任何 DDL，也不要再尝试用 `robot_dh_app` 自己跑 `ALTER TABLE`——它没有 owner 权限，注定失败。

### 3.2 必须做：确认 SQLAlchemy 模型与字段集合一致

确保主项目 `EtlShard` 模型至少包含以下列（顺序不限）：

```python
id, plan_id, shard_id, shard_index, dataset_count, input_bytes, status,
started_at, finished_at, duration_sec, succeeded, failed, skipped,
summary_uri, error_message, metrics_json, created_at
```

关键约束：

- `shard_id` 在模型里要明确声明为 `String` / `Text`，不要按 int 处理
- 写入时 `shard_id` 用 `f"{plan_id}::shard-{idx:03d}"` 复合字符串，与日志中观察到的格式一致
- `shard_index` 是 int，与 `shard_id` 末尾的 `idx` 数值一致；这一列只是为了排序 / 排错冗余，不强制和 `shard_id` 一一推导

### 3.3 建议做：去掉 soft-mode 对 UndefinedColumn 的吞没

当前 `warehouse record_etl_shard` 用 soft-mode 把所有异常打成 WARNING 继续跑。建议：

- 保留 soft-mode 对网络抖动 / 暂时性连接错误的容忍
- 但把 `psycopg.errors.UndefinedColumn` / `psycopg.errors.UndefinedTable` 这类**确定性 schema 异常**升级成 ERROR 或直接 raise

否则下次 schema 再漂移，仍然要等到工作流跑完之后看日志才能发现。

### 3.4 不要做：在主项目里给远端表 DDL

`robot_dh_app` 账号被设计为应用层账号，**没有** DDL 权限。任何 `ALTER TABLE` / `CREATE TABLE` 都应该走 `robot-dh-infra` 仓库的 migration 文件，由 admin 账号在远端执行。

## 4. 三个相邻问题的边界

scale30 ETL 排查过程中暴露了 3 件事，请分别在对应的仓库 / 模块里推进：

| # | 问题 | 责任落点 | 当前状态 |
|---|------|---------|----------|
| A | `bridgedata_v2_scale30` 的 normalize 失败：`Unable to extract pose episodes ... Add an explicit adapter mapping for this dataset schema.` | **`robot-data-harness` 主项目**：normalize 适配器需要补 OXE BridgeData V2 的 pose / action / state 列映射 | 待主项目修 |
| B | `etl_shards` schema 漂移（本文档主题） | **`robot-dh-infra`** | **已在本仓库修复**：002 改 + 003 补 + 33 脚本 |
| C | `benchmark_cases` 旧列 `passed` vs 新列 `match`；exporter 需要 `COALESCE(match, passed)` 聚合 | **主项目改模型 → 本仓库补迁移 → exporter 改聚合**，三方接力 | 待主项目先定义 `match` 列类型 / 语义，本仓库再补 `004_benchmark_cases_match.sql` |

A 和 B 互相独立：B 修完之前 A 仍然会失败，A 修完之后 shard-002 才有可能跑通。失败 shard 不会因为 schema 对齐而变成 SUCCESS。

## 5. 远端落地步骤回放（运维侧已执行 / 待执行）

> 仅供本地排错时对齐时间线，不需要在 WSL 端复述。

```bash
cd /opt/robot-dh-infra

# 1. 全新环境：002 现在直接建对齐后的 etl_shards
./scripts/29_pg_apply_v1_5_schema.sh

# 2. 老环境：跑 003 把已有 etl_shards 升级到对齐 schema
./scripts/33_pg_apply_etl_shards_align.sh

# 3. smoke：用主项目期望的列写入 + 删除，确认应用账号 GRANT 已就绪
./scripts/30_pg_v1_5_smoke_test.sh
```

`33_pg_apply_etl_shards_align.sh` 的关键行为：

- 用 admin 账号 (`POSTGRES_USER`) 跑，绕开 `robot_dh_app` 无 DDL 权限的限制
- 通过 `PGOPTIONS="-c robot_dh.app_user=$ROBOT_DH_APP_USER"` 注入 GUC，让 migration 内的 `DO` 块自动给应用账号 `GRANT SELECT/INSERT/UPDATE/DELETE` + 序列权限
- 全程幂等：列已对齐时只打印 NOTICE / 跳过

## 6. 本仓库改动清单（对账用）

| 文件 | 改动 |
|------|------|
| `postgres/migrations/002_v1_5_scale_benchmark.sql` | `etl_shards` 改成与主项目对齐的列集合；`shard_id` 从 `int NOT NULL` 改为 `text NOT NULL`；新增 7 列 |
| `postgres/migrations/003_v1_5_etl_shards_align.sql` | **新增**：针对已经跑过旧版 002 的环境，做幂等 `ALTER COLUMN TYPE` + `ADD COLUMN IF NOT EXISTS` + `GRANT` |
| `scripts/33_pg_apply_etl_shards_align.sh` | **新增**：用 admin 账号执行 003 的入口脚本，参考 29 的写法 |
| `scripts/30_pg_v1_5_smoke_test.sh` | `etl_shards` 的 INSERT 改用新列（`shard_id` 字符串 + `shard_index` + `succeeded/failed/skipped` 等） |
| `README.md` 10.7.3 / 10.7.6 / 13 | 加 `etl_shards` 字段约定、升级路径与 33 脚本在验收清单中的位置 |

## 7. FAQ

**Q1：旧的 `etl_shards` 数据会丢吗？**
A：跑 003 之前 `etl_shards` 在 soft-mode 下基本写不进数据（旧 schema `shard_id int NOT NULL` 拒绝主项目传的 text），所以表本身应当是空的或近似空。003 里的 `ALTER COLUMN TYPE shard_id TYPE text USING shard_id::text` 即便有少量旧 int 数据，也只是把它们的 `shard_id` 当作字符串形式保留，原行不会丢。

**Q2：主项目 SQLAlchemy 模型里也有 `shard_uri` / `assigned_worker` 怎么办？**
A：保留映射也行、删除映射也行。远端列还在，SELECT 出来就是 `NULL`；INSERT 不指定这两列也合法。

**Q3：什么时候在远端跑 33？**
A：在主项目重跑任何 ETL workflow **之前**跑一次。跑完后远端 `etl_shards` 即对齐到主项目模型，soft-mode WARNING 应消失。

**Q4：如果 33 报错怎么办？**
A：典型报错只有两类：
- `robot-dh-postgres` 容器未启动 → 先跑 `./scripts/04_up.sh`
- `.env` 里 `ROBOT_DH_APP_USER` 缺失 → 33 会 `:= ${ROBOT_DH_APP_USER:?...}` 报错；从 `./scripts/03_generate_env.sh` 重新生成

**Q5：还要不要先备份？**
A：脚本本身幂等且只对 `etl_shards` 单表做 ALTER；如果对生产数据有顾虑，建议在跑 33 之前先：

```bash
./scripts/07_backup_postgres.sh
```

备份会落在 `/data/robot-dh/postgres/backups/`，可用 `09_restore_postgres.sh` 回滚。

---

如有任何字段或语义疑问，直接 ping 本仓库维护方（`robot-dh-infra`），不要在本地强行 DDL。
