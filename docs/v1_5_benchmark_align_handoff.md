# v1.5 benchmark_cases / benchmark_runs 对齐交接文档

> 面向：本地 WSL 上的主项目 `robot-data-harness`
> 关联远端仓库：`robot-dh-infra`（已在远端服务器 `/opt/robot-dh-infra` 部署）
> 关联工作流：benchmark suite workflow（Argo `robot-dh` 命名空间）
> 编写时间：2026-05-23
> 前置：`etl_shards` 对齐已落地，详见 [`v1_5_etl_shards_align_handoff.md`](v1_5_etl_shards_align_handoff.md)

## 1. 背景

主项目反馈：`etl_shards` 已经在本仓库的 002 / 003 + `33_pg_apply_etl_shards_align.sh` 落地后对齐通过；但远端 `benchmark_cases` / `benchmark_runs` 仍是 002 早期版本的字段集，运行 benchmark workflow 时会再次出现与 v1.5 etl_shards 类似的 `UndefinedColumn` 漂移——并且主项目侧的 schema 错误已经从 soft-mode 升级成 `V15SchemaMissingError`，再漂移会直接抛错而不是被吞没。

本次本仓库按主项目的字段清单一次性补齐：

- `benchmark_cases` 加 4 列：`mutation / match / duration_sec / error_message`
- `benchmark_runs` 加 6 列：`suite_path / total_cases / passed / failed / mismatched / report_uri`
- 旧列 `passed (boolean)` / `mutation_type (text)` **保留**作为兼容字段，给 exporter `COALESCE(...)` 聚合使用
- 历史数据回填：`match <- passed`、`mutation <- mutation_type`（仅在新列 `IS NULL` 时回填，幂等）

## 2. 远端对齐后字段约定

### 2.1 `benchmark_runs`

```sql
CREATE TABLE benchmark_runs (
  id           bigserial PRIMARY KEY,
  benchmark_id text NOT NULL UNIQUE,
  suite_name   text NOT NULL,
  suite_path   text,                  -- ★ suite YAML 的 URI
  status       text NOT NULL,
  started_at   timestamptz,
  finished_at  timestamptz,
  duration_sec double precision,
  total_cases  int,                   -- ★
  passed       int,                   -- ★ case 级聚合计数
  failed       int,                   -- ★
  mismatched   int,                   -- ★
  report_uri   text,                  -- ★ HTML 报告 URI（robot-dh-artifacts/...）
  metrics_json jsonb,
  created_at   timestamptz NOT NULL DEFAULT now()
);
```

打 ★ 的 6 列是本次新增。

### 2.2 `benchmark_cases`

```sql
CREATE TABLE benchmark_cases (
  id            bigserial PRIMARY KEY,
  benchmark_id  text NOT NULL,
  case_id       text NOT NULL,
  dataset_uri   text,
  mutation_type text,                 -- 兼容列：旧 mutation 名称
  mutation      text,                 -- ★ 主项目新口径的 mutation 名称
  expected_status text,
  actual_status  text,
  expected_failed_validators jsonb,
  actual_failed_validators   jsonb,
  passed        boolean,              -- 兼容列：旧布尔
  match         boolean,              -- ★ 新口径，nullable
  duration_sec  double precision,     -- ★
  error_message text,                 -- ★
  metrics_json  jsonb,
  artifacts_uri text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (benchmark_id, case_id)
);
```

打 ★ 的 4 列是本次新增。

`match` 三态语义（远端这次定义与主项目反馈完全一致，作为 schema 注释固化在 `002` / `004` 迁移文件里）：

- `TRUE` = `actual_status` 与 `expected_status` 匹配，且 `expected_failed_validators` 为空或是 `actual_failed_validators` 的子集
- `FALSE` = 不匹配或 case 运行异常
- `NULL` = 未知 / 历史数据未回填

兼容字段 `passed` / `mutation_type`：

- **不删除**，作为 exporter 聚合的兜底
- 主项目侧新写入只写新列（`match` / `mutation`）
- exporter 用 `COALESCE(match, passed)` / `COALESCE(mutation, mutation_type)` 聚合，避免补列后旧 benchmark 历史变 unknown

## 3. 主项目（WSL 端）需要做的事

### 3.1 必须做：等远端跑完 schema 对齐脚本

远端管理员会执行：

```bash
cd /opt/robot-dh-infra
./scripts/34_pg_apply_benchmark_align.sh
```

执行完成后 `benchmark_cases` / `benchmark_runs` 即对齐到 §2 的字段集合。本地无需做任何 DDL。

### 3.2 必须做：确认 SQLAlchemy 模型与字段集合一致

`BenchmarkRun` 模型至少包含：

```python
id, benchmark_id, suite_name, suite_path, status,
started_at, finished_at, duration_sec,
total_cases, passed, failed, mismatched, report_uri,
metrics_json, created_at
```

`BenchmarkCase` 模型至少包含：

```python
id, benchmark_id, case_id, dataset_uri,
mutation_type, mutation,
expected_status, actual_status,
expected_failed_validators, actual_failed_validators,
passed, match, duration_sec, error_message,
metrics_json, artifacts_uri, created_at
```

关键约束：

- `match` 字段必须声明为 `Boolean(nullable=True)`，不要使用 `Boolean(nullable=False, default=False)`，否则 NULL 历史会被默认填成 False，污染 mismatch 计数
- `mutation` / `mutation_type` 可以并存在模型中，写入时主项目只写 `mutation`；如果暂不维护 `mutation_type` 映射，模型里可省略它，无影响
- benchmark_runs 的 `passed / failed / mismatched / total_cases` 是 int 计数，与 case 表的 `passed (boolean)` 不冲突（不同列名作用域，PG 不会混淆）

### 3.3 建议做：exporter 走 `COALESCE` 聚合

如果 exporter 的指标里有按 `match` 计算 mismatch ratio：

```sql
SELECT
  benchmark_id,
  COUNT(*) FILTER (WHERE COALESCE(match, passed) IS TRUE)   AS matched,
  COUNT(*) FILTER (WHERE COALESCE(match, passed) IS FALSE)  AS mismatched,
  COUNT(*) FILTER (WHERE COALESCE(match, passed) IS NULL)   AS unknown
FROM benchmark_cases
GROUP BY benchmark_id;
```

同理 mutation 维度：

```sql
SELECT COALESCE(mutation, mutation_type) AS mutation_label, ...
```

这样新旧数据在同一份指标里都计入，避免老 benchmark 历史变成 unknown。

### 3.4 不要做：在主项目里给远端表 DDL

仍然走 `robot-dh-infra` 仓库的 migration 文件，由 admin 账号在远端执行。`robot_dh_app` 没有 owner 权限，主项目里跑 `ALTER TABLE` 会被 PG 拒绝。

## 4. 远端落地步骤回放

```bash
cd /opt/robot-dh-infra

# 1. 全新环境：002 现在直接建对齐后的 benchmark_cases / benchmark_runs
./scripts/29_pg_apply_v1_5_schema.sh

# 2. 老环境：跑 004 把已有 benchmark_* 升级到对齐 schema
./scripts/34_pg_apply_benchmark_align.sh

# 3. smoke：用主项目期望的列写入 + 删除，确认应用账号 GRANT 已就绪
./scripts/30_pg_v1_5_smoke_test.sh
```

`34_pg_apply_benchmark_align.sh` 的关键行为：

- 用 admin 账号 (`POSTGRES_USER`) 跑，绕开 `robot_dh_app` 无 DDL 权限的限制
- 通过 `PGOPTIONS="-c robot_dh.app_user=$ROBOT_DH_APP_USER"` 注入 GUC，让 migration 内的 `DO` 块自动给应用账号 `GRANT SELECT/INSERT/UPDATE/DELETE` + 序列权限
- 全程幂等：列已对齐时只命中 `ADD COLUMN IF NOT EXISTS` 的 no-op 分支；`UPDATE ... WHERE new_col IS NULL` 只回填一次

## 5. 本仓库改动清单（对账用）

| 文件 | 改动 |
|------|------|
| `postgres/migrations/002_v1_5_scale_benchmark.sql` | `benchmark_cases` 加 `mutation / match / duration_sec / error_message`；`benchmark_runs` 加 `suite_path / total_cases / passed / failed / mismatched / report_uri`；新增 `(benchmark_id, match)` 索引 |
| `postgres/migrations/004_v1_5_benchmark_align.sql` | **新增**：针对已经跑过 002 早期版本的环境，做幂等 `ADD COLUMN IF NOT EXISTS` + `UPDATE` 回填 + `GRANT` |
| `scripts/34_pg_apply_benchmark_align.sh` | **新增**：用 admin 账号执行 004 的入口脚本，写法与 29 / 33 对称 |
| `scripts/30_pg_v1_5_smoke_test.sh` | `benchmark_runs` / `benchmark_cases` 的 INSERT 同时演练新列 + 兼容列 |
| `README.md` 10.7.3 / 10.7.6 / 13 | 加 `benchmark_*` 字段约定、升级路径、`34` 入验收清单 |

## 6. 与 etl_shards 对齐的差异点

| 维度 | `etl_shards`（003） | `benchmark_*`（004） |
|------|---------------------|----------------------|
| 类型变更 | `shard_id int -> text` | 无 |
| 新增列 | 7 列 | benchmark_cases 4 列 + benchmark_runs 6 列 |
| 兼容列 | `shard_uri / assigned_worker` 保留但主项目不读不写 | `passed / mutation_type` 保留并由 exporter 用 `COALESCE` 兜底 |
| 历史数据回填 | 不需要（旧表为空） | 需要：`match <- passed`、`mutation <- mutation_type` |
| 入口脚本 | `33_pg_apply_etl_shards_align.sh` | `34_pg_apply_benchmark_align.sh` |

最大的差异是 **004 多了 `UPDATE` 回填**：因为 benchmark_cases 旧表的 `passed` / `mutation_type` 已经有真实历史数据，不能像 etl_shards 那样视为空表直接重命名 / 转换。回填只在新列 `IS NULL` 时执行，重复跑也不会污染。

## 7. FAQ

**Q1：旧的 `benchmark_cases` / `benchmark_runs` 历史数据会丢吗？**
A：不会。004 只做 `ADD COLUMN IF NOT EXISTS` + `UPDATE WHERE new_col IS NULL`，没有 `DROP` / 类型变更 / `DELETE`。

**Q2：跑完 004 之后，旧 exporter 还能继续工作吗？**
A：能。旧列 `passed` / `mutation_type` 仍在并继续被写入兼容；除非主项目完全停止写旧列，否则旧 SELECT 一样能取到值。建议 exporter 同步切到 `COALESCE(...)`，让新旧 row 都计入。

**Q3：如果主项目模型还没声明 `match` / `mutation`，可以先跑 004 吗？**
A：可以。004 只动 schema，不要求写入端立即换。主项目可以分两阶段：先跑 004 → 远端表对齐；再合并主项目代码切到新字段。

**Q4：什么时候在远端跑 34？**
A：建议在主项目 benchmark workflow 下一次启动**之前**跑一次。新旧环境都安全：新环境 no-op，老环境对齐 + 回填。

**Q5：还要不要先备份？**
A：建议。`benchmark_cases` / `benchmark_runs` 有真实历史。先跑：

```bash
./scripts/07_backup_postgres.sh
```

备份会落在 `/data/robot-dh/postgres/backups/`，可用 `09_restore_postgres.sh` 回滚。

---

如有任何字段或语义疑问，直接 ping 本仓库维护方（`robot-dh-infra`），不要在本地强行 DDL。
