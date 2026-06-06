# `etl_perf_runs` schema 对齐 + perf writer 容错需求

> 提交方：`robot-dh-infra`（云端 PostgreSQL / MinIO / Redis）
> 接收方：WSL 侧 `robot-data-harness` 主项目（`robot_dh.warehouse.service` / `robot_dh.perf.writer` / SQLAlchemy 模型）
> 优先级：P1（v1.6 `multisource-scale30` etl-phase 当前 100% FAIL，**业务 OK 但 perf record 写 PG 直接 abort 整个 step**，导致 Argo 不会跳到 transform）
> 关联：
>
> - [`docs/v1_6_bridgedata_v2_normalize_adapter_request.md`](v1_6_bridgedata_v2_normalize_adapter_request.md)（已闭环：A/B/C/D/E 5 类错误本次 jddlp 全部 ✅）
> - [`docs/v1_5_benchmark_align_handoff.md`](v1_5_benchmark_align_handoff.md)（与本次套路完全一致：v1.5 给 `benchmark_*` 加列；本次给 `etl_perf_runs` 加列）
> - 本次完整 5-step log 归档：[`docs/runs/20260524/robot-dh-multisource-scale30-jddlp/INDEX.md`](runs/20260524/robot-dh-multisource-scale30-jddlp/INDEX.md)

## 1. 背景

`robot-dh-multisource-scale30-jddlp` 是主项目 v1.6 修复 PR 上线后的**第一条端到端走通 normalize 业务的 workflow**。它把上一条 [`dls4z`](runs/20260524/robot-dh-multisource-scale30-dls4z/INDEX.md) 列出的 5 类错误（adapter / qc-contract / resume / partition_plan / botocore pool）全部 ✅ 消化掉，但同时曝出**之前被掩盖的可观测性 schema 漂移**：

```text
etl_run END:   job_id=etl-run-bridgedata_v2_scale30-v1-a7fcf7d3 status=OK duration=640.86s
warehouse record_etl_perf_run schema mismatch:
  (psycopg.errors.UndefinedColumn) column "started_at" of relation "etl_perf_runs" does not exist
[SQL: INSERT INTO etl_perf_runs (..., started_at, finished_at, metrics_json, created_at) VALUES (...)]
robot_dh.warehouse.service.V15SchemaMissingError: ...
  Apply the matching schema migration in the infra project first.
```

业务侧 `etl_run END status=OK`，ods 工件已经完整写入 `s3://robot-lake/ods/bridgedata_v2_scale30/v1/`（`output_rows=318`，`_manifest.json` 已落），但 `emit_perf_records` 抛 `V15SchemaMissingError`，进程退出码非 0，step pod FAIL，Argo 不会触发 transform。

这是**典型的"业务 OK，可观测性 KO"失败模式**，与 v1.5 `benchmark_*` / `etl_shards` 漂移同源。

## 2. infra 端实测：`etl_perf_runs` 缺 2 列

```bash
docker exec robot-dh-postgres bash -lc \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\d etl_perf_runs"'
```

实测列集（infra 端 002 创建，003/004/005 没动它）：

```text
id, job_id, run_id, dataset_id, version, phase,
input_uri, output_uri,
input_bytes, output_bytes, input_rows, output_rows,
duration_sec, download_duration_sec, upload_duration_sec, compute_duration_sec,
peak_memory_mb, worker_id, status, error_message, metrics_json, created_at
```

主项目 ORM 实际写入的 INSERT 列（来自 `etl-phase.3033229424.log` L24）：

```text
job_id, run_id, dataset_id, version, phase,
input_uri, output_uri,
input_bytes, output_bytes, input_rows, output_rows,
duration_sec, download_duration_sec, upload_duration_sec, compute_duration_sec,
peak_memory_mb, worker_id, status, error_message,
started_at, finished_at,           ← ★★ infra 端缺这两列
metrics_json, created_at
```

差集：`started_at timestamptz` / `finished_at timestamptz`。

## 3. 错误清单与优先级

| # | 错误 | 致命？ | 阻塞 etl-phase？ | 责任方 |
|---|------|--------|------------------|--------|
| F1 | `etl_perf_runs` 缺 `started_at` / `finished_at` 列 | **是** | **是** | infra 端补 migration（按本文档 §5 字段清单）+ 主项目确认字段集合 |
| F2 | perf record 写 PG 失败时**直接 raise** `V15SchemaMissingError` 让整个 CLI 进程 exit 非 0 | 是（间接） | 是（业务 OK 也阻塞下游） | **robot-data-harness（perf writer 容错）** |
| F3 | 主项目 ORM 加新列时没有同步给 infra 对齐 SQL（schema contract 单向更新） | 否（流程问题） | 否 | **robot-data-harness（release notes 同步）** |

> 上一条 dls4z 的 A/B/C/D/E **5 类错误**已经在本次 jddlp 全部 ✅ 修复，详见 [`runs/20260524/robot-dh-multisource-scale30-jddlp/INDEX.md`](runs/20260524/robot-dh-multisource-scale30-jddlp/INDEX.md) §B 对账表。

## 4. 主项目（WSL 端）需要做的事

### 4.1 必须做：确认对齐字段清单（F1 配套）

infra 端会在 `postgres/migrations/006_v1_6_etl_perf_runs_align.sql` 里 **`ADD COLUMN IF NOT EXISTS`** `started_at` / `finished_at`，与 002 创建的现有列完全兼容（不删不改）。但在 infra 落地前，请主项目确认下面 5 个问题：

1. **是否还有其他列缺失**？请贴出当前 `EtlPerfRun` SQLAlchemy 模型的完整列定义。本次日志只暴露 `started_at` / `finished_at`，但 ORM 可能还有别的列将来会写。
2. **`started_at` / `finished_at` 是否允许 NULL**？日志样本两次都是 `'started_at': None, 'finished_at': None`，说明主项目当前**永远写 NULL**，infra 端建议建表为 `timestamptz NULL`。
3. **是否需要复合索引**？v1.5 已经有 `(dataset_id, version, phase, created_at)` 和 `(status, created_at)`。如果有按 `started_at` 切片的查询，请明确点名。
4. **有没有历史回填需求**？v1.5 `etl_perf_runs` 表里 `created_at` 与 `started_at` 经常一致，是否要 `UPDATE etl_perf_runs SET started_at = created_at WHERE started_at IS NULL`？还是保持 NULL 表示 "v1.5 时代未采集"？
5. **`finished_at` 计算口径**：是 `created_at`（写入 PG 时间）还是 `created_at - duration_sec`（业务结束时间）？日志 `duration_sec=639.73` 已经存在，可以反推。

### 4.2 必须做：perf writer 软降级（F2）

当前路径（来自 traceback）：

```text
robot_dh/cli.py L628        emit_perf_records(perf_records, work_dir=perf_dir)
robot_dh/perf/writer.py L120  write_perf_record_to_db(rec, warehouse=wh)
robot_dh/perf/writer.py L34   wh.record_etl_perf_run(record)
robot_dh/warehouse/service.py L519  session.commit()
robot_dh/warehouse/service.py L522  self._handle_write_error(...)
robot_dh/warehouse/service.py L134  raise V15SchemaMissingError(msg) from err
```

**问题**：`record_etl_perf_run` 是观测性副作用，不是业务关键路径，但 `_handle_write_error` 直接 raise 让 CLI exit 非 0。当 schema 漂移发生时，即使业务 ETL 工件 100% 写入 ODS，整个 step 仍然 FAIL，下游 transform 不会被 Argo 触发。

**修复建议**（择一）：

#### 4.2.A 推荐：fallback 到 S3 pending（业务 / 观测性解耦）

```python
# robot_dh/perf/writer.py
def write_perf_record_to_db(record: PerfRecord, *, warehouse: WarehouseService) -> None:
    try:
        warehouse.record_etl_perf_run(record)
    except V15SchemaMissingError as e:
        # PG schema 漂移属于 infra 配置滞后，不阻塞业务 step。
        # 落 S3 pending，admin 跑完 migration 后批量 ingest（参见 §6 admin 工具）。
        pending_uri = _emit_pending_perf_record(record, reason=str(e))
        log.error(
            "perf record schema mismatch, deferred to S3 pending: %s. "
            "Run schema migration on infra side then run "
            "`robot-dh perf reingest-pending` to backfill.",
            pending_uri,
        )
```

`_emit_pending_perf_record` 的目标位置建议：

```text
s3://robot-dh-artifacts/perf-records-pending/<dataset_id>/<version>/<phase>/<job_id>.json
```

infra 端 `robot-dh-artifacts` bucket 已经为 argo-logs 开了写权限，复用即可。

#### 4.2.B 备选：从 ENV 控制 fail-loud / fail-soft

```python
# 默认 soft，CI 跑 strict 验收时切 loud
PERF_RECORD_ON_SCHEMA_MISMATCH = os.environ.get(
    "ROBOT_DH_PERF_RECORD_ON_SCHEMA_MISMATCH", "soft"
)  # soft / loud
```

在 `argo-workflows/multisource-scale30.yaml` 里默认设 `soft`，主项目 CI 单测里设 `loud` 保留发现漂移的能力。

#### 4.2.C 不推荐：try-except 后丢失记录

直接 swallow 不写也不落盘是最差的方案，会让 perf 历史出现"业务跑了但没记录"的暗洞。

### 4.3 必须做：schema contract 同步流程（F3）

参考 v1.5 `etl_shards` / `benchmark_*` 的 handoff 套路（见 [`docs/v1_5_benchmark_align_handoff.md`](v1_5_benchmark_align_handoff.md) §3）。

主项目改 `EtlPerfRun` 模型的 PR 应该附带：

1. 列出新增列名 + 类型 + 是否 NULL 的清单（直接放 PR 描述）
2. 主项目 PR 里**不动远端表**（`robot_dh_app` 没有 owner 权限，跑 `ALTER TABLE` 会被拒），但要在 PR 描述里 `@` infra 维护方
3. infra 端按清单产出 `postgres/migrations/006_*.sql` + `scripts/3X_pg_apply_*.sh`，两边合并后 admin 在远端 apply

如果主项目希望模型改动可以**自验证**，建议在主项目 CI 加一个 `pytest -k test_etl_perf_runs_schema_alignment`，连本地 docker-compose PG，跑 `inspect(EtlPerfRun.__table__).columns` vs `\d etl_perf_runs` 的差集断言。这能在主项目侧第一时间发现新列没有对应 migration，避免上线后才在 Argo 里 FAIL。

### 4.4 不要做：在主项目里给远端表 DDL

仍然走 `robot-dh-infra` 仓库的 migration 文件，由 admin 账号在远端执行。`robot_dh_app` 没有 owner 权限，主项目里跑 `ALTER TABLE` 会被 PG 拒绝。

## 5. 远端对齐后字段约定（infra 这边将落到 002 + 006）

```sql
CREATE TABLE etl_perf_runs (
  id                    bigserial PRIMARY KEY,
  job_id                text NOT NULL,
  run_id                text,
  dataset_id            text,
  version               text,
  phase                 text NOT NULL,
  input_uri             text,
  output_uri            text,
  input_bytes           bigint,
  output_bytes          bigint,
  input_rows            bigint,
  output_rows           bigint,
  duration_sec          double precision,
  download_duration_sec double precision,
  upload_duration_sec   double precision,
  compute_duration_sec  double precision,
  peak_memory_mb        double precision,
  worker_id             text,
  status                text NOT NULL,
  error_message         text,
  started_at            timestamptz,           -- ★ v1.6 新增（本次需求）
  finished_at           timestamptz,           -- ★ v1.6 新增（本次需求）
  metrics_json          jsonb,
  created_at            timestamptz NOT NULL DEFAULT now()
);
```

打 ★ 的 2 列是本次新增。已有列**不删不改**，与 002 创建的 schema 完全兼容。

历史数据回填（infra 端待主项目确认 §4.1 第 4 项后选择执行）：

```sql
-- 选项 A：保持 NULL，表示 v1.5 时代未采集
-- 选项 B：用 created_at 回填 started_at（duration_sec 反推 finished_at）
UPDATE etl_perf_runs
   SET started_at = created_at,
       finished_at = created_at  -- duration_sec 在 v1.5 写入时已经是 0，用 created_at 即可
 WHERE started_at IS NULL
   AND status IN ('OK', 'FAIL');
```

## 6. 验收清单

| # | 项 | 责任方 | 通过标准 |
|---|----|--------|----------|
| 1 | infra 落 `006_v1_6_etl_perf_runs_align.sql` + `38_pg_apply_etl_perf_runs_align.sh` | `robot-dh-infra` | 远端 `\d etl_perf_runs` 含 `started_at` / `finished_at` |
| 2 | smoke：用主项目期望的列 INSERT + DELETE，确认应用账号 GRANT 已就绪 | `robot-dh-infra` | `30_pg_v1_5_smoke_test.sh` 扩展 etl_perf_runs INSERT 用例并跑通 |
| 3 | perf writer 软降级（§4.2.A） | robot-data-harness | schema 漂移时业务 step exit 0；漂移记录落 `s3://robot-dh-artifacts/perf-records-pending/...` |
| 4 | `robot-dh perf reingest-pending` 子命令 | robot-data-harness | admin apply migration 后跑该命令，把 pending 批量回灌 PG，回灌成功后 S3 对象移到 `perf-records-archived/` |
| 5 | 主项目 PR 模板加 schema contract 检查清单 | robot-data-harness | 后续改 ORM 模型的 PR 描述里必须列出"是否新增列 / 是否要 infra migration" |
| 6 | 端到端 `multisource-scale30` workflow 跑到 Succeeded | robot-data-harness + WSL/kind | argo UI 节点全绿；transform / publish step 都被触发；`s3://robot-lake/ods/bridgedata_v2_scale30/v1/` + `s3://robot-lake/qc/...` 全部产出 |

## 7. 时间窗口建议

| 阶段 | 预估 | 备注 |
|------|------|------|
| 主项目确认 §4.1 字段清单 5 问 | < 30min | 直接回 PR 评论 |
| infra 落 006 migration + 入口脚本 | < 1h | 与 003 / 004 同款套路，复制改名即可 |
| 主项目实现 §4.2.A 软降级 + reingest 子命令 | 0.5 day | 含单测 |
| 主项目 CI 加 schema alignment 测试 | < 1h | 本地 docker-compose PG，跑 `inspect(...) vs information_schema` 差集 |
| 主项目镜像重 build + 推 registry | < 30min | 走现有 CI |
| WSL/kind 联调 `make argo-submit-multisource-scale30` | 0.5 day | 终态后云端拉 5 个 step log 重做本文档 §1 的对账 |

总计 ~1.5 天，与 v1.5 benchmark_align 同量级。

## 8. infra 端零额外改动证明（除 006 + 入口脚本外）

| 检查项 | 结论 | 证据 |
|--------|------|------|
| `robotdhapp` policy / GRANT | ✅ | 002 / 003 / 004 末尾的 DO 块都给 `etl_perf_runs` 授了 INSERT 权限，本次 INSERT 失败的是**列**而不是**权限** |
| MinIO `robot-dh-artifacts` bucket 是否有 `perf-records-pending/` 写权限 | ✅ | argo-logs 已经在写同 bucket 同 prefix 风格的对象，policy 复用 |
| MinIO endpoint 可达 | ✅ | 第 1 次 etl-phase 已经 `materialize_input.done` + `upload_outputs.done` + `write_manifest.done` |
| log 是否已落 MinIO | ✅ | 5 个 step pod 全归档，参见 [`runs/20260524/robot-dh-multisource-scale30-jddlp/INDEX.md`](runs/20260524/robot-dh-multisource-scale30-jddlp/INDEX.md) |
| `etl_perf_runs` schema 是否对齐 | ❌ | 见错误 F1（infra 待出 006） |
| perf writer 失败策略 | ❌ | 见错误 F2（主项目改） |
| schema contract 同步流程 | ❌ | 见错误 F3（主项目改） |

→ 立即恢复路径：infra 出 006，admin 在远端跑一次 `psql ... -f 006_v1_6_etl_perf_runs_align.sql`，主项目无需改一行代码就能让本 workflow 重启后跑到 Succeeded。但 §4.2 软降级是长期防御性建议，避免下次 schema 漂移再次让业务 step 整体 FAIL。

## 9. 本仓库这边的 follow-up

| 项 | 状态 |
|----|------|
| 5-step log（jddlp）已落 `docs/runs/20260524/robot-dh-multisource-scale30-jddlp/` | ✅ 已落 |
| `INDEX.md`（jddlp）含与 dls4z 的对账表 | ✅ 已落 |
| 本需求文档（`v1_6_etl_perf_runs_schema_align_request.md`） | ✅ 已落 |
| `postgres/migrations/006_v1_6_etl_perf_runs_align.sql` | ⏭ 等主项目确认 §4.1 字段清单 5 问后再写 |
| `scripts/38_pg_apply_etl_perf_runs_align.sh` | ⏭ 与 006 一起出 |
| `30_pg_v1_5_smoke_test.sh` 扩展 etl_perf_runs 用例 | ⏭ 与 006 一起出 |
| `docs/v1_6_argo_log_archive_request.md` §11.x 末尾追加 jddlp 端到端验收记录 | ⏭ 主项目联调 Succeeded 后再补 |

## 10. scp 拉取（一条命令拉完）

把本次需求所需的 **文档 + 失败 log** 一次性拉到 WSL：

```bash
mkdir -p ./robot-dh-jddlp-bundle
scp -r ubuntu@<cloud-host>:/home/ubuntu/robot-dh-infra/docs/v1_6_etl_perf_runs_schema_align_request.md \
       ubuntu@<cloud-host>:/home/ubuntu/robot-dh-infra/docs/runs/20260524/robot-dh-multisource-scale30-jddlp \
       ubuntu@<cloud-host>:/home/ubuntu/robot-dh-infra/docs/v1_5_benchmark_align_handoff.md \
       ./robot-dh-jddlp-bundle/
```

拉完后本地结构（拍平）：

```text
robot-dh-jddlp-bundle/
├── v1_6_etl_perf_runs_schema_align_request.md       # 本文档
├── v1_5_benchmark_align_handoff.md                  # 同套路参考（v1.5 给 benchmark_* 加列的 handoff）
└── robot-dh-multisource-scale30-jddlp/              # jddlp 失败 workflow 完整 log
    ├── INDEX.md
    ├── lake-list.623652825.log
    ├── qc-contract-run.1491557645.log
    ├── partition-plan.1987402368.log
    ├── etl-phase.3033229424.log
    └── etl-phase.2697529949.log
```

---

如有任何字段或语义疑问，直接 ping 本仓库维护方（`robot-dh-infra`），不要在主项目侧强行 DDL。
