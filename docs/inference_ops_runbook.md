# robot-dh-infra 推理数据平面运维 runbook（v1.9）

> 运行位置：腾讯云 Ubuntu 服务器 `/opt/robot-dh-infra`，PostgreSQL 跑在 docker 容器 `robot-dh-postgres`。
> 本 runbook 只用 `docker exec` + `psql`，不引入额外依赖。所有脚本 `set -euo pipefail`，失败即非 0 退出。

## 1. v1.9 范围

在 v1.8 数仓 / quality ops 基础上新增 AI 推理数据平面（10 张表，见 `docs/inference_data_plane_schema.md`）与 5 个运维脚本：

| 脚本 | 作用 |
| --- | --- |
| `45_pg_apply_inference_schema.sh` | 幂等应用 `007_inference_data_plane.sql`，应用后列出 v1.9 新表 |
| `46_pg_inference_smoke_test.sh` | 用 `robot_dh_app` 验证 6 张关键表读写权限，插入后清理 |
| `47_inference_ops_report.sh` | 推理任务 / 输出 / 失败 / benchmark 运营报告（MD + JSON） |
| `48_distill_dataset_report.sh` | 蒸馏数据集统计报告（MD + JSON） |
| `49_export_inference_client_env.sh` | 导出 client env（含推理变量），默认脱敏，`--show-secrets` 才出真实文件 |

> 命名约定：文件名不含版本号（遵循项目全局约定），版本只体现在脚本内部注释与 `ROBOT_DH_PLATFORM_VERSION` 取值。迁移序号 `007`、运维序号 `45~49` 是执行顺序序号，不是版本号。

## 2. 部署脚本到云端

本仓库把 v1.9 云端脚本放在 `infra/scripts/`（与 WSL 本地脚本隔离，不在本仓 `scripts/` 混放）。部署时同步到云端仓库的 `scripts/` 目录，使其与 `postgres/` 同级：

```bash
# 在 WSL 仓库根执行（HOST 换成云端公网 IP/DNS）
rsync -avz infra/scripts/ "ubuntu@HOST:/opt/robot-dh-infra/scripts/"
rsync -avz postgres/migrations/007_inference_data_plane.sql \
  "ubuntu@HOST:/opt/robot-dh-infra/postgres/migrations/"
```

之后所有命令在云端 `cd /opt/robot-dh-infra` 下执行；脚本会以「脚本所在目录的上一级」为仓库根定位 migration，因此放到 `scripts/` 后路径自洽。也可用 `ROBOT_DH_INFRA_ROOT` 环境变量显式覆盖。

可覆盖的环境变量（均有合理默认）：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `ROBOT_DH_INFRA_ROOT` | 脚本上级目录 | 仓库根 |
| `ROBOT_DH_PG_CONTAINER` | `robot-dh-postgres` | PG 容器名 |
| `ROBOT_DH_PG_ADMIN_USER` | `robot_dh_admin` | 管理员账号（DDL） |
| `ROBOT_DH_PG_APP_USER` | `robot_dh_app` | 应用账号（smoke / 报告） |
| `ROBOT_DH_PG_DB` | `robot_dh` | 数据库名 |
| `ROBOT_DH_LOG_DIR` | `/data/robot-dh/logs` | 报告输出目录 |

## 3. 初始化 schema

```bash
cd /opt/robot-dh-infra
./scripts/06_healthcheck.sh            # 既有：确认 PG / MinIO / Redis 健康
./scripts/45_pg_apply_inference_schema.sh
```

- 用 `robot_dh_admin` 执行 `007_inference_data_plane.sql`，只做 `CREATE TABLE/INDEX IF NOT EXISTS` + `GRANT`，**不 drop、不 truncate**，可重复执行。
- 应用后列出 10 张 v1.9 新表（含 total_size / index_count）；若新表数量不足会非 0 退出。

## 4. smoke test（读写权限验证）

```bash
./scripts/46_pg_inference_smoke_test.sh
```

- 用 `robot_dh_app` 对 `model_registry` / `inference_jobs` / `inference_outputs` / `distillation_datasets` / `inference_benchmark_runs` / `ai_task_events` 做 `INSERT → UPDATE → DELETE`。
- smoke 数据用 `__smoke__<ts>_<pid>` 前缀隔离，单事务内删除并在退出时兜底清理，**绝不影响真实数据**。
- 失败通常意味着：缺表（先跑 45）、或 `robot_dh_app` 缺少表级 DML 授权（重跑 45 的 GRANT 段）。

## 5. 看推理任务状态

```bash
./scripts/47_inference_ops_report.sh
```

产物（表为空也会生成零值报告，不失败）：

```
/data/robot-dh/logs/v1_9_inference_ops_YYYYmmdd_HHMMSS.md
/data/robot-dh/logs/v1_9_inference_ops_YYYYmmdd_HHMMSS.json
```

报告覆盖：

- `inference_jobs`：按 `status` / `task_type` 的任务数、样本数（total / processed / failed）。
- `inference_outputs`：按 `prediction_type` 的输出数、平均 latency / confidence。
- `inference_failures`：按 `error_type` 的失败数、retryable / non_retryable 拆分。
- `inference_benchmark_runs`：最近 10 次压测概要。

JSON 由单条 `json_build_object` 查询产出，便于被监控 / dashboard 直接消费。

## 6. 看 benchmark

benchmark 概要已包含在 §5 的 ops 报告（`inference_benchmark_runs` 段）。需要更细可直接查库：

```bash
docker exec -i robot-dh-postgres psql -U robot_dh_app -d robot_dh -P pager=off -c "
SELECT benchmark_id, model_id, backend, workload_name, status,
       samples_per_sec, p50_latency_ms, p95_latency_ms, p99_latency_ms, error_rate, created_at
FROM inference_benchmark_runs
ORDER BY created_at DESC NULLS LAST
LIMIT 20;"
```

## 7. 看 distillation dataset

```bash
./scripts/48_distill_dataset_report.sh
```

产物（空表不失败）：

```
/data/robot-dh/logs/v1_9_distill_datasets_YYYYmmdd_HHMMSS.md
/data/robot-dh/logs/v1_9_distill_datasets_YYYYmmdd_HHMMSS.json
```

覆盖：按 `status` / `distill_format` / `teacher_model_id` 的数据集数与样本数（train/val/test），以及最近 10 个蒸馏数据集明细。

## 8. 导出 client env

```bash
# 默认脱敏：生成 client/robot-dh-platform.env.example（占位符，可提交 git）
./scripts/49_export_inference_client_env.sh

# 真实文件（chmod 600，禁止提交 git）：先 source 真实凭据再带 --show-secrets
set -a; source /opt/robot-dh-infra/secrets/robot-dh.runtime.env; set +a
./scripts/49_export_inference_client_env.sh --show-secrets
```

- 在 v1.6 平台 env 基础上新增 5 个变量：`ROBOT_DH_INFER_OUTPUT_ROOT` / `ROBOT_DH_DISTILL_OUTPUT_ROOT` / `ROBOT_DH_DEFAULT_INFER_BACKEND` / `ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL` / `ROBOT_DH_OPENAI_COMPATIBLE_API_KEY`，并把 `ROBOT_DH_PLATFORM_VERSION` 置为 `1.9`。
- 两种模式都**不打印密码 / api_key**；真实文件 `chmod 600`，用完建议 `shred -u`。
- 生成后部署 K8s Secret：scp `client/robot-dh-platform.env` 到 WSL，`set -a; source ...; set +a`，再跑 `client/k8s-create-platform-secret.example.sh`（Secret 名沿用 `robot-dh-v1-6-secrets`，Argo 引用不变）。

## 9. 验收命令

用户在云端手动执行（全部应可重复执行、不破坏既有数据）：

```bash
cd /opt/robot-dh-infra
./scripts/06_healthcheck.sh
./scripts/45_pg_apply_inference_schema.sh
./scripts/46_pg_inference_smoke_test.sh
./scripts/47_inference_ops_report.sh
./scripts/48_distill_dataset_report.sh
./scripts/49_export_inference_client_env.sh
```

验收标准：

- 所有脚本可重复执行（幂等）。
- 不删除已有数据；不 drop / truncate 已有表。
- 不暴露密码 / api_key。
- v1.9 全部 10 张新表存在，`robot_dh_app` 可读写。
- 报告脚本在空表与有数据两种情况下都能运行。

## 10. 常见故障

| 现象 | 根因 | 处理 |
| --- | --- | --- |
| 45 报「找不到 migration 文件」 | 脚本不在 `scripts/`，或未同步 007 | 确认 `007_*.sql` 在 `postgres/migrations/`，或设 `ROBOT_DH_INFRA_ROOT` |
| 45 报「PG 容器不存在」 | 容器名不是 `robot-dh-postgres` | 设 `ROBOT_DH_PG_CONTAINER`，或先 `./scripts/06_healthcheck.sh` |
| 46 报权限错误 | `robot_dh_app` 缺表级 DML | 重跑 45（含 GRANT 段）；用 admin `\dp model_registry` 核对 |
| 49 `--show-secrets` 报「仍是占位符」 | 没 source 真实凭据 | 先 `source` 真实 env 再带 `--show-secrets` |
| 报告里全是 0 行 | 推理链路尚未产生数据 | 正常；待 Prompt B 的 infer job 写入后再看 |

排障示例：

```bash
docker exec -it robot-dh-postgres psql -U robot_dh_admin -d robot_dh -c "\dp model_registry;"
docker exec -it robot-dh-postgres psql -U robot_dh_admin -d robot_dh -c "\d+ inference_jobs;"
```
