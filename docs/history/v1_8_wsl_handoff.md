# v1.8 Warehouse / Quality Ops 交接给 WSL 项目

> 接收方：本地 Win11 / WSL2 上的 `robot-data-harness` 主项目（含 kind 集群、Argo、FastAPI、CLI）。
> 发起方：腾讯云 Ubuntu 服务器上的 `robot-dh-infra`（PostgreSQL / MinIO / Redis）。
> 默认通道：**scp**（项目侧通过 SSH 拉取，不需要从云端 push）。
> 生成时间：见文件 mtime；脚本可以重新跑出最新版本。

本文档列出本次 v1.8 升级"WSL 侧需要拿到 / 检查 / 跑哪些东西"。infra 已经把 schema、权限、env 文件、k8s 模板全部准备好，**主项目只需要 scp 4 类文件 + 1 次 kubectl apply**，就能在 Argo Pod 内开始读写 v1.8 表。

## 1. 一句话概述

- PostgreSQL 在云端已经新增 15 张数仓表（DIM / FACT / DWS / ADS / Backfill / SLA），全部已对 `robot_dh_app` 账号开通 `SELECT/INSERT/UPDATE/DELETE`。
- WSL 端无需再申请新账号，**继续用 v1.6 的 `robot_dh_app` + 同一份 secret**，只需要追加 v1.8 的 3 个新环境变量。
- 主项目侧的工作：① 在 ingest / register 流程写 `dim_dataset`；② 在离线 job 物化 `fact_*`；③ 写一个聚合 job 输出 `dws_*` 与 `ads_*`；④ 按需写 `sla_*` / `backfill_*`。infra 不会主动写任何业务数据。

## 2. 需要从云端 scp 拉取的文件

云端公网入口：`82.156.129.81`（端口 22 走默认 SSH）。
云端项目目录：`/opt/robot-dh-infra`（= `/home/ubuntu/robot-dh-infra`）。

在 WSL host 执行（推荐放到主项目某个临时目录，比如 `~/robot-dh-infra-handoff/`）：

```bash
SSH_USER=ubuntu                # 按实际改
SSH_HOST=82.156.129.81
REMOTE_DIR=/opt/robot-dh-infra
LOCAL_DIR=$HOME/robot-dh-infra-handoff/v1.8

mkdir -p "$LOCAL_DIR"

# 1) 真实凭据 env（已 chmod 600；含 v1.6 全集 + v1.8 3 个新增）
scp "$SSH_USER@$SSH_HOST:$REMOTE_DIR/client/robot-dh-v1-8.env" "$LOCAL_DIR/"

# 2) k8s Secret / RBAC 骨架（apply 一次创建 namespace + ServiceAccount + Role + 空 Secret）
scp "$SSH_USER@$SSH_HOST:$REMOTE_DIR/client/k8s-v1-8-secret.example.yaml" "$LOCAL_DIR/"

# 3) k8s Secret 真实凭据写入脚本（pass-through 模式）
scp "$SSH_USER@$SSH_HOST:$REMOTE_DIR/client/k8s-create-v1-8-secret.example.sh" "$LOCAL_DIR/"

# 4) 文档（可选，但建议拿，便于查 schema 字段）
scp "$SSH_USER@$SSH_HOST:$REMOTE_DIR/docs/v1_8_warehouse_schema.md" "$LOCAL_DIR/"
scp "$SSH_USER@$SSH_HOST:$REMOTE_DIR/docs/v1_8_quality_ops_runbook.md" "$LOCAL_DIR/"
scp "$SSH_USER@$SSH_HOST:$REMOTE_DIR/docs/v1_8_backfill_sla_notes.md" "$LOCAL_DIR/"

# 5) PostgreSQL migration（仅做参考，不需要在 WSL 端执行；schema 已在云端应用过）
scp "$SSH_USER@$SSH_HOST:$REMOTE_DIR/postgres/migrations/006_v1_8_warehouse_quality_ops.sql" "$LOCAL_DIR/"

chmod 600 "$LOCAL_DIR/robot-dh-v1-8.env"
chmod +x "$LOCAL_DIR/k8s-create-v1-8-secret.example.sh"

ls -l "$LOCAL_DIR"
```

预期产出：

```
robot-dh-v1-8.env                          (chmod 600)
k8s-v1-8-secret.example.yaml
k8s-create-v1-8-secret.example.sh          (chmod +x)
v1_8_warehouse_schema.md
v1_8_quality_ops_runbook.md
v1_8_backfill_sla_notes.md
006_v1_8_warehouse_quality_ops.sql         (只读参考)
```

> 如果你之前已经 scp 过 v1.6 的同名 yaml / 脚本，可以直接覆盖；v1.8 的 namespace 仍是 `robot-dh`，但 Secret 名升级为 `robot-dh-v1-8-secrets`，与 `robot-dh-v1-6-secrets` 并存。

## 3. WSL / kind 应用步骤

```bash
cd $HOME/robot-dh-infra-handoff/v1.8

# 1) 先一次性应用 namespace + ServiceAccount + RBAC + 空 Secret 骨架
kubectl apply -f k8s-v1-8-secret.example.yaml

# 2) 载入真实凭据
set -a; source robot-dh-v1-8.env; set +a

# 3) 用真实凭据覆盖 Secret（会硬校验占位符 / 受保护前缀 / 非法 schema 名）
./k8s-create-v1-8-secret.example.sh
```

成功输出长这样（凭据已脱敏）：

```
Applied secret robot-dh-v1-8-secrets in namespace robot-dh.
DB host=82.156.129.81  S3 host=82.156.129.81  Redis host=82.156.129.81
Platform version=1.8  Warehouse schema=public  Warehouse root=s3://robot-lake/warehouse
（其他凭据已脱敏，不会打印）
```

如果在尚未上 kind 的开发机上跑，可以加 `--dry-run` 只校验：

```bash
./k8s-create-v1-8-secret.example.sh --dry-run
```

## 4. v1.8 表 schema 与权限一览

下面 15 张表全部已对 `robot_dh_app` 账号开通 SELECT / INSERT / UPDATE / DELETE（云端 `pg_table_privileges` 已实测）：

| 层 | 表 |
|---|---|
| DIM | `dim_dataset` |
| FACT | `fact_etl_run` / `fact_qc_rule_result` / `fact_workflow_step` / `fact_asset_profile` |
| DWS | `dws_dataset_quality_daily` / `dws_rule_failure_daily` / `dws_workflow_ops_daily` |
| ADS | `ads_quality_dashboard` / `ads_workflow_ops_dashboard` |
| Backfill | `backfill_plans` / `backfill_tasks` |
| SLA | `sla_policies` / `sla_checks` / `dataset_partition_readiness` |

字段语义参见 [`v1_8_warehouse_schema.md`](v1_8_warehouse_schema.md)。要点：

- 全部 v1.8 表使用 `text` 主键（无 `bigserial`），**主项目按规则生成 key**：
  - `dim_dataset.dataset_key`：建议 `'dataset:<dataset_id>:<version>'`
  - `fact_etl_run.run_key`：建议 `'<job_id>:<run_id>'`
  - `fact_workflow_step.step_key`：建议 `'<ns>:<workflow_name>:<step_name>:<finished_at_epoch>'`
  - `backfill_plans.plan_id` / `backfill_tasks.task_id` / `sla_*.{policy_id,check_id}`：业务自由命名，避免 `:` 之外的特殊字符即可
- DWS / ADS 使用复合主键，主项目用 `INSERT ... ON CONFLICT (dt, ...) DO UPDATE SET ...` 做 UPSERT。
- `backfill_tasks.plan_id` **没有强 FK** 约束（避免大量 partial DELETE 时卡住），需要主项目应用层保证 referential integrity。
- v1.8 不引入新账号、不引入新 namespace、不引入新 ServiceAccount；继续用 v1.6 的 `robot_dh_app` + `robot-dh` namespace。

## 5. 验证你能跑通的最小路径

WSL / kind 上的最小回路（替换为你项目的实际 import）：

```python
# Python 端：用 psycopg + ROBOT_DH_DB_URI
import psycopg, os
uri = os.environ["ROBOT_DH_DB_URI"]
with psycopg.connect(uri.replace("postgresql+psycopg://", "postgresql://")) as conn:
    with conn.cursor() as cur:
        # 1) 读：v1.8 表都存在
        cur.execute("""
          SELECT table_name FROM information_schema.tables
           WHERE table_schema='public'
             AND table_name IN ('dim_dataset','ads_quality_dashboard','sla_checks')
           ORDER BY table_name;
        """)
        assert {r[0] for r in cur.fetchall()} == {
            "ads_quality_dashboard", "dim_dataset", "sla_checks"
        }

        # 2) 写：单事务 UPSERT 一行后回滚（不留痕）
        with conn.transaction():
            cur.execute("""
              INSERT INTO dim_dataset (dataset_key, dataset_id, version, dataset_family)
              VALUES (%s, %s, %s, %s)
              ON CONFLICT (dataset_key) DO UPDATE SET updated_at = now()
            """, ("dataset:wsl_smoke:1", "wsl_smoke", "1", "smoke"))
            raise psycopg.Rollback()
```

如果上面跑过，说明：

1. SSH tunnel / 公网直连工作正常
2. `robot_dh_app` 账号能 SELECT + INSERT + UPDATE + DELETE 所有 v1.8 表
3. v1.8 schema 已落地

## 6. 主项目侧需要做的事情（不是 infra 范围）

infra 仓库不会写业务数据。主项目接到 v1.8 后建议按下面顺序补：

1. **ingest / register 写 `dim_dataset`**：每个新 dataset 注册时 UPSERT 一行。
2. **离线 job 物化 `fact_*`**：从 v1.5 `etl_perf_runs` / v1.6 `qc_contract_runs` / `workflow_steps` / `asset_profiles` 增量物化到 `fact_etl_run` / `fact_qc_rule_result` / `fact_workflow_step` / `fact_asset_profile`。
3. **日聚合 job 写 `dws_*`**：建议每天 UTC 03:00 跑一次 Argo CronWorkflow，把 fact 表聚合到 `dws_dataset_quality_daily` / `dws_rule_failure_daily` / `dws_workflow_ops_daily`。
4. **看板物化 `ads_*`**：在 dws 跑完后立刻把 `ads_quality_dashboard` / `ads_workflow_ops_dashboard` 重新计算（含 `alert_level` 报警等级）。
5. **SLA 校验**：参见 [`v1_8_backfill_sla_notes.md`](v1_8_backfill_sla_notes.md) 第 3 节，主项目在 sla CronWorkflow 中写 `sla_policies` / `sla_checks`，并用 `dataset_partition_readiness` 计算 `missing_outputs_json`。
6. **补数**：失败 SLA 转 `backfill_plans` + `backfill_tasks`，Argo 模板按 `recommended_command` 渲染 container args。

infra 这边对主项目的承诺：

- 表 / 索引 / 权限**完全幂等**：主项目重启 / 重部署不会破坏现有数据
- 报告脚本即使表为空也不会失败（云端运维 / CI 可放心调）
- 不会"自动"插入业务数据，保持纯元数据基础设施定位

## 7. 后续如果云端 schema 更新

云端推 v1.9 / v2.0 后：

1. infra 仓库会更新 migration 与脚本编号
2. **本地不需要重新 apply migration**（migration 在云端跑），WSL 项目仅需要：
   - 重新 scp `client/robot-dh-vX-Y.env`、`client/k8s-vX-Y-secret.example.yaml`、`client/k8s-create-vX-Y-secret.example.sh`
   - 重新 `kubectl apply -f k8s-vX-Y-secret.example.yaml` + 重新跑 create-secret 脚本
3. 老的 Secret（如 `robot-dh-v1-6-secrets`、`robot-dh-v1-8-secrets`）可保留，互不影响

如果主项目希望接到 schema 变更通知，建议监听本仓库 `docs/v1_*_wsl_handoff.md` 的新增。

## 8. 常见故障

| 现象 | 可能原因 | 处理 |
|---|---|---|
| `kubectl apply -f k8s-v1-8-secret.example.yaml` 报 `namespace already exists` | 已有 v1.6 部署过 namespace | 可忽略，apply 是幂等的 |
| `./k8s-create-v1-8-secret.example.sh` 报 `仍然包含占位符` | 没有先 source env 文件 | `set -a; source robot-dh-v1-8.env; set +a` 后再执行 |
| `psql` 报 `permission denied for table dim_dataset` | 实际用了 `POSTGRES_USER` 管理员账号或老账号 | 改用 `ROBOT_DH_DB_URI` 中的 `robot_dh_app` |
| Pod 内连不上 | secret 里 host 写成了 `127.0.0.1` | 重新 export `--mode public --host 82.156.129.81 --show-secrets` 后 scp |
| `psql` 连接超时 | WSL 出口 IP 变了，云端 UFW 未放行 | 见 `client/wsl-public-access-checklist.md` 的"WSL 出口 IP 变更后的增量放行"章节 |

## 9. 验收 checklist（WSL 侧执行）

- [ ] scp 了 `client/robot-dh-v1-8.env`、`client/k8s-v1-8-secret.example.yaml`、`client/k8s-create-v1-8-secret.example.sh`
- [ ] `kubectl apply -f k8s-v1-8-secret.example.yaml` 成功
- [ ] `set -a; source robot-dh-v1-8.env; set +a` 后跑 `./k8s-create-v1-8-secret.example.sh` 输出 `Applied secret robot-dh-v1-8-secrets`
- [ ] 第 5 节"最小路径" Python 脚本跑过，回滚成功
- [ ] 主项目能从 Argo Pod 内 SELECT / INSERT v1.8 表

## 10. 联系点

- 云端服务器：`82.156.129.81`（ubuntu）
- 项目目录：`/opt/robot-dh-infra`
- 验收命令：`./scripts/42_pg_apply_v1_8_schema.sh ... ./scripts/47_export_v1_8_client_env.sh`（详见 README v1.8 章节）
- infra 仓库已经预跑过本次验收：
  - 15 张表全部 `exists=True`
  - smoke test 在 `robot_dh_app` 下 INSERT/DELETE 7 张表均通过
  - 三份 Markdown / JSON 报告已落在 `/data/robot-dh/logs/`：
    - `v1_8_warehouse_counts_*.{md,json}`
    - `v1_8_quality_ops_daily_*.md`
    - `v1_8_sla_ops_*.md`
