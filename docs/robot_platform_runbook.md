# robot-dh-infra v1.6 Runbook

本 runbook 描述 v1.6 升级后的运维操作流程。v1.6 不更换服务进程，不新增容器，重点是：

- 新增 9 张 PostgreSQL 表（QC contract / workflow metadata / asset profile / ml-ready / OpenLineage 事件）
- 新增 7 个脚本（编号 35–41）
- 新增 3 个 client 模板（env + k8s yaml + create-secret 脚本）
- 老表 / 老 bucket / 已有数据 / 磁盘布局**全部保持不动**

## 1. v1.6 范围

- PostgreSQL schema：`postgres/migrations/005_v1_6_robot_platform.sql`，幂等
- 运维脚本：编号 35–41，全部 `set -euo pipefail`，默认 read-only
- Client 模板：`client/robot-dh-platform.env.example`、`client/k8s-platform-secret.example.yaml`、`client/k8s-create-platform-secret.example.sh`

> v1.5 已知的 `activeDeadlineSeconds=7200` deadline 失败（run-shard-0 / run-shard-1 卡在 normalize）是 v1.6 metadata 设计的直接动机；详见 [`v1_6_storage_and_deadline_notes.md`](v1_6_storage_and_deadline_notes.md)。

## 2. v1.6 PostgreSQL schema 初始化

```bash
cd /opt/robot-dh-infra

./scripts/06_healthcheck.sh
./scripts/35_pg_apply_v1_6_schema.sh
./scripts/36_pg_v1_6_smoke_test.sh
```

`35_pg_apply_v1_6_schema.sh` 行为：

- 用 `POSTGRES_USER`（管理员）账号执行 `005_v1_6_robot_platform.sql`
- migration 末尾直接给 `robot_dh_app` 授 `SELECT/INSERT/UPDATE/DELETE` + 序列 `USAGE/SELECT`
- 全部 `CREATE IF NOT EXISTS`，幂等；不 DROP，不 TRUNCATE
- 末尾会列出新增 9 张表与它们当前的物理大小（KiB）

`36_pg_v1_6_smoke_test.sh` 行为：

- 用 `robot_dh_app` 在 9 张新表中插入 smoke 行
- 单事务结尾全部 `DELETE`，不留痕
- 退出码非 0 = 表缺失 / 列缺失 / 权限不足

## 3. 平台状态审计

```bash
cd /opt/robot-dh-infra
./scripts/37_audit_v1_6_platform_state.sh
```

行为：

- 汇总 v1.3 / v1.4 / v1.5 / v1.6 所有核心表 row count（`to_regclass` 判存，缺失标 -1）
- 调用 `mc du --recursive --json` 汇总 4 个 bucket 的对象数与字节数
- JSON 落到 `/data/robot-dh/logs/v1_6_platform_state_YYYYmmdd_HHMMSS.json`
- 终端 summary 包含每个 version 的表列表 + MinIO bucket 汇总
- 容器未启动时优雅降级，不抛错

## 4. workflow metadata 审计

```bash
cd /opt/robot-dh-infra
./scripts/38_workflow_metadata_report.sh
```

输出：

- 最近 20 个 workflow：合并 `workflow_runs` 与 `argo_workflow_runs`，按 `finished_at DESC NULLS LAST, created_at DESC`
- 失败 step 分布：按 `(workflow_name, template_name, phase)` 聚合 `workflow_steps`，命中 `Failed / Error / Aborted / Timeout`
- 多源 step phase 分布：按 `(dataset_family, dataset_id, version, phase)` 聚合
- runtime_events 最近 20 条：兼容 v1.5 事件总线

报告：`/data/robot-dh/logs/v1_6_workflow_metadata_report_YYYYmmdd_HHMMSS.md`

> 任一表缺失时只标 "缺失" 不抛错，便于在还没接通 sync 的环境上跑。

## 5. QC contract 报告

```bash
cd /opt/robot-dh-infra
./scripts/39_qc_contract_report.sh
```

输出：

- `dataset_family` 维度的 pass / warn / fail / other / total（status 做归一化）
- 最近 20 条 `qc_contract_runs`
- 当前 `qc_contracts` 列表（默认按 enabled DESC）

报告：`/data/robot-dh/logs/v1_6_qc_contract_report_YYYYmmdd_HHMMSS.md`

表为空时输出 `_无数据_` 行，不失败。

## 6. tmp lifecycle 审计与清理

```bash
cd /opt/robot-dh-infra
./scripts/40_storage_tmp_lifecycle_audit.sh                  # 只报告
./scripts/40_storage_tmp_lifecycle_audit.sh --apply-cleanup  # 需交互输入 APPLY_TMP_CLEANUP
./scripts/40_storage_tmp_lifecycle_audit.sh --apply-cleanup --days 14
```

行为：

- 默认 read-only，扫描以下 prefix：
  - `robot-lake/tmp/`
  - `robot-dh-artifacts/tmp/`
  - `robot-lake/tmp/workflows/`
  - `robot-dh-artifacts/tmp/workflows/`
- 报告对象数、总大小（GiB）、最老对象时间
- `--apply-cleanup`：
  - 必须交互输入 `APPLY_TMP_CLEANUP` 才会执行
  - 执行 `mc rm --recursive --force --older-than ${DAYS}d`，默认 7 天
  - 二次校验：任何非 `tmp/` 入口出现在白名单立即 FATAL；任何匹配 `raw/ods/dwd/ads/lineage/manifests/runs` 立即 FATAL
- 报告：`/data/robot-dh/logs/v1_6_storage_tmp_lifecycle_YYYYmmdd_HHMMSS.md`

> 与 `28_minio_lifecycle_plan.sh` 的差别：
> - 28 号脚本管 ILM 规则（lifecycle policy）
> - 40 号脚本是按需即时清理 tmp 旧对象（不写 ILM）

## 7. 客户端 env 导出

```bash
cd /opt/robot-dh-infra
./scripts/41_export_v1_6_client_env.sh                       # 默认 public + 脱敏
./scripts/41_export_v1_6_client_env.sh --show-secrets        # 写 client/robot-dh-platform.env (chmod 600)
./scripts/41_export_v1_6_client_env.sh --mode tunnel         # 仅给 WSL host 单进程
```

输出变量包含 v1.5 全集，再加：

- `ROBOT_DH_PLATFORM_VERSION=1.6`
- `ROBOT_DH_QC_CONTRACT_BUCKET_PREFIX=s3://robot-lake/qc`
- `ROBOT_DH_ML_READY_ROOT=s3://robot-lake/ml-ready`
- `ROBOT_DH_WORKFLOW_TMP_PREFIX=s3://robot-lake/tmp/workflows`

stdout 永远脱敏，不会把真实密码打到 CI / journald。
`--show-secrets` 才会生成 `client/robot-dh-platform.env`，权限 `0600`。

WSL 端注入 Secret：

```bash
# 一次性创建 namespace / ServiceAccount / RBAC / 空 Secret
kubectl apply -f client/k8s-platform-secret.example.yaml

# 用真实凭据覆盖 Secret
set -a; source client/robot-dh-platform.env; set +a
./client/k8s-create-platform-secret.example.sh
```

`k8s-create-v1-6-secret.example.sh` 在 apply 前会硬校验：

- 任何 `CHANGE_ME` / `PUBLIC_SERVER_IP_OR_DNS` / 空值都拒绝
- DB / S3 / Redis host 不一致只 WARN，不阻塞（适配多机部署）
- 默认拒绝 `127.0.0.1 / localhost / ::1`，加 `--allow-localhost` 才放行
- v1.6 三个新前缀必须 `s3://` 开头，且禁止指向 `raw / ods / dwd / ads / lineage / manifests`

## 8. 验收命令

```bash
cd /opt/robot-dh-infra

./scripts/06_healthcheck.sh
./scripts/35_pg_apply_v1_6_schema.sh
./scripts/36_pg_v1_6_smoke_test.sh
./scripts/37_audit_v1_6_platform_state.sh
./scripts/38_workflow_metadata_report.sh
./scripts/39_qc_contract_report.sh
./scripts/40_storage_tmp_lifecycle_audit.sh
./scripts/41_export_v1_6_client_env.sh
```

通过条件：

- 所有脚本以 `0` 退出
- `35_pg_apply_v1_6_schema.sh` 列出 9 张新表
- `36_pg_v1_6_smoke_test.sh` 在 `robot_dh_app` 账号下 9 张表均可插入 + 删除
- `37_audit_v1_6_platform_state.sh` 落 JSON 报告，且包含 v1.3 / v1.4 / v1.5 / v1.6 四个版本的表 row count
- `38_workflow_metadata_report.sh` / `39_qc_contract_report.sh` 落 Markdown 报告，即使所有表为空也不失败
- `40_storage_tmp_lifecycle_audit.sh` 默认 read-only，不删除任何对象
- `41_export_v1_6_client_env.sh` 默认输出脱敏，不会打印真实密码
- `client/robot-dh-platform.env` 仅在显式 `--show-secrets` 时生成，权限 `0600`

## 9. 常见故障

### 9.1 v1.6 表 GRANT 缺失

现象：smoke test 报 `permission denied for table xxx`。

排查：

```bash
docker exec -it robot-dh-postgres \
  psql -U robot_dh_admin -d robot_dh -c \
  "\dp qc_contracts;"
```

处理：重新执行 `./scripts/35_pg_apply_v1_6_schema.sh`。迁移末尾会再次执行固定 `GRANT`，幂等。

### 9.2 audit / report 看不到 v1.6 表

现象：`37_audit_v1_6_platform_state.sh` 输出中 v1.6 行全部 `exists=False`。

原因：`005_v1_6_robot_platform.sql` 未执行。

处理：

```bash
./scripts/35_pg_apply_v1_6_schema.sh
./scripts/37_audit_v1_6_platform_state.sh
```

### 9.3 workflow metadata 报告 "无数据"

现象：`38_workflow_metadata_report.sh` 全部章节都是 `_无数据_`。

原因：v1.6 `workflow_runs` / `workflow_steps` 由主项目 `robot-data-harness` CLI / sync 脚本写入，本仓库不主动写。报告本身不应该失败。

处理：等主项目 v1.6 sync 写入数据后重新执行；空报告本身是合法状态。

### 9.4 tmp lifecycle 审计想清非 7 天阈值

```bash
./scripts/40_storage_tmp_lifecycle_audit.sh --apply-cleanup --days 14
```

`--days` 仅在 `--apply-cleanup` 时生效；纯审计模式不会按 days 过滤报告，避免遗漏陈旧数据。

### 9.5 client env 中残留 `CHANGE_ME` / 占位符

`k8s-create-v1-6-secret.example.sh` 会硬拒绝。处理：

```bash
# 云端
./scripts/41_export_v1_6_client_env.sh --show-secrets

# scp 到 WSL
set -a; source client/robot-dh-platform.env; set +a
./client/k8s-create-platform-secret.example.sh
```
