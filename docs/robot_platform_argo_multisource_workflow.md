# v1.6.4 — Argo Multi-source Robot Data Platform DAG

> 把 v1.5 的「单脚本 ETL」升级为多源、多阶段、多工具编排的真正 DAG。

## DAG 总览

```
discover-assets
  ├─> droid-qc        ─> droid-partition     ─> droid-normalize     ─> droid-features
  ├─> robomimic-qc    ─> robomimic-partition ─> robomimic-normalize ─> robomimic-features
  └─> bridge-qc       ─> bridge-partition    ─> bridge-normalize    ─> bridge-features

(droid-features && robomimic-features && bridge-features)
  └─> build-ads
       ├─> benchmark-regression
       └─> ml-ready-export ─> publish-lineage-report ─> argo-sync
```

- 三分支可并发；任一 branch 数据缺失会让 contract / partition 早期 SKIP，但不会让 workflow 整体失败。
- `fail_on_contract_fail=false` 时即使 QC FAIL 也继续往下，最终 workflow status 标记 WARN/FAIL，靠 lineage report 汇总。

## 文件分布

| 文件 | 作用 |
|---|---|
| `argo/templates/robot-dh-multisource-scale30-workflowtemplate.yaml` | 三源主 DAG |
| `argo/templates/robot-dh-contract-qc-workflowtemplate.yaml` | 仅跑 QC |
| `argo/templates/robot-dh-ml-ready-workflowtemplate.yaml` | 仅跑 ML-ready export |
| `argo/cron/multisource-scale30-cronworkflow.yaml` | 每天 UTC 02:00 自动跑主 DAG |
| `argo/workflows/submit-multisource-scale30.yaml` 等 | 一次性 submit 文件 |

所有容器：

```yaml
envFrom:
  - secretRef: { name: robot-dh-v1-6-secrets }
  - configMapRef: { name: robot-dh-v1-6-config, optional: true }
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  allowPrivilegeEscalation: false
  capabilities:
    drop: ["ALL"]
```

资源：

| step | requests | limits |
|---|---|---|
| profile / qc / partition / lineage / sync | cpu 200m–500m, mem 256Mi–1Gi | cpu 1, mem 1–2Gi |
| normalize / features | cpu 1, mem 2Gi | cpu 2, mem 6Gi |
| ads / benchmark / ml-ready | cpu 500m, mem 1Gi | cpu 2, mem 4Gi |

`activeDeadlineSeconds`：workflow 43200（12h）；ETL step 21600（6h）；其余 step 7200（2h）。

## 工作流元数据 sync

每次 workflow 末尾的 `argo-sync` step 会调：

```bash
robot-dh argo sync --workflow-name {{workflow.name}} --namespace robot-dh
```

行为：
1. `kubectl get workflow <name> -o json`（容器内需 RBAC：list/get workflows）。
2. 解析 `metadata / spec / status / nodes`。
3. UPSERT `workflow_runs`（按 `(namespace, name)` 唯一），UPSERT `workflow_steps`（按 `(namespace, workflow_name, step_name)` 唯一）。
4. 失败时 stderr 报错；不阻断主 DAG（retryStrategy=1 + soft warehouse）。

人工触发：`make argo-sync-latest` —— 自动取 robot-dh ns 中最新一个 workflow 跑同步。

## lineage report

`publish-lineage-report` step 调：

```bash
robot-dh lineage report --workflow-name {{workflow.name}} \
  --output s3://robot-lake/lineage/reports/{{workflow.name}}.json
```

报告里聚合：
- `workflow_run`：workflow 状态 + 参数
- `workflow_steps`：所有 step 时序 + phase
- `qc_runs`：相关 QC 报告
- `ml_ready_datasets`：本次产出
- `asset_profiles`：相关 profile

PG 不可达时给空数组而非 raise，保证 sync step 不会拖垮 workflow。

## Makefile 一键命令

```bash
make argo-apply-v1-6           # apply 三个 WorkflowTemplate + CronWorkflow
make argo-submit-contract-qc   # 提交一次纯 QC
make argo-submit-multisource-scale30
make argo-submit-ml-ready
make argo-sync-latest          # 把最新 workflow 状态写 PG
make argo-v1-6-logs
make argo-v1-6-status
make v1-6-platform-smoke       # 检查 secret / image / template 是否就位
```

## 常见故障

| 现象 | 排查思路 |
|---|---|
| `secret missing` | `make v1-6-platform-smoke` 看哪一项缺；`./scripts/k8s_create_platform_secret_from_env.sh` 重建 |
| `endpoint 127.0.0.1` | 容器里 SSH tunnel 不可用，需要换成集群可达地址；secret 创建脚本会硬校验 |
| `S3 AccessDenied` | 检查 IAM 策略包含 `s3:GetObject / PutObject / ListBucket`；bucket policy 是否允许该 ServiceAccount 关联的 IAM Role |
| `Postgres auth failed` | 检查 `ROBOT_DH_DB_URI` 是否指向 v1.6 已 apply schema 的实例；`postgres/migrations/005_robot_platform.sql` 是否落库 |
| `normalize heartbeat 不更新` | `tail -f runs/events/heartbeats_*.jsonl` 或查 `task_heartbeats` 表，确认 step 是哪一阶段卡住 |
| `DeadlineExceeded` | 看 step 级 deadline；可拆 partition 后用多个 normalize step 并行 |
| `OOMKilled` | 调 step 级 limit；`build_pose_table` 大数据集需要更高 mem |

## 不在本阶段做的事

- 不实现 Go Operator
- 不做复杂前端
- 不在 API 内 submit Argo workflow（POST /workflows/scale30 返回 501）
- 不写真实 secret 到仓库
