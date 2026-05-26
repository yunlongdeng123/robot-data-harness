# v1.7 Local Argo Workflow Templates

本目录是 v1.7 Local-First Data Runtime 的 Argo 落地工程，**默认 workflow 只跑 <=3GB
devscale**：raw 数据从 PVC `robot-dh-local-data-pvc` 读取（背后是 kind extraMounts
挂的 Windows D 盘 `/mnt/d/robot-dh-local`），lake 写到同一 PVC，不访问任何远端 S3。

scale30 远端 workflow 仍保留在 `argo/templates/`（v1.6），只用于手动压测；
默认 Makefile target 不会自动提交 scale30。

## 目录

```
argo/v1_7_local/
├── templates/
│   ├── robot-dh-local-devscale-workflowtemplate.yaml   # 主 DAG：doctor → verify → 3 family（probe → qc → normalize → features）→ build-ads → ml-ready → benchmark → lineage → sync → logs index
│   ├── robot-dh-local-qc-workflowtemplate.yaml         # 只跑 verify + 3 family qc，调试用
│   └── robot-dh-local-ml-ready-workflowtemplate.yaml   # 只跑 build-ads + ml-ready，假设 lake/dwd 已就绪
├── workflows/
│   ├── submit-local-devscale.yaml
│   ├── submit-local-qc.yaml
│   └── submit-local-ml-ready.yaml
├── cron/
│   └── local-devscale-cronworkflow.yaml                # 凌晨 2:30 跑一遍 devscale 回归（concurrencyPolicy=Forbid）
├── scripts/
│   ├── submit_local_devscale.sh                        # context/PVC/template 三重前置检查 + 提交
│   ├── watch_local_workflow.sh                         # 每 N 秒 echo 节点 phase，看终态
│   ├── tail_live_workflow_logs.sh                      # 真·follow：DAG 中后续 pod 起来后自动 attach
│   └── sync_workflow_steps.sh                          # argo sync + argo logs index（dry-run 可用）
└── README.md
```

## 主 DAG 一览

```
local-runtime-doctor
   └── verify-devscale-data          # 一旦失败，下游全部 Skip（依赖 depends）
         ├── adapter-probe-droid     ─┐
         ├── adapter-probe-robomimic ─┼─ 三个 family 并行
         └── adapter-probe-bridge    ─┘

   droid-qc      → droid-normalize      → droid-features      ─┐
   robomimic-qc  → robomimic-normalize  → robomimic-features  ─┼─ build-ads
   bridge-qc     → bridge-normalize     → bridge-features     ─┘
                                                              → ml-ready-export
                                                              → benchmark-regression
                                                              → publish-lineage
                                                              → argo-sync
                                                              → archive-logs-index
```

## 与 v1.6 的差异

| 维度                  | v1.6 multisource-scale30                       | v1.7 local-devscale                                  |
|-----------------------|-----------------------------------------------|-----------------------------------------------------|
| dataset_uri           | `s3://robot-datasets/raw/...`                  | `file:///mnt/local-data/robot-dh-local/raw/...`     |
| lake_root             | `s3://robot-lake`                              | `file:///mnt/local-data/robot-dh-local/lake`        |
| 数据规模              | 30+ GB 单 family                               | <=3 GB 全 family 合计                                |
| volume                | emptyDir 32Gi + Argo S3 archive                | PVC `robot-dh-local-data-pvc` + tmp emptyDir         |
| 前置 step             | discover-assets                                | local-runtime-doctor + verify-devscale-data         |
| activeDeadlineSeconds | etl-phase 21600 / qc 1800 / workflow 43200     | etl-phase 3600 / qc 900 / workflow 7200             |
| 失败语义              | scale30 retry 1                                | retry 1（benchmark=0）                              |

## 提交

```bash
# 一次性 apply
make argo-local-apply

# 提交默认主 DAG（同时启动 tail）
./argo/v1_7_local/scripts/submit_local_devscale.sh --watch

# 只跑 QC / 只跑 ml-ready
kubectl -n robot-dh create -f argo/v1_7_local/workflows/submit-local-qc.yaml
kubectl -n robot-dh create -f argo/v1_7_local/workflows/submit-local-ml-ready.yaml
```

## 真·follow 日志

```bash
WF=$(kubectl -n robot-dh get wf -l role=devscale-main -o jsonpath='{.items[-1:].metadata.name}')
./argo/v1_7_local/scripts/tail_live_workflow_logs.sh "$WF" --container main
```

与 `kubectl logs -l workflow=$WF -f` 的本质差异：后者只 stream **调用瞬间**已存在的
pod；DAG 中 build-ads → ml-ready 等后续 step 起来后**不会**自动 attach。本脚本
每 3 秒 poll 一次 workflow JSON，发现新 Pod 立即 `kubectl logs -f` 后台 attach；
pod 失败时自动 `describe pod` + `logs --previous`；workflow 终态后回收所有后台 tail。

## archive logs

devscale 默认 `archive_root=file:///mnt/local-data/robot-dh-local/lake/argo-logs`，
跟 PVC 同一卷，pod 终态后日志会被 Argo controller 归档到这里（前提：
`workflow-controller-configmap` 的 `archiveLogs` 指向同一 file URI，否则就只剩
`kubectl logs --previous` 兜底）。

如果你想把日志归档到 S3，把 workflow.parameters 里的 `archive_root` 改成
`s3://robot-dh-artifacts/argo-logs`，并确保 `robot-dh-local-secrets` 提供了
`ROBOT_DH_S3_*` 三个字段；devscale 默认是不需要任何 secret 的。

## 资源 / 截断

| step                  | requests       | limits         | activeDeadlineSeconds | retryStrategy |
|-----------------------|---------------|---------------|----------------------|---------------|
| doctor / verify / probe / qc | 250m / 512Mi  | 1 / 2Gi       | 900                  | limit: 1      |
| normalize / features  | 500m / 1Gi    | 2 / 4Gi       | 3600                 | limit: 1      |
| ads / ml-ready / benchmark | 500m / 1Gi    | 2 / 4Gi       | 1800                 | limit: 1（benchmark=0）|
| publish-lineage / argo-sync / archive-logs-index | 250m / 256~512Mi | 1 / 1~2Gi | 600 | limit: 1 |
| **整 workflow**       |               |               | **7200**             |               |

## 安全

所有 container 走相同模板：

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: false   # 见下
  capabilities:
    drop: ["ALL"]
```

`readOnlyRootFilesystem` 暂设 `false`：

- `tee /tmp/...` 需要写 `/tmp`，否则 tee 失败、stdout 又会被丢；
- `matplotlib / fontconfig` 默认会写 `~/.config`，本镜像虽已重定向到 `/tmp`，但 lerobot /
  pyarrow 等依赖偶尔仍会摸 root fs；
- 唯一**有真实可写**的位置是 `/tmp`（emptyDir）和 PVC mountPath，不会落到镜像层。

后续如果想收紧到 `readOnlyRootFilesystem: true`，需要给所有 step 加 `/var/tmp`、
`/.config`、`/.cache` 的 emptyDir + env 重定向，工程量大、收益小，列为 v1.7+ 后续工作。

## 已知差异 / 不做的事

- 不引入 Argo CLI（参见仓库 memory：「robot-data-harness 已脱钩 argo 官方 CLI」），所有提交 / 看日志走 `kubectl`。
- 不把默认 `make argo-local-submit` 指向 scale30；scale30 必须显式 `make argo-submit-multisource-scale30`。
- 不在 v1.7 阶段重写 Go exporter / FastAPI 控制面；它们继续按 v1.6 入口提供 metrics / 查询。
