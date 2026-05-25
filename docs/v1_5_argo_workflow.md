# v1.5 Argo Workflow 接入说明

本文档与 `argo/README.md` 配合使用，记录 v1.5 Argo Workflows 接入设计、上线步骤、验收清单与故障排查。

## 1. 设计目标

- 在 kind / Kubernetes 上以 Argo Workflows DAG 调度 v1.5 的 scale ETL / benchmark / build-ads 流水线。
- 复用现有 `robot-data-harness:local` 镜像；不引入 Operator，不维护额外的前端。
- 所有真实凭据通过 K8s Secret 注入，仓库内只保留 `secret.example.yaml` 占位。
- 不在 Pod 内使用 WSL `127.0.0.1` SSH tunnel；Secret 必须填云端可达地址。

## 2. 目录与资源

| 资源 | 路径 |
| --- | --- |
| Argo namespace 安装脚本 | `argo/scripts/argo_install.sh` |
| WorkflowTemplate (scale-etl) | `argo/templates/robot-dh-scale-etl-workflowtemplate.yaml` |
| WorkflowTemplate (benchmark) | `argo/templates/robot-dh-benchmark-workflowtemplate.yaml` |
| WorkflowTemplate (build-ads) | `argo/templates/robot-dh-build-ads-workflowtemplate.yaml` |
| CronWorkflow (scale-etl) | `argo/cron/scale-etl-cronworkflow.yaml` |
| Workflow submit 样板 | `argo/workflows/submit-*.yaml` |
| ServiceAccount / RBAC | `k8s/v1_5_argo/serviceaccount.yaml` `role.yaml` `rolebinding.yaml` |
| ConfigMap | `k8s/v1_5_argo/configmap.yaml` |
| Secret 示例 | `k8s/v1_5_argo/secret.example.yaml` |
| Secret 生成脚本 | `scripts/k8s_create_v1_5_secret_from_env.sh` |
| Makefile target | `make argo-*` 系列 |
| v1.6 archiveLogs ConfigMap patch | `argo/install/workflow-controller-artifact-repository.yaml` |
| v1.6 archiveLogs apply / verify 脚本 | `argo/scripts/argo_apply_log_archive.sh` `argo_sync_log_archive_secret.sh` `argo_verify_log_archive.sh` |
| v1.6 archiveLogs 入口 | `make argo-enable-log-archive` |

## 3. 完整上线步骤

```
source client/robot-dh-v1-5.env

./scripts/k8s_create_v1_5_secret_from_env.sh

make docker-build
make kind-load

make argo-install
make argo-status
make argo-apply-rbac
make argo-apply-templates

make argo-submit-benchmark
make argo-logs

make argo-submit-scale-etl
make argo-logs

make argo-submit-build-ads
make argo-logs
```

如需周期触发 scale ETL：

```
make argo-apply-cron
```

## 4. 验收清单

- `make argo-status` 中：
  - `argo` namespace 下 `argo-server` / `workflow-controller` pod Ready；
  - `robot-dh` namespace 下能看到 `workflowtemplates.argoproj.io` 3 个、`cronworkflows.argoproj.io` 1 个（若已 apply cron）。
- `benchmark` workflow：
  - 在 PG `benchmark_runs` / `benchmark_cases` 有新记录；
  - `runs/benchmark/v1_5/benchmark_report.json` 至少在 Pod 内生成。
- `scale ETL` workflow：
  - `s3://robot-lake/tmp/{workflow.name}/scale30_plan.json` 存在；
  - 至少一个 shard 处理成功（`s3://robot-lake/tmp/{workflow.name}/shards/shard_*/shard_summary.json`）；
  - PG `etl_perf_runs` / `etl_shards` 出现对应记录；
  - `s3://robot-lake/ods/{dataset}/{version}/_manifest.json` 与 `dwd/` slice 出现。
- `build-ads` workflow：
  - `s3://robot-lake/ads/quality/` 下出现 parquet + `_manifest.json`。

## 5. 跨 step 数据传递策略

采用 **S3 URI 方案**，避免依赖 Argo artifact repository：

```
PLAN_URI=s3://robot-lake/tmp/{{workflow.name}}/scale30_plan.json
SHARD_RESULT_PREFIX=s3://robot-lake/tmp/{{workflow.name}}/shards
SUMMARY_URI=s3://robot-lake/tmp/{{workflow.name}}/scale30_summary.json
```

`robot-dh etl plan / run-shard / merge-summary` CLI 直接支持 `s3://` 路径，无需 sidecar 上传。

## 6. 安全增强

- WorkflowTemplate 中每个 container 都有：
  - `runAsNonRoot: true`
  - `allowPrivilegeEscalation: false`
  - `capabilities.drop: ["ALL"]`
  - `readOnlyRootFilesystem: false`（pyarrow 临时文件需要可写，否则 OOM/IO 故障，详见 7. 故障排查）
- WorkflowTemplate 设置 `activeDeadlineSeconds`、`ttlStrategy`，避免 namespace 内堆积失败 Pod；scale ETL 默认 43200 秒（12 小时）。
- ServiceAccount `robot-dh-workflow` 只能读 `robot-dh-v1-5-config` / `robot-dh-v1-5-secrets`，对 Pod / Job 只读，禁止写其它 Secret。

## 7. 常见故障

| 现象 | 排查方向 |
| --- | --- |
| Argo pod pending | `kubectl -n argo describe pod ...`；多为镜像未拉或资源不足 |
| `image pull failed` | image 未 kind load 或 tag 写错；执行 `make docker-build && make kind-load` |
| `secret missing` | 没跑 `./scripts/k8s_create_v1_5_secret_from_env.sh` |
| `S3 AccessDenied` | Secret 里 access/secret key 错，或 bucket 拼错 |
| `Postgres auth failed` | `ROBOT_DH_DB_URI` 用户名/密码不对，或 PG `pg_hba.conf` 未放行 |
| `plan URI 读不到` | plan step 没写成功；查 `kubectl logs <plan-pod>` |
| `run-shard OOMKilled` | 加 `resources.limits.memory`，或减小 `target-shard-size-gb` |
| `DeadlineExceeded` / `exit status 143` | Workflow deadline 到期；scale ETL 默认 12 小时，仍超时则按 `docs/v1_5_scale_etl_deadline_report.md` 排查 normalize 吞吐、资源和 S3 瓶颈 |
| Pod 里 endpoint 是 `127.0.0.1` | Secret 中 endpoint 错指 WSL tunnel；改云端公网/VPC 地址 |
| workflow artifact 传递失败 | 未走默认 S3 URI 方案；可在 WorkflowTemplate 中切回 emptyDir + S3 URI |
| `readOnlyRootFilesystem` 报错 | pyarrow 临时目录写失败；模板默认 `readOnlyRootFilesystem` 未开；若手动开，请 `--tmp-dir /tmp` 并挂 emptyDir |

## 8. 与 v1.4 Job 的关系

- v1.4 K8s Job（`k8s/v1_4_lake/`）保留原口径，仅追加 `runAsNonRoot` / `capabilities drop ALL` / `activeDeadlineSeconds`，行为兼容。
- v1.5 Argo Workflow 是新的调度路径，与 v1.4 Job 并存；两者读写同一份 PG 元数据。
