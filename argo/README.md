# Argo Workflows for robot-data-harness v1.5

本目录给 `robot-data-harness` v1.5 提供 Kubernetes-native 编排：

- **WorkflowTemplate**：scale ETL DAG / benchmark DAG / build ADS。
- **CronWorkflow**：周期 scale ETL 触发。
- **install / RBAC**：本地 kind 调试用的 Argo 安装脚本与 robot-dh namespace 内的 ServiceAccount / Role / RoleBinding。
- **shell scripts**：submit / wait / logs / delete 等运维脚本。

## 为什么引入 Argo

`robot-data-harness` v1.4 已经覆盖单数据集 `etl run` / 批量 `etl scan`。v1.5 加入：

- 多 shard 并行执行（scale 30GB+ 数据）
- 步骤级 retry（Argo `retryStrategy`）
- DAG 形式的依赖（plan → run-shard-N → merge → build-ads）
- 统一可视化（Argo UI）

不需要 Go Operator，不需要复杂前端；直接复用 `robot-data-harness:local` 镜像即可。

## 本地 kind 前置

1. Docker image 已加载到 kind 节点：
   ```
   make docker-build
   make kind-load
   ```
2. v1.5 Secret 已经在 `robot-dh` namespace 创建（**绝不**提交真实凭据）：
   ```
   source client/robot-dh-v1-5.env
   ./scripts/k8s_create_v1_5_secret_from_env.sh
   ```
3. kind Pod **不能**用 WSL 的 `127.0.0.1` SSH tunnel；Secret 中的 endpoint 必须填云服务公网 IP / DNS（或 Pod 网络可达地址）。

## 执行顺序

```
source client/robot-dh-v1-5.env
./scripts/k8s_create_v1_5_secret_from_env.sh
make docker-build
make kind-load
make argo-install
make argo-apply-rbac
make argo-apply-templates
make argo-submit-scale-etl
make argo-logs
```

scale ETL 是长时间任务，模板默认 `activeDeadlineSeconds: 43200`（12 小时）。建议在 `tmux` 中提交并等待：

```
tmux new -s robot-dh-scale-etl
cd /home/yunlong/workspace/robot-data-harness
make argo-apply-templates

wf=$(kubectl -n robot-dh create -f argo/workflows/submit-scale30-etl.yaml -o jsonpath='{.metadata.name}')
echo "workflow=${wf}"

TIMEOUT=43200 ./argo/scripts/argo_wait_workflow.sh "${wf}"
```

另开窗口看 Pod 与单个 shard 日志：

```
wf="robot-dh-scale30-etl-xxxxx"  # 替换为上一步输出的 workflow 名称
kubectl -n robot-dh get pods -l workflows.argoproj.io/workflow="${wf}" -w
kubectl -n robot-dh logs -f <pod-name> -c main
```

也可以单独提交 benchmark / build-ads：

```
make argo-submit-benchmark
make argo-submit-build-ads
```

需要周期触发：

```
make argo-apply-cron
```

## 目录结构

```
argo/
  README.md
  install/
    namespace.yaml
    workflow-controller-artifact-repository.yaml   # v1.6 archiveLogs ConfigMap patch 模板
  templates/
    robot-dh-scale-etl-workflowtemplate.yaml
    robot-dh-benchmark-workflowtemplate.yaml
    robot-dh-build-ads-workflowtemplate.yaml
    robot-dh-multisource-scale30-workflowtemplate.yaml
    robot-dh-contract-qc-workflowtemplate.yaml
    robot-dh-ml-ready-workflowtemplate.yaml
  workflows/
    submit-scale30-etl.yaml
    submit-benchmark.yaml
    submit-build-ads.yaml
  cron/
    scale-etl-cronworkflow.yaml
  scripts/
    argo_install.sh
    argo_submit_scale_etl.sh
    argo_submit_benchmark.sh
    argo_wait_workflow.sh
    argo_get_latest_logs.sh
    argo_delete_completed.sh
    argo_sync_log_archive_secret.sh   # v1.6 把 S3 secret 复制到 argo namespace
    argo_apply_log_archive.sh         # v1.6 patch workflow-controller-configmap
    argo_verify_log_archive.sh        # v1.6 验证 archiveLogs 是否生效
```

## v1.6 Argo 日志归档（来自 robot-dh-infra 的需求）

需求来源：[`docs/history/v1_6_argo_log_archive_request.md`](../docs/history/v1_6_argo_log_archive_request.md)。
完成回执：[`docs/history/v1_6_argo_log_archive_handoff.md`](../docs/history/v1_6_argo_log_archive_handoff.md)。

工作流：

1. 平台 secret 已 apply（`scripts/k8s_create_platform_secret_from_env.sh` 写出 `robot-dh/robot-dh-v1-6-secrets`）。
2. Argo 已安装（`make argo-install`）。
3. 一键启用日志归档：

   ```
   make argo-enable-log-archive
   # = argo-sync-log-archive-secret + argo-apply-log-archive + argo-verify-log-archive
   ```

4. 重新 apply WorkflowTemplate（已把 `podGC.strategy` 改成 `OnWorkflowCompletion`，给 controller 留出归档窗口）：

   ```
   make argo-apply-templates
   make argo-apply-platform
   ```

5. 提交一个 workflow，等终态后从 MinIO 验证：

   ```
   mc ls -r local/robot-dh-artifacts/argo-logs/ | head
   mc cat local/robot-dh-artifacts/argo-logs/<ns>/<workflow.name>/<pod.name>/main.log | head
   ```

   或者：

   ```
   CHECK_OBJECTS=1 MC_ALIAS=local make argo-verify-log-archive
   ```

注意：

- 对象路径模板与远端 robot-dh-infra 的 lifecycle 规则强耦合，**任何改动**都要先同步给 `docs/history/v1_6_argo_log_archive_request.md` 那边。
- 不要把 `accessKey` / `secretKey` 直接写进 ConfigMap；`argo_apply_log_archive.sh` 永远引用 `argo/robot-dh-v1-6-secrets`。
- `endpoint` 由脚本从 `robot-dh/robot-dh-v1-6-secrets.ROBOT_DH_S3_ENDPOINT_URL` 推断 host:port + insecure 字段；脚本会拒绝 127.0.0.1/localhost。

K8s manifests for v1.5 RBAC live in `k8s/v1_5_argo/`.

## plan / shard summary 跨 step 传递

WorkflowTemplate 默认通过 **S3 URI** 在 step 之间传递 plan 与 shard summary，避免依赖 Argo artifact repository：

```
PLAN_URI=s3://robot-lake/tmp/{{workflow.name}}/scale30_plan.json
SHARD_RESULT_PREFIX=s3://robot-lake/tmp/{{workflow.name}}/shards
SUMMARY_URI=s3://robot-lake/tmp/{{workflow.name}}/scale30_summary.json
```

`robot-dh etl plan / run-shard / merge-summary` 都原生支持 `s3://` 路径作为 plan 文件位置。

## 生产 vs kind

- 生产环境推荐由集群管理员安装 Argo（Helm / kubectl apply），用单独的 ServiceAccount + 网络策略隔离。
- 本目录的 `argo/scripts/argo_install.sh` 只用于本地 kind 调试；不在生产路径执行。

## 常见故障

| 现象 | 排查思路 |
| --- | --- |
| Argo pod pending | `kubectl -n argo describe pod ...`；多半是资源不足或镜像未拉 |
| `image pull failed` | image 未 kind load 或 tag 写错；执行 `make docker-build && make kind-load` |
| `secret missing` | 没跑 `./scripts/k8s_create_v1_5_secret_from_env.sh` |
| `S3 AccessDenied` | Secret 里的 access/secret key 错，或 bucket 名拼错 |
| `Postgres authentication failed` | `ROBOT_DH_DB_URI` 用户名/密码不对，或 `pg_hba.conf` 没放行 kind 出口 IP |
| `plan URI 读不到` | 上一步 plan step 没写成功；查 `kubectl -n robot-dh logs <pod>` |
| `run-shard OOMKilled` | 加 `resources.limits.memory` 或减小 `target-shard-size-gb` |
| `DeadlineExceeded` / `exit status 143` | Workflow 或等待脚本超时；scale ETL 默认 12 小时，仍超时则查看 `normalize` 阶段吞吐和资源瓶颈 |
| `Pod 里 endpoint 是 127.0.0.1` | Secret 里 endpoint 写成了 WSL 的 SSH tunnel；改成云端公网地址 |
| workflow artifact 传递失败 | 已采用 S3 URI 方案；如开启 Argo artifact repository，请参考 `argo/templates/...` 中注释 |
