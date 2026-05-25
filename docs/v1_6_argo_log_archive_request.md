# Argo Workflows 日志归档需求（交付 WSL/kind 项目）

> 提交方：`robot-dh-infra`（云端 PostgreSQL / MinIO / Redis）
> 接收方：WSL/kind 上部署 `argo-server` / `workflow-controller` 的项目
> 优先级：P1（影响 v1.5/v1.6 scale ETL 的根因追溯能力）
> 关联：[`docs/v1_5_argo_env.md`](v1_5_argo_env.md)、[`docs/v1_6_storage_and_deadline_notes.md`](v1_6_storage_and_deadline_notes.md)

## 1. 背景

- v1.5 scale30 ETL 在 `activeDeadlineSeconds=7200` 被 controller 杀掉，事后只能看 `etl_perf_runs` 留下的 `status='running'` 孤儿记录与 Argo 网页上还在线的实时 log。详见 [`docs/v1_6_storage_and_deadline_notes.md`](v1_6_storage_and_deadline_notes.md) §1。
- 当前 Argo 网页节点 log 面板会显示告警：
  > Your pod GC settings will delete pods and their logs immediately on completion. Logs may not be available.
- 也就是说，**workflow 一旦终态，step pod 立即被 GC，stdout/stderr 同步消失**。一旦失败发生在没人盯屏的时段，事后无法回溯。
- v1.6 已经把"运行时观测"在 PostgreSQL 这一侧建好（`task_heartbeats / dataset_partitions / workflow_steps / openlineage_events`，见 [`postgres/migrations/005_v1_6_robot_platform.sql`](../postgres/migrations/005_v1_6_robot_platform.sql)），但**容器级 stdout/stderr 与 Python traceback** 仍只能从 pod log 拿到，因此必须有独立的 log 归档通道。

## 2. 责任分工（重要）

| 项目 | 范围 |
|------|------|
| `robot-dh-infra`（本仓库） | 云端 PG / MinIO / Redis、bucket 与 policy、`ROBOT_DH_*` Secret 模板 |
| WSL/kind 项目（**本文档接收方**） | `argo-server`、`workflow-controller`、`workflow-controller-configmap`、kind 集群本身 |
| `robot-data-harness` 主项目 | step container 镜像、CLI、heartbeat / adapter / OpenLineage emitter |

本需求**只**需要 WSL/kind 那边改一处：`workflow-controller-configmap` 的 `artifactRepository` + `archiveLogs`。本仓库不会、也不应该去改 argo 控制面。

## 3. 需求总结

让 Argo 在 step pod 终态时，把 pod 的 stdout/stderr 永久归档到云端 MinIO，并满足：

1. **不污染**已有 validator / quality gate 产物的命名空间（`s3://robot-dh-artifacts/runs/{run_id}/`，见 `README.md` §10.7.3 中 `robot-dh-artifacts` 主要对象表）。
2. **不新建 bucket**：复用已有 `robot-dh-artifacts`，应用账号已经具备读写权限（见 [`minio/policies/robot_dh_readwrite.json`](../minio/policies/robot_dh_readwrite.json)）。
3. **可按 workflow / pod 定位**：路径里要带 workflow name 与 pod name，方便用 `mc cp` / `mc cat` 直接拉取。
4. **可回收**：归档对象有明确的 lifecycle，不要让 `robot-dh-artifacts` 无限膨胀。

## 4. 推荐 bucket / prefix / keyFormat

| 项 | 值 | 说明 |
|----|----|------|
| bucket | `robot-dh-artifacts` | 复用现有 bucket，应用账号已具备读写权限 |
| 顶层 prefix | `argo-logs/` | **新增一级 prefix**，与 `runs/`（validator）、`tmp/`（短期）并列，互不污染 |
| keyFormat | `argo-logs/{{workflow.namespace}}/{{workflow.name}}/{{pod.name}}/main.log` | 与 Argo 默认 `keyFormat` 同构，只换了顶层 prefix |
| accessKey / secretKey | 见 `robot-dh-v1-6-secrets` 中的 `ROBOT_DH_S3_ACCESS_KEY` / `ROBOT_DH_S3_SECRET_KEY` | 已经挂载到 `robot-dh` namespace 的 step pod，controller 也可直接挪用 |
| endpoint | `ROBOT_DH_S3_ENDPOINT_URL`（公网 IP/DNS:9000） | 与 step pod 用同一个 endpoint，避免再走一条网络路径 |
| insecure | `true`（HTTP）/ `false`（HTTPS） | 与 step pod 现状一致即可，本仓库当前用 HTTP |

> `tmp/` 与 `argo-logs/` 一定**不要合并**：`tmp/` 走 7 天 ILM（见 [`docs/v1_5_scale_runbook.md`](v1_5_scale_runbook.md) §3），节点 log 至少要留够事后定位窗口（≥ 30 天）。

## 5. WSL/kind 侧操作清单

### 5.1 ConfigMap patch 草稿（基于 Argo Workflows 默认 controller-configmap）

> 这是建议的 patch，最终生效字段名以你们集群里实际安装的 chart / manifest 为准（Helm `artifactRepository:` 或 ConfigMap `artifactRepository: |` 都行）。

```yaml
# kubectl -n argo edit configmap workflow-controller-configmap
apiVersion: v1
kind: ConfigMap
metadata:
  name: workflow-controller-configmap
  namespace: argo
data:
  artifactRepository: |
    archiveLogs: true
    s3:
      endpoint: PUBLIC_HOST:9000      # 与 step pod 同 endpoint
      bucket: robot-dh-artifacts
      keyFormat: "argo-logs/{{workflow.namespace}}/{{workflow.name}}/{{pod.name}}/main.log"
      insecure: true                  # 当前 MinIO 走 HTTP；上 HTTPS 改 false 并去掉 insecure
      accessKeySecret:
        name: robot-dh-v1-6-secrets
        key: ROBOT_DH_S3_ACCESS_KEY
      secretKeySecret:
        name: robot-dh-v1-6-secrets
        key: ROBOT_DH_S3_SECRET_KEY
```

注意：

- `archiveLogs: true` 是开关；不开就算配了 `s3` 也只对 `outputs.artifacts` 生效，pod log 不会落盘。
- `accessKeySecret` / `secretKeySecret` 引用的 Secret 必须与 **workflow-controller** 同 namespace（通常是 `argo`）。如果 controller 在 `argo` namespace、Secret 在 `robot-dh` namespace，需要在 `argo` namespace **复制一份同名 Secret**，或者改 controller 的 RBAC 让它能读 `robot-dh/robot-dh-v1-6-secrets`。推荐前者，避免跨 namespace 读 Secret。
- WSL/kind 那边需要确认 `PUBLIC_HOST:9000` 可从 controller pod 到达；这与 step pod 走同一条出口，无需新开防火墙规则。

### 5.2 RBAC / Secret 复制（如果 controller 不在 `robot-dh` namespace）

```bash
SRC_NS=robot-dh
DST_NS=argo

kubectl -n "$SRC_NS" get secret robot-dh-v1-6-secrets -o yaml \
  | sed "s/namespace: $SRC_NS/namespace: $DST_NS/" \
  | kubectl apply -f -
```

> 仅复制 `ROBOT_DH_S3_*` 字段也可以；推荐整 Secret 复制，避免日后 Secret rotate 漏改一处。

### 5.3 PodGC / 节点保留策略

- `archiveLogs` 是**在 pod 进入终态时**把 pod log 上传到 S3。这意味着 controller 必须在 pod 被 GC **之前**完成上传。
- 现状是 "pods deleted immediately on completion"，存在竞态。请把 WorkflowTemplate 顶层 `podGC.strategy` 调成 `OnWorkflowSuccess` 或 `OnWorkflowCompletion`，给 controller 足够时间归档。

```yaml
spec:
  podGC:
    strategy: OnWorkflowCompletion
```

### 5.4 验证步骤

1. 提交任意 WorkflowTemplate（推荐拿 `etl-run-bridgedata_v2_scale30` 这种长任务复测）。
2. 等 workflow 终态。
3. 在云端跑：

   ```bash
   mc alias set local http://127.0.0.1:9000 robotdhapp '***'
   mc ls -r local/robot-dh-artifacts/argo-logs/ | head
   mc cat local/robot-dh-artifacts/argo-logs/<ns>/<workflow.name>/<pod.name>/main.log | head
   ```

4. 在 Argo 网页打开同一 workflow 的同一个 node，确认 log 内容与 MinIO 上的对象一致。

## 6. 命名与生命周期约定（建议 `robot-dh-infra` 这边落地）

WSL/kind 改完之后，本仓库会补两件事，**不需要 WSL/kind 侧操作**：

- 在 [`README.md`](../README.md) §10.7.3 `robot-dh-artifacts` 主要对象表里加一行 `argo-logs/{namespace}/{workflow.name}/{pod.name}/main.log`。
- 在 [`docs/v1_5_scale_runbook.md`](v1_5_scale_runbook.md) §3 lifecycle 表里加：
  - `robot-dh-artifacts/argo-logs/` → 建议 **30 天过期**，人工 `mc ilm rule add`（不进入 `28_minio_lifecycle_plan.sh --apply` 的白名单，避免脚本自动改）。
- `40_storage_tmp_lifecycle_audit.sh` 仍然只动 `tmp/`，**不会**触碰 `argo-logs/`。

## 7. 安全 / 权限要点

- 应用账号 `robotdhapp` 已经具备 `robot-dh-artifacts/*` 的 `PutObject / GetObject / DeleteObject` 权限（见 [`minio/policies/robot_dh_readwrite.json`](../minio/policies/robot_dh_readwrite.json)），**无需新开 policy**。
- 不要把 controller 的 S3 凭据写在 ConfigMap 里；务必走 `accessKeySecret` / `secretKeySecret`。
- 归档对象路径里**不会**带任何敏感字段；但 step container 自己往 stdout 打 `psycopg` / `boto3` debug log 时可能把连接串吐出来——这部分由 robot-data-harness 主项目把 client 配成 `--log-level INFO` 控制，不在本需求范围。

## 8. 非本需求范围

以下问题在 v1.5/v1.6 排障截图里出现过，但归属在 robot-data-harness 主项目，请单开 issue，不在本文档解决：

| 现象 | 归属 | 修复方向 |
|------|------|----------|
| `heartbeat jsonl write failed: [Errno 13] Permission denied: '/app/runs/events/heartbeats_*.jsonl'` | robot-data-harness 镜像 | heartbeat 默认路径改成可写卷（推荐 emptyDir / `/tmp`），或镜像 build 时 `mkdir -p && chmod` |
| `ValueError: Unable to extract pose episodes from HuggingFace-style dataset. ... Add an explicit adapter mapping for this dataset schema.` | robot-data-harness normalize adapter | 给 `bridgedata_v2_scale30` 注册显式列映射（pose-like 列） |
| `etl_perf_runs` 留下 `status='running'` 孤儿记录 | robot-data-harness ETL CLI | 在 step container 收到 SIGTERM / 容器退出前把 `status` 写为 `failed` |

## 9. 验收清单（本需求闭环）

| 项 | 责任方 | 通过标准 |
|----|--------|----------|
| `workflow-controller-configmap` 已开启 `archiveLogs: true` | WSL/kind | `kubectl -n argo get cm workflow-controller-configmap -o yaml \| grep archiveLogs` 返回 `archiveLogs: true` |
| 提交一个 workflow 后，`mc ls robot-dh-artifacts/argo-logs/...` 能列出对应对象 | WSL/kind 自验 | 至少 1 个 `main.log` 对象，size > 0 |
| Argo 网页 node log 与 MinIO 上对象内容一致 | WSL/kind 自验 | `mc cat` 头 50 行与网页前 50 行匹配 |
| `robot-dh-artifacts/runs/` 不受影响 | `robot-dh-infra` 侧 | `mc ls robot-dh-artifacts/runs/` 对象数 / 大小与归档启用前一致 |
| README / runbook 已更新 `argo-logs/` prefix | `robot-dh-infra` 侧 | 见 §6 |

## 10. 时间窗口

- 第 1 步（ConfigMap 改 + Secret 复制）：< 30 min
- 第 2 步（一次 workflow 验证）：复用任何现有 30 GB 级 ETL 即可
- 第 3 步（本仓库补 README / lifecycle 条目）：本仓库这边在 WSL/kind 验证通过后追加一次提交

请 WSL/kind 同事完成 §5 后回一份截图（`mc ls -r robot-dh-artifacts/argo-logs/ | head`），本仓库这边再落第 3 步。

## 11. 落地状态（2026-05-24）

### 11.1 WSL/kind 侧

WSL/kind 项目已按 §5 全量实现并 apply，详见交还回执 [`docs/v1_6_argo_log_archive_handoff.md`](v1_6_argo_log_archive_handoff.md)：

- `argo/install/workflow-controller-artifact-repository.yaml` + `argo_apply_log_archive.sh` 把 `archiveLogs: true` 与 `s3` 字段 patch 到 `workflow-controller-configmap`
- `argo_sync_log_archive_secret.sh` 仅把 `ROBOT_DH_S3_ACCESS_KEY` / `ROBOT_DH_S3_SECRET_KEY` 复制到 `argo` namespace
- 所有 WorkflowTemplate 的 `podGC.strategy` 已切到 `OnWorkflowCompletion`
- `make argo-verify-log-archive` 三段验证全绿（ConfigMap 字段、Secret 字段、对象端可选）

### 11.2 本仓库（`robot-dh-infra`）落地的 follow-up

| 项 | 文件 | 状态 |
|----|------|------|
| `robot-dh-artifacts` bucket 描述补 `argo-logs/` 主要内容 | [`README.md`](../README.md) §10.6.3 bucket 表 | 已落 |
| `robot-dh-artifacts` 主要对象段落追加 `argo-logs/` 路径 / 写入方 / 前置条件 / lifecycle 说明 | [`README.md`](../README.md) §10.6.3 | 已落 |
| MinIO lifecycle 表追加 `robot-dh-artifacts/argo-logs/` 30 天人工 ILM 行 | [`docs/v1_5_scale_runbook.md`](v1_5_scale_runbook.md) §3 | 已落 |
| 显式声明 `28_minio_lifecycle_plan.sh --apply` 不动 `argo-logs/`，给出 `mc ilm rule add` 命令模板 | [`docs/v1_5_scale_runbook.md`](v1_5_scale_runbook.md) §3 | 已落 |
| `40_storage_tmp_lifecycle_audit.sh` 保持只看 `tmp/`，不需改动 | 见 handoff §6.3 | 不动（保持） |
| `robotdhapp` policy 已覆盖 `robot-dh-artifacts/*` 读写删 | [`minio/policies/robot_dh_readwrite.json`](../minio/policies/robot_dh_readwrite.json) | 不需新建 |

可选 follow-up（暂不动，下一个迭代再评估）：

- 新增 `scripts/42_storage_argo_logs_audit.sh`：read-only 列 `argo-logs/` 体积 / object 数 / 最旧对象时间，与 `40_storage_tmp_lifecycle_audit.sh` 解耦（handoff §6.3 建议另起，避免与 `tmp/` 语义混淆）

### 11.3 实测验证（workflow `robot-dh-multisource-scale30-fhkvr`）

云端 `mc ls rdh/robot-dh-artifacts/` 当前结果：

```text
runs/
tests/
```

`argo-logs/` prefix 尚未出现对象。诊断结论：

- `archiveLogs` 由 step pod 的 `wait` sidecar 在 pod 终态时上传，**配置在 pod 创建时注入**，不会回填到已存在的 pod
- WSL/kind 终端时间线显示 `make argo-enable-log-archive` → `make argo-apply-templates` → `make argo-platform-status`，而 `fhkvr` 已经 `Running 105m`，说明它在 controller restart **之前**就已提交
- 因此 `fhkvr` 这一条的 step pod 大概率没有归档 sidecar 配置，即便 controller 已经开启 `archiveLogs`，已存在的 pod 也不会回写
- 验收对象端这一项，需要等 `fhkvr` 终态后**重新提交一条** workflow（例如 `make argo-submit-multisource-scale30` 或新触发 `contract-qc`），让 step pod 在 controller restart 之后创建

建议的下一步实测命令（在云端运行）：

```bash
# 1) 在 WSL/kind 触发新一条 workflow（确保 step pod 在 controller restart 之后创建）
#    make argo-submit-multisource-scale30   # 或 make argo-submit-contract-qc

# 2) 等终态后云端验证
mc ls -r rdh/robot-dh-artifacts/argo-logs/ | head
mc cat rdh/robot-dh-artifacts/argo-logs/robot-dh/<workflow.name>/<pod.name>/main.log | head -50
```

只要 §11.3 的两条命令能列出 ≥ 1 个 `main.log` 且与 Argo 网页同一 node 头部内容一致，则 §9 验收清单第 2/3 项闭环，本需求整体关单。


