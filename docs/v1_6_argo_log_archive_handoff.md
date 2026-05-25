# Argo Workflows 日志归档完成回执（交还 `robot-dh-infra`）

> 提交方：WSL/kind 项目（`robot-data-harness` 仓库内的 argo 控制面接入）
> 接收方：`robot-dh-infra`（云端 PostgreSQL / MinIO / Redis）
> 关联需求：[`docs/v1_6_argo_log_archive_request.md`](v1_6_argo_log_archive_request.md)
> 状态：**已实现，待远端集群 apply 后做一次 workflow 验证**

## 1. 范围

完成 §3 中要求的 4 项：

1. 不污染 `runs/` 命名空间。
2. 不新建 bucket，复用 `robot-dh-artifacts`。
3. 路径带 `workflow.namespace / workflow.name / pod.name`。
4. 留出独立的 `argo-logs/` prefix，方便单独配 lifecycle。

`activeDeadlineSeconds` 触发时的 SIGTERM、`OOMKilled`、`Failed` 等终态都会触发 controller 把 pod log 上传到 MinIO；上传完成后再走 podGC。

## 2. 仓库内改动一览

| 类型 | 文件 | 说明 |
|------|------|------|
| 新增 | [`argo/install/workflow-controller-artifact-repository.yaml`](../argo/install/workflow-controller-artifact-repository.yaml) | `workflow-controller-configmap.data.artifactRepository` 模板。`endpoint` / `insecure` 用 `__ROBOT_DH_S3_*__` 占位符，由 apply 脚本渲染。 |
| 新增 | [`argo/scripts/argo_sync_log_archive_secret.sh`](../argo/scripts/argo_sync_log_archive_secret.sh) | 把 `robot-dh/robot-dh-v1-6-secrets` 中**只**复制 `ROBOT_DH_S3_ACCESS_KEY` / `ROBOT_DH_S3_SECRET_KEY` 到 `argo/robot-dh-v1-6-secrets`，避免把 DB / Redis 凭据带进 controller pod。 |
| 新增 | [`argo/scripts/argo_apply_log_archive.sh`](../argo/scripts/argo_apply_log_archive.sh) | 从源 secret 读 `ROBOT_DH_S3_ENDPOINT_URL` 推断 `host:port` + `insecure`，渲染模板后 `kubectl apply`，再 `kubectl rollout restart deploy/workflow-controller`。 |
| 新增 | [`argo/scripts/argo_verify_log_archive.sh`](../argo/scripts/argo_verify_log_archive.sh) | 三段验证：ConfigMap 字段 → Secret 字段 → （可选）`mc ls argo-logs/`。 |
| 修改 | `argo/templates/robot-dh-{scale-etl,benchmark,build-ads,multisource-scale30,contract-qc,ml-ready}-workflowtemplate.yaml` | `podGC.strategy: OnWorkflowSuccess` → **`OnWorkflowCompletion`**（也给原本没有 `podGC` 字段的 ml-ready 加上）。 |
| 修改 | `Makefile` | 新增 `argo-sync-log-archive-secret` / `argo-apply-log-archive` / `argo-verify-log-archive` / `argo-enable-log-archive`（一键串）。 |
| 修改 | `tests/test_argo_workflow_yaml.py` | 新增两组断言：所有 WorkflowTemplate 的 `podGC.strategy` 不能让 step pod 终态立刻 GC；ConfigMap 模板的 keyFormat / bucket / accessKeySecret 字段与 `robot-dh-infra` 需求一致。 |
| 修改 | `argo/README.md` `docs/v1_5_argo_workflow.md` | 把 v1.6 archiveLogs 接入步骤、Makefile target 列入资源表与目录树。 |

仓库本地 `make test`、`pytest tests/test_argo_workflow_yaml.py` 全绿。

## 3. 与 §4「推荐 bucket / prefix / keyFormat」的字段对照

| 项 | `robot-dh-infra` 要求 | 仓库实现 | 对齐 |
|----|----------------------|----------|------|
| bucket | `robot-dh-artifacts` | `argo/install/workflow-controller-artifact-repository.yaml::data.artifactRepository.s3.bucket` | 一致 |
| 顶层 prefix | `argo-logs/` | 同 | 一致 |
| keyFormat | `argo-logs/{{workflow.namespace}}/{{workflow.name}}/{{pod.name}}/main.log` | 同 | 一致 |
| accessKey | `robot-dh-v1-6-secrets.ROBOT_DH_S3_ACCESS_KEY` | 同（在 `argo` namespace 复制一份） | 一致 |
| secretKey | `robot-dh-v1-6-secrets.ROBOT_DH_S3_SECRET_KEY` | 同 | 一致 |
| endpoint | `ROBOT_DH_S3_ENDPOINT_URL` | 同（脚本运行时从 secret 读取并去掉 scheme） | 一致 |
| insecure | HTTP=true / HTTPS=false | 同（脚本根据 scheme 自动判定） | 一致 |

## 4. 在远端 / kind 集群的标准操作流程

> 前提：`scripts/k8s_create_platform_secret_from_env.sh` 已经把
> `robot-dh/robot-dh-v1-6-secrets` 写到目标 K8s 集群；`make argo-install`
> 已安装 quick-start-minimal（或等价 chart）。

```
# 1) 同步 S3 凭据到 argo namespace
make argo-sync-log-archive-secret

# 2) 把 archiveLogs + s3 patch 到 workflow-controller-configmap，
#    并 rollout restart deploy/workflow-controller
make argo-apply-log-archive

# 3) 验证 ConfigMap + Secret
make argo-verify-log-archive

# 4) 重新 apply WorkflowTemplate（podGC 策略已改）
make argo-apply-templates
make argo-apply-platform   # v1.6 多源 / contract-qc / ml-ready

# 5) 提交一个 workflow（例如 multisource-scale30）后等终态
make argo-submit-multisource-scale30
make argo-platform-status

# 6) 可选：用 mc 验证对象端
CHECK_OBJECTS=1 MC_ALIAS=local make argo-verify-log-archive
```

`make argo-enable-log-archive` 是步骤 1+2+3 的串联，方便一次跑完。

## 5. §9 验收清单逐条勾选

| 项 | 责任方 | 通过标准 | 仓库交付物 |
|----|--------|----------|------------|
| `archiveLogs: true` 已在 ConfigMap 中 | WSL/kind | `kubectl -n argo get cm workflow-controller-configmap -o yaml \| grep archiveLogs` | `argo/install/workflow-controller-artifact-repository.yaml` + `argo_apply_log_archive.sh`，apply 后 `argo_verify_log_archive.sh` 第 1 段会断言 |
| `mc ls argo-logs/` 能列出对象 | WSL/kind 自验 | 至少 1 个 `main.log` size > 0 | `argo_verify_log_archive.sh --check-objects` 段 3，需要先跑过一次 workflow |
| Argo 网页 node log 与 MinIO 上对象内容一致 | WSL/kind 自验 | `mc cat` 头 50 行匹配 | 操作步骤已写入 `argo/README.md` v1.6 节 |
| `runs/` 不受影响 | `robot-dh-infra` | `mc ls robot-dh-artifacts/runs/` 数量/大小不变 | keyFormat 顶层强制 `argo-logs/` prefix，与 `runs/` 互斥 |
| README / runbook 更新 `argo-logs/` prefix | `robot-dh-infra` | §6 中 README §10.7.3 与 v1_5_scale_runbook §3 lifecycle 表 | 由 `robot-dh-infra` 落地，本仓库不动 |

## 6. 给 `robot-dh-infra` 的 follow-up 建议

1. **lifecycle 规则**：`robot-dh-artifacts/argo-logs/` 建议 30 天自动过期，建议**不要**接入 `28_minio_lifecycle_plan.sh --apply` 白名单（与 `tmp/` 的 7 天策略明显不同），手工 `mc ilm rule add` 落一次即可。
2. **README §10.7.3 主要对象表**追加：
   - `argo-logs/{namespace}/{workflow.name}/{pod.name}/main.log`：Argo Workflows step pod stdout/stderr，30 天 ILM。
3. **`40_storage_tmp_lifecycle_audit.sh`** 已经只看 `tmp/`，**不需要**改。可以追加一条 `argo-logs/` 的可选 audit（建议另起脚本 `42_storage_argo_logs_audit.sh`，避免与 `tmp/` 的语义混淆）。
4. **policy 复核**：应用账号 `robotdhapp` 已具备 `robot-dh-artifacts/*` 读写删权限（`minio/policies/robot_dh_readwrite.json`）；本归档通道完全使用应用账号，不需要新建专用账号或 policy。
5. **凭据 rotate**：`ROBOT_DH_S3_ACCESS_KEY` / `ROBOT_DH_S3_SECRET_KEY` 一旦轮转，请在远端写客户端 env 后，执行：

   ```
   scripts/k8s_create_platform_secret_from_env.sh   # 重新写 robot-dh/robot-dh-v1-6-secrets
   make argo-sync-log-archive-secret                # 同步到 argo namespace
   kubectl -n argo rollout restart deploy/workflow-controller
   ```

   `argo_apply_log_archive.sh` 已经会自动 rollout restart，所以**只要重跑** `make argo-enable-log-archive` 也能完成全链路 rotate。

## 7. 已知限制 / 后续 issue

- **kubectl logs follow** 只跟踪调用瞬间已存在的 step pod；DAG 后续新拉起的 pod 不会自动接入。这是 kubectl 行为，与本归档无关；建议事后改用 MinIO 上的 `argo-logs/` 对象做长期排障源。
- 本仓库已脱钩 argo 官方 CLI（`memory.mdc` 第 3 节），所以**不会**新增 `argo logs` / `argo cron` 等 fallback。`make argo-platform-tail` 仍是 `kubectl logs -l workflows.argoproj.io/workflow=...`。
- `§8 非本需求范围` 中 3 条仍归属 `robot-data-harness` 主项目（heartbeat 写权限、normalize adapter 缺失、`status='running'` 孤儿记录），**未在本回执中处理**，仍按各自 backlog 单独跟进。
- 历史踩坑：早期版本 `argo_sync_log_archive_secret.sh` 给同步出的 Secret 写了 `metadata.labels.source: <ns>/<name>`，被 K8s apiserver 拒绝（`Invalid value ... a valid label must consist of alphanumeric characters, '-', '_' or '.'`）。已经改用 `metadata.annotations.robot-dh.io/synced-from` 记录来源，label 值不再含 `/`。重跑 `make argo-enable-log-archive` 即可。

## 8. 时间线

| 阶段 | 状态 | 备注 |
|------|------|------|
| 仓库代码 + yaml + 脚本 + 单测 | 已完成 | 本回执提交时已 git track，未提交本地 commit（按用户偏好） |
| 远端 / kind 集群 apply | 待执行 | 需要操作方：sourced 平台 env 后跑 `make argo-enable-log-archive` |
| Workflow 验证（`mc ls -r argo-logs/ \| head` 截图） | 待执行 | 触发任意一个 multisource-scale30 / contract-qc 即可 |
| `robot-dh-infra` 侧 README / lifecycle / runbook 落地 | 待 `robot-dh-infra` | 见 §6 |

收到 §6 的 `robot-dh-infra` 侧改动 PR 后，本仓库这边再追加一条 `docs/v1_6_argo_log_archive_handoff.md` 的 changelog（标记 **「已 apply，闭环」**），与 `memory.mdc` 中其它 v1.6 hand-off 一致。
