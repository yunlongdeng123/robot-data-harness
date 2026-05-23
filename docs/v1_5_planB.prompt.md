你是 Kubernetes 平台工程师、Argo Workflows 工程师、数据流水线工程师。当前仓库 robot-data-harness 已完成 v1.5 Python 核心能力：

- robot-dh etl plan
- robot-dh etl run-shard
- robot-dh etl merge-summary
- robot-dh mutate
- robot-dh benchmark run
- robot-dh benchmark report
- performance profiler
- runtime events
- PostgreSQL v1.5 表写入
- MinIO robot-lake / robot-datasets 对接

现在需要接入 Argo Workflows DAG。不要引入 Go Operator，不要引入复杂前端。目标是让本地 kind 集群能以 Argo Workflow 方式调度 30GB scale 数据的 ETL / benchmark / ADS 流水线。

============================================================
一、目标
============================================================

新增 Argo Workflow 支持：

1. 安装 / 检查 Argo Workflows。
2. 创建 robot-dh namespace 下的 Argo RBAC、ServiceAccount、Secret 模板。
3. 提供 WorkflowTemplate：
   - scale ETL DAG
   - benchmark DAG
   - ADS build DAG
4. 提供 CronWorkflow：
   - 周期性 scale ETL scan / plan
5. 提供 Makefile target：
   - argo-install
   - argo-status
   - argo-submit-scale-etl
   - argo-submit-benchmark
   - argo-submit-build-ads
   - argo-logs
   - argo-ui-port-forward
6. 所有 Workflow 使用现有 robot-data-harness Docker image。
7. 不提交真实 secret。
8. 不硬编码真实服务器地址和密码。

============================================================
二、新增目录结构
============================================================

新增：

argo/
  README.md

  install/
    argo-install.yaml 或 install_argo.sh
    namespace.yaml
    rbac.yaml

  templates/
    robot-dh-scale-etl-workflowtemplate.yaml
    robot-dh-benchmark-workflowtemplate.yaml
    robot-dh-build-ads-workflowtemplate.yaml

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

k8s/v1_5_argo/
  serviceaccount.yaml
  role.yaml
  rolebinding.yaml
  secret.example.yaml
  configmap.yaml

docs/
  v1_5_argo_workflow.md

============================================================
三、Argo 安装策略
============================================================

提供 argo/scripts/argo_install.sh。

要求：
1. 不默认自动执行。
2. 用户手动执行 make argo-install。
3. 支持安装到 namespace argo。
4. 安装完成后检查：
   kubectl get pods -n argo
5. 不要求暴露公网。
6. 提供 argo-ui-port-forward：
   kubectl -n argo port-forward svc/argo-server 2746:2746

如果当前集群已有 Argo，脚本应检测并跳过安装或提示。

README 说明：
  kind 本地 Argo 仅用于开发和演示。
  生产环境应由集群管理员管理 Argo 安装。

============================================================
四、Secret / ConfigMap
============================================================

k8s/v1_5_argo/secret.example.yaml：

namespace: robot-dh
name: robot-dh-v1-5-secrets

字段：
  ROBOT_DH_DB_URI
  ROBOT_DH_ARTIFACT_STORE
  ROBOT_DH_S3_ENDPOINT_URL
  ROBOT_DH_S3_ACCESS_KEY
  ROBOT_DH_S3_SECRET_KEY
  ROBOT_DH_S3_DATA_BUCKET
  ROBOT_DH_S3_ARTIFACT_BUCKET
  ROBOT_DH_S3_LAKE_BUCKET
  ROBOT_DH_REDIS_URL

k8s/v1_5_argo/configmap.yaml：

字段：
  SCALE_ROOT=s3://robot-datasets/raw
  LAKE_ROOT=s3://robot-lake
  SCALE_INCLUDE=*scale30*
  TARGET_SHARD_SIZE_GB=5
  MAX_WORKERS=2
  BENCHMARK_SUITE=configs/benchmark_suite.yaml

注意：
  kind Pod 不能使用 WSL 的 127.0.0.1 SSH tunnel。
  K8s Secret 里必须使用云服务器公网 IP / DNS 或 Pod 可达地址。
  README 要强调这一点。

============================================================
五、ServiceAccount / RBAC
============================================================

ServiceAccount：
  robot-dh-workflow

Role 权限：
  - get/list/watch pods
  - get/list/watch jobs
  - create/get/list/watch workflows.argoproj.io
  - patch/update workflows.argoproj.io/status 如果需要
  - get configmaps/secrets 仅限必要

RoleBinding：
  绑定 robot-dh-workflow。

Workflow pod 使用：
  serviceAccountName: robot-dh-workflow

============================================================
六、WorkflowTemplate 1：scale ETL DAG
============================================================

文件：
  argo/templates/robot-dh-scale-etl-workflowtemplate.yaml

名称：
  robot-dh-scale-etl

入口：
  main

参数：
  root_uri default s3://robot-datasets/raw
  lake_root default s3://robot-lake
  include_pattern default *scale30*
  target_shard_size_gb default 5
  max_workers default 2
  plan_output default /tmp/scale30_plan.json
  summary_output default /tmp/scale30_summary.json

DAG：

  plan
    -> run-shard-0
    -> run-shard-1
    -> run-shard-2
    -> merge-summary
    -> build-ads
    -> publish-event

由于 Argo 动态 fan-out 实现复杂，v1.5 可以先固定 3 个 shard：
  shard_id 0
  shard_id 1
  shard_id 2

但 plan 可能生成少于 3 个 shard，run-shard 命令必须能在 shard 不存在时优雅 SKIP。

每个 step 使用镜像：
  robot-data-harness:local

imagePullPolicy:
  IfNotPresent

envFrom:
  secretRef robot-dh-v1-5-secrets
  configMapRef robot-dh-v1-5-config

资源：
  plan:
    cpu 500m / memory 1Gi
  run-shard:
    cpu 2 / memory 4Gi
  build-ads:
    cpu 1 / memory 2Gi

安全上下文：
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: false
  capabilities drop ALL

activeDeadlineSeconds:
  43200

ttlStrategy:
  secondsAfterCompletion: 86400

命令：
  plan:
    robot-dh etl plan --root {{inputs.parameters.root_uri}} --lake-root {{inputs.parameters.lake_root}} --include "{{inputs.parameters.include_pattern}}" --target-shard-size-gb {{inputs.parameters.target_shard_size_gb}} --output /workspace/scale30_plan.json --log-format json

  run-shard-i:
    robot-dh etl run-shard --plan /workspace/scale30_plan.json --shard-id i --lake-root {{inputs.parameters.lake_root}} --output /workspace/shards/shard_i --max-workers {{inputs.parameters.max_workers}} --log-format json

注意：
  plan 文件需要跨 step 传递。
  可以使用 Argo artifacts 或 emptyDir / artifact repository。
  本地 kind 第一版可使用 workflow artifact passing 到 MinIO，如果配置复杂，也可以让 plan 同时上传到 s3://robot-lake/tmp/{workflow.name}/scale30_plan.json，然后 run-shard 从该 URI 读取。
  推荐实现 S3 plan URI 方案，减少 Argo artifact repository 配置复杂度。

因此请让 Workflow 使用：
  PLAN_URI=s3://robot-lake/tmp/{{workflow.name}}/scale30_plan.json
  SHARD_RESULT_PREFIX=s3://robot-lake/tmp/{{workflow.name}}/shards
  SUMMARY_URI=s3://robot-lake/tmp/{{workflow.name}}/scale30_summary.json

如果 robot-dh etl plan / run-shard 暂不支持 S3 plan URI，请补充支持。

============================================================
七、WorkflowTemplate 2：benchmark DAG
============================================================

文件：
  argo/templates/robot-dh-benchmark-workflowtemplate.yaml

名称：
  robot-dh-benchmark

DAG：
  prepare-demo
    -> run-benchmark
    -> publish-benchmark-report

命令：
  prepare-demo:
    robot-dh generate-demo --output /workspace/samples/button_press_001

  run-benchmark:
    robot-dh benchmark run --suite configs/benchmark_suite.yaml --output /workspace/runs/benchmark/v1_5 --record-to-registry --log-format json

  publish:
    可调用 robot-dh benchmark report，或者把 artifacts 上传到 MinIO。

要求：
  benchmark 失败时 workflow 应该失败。

============================================================
八、WorkflowTemplate 3：build ADS
============================================================

文件：
  argo/templates/robot-dh-build-ads-workflowtemplate.yaml

名称：
  robot-dh-build-ads

命令：
  robot-dh build-ads --input-root s3://robot-lake/dwd --output s3://robot-lake/ads/quality --log-format json

============================================================
九、CronWorkflow
============================================================

文件：
  argo/cron/scale-etl-cronworkflow.yaml

名称：
  robot-dh-scale-etl-cron

schedule:
  "0 */12 * * *"

concurrencyPolicy:
  Forbid

调用 WorkflowTemplate:
  robot-dh-scale-etl

参数：
  root_uri=s3://robot-datasets/raw
  lake_root=s3://robot-lake
  include_pattern=*scale30*
  target_shard_size_gb=5

不要默认开启自动 apply。用户手动执行 make argo-apply-cron。

============================================================
十、Makefile
============================================================

新增 target：

make argo-install
make argo-status
make argo-ui-port-forward
make argo-apply-rbac
make argo-apply-templates
make argo-submit-scale-etl
make argo-submit-benchmark
make argo-submit-build-ads
make argo-apply-cron
make argo-list
make argo-logs
make argo-delete-completed

要求：
  - 不自动创建真实 secret。
  - argo-submit 前检查 kubectl get secret robot-dh-v1-5-secrets -n robot-dh。
  - 如果缺少 secret，给清晰提示。
  - argo-submit 前检查 image 是否已加载到 kind：
      docker exec robot-dh-control-plane crictl images | grep robot-data-harness
    如果检查失败，提示 make docker-build && make kind-load。

============================================================
十一、K8s Job 安全增强
============================================================

同时更新现有 k8s/v1_4_lake 或 v1_5 相关 Job：

增加：
  securityContext:
    runAsNonRoot: true
    allowPrivilegeEscalation: false
    capabilities:
      drop:
      - ALL

增加：
  activeDeadlineSeconds
  ttlSecondsAfterFinished

说明：
  如果 readOnlyRootFilesystem=true 导致 pyarrow/temp 写失败，则暂时设 false，并通过 --tmp-dir 指向 /tmp 或 emptyDir。
  README 解释原因。

============================================================
十二、Runtime events
============================================================

Workflow 每个关键步骤应通过 robot-dh runtime event 或现有 CLI 自动记录：
  argo_workflow_submitted
  argo_step_started
  argo_step_finished
  argo_workflow_finished

如果当前 CLI 不支持 step-level event，可以至少在每个 step 开头/结尾 echo JSON log。

============================================================
十三、README / docs
============================================================

argo/README.md 和 docs/v1_5_argo_workflow.md 必须包含：

1. 为什么引入 Argo：
   - DAG
   - step-level retry
   - parallel shard execution
   - workflow visibility

2. 本地 kind 前置：
   - docker image 已 kind load
   - robot-dh-v1-5-secrets 已创建
   - 云服务 endpoint Pod 可达

3. 执行顺序：
   source client/robot-dh-v1-5.env
   ./scripts/k8s_create_v1_5_secret_from_env.sh
   make docker-build
   make kind-load
   make argo-install
   make argo-apply-rbac
   make argo-apply-templates
   make argo-submit-scale-etl
   make argo-logs

4. 常见故障：
   - Argo pod pending
   - image pull failed
   - secret missing
   - S3 access denied
   - Postgres auth failed
   - plan URI 读不到
   - run-shard OOMKilled
   - workflow artifact 传递失败
   - Pod 里使用 127.0.0.1 endpoint 失败

============================================================
十四、验收命令
============================================================

用户手动执行：

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

验收：
  - benchmark workflow 能成功或按预期失败并记录 benchmark_cases
  - scale ETL workflow 能生成 plan
  - 至少一个 shard 能成功处理 scale30 数据
  - build ADS 成功
  - PostgreSQL etl_perf_runs / etl_shards / argo_workflow_runs 有记录
  - MinIO robot-lake/tmp/{workflow} 有 plan / shard summary
  - robot-lake/ads/quality 有 ADS parquet

请开始实现。不要引入 Go Operator，不要提交真实 Secret，不要硬编码服务器地址和密码。不要留 TODO，不要写伪代码。