你是 Kubernetes / Argo Workflows 平台工程师。当前 robot-data-harness 已完成 v1.7 local-first runtime：

- Windows D 盘 devscale 数据同步
- kind robot-dh-dev cluster
- D 盘通过 extraMounts 挂载到 kind node
- Pod 能通过 /mnt/local-data/robot-dh-local 访问 raw/lake/cache
- robot-dh CLI 支持 local file URI
- adapter 支持 DROID / LeRobot、robomimic、BridgeData
- local runtime doctor / datasets verify 可用

现在重写 Argo v1.7 模板。目标：
1. 默认 workflow 只跑 <=3GB devscale。
2. workflow 第一步必须 verify 本地数据完整。
3. verify 成功后才执行 QC / normalize / features / ADS / ml-ready。
4. 使用本地 PV/PVC 和 input cache。
5. Argo 可视化完整展示三类数据分支。
6. 强化日志、生命周期管理、隔离、重试、超时。
7. scale30 workflow 保留，但必须显式手动提交，不作为默认。

============================================================
一、新增目录
============================================================

新增：

argo/v1_7_local/
  README.md

  templates/
    robot-dh-local-devscale-workflowtemplate.yaml
    robot-dh-local-qc-workflowtemplate.yaml
    robot-dh-local-ml-ready-workflowtemplate.yaml

  workflows/
    submit-local-devscale.yaml
    submit-local-qc.yaml
    submit-local-ml-ready.yaml

  cron/
    local-devscale-cronworkflow.yaml

  scripts/
    submit_local_devscale.sh
    watch_local_workflow.sh
    tail_live_workflow_logs.sh
    sync_workflow_steps.sh

k8s/v1_7_local/
  local-argo-rbac.yaml
  local-argo-configmap.yaml
  local-argo-secret.example.yaml

docs/
  v1_7_argo_local_first.md

============================================================
二、Local WorkflowTemplate
============================================================

主 WorkflowTemplate：

metadata.name:
  robot-dh-local-devscale

参数：
  local_data_root: /mnt/local-data/robot-dh-local
  raw_root: file:///mnt/local-data/robot-dh-local/raw
  lake_root: file:///mnt/local-data/robot-dh-local/lake
  input_cache_dir: /mnt/local-data/robot-dh-local/cache/input-cache
  quality_threshold: "80"
  max_workers: "4"
  heartbeat_interval_sec: "30"
  fail_on_contract_fail: "false"

DAG：

local-runtime-doctor
  -> verify-devscale-data
  -> adapter-probe-droid
  -> adapter-probe-robomimic
  -> adapter-probe-bridge

adapter-probe-droid -> droid-qc -> droid-normalize -> droid-features
adapter-probe-robomimic -> robomimic-qc -> robomimic-normalize -> robomimic-features
adapter-probe-bridge -> bridge-qc -> bridge-normalize -> bridge-features

droid-features && robomimic-features && bridge-features
  -> build-ads
  -> ml-ready-export
  -> benchmark-regression
  -> publish-lineage
  -> argo-sync
  -> archive-logs-index

注意：
  - verify-devscale-data 失败，后续全部不跑。
  - 某个数据 family 缺失时，对应 branch 可以 SKIP，但要在 summary 记录。
  - 默认三个 family 都应该存在。
  - fail_on_contract_fail=false 时 QC FAIL 不阻断后续 ADS，但 final summary 标 WARN。
  - fail_on_contract_fail=true 时 QC FAIL 阻断。

============================================================
三、每个节点命令
============================================================

local-runtime-doctor:
  robot-dh local runtime doctor --log-format json

verify-devscale-data:
  robot-dh local datasets verify --log-format json

adapter-probe-droid:
  robot-dh adapter probe --dataset-uri file:///mnt/local-data/robot-dh-local/raw/droid_lerobot_dev1g/v1 --log-format json

adapter-probe-robomimic:
  robot-dh adapter probe --dataset-uri file:///mnt/local-data/robot-dh-local/raw/robomimic_dev1g/v1 --log-format json

adapter-probe-bridge:
  robot-dh adapter probe --dataset-uri file:///mnt/local-data/robot-dh-local/raw/bridgedata_v2_dev/v1 --log-format json

droid-qc:
  robot-dh qc contract run --dataset-family droid --dataset-uri file:///mnt/local-data/robot-dh-local/raw/droid_lerobot_dev1g/v1 --dataset-id droid_lerobot_dev1g --version v1 --output file:///mnt/local-data/robot-dh-local/lake/qc/droid_lerobot_dev1g/v1 --contract configs/qc/droid_contract.yaml --log-format json

robomimic-qc:
  robot-dh qc contract run --dataset-family robomimic --dataset-uri file:///mnt/local-data/robot-dh-local/raw/robomimic_dev1g/v1 --dataset-id robomimic_dev1g --version v1 --output file:///mnt/local-data/robot-dh-local/lake/qc/robomimic_dev1g/v1 --contract configs/qc/robomimic_contract.yaml --max-workers 4 --file-timeout-sec 300 --log-format json

bridge-qc:
  robot-dh qc contract run --dataset-family bridge --dataset-uri file:///mnt/local-data/robot-dh-local/raw/bridgedata_v2_dev/v1 --dataset-id bridgedata_v2_dev --version v1 --output file:///mnt/local-data/robot-dh-local/lake/qc/bridgedata_v2_dev/v1 --contract configs/qc/bridge_contract.yaml --probe-timeout-sec 120 --max-retries 2 --log-format json

normalize:
  robot-dh etl run --dataset file:///mnt/local-data/robot-dh-local/raw/<dataset>/v1 --dataset-id <dataset> --version v1 --lake-root file:///mnt/local-data/robot-dh-local/lake --phase normalize --resume --heartbeat-interval-sec 30 --log-format json

features:
  robot-dh etl run --dataset file:///mnt/local-data/robot-dh-local/raw/<dataset>/v1 --dataset-id <dataset> --version v1 --lake-root file:///mnt/local-data/robot-dh-local/lake --phase features --resume --heartbeat-interval-sec 30 --log-format json

build-ads:
  robot-dh build-ads --input-root file:///mnt/local-data/robot-dh-local/lake/dwd --output file:///mnt/local-data/robot-dh-local/lake/ads/quality --log-format json

ml-ready-export:
  robot-dh ml-ready export --input-root file:///mnt/local-data/robot-dh-local/lake/dwd --quality-root file:///mnt/local-data/robot-dh-local/lake/ads/quality --qc-root file:///mnt/local-data/robot-dh-local/lake/qc --output file:///mnt/local-data/robot-dh-local/lake/ml-ready/devscale/v1 --quality-threshold {{workflow.parameters.quality_threshold}} --split 0.8,0.1,0.1 --log-format json

benchmark-regression:
  robot-dh benchmark run --suite configs/benchmark_suite.yaml --output file:///mnt/local-data/robot-dh-local/lake/benchmark/{{workflow.name}} --log-format json

publish-lineage:
  robot-dh lineage report --workflow-name {{workflow.name}} --output file:///mnt/local-data/robot-dh-local/lake/lineage/reports/{{workflow.name}}.json --log-format json

argo-sync:
  robot-dh argo sync --workflow-name {{workflow.name}} --namespace robot-dh --log-format json

archive-logs-index:
  robot-dh argo logs index --workflow-name {{workflow.name}} --namespace robot-dh --archive-root file:///mnt/local-data/robot-dh-local/lake/argo-logs --log-format json

============================================================
四、Volume / cache
============================================================

所有 workflow pods 必须挂载：

volume:
  persistentVolumeClaim:
    claimName: robot-dh-local-data-pvc

mountPath:
  /mnt/local-data/robot-dh-local

环境变量：
  ROBOT_DH_LOCAL_DATA_ROOT=/mnt/local-data/robot-dh-local
  ROBOT_DH_DEV_DATA_ROOT=file:///mnt/local-data/robot-dh-local/raw
  ROBOT_DH_DEV_LAKE_ROOT=file:///mnt/local-data/robot-dh-local/lake
  ROBOT_DH_INPUT_CACHE_DIR=/mnt/local-data/robot-dh-local/cache/input-cache
  PYTHONUNBUFFERED=1

不要使用 emptyDir 作为 input cache。
可以为 /tmp 使用 emptyDir，但大文件缓存必须走 PVC。

============================================================
五、安全与资源
============================================================

securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  allowPrivilegeEscalation: false
  capabilities:
    drop: ["ALL"]

由于需要写 /tmp / cache，readOnlyRootFilesystem 暂设 false。
README 解释原因。

resources：
  doctor/probe/qc:
    requests cpu 250m memory 512Mi
    limits cpu 1 memory 2Gi

  normalize/features:
    requests cpu 500m memory 1Gi
    limits cpu 2 memory 4Gi

  ads/ml-ready/benchmark:
    requests cpu 500m memory 1Gi
    limits cpu 2 memory 4Gi

activeDeadlineSeconds：
  doctor/probe/qc: 900
  normalize/features: 3600
  whole workflow: 7200

retryStrategy：
  qc/probe:
    limit 1
  normalize/features:
    limit 1
  benchmark:
    limit 0

ttlStrategy:
  secondsAfterCompletion: 86400

============================================================
六、真·follow log watcher
============================================================

新增脚本：

argo/v1_7_local/scripts/tail_live_workflow_logs.sh

功能：
1. 参数 workflow name。
2. 每 3 秒查询：
   argo get 或 kubectl get workflow -o json。
3. 发现新 pod 后立即 kubectl logs -f。
4. 已 attach 的 pod 不重复 attach。
5. pod 失败时自动：
   kubectl describe pod
   kubectl logs --previous if exists
6. workflow 结束后退出。
7. 输出每个 pod 的 archive log path。

Makefile target：
  make argo-local-tail

============================================================
七、Makefile
============================================================

新增：

make argo-local-apply
make argo-local-submit
make argo-local-tail
make argo-local-status
make argo-local-logs
make argo-local-sync
make argo-local-debug
make argo-local-clean-completed
make v1-7-local-platform-smoke

v1-7-local-platform-smoke：
  - local runtime doctor
  - datasets verify
  - docker image check
  - kind context check
  - pvc check
  - argo template check
  - 提示用户提交 workflow

============================================================
八、文档
============================================================

docs/v1_7_argo_local_first.md 必须包含：

1. 为什么 v1.7 默认跑 devscale。
2. 为什么先同步到 D 盘。
3. 为什么 Argo 第一步必须 verify data。
4. DAG 图。
5. 如何查看 Argo UI。
6. 如何 live tail。
7. 如何看 archive logs。
8. 如何区分 devscale workflow 和 scale30 workflow。
9. 常见故障：
   - D 盘没挂进 kind
   - PVC 为空
   - Pod Permission denied
   - benchmark 失败
   - droid normalize 缺 meta
   - robomimic HDF5 结构不一致
   - bridge parquet 读失败
   - C 盘空间上涨

============================================================
九、保留 scale30 手动模板
============================================================

保留现有 scale30 workflow，但：
1. 命名必须带 scale30。
2. README 标注只用于手动压测。
3. Makefile target 不能叫默认。
4. 默认 argo-local-submit 只提交 devscale workflow。

============================================================
十、验收命令
============================================================

用户手动执行：

kubectl config use-context kind-robot-dh-dev

make docker-build
make kind-load

make local-apply-data-pvc
make argo-local-apply
make v1-7-local-platform-smoke

make argo-local-submit
make argo-local-tail

验收标准：
  - Argo UI 能看到 local-runtime-doctor -> verify-devscale-data -> 三分支 DAG。
  - 三类 adapter probe 成功。
  - 三类 QC contract 至少能产出 contract_report。
  - DROID normalize 使用 local direct input。
  - robomimic QC 本地直接读 HDF5。
  - Bridge QC 不访问远端 S3。
  - 输出写到 /mnt/local-data/robot-dh-local/lake。
  - archive log index 可用。
  - workflow_steps 能记录 pod / exit_code / log uri。
  - 默认 workflow 不读取远端 scale30 raw。

请开始实现。不要提交真实 Secret。不要把默认 workflow 指向 scale30。不要引入 Kafka。不要做 Go Operator。不要留 TODO。