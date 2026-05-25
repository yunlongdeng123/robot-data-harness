## robot-data-harness v1.6

`robot-data-harness` 是一个面向机械臂末端位姿 `eexyzxyzw` 数据集的 Kubernetes-native 数据质量与评测 Harness。它覆盖数据集注册、轨迹校验、质量门禁、报告生成、运行历史沉淀，以及在 WSL / kind / Kubernetes Job 中对远端 PostgreSQL、MinIO、Redis 的统一接入。

**v1.6 在 v1.5 Sharded ETL + Benchmark 的基础上扩展为多源机器人数据平台**：

- **heartbeat / checkpoint / partition / resumable normalize**（v1.6.1）：`robot-dh partition plan / run-normalize`、`etl run --phase normalize|features|ads --resume`，把 v1.5 scale-ETL 的 DeadlineExceeded 故障转成可观测、可断点续跑
- **Multi-source QC Contract Layer**（v1.6.2）：`robot-dh qc contract run / profile / report`，对 DROID / LeRobot、robomimic、BridgeData V2 三类数据集执行 dataset-specific schema/temporal 规则
- **ML-ready Dataset Export + FastAPI 控制面**（v1.6.3）：`robot-dh ml-ready export / list / show` 输出 train/val/test parquet + dataset_card.json/md + lineage.json，并扩 `/qc/*` `/assets/profiles` `/ml-ready` `/workflows` 等只读 endpoint
- **Argo 多源 DAG + workflow metadata sync**（v1.6.4）：`robot-dh-multisource-scale30` / `robot-dh-contract-qc` / `robot-dh-ml-ready-export` 三个 WorkflowTemplate + CronWorkflow，并提供 `robot-dh argo sync` / `lineage report` 把 workflow status 写回 PG
- **robot-dh-exporter 平台层 metrics**（v1.6.5）：保留所有 v1.5 指标，新增 14 个指标覆盖 `qc_contracts` / `qc_contract_runs` / `workflow_runs` / `workflow_steps` / `asset_profiles` / `ml_ready_datasets` / `dataset_partitions` / `task_heartbeats` / `openlineage_events`，`/healthz` 返回 `db_connected` / `last_scrape_time` / `last_scrape_error`
- **多源 scale30 实跑回归修复**（v1.6.6 ~ v1.6.8）：基于 `robot-dh-multisource-scale30-{fhkvr,qptk9,ddbfb,fvx5z,dls4z}` 五次实跑的 9 类失败，沉淀为 QC probe 容错链（`__cause__` / `__context__` / `traceback` 三段 fallback、fast vs default boto3 client 分档）、droid lerobot v2 / bridgedata_v2 专属 adapter、normalize resume input cache、`S3LakeStore.download_dir` 进度心跳（每 N 文件 + 每 N 秒双触发）、Argo `archiveLogs` + `podGC=OnWorkflowCompletion` 闭环、`etl-phase` / `qc-contract-run` ephemeral-storage + `python -u | tee` 模板

完全向后兼容 v1.5 Argo `robot-dh-scale-etl` / v1.4 数据湖 / v1.3 validate / scan / gate / registry / S3 artifact 行为。

当前仓库的运行口径有五条主线：

- 默认兼容模式：本地 SQLite + 本地 artifact + kind PVC demo（与 v1.3 完全一致）
- 远端直连模式：公网白名单直连 PostgreSQL / MinIO / Redis（推荐生产路径）
- v1.4 数据湖 ETL：`normalize → build-features → build-ads` 三段流水，落 `lake_assets` / `etl_jobs` / `lineage_edges` / `dataset_versions` / `quality_snapshots`
- v1.5 Argo 编排：Sharded ETL / Benchmark / build-ADS 由 Argo Workflows 调度；写入 `etl_perf_runs` / `etl_shards` / `benchmark_runs` / `benchmark_cases` / `runtime_events`
- **v1.6 平台层**：多源 QC contract + 可恢复 normalize + ML-ready export 由 `robot-dh-multisource-scale30` DAG 编排；写入 `qc_contracts` / `qc_contract_runs` / `workflow_runs` / `workflow_steps` / `asset_profiles` / `ml_ready_datasets` / `dataset_partitions` / `task_heartbeats` / `openlineage_events`

本仓库已经完成并验证以下链路：

- 本地 `make test`（v1.3 完整 + v1.4 lake + v1.5 sharded ETL / benchmark / profiler / runtime events + v1.6 heartbeat / checkpoint / partition / QC contract / ML-ready / argo sync / lineage report / API 只读端点；无远端服务时可选测试跳过）
- WSL 公网白名单直连：`infra doctor`（含 lake）、`lake audit`、`lake list`、`etl run --phase ...`、`etl scan`、`etl plan / run-shard / merge-summary`、`benchmark run / report`、`qc contract run`、`ml-ready export`
- kind / K8s remote Secret 模式下的 validator job、scan job、API `/health`、`/infra/health`、`/qc/*`、`/ml-ready/*`、`/workflows/*`
- kind 上的 Argo Workflows：v1.5 `robot-dh-scale-etl` / `robot-dh-benchmark` / `robot-dh-build-ads` + **v1.6 `robot-dh-multisource-scale30` / `robot-dh-contract-qc` / `robot-dh-ml-ready-export` + `robot-dh-multisource-scale30-cron`**
- 独立 Go exporter `robot-dh-exporter`（`/metrics` + 增强 `/healthz` 含 `db_connected`/`last_scrape_time`/`last_scrape_error`）

包版本当前为 `0.1.6`。

> 端到端可跑命令清单：见末尾 [v1.6 端到端命令清单](#v16-端到端命令清单)。

## 目录

- [核心能力](#核心能力)
- [运行模式](#运行模式)
- [仓库结构](#仓库结构)
- [环境要求](#环境要求)
- [安装](#安装)
- [输出产物](#输出产物)
- [核心配置](#核心配置)
- [快速开始](#快速开始)
- [WSL 公网白名单直连](#wsl-公网白名单直连)
- [kind / Kubernetes](#kind--kubernetes)
- [正式镜像重建](#正式镜像重建)
- [CLI 参考](#cli-参考)
- [API 参考](#api-参考)
- [Makefile 常用目标](#makefile-常用目标)
- [测试与验收](#测试与验收)
- [故障排查](#故障排查)
- [安全与提交说明](#安全与提交说明)
- [v1.4 数据湖](#v14-数据湖)
- [v1.4 K8s ETL 调度](#v14-k8s-etl-调度)
- [v1.5 Scale Benchmark + Sharded ETL + Runtime Profiling](#v15-scale-benchmark--sharded-etl--runtime-profiling)
- [v1.6.1 — heartbeat / checkpoint / partition / resumable normalize](#v161--heartbeat--checkpoint--partition--resumable-normalize)
- [v1.6.2 — Multi-source QC Contract Layer](#v162--multi-source-qc-contract-layer)
- [v1.6.3 — ML-ready export + FastAPI 控制面](#v163--ml-ready-export--fastapi-控制面)
- [v1.6.4 — Argo multi-source DAG + workflow metadata sync](#v164--argo-multi-source-dag--workflow-metadata-sync)
- [v1.6.5 — robot-dh-exporter 平台层 metrics](#v165--robot-dh-exporter-v16-metrics)
- [v1.6.6 ~ v1.6.8 — 多源 scale30 实跑回归修复](#v166--v168--多源-scale30-实跑回归修复)
- [v1.6 端到端命令清单](#v16-端到端命令清单)

## 核心能力

- validator pipeline：轨迹连续性、按压事件、按钮聚类结构、质量门禁
- 注册表后端：同时支持 `sqlite:///` 与 `postgresql+psycopg://`
- 产物后端：同时支持本地文件系统与 S3/MinIO
- infra doctor：检查 DB、S3、Redis 连通性与基础配置
- FastAPI：暴露健康检查、数据集列表、运行历史、同步 validate 接口
- 面向 Kubernetes：支持 Deployment、Job、CronJob，通过可选 Secret 注入远端配置
- 向后兼容：不重写既有本地 SQLite、本地 artifact、kind PVC demo 流程

## 运行模式

### 模式 1：本地默认模式

```text
数据集目录
  -> robot-dh validate / scan
  -> validators + gate policy
  -> report.json / report.html / gate_report.json / plots
  -> SQLite registry (.robot_dh/robot_dh.db)
  -> LocalArtifactStore (file://...)
```

### 模式 2：WSL 公网白名单直连

```text
WSL 终端
  -> robot-dh CLI / uvicorn / infra doctor
  -> PostgreSQL DSN -> 云端 PostgreSQL
  -> S3 endpoint    -> 云端 MinIO
  -> Redis URL      -> 云端 Redis
```

### 模式 3：kind / K8s remote Secret 模式

```text
kind 集群 / K8s Pod
  -> validator Job / scan CronJob / API Deployment
  -> envFrom Secret（可选）
  -> 经公网白名单访问 PostgreSQL + MinIO + Redis
```

### 远端模式的当前建议

- WSL 和 kind 都直接连接公网 IP 或 DNS
- `client/wsl-export-public-env.sh` 是推荐入口
- `k8s/secret.yaml` 中必须写公网地址，不应再写 `127.0.0.1`

## 仓库结构

```text
robot-data-harness/
  argo/
    cron/                          # v1.5 scale-etl + v1.6 multisource cron
    scripts/
    templates/                     # robot-dh-multisource-scale30 等 WorkflowTemplate
    workflows/                     # 提交用 submit-*.yaml
  client/
    robot-dh-lake.env.example      # v1.4 lake env 模板（历史）
    robot-dh-platform.env.example  # v1.6 平台层 env 模板（pg/minio/redis + qc/ml-ready 前缀）
    robot-dh-platform.env          # 真实凭据（chmod 600，**禁止入 git**）
    k8s-lake-secret.example.yaml   # v1.4 lake secret 模板
    k8s-platform-secret.example.yaml          # v1.6 平台层 secret 模板
    k8s-create-platform-secret.example.sh     # 推荐：source env 后跑这个写 Secret
    wsl-*.sh                       # 通用 WSL 工具
  configs/
    benchmark_suite.yaml
    button_press.yaml
    datasets.yaml
    default.yaml
    etl_default.yaml
    gate_policy.yaml
    lake.yaml
    qc/                            # universal / droid / robomimic / bridge contract
    runtime_default.yaml
    runtime_platform.yaml          # v1.6 normalize/etl/partition 默认参数
    scale30_etl.yaml
  docker/
    Dockerfile
  docs/
    lake_layout.md                 # v1.4 数据湖布局
    robot_platform_runbook.md      # 平台 PG / secret / 运维流程
    robot_platform_metadata_schema.md
    robot_platform_argo_multisource_workflow.md
    robot_platform_storage_and_deadline_notes.md
    robot_platform_dependencies_and_skipped.md
    v1_5_argo_workflow.md          # v1.5 Argo 历史交付
    v1_4_*.md                      # v1.4 lake 交接（历史）
    v1_6_plan*.prmpt.md            # 本轮 v1.6 plan prompt 历史
  eexyzxyzw/
  go/robot-dh-exporter/            # Go Prometheus exporter（v1.5 + v1.6 metrics）
  k8s/
    api-deployment.yaml
    api-service.yaml
    configmap.yaml
    debug-pod.yaml
    etl-job.yaml
    namespace.yaml
    pvc.yaml
    scan-cronjob.yaml
    secret.example.yaml
    validator-job.yaml
    v1_4_lake/                     # v1.4 lake K8s 资源（历史，已闭环）
    v1_5_argo/                     # v1.5 Argo RBAC（历史，已闭环）
  postgres/
    migrations/
      001_lake_metadata.reconstructed.sql
      005_robot_platform.sql       # v1.6 9 张平台表（任由 infra 项目 apply）
  samples/
  scripts/
    argo_submit_multisource_scale30.sh
    argo_sync_latest.sh
    argo_watch_multisource.sh
    copy_artifacts_from_pvc.sh
    copy_dataset_to_pvc.sh
    k8s_create_lake_secret_from_env.sh
    k8s_create_v1_5_secret_from_env.sh
    k8s_create_platform_secret_from_env.sh   # v1.6 platform secret 创建
    generate_demo_dataset.py
    wait_job.sh
  src/robot_dh/
    api/
    argo/                          # v1.6 argo sync
    artifacts/
    benchmark/
    data/
    etl/                           # normalize / features / ads / runner / lineage
    gate/
    infra/
    lake/
    lineage/                       # v1.6 lineage report
    ml_ready/                      # v1.6 ML-ready export
    partition/                     # v1.6 partition planner
    perf/
    progress/                      # v1.6 heartbeat / checkpoint / progress logger
    qc/                            # v1.6 QC contract layer
    registry/
    reports/
    runtime/
    sharding/
    validators/
    warehouse/                     # models / service / robot_platform
    cli.py
    pipeline.py
    scan.py
  tests/
  kind-robot-dh.yaml
  Makefile
  pyproject.toml
  README.md
```

## 环境要求

- Python `>=3.10`
- Docker
- kind
- kubectl
- 可选云端依赖：PostgreSQL、MinIO 或兼容 S3 的对象存储、Redis
- 如果启用公网直连，远端 PostgreSQL / MinIO / Redis 必须已对白名单 CIDR 放行

## 安装

推荐直接执行：

```bash
make setup
```

等价命令：

```bash
python -m pip install --upgrade pip
python -m pip install --index-url https://download.pytorch.org/whl/cpu torch
python -m pip install -e .[dev]
```

核心运行依赖包括：

- `sqlalchemy>=2.0,<3`
- `psycopg[binary]>=3.2,<4`
- `boto3>=1.34,<2`
- `redis>=5,<7`
- `fastapi>=0.115,<1`

## 输出产物

一次典型 `validate` 会在输出目录生成：

```text
runs/<run_id>/
  gate_report.json
  report.html
  report.json
  plots/
```

如果启用了 `--record-to-registry`，运行结果还会写入 registry：

- `datasets`
- `runs`
- `validator_results`
- `gate_results`
- `artifacts`
- `scan_jobs`

如果启用了 `--artifact-store s3`，本地生成的报告和图片会继续上传到 MinIO / S3，例如：

```text
s3://robot-dh-artifacts/runs/public-demo-v13/report.json
s3://robot-dh-artifacts/runs/public-demo-v13/report.html
s3://robot-dh-artifacts/runs/public-demo-v13/plots/z_press_events.png
```

## 核心配置

### 默认配置

- 默认 DB URI：`sqlite:///.robot_dh/robot_dh.db`
- 默认 artifact store：`local`
- 默认 demo 数据集路径：`samples/button_press_001`
- 默认 quality gate 配置：`configs/gate_policy.yaml`

### 常用环境变量

| 变量 | 用途 | 默认值或示例 |
| --- | --- | --- |
| `ROBOT_DH_DB_URI` | registry 数据库连接串 | `sqlite:///.robot_dh/robot_dh.db` |
| `ROBOT_DH_ARTIFACT_STORE` | artifact 后端类型 | `local` 或 `s3` |
| `ROBOT_DH_S3_ENDPOINT_URL` | S3/MinIO endpoint | `http://PUBLIC_SERVER_IP_OR_DNS:9000` |
| `ROBOT_DH_S3_ACCESS_KEY` | S3 access key | `CHANGE_ME` |
| `ROBOT_DH_S3_SECRET_KEY` | S3 secret key | `CHANGE_ME` |
| `ROBOT_DH_S3_ARTIFACT_BUCKET` | artifact bucket | `robot-dh-artifacts` |
| `ROBOT_DH_S3_DATA_BUCKET` | data bucket | `robot-datasets` |
| `ROBOT_DH_S3_REGION` | S3 region | `us-east-1` |
| `ROBOT_DH_REDIS_URL` | Redis URL | `redis://:CHANGE_ME@redis.example.com:6379/0` |

K8s remote Secret 模式支持的键与上表一致，示例见 `k8s/secret.example.yaml`。

## 快速开始

### 1. 生成 demo 数据

```bash
make demo-data
```

等价命令：

```bash
PYTHONPATH=src python -m robot_dh.cli generate-demo \
  --output samples/button_press_001 \
  --duration-sec 46 \
  --fps 30 \
  --num-buttons 5 \
  --num-presses 25
```

### 2. 本地 validate

```bash
make demo-local
```

等价命令：

```bash
PYTHONPATH=src python -m robot_dh.cli validate \
  --dataset samples/button_press_001 \
  --config configs/button_press.yaml \
  --output runs/button_press_001 \
  --run-id local-demo \
  --record-to-registry \
  --gate-policy configs/gate_policy.yaml
```

### 3. 本地 scan

```bash
make scan-local
```

### 4. 本地测试

```bash
make test
```

### 5. 本地 API

```bash
PYTHONPATH=src uvicorn robot_dh.api.main:app --host 0.0.0.0 --port 8000
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/infra/health
```

## WSL 公网白名单直连

这是当前推荐的本地 CLI、API、本地调试和人工验收模式。

### 1. 准备环境变量

```bash
source client/wsl-export-public-env.sh
```

### 2. 运行远端诊断

```bash
./client/wsl-remote-doctor.sh
PYTHONPATH=src python -m robot_dh.cli infra doctor --output json
```

期望结果：

- `db`：PASS
- `s3`：PASS
- `redis`：PASS

### 3. 注册数据集

```bash
PYTHONPATH=src python -m robot_dh.cli dataset register \
  --dataset samples/button_press_001 \
  --dataset-id button_press_001 \
  --version v1 \
  --storage-uri file://samples/button_press_001
```

### 4. 执行远端 validate

```bash
PYTHONPATH=src python -m robot_dh.cli validate \
  --dataset samples/button_press_001 \
  --config configs/button_press.yaml \
  --output runs/button_press_001_public \
  --run-id public-demo-v13 \
  --record-to-registry \
  --gate-policy configs/gate_policy.yaml \
  --artifact-store s3 \
  --artifact-prefix runs/{run_id}
```

### 5. 执行远端 scan

```bash
PYTHONPATH=src python -m robot_dh.cli scan \
  --root samples \
  --config configs/button_press.yaml \
  --output-root runs/scan_public \
  --registry \
  --artifact-store s3 \
  --artifact-prefix runs/{run_id}
```

### 6. 查看运行历史

```bash
PYTHONPATH=src python -m robot_dh.cli runs list
PYTHONPATH=src python -m robot_dh.cli runs show --run-id public-demo-v13
```

## kind / Kubernetes

### 1. 创建 kind 集群

仓库已提供 `kind-robot-dh.yaml`：

```bash
kind create cluster --name robot-dh --config kind-robot-dh.yaml
```

### 2. 本地 PVC demo 流程

```bash
make docker-build
make kind-load
make k8s-apply
make k8s-copy-data
make k8s-run-job
./scripts/wait_job.sh
make k8s-logs
make k8s-copy-artifacts
```

这一套流程不依赖远端云服务，用于验证本地 SQLite + 本地 artifact + PVC。

### 3. 远端 Secret 模式

从模板生成真实 Secret：

```bash
cp k8s/secret.example.yaml k8s/secret.yaml
# 编辑 PostgreSQL / MinIO / Redis 的公网地址与凭证
```

应用 manifests：

```bash
make k8s-apply
make k8s-apply-cronjob
make k8s-apply-remote-secret
```

当前 manifests 的特点：

- `envFrom.secretRef.optional: true`
- 没有 Secret 时，仍可回退到本地 SQLite / 本地 artifact 模式
- 有 Secret 时，`validator-job.yaml`、`scan-cronjob.yaml`、`api-deployment.yaml` 会切到远端 PostgreSQL / MinIO / Redis

### 4. 远端 validator job

```bash
make kind-load
make k8s-run-job-remote
kubectl -n robot-dh wait --for=condition=complete job/robot-dh-validator --timeout=240s
make k8s-logs
```

### 5. 远端 scan job

```bash
make k8s-run-scan-remote
kubectl -n robot-dh wait --for=condition=complete job/robot-dh-scan-manual --timeout=240s
make k8s-scan-logs
```

如果目标数据集已经存在成功 run，scan job 可能显示 `skipped: 1`。这在 `--only-new` 语义下是正常通过，不是失败。

### 6. API 健康检查

```bash
kubectl -n robot-dh rollout restart deployment/robot-dh-api
kubectl -n robot-dh rollout status deployment/robot-dh-api --timeout=240s
kubectl -n robot-dh port-forward deployment/robot-dh-api 18080:8000
curl http://127.0.0.1:18080/health
curl http://127.0.0.1:18080/infra/health
curl http://127.0.0.1:18080/runs
```

## 正式镜像重建

日常开发可以继续使用：

```bash
make docker-build
```

`docker/Dockerfile` 默认走阿里云国内镜像源（PyPI + PyTorch CPU wheel），用于解决国内拉 `download.pytorch.org` 超时的问题：

- `PIP_INDEX_URL` 默认 `https://mirrors.aliyun.com/pypi/simple/`
- `TORCH_WHEEL_INDEX` 默认 `https://mirrors.aliyun.com/pytorch-wheels/cpu/`
- `TORCH_SPEC` 默认 `torch==2.6.0+cpu`（阿里云镜像下 cp311 linux 最新可用 CPU wheel）

海外环境 / CI 想切回 PyTorch 官方源，通过 `DOCKER_BUILD_ARGS` 透传 `--build-arg` 即可：

```bash
make docker-build DOCKER_BUILD_ARGS='\
  --build-arg PIP_INDEX_URL=https://pypi.org/simple/ \
  --build-arg TORCH_WHEEL_INDEX=https://download.pytorch.org/whl/cpu \
  --build-arg TORCH_SPEC=torch'
```

如果要做干净的正式重建并避免混入历史热修补丁层，当前已验证的路径是：

### 1. 构建正式镜像

```bash
docker build -q -t robot-data-harness:formal -f docker/Dockerfile .
```

### 2. 检查 history 是否为标准 Dockerfile 层

```bash
docker history --format '{{.ID}}|{{.CreatedSince}}|{{.CreatedBy}}|{{.Size}}' robot-data-harness:formal | head -n 8
```

期望顶部层序列类似：

- `CMD ["--help"]`
- `ENTRYPOINT ["robot-dh"]`
- `RUN pip install --no-cache-dir .`
- `COPY . /app`

如果顶层多出类似 `--help` 的额外匿名层，通常表示镜像来自 `docker commit` 热修，而不是正式构建。

### 3. 验证 PostgreSQL URI 不再被掩码

```bash
docker run --rm --entrypoint python3 \
  -e ROBOT_DH_DB_URI='postgresql+psycopg://user:secretpass@db.example.com:5432/robot_dh' \
  robot-data-harness:formal \
  -c "from robot_dh.registry.db import _normalized_engine_uri; uri = _normalized_engine_uri(); print('MASKED', '***' in uri); print(uri)"
```

期望结果：

- 输出 `MASKED False`
- 返回值中保留真实密码，而不是 `***`

### 4. 推广到 kind 使用的 tag

```bash
docker tag robot-data-harness:formal robot-data-harness:local
make kind-load
kubectl -n robot-dh rollout restart deployment/robot-dh-api
kubectl -n robot-dh rollout status deployment/robot-dh-api --timeout=240s
```

## CLI 参考

### 顶层命令

v1.3 命令：

- `robot-dh --version`
- `robot-dh generate-demo`
- `robot-dh validate`
- `robot-dh compare`
- `robot-dh gate`
- `robot-dh scan`
- `robot-dh dataset register|list|show`
- `robot-dh runs list|show`
- `robot-dh infra doctor`

v1.4 新增数据湖 / ETL 命令（完整签名见 [v1.4 数据湖 → v1.4 CLI / API / 元数据表](#v14-cli--api--元数据表)）：

- `robot-dh normalize`（raw → ODS）
- `robot-dh build-features`（ODS → DWD）
- `robot-dh build-ads`（DWD → ADS）
- `robot-dh etl run`（单数据集编排：normalize + features + 可选 build-ads）
- `robot-dh etl scan`（按 `s3://.../raw/` 批量发现并执行 etl run）
- `robot-dh lake init|list|audit|manifest`

### validate

```bash
robot-dh validate \
  --dataset samples/button_press_001 \
  --config configs/button_press.yaml \
  --output runs/button_press_001_remote \
  --run-id remote-demo-v13 \
  --record-to-registry \
  --gate-policy configs/gate_policy.yaml \
  --artifact-store s3 \
  --artifact-prefix runs/{run_id}
```

### compare

```bash
robot-dh compare \
  --baseline runs/baseline/report.json \
  --candidate runs/candidate/report.json
```

### gate

```bash
robot-dh gate \
  --report runs/button_press_001/report.json \
  --policy configs/gate_policy.yaml
```

### scan

```bash
robot-dh scan \
  --root samples \
  --config configs/button_press.yaml \
  --output-root runs/scan \
  --registry \
  --only-new
```

### infra doctor

```bash
robot-dh infra doctor
robot-dh infra doctor --output json
robot-dh infra doctor --check db,s3,redis
```

行为说明：

- DB：检查后端类型、`SELECT 1`、schema 表
- S3：检查 endpoint 与 bucket
- Redis：执行 `PING`
- 如果未配置远端变量，S3 和 Redis 可以返回 `SKIP`

## API 参考

当前 FastAPI 服务位于 `robot_dh.api.main:app`，版本号为 `0.1.5`。

### 接口列表

v1.3 基础接口：

- `GET /health`
- `GET /infra/health`
- `GET /datasets`
- `GET /runs`
- `GET /runs/{run_id}`
- `POST /validate`

v1.4 数据湖只读查询（DB 不可达时返回 503）：

- `GET /lake/assets?layer=&dataset_id=&version=&limit=`
- `GET /lake/lineage?uri=&limit=`（返回入边 + 出边）
- `GET /etl/jobs?limit=`
- `GET /etl/jobs/{job_id}`
- `GET /quality/summary?limit=`（每个 `(dataset_id, version)` 最新一条 quality_snapshot）

### `/validate` 请求示例

```bash
curl -X POST http://127.0.0.1:8000/validate \
  -H 'Content-Type: application/json' \
  -d '{
    "dataset_path": "samples/button_press_001",
    "config_path": "configs/button_press.yaml",
    "output_dir": "runs/api-demo",
    "run_id": "api-demo",
    "record_to_registry": true,
    "gate_policy_path": "configs/gate_policy.yaml",
    "artifact_store": "s3",
    "artifact_prefix": "runs/{run_id}"
  }'
```

`/validate` 当前仍是同步执行接口，更适合调试与轻量调用；如果要进一步工业化，建议下一步接入队列或异步任务系统。

## Makefile 常用目标

### 开发与测试

- `make setup`
- `make test`
- `make demo-data`
- `make demo-local`
- `make scan-local`
- `make infra-doctor`
- `make infra-doctor-json`

### 镜像与 kind

- `make docker-build`
- `make kind-load`

### K8s 资源管理

- `make k8s-apply`
- `make k8s-apply-cronjob`
- `make k8s-apply-remote-secret`
- `make k8s-status`
- `make k8s-clean`

### K8s 运行与日志

- `make k8s-copy-data`
- `make k8s-run-job`
- `make k8s-run-job-remote`
- `make k8s-logs`
- `make k8s-copy-artifacts`
- `make k8s-api-port-forward`
- `make k8s-run-scan-once`
- `make k8s-run-scan-remote`
- `make k8s-scan-logs`

### 清理

- `make clean-runs`

### v1.4 数据湖 / ETL（本地 + 远端 CLI）

- `make lake-doctor` — db + s3 + redis + lake bucket 全检查
- `make lake-audit` — bucket + 6 层 prefix + manifest 完整性 + 5 张元数据表
- `make lake-list` — 列出所有层下的 `(dataset_id, version)` slice
- `make normalize-demo-local` — raw → `runs/lake/ods/button_press_001/v1`
- `make features-demo-local` — ods → `runs/lake/dwd/button_press_001/v1`
- `make ads-demo-local` — dwd → `runs/lake/ads/quality`
- `make etl-demo-local` — 一条命令跑完三段流水 + 写 `etl_summary.json`
- `make etl-remote-one` — 远端单数据集完整三段流水（默认 `s3://robot-datasets/raw/button_press_001/v1`）
- `make etl-remote-scan` — 远端批量发现并执行 ETL（`--limit 10`）

### v1.4 K8s lake target（kind 调度）

完整速查见 [v1.4 K8s ETL 调度 → Makefile 目标速查](#makefile-目标速查)。

## 测试与验收

默认测试命令：

```bash
make test
```

测试覆盖包括：

- 本地 validator、gate、scan 流程
- SQLAlchemy registry 与 SQLite 往返读写
- PostgreSQL URI 掩码回归测试
- `infra doctor` 行为
- FastAPI v1.3 接口
- 可选 PostgreSQL / S3 集成测试

可选集成测试环境变量：

- `ROBOT_DH_TEST_POSTGRES_URI`
- `ROBOT_DH_TEST_S3_ENDPOINT_URL`
- `ROBOT_DH_TEST_S3_ACCESS_KEY`
- `ROBOT_DH_TEST_S3_SECRET_KEY`
- `ROBOT_DH_TEST_S3_ARTIFACT_BUCKET`
- `ROBOT_DH_TEST_S3_REGION`

如果这些变量不存在，对应测试会自动 skip，因此没有云服务时 `make test` 仍可通过。

推荐的完整验收链路如下。

### 本地验收

```bash
make test
make demo-local
```

### WSL 公网直连验收

```bash
source client/wsl-export-public-env.sh
./client/wsl-remote-doctor.sh
PYTHONPATH=src python -m robot_dh.cli infra doctor --output json
PYTHONPATH=src python -m robot_dh.cli dataset register \
  --dataset samples/button_press_001 \
  --dataset-id button_press_001 \
  --version v1 \
  --storage-uri file://samples/button_press_001
PYTHONPATH=src python -m robot_dh.cli validate \
  --dataset samples/button_press_001 \
  --config configs/button_press.yaml \
  --output runs/button_press_001_public \
  --run-id public-demo-v13 \
  --record-to-registry \
  --gate-policy configs/gate_policy.yaml \
  --artifact-store s3 \
  --artifact-prefix runs/{run_id}
PYTHONPATH=src python -m robot_dh.cli scan \
  --root samples \
  --config configs/button_press.yaml \
  --output-root runs/scan_public \
  --registry \
  --artifact-store s3 \
  --artifact-prefix runs/{run_id}
```

### K8s remote 验收

```bash
docker build -q -t robot-data-harness:formal -f docker/Dockerfile .
docker tag robot-data-harness:formal robot-data-harness:local
make kind-load
make k8s-apply
make k8s-apply-cronjob
make k8s-apply-remote-secret
kubectl -n robot-dh rollout restart deployment/robot-dh-api
kubectl -n robot-dh rollout status deployment/robot-dh-api --timeout=240s
make k8s-run-job-remote
kubectl -n robot-dh wait --for=condition=complete job/robot-dh-validator --timeout=240s
make k8s-logs
make k8s-run-scan-remote
kubectl -n robot-dh wait --for=condition=complete job/robot-dh-scan-manual --timeout=240s
make k8s-scan-logs
kubectl -n robot-dh port-forward deployment/robot-dh-api 18080:8000
curl http://127.0.0.1:18080/health
curl http://127.0.0.1:18080/infra/health
```

## 故障排查

### `psycopg` 导入失败

- 重新执行 `python -m pip install -e .[dev]`
- 确认当前 Python 环境就是实际运行 `robot-dh` 的环境

### PostgreSQL 认证失败

- 检查 `ROBOT_DH_DB_URI`
- 确认数据库名为 `robot_dh`
- 确认用户名为 `robot_dh_app`
- 检查密码、防火墙与白名单配置

### `no pg_hba.conf entry for host ...`

- 这通常不是本地代码问题，而是远端 PostgreSQL 尚未放行当前出口 IP / CIDR
- 在远端 infra 配置中检查 `POSTGRES_APP_TRUSTED_CIDRS`
- 执行远端 `./scripts/04_up.sh` 同步受控 `pg_hba.conf` 区块
- 确认 UFW 与安全组中的 `TRUSTED_CIDR` / `SSH_TRUSTED_CIDR` 一致收口

### S3 签名错误

- 检查 `ROBOT_DH_S3_ACCESS_KEY` 和 `ROBOT_DH_S3_SECRET_KEY`
- 确认 endpoint 指向的是 MinIO S3 API 端口，而不是 console 端口
- 检查本机时间是否漂移

### Redis `NOAUTH`

- 检查 `ROBOT_DH_REDIS_URL`
- 确认 URL 中包含密码段

### WSL 能连但 K8s Pod 不能连

- 检查 `k8s/secret.yaml` 是否仍然写着 `127.0.0.1` 或 tunnel 端口
- 确认 `k8s/secret.yaml` 使用的是公网 IP 或 DNS
- 检查远端白名单是否覆盖到 kind / K8s 的出口 IP / CIDR

### `ImagePullBackOff`

- 重新执行 `make kind-load`
- 确认 kind 中使用的 tag 仍然是 `robot-data-harness:local`
- 必要时重新执行 `docker tag robot-data-harness:formal robot-data-harness:local`

### Secret 没注入

- 确认 `k8s/secret.yaml` 是由 `k8s/secret.example.yaml` 复制而来
- 确认已经执行 `make k8s-apply-remote-secret`
- 检查 `kubectl -n robot-dh get secret robot-dh-remote-env`

### 怀疑镜像仍是热修补丁

- 执行 `docker history --format '{{.ID}}|{{.CreatedSince}}|{{.CreatedBy}}|{{.Size}}' robot-data-harness:local | head`
- 如果最顶层多出一个不属于 Dockerfile 的匿名层，说明当前 tag 可能仍指向历史热修镜像
- 按“正式镜像重建”章节重新构建并重新 `kind-load`

### BuildKit 本地异常

- 当前正式重建已验证可直接使用默认 BuildKit：`docker build -q -t robot-data-harness:formal -f docker/Dockerfile .`
- 如果你遇到本地 Docker Desktop / BuildKit 状态问题，可以临时退回：

```bash
DOCKER_BUILDKIT=0 docker build -t robot-data-harness:local -f docker/Dockerfile .
```

但这只应作为临时兜底，不是 README 的主路径。

## 安全与提交说明

- `k8s/secret.yaml` 应视为本地私有文件，不应提交
- `client/wsl-export-public-env.sh` 可能包含真实凭证，建议仅保留在本地环境
- `client/robot-dh-public.env` 与 `k8s/secret.example.yaml` 只应存放示例值，不应写入真实密码
- v1.4 真实密码 lake env 必须放在 `~/.config/robot-dh/robot-dh-lake.env`（权限 0600），仓库已配置 `.gitignore` 兜底

---

## v1.4 数据湖

### 分层

```text
robot-lake/raw/{dataset_id}/{version}/         # 原始资产，只追加
robot-lake/ods/{dataset_id}/{version}/         # 标准化明细 parquet
robot-lake/dwd/{dataset_id}/{version}/         # 清洗 + 特征 parquet
robot-lake/ads/quality/                        # 应用指标 parquet（跨 dataset 共享）
robot-lake/lineage/events/yyyy/mm/dd/*.jsonl   # 血缘事件
robot-lake/tmp/{run_id}/                       # ETL 临时区
```

### 产物 schema 概览

| 层级 | 文件 | 关键字段 |
|---|---|---|
| ods | `pose.parquet` | episode_id / frame_idx / timestamp_sec / x / y / z / qx-qw / quat_norm |
| ods | `video_meta.parquet` | dataset_id / video_uri / fps / frame_count / duration_sec / width / height |
| ods | `episode_meta.parquet` | dataset_id / episode_id / num_samples / duration_sec / source_uri / meta_json |
| dwd | `pose_feature.parquet` | roll / pitch / yaw / velocity_mps / delta_d / is_velocity_jump / is_press_candidate |
| dwd | `press_event.parquet` | event_id / frame_idx / cluster_id / cluster_center_x,y / z_prominence |
| dwd | `trajectory_segment.parquet` | segment_id / start_frame / end_frame / segment_type / duration_sec / distance |
| dwd | `episode_feature.parquet` | num_samples / z_min,max / max/mean/p95_velocity_mps / detected_press_count / cluster_silhouette |
| ads | `dataset_quality_summary.parquet` | num_episodes / avg_quality_score / pass_rate / total_press_count |
| ads | `validator_failure_stats.parquet` | validator_name / total_runs / fail_count / failure_rate |
| ads | `episode_quality_score.parquet` | episode_id / quality_score / quality_status |

每层目录下都会附带一份 `_manifest.json`（字段：dataset_id / version / layer / created_at / schema_version / source_uris / output_uri / files[checksum/size/row_count] / metrics / job / code）。`normalize` 同时支持 v1.3 demo 三件套（`endpose.pt` / `video.mp4` / `meta.yaml`）和远端 HuggingFace/LeRobot/robomimic 风格 raw（parquet / HDF5），后者通过通用 7D pose 适配器抽取多 episode ODS。

### PostgreSQL lake 元数据表

云端 `robot_dh` 库已经创建以下 5 张表，主项目**只用不迁**：

| 表 | 主要用途 | 唯一键 |
|---|---|---|
| `lake_assets` | 单对象元数据（uri/size/row_count/checksum/asset_type） | `uri` UNIQUE |
| `etl_jobs` | ETL 作业运行（含 `metrics_json` jsonb） | `job_id` UNIQUE |
| `lineage_edges` | source_uri → target_uri 血缘边 | 无唯一约束 |
| `dataset_versions` | dataset 版本聚合（raw/ods/dwd URI） | `(dataset_id, version)` UNIQUE |
| `quality_snapshots` | quality gate 结果快照 | 无唯一约束（按 created_at desc 取最新） |

完整 DDL 参考：`postgres/migrations/001_lake_metadata.reconstructed.sql`。SQLAlchemy 模型在 `src/robot_dh/warehouse/models.py`。

### v1.4 环境变量（在 v1.3 基础上新增 1 个）

```text
ROBOT_DH_S3_LAKE_BUCKET=robot-lake   # v1.4 新增；其余 8 个与 v1.3 一致
```

完整 9 变量清单见 `client/robot-dh-lake.env.example`。真实密码版放 `~/.config/robot-dh/robot-dh-lake.env`（0600）。

### 本地 ETL（无远端服务也能跑）

```bash
make demo-data                # 生成 samples/button_press_001
make normalize-demo-local     # raw → runs/lake/ods/button_press_001/v1
make features-demo-local      # ods → runs/lake/dwd/button_press_001/v1
make ads-demo-local           # dwd → runs/lake/ads/quality
make etl-demo-local           # 一条命令跑完三段流水 + 写 etl_summary.json
```

### 远端 ETL（MinIO + PostgreSQL）

```bash
source ~/.config/robot-dh/robot-dh-lake.env
make lake-doctor              # v1.4 = db + s3 + redis + lake bucket 全检查
make lake-audit               # bucket + 6 层 prefix + manifest 完整性 + 5 张元数据表
make lake-list                # 列出所有层下的 (dataset_id, version) slice
make etl-remote-one           # 对默认 dataset 跑完整三段流水（含 build-ads）
make etl-remote-scan          # 自动发现 robot-datasets/raw 下所有数据集并跑 ETL
make k8s-run-etl-remote       # 在 K8s 里提交一次远端 ETL scan Job
```

### CLI 一览（v1.4 新增命令）

```bash
robot-dh lake init                 # 探测 bucket / prefix / 元数据表（不创建资源）
robot-dh lake list [--layer LY]    # 列资产；LY = raw|ods|dwd|ads|lineage|tmp
robot-dh lake audit                # 4 维度审计
robot-dh lake manifest --uri URI   # 读取并打印某层的 _manifest.json

robot-dh normalize --dataset URI --output URI [--dataset-id ID --version V]
robot-dh build-features --input URI --output URI [--config configs/etl_default.yaml]
robot-dh build-ads --input-root URI --output URI

robot-dh etl run --dataset URI --lake-root URI [--build-ads]
robot-dh etl scan --root URI --lake-root URI [--limit N] [--force] [--build-ads]
```

### FastAPI 只读查询（v1.4 新增 5 个端点）

```text
GET /lake/assets ? layer & dataset_id & version & limit
GET /lake/lineage ? uri & limit            # 返回入边 + 出边
GET /etl/jobs ? limit
GET /etl/jobs/{job_id}
GET /quality/summary ? limit               # 每个 (dataset_id,version) 最新一条 quality_snapshot
```

DB 不可达时返回 `503` + 清晰错误。

### 运维硬约束（必须遵守）

- ❌ 禁止删除 `raw/` 层任何对象（只追加；覆盖通过 MinIO versioning 兜底）
- ❌ 禁止跨层反向写（dwd 不能写 ods，ads 不能写 dwd…）
- ❌ 禁止 `mc rb --force local/robot-lake`
- ❌ 禁止 `DROP DATABASE robot_dh`
- ❌ 同一个 `lake_assets.uri` 不要重复登记（唯一索引会报错；服务层做了写入或更新兜底）
- ✅ ETL 本地临时目录由进程自清理；`tmp/{run_id}/` 保留给远端编排临时交换使用
- ✅ 真实密码 env 文件权限 0600，绝对不入 git

### 常见故障

- `pyarrow` / `h5py` 安装失败：检查 Python 版本（>=3.10），优先尝试 `pip install --upgrade pip` 再装
- s3 endpoint 连接失败：先 `make lake-doctor`，看是 env 没 source 还是网络/防火墙问题
- bucket 不存在：联系 infra 团队，主项目不创建 bucket（`lake init` 只探测）
- manifest 缺失：跑 `robot-dh lake audit --output json`，输出 `manifest_completeness.incomplete[]` 给出哪一层哪个 (ds, version) 缺
- PostgreSQL `lake_assets` 等表不存在：让 infra 跑 `./scripts/21_pg_apply_lake_schema.sh`；主项目不做远端迁移
- WSL CLI 能访问但 K8s Pod 不能访问：检查 `k8s/secret.yaml` 是否已经 `make k8s-apply-remote-secret`，并确认 K8s 出口 IP 在 MinIO/PG/Redis 白名单内

## v1.4 K8s ETL 调度

> 资源形态：kind / Kubernetes Job + CronJob。

v1.4 的目标之一是把 ETL 从 WSL CLI 推进到本地 kind 集群里跑——把"raw 机器人数据"变成"可查询、可治理、可调度的 Parquet 数据湖"。本阶段**只用 Kubernetes Job / CronJob**，不引入 Argo Workflows、不引入 Operator（这些留到 v1.6）。

### 资源布局

`k8s/v1_4_lake/` 下的清单：

| 文件 | 资源 | 作用 |
| --- | --- | --- |
| `lake-secret.example.yaml` | `Secret robot-dh-lake-secrets` | 占位模板，**禁止**直接 apply，请走脚本注入 |
| `lake-debug-pod.yaml` | `Pod robot-dh-lake-debug` | sleep infinity 的 debug pod，便于 `kubectl exec` 跑 `robot-dh infra doctor` / `lake audit` / `lake list` |
| `lake-etl-one-job.yaml` | `Job robot-dh-lake-etl-one` | 单数据集 `etl run`（raw→ods→dwd），通过 env 注入 `DATASET_URI` / `DATASET_ID` / `DATASET_VERSION` / `LAKE_ROOT` |
| `lake-etl-scan-job.yaml` | `Job robot-dh-lake-etl-scan` | 批量 `etl scan`，通过 env 注入 `DATA_ROOT` / `LAKE_ROOT` / `SCAN_LIMIT` |
| `lake-build-ads-job.yaml` | `Job robot-dh-lake-build-ads` | 单独的 ADS 聚合（`dwd → ads/quality`） |
| `lake-etl-cronjob.yaml` | `CronJob robot-dh-lake-etl-scan` | 默认 `0 */6 * * *` 周期扫描；`concurrencyPolicy: Forbid` |

### 前置条件

1. v1.4 Python ETL 已通过本地验收（`make test` + `make etl-demo-local`）
2. 已经 build 并 `kind load` 镜像：
   ```bash
   make docker-build
   make kind-load
   ```
3. 远端 PostgreSQL / MinIO / Redis 对 kind Pod 网络**可直达**，且对端安全组放行了 kind 出口 IP
4. K8s Secret `robot-dh-lake-secrets` 已经从 shell 环境注入（见下一节）

### WSL CLI SSH tunnel vs K8s Pod 直连

> **关键事实**：kind Pod 不能复用 WSL 的 `127.0.0.1` SSH tunnel。
>
> - WSL CLI 里 `ssh -L 127.0.0.1:5432:pg:5432 user@bastion` 把端口绑在 WSL loopback；
> - kind 在 Docker 里跑，Pod 网络看不见 WSL 的 loopback；
> - 所以 K8s Pod 必须直连**云端真实地址**（公网 IP 或同 VPC 内网地址），并由对端安全组放行 kind 节点的出口 IP / NAT 网段。
>
> `scripts/k8s_create_lake_secret_from_env.sh` 默认会拒绝任何 `127.0.0.1` / `localhost` / `::1` 的 endpoint，避免把 WSL tunnel 的地址错误注入到 Pod。如果你确实在 kind 内部跑 MinIO / PG（cluster-internal service），可以加 `--allow-localhost` 跳过该检查。

### Secret 注入（不要提交真实密钥）

> 真实密码版的 lake env 仓库**不会提交**（仓库里只有 `client/robot-dh-lake.env.example` 占位模板）。权威路径是 `~/.config/robot-dh/robot-dh-lake.env`（权限 0600）。

```bash
source ~/.config/robot-dh/robot-dh-lake.env   # 把 ROBOT_DH_* 加载到 shell
./scripts/k8s_create_lake_secret_from_env.sh
```

脚本行为：
- 检查必填变量（`ROBOT_DH_DB_URI` / `ROBOT_DH_S3_ENDPOINT_URL` / `ROBOT_DH_S3_*` / `ROBOT_DH_S3_LAKE_BUCKET` 等）；
- 拒绝 loopback endpoint（`127.0.0.1` / `localhost` / `::1`），加 `--allow-localhost` 可强制放行（仅 cluster-internal service 才需要）；
- 用 `kubectl create secret generic --dry-run=client -o yaml | kubectl apply -f -` 做存在则更新、不存在则创建；
- **不回显 secret 值**，最后只打印 `kubectl describe secret` 的 KEY / Type / 长度摘要。

`make k8s-apply-lake-secret-example` 只会打印警告——它**不会**把 `lake-secret.example.yaml` 应用到集群（避免误把 `CHANGE_ME` 写进真实集群）。

### 完整调度链路

```bash
# 1) image + namespace + debug pod
make docker-build
make kind-load
make k8s-apply-lake

# 2) 注入 secret（从 shell 环境拿，不走 git）
source ~/.config/robot-dh/robot-dh-lake.env
./scripts/k8s_create_lake_secret_from_env.sh

# 3) 健康检查（在集群里跑，不是 WSL）
make k8s-lake-status
make k8s-lake-doctor

# 4) 单数据集
make k8s-run-etl-one
./scripts/k8s_wait_lake_job.sh robot-dh-lake-etl-one
make k8s-lake-logs

# 5) 批量扫描
make k8s-run-etl-scan
./scripts/k8s_wait_lake_job.sh robot-dh-lake-etl-scan
make k8s-lake-logs

# 6) ADS 聚合
make k8s-run-build-ads
./scripts/k8s_wait_lake_job.sh robot-dh-lake-build-ads
make k8s-lake-logs

# 7) 定时调度
make k8s-apply-lake-cron
kubectl -n robot-dh get cronjob robot-dh-lake-etl-scan

# 8) 状态 / 清理
make k8s-lake-status
make k8s-delete-lake-jobs   # 只删 jobs，不动 namespace / secret / debug pod
```

> 覆盖 Job 默认参数：直接编辑 `k8s/v1_4_lake/lake-etl-*-job.yaml` 里 `env` 区块（`DATASET_URI` / `SCAN_LIMIT` / `DWD_INPUT_ROOT` 等），或在 apply 前 `kubectl set env --local -f ... -o yaml | kubectl apply -f -`。

### Makefile 目标速查

| 目标 | 行为 |
| --- | --- |
| `make k8s-apply-lake-secret-example` | 只打印警告，不真正 apply example secret |
| `make k8s-apply-lake` | apply namespace + lake debug pod |
| `make k8s-lake-doctor` | 在 debug pod 内执行 `robot-dh infra doctor` |
| `make k8s-run-etl-one` | 删旧 job 再 apply `lake-etl-one-job.yaml` |
| `make k8s-run-etl-scan` | 删旧 job 再 apply `lake-etl-scan-job.yaml` |
| `make k8s-run-build-ads` | 删旧 job 再 apply `lake-build-ads-job.yaml` |
| `make k8s-lake-logs` | 打印最近一个 `component=lake-etl` Job 的日志 |
| `make k8s-apply-lake-cron` | apply `lake-etl-cronjob.yaml` |
| `make k8s-lake-status` | `kubectl -n robot-dh get pods,jobs,cronjobs -l app=robot-dh` |
| `make k8s-delete-lake-jobs` | 删 lake Jobs，保留 namespace / secret / cronjob / debug pod |

### 验收清单

`make k8s-run-etl-scan` 与 `make k8s-run-build-ads` 都跑完后，应满足：

- `kubectl -n robot-dh get jobs` 中 `robot-dh-lake-build-ads` 状态为 `Complete`；
- `robot-dh-lake-etl-scan` 的状态视输入而定：
  - 全部数据集 schema 兼容 → `Complete`；
  - 含 schema 不兼容数据集 → Job 状态为 `Failed`、Pod `Error`，**但这是预期行为**：`robot-dh etl scan` 在任一 dataset FAIL 时返回非零退出码。判定标准应改为查看日志摘要：`make k8s-lake-logs` 末尾 JSON 中 `succeeded >= 1` 即表示业务上成功，对应数据集的 ods/dwd 仍会落地；
- MinIO `robot-lake/ods/<dataset>/<version>/` 下有 `pose.parquet` / `video_meta.parquet` / `episode_meta.parquet` / `_manifest.json`；
- MinIO `robot-lake/dwd/<dataset>/<version>/` 下有 `pose_feature.parquet` / `press_event.parquet` / `trajectory_segment.parquet` / `episode_feature.parquet` / `_manifest.json`；
- MinIO `robot-lake/ads/quality/` 下有 `dataset_quality_summary.parquet` / `validator_failure_stats.parquet` / `episode_quality_score.parquet` / `_manifest.json`；
- PostgreSQL `lake_assets` / `etl_jobs` / `lineage_edges` 有新记录（用 `robot-dh lake list` 或直连 `psql` 验证）；
- 集群内复查：`kubectl -n robot-dh exec robot-dh-lake-debug -- robot-dh lake list` 应能看到 `ods` / `dwd` / `ads` 三层的 dataset slice。

### 常见故障

| 现象 | 排查思路 |
| --- | --- |
| `etl-scan` Job 显示 Error，但 ods/dwd 有新数据 | scan 在任意 dataset schema 不兼容时返回非零退出码，符合设计。查 `make k8s-lake-logs` 摘要 JSON 中 `succeeded`/`failed`/`skipped`；`failed` 项的 `error` 字段会指出是 raw layout 不被适配（如 bridgedata_v2 / droid/calibration 这类） |
| Pod 里连不上 MinIO / PG | endpoint 是不是写成了 `127.0.0.1`？kind Pod 不能走 WSL tunnel；改成云端真实地址 + 安全组放行 kind 出口 |
| `source client/robot-dh-lake.env: No such file or directory` | 仓库不提交真实凭据；权威路径是 `~/.config/robot-dh/robot-dh-lake.env`（0600） |
| `secret "robot-dh-lake-secrets" not found` | 跑 `./scripts/k8s_create_lake_secret_from_env.sh`；确认 namespace=`robot-dh` |
| `ImagePullBackOff` | `make docker-build && make kind-load`；检查 image tag = `robot-data-harness:local` |
| `ModuleNotFoundError: pyarrow` / `robot-dh --version` 报旧版本号 | docker image 不是最新版（用 `docker run --rm --entrypoint robot-dh robot-data-harness:local --version` 检查）；重 build 后再 `kind load` |
| `pip install torch` 在 docker-build 阶段超时 | 默认已走阿里云 PyTorch CPU wheel 镜像；若仍超时，参考[正式镜像重建](#正式镜像重建) 切换 `DOCKER_BUILD_ARGS` |
| Job `OOMKilled` | Job YAML 里调高 `resources.limits.memory`（默认 4Gi） |
| `S3 AccessDenied` | MinIO access/secret key 错；或 bucket 名拼错（`ROBOT_DH_S3_LAKE_BUCKET` vs `ROBOT_DH_S3_DATA_BUCKET`） |
| `PostgreSQL authentication failed` | `ROBOT_DH_DB_URI` 用户/密码不对，或者主机不在 PG `pg_hba.conf` 白名单 |
| `lake_assets table not found` | 远端 PG 还没跑 `scripts/21_pg_apply_lake_schema.sh`；主项目不做自动迁移 |
| CronJob 不触发 | `kubectl -n robot-dh describe cronjob robot-dh-lake-etl-scan`，看 `LastScheduleTime` 和 events；上一次 Job 还在跑会被 `Forbid` 拦截 |

### 接收侧交接物清单

详见 `docs/v1_4_handoff_inbox.md`。本仓库针对 SSH 暂不可用时已经做了完整的"直连反查"补救，所有 v1.4 关键资产（env、SQL、policy、资产清单）都在仓库内有对应文件。SSH 恢复后用 `scripts/fetch_v1_4_handoff.sh` 拉云端权威版覆盖即可。

## v1.5 Scale Benchmark + Sharded ETL + Runtime Profiling

v1.5 在 v1.4 数据湖之上叠加四个能力，全部向后兼容、不破坏已有命令：

1. **ETL Performance Profiler**：`robot_dh.perf.EtlProfiler` 记录 normalize / build-features / build-ads / etl_run / shard 各阶段的
   `input_bytes / output_bytes / duration_sec / download_duration_sec / upload_duration_sec / compute_duration_sec /
   peak_memory_mb / status`，并落到 PostgreSQL `etl_perf_runs`（缺表时仅 warning）。
2. **Sharded ETL**：`robot-dh etl plan / run-shard / merge-summary` 三段命令，把 30GB 级 raw 数据装箱到多个 shard 并独立执行。
3. **Scale Benchmark**：`robot-dh mutate` 注入 8 种异常 + `robot-dh benchmark run` 把 mutated dataset 喂给 validator，验证 quality gate 是否能识别预期失败。
4. **Runtime Events**：`robot-dh.runtime.events.RuntimeEventLogger` 写本地 `runs/events/runtime_events_YYYYmmdd.jsonl` + 可选 `runtime_events` 表。

### 本地 benchmark

```
make demo-data
make benchmark-local
# 等价：robot-dh benchmark run --suite configs/benchmark_suite.yaml --output runs/benchmark/v1_5
```

`benchmark_report.json` / `benchmark_report.md` / `benchmark_report.html` 同时落到输出目录；任何 case 不符合 `expected_status` /
`expected_failed_validators` 时进程退出码非零。

### scale30 远端 ETL（CLI）

前置：`source client/robot-dh-v1-5.env` 之后 `ROBOT_DH_S3_*` / `ROBOT_DH_DB_URI` 等环境变量已就绪。

```
mkdir -p runs/plans runs/shards/scale30

robot-dh etl plan \
  --root s3://robot-datasets/raw \
  --lake-root s3://robot-lake \
  --include "*scale30*" \
  --exclude "*bridgedata_v2_scale30*" \
  --target-shard-size-gb 5 \
  --max-shards 16 \
  --output runs/plans/scale30_plan.json \
  --log-format json

robot-dh etl run-shard \
  --plan runs/plans/scale30_plan.json \
  --shard-id 0 \
  --lake-root s3://robot-lake \
  --output runs/shards/scale30/shard_0 \
  --summary-uri runs/shards/scale30/shard_0/shard_summary.json \
  --max-workers 2 \
  --log-format json

robot-dh etl merge-summary \
  --plan runs/plans/scale30_plan.json \
  --shard-results runs/shards/scale30 \
  --output runs/plans/scale30_summary.json \
  --log-format json
```

`etl plan` 与 `etl run-shard` 也支持 `s3://...` 路径作为 `--plan` / `--output`：在 Argo 中 plan 可以落到
`s3://robot-lake/tmp/{workflow.name}/scale30_plan.json`，再被 `run-shard` step 直接读取，避免 Argo artifact repository 配置成本。

### scale30 Argo 长任务（推荐 tmux）

`robot-dh-scale-etl` 模板默认 `activeDeadlineSeconds: 43200`，即 12 小时。长任务不要放在 IDE 临时终端里等，推荐用 `tmux`：

```
tmux new -s robot-dh-scale-etl
cd /home/yunlong/workspace/robot-data-harness

source client/robot-dh-v1-5.env
./scripts/k8s_create_v1_5_secret_from_env.sh

make docker-build
make kind-load
make argo-install
make argo-apply-rbac
make argo-apply-templates

wf=$(kubectl -n robot-dh create -f argo/workflows/submit-scale30-etl.yaml -o jsonpath='{.metadata.name}')
echo "workflow=${wf}"

TIMEOUT=43200 ./argo/scripts/argo_wait_workflow.sh "${wf}"
```

另开一个窗口持续观察 Pod：

```
tmux new -s robot-dh-scale-watch
wf="robot-dh-scale30-etl-xxxxx"  # 替换为上一步输出的 workflow 名称
kubectl -n robot-dh get pods -l workflows.argoproj.io/workflow="${wf}" -w
```

常用排查命令：

```
kubectl -n robot-dh get wf "${wf}" -o wide

kubectl get wf -n robot-dh "${wf}" -o json \
  | jq -r '.status.nodes | to_entries[] | select(.value.phase != "Succeeded" and .value.phase != "Skipped") | [.value.displayName,.value.type,.value.phase,(.value.message // ""),(.value.startedAt // ""),(.value.finishedAt // "")] | @tsv'

kubectl -n robot-dh get pods -l workflows.argoproj.io/workflow="${wf}" -o wide
kubectl -n robot-dh describe pod <pod-name>
kubectl -n robot-dh logs -f <pod-name> -c main
```

历史事故与优化方向见 [`docs/v1_5_scale_etl_deadline_report.md`](./docs/v1_5_scale_etl_deadline_report.md)。

### performance metrics 字段含义

| 字段 | 含义 |
| --- | --- |
| `input_bytes` / `output_bytes` | 阶段输入 / 输出对象字节数（S3 / 本地 stat 累计） |
| `input_rows` / `output_rows` | 阶段输入 / 输出 parquet 行数 |
| `duration_sec` | 阶段 wall-clock 耗时 |
| `download_duration_sec` | 显式标记的 S3 下载累计 |
| `upload_duration_sec` | 显式标记的 S3 上传累计 |
| `compute_duration_sec` | `duration_sec - download - upload`，纯计算时间 |
| `peak_memory_mb` | 阶段内进程 RSS 峰值（psutil 后台采样） |

### PostgreSQL 表（v1.5 新增）

| 表 | 用途 |
| --- | --- |
| `etl_perf_runs` | 每阶段 PerfRecord |
| `etl_shards` | 每个 shard 的 status / succeeded / failed / duration_sec |
| `benchmark_runs` | 整次 benchmark 汇总 |
| `benchmark_cases` | 每个 case 的 expected / actual / match |
| `runtime_events` | runtime event 流水 |
| `argo_workflow_runs` | 可选：Argo workflow 元数据（Prompt B 写入） |

如果远端 v1.5 表不存在或仍是早期字段集，写入路径会提示 schema 缺失；API 严格模式下会返回 503。DDL 由 `robot-dh-infra` 维护，不在本仓库执行。补救入口：

```
# 远端 robot-dh-infra 项目中执行
./scripts/29_pg_apply_v1_5_schema.sh
./scripts/33_pg_apply_etl_shards_align.sh
./scripts/34_pg_apply_benchmark_align.sh
./scripts/30_pg_v1_5_smoke_test.sh
```

### CLI 新增命令一览

```
robot-dh etl plan ...
robot-dh etl run-shard ...
robot-dh etl merge-summary ...
robot-dh mutate --dataset samples/button_press_001 --output runs/mutated/velocity --mutation velocity_spike
robot-dh benchmark run --suite configs/benchmark_suite.yaml --output runs/benchmark/v1_5
robot-dh benchmark report --benchmark-dir runs/benchmark/v1_5
```

所有 v1.5 命令支持 `--log-format human|json`；ETL 相关命令支持 `--max-workers / --work-dir / --tmp-dir / --fail-fast`。

### API 新增只读端点

| Method | Path | 用途 |
| --- | --- | --- |
| GET | `/etl/perf` | 过滤 dataset_id / version / phase / status 的 perf 记录 |
| GET | `/etl/shards` | 过滤 plan_id / status 的 shard 状态 |
| GET | `/benchmark/runs` | 列出 benchmark run |
| GET | `/benchmark/runs/{benchmark_id}` | 单次 benchmark 详情（含每 case） |
| GET | `/events` | runtime events 过滤 event_type / run_id / job_id |

### Makefile target

```
make benchmark-local         # 本地端到端 benchmark
make etl-plan-scale30        # 远端 plan
make etl-run-shard-0         # 远端 shard 0
make etl-merge-scale30       # 汇总 shard 结果
make perf-query              # 列举常用 /etl/perf curl 示例
make v1-5-smoke              # demo-data + benchmark-local 一键 smoke
```

### 常见故障

| 现象 | 排查思路 |
| --- | --- |
| `v1.5 PostgreSQL tables missing` 或 `UndefinedColumn` | 远端 `robot-dh-infra` 没跑 `29` / `33` / `34` / `30`；本仓库只更新模型和写入逻辑，不负责 DDL |
| `S3 endpoint 失败` | 检查 `ROBOT_DH_S3_*` 与 bucket 访问权限；kind Pod 内 endpoint **不能**用 WSL 的 127.0.0.1 SSH tunnel |
| `scale30 prefix 发现不到` | 确认 `--include` glob 与 raw key 实际命名一致；可用 `robot-dh lake list --layer raw --include "*scale30*"` 反查 |
| `pyarrow OOM` | 调高 K8s `resources.limits.memory`，或拆细 `--target-shard-size-gb` |
| `DeadlineExceeded` / `exit status 143` | Argo deadline 到期杀掉 Pod；scale ETL 默认 12 小时，仍超时则看 normalize 内部吞吐和资源瓶颈 |
| `benchmark expected_failed_validators 不匹配` | 检查 `configs/button_press.yaml` 与 demo dataset 是否同步；本地用 46s / 30fps 重新生成 demo |
| `shard 某些 dataset 失败` | `shard_summary.json` 中 `runs[].error_message`；可单独 `robot-dh etl run --dataset ...` 复现 |

### Argo Workflows（kind / K8s）

v1.5 把上述 CLI 编排成 Argo Workflows：

- WorkflowTemplate：`robot-dh-scale-etl`、`robot-dh-benchmark`、`robot-dh-build-ads`
- CronWorkflow：`robot-dh-scale-etl-cron`（12h 一次）
- scale ETL deadline：`activeDeadlineSeconds: 43200`（12 小时）
- RBAC + ConfigMap + Secret 示例：`k8s/v1_5_argo/`
- 跨 step 用 S3 URI 传 plan / shard summary，避免 Argo artifact repository 配置成本

具体目录与上线流程见 [`argo/README.md`](./argo/README.md) 与 [`docs/v1_5_argo_workflow.md`](./docs/v1_5_argo_workflow.md)。常用 Make target：

```
make argo-install
make argo-apply-rbac
make argo-apply-templates
make argo-submit-scale-etl
make argo-submit-benchmark
make argo-submit-build-ads
make argo-apply-cron
make argo-list
make argo-logs
make argo-delete-completed
```

### Prometheus exporter (Go)

v1.5 同步引入了独立的 Go exporter `robot-dh-exporter`：读取 PostgreSQL 中的元数据并暴露 Prometheus 指标。详见 [`go/robot-dh-exporter/README.md`](./go/robot-dh-exporter/README.md)。常用 Makefile target：

```
make exporter-test           # go test ./...
make exporter-docker-build   # 构建 robot-dh-exporter:local
make exporter-kind-load
make exporter-k8s-apply
make exporter-port-forward
curl http://localhost:9108/metrics | grep robot_dh
```


## v1.6.1 — heartbeat / checkpoint / partition / resumable normalize

v1.5 scale ETL 在 normalize 阶段曾因 deadline 超时（`activeDeadlineSeconds=7200`）失败，根因不是代码异常而是「长任务无内部观测 + 不支持局部恢复 + 单 dataset 拖尾」。v1.6.1 解决这三个工程短板：

- **heartbeat**：`HeartbeatReporter` 周期性写 `runs/events/heartbeats_YYYYmmdd.jsonl` + structured log + 可选 `task_heartbeats` 表；`normalize` 内每个子阶段进入/退出都打点。
- **checkpoint**：normalize 输出目录写 `_checkpoint.json`，记录 `completed_steps` 与每个产出文件状态；与 `_manifest.json` 互补。
- **resume / force / SKIP**：
  - 默认 `--resume`：远端有 `_manifest.json` 直接 SKIP；只有 parquet 没有 manifest 则 RESUMED（不重写 parquet，仅补 manifest）。
  - `--no-resume`：禁用 resume。
  - `--force`：跳过所有缓存逻辑，重跑全流程。
- **phase-level etl run**：`robot-dh etl run --phase normalize|features|ads|all`，每个 phase 独立 resume。
- **partition plan**：`robot-dh partition plan` 把单大数据集切成 N 个 partition；后续 Argo 可按 partition 投递。
- **sub-stage profiling**：`EtlProfiler` 新增 `materialize_input_duration_sec / load_bundles_duration_sec / build_pose_table_duration_sec / write_parquet_duration_sec / upload_duration_sec / manifest_duration_sec` 等指标。

### 新命令

```bash
# normalize（带 heartbeat / checkpoint / resume）
robot-dh normalize \
  --dataset samples/button_press_001 \
  --output runs/lake/ods/button_press_001/v1 \
  --heartbeat-interval-sec 5 --progress-log-interval-sec 5

# 复跑同一目录：远端有 manifest -> SKIP；只有 parquet -> RESUMED
robot-dh normalize ... --resume
robot-dh normalize ... --force        # 强制重跑

# phase-level etl run
robot-dh etl run --dataset ... --dataset-id ... --version ... \
  --lake-root runs/lake --phase normalize --resume

# partition plan / list / run-normalize
robot-dh partition plan \
  --dataset s3://robot-datasets/raw/droid_lerobot_scale30/v1 \
  --dataset-id droid_lerobot_scale30 --version v1 \
  --output s3://robot-lake/tmp/partitions/droid_lerobot_scale30_v1.json \
  --target-partition-size-gb 2

robot-dh partition list --plan <plan_uri>

robot-dh partition run-normalize \
  --plan <plan_uri> --partition-index 0 \
  --output s3://robot-lake/ods/droid_lerobot_scale30/v1/part-000
```

### 如何排查 normalize 卡住

1. `tail -f runs/events/heartbeats_$(date -u +%Y%m%d).jsonl | jq` 看心跳；如果 `phase` 一直不变 -> 卡在某个子阶段。
2. `cat <output_uri>/_checkpoint.json` 看到哪一步已完成；下一步就是当前正在跑的步骤。
3. PG 上 `task_heartbeats` 按 `task_id` 过滤；`updated_at` 超 5 倍 `--heartbeat-interval-sec` 没更新 -> 任务可能被 OOM-killed。

更多说明见 `docs/robot_platform_storage_and_deadline_notes.md` 第 3 / 6 节。


## v1.6.2 — Multi-source QC Contract Layer

v1.5 的 validator 只覆盖 button-press demo 的 7D pose 检查；多源数据集（DROID / LeRobot / robomimic / BridgeData V2）需要 dataset-specific 的 schema / temporal / completeness 检查。v1.6.2 在不重写原 validator 的前提下，新增 contract 层：

- **universal contract**：基础 schema / readability / temporal 检查（任何 dataset 都跑）。
- **droid / lerobot contract**：parquet + video，检查 action / timestamp / language / camera coverage。
- **robomimic contract**：HDF5 group 结构，检查 demo / actions / obs / rewards / dones 完整性。
- **bridge contract**：parquet shard，检查 trajectory length / language / image / environment / skill 覆盖。
- **asset profile**：file count / bytes / parquet rows / hdf5 group / mp4 decode info / schema_hash / null_rate / episodes_count / videos_count。
- **contract report**：`contract_report.json` + `contract_report.html` + `asset_profile.json`。
- **PostgreSQL 写入**：`qc_contracts` / `qc_contract_runs` / `asset_profiles`。
- **FastAPI 只读接口**：`/qc/contracts`、`/qc/runs`、`/assets/profiles`。

### 新命令

```bash
robot-dh qc contract list

robot-dh qc profile \
  --dataset-uri samples/button_press_001 \
  --dataset-family universal \
  --output runs/qc/button_press_001

robot-dh qc contract run \
  --dataset-family droid \
  --dataset-uri s3://robot-datasets/raw/droid_lerobot_scale30/v1 \
  --dataset-id droid_lerobot_scale30 --version v1 \
  --output s3://robot-lake/qc/droid_lerobot_scale30/v1 \
  --contract configs/qc/droid_contract.yaml \
  --log-format json

robot-dh qc report --input runs/qc/.../contract_report.json
```

contract_report.json 字段：`contract_id / dataset_family / dataset_id / version / dataset_uri / status / metrics / rules / failed_rules / warning_rules / artifacts`。

### contract 决策树

| 状态 | 触发条件 |
|---|---|
| PASS | 所有 rule 都通过 |
| WARN | 至少一条 severity=warn 不达标 |
| FAIL | 至少一条 severity=fail 不达标 |

每条 rule 走 `metric op threshold` 比较：`>=`、`<=`、`>`、`<`、`==`、`!=`、`in`、`exists`。


## v1.6.3 — ML-ready export + FastAPI 控制面

v1.6.3 把 dwd / ads / qc 的产物聚合为 train/val/test 数据集，并暴露只读的控制面 API。**不做完整训练平台、不引入 vLLM、不做复杂前端**。

### 输出目录

```
output/
  train.parquet
  val.parquet
  test.parquet
  dataset_card.json
  dataset_card.md
  feature_schema.json
  quality_filter.json
  lineage.json
  _manifest.json
```

train/val/test 至少含：`dataset_id / version / dataset_family / episode_id / source_dwd_uri / quality_score / quality_status / selected_features_json / row_count / split`，并直接保留 `episode_feature.parquet` 的核心特征列（`max_velocity_mps / quat_max_norm_error / detected_press_count / cluster_silhouette / duration_sec / num_samples`）。

### Quality filter

- `quality_score >= --quality-threshold`（默认 80）
- `quality_status not in --excluded-status`（默认 `["FAIL"]`）
- 可选 `--dataset-family <list>`、`--min-episode-length <int>`、`exclude_failed_contract`

split 由 `(dataset_id, episode_id)` 哈希决定，固定 seed 保证多次 export 结果一致。

### CLI

```bash
robot-dh ml-ready export \
  --input-root runs/lake/dwd \
  --quality-root runs/lake/ads/quality \
  --output runs/lake/ml-ready/demo/v1 \
  --quality-threshold 80 --split 0.8,0.1,0.1

robot-dh ml-ready list
robot-dh ml-ready show --dataset-id scale30 --version v1
```

### FastAPI endpoints

| 路径 | 说明 |
|---|---|
| GET `/qc/contracts` | contract 定义清单 |
| GET `/qc/contracts/{contract_id}` | contract 详情 |
| GET `/qc/runs` | qc_contract_runs 列表（支持 `contract_id / dataset_id / status`） |
| GET `/qc/runs/{run_id}` | qc 单次运行详情 |
| GET `/assets/profiles` | asset_profiles 列表 |
| GET `/assets/profiles/{profile_id}` | asset_profile 详情 |
| GET `/ml-ready` | ml_ready_datasets 列表 |
| GET `/ml-ready/{dataset_id}/{version}` | ml-ready 单条详情 |
| GET `/workflows` / `/workflows/{name}` / `/workflows/{name}/steps` | workflow_runs / workflow_steps |
| POST `/workflows/scale30` | 不在 API 内运行重 ETL；当前直接返回 501 + 提示走 Argo |

DB 不可达 -> 503；不存在 -> 404。


## v1.6.4 — Argo multi-source DAG + workflow metadata sync

把多源 robot data 真正编排成 DAG（不是包一层单脚本）：

- `robot-dh-multisource-scale30`：discover -> {droid|robomimic|bridge}-{qc|partition|normalize|features} -> build-ads -> benchmark / ml-ready -> lineage / argo-sync。
- `robot-dh-contract-qc` / `robot-dh-ml-ready-export`：单独子任务模板。
- `multisource-scale30-cron`：每天 UTC 02:00 自动跑。
- `robot-dh argo sync`：把 workflow 状态从 kubectl 同步进 `workflow_runs / workflow_steps`，可直接接进 workflow 末尾。
- `robot-dh lineage report`：聚合 workflow_steps + qc_runs + ml_ready_datasets + asset_profiles 为单 JSON。

完整 DAG 与故障矩阵见 `docs/robot_platform_argo_multisource_workflow.md`。

### Make targets

```
make argo-apply-platform              # apply 三个 WorkflowTemplate + CronWorkflow
make argo-submit-multisource-scale30
make argo-submit-contract-qc
make argo-submit-ml-ready
make argo-sync-latest                 # 把最新 workflow 同步到 PG
make argo-platform-logs               # 默认拉最新 Workflow 全部 step pod 的 main 容器日志
                                      # LOG_CONTAINER=wait make argo-platform-logs 可改看 wait/init
make argo-platform-tail               # follow 模式（kubectl logs -f -l ...）
                                      # WF=<wf-name> 覆盖目标；LOG_CONTAINER 同上
                                      # 注意：-f 不会自动接入之后新建的 step pod，需 rerun
make argo-platform-status
make platform-smoke                   # 检查 secret / image / template 是否就位
```

### Secret 流程

```
set -a; source client/robot-dh-platform.env; set +a   # 真实凭据，chmod 600
./scripts/k8s_create_platform_secret_from_env.sh      # 自检 mode/endpoint，不打印 secret
```


## v1.6.5 — robot-dh-exporter v1.6 metrics

`go/robot-dh-exporter` 升级到 v1.6：保留所有 v1.5 指标，新增以下 14 个指标，全部按 v1.6 metadata 表生成。

| Metric | Labels | 说明 |
|---|---|---|
| `robot_dh_qc_contracts_total` | dataset_family, enabled | qc_contracts 计数 |
| `robot_dh_qc_contract_runs_total` | dataset_family, contract_id, status | 每个 contract 每个 status 的次数 |
| `robot_dh_qc_contract_duration_seconds` | dataset_family, contract_id, status | duration_sec 总和 |
| `robot_dh_workflows_total` | workflow_type, status | workflow_runs 计数 |
| `robot_dh_workflow_steps_total` | step_name, phase | workflow_steps 计数 |
| `robot_dh_workflow_step_duration_seconds` | step_name, phase | step duration_sec 总和 |
| `robot_dh_asset_profiles_total` | dataset_family, asset_format, status | asset_profiles 计数 |
| `robot_dh_asset_profile_bytes` | dataset_family, asset_format | bytes 总和 |
| `robot_dh_asset_profile_rows` | dataset_family, asset_format | rows 总和 |
| `robot_dh_ml_ready_datasets_total` | dataset_family, status | ml_ready_datasets 计数 |
| `robot_dh_ml_ready_rows` | dataset_family, split | num_train / num_val / num_test |
| `robot_dh_dataset_partitions_total` | dataset_family, partition_type, status | partition 计数 |
| `robot_dh_task_heartbeat_age_seconds` | phase | 最近 heartbeat 距今秒数（用于报警 stale） |
| `robot_dh_openlineage_events_total` | event_type | OL 事件计数 |

### 行为约束

- 表缺失 -> warning + 该 metric 跳过；其它表继续抓。
- DB 不可达 -> 进程不退出，`robot_dh_exporter_up=0`，`/healthz` 仍 200。
- 不打印 DB 密码（DSN 在日志里被替换为 `user=app:***`）。
- 查询带 ctx timeout，避免卡死后台 goroutine。

### 验收

```bash
make exporter-test                # go test ./...
make exporter-build               # 本地编译
make exporter-docker-build        # Dockerfile 构建（需要 docker）
make exporter-kind-load
make exporter-k8s-apply           # 默认 envFrom: robot-dh-v1-6-secrets
make exporter-port-forward
curl http://localhost:9108/healthz
curl http://localhost:9108/metrics | grep robot_dh_qc_contract
```


## v1.6.6 ~ v1.6.8 — 多源 scale30 实跑回归修复

v1.6.1 ~ v1.6.5 上线后，在 kind 集群跑了 5 轮 `robot-dh-multisource-scale30-{fhkvr,qptk9,ddbfb,fvx5z,dls4z}`，从 archiveLogs 归档把 9 类失败定位到位并写回主线。这一节只记录结论与守门测试，**完整根因 / 复现 / archive log 行号留在 `docs/v1_6_*_request.md` 与 `docs/runs/<date>/<workflow>/INDEX.md`**。

### 失败矩阵（每条都有对应 pytest 守门）

| 现象 | 根因 | 修复点 | 守门测试 |
| --- | --- | --- | --- |
| QC profile 把 `Max Retries Exceeded` 吞成 `cause_type=None` | `_summarize_exception` 只走一级 `__cause__` fallback，botocore 不带 `from` 的 raise 让 cause = err 自身 | `parquet_probe._summarize_exception` 三段 fallback（`__cause__` → `__context__` → `traceback`），最多沿链回溯 8 层并跳过同类型祖先 | `tests/test_qc_probe_failure_surface.py::test_summarize_exception_skips_same_type_self_reference` 等 3 条 |
| bridge-qc `episode_count=0` 但 status=PASS | `_maybe_fill_null_rate` 在 lazy s3fs 抛 `aiohttp.ContentLengthError` 把共享 `ParquetFile` fobj 状态打坏 | `_fill_bridge_metrics` 改先于 null_rate 调用 + 新开 fast `ParquetFile` 句柄；`bridge_metrics` 删除 `samples = per_ep_lengths or fallback_rows` 软降级；新增 contract rule `episode_count_min >= 1` | `tests/test_qc_bridge_contract.py::test_bridge_fails_when_no_episode_column` |
| droid lerobot v2 normalize 静默写出错位 (N, 7) pose | 通用 `_load_parquet_episodes` 没有 cartesian_position 优先级，把 `observation.state[:7]` 当 (xyz, quat) | `src/robot_dh/lake/hf_adapters.py::adapt_droid_lerobot_v2` 注册表 + `cartesian_position(6) → euler_to_quaternion` 优先，state[:7] fallback 必带 joint-angle warning，跨 shard 先 concat 再 group_by `episode_index` | `tests/test_droid_lerobot_v2_adapter.py`（8 条，覆盖 cartesian / pose / state-warning / cross-shard / fail-fast） |
| bridgedata_v2 normalize `not coercible to 7-dim` | raw shard 是 `state: struct<end_effector_pose: struct<x,y,z,roll,pitch,yaw>>` + `episode_idx/step_idx`（非 LeRobot 命名）；`to_pandas()` 把 struct 转 dict 丢字段名 | `_try_adapt_bridge_nested()` 在 pyarrow 层先嗅探 schema，用 `_flatten_pose_struct` 提取 (x,y,z,roll,pitch,yaw) → quaternion；按 `episode_idx` group + `step_idx` 排序 | `tests/test_bridgedata_v2_scale30_fixture.py`（5 条） |
| robomimic `episode_len_p50/p95 = 0` 但 `demo_count=21000` | 老 probe 只取 `data/demo_0/actions.shape`，metric 硬编码 0 | `probe_hdf5` 遍历**所有** `data/demo_X` 收集 `episode_lens`，多文件 concat 后再算 percentile；新增 contract rule `episode_len_p50_min >= 5` | `tests/test_qc_robomimic_episode_lens.py`（6 条） |
| droid-normalize 18 GiB 全量下载 + emptyDir 撑爆 | lerobot v2 layout 下 `videos/` 占 ~10 GiB；通用 download_dir 整个 prefix 都拉 | `S3LakeStore.download_dir` 加 `exclude_prefixes` / `include_prefixes`；`_materialize_input` 命中 lerobot v2 自动 `exclude_prefixes=("videos/",)`；WorkflowTemplate `etl-phase` emptyDir `50Gi → 32Gi` + `requests/limits.ephemeral-storage 4Gi/32Gi` | `tests/test_s3_lake_concurrency.py` + `tests/test_argo_workflow_yaml.py::test_multisource_scale30_etl_phase_has_ephemeral_storage_limit` |
| `download_dir` 静默几小时像卡死 | 只有 "每 50 个文件" 一行进度，单文件 100+ MiB 时进度间隔分钟级 | 叠加 wall-clock 心跳：`ROBOT_DH_DOWNLOAD_PROGRESS_INTERVAL_SEC` 默认 30s 至少一行 `progress=N/M files (X MiB / Y MiB)` | `tests/test_s3_lake_concurrency.py::test_download_dir_wallclock_progress_log` |
| qc-contract-run archive log 0 字节 | python 默认 stdout block-buffered，`SIGKILL` 时 buffer 全丢 | `cli.main()` 入口 `_emit_runner_boot()` 第一行 print 必走 stderr + `flush=True`；WorkflowTemplate qc-contract-run 入口改 `bash -lc 'python -u ... 2>&1 \| tee /events/qc-stdout.log'`；activeDeadlineSeconds 7200 → **1800**；memory 2Gi → 6Gi | `tests/test_cli_runner_boot.py` + `tests/test_argo_workflow_yaml.py::test_multisource_scale30_qc_contract_run_active_deadline_capped` |
| Argo step pod 终态后 `kubectl logs` 立刻 404 | `podGC.strategy: OnPodCompletion` 在 pod 终态时立即删 pod，controller 来不及上传 stdout 到 `archiveLogs` | 所有 WorkflowTemplate 改 `podGC.strategy: OnWorkflowCompletion`；`argo/workflow-controller-configmap` 配 `archiveLogs: true` + `keyFormat=argo-logs/{{workflow.namespace}}/{{workflow.name}}/{{pod.name}}/main.log`；`argo` namespace 同步 robot-dh secret 的 access/secret key | `tests/test_argo_workflow_yaml.py::test_pod_gc_keeps_archivelogs_window` + `test_workflow_controller_artifact_repository_template_shape` |

### 关键反模式（不要再踩）

1. **不要给 QC core metric 写软降级**：`samples = per_ep_lengths if per_ep_lengths else fallback_rows` 会把 enrich 静默失败包装成"看似合理"的失真值。核心 metric 算不出来必须 FAIL。
2. **不要在多处复制 `cause = type(err.__cause__).__name__ if err.__cause__ else None`**：botocore 一类不带 `from` 的 raise 会让 cause_type=None 排不出根因；统一走 `_summarize_exception` 三段 fallback。
3. **不要让 droid lerobot v2 走通用 parquet fallback**：通用 fallback 没有 cartesian_position 优先级，会把 8 维 joint state 错认成 (xyz, quat)，比"normalize 失败"更糟。`adapt_droid_lerobot_v2` registry 命中是硬要求。
4. **不要对长 IO 单靠"按 N 个文件触发"的进度日志**：当单文件 / 单 batch 处理时间 > 30s，进度间隔会拉到分钟级看上去像卡死。必须叠加 wall-clock 心跳。
5. **不要对"轻量 enrichment"和"大文件 download"共享同一个 boto3 client**：300s × 10 × adaptive 在弱网 `ContentLengthError` 上能跑 30 min；`get_s3_boto_client_fast()`（5s/60s/3 attempts）与默认档分开各管各的语义。
6. **不要给 Argo step pod 用 `podGC=OnPodCompletion`**：会让 stdout 在 controller 上传 archiveLogs 之前就被删掉。
7. **不要把 v1.6 archiveLogs 的 source-of-truth 写到 K8s Secret label**：label value 不允许 `/`，跨 namespace 复制 Secret 时来源信息必须走 annotation。

### 已知遗留

- **input cache 在 pod-level retry 仍要重下载**：`/tmp/robot-dh/input-cache/<sha256>` 在 Argo emptyDir step pod 是「同 pod 内 container restart 才生效；pod-level retry 起新 pod 时随 emptyDir 销毁」。如果要在 pod-level retry 也命中，需要 WorkflowTemplate 给 `/tmp/robot-dh` 挂 hostPath / PVC（多节点 K8s 必须 PVC），留给 v1.7。
- **kubectl logs `-f -l ...` 不会自动接入后续新建 step pod**：DAG fanout 后期新拉起的 pod 需要 rerun `make argo-platform-tail`。这是 kubectl 行为，不要再用 argo CLI 绕过；如需「真·全程 follow」走 `make argo-sync-latest` 把状态写 PG 后再查。


## v1.6 端到端命令清单

以下命令是 v1.6 整个数据平台的端到端跑通脚本，按"本地 → 远端 WSL 直连 → kind / K8s Argo"三段递进，每段都可以独立验收。**所有命令都已在本仓库验证可跑（pytest 244 passed / 14 skipped / Go exporter test 全过）**。

> **依赖前提**（仅一次）：`make setup` 完成；Python 3.10+；可选 docker / kind / kubectl / Argo。

### A. 本地零依赖快速验收（5 分钟）

无需远端 PG / MinIO / Redis；用 SQLite + 本地文件系统验证 v1.6 全部新模块。

```bash
# A1. 安装依赖（一次性）
make setup

# A2. 跑全量单元测试：v1.3~v1.6 模块 + Go exporter
make test
( cd go/robot-dh-exporter && GOPROXY=https://goproxy.cn,direct go test ./... )

# A3. 生成 demo 数据
make demo-data
# 等价：robot-dh generate-demo --output samples/button_press_001 --duration-sec 46 --fps 30

# A4. 本地 normalize（v1.6 heartbeat + checkpoint + resume；可中断后再跑）
robot-dh normalize \
  --dataset samples/button_press_001 \
  --output runs/lake/ods/button_press_001/v1 \
  --dataset-id button_press_001 \
  --version v1 \
  --heartbeat-interval-sec 5 \
  --progress-log-interval-sec 5

# A5. 复跑：已 manifest → SKIP；只有部分输出 → RESUMED
robot-dh normalize \
  --dataset samples/button_press_001 \
  --output runs/lake/ods/button_press_001/v1 \
  --dataset-id button_press_001 --version v1 \
  --resume

# A6. phase-level run（normalize → features → ads）
robot-dh etl run \
  --dataset samples/button_press_001 \
  --dataset-id button_press_001 --version v1 \
  --lake-root runs/lake \
  --phase normalize --resume
robot-dh etl run \
  --dataset samples/button_press_001 \
  --dataset-id button_press_001 --version v1 \
  --lake-root runs/lake \
  --phase features --resume
robot-dh etl run \
  --dataset samples/button_press_001 \
  --dataset-id button_press_001 --version v1 \
  --lake-root runs/lake \
  --phase ads --resume

# A7. v1.6.2 QC contract（universal 适配 demo）
robot-dh qc contract list
robot-dh qc profile \
  --dataset-uri samples/button_press_001 \
  --dataset-family universal \
  --output runs/qc/button_press_001/v1
robot-dh qc contract run \
  --dataset-family universal \
  --dataset-uri samples/button_press_001 \
  --dataset-id button_press_001 --version v1 \
  --output runs/qc/button_press_001/v1 \
  --contract configs/qc/universal.yaml \
  --log-format json

# A8. v1.6.3 ML-ready export（本地 dwd + ads → train/val/test）
robot-dh ml-ready export \
  --input-root runs/lake/dwd \
  --quality-root runs/lake/ads/quality \
  --qc-root runs/qc \
  --output runs/lake/ml-ready/button_press_demo/v1 \
  --dataset-id button_press_demo --version v1 \
  --quality-threshold 0 \
  --split 0.8,0.1,0.1

# A9. v1.6.4 lineage report（无 K8s 仍能跑：用 SQLite 空 DB 出空 report）
robot-dh lineage report \
  --workflow-name local-smoke \
  --namespace robot-dh \
  --output runs/lake/lineage/reports/local-smoke.json

# A10. FastAPI（v1.6 新增 /qc/* /assets/profiles /ml-ready /workflows）
uvicorn robot_dh.api.main:app --host 0.0.0.0 --port 8000 &
curl -s http://127.0.0.1:8000/health
curl -s http://127.0.0.1:8000/qc/contracts
curl -s http://127.0.0.1:8000/ml-ready
kill %1
```

预期：每条命令都返回 0，A2 看到 `244 passed, 14 skipped`，A4/A5 产物在 `runs/lake/ods/...`，A8 在 `runs/lake/ml-ready/...` 看到 `train.parquet / val.parquet / test.parquet / dataset_card.json / dataset_card.md / lineage.json / _manifest.json`。

---

### B. WSL 公网直连（与云端 PG / MinIO / Redis 联调）

前提：远端 PG（含 v1.5 + v1.6 schema）/ MinIO（含 `robot-datasets` / `robot-lake`）/ Redis 已就绪，白名单已放行 WSL host。

```bash
# B1. 加载平台 env（chmod 600，禁止入 git）
ls -l client/robot-dh-platform.env   # 期望 -rw------- 600
set -a; source client/robot-dh-platform.env; set +a

# B2. 健康检查（含 lake）
robot-dh infra doctor --check db,s3,redis,lake

# B3. 多源 QC contract（DROID / robomimic / Bridge）
robot-dh qc contract run \
  --dataset-family droid \
  --dataset-uri  s3://robot-datasets/raw/droid_lerobot_scale30/v1 \
  --dataset-id   droid_lerobot_scale30 --version v1 \
  --output       s3://robot-lake/qc/droid_lerobot_scale30/v1 \
  --contract     configs/qc/droid_contract.yaml \
  --log-format json

robot-dh qc contract run \
  --dataset-family robomimic \
  --dataset-uri  s3://robot-datasets/raw/robomimic_scale30/v1 \
  --dataset-id   robomimic_scale30 --version v1 \
  --output       s3://robot-lake/qc/robomimic_scale30/v1 \
  --contract     configs/qc/robomimic_contract.yaml \
  --log-format json

robot-dh qc contract run \
  --dataset-family bridge \
  --dataset-uri  s3://robot-datasets/raw/bridgedata_v2_scale30/v1 \
  --dataset-id   bridgedata_v2_scale30 --version v1 \
  --output       s3://robot-lake/qc/bridgedata_v2_scale30/v1 \
  --contract     configs/qc/bridge_contract.yaml \
  --log-format json

# B4. partition plan（避免 30GB+ 单分片 normalize 超时）
robot-dh partition plan \
  --dataset s3://robot-datasets/raw/droid_lerobot_scale30/v1 \
  --dataset-id droid_lerobot_scale30 --version v1 \
  --output  s3://robot-lake/tmp/partitions/droid_lerobot_scale30_v1.json \
  --target-partition-size-gb 2

# B5. phase-level normalize（resume + heartbeat）
robot-dh etl run \
  --dataset    s3://robot-datasets/raw/droid_lerobot_scale30/v1 \
  --dataset-id droid_lerobot_scale30 --version v1 \
  --lake-root  s3://robot-lake \
  --phase normalize --resume \
  --heartbeat-interval-sec 30 \
  --log-format json

# B6. ML-ready export（全量三源）
robot-dh ml-ready export \
  --input-root   s3://robot-lake/dwd \
  --quality-root s3://robot-lake/ads/quality \
  --qc-root      s3://robot-lake/qc \
  --output       s3://robot-lake/ml-ready/scale30/v1 \
  --dataset-id scale30 --version v1 \
  --quality-threshold 80 \
  --split 0.8,0.1,0.1 \
  --log-format json

# B7. 控制面查询（FastAPI）
uvicorn robot_dh.api.main:app --host 0.0.0.0 --port 8000 &
curl -s http://127.0.0.1:8000/qc/runs | jq '.[0:2]'
curl -s http://127.0.0.1:8000/ml-ready
curl -s http://127.0.0.1:8000/workflows
kill %1
```

---

### C. kind / K8s Argo 多源 DAG（30GB+ scale）

前提：本地 kind 集群 `robot-dh` 已存在；Argo 已 install；已 build `robot-data-harness:local` 镜像并 `kind load`。

```bash
# C1. 准备 image
make docker-build
make kind-load

# C2. 推平台 secret（不打印凭据）
set -a; source client/robot-dh-platform.env; set +a
./scripts/k8s_create_platform_secret_from_env.sh
# 预期：[OK] secret robot-dh/robot-dh-v1-6-secrets 创建/更新成功

# C3. apply v1.5 + v1.6 全部 Argo 资源
make argo-apply-rbac
make argo-apply-templates           # v1.5 三个 WorkflowTemplate
make argo-apply-platform            # v1.6 三个 WorkflowTemplate + CronWorkflow

# C3.5 v1.6 archiveLogs：把 step pod stdout 归档到 s3://robot-dh-artifacts/argo-logs/
#      详见 docs/v1_6_argo_log_archive_request.md / docs/v1_6_argo_log_archive_handoff.md
#      重跑 C2 后必跑：C2 会 delete+create robot-dh ns 的 secret,
#      argo ns 的同名 secret 不会自动跟着变,这一步把新 access/secret key 同步过去并刷新 ConfigMap
make argo-enable-log-archive
# = argo-sync-log-archive-secret + argo-apply-log-archive + argo-verify-log-archive

# C4. smoke：检查 secret / image / template 是否齐
make platform-smoke

# C5. 提交多源 DAG（推荐 tmux，因 scale30 单次约 8~12h）
make argo-submit-multisource-scale30
# 或：make argo-submit-contract-qc       # 只跑 QC
#     make argo-submit-ml-ready          # 只跑 ML-ready

# C6. 观察
make argo-platform-status
make argo-ui-port-forward                # 浏览器打开 https://localhost:2746
make argo-platform-logs                  # 默认取最新 Workflow 全部 step pod 的 main 容器日志
                                         # 想看 executor/init 噪声：LOG_CONTAINER=wait make argo-platform-logs
make argo-platform-tail                  # follow 模式；WF=<wf-name> 指定，LOG_CONTAINER 同上
kubectl -n robot-dh get workflows.argoproj.io -w

# C7. workflow 结束后把 status 写回 PG（Argo 末尾节点已自动调用，这是手动补救入口）
make argo-sync-latest

# C8. exporter（Prometheus）部署 + 验收
make exporter-docker-build
make exporter-kind-load
make exporter-k8s-apply
make exporter-port-forward &
curl -s http://localhost:9108/healthz | jq         # 含 db_connected / last_scrape_time / last_scrape_error
curl -s http://localhost:9108/metrics | grep robot_dh_qc_contract_runs_total
```

验收对照：
- `s3://robot-lake/qc/<dataset_id>/<v1>/contract_report.json` 三种 family 都生成
- `s3://robot-lake/ml-ready/scale30/v1/{train,val,test}.parquet` 已生成
- PG 表 `qc_contract_runs / workflow_runs / workflow_steps / task_heartbeats / ml_ready_datasets / dataset_partitions / asset_profiles / openlineage_events` 有写入
- `robot-dh-exporter` `/healthz` 返回 `db_connected: true`
- Prometheus 抓 `robot_dh_workflow_steps_total{phase="Succeeded"}` > 0
- `s3://robot-dh-artifacts/argo-logs/robot-dh/<workflow.name>/<pod.name>/main.log` 至少有 1 个对象（`CHECK_OBJECTS=1 MC_ALIAS=local make argo-verify-log-archive` 一键确认）

---

### D. 完整命令一行版（仅本地，最短验收路径）

如果只想"5 行内"证明仓库 v1.6 可跑：

```bash
make setup
make test                                  # 244 passed, 14 skipped
( cd go/robot-dh-exporter && GOPROXY=https://goproxy.cn,direct go test ./... )
make demo-data
robot-dh etl run --dataset samples/button_press_001 --dataset-id button_press_001 --version v1 \
  --lake-root runs/lake --phase normalize --resume
```

> 跑通后可以接 B / C 两段做远端联调。耗时较长的远端命令建议放进 tmux 或 Argo。
