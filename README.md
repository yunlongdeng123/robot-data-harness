## robot-data-harness v1.5

`robot-data-harness` 是一个面向机械臂末端位姿 `eexyzxyzw` 数据集的 Kubernetes-native 数据质量与评测 Harness。它覆盖数据集注册、轨迹校验、质量门禁、报告生成、运行历史沉淀，以及在 WSL / kind / Kubernetes Job 中对远端 PostgreSQL、MinIO、Redis 的统一接入。

**v1.5 在 v1.4 数据湖之上扩展为 scale-out 流水线 + 评测体系**：

- **Sharded ETL**：`robot-dh etl plan / run-shard / merge-summary`，把 30GB+ raw 数据装箱到多个 shard 并行执行
- **Scale Benchmark**：`robot-dh mutate` 注入异常 + `robot-dh benchmark run` 验证 validator quality gate
- **ETL Performance Profiler**：每阶段写入 `etl_perf_runs`（input/output bytes、rows、duration、peak memory）
- **Runtime Events**：`runtime_events_YYYYmmdd.jsonl` + 可选 `runtime_events` 表
- **Argo Workflows**：`robot-dh-scale-etl` / `robot-dh-benchmark` / `robot-dh-build-ads` 三个 WorkflowTemplate + CronWorkflow
- **Prometheus exporter**：独立 Go 进程 `robot-dh-exporter`，把 PostgreSQL 元数据转成 Prometheus 指标

完全向后兼容 v1.4 数据湖（`normalize → build-features → build-ads`）与 v1.3 的 validate / scan / gate / registry / S3 artifact 行为。

当前仓库的运行口径有四条主线：

- 默认兼容模式：本地 SQLite + 本地 artifact + kind PVC demo（与 v1.3 完全一致）
- 远端直连模式：公网白名单直连 PostgreSQL / MinIO / Redis（推荐生产路径）
- v1.4 数据湖 ETL：`normalize → build-features → build-ads` 三段流水，落 `lake_assets` / `etl_jobs` / `lineage_edges` / `dataset_versions` / `quality_snapshots`
- **v1.5 Argo 编排**：Sharded ETL / Benchmark / build-ADS 由 Argo Workflows 调度；写入 `etl_perf_runs` / `etl_shards` / `benchmark_runs` / `benchmark_cases` / `runtime_events`

本仓库已经完成并验证以下链路：

- 本地 `make test`（v1.3 完整 + v1.4 lake + v1.5 sharded ETL / benchmark / profiler / runtime events / API 只读端点；无远端服务时可选测试跳过）
- WSL 公网白名单直连的 `infra doctor` (含 lake)、`lake audit`、`lake list`、`etl run`、`etl scan`、`etl plan / run-shard / merge-summary`、`benchmark run / report`
- kind / K8s remote Secret 模式下的 validator job、scan job、API `/health`、`/infra/health`
- kind 上的 Argo Workflows：`robot-dh-scale-etl` / `robot-dh-benchmark` / `robot-dh-build-ads` + CronWorkflow
- 独立 Go exporter `robot-dh-exporter`（`/metrics` + `/healthz`）

包版本当前为 `0.1.5`。

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
  client/
    robot-dh-public.env
    robot-dh-remote.env
    wsl-export-env.sh
    wsl-export-public-env.sh
    wsl-open-tunnels.sh
    wsl-public-access-checklist.md
    wsl-remote-doctor.sh
  configs/
    button_press.yaml
    datasets.yaml
    default.yaml
    etl_default.yaml
    gate_policy.yaml
    lake.yaml
  docker/
    Dockerfile
  eexyzxyzw/
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
  samples/
  scripts/
    copy_artifacts_from_pvc.sh
    copy_dataset_to_pvc.sh
    wait_job.sh
  src/robot_dh/
    api/
    artifacts/
    gate/
    infra/
    etl/
    lake/
    registry/
    reports/
    validators/
    warehouse/
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
