你是 Go 后端工程师、Kubernetes 运维开发工程师、Prometheus 可观测性工程师。当前项目 robot-data-harness 已完成 v1.5：

- PostgreSQL 保存 registry / lake metadata / etl_perf_runs / benchmark_runs / argo_workflow_runs
- MinIO 保存 robot-datasets / robot-lake / artifacts
- Argo Workflows 调度 scale ETL DAG
- Python CLI 负责数据 ETL 和 benchmark

现在新增一个独立 Go 小模块：

  robot-dh-exporter

目标：
  读取 PostgreSQL 中的 robot-data-harness 元数据，暴露 Prometheus metrics。

不要重写 Python 项目。
不要实现 Operator。
不要直接操作 MinIO 数据。
不要修改 PostgreSQL schema。

============================================================
一、目录结构
============================================================

新增：

go/
  robot-dh-exporter/
    go.mod
    go.sum
    main.go
    internal/
      config/
      db/
      metrics/
      server/
    Dockerfile
    README.md
    k8s/
      deployment.yaml
      service.yaml
      servicemonitor.example.yaml

============================================================
二、配置
============================================================

环境变量：

ROBOT_DH_DB_URI
EXPORTER_ADDR=:9108
SCRAPE_INTERVAL_SEC=30

支持 PostgreSQL DSN：
  postgresql://user:pass@host:5432/robot_dh?sslmode=disable

不要打印密码。

============================================================
三、Metrics
============================================================

暴露：

robot_dh_datasets_total
robot_dh_lake_assets_total{layer}
robot_dh_etl_jobs_total{status,job_type}
robot_dh_etl_failures_total{phase}
robot_dh_etl_duration_seconds{phase}
robot_dh_etl_input_bytes{phase}
robot_dh_etl_output_bytes{phase}
robot_dh_benchmark_cases_total{passed}
robot_dh_argo_workflows_total{status}
robot_dh_quality_score_avg
robot_dh_runtime_events_total{event_type}

实现方式：
1. /metrics endpoint。
2. /healthz endpoint。
3. 定时查询 PostgreSQL，缓存最近一次结果。
4. 查询失败时暴露 exporter_up=0。
5. 查询成功 exporter_up=1。

============================================================
四、SQL 查询
============================================================

读取表：
  datasets
  lake_assets
  etl_jobs
  etl_perf_runs
  benchmark_runs
  benchmark_cases
  argo_workflow_runs
  quality_snapshots
  runtime_events

如果某些表不存在：
  不 panic。
  对对应 metric 输出 0。
  log warning。

============================================================
五、Docker / K8s
============================================================

Dockerfile：
  multi-stage build。
  final image 使用 gcr.io/distroless/static 或 alpine。
  暴露 9108。

K8s:
  deployment.yaml
  service.yaml
  secretRef:
    robot-dh-v1-5-secrets

resources:
  requests:
    cpu: 50m
    memory: 64Mi
  limits:
    cpu: 200m
    memory: 256Mi

securityContext:
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  capabilities drop ALL

============================================================
六、README
============================================================

说明：
1. 为什么这个模块用 Go：
   - 小型常驻 exporter
   - 低资源占用
   - Prometheus 原生生态
2. 如何本地运行。
3. 如何 Docker build。
4. 如何部署到 kind。
5. 如何 curl /metrics。
6. 常见故障：
   - DB URI 错误
   - 表不存在
   - Postgres 网络不通
   - Secret 未注入

============================================================
七、Makefile 集成
============================================================

在根 Makefile 或 go/robot-dh-exporter/Makefile 添加：

make exporter-build
make exporter-docker-build
make exporter-k8s-apply
make exporter-port-forward
make exporter-logs

============================================================
八、验收
============================================================

本地：
  cd go/robot-dh-exporter
  go test ./...
  go run ./main.go

K8s：
  make exporter-docker-build
  kind load docker-image robot-dh-exporter:local --name robot-dh
  make exporter-k8s-apply
  make exporter-port-forward
  curl http://localhost:9108/metrics

请开始实现。保持模块独立，不要污染 Python 包。不要实现 Operator。