# robot-dh-exporter

`robot-dh-exporter` 是 robot-data-harness v1.5 的 Prometheus exporter。

它独立于 Python 项目之外：仅通过 `ROBOT_DH_DB_URI` 读 PostgreSQL，把 v1.4 / v1.5 元数据
（datasets / lake_assets / etl_jobs / etl_perf_runs / etl_shards / benchmark_runs / benchmark_cases /
runtime_events / quality_snapshots / argo_workflow_runs）转成 Prometheus 指标。

## 为什么用 Go

- 小型常驻 exporter；启动快、内存占用低。
- Prometheus 生态原生：`prometheus/client_golang` + `pgx/v5`。
- 与 Python 数据流水线解耦，独立部署、独立升级。

## 指标

| Metric | 类型 | 标签 | 来源表 |
| --- | --- | --- | --- |
| `robot_dh_datasets_total` | Gauge | — | `datasets` |
| `robot_dh_lake_assets_total` | Gauge | `layer` | `lake_assets` |
| `robot_dh_etl_jobs_total` | Gauge | `status,job_type` | `etl_jobs` |
| `robot_dh_etl_failures_total` | Gauge | `phase` | `etl_perf_runs` (FAIL) |
| `robot_dh_etl_duration_seconds` | Gauge | `phase` | `etl_perf_runs` |
| `robot_dh_etl_input_bytes` | Gauge | `phase` | `etl_perf_runs` |
| `robot_dh_etl_output_bytes` | Gauge | `phase` | `etl_perf_runs` |
| `robot_dh_benchmark_cases_total` | Gauge | `passed` | `benchmark_cases` |
| `robot_dh_argo_workflows_total` | Gauge | `status` | `argo_workflow_runs` |
| `robot_dh_quality_score_avg` | Gauge | — | `quality_snapshots` |
| `robot_dh_runtime_events_total` | Gauge | `event_type` | `runtime_events` |
| `robot_dh_exporter_up` | Gauge | — | exporter 自己 |
| `robot_dh_exporter_last_scrape_timestamp_seconds` | Gauge | — | exporter 自己 |
| `robot_dh_exporter_scrape_duration_seconds` | Gauge | — | exporter 自己 |

未建表 / 暂时无权限时不 panic：对应 metric 退为 0，`robot_dh_exporter_up` 置 0。

## 端点

- `GET /metrics`：Prometheus 抓取入口。
- `GET /healthz`：liveness/readiness 用，返回 `{"status":"ok"}`。

## 环境变量

| 变量 | 必填 | 默认 |
| --- | --- | --- |
| `ROBOT_DH_DB_URI` | ✅ | — （示例：`postgresql://user:pw@host:5432/robot_dh?sslmode=disable`） |
| `EXPORTER_ADDR` | ❌ | `:9108` |
| `SCRAPE_INTERVAL_SEC` | ❌ | `30` |
| `LOG_LEVEL` | ❌ | `info`（debug/info/warn/error） |

启动日志只打印 redacted DSN（密码被替换为 `***`），不会回显凭据。

## 本地运行

```
cd go/robot-dh-exporter
go test ./...

export ROBOT_DH_DB_URI='postgresql://user:pw@host:5432/robot_dh?sslmode=disable'
go run ./
# 另开终端
curl http://localhost:9108/metrics | head
```

## Docker

```
make exporter-docker-build       # 构建 image: robot-dh-exporter:local
kind load docker-image robot-dh-exporter:local --name robot-dh
```

## K8s

```
make exporter-k8s-apply
make exporter-port-forward       # localhost:9108 -> exporter
curl http://localhost:9108/metrics | grep robot_dh
```

可选：把 `k8s/servicemonitor.example.yaml` apply 到带 Prometheus Operator 的集群。

## 常见故障

| 现象 | 排查 |
| --- | --- |
| 启动报 `ROBOT_DH_DB_URI is required` | Secret 未注入；检查 `robot-dh-v1-5-secrets` 是否存在 |
| `robot_dh_exporter_up=0` | DB 连接或权限失败；查 stderr JSON log 中的 "scrape failed" 行 |
| 某些 metric 不出现 | 对应表未建（如 `etl_perf_runs`）；先在 infra 项目 apply v1.5 schema |
| Pod 连不到 Postgres | 网络 / pg_hba.conf；不要把 `127.0.0.1` SSH tunnel 写到 Secret |
| OOMKilled | 调高 `resources.limits.memory`（默认 256Mi 一般够用） |
