# v1.4 基础设施运行手册

本手册覆盖机器人数据湖在**应用侧**的检查项。Bucket 创建、PostgreSQL 迁移、防火墙规则与凭据轮换仍由基础设施项目负责。

## 必需环境

执行远端命令前先加载 lake 客户端环境：

```bash
source ~/.config/robot-dh/robot-dh-lake.env
```

必需变量：

- `ROBOT_DH_DB_URI`
- `ROBOT_DH_ARTIFACT_STORE`
- `ROBOT_DH_S3_ENDPOINT_URL`
- `ROBOT_DH_S3_ACCESS_KEY`
- `ROBOT_DH_S3_SECRET_KEY`
- `ROBOT_DH_S3_DATA_BUCKET`
- `ROBOT_DH_S3_ARTIFACT_BUCKET`
- `ROBOT_DH_S3_LAKE_BUCKET`
- `ROBOT_DH_REDIS_URL`

不要提交真实 env 文件。以 `client/robot-dh-lake.env.example` 为模板。

## 本地冒烟测试（smoke test）

```bash
make test
make demo-data
make normalize-demo-local
make features-demo-local
make ads-demo-local
make etl-demo-local
```

以上命令在不依赖 PostgreSQL、MinIO、Redis 的情况下必须全部通过。

## 远端冒烟测试

```bash
robot-dh infra doctor
robot-dh lake audit
robot-dh lake list --layer raw
robot-dh etl scan --root s3://robot-datasets --lake-root s3://robot-lake --limit 3
robot-dh lake list --layer ods
robot-dh lake list --layer dwd
robot-dh build-ads --input-root s3://robot-lake/dwd --output s3://robot-lake/ads/quality
robot-dh lake list --layer ads
```

`etl scan` 通过列举 S3 前缀并查找 `raw/{dataset_id}/{version}` 发现数据集，不假定固定的 dataset ID。

## PostgreSQL 元数据

应用读写以下表：

- `lake_assets`
- `etl_jobs`
- `lineage_edges`
- `dataset_versions`
- `quality_snapshots`

SQLite 测试时由应用在本地建表；PostgreSQL 下应用**不会**创建 lake 表，若缺表会返回明确错误，提示运维先执行基础设施侧迁移。

## 排障

- S3 鉴权或 endpoint 错误：执行 `robot-dh infra doctor --check s3,lake`。
- 缺少 bucket 或前缀：由基础设施创建；`lake init` 仅为探测（probe-only），不负责建桶。
- 缺少 manifest：执行 `robot-dh lake audit --output json`，检查 `manifest_completeness.incomplete`。
- PostgreSQL 缺少 lake 表：先应用基础设施迁移，再重跑 `robot-dh lake audit`。
- K8s Pod 无法访问服务而 WSL 可以：确认 `k8s/secret.yaml` 使用可路由的公网 endpoint，且 Pod 出口 IP 已加入白名单。
