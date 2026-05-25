# v1.6 自动开发的远端依赖跳过项

> 本文件记录 v1.6 plan A~E 自动开发过程中**因为依赖远端服务/权限而跳过**的步骤。
> 每条都标明：跳过原因、对应文档 / 命令、未来人工执行入口。

---

## 依赖矩阵概览

| 类别 | 依赖项 | 跳过的验收命令 / 操作 |
|---|---|---|
| Postgres DDL | 远端 PG 已经 apply `005_v1_6_robot_platform.sql` | 自动开发期间不会主动 ssh 到云端跑 `scripts/35_pg_apply_v1_6_schema.sh` |
| Postgres rw | `ROBOT_DH_DB_URI` 指向 v1.6 表已建好的 PG 实例 | `tests/test_postgres_v1_6_optional.py` 在未设置 `ROBOT_DH_TEST_POSTGRES_URI` 时 skip |
| 真实凭据 | `client/robot-dh-platform.env` | 未拉取，相关 K8s `kubectl apply` / `argo submit` 全部跳过 |
| Argo 集群 | `kind robot-dh` 集群 + Argo controller | `make argo-apply-v1-6 / argo-submit-multisource-scale30` 等仅做 yaml 静态校验 |
| Go exporter K8s | exporter image 已 push / kind load | `make exporter-docker-build / exporter-kind-load / exporter-k8s-apply` 仅本地 `go test ./... && go build ./...` |
| 真实 S3 数据 | `s3://robot-datasets/raw/{droid,robomimic,bridge}_scale30/v1` | 远端 `robot-dh qc contract run` / `robot-dh ml-ready export --input-root s3://...` 不会自动执行 |

## 跳过项详细列表

### A.1 远端 normalize / partition 验收

- 文档：`docs/v1_6_planA.prmpt.md` 第十一节 “远端”
- 跳过原因：依赖 `s3://robot-datasets/raw/droid_lerobot_scale30/v1` + 远端 PG。
- 替代验证：本地 demo 数据 + sqlite 跑 `make demo-data` + `robot-dh normalize --resume`，覆盖 checkpoint / heartbeat。

### A.2 PG `task_heartbeats / dataset_partitions` 写入

- 真实 PG（`ROBOT_DH_TEST_POSTGRES_URI` + v1.6 schema）下应能 INSERT。
- 跳过：未设置 env，`tests/test_postgres_v1_6_optional.py` 自动 skip。
- 人工恢复：`source client/robot-dh-platform.env && export ROBOT_DH_TEST_POSTGRES_URI=$ROBOT_DH_DB_URI && pytest tests/test_postgres_v1_6_optional.py`.

### B.1 远端 droid / robomimic / bridge contract run

- 文档：`docs/v1_6_planB.prmpt.md` 第十三节
- 跳过：缺远端数据集和 PG。
- 本地等价：`pytest tests/test_qc_*` 用 fake parquet/HDF5 覆盖 contract 决策树。

### C.1 远端 ML-ready export

- 文档：`docs/v1_6_planC.prmpt.md` 第十一节
- 跳过：缺远端 dwd / ads / qc 数据。
- 本地等价：`pytest tests/test_ml_ready_*` 构造 fake dwd/ads parquet。

### D.1 Argo apply / submit / sync

- 文档：`docs/v1_6_planD.prmpt.md` 第十一节
- 跳过：未启动 kind 集群，secret 也未 apply。
- 本地等价：`pytest tests/test_argo_sync_parser.py / test_lineage_report.py` + YAML 静态校验。

### E.1 exporter K8s 部署

- 文档：`docs/v1_6_planE.prmpt.md` 第七节 “K8s”
- 跳过：未 build image、未 load 到 kind。
- 本地等价：`go test ./... && go build ./...`。

## 人工恢复执行清单（顺序）

1. 在云端执行：`ssh robot-dh-server 'cd /home/ubuntu/robot-dh-infra && ./scripts/35_pg_apply_v1_6_schema.sh'`
2. 拉真实 env：`scp robot-dh-server:/home/ubuntu/robot-dh-infra/client/robot-dh-platform.env client/`
3. `chmod 600 client/robot-dh-platform.env`
4. `set -a; source client/robot-dh-platform.env; set +a`
5. `kubectl apply -f client/k8s-platform-secret.example.yaml`
6. `client/k8s-create-platform-secret.example.sh`
7. `make docker-build && make kind-load && make argo-apply-v1-6`
8. `make argo-submit-multisource-scale30`
9. 用完即删：`shred -u client/robot-dh-platform.env`
