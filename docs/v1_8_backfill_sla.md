# v1.8 Backfill & SLA

> v1.8 中 backfill 与 SLA 是**轻量化的运营元数据 + CLI 接线**：不实现大型调度器、不与 Argo 强耦合；plan / check 的产物落 PostgreSQL，由人工或 cron 触发执行。

## 1. Backfill plan / task 设计

### 1.1 表结构

| 表 | 主键 | 关键字段 |
|---|---|---|
| `backfill_plans` | `plan_id`（业务生成，推荐 `bf-<ts>-<scope>-<rand>`） | from_date / to_date / dataset_id / version / phase / reason / status / task_count / plan_json |
| `backfill_tasks` | `task_id` | plan_id（**无强 FK**，避免大批量 DELETE 卡死） / dt / phase / recommended_command / status / attempts / last_error |

`recommended_command` 是 v1.8 backfill 的灵魂：每个 task 派生出一条**可直接 `subprocess.run` 的 CLI**，与 v1.7 已有命令对齐：

| phase | 推荐命令 |
|---|---|
| `normalize` / `etl` / `build_features` / `build_ads` | `robot-dh etl run --dataset-id <id> --version <v> --phase <p> --resume` |
| `qc` | `robot-dh qc contract run --dataset-id <id> --version <v> --resume` |
| `ml_ready` | `robot-dh ml-ready export --dataset-id <id> --version <v>` |
| `sla` | `robot-dh sla check --policy configs/sla_policies.yaml` |
| 其它 | 默认 fallback 到 `etl run --phase normalize` |

### 1.2 plan 生成口径

`robot-dh backfill plan --from-date ... --to-date ...` 按下面顺序收集失败 candidates：

1. `fact_etl_run.status ∈ status_filter`（默认 `FAILED, WARN, ERROR, FAIL`）
2. `fact_workflow_step.phase ∈ status_filter`
3. `ads_quality_dashboard.overall_status ∈ status_filter`
4. `sla_checks.status ∈ status_filter`

如果显式传了 `--dataset` + `--version` 但**没**有任何历史失败记录，仍会按 `[from_date..to_date]` 每天补一行（人工补数路径）。

### 1.3 v1.8 不做的事

- **不**实现并发调度器：`backfill run` 默认只打印命令；`--execute` 时单线程 subprocess（v1.8 故意没接线 `max_parallel`）。
- **不**直接提交 Argo Workflow；要走 Argo 时由 `recommended_command` 派生 `argo submit` 的步骤交给操作员。
- **不**做"自动补数"：plan 是手工触发的元数据，避免误触发巨量 ETL。

## 2. SLA policy 设计

### 2.1 配置文件

`configs/sla_policies.yaml` 是真源；`robot-dh sla check` 在每次 `--persist` 时会把 yaml 中的 enabled policy **UPSERT** 写入 `sla_policies` 表（dt 无关）。

```yaml
policies:
  - policy_id: devscale_daily_ready
    policy_name: Devscale Daily Ready
    dataset_pattern: "*dev*"        # fnmatch；与 dataset_family 任一匹配即纳入
    dataset_family: null
    deadline_hour: 23                # 仅做运营展示，引擎不做小时级判定
    required_outputs:                # qc_contract / dwd / ads / ml_ready / raw
      - qc_contract
      - dwd
      - ads
      - ml_ready
    min_qc_pass_rate: 0.8
    min_etl_success_rate: 0.8
    max_failed_workflows: 0
    enabled: true
```

### 2.2 状态判定（在 `sla.py::_evaluate`）

| 触发条件 | 状态 |
|---|---|
| `missing_outputs != []` | FAIL（缺产出比所有阈值优先） |
| `qc_pass_rate < min_qc_pass_rate` 或 `etl_success_rate < min_etl_success_rate` | FAIL |
| `failed_workflows > max_failed_workflows` | FAIL |
| 某个 metric 是 NULL（如当日完全没跑过 QC） | WARN |
| 所有阈值通过 + required_outputs 齐备 | PASS |
| dataset_pattern + dataset_family 全空且没匹配到 dataset | SKIPPED |

`failed_reason` 字段把所有失败原因拼成一行（如 `qc_pass_rate 0.500<0.800; missing_outputs=['ml_ready']`），方便快速排错。

### 2.3 required_outputs 判定细则

| 名字 | 判定 |
|---|---|
| `qc_contract` / `qc` | 当日有 ≥ 1 条 `qc_contract_runs` 且 status ∈ {PASS, WARN} |
| `ml_ready` / `ml-ready` | `ml_ready_datasets.output_uri` 非空 |
| `ads` | `ads_quality_dashboard` 行存在 |
| `dwd` / `dwd_bytes` | `ads_quality_dashboard.dwd_bytes > 0` |
| `ods` / `raw` / `raw_bytes` | `ads_quality_dashboard.raw_bytes > 0` |

## 3. 与 checkpoint / Argo workflow 的结合

backfill 与 v1.6 checkpoint / v1.7 Argo local DAG 是**互补关系**：

| 维度 | v1.6 checkpoint | v1.7 Argo workflow | v1.8 backfill |
|---|---|---|---|
| 触发方 | normalize / build_features 内部自恢复 | Argo template `retryStrategy` | 人工 / cron 手动 |
| 粒度 | 单 dataset 内 partition | 单 workflow step 内 pod | 跨 dataset / 跨日期范围 |
| 写入表 | `dataset_partitions` / `task_heartbeats` | `workflow_runs` / `workflow_steps` | `backfill_plans` / `backfill_tasks` |
| 触发方式 | 自动 | 自动 | `robot-dh backfill plan/run` |
| 适用场景 | normalize 进程崩了恢复 | step pod OOM / Deadline 自动重试 | 上线后批量回补历史失败 |

### 3.1 推荐工作流

1. 看板（`quality report` / `sla report`）发现 dataset X / Y / Z 当日 FAIL。
2. `robot-dh backfill plan --from-date <D-7> --to-date <D> --dataset X --phase normalize --output runs/backfill`。
3. 检查 `runs/backfill/plan.md` 里的 task 列表与 `recommended_command`。
4. （手工）`robot-dh backfill run --plan-id <plan_id> --execute` 或者人工拿 `recommended_command` 提交 Argo。
5. `robot-dh backfill status --plan-id <plan_id>` 看实时状态。
6. 第二天 `robot-dh sla check` 看是否变 PASS。

## 4. FastAPI 端点

```http
GET /backfill/plans                       # 最近 N 条 backfill_plans
GET /backfill/plans/{plan_id}             # 单 plan + 全 task 状态分布
GET /sla/checks?date=2026-05-25           # 当日 sla_checks
```

不暴露 `POST /backfill/plan`——v1.8 故意把"创建 plan"留给 CLI 而非 HTTP，避免误调用。

## 5. 测试覆盖

| 文件 | 覆盖项 |
|---|---|
| `tests/test_backfill_plan.py` | plan 从 fact_etl_run 派生 task / dry-run / json+md 输出 / recommended_command 路由 / status 桶 |
| `tests/test_sla_check.py` | yaml 加载 / PASS / WARN / FAIL / persist policy + check |
| `tests/test_postgres_v1_8_optional.py` | （可选）远端 PG 上 15 张表都存在 + dim_dataset upsert + warehouse query |
