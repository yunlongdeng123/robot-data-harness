# v1.8 Quality Ops

> 数据质量运营视角：基于 v1.8 数仓指标层产出**日度 summary、HTML/JSON/CSV 报告、TopN 失败规则、archive log 索引**，回答"昨天数据质量到底好不好、出问题在哪、谁要补数"。

## 1. 报告内容

`robot-dh quality report --date 2026-05-25 --output runs/quality_report` 在指定目录生成 7 个文件：

| 文件 | 内容 |
|---|---|
| `quality_summary.json` | 完整 summary（json 序列化的 `QualitySummary.to_dict()`） |
| `quality_summary.html` | 单页可视化（Jinja2 模板 `quality_summary.html.j2`） |
| `rule_failure_top10.csv` | Top 10 失败规则 (rule_id / contract_id / family / fail / run / fail_rate) |
| `dataset_quality_daily.parquet` (or .csv) | `ads_quality_dashboard` 当日全量（可选 parquet；pyarrow 不可用回退 csv） |
| `workflow_sla_summary.csv` | `ads_workflow_ops_dashboard` 当日全量 |
| `abnormal_partitions.csv` | alert_level != OK 的 dataset 行 |
| `archive_log_index.csv` | 当日 `fact_workflow_step.archive_log_uri` 去重列表 |

`QualitySummary` 主体字段（也是 FastAPI `GET /quality/summary?date=...` 的响应）：

```python
@dataclass
class QualitySummary:
    dt: str
    dataset_count: int
    qc_pass_rate: float | None       # ads_quality_dashboard.qc_pass_rate 均值
    etl_success_rate: float | None
    workflow_success_rate: float | None
    top_failed_rules: list[TopFailedRule]
    p95_step_duration_sec: float | None
    stale_heartbeat_count: int
    ml_ready_rows: int
    raw_bytes: int
    dwd_bytes: int
    alert_level: str                 # OK / WARN / CRITICAL（由所有 ads 行的 alert_level 聚合）
    archive_log_uris: list[str]
    workflow_ops: list[dict]         # 每行对应 ads_workflow_ops_dashboard 一行
    dashboards: list[dict]           # 每行对应 ads_quality_dashboard 一行
```

## 2. TopN 失败规则口径

`top_failed_rules` 从 `dws_rule_failure_daily` 查询：

```sql
SELECT * FROM dws_rule_failure_daily
WHERE dt = :target AND fail_count > 0
ORDER BY fail_count DESC, rule_id ASC
LIMIT 10;
```

- `fail_count` 是当日 (dataset_family, contract_id, rule_id, severity) 维度下 `status='FAIL'` 的 fact_qc_rule_result 数量。
- `fail_rate = fail_count / run_count`。

> ❗ 注意：`fact_qc_rule_result` 里的 `contract_status` 行是**整次 QC contract run 的状态汇总**（rule_id='contract_status'），在 dws_rule_failure 聚合时被显式过滤掉，避免与具体规则混淆。

## 3. 数据质量运营视角

### 3.1 一日健康度自检

```bash
robot-dh quality summary --date 2026-05-25 --output table
# 期望输出：
#   alert_level=OK
#   qc_pass_rate>=0.95, etl_success_rate>=0.95, workflow_success_rate>=0.9
#   top_failed_rules=[]（或非 critical 的 warn 规则）
#   stale_heartbeat_count=0
```

若 `alert_level=CRITICAL`（v1.8 内部映射：`qc_pass_rate<0.8 OR etl_success_rate<0.8`），需要进入下一步定位。

### 3.2 定位失败规则

```bash
robot-dh warehouse query --table dws_rule_failure_daily \
    --where "dt='2026-05-25'" --order-by "fail_count DESC" --limit 10 \
    --output table
```

每条规则对应到 `fact_qc_rule_result` 行：

```bash
robot-dh warehouse query --table fact_qc_rule_result \
    --where "dt='2026-05-25' AND rule_id='row_count_min' AND status='FAIL'" \
    --output json | head -30
```

可拿到 `run_id` → 拿去看具体 contract run：

```bash
robot-dh warehouse query --table fact_qc_rule_result \
    --where "run_id='qrun-12345'" --output table
```

### 3.3 定位异常 workflow

```bash
robot-dh warehouse query --table ads_workflow_ops_dashboard \
    --where "dt='2026-05-25'" --output table

# 关注 alert_level=CRITICAL 行：
#  - success_rate<0.8        → 整体失败率高
#  - oom_kill                → 有 OOMKilled step
#  - deadline_exceeded       → 有 DeadlineExceeded step
```

`ads_workflow_ops_dashboard.alert_reason` 字段直接给出具体原因。

### 3.4 archive log 索引

`quality report` 生成的 `archive_log_index.csv` 把当日所有 step pod 的 archive log URI 列成一列，可直接 grep s3 路径回查日志：

```bash
mc cp s3/robot-dh-artifacts/argo-logs/robot-dh/wf-xxx/step-yyy/main.log /tmp/main.log
less /tmp/main.log
```

## 4. 空数据兜底

`quality report` / `quality summary` 在**任何 v1.8 表为空**时仍能生成 7 个文件（每个 csv 至少包含表头；HTML 用 `(empty)` 占位）。这与 promptB 第七节"没有数据时也能生成空报告。报告不要报错"对齐。

底层实现：
- `build_quality_summary()` 检测到任一 v1.8 表缺失 → 直接返回 `QualitySummary(dt=target)`（dataset_count=0, alert_level=OK），并打 WARNING 日志。
- `QualityReportRenderer` 即使 rows=[] 也写文件（csv 写 header-only，json 写 `[]`），不抛异常。

## 5. FastAPI 端点

```http
GET /quality/summary?date=2026-05-25     # 返回 QualitySummary.to_dict()
GET /quality/summary                     # 不传 date → 回退 v1.4 quality_snapshots 列表（向后兼容）
GET /quality/report/latest               # 等价于 GET /quality/summary（最近一天）
```

均为只读；DB 不可达返回 503；不在 API 内部触发重 build。

## 6. 与 v1.4 quality 路径的兼容

- v1.4 `WarehouseService.latest_quality_summary` 走 `quality_snapshots` 直查；FastAPI 路径 `GET /quality/summary?limit=50` 保持不变（不传 date 时）；
- v1.8 `build_quality_summary` 走 ads/dws 聚合；FastAPI 路径 `GET /quality/summary?date=YYYY-MM-DD` 触发；
- 同一端点按是否传 `date` 路由到不同实现，**不破坏 v1.4 测试与既有客户端**。
