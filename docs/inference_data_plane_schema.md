# robot-dh-infra 推理数据平面 schema 设计（v1.9）

> 对应 migration：`postgres/migrations/007_inference_data_plane.sql`
> 应用账号：`robot_dh_app`（只授予表级 DML，不涉及 SEQUENCE）
> 设计目标：把平台从「ML-ready 数据集」延伸到「模型推理 → 伪标签 / caption / embedding → 蒸馏数据集 → 推理 benchmark → 运营回流」的 AI 数据生产飞轮，**不强依赖 GPU / vLLM**，先支持 mock / CPU / OpenAI-compatible 后端。

## 0. 总览

v1.9 新增 10 张表，与 v1.3~v1.8 既有表完全并存（迁移幂等可重复执行）：

| 表 | 层 | 用途 |
| --- | --- | --- |
| `model_registry` | 注册 | 可调用模型版本注册表（type / backend / I/O schema / 限额） |
| `inference_jobs` | 任务 | 批量推理任务（一份输入数据集 → 一次批处理） |
| `inference_outputs` | 输出 | 单样本（sample / frame）推理结果 |
| `inference_failures` | 失败 | 失败 / 可重试样本，喂给 retry / dead-letter |
| `distillation_datasets` | 蒸馏 | 由 teacher 推理结果蒸馏出的训练数据集 |
| `inference_benchmark_runs` | 压测 | 推理后端吞吐 / 时延 benchmark |
| `ai_task_events` | 事件 | 统一事件流，为 Kafka / Redis Streams 预留 |
| `dead_letter_tasks` | 死信 | 多次重试仍失败的任务，人工 / 离线补偿 |
| `dws_inference_job_daily` | DWS | 推理任务按天聚合（model × task_type） |
| `ads_inference_dashboard` | ADS | 推理运营看板 / 告警最终层 |

设计约定：

1. 全部主键用 `text`（业务侧生成 key），不引入 `bigserial`，因此**无需** `GRANT ... ON SEQUENCE`。
2. `dws_` / `ads_` 用复合主键 `(dt, model_id, task_type)`，配合 `ON CONFLICT DO UPDATE` 做幂等 UPSERT。
3. `*_json` 列统一 `jsonb`，承载非结构化 schema / 指标 / 配置 / 事件 payload；**大对象（embedding 向量、长文本）不入库**，只在 `prediction_json` 放摘要或外链，原文落 `output_uri`。
4. `model_registry` / `inference_jobs` / `inference_outputs` 由主项目 robot-data-harness 在 model register / infer / distill 阶段写入；infra 仓库只建表，不写业务数据。

## 1. 与既有数据湖 / 数仓的关系

```
raw / ods / dwd / ads / ml-ready（v1.4~v1.8 数据湖）
        │
        ▼  作为推理输入（input_uri 通常指向 ml-ready 或 dwd parquet）
  inference_jobs ──► inference_outputs ──► distillation_datasets
        │                  │                      │
        │                  │                      └─► s3://robot-lake/distill/...
        │                  └─► s3://robot-lake/infer/...（embedding / caption 落盘）
        │
        ├─► inference_failures ──► dead_letter_tasks（重试 / 死信）
        └─► ai_task_events（事件流，对接 Redis Streams / Kafka）

  inference_jobs + inference_outputs ──聚合──► dws_inference_job_daily ──► ads_inference_dashboard
```

- **输入**：`inference_jobs.input_uri` 一般指向 v1.6 的 `ROBOT_DH_ML_READY_ROOT`（`s3://robot-lake/ml-ready/...`）或 dwd parquet。
- **输出**：`ROBOT_DH_INFER_OUTPUT_ROOT`（`s3://robot-lake/infer`）放推理结果大对象；`ROBOT_DH_DISTILL_OUTPUT_ROOT`（`s3://robot-lake/distill`）放蒸馏训练集。
- **回流**：主项目 warehouse builder 把 `inference_jobs` / `inference_outputs` 聚合进 `dws_inference_job_daily` / `ads_inference_dashboard`，与既有 quality / SLA 体系同源。

## 2. 表字段语义

### 2.1 model_registry

一行一个**可调用模型版本**。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `model_id` | text PK | 模型唯一 key，建议 `<name>:<version>` 或 `<name>-<sha>` |
| `model_name` | text not null | 人类可读模型名 |
| `model_type` | text not null | 模型能力类型：`caption` / `embedding` / `anomaly_scorer` / `vlm` / `llm` / `mock` |
| `backend` | text not null | 执行后端：`mock` / `local_cpu` / `openai_compatible` / `autodl_worker` / `http_json` |
| `endpoint_url` | text | http 类后端的调用地址；`mock` / `local_cpu` 可空 |
| `input_schema_json` | jsonb | 输入契约（字段 / dtype / shape），供 infer job 校验 |
| `output_schema_json` | jsonb | 输出契约（prediction 结构），供下游消费 |
| `max_batch_size` | int | 单批最大样本数，超出由 worker 自行切批 |
| `timeout_sec` | int | 单次调用超时 |
| `status` | text not null default `ACTIVE` | `ACTIVE` / `INACTIVE` / `DEPRECATED` |
| `tags_json` | jsonb | 自由标签（modality / language / 任务族等） |

索引：`(model_type)`、`(backend)`、`(status)`。

### 2.2 inference_jobs

一行一个**批量推理任务**（对一份输入做 caption / embedding / 打分）。

关键字段：

- 输入输出：`input_uri` / `output_uri`（均 not null），`input_format` / `output_format`（如 `parquet` / `jsonl`）。
- 数据集维度：`dataset_id` / `version` / `dataset_family`，与数据湖对齐，方便和 QC / 数仓联查。
- 调度：`task_type`（not null，如 `caption` / `embedding` / `anomaly`）、`priority`、`batch_size`、`max_workers`。
- 进度：`total_samples` / `processed_samples` / `failed_samples`（worker 增量回写）。
- 时序：`started_at` / `finished_at` / `duration_sec`。
- 诊断：`error_message`、`config_json`（批处理参数）、`metrics_json`（收尾态聚合指标）。

`status` 取值（状态机）：

```
CREATED ─► QUEUED ─► RUNNING ─► SUCCEEDED
                       │  │
                       │  └─► FAILED ─► RETRYING ─► RUNNING
                       │                    └─► DEAD_LETTER
                       └─► CANCELLED
```

索引：`(status, created_at)`、`(model_id, status)`、`(dataset_id, version)`、`(task_type, status)`。

### 2.3 inference_outputs

一行一个**样本（sample / frame）的预测结果**。

| 字段 | 说明 |
| --- | --- |
| `output_id` (PK) / `job_id` / `model_id` | 关联任务与模型 |
| `sample_id` / `episode_id` / `frame_id` | 样本定位，episode / frame 与机器人数据对齐 |
| `dataset_id` / `version` | 数据集维度 |
| `input_uri` / `output_uri` | 单样本输入与落盘大对象地址 |
| `prediction_type` | 预测类型（与 `model_type` 呼应） |
| `prediction_json` | 结构化预测**摘要**（caption 文本 / score / embedding 维度 + 外链） |
| `confidence` / `latency_ms` / `token_count` | 质量与开销指标 |
| `status` / `error_message` | 单样本级状态 |

> 约定：embedding 等大向量写 `output_uri`（parquet / npy），`prediction_json` 只放维度、范数、外链，避免把大向量塞进 jsonb 撑爆库。

索引：`(job_id)`、`(model_id, created_at)`、`(dataset_id, version)`、`(prediction_type, status)`。

### 2.4 inference_failures

一行一个**失败样本**，供 retry / dead-letter 消费。

- `retryable`（默认 true）：可重试样本进重试队列；`false` 的搬到 `dead_letter_tasks`。
- `attempt`：已重试次数。
- `error_type`：根因聚合维度（`timeout` / `oom` / `schema` / `rate_limit` / `backend_5xx` 等）。

索引：`(job_id)`、`(error_type)`、`(retryable)`。

### 2.5 distillation_datasets

一行一个**由 teacher 模型推理结果蒸馏出的训练数据集**。

| 字段 | 说明 |
| --- | --- |
| `distill_id` (PK) | 蒸馏数据集 key |
| `dataset_id` / `version` | 源数据集维度 |
| `source_inference_job_id` | 指向产出 pseudo label 的 `inference_jobs.job_id`，做血缘回溯 |
| `teacher_model_id` | teacher 模型 |
| `distill_format` (not null) | 蒸馏格式：`instruction_tuning` / `caption_sft` / `embedding_pairs` / `anomaly_detection` |
| `output_uri` (not null) | 蒸馏集根目录（`s3://robot-lake/distill/...`） |
| `train_uri` / `val_uri` / `test_uri` | 切分后的分片根 |
| `dataset_card_uri` | dataset card（json / md） |
| `num_train` / `num_val` / `num_test` | 各切分样本数 |
| `status` (not null) | `CREATED` / `BUILDING` / `READY` / `FAILED` |
| `metrics_json` | 蒸馏质量指标（label 分布 / 过滤率等） |

索引：`(dataset_id, version)`、`(teacher_model_id)`、`(status)`。

### 2.6 inference_benchmark_runs

一行一次**推理后端压测**（固定 workload + concurrency + batch_size）。

核心指标：`samples_per_sec`、`p50/p95/p99_latency_ms`、`error_rate`；`cost_estimate_json` 预留 GPU 时长 / token 成本（AutoDL / OpenAI-compatible 接入后填）。

索引：`(model_id, created_at)`、`(backend, status)`、`(workload_name)`。

### 2.7 ai_task_events / 2.8 dead_letter_tasks

- `ai_task_events`：统一事件表，`event_type` 示例 `JOB_CREATED` / `JOB_STARTED` / `SAMPLE_DONE` / `JOB_SUCCEEDED` / `JOB_FAILED` / `DISTILL_BUILT`；`payload_json` 存事件原文。为后续把事件流接到 **Redis Streams / Kafka** 做准备（先写库，后续可改为双写）。
- `dead_letter_tasks`：多次重试仍失败、或显式判定不可重试的任务落到这里，`task_type` 示例 `inference_job` / `inference_sample` / `distill_build`，供人工排查或离线补偿。

### 2.9 dws_inference_job_daily / ads_inference_dashboard

由主项目 warehouse builder 从 `inference_jobs` / `inference_outputs` 聚合：

- `dws_inference_job_daily`：按 `(dt, model_id, task_type)` 聚合 job_count / success_rate / total_samples / samples_per_sec / avg & p95 latency / error_rate。
- `ads_inference_dashboard`：在 DWS 之上按阈值判定 `overall_status` / `alert_level` / `alert_reason` / `top_error_type`，面向看板与告警，与 v1.8 `ads_quality_dashboard` 口径同源。

两表均复合主键 + UPSERT，重复 build 当天幂等覆盖。

## 3. 推理后端（backend）说明

| backend | 是否需 GPU | 是否需 endpoint | 典型用途 |
| --- | --- | --- | --- |
| `mock` | 否 | 否 | 链路联调 / CI / 单测；返回确定性假结果，验证写库与聚合 |
| `local_cpu` | 否 | 否 | 本地小模型（CPU 推理，如句向量 / 规则打分），无外部依赖 |
| `openai_compatible` | 否（远端 GPU 由对端承担） | 是（`ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL`） | 对接 vLLM / 任意 OpenAI 协议 endpoint，需 `base_url` + 可选 `api_key` |
| `autodl_worker` | 是（AutoDL GPU） | 否（pull-based） | AutoDL GPU 实例作为**纯推理 worker**，从任务队列拉 job，不承载 DB / MinIO（见 `docs/autodl_worker_notes.md`） |
| `http_json` | 取决于对端 | 是 | 通用 HTTP+JSON 自定义推理服务 |

> v1.9 只要求 `mock` / `local_cpu` / `openai_compatible` 可跑通；`autodl_worker` 在 Prompt C 作为**可插拔后端**接入，不改动本 schema。

## 4. 从 ML-ready dataset 生成 inference outputs（数据流）

以「给一份 ML-ready 数据集生成 caption 伪标签」为例：

1. **注册模型**：向 `model_registry` 插入一行（`model_type='caption'`，`backend='mock'` 或 `openai_compatible`）。
2. **建任务**：向 `inference_jobs` 插入一行：
   - `input_uri = s3://robot-lake/ml-ready/<dataset_id>/<version>/train/*.parquet`
   - `output_uri = s3://robot-lake/infer/<job_id>/`
   - `task_type='caption'`，`status='CREATED'`，记 `total_samples`。
3. **跑推理**：worker 按 `batch_size` 读输入，逐 batch 调用 backend：
   - 每个样本写一行 `inference_outputs`（`prediction_json` 放 caption 文本，大对象落 `output_uri`）。
   - 失败样本写 `inference_failures`；增量回写 `inference_jobs.processed_samples / failed_samples`。
   - 关键节点写 `ai_task_events`。
4. **收尾**：任务结束置 `inference_jobs.status = SUCCEEDED / FAILED`，写 `metrics_json`、`finished_at`、`duration_sec`。
5. **蒸馏**（可选）：把高置信 `inference_outputs` 组织成训练集，写 `distillation_datasets`（`distill_format='caption_sft'`，`source_inference_job_id=<job_id>`），落 `train/val/test_uri`。
6. **回流**：warehouse build 把当天推理聚合进 `dws_inference_job_daily` / `ads_inference_dashboard`，纳入 quality / SLA 看板。

## 5. 索引说明

索引命名沿用 `idx_<table>_<cols>`，全部 `CREATE INDEX IF NOT EXISTS`，覆盖三类高频查询：

- **运营列表**：按 `status` / `created_at` 翻任务、按 `model_id` 看单模型历史。
- **数据集联查**：按 `(dataset_id, version)` 与数据湖 / 数仓 join。
- **根因聚合**：`inference_failures(error_type)`、`ads_inference_dashboard(alert_level, dt)`。
