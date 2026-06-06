# v1.9 AI Inference Data Plane（主项目）

> 配套：`docs/model_registry.md`（模型注册 / backend）、`docs/distillation.md`（蒸馏）、
> `docs/inference_data_plane_schema.md`（PG schema）、`docs/autodl_gpu_worker.md`（GPU worker）。

把平台从「ML-ready 数据集」延伸到 AI 数据生产飞轮的关键一环，**不强依赖 GPU / vLLM**：
先用 `mock` / `local_cpu` / `openai_compatible` 后端跑通，AutoDL GPU 只是可插拔后端。

## 1. 数据流

```
ML-ready / DWD parquet
   └─(InferenceInputBuilder)─> InferenceSample[]
        └─(batch + backend.predict_batch)─> InferencePrediction[]
             ├─> predictions.parquet        （全部样本，含失败行）
             ├─> failed_samples.parquet      （失败样本，可 retry）
             ├─> inference_report.json        （吞吐 / 时延 / 错误率）
             └─> _manifest.json
        └─(可选回流 DB)─> inference_jobs / inference_outputs / inference_failures / ai_task_events
   └─(DistillBuilder)─> distillation dataset（train/val/test JSONL + dataset_card）
   └─(benchmark)─> inference_benchmark_runs + benchmark_report.{json,html,csv}
   └─(warehouse build --layers inference)─> dws_inference_job_daily / ads_inference_dashboard
```

## 2. 模块结构

| 包 | 职责 |
| --- | --- |
| `robot_dh.models` | ModelRegistry + ModelSpec + backends（mock/local_cpu/openai_compatible） |
| `robot_dh.inference` | InferenceInputBuilder / runner / batch / outputs / metrics / report / failures / benchmark |
| `robot_dh.distill` | 蒸馏 builder / formats / dataset_card / report |
| `robot_dh.ai_tasks` | 事件（events）/ 状态机（state）/ 持久化（store：JSONL + 可选 PG） |

## 3. CLI 一览

```bash
# 模型注册
robot-dh model register --config configs/model_registry.yaml
robot-dh model list
robot-dh model show --model-id mock-captioner-v1
robot-dh model health --model-id mock-captioner-v1

# 批量推理
robot-dh infer run \
  --input file://$PWD/runs/lake/ml-ready/demo/v1 \
  --model-id mock-captioner-v1 \
  --output file://$PWD/runs/lake/infer/captions/demo/v1 \
  --task-type caption --batch-size 32 --max-workers 4 --limit 100

robot-dh infer list
robot-dh infer show --job-id <job_id>
robot-dh infer report --job-output file://$PWD/runs/lake/infer/captions/demo/v1
robot-dh infer retry --job-output <job_output> --model-id mock-captioner-v1 --output <retry_out>

# benchmark
robot-dh infer benchmark --input <input> --model-id mock-captioner-v1 \
  --output runs/infer_benchmark/mock --concurrency 1,2,4 --batch-size 8,16 --limit 200

# 蒸馏
robot-dh distill build --teacher-output <infer_out> --format instruction_tuning \
  --output <distill_out> --split 0.8,0.1,0.1

# 回流数仓 + 看板
robot-dh warehouse build --layers inference --date $(date -u +%F)
robot-dh warehouse query --table ads_inference_dashboard --output table
robot-dh quality summary --date $(date -u +%F)   # 含 inference_* 指标
```

## 4. predictions.parquet schema

| 列 | 类型 | 说明 |
| --- | --- | --- |
| output_id / job_id / model_id / sample_id | string | 标识 |
| dataset_id / version / episode_id / frame_id | string | 数据集维度 |
| input_uri | string | 单样本输入 |
| prediction_type | string | caption / embedding / anomaly_score |
| prediction_json | string | 结构化预测（JSON 字符串；大向量放摘要 + 外链） |
| confidence / latency_ms | double | 质量 / 时延 |
| token_count | int | LLM token 数（可空） |
| status / error_message | string | OK / FAILED |
| created_at | string | ISO 时间 |

## 5. exit code 约定（infer run）

- 全部成功 → `SUCCEEDED`，exit 0。
- 部分失败但 error_rate ≤ `max_error_rate`（默认 0.5）→ `SUCCEEDED` + WARN，exit 0。
- error_rate 超阈值或 `--fail-fast` 命中 → `FAILED`，exit 1，并登记 dead_letter。

## 6. 本地 vs 远端 vs 无 DB

- 本地 `file://` + SQLite：`make test` / devscale 全程可跑，无需远端。
- 远端 `s3://robot-lake` + PostgreSQL：`ROBOT_DH_DB_URI` / `ROBOT_DH_S3_*` 指向云端即回流。
- DB 不可用：仍产出全部本地产物（predictions / report / distill），PG 写入静默降级（best-effort）。

## 7. 面向 AI Infra 数据系统岗位的能力映射

- 模型注册表 + backend 抽象 = 推理服务的「控制面元数据」。
- 批量推理 + 失败/死信/事件 = 离线推理「数据面 + 可观测」。
- 蒸馏数据集 = pseudo-label 训练数据生产闭环。
- benchmark + DWS/ADS 回流 = 推理吞吐 / 成本 / SLA 运营。
