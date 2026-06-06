# v1.9 蒸馏数据集（distillation）

把 teacher 模型的推理结果（pseudo labels）整理成可直接训练 student 的数据集。

## 1. 输入：teacher output

`robot-dh infer run` 产出的 `predictions.parquet`（+ `inference_report.json`）。蒸馏只消费
`status=OK` 的行；`inference_report.json` 用于回填 `teacher_model_id` / `source_inference_job_id`
/ `dataset_id` / `version`。

## 2. 支持格式

| format | 输出每行结构 | 来源字段 |
| --- | --- | --- |
| `instruction_tuning` | `{id, instruction, input, output, teacher_model, metadata}` | caption `prediction_json.text` |
| `caption_sft` | 同上，instruction 固定为 caption 模板 | caption `text` |
| `embedding_pairs` | `{id, sample_id, embedding, dim, teacher_model}` | `prediction_json.embedding` |
| `anomaly_detection` | `{id, sample_id, anomaly_score, label, teacher_model}` | `prediction_json.anomaly_score` |

缺字段的行自动跳过（计入 `num_skipped`）。

## 3. 命令

```bash
robot-dh distill build \
  --teacher-output file://$PWD/runs/lake/infer/captions/demo/v1 \
  --format instruction_tuning \
  --output file://$PWD/runs/lake/distill/demo/v1 \
  --split 0.8,0.1,0.1
```

## 4. 产物

```
<output>/
  train.jsonl  val.jsonl  test.jsonl    # 按 id 的 sha256 稳定分桶，跨次运行可复现
  distill_report.json                   # 计数 / 比例 / 血缘
  dataset_card.md                       # 中文数据卡片
  _manifest.json
```

DB 可用时写一行 `distillation_datasets` + `ai_task_events`（distill_build_started/finished）。

## 5. instruction_tuning JSONL 示例

```json
{
  "id": "demo:v1:e0:0",
  "instruction": "Describe the robot episode.",
  "input": {"sample_id": "demo:v1:e0:0", "dataset_id": "demo", "episode_id": "e0", "input_refs": ["file://.../train.parquet"]},
  "output": "A robot manipulation episode from dataset demo.",
  "teacher_model": "mock-captioner-v1",
  "metadata": {"confidence": 0.71, "prediction_type": "caption"}
}
```

## 6. 切分策略

按 `sha256(id)` 映射到 `[0,1)` 再按 `train/val/test` 比例分桶：同一 `id` 永远落同一 split，
跨次运行 / 跨格式稳定可复现，避免训练集泄漏到验证集。

> 注意：蒸馏标签质量受 teacher 能力限制，下游使用前请结合 `confidence` 与人工抽检。
