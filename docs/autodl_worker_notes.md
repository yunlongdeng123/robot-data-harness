# AutoDL GPU Worker 接入说明（v1.9，前瞻）

> 本文是 v1.9 为 Prompt C「可选 AutoDL GPU Worker / OpenAI-compatible vLLM 接入」预留的设计约定。
> v1.9 schema 与运维脚本**不依赖** AutoDL；AutoDL 只是后续一个**可插拔的推理后端**（`model_registry.backend = autodl_worker` 或 `openai_compatible`）。

## 1. 定位：AutoDL 只做 GPU worker

- AutoDL 实例**只承担推理计算**，是无状态的 GPU worker。
- AutoDL **不**充当以下角色（这些继续留在腾讯云 `robot-dh-infra`）：
  - K8s 控制面 / 节点（kind / Argo 仍在原集群）
  - PostgreSQL 元数据库（`robot-dh-postgres`）
  - MinIO 对象存储 / 数据湖
  - Redis 事件总线 / 任务队列
- 原因：AutoDL 实例生命周期短、网络与磁盘不稳定，不适合承载有状态服务；让它专注「拉任务 → 推理 → 回写结果」。

## 2. 推荐 pull-based worker（而非 push）

推荐 worker 主动从队列**拉**任务，而不是由控制面 push：

```
robot-dh-infra（腾讯云）                         AutoDL GPU 实例
┌───────────────────────────┐                  ┌────────────────────────┐
│ inference_jobs (QUEUED)    │  ① 拉 job        │ pull-based worker       │
│ Redis Streams / 任务表      │◄─────────────────│  - 读 input_uri (S3)    │
│ S3: robot-lake/infer       │                  │  - 本地 GPU / vLLM 推理 │
│ inference_outputs          │  ② 回写结果       │  - 写 output_uri (S3)   │
│ inference_failures         │◄─────────────────│  - 回写 outputs / events│
│ ai_task_events             │                  └────────────────────────┘
└───────────────────────────┘
```

pull-based 的好处：

- AutoDL 只需**出站**连接（连公网 PG / MinIO / Redis 端口即可），无需公网入站端口与固定 IP。
- 实例随时可加可减：扩容就是多起几个 worker，缩容直接关机，控制面无需感知。
- 断点友好：worker 崩溃后，未 ack 的 job 留在 `QUEUED` / `RETRYING`，由新 worker 接管；失败样本进 `inference_failures` → `dead_letter_tasks`。

worker 主循环约定：

1. 认领一个 `inference_jobs`（`QUEUED → RUNNING`，写 `started_at`）。
2. 按 `batch_size` 从 `input_uri` 读输入；逐 batch 调用本地推理。
3. 每样本写 `inference_outputs`，失败写 `inference_failures`，增量回写 `processed_samples / failed_samples`，关键节点写 `ai_task_events`。
4. 收尾置 `SUCCEEDED / FAILED`，写 `metrics_json` / `finished_at` / `duration_sec`。

## 3. 不要把核心数据放在 AutoDL 临时盘

- AutoDL 的系统盘 / 数据盘在实例释放后可能丢失，**只当临时缓存用**。
- 约定：
  - **输入**：从 `s3://robot-lake/ml-ready/...`（或 dwd）拉到本地临时目录推理，用完即删。
  - **输出**：推理结果**立即**写回 `ROBOT_DH_INFER_OUTPUT_ROOT`（`s3://robot-lake/infer`）/ `ROBOT_DH_DISTILL_OUTPUT_ROOT`（`s3://robot-lake/distill`），不要只留在本地盘。
  - 元数据（job / output / event）一律写回腾讯云 PostgreSQL，AutoDL 本地不留权威状态。
- 模型权重可缓存到本地盘加速冷启动，但**真源**仍在对象存储 / HuggingFace；缓存丢失能重新拉取。

## 4. 后续接 vLLM / OpenAI-compatible endpoint 的 env

两种接入方式，对应两种 backend：

### 4.1 openai_compatible（推荐，最通用）

在 AutoDL 实例上起 vLLM（或任意暴露 OpenAI 协议的服务），worker 通过下列 env 调用（已在 client env / Secret 预留）：

```bash
ROBOT_DH_DEFAULT_INFER_BACKEND=openai_compatible
ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL=http://<autodl-or-tunnel-host>:8000/v1
ROBOT_DH_OPENAI_COMPATIBLE_API_KEY=<token-if-any>   # 无鉴权可留空
```

`model_registry` 对应一行：`backend='openai_compatible'`，`endpoint_url` 填上述 `base_url`，`model_name` 填 vLLM 加载的模型名。

vLLM 启动参考（AutoDL 实例本地）：

```bash
python -m vllm.entrypoints.openai.api_server \
  --model <hf-model-id> --host 0.0.0.0 --port 8000
```

### 4.2 autodl_worker（pull-based）

worker 直接在 AutoDL 本地加载模型（不暴露 endpoint），`model_registry.backend='autodl_worker'`，`endpoint_url` 留空；连接信息走 client env 里的 PG / S3 / Redis（出站）。

### 4.3 安全约定

- `ROBOT_DH_OPENAI_COMPATIBLE_API_KEY` 是敏感值：只进 K8s Secret / `chmod 600` 的真实 env，**不进** `.example`、不打印日志（导出脚本 `49_export_inference_client_env.sh` 已脱敏处理）。
- AutoDL ↔ 腾讯云若需打洞，用反向隧道 / 内网穿透只暴露推理端口，不要把 PG / MinIO / Redis 管理端口暴露到公网。

> 小结：v1.9 先用 `mock` / `local_cpu` / `openai_compatible` 跑通整条飞轮；AutoDL GPU 是 Prompt C 的可插拔增量，接入时只需在 `model_registry` 加一行 + 配 env，**不改 schema、不改运维脚本**。
