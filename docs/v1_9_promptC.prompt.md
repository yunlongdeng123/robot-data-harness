你是资深 AI Infra 工程师、GPU 推理服务工程师。当前 robot-data-harness 已完成 v1.9 AI Inference Data Plane Lite：

- Model Registry
- Mock / local_cpu / openai-compatible backend
- Inference Job
- predictions.parquet
- Distillation Dataset
- Inference Benchmark
- PostgreSQL v1.9 schema
- FastAPI 查询接口

现在新增可选 AutoDL GPU Worker / vLLM 接入脚手架。注意：
1. 不要求用户现在租 AutoDL。
2. 不把 AutoDL 当 K8s / Docker / DB 节点。
3. AutoDL 只作为 GPU inference worker。
4. 不引入复杂部署。
5. 不依赖真实 GPU 测试。

============================================================
一、目标
============================================================

新增：

1. AutoDL pull-based worker 脚本。
2. vLLM OpenAI-compatible endpoint 配置示例。
3. worker 从 PostgreSQL / MinIO 拉 inference job。
4. worker 执行 OpenAI-compatible backend。
5. worker 上传 predictions 到 MinIO。
6. worker 更新 PostgreSQL inference_jobs 状态。
7. 文档说明如何短租 AutoDL 验证。

============================================================
二、目录结构
============================================================

新增：

workers/
  autodl_inference_worker/
    README.md
    worker.py
    config.example.env
    requirements.txt
    run_worker.sh
    run_vllm_example.sh

docs/
  v1_9_autodl_gpu_worker.md
  v1_9_vllm_openai_compatible.md

tests/
  test_autodl_worker_config.py
  test_autodl_worker_dry_run.py

============================================================
三、Worker 行为
============================================================

worker.py：

参数：
  --poll-interval-sec 10
  --max-jobs 1
  --dry-run
  --model-id openai-compatible-chat-v1

环境变量：
  ROBOT_DH_DB_URI
  ROBOT_DH_S3_ENDPOINT_URL
  ROBOT_DH_S3_ACCESS_KEY
  ROBOT_DH_S3_SECRET_KEY
  ROBOT_DH_S3_LAKE_BUCKET
  ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL
  ROBOT_DH_OPENAI_COMPATIBLE_API_KEY
  ROBOT_DH_OPENAI_COMPATIBLE_MODEL

行为：
  1. 查询 inference_jobs 中 status=QUEUED 且 model_id 匹配的任务。
  2. 将任务标记 RUNNING。
  3. 下载或流式读取 input。
  4. 调用 OpenAI-compatible backend。
  5. 写 predictions.parquet 到 output_uri。
  6. 写 inference_report.json。
  7. 更新 inference_jobs 为 SUCCEEDED / FAILED。
  8. 失败写 inference_failures。
  9. 支持 dry-run，只打印将处理哪些 job。

锁：
  - v1.9 可以使用简单 SQL update where status=QUEUED returning 方式抢占任务。
  - 避免两个 worker 抢同一个 job。

============================================================
四、vLLM 示例
============================================================

run_vllm_example.sh：

只提供示例，不保证在无 GPU 环境运行。

内容：
  python -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-0.5B-Instruct \
    --host 0.0.0.0 \
    --port 8000

README 说明：
  - 这是 AutoDL GPU 容器内运行示例。
  - 不要在普通 AutoDL 容器里跑 Docker。
  - 不要把核心数据放 AutoDL 临时盘。
  - 重要输出必须写回 MinIO。
  - 可以用 SSH tunnel 或安全组白名单访问 endpoint。

============================================================
五、测试
============================================================

test_autodl_worker_config.py:
  - env 缺失时错误清晰。
  - dry-run 不访问真实 GPU。

test_autodl_worker_dry_run.py:
  - mock DB 查询。
  - mock backend。
  - 验证状态转换。

不要求真实 AutoDL / GPU / vLLM。

============================================================
六、文档
============================================================

docs/v1_9_autodl_gpu_worker.md：
  - AutoDL 角色定位。
  - 为什么只做 worker。
  - 如何配置 env。
  - 如何启动 worker。
  - 如何从主平台提交 inference job。
  - 如何查看结果。

docs/v1_9_vllm_openai_compatible.md：
  - vLLM OpenAI-compatible endpoint。
  - model registry 配置。
  - robot-dh infer run 如何指定 backend。
  - 常见故障：
      endpoint 不通
      API key 错误
      timeout
      output 没写回 MinIO
      AutoDL 数据丢失

============================================================
七、验收命令
============================================================

本地 dry-run：

cd workers/autodl_inference_worker

python worker.py --dry-run --max-jobs 1

主项目侧：

robot-dh model register \
  --model-id openai-compatible-chat-v1 \
  --model-name "OpenAI Compatible Chat" \
  --model-type llm \
  --backend openai_compatible

robot-dh model health --model-id openai-compatible-chat-v1

如果未来有 AutoDL / vLLM endpoint：

export ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL=http://<autodl-host>:8000/v1
export ROBOT_DH_OPENAI_COMPATIBLE_MODEL=Qwen/Qwen2.5-0.5B-Instruct

robot-dh infer run \
  --input file:///mnt/local-data/robot-dh-local/lake/ml-ready/devscale/v1 \
  --model-id openai-compatible-chat-v1 \
  --output file:///mnt/local-data/robot-dh-local/lake/infer/vllm/devscale/v1 \
  --limit 10

请开始实现。不要依赖真实 GPU。不要用 Docker-in-Docker。不要留 TODO，不要写伪代码。