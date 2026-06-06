# AutoDL 推理 Worker（可选，v1.9）

pull-based GPU 推理 worker：从腾讯云 PostgreSQL 拉 `inference_jobs`（`status=QUEUED`），
用本机 vLLM（OpenAI-compatible endpoint）执行推理，把 `predictions.parquet` 写回 MinIO，
并更新 `inference_jobs` 状态。**不**要求你现在就租 AutoDL；不租也能用主项目 mock / local_cpu
后端把整条飞轮跑通（见仓库根 README 的 v1.9 章节）。

## 角色边界（重要）

- AutoDL 实例**只**作为 GPU inference worker。
- AutoDL **不**承载：K8s / Argo、PostgreSQL、MinIO、Redis。这些留在腾讯云 `robot-dh-infra`。
- worker 只需"出站"连接（PG / MinIO 公网端口 + 本机 vLLM），无需任何入站端口与固定公网 IP。
- 不在 AutoDL 容器里跑 Docker-in-Docker；重要输出必须写回 MinIO，临时盘只当缓存。

## 文件

| 文件 | 作用 |
| --- | --- |
| `worker.py` | pull-based worker：claim / 执行 / 回写；`--dry-run` 只打印将处理的 job |
| `config.example.env` | 环境变量模板（PG / MinIO / vLLM endpoint） |
| `run_worker.sh` | 启动 worker 的包装脚本 |
| `run_vllm_example.sh` | 在 AutoDL GPU 容器内起 vLLM endpoint 的示例 |
| `requirements.txt` | worker 依赖（推荐直接 `pip install -e` 主项目） |

## 快速开始

```bash
# 1) 安装主项目（提供 robot_dh）
pip install -e /path/to/robot-data-harness

# 2) 配置环境（不要提交真实 config.env）
cp config.example.env config.env
$EDITOR config.env
set -a; source config.env; set +a

# 3) 本地 dry-run：只打印将处理哪些 job，不访问 GPU、不改状态
./run_worker.sh --dry-run --max-jobs 1

# 4)（AutoDL GPU 容器内）起 vLLM endpoint
./run_vllm_example.sh

# 5) 真正执行：认领 QUEUED job 并跑推理
./run_worker.sh --model-id openai-compatible-chat-v1 --max-jobs 4
```

## 从主平台提交 inference job

worker 消费的 job 由主平台产生。两种方式：

- CLI（直接执行，不经 worker）：`robot-dh infer run --backend openai_compatible ...`
- 仅入队（交给 worker 执行）：通过 FastAPI `POST /inference/jobs` 创建 `status=QUEUED` 记录，
  worker 轮询认领。

## 抢占与并发

worker 用 `UPDATE inference_jobs SET status='RUNNING' WHERE job_id=? AND status='QUEUED'`
的原子条件更新认领任务，`rowcount==1` 才算抢到，避免两个 worker 抢同一个 job。

## 常见故障

见 `docs/autodl_gpu_worker.md` 与 `docs/vllm_openai_compatible.md`（endpoint 不通 / API key 错 /
timeout / output 没写回 MinIO / AutoDL 数据丢失）。
