# v1.9 AutoDL GPU Worker（可选）

> 概念背景见 `docs/autodl_worker_notes.md`；endpoint 接入见 `docs/vllm_openai_compatible.md`；
> 代码在 `workers/autodl_inference_worker/`。

## 1. AutoDL 角色定位

- AutoDL 实例**只**作为 GPU inference worker。
- **不**承载 K8s / Argo、PostgreSQL、MinIO、Redis（这些留在腾讯云 `robot-dh-infra`）。
- 只需"出站"连接：腾讯云 PG / MinIO 公网端口 + 本机 vLLM endpoint；无需入站端口 / 固定公网 IP。

## 2. 为什么只做 worker（pull-based）

- AutoDL 生命周期短、磁盘 / 网络不稳定，不适合放有状态服务。
- pull-based：worker 主动 `claim` `inference_jobs` 中 `status=QUEUED` 的任务，崩溃后未完成任务
  仍是 QUEUED，由新 worker 接管；扩容就是多起几个 worker，缩容直接关机。
- 抢占用原子条件更新：`UPDATE inference_jobs SET status='RUNNING' WHERE job_id=? AND status='QUEUED'`，
  `rowcount==1` 才算抢到，避免两个 worker 抢同一个 job。

## 3. 配置 env

```bash
cd workers/autodl_inference_worker
cp config.example.env config.env   # 填真实值，不要提交 git
$EDITOR config.env
set -a; source config.env; set +a
```

关键变量：`ROBOT_DH_DB_URI`（腾讯云 PG）、`ROBOT_DH_S3_*`（MinIO）、
`ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL`（本机 vLLM）、`ROBOT_DH_WORKER_MODEL_ID`。

## 4. 启动 worker

```bash
# 安装主项目（提供 robot_dh）
pip install -e /path/to/robot-data-harness

# dry-run：只打印将处理哪些 job，不访问 GPU、不改状态
./run_worker.sh --dry-run --max-jobs 1

# 真正执行
./run_worker.sh --model-id openai-compatible-chat-v1 --max-jobs 4
```

## 5. 从主平台提交 inference job

- 直接执行（不经 worker）：`robot-dh infer run --model-id <openai_compatible 模型> ...`。
- 仅入队交给 worker：FastAPI `POST /inference/jobs` 创建 `status=QUEUED` 记录，worker 轮询认领。

## 6. 查看结果

```bash
robot-dh infer list
robot-dh infer show --job-id <job_id>
robot-dh infer report --job-output s3://robot-lake/infer/<...>
robot-dh warehouse query --table ads_inference_dashboard --output table
```

## 7. 常见故障

| 现象 | 排查 |
| --- | --- |
| worker 启动报缺 env | `set -a; source config.env; set +a`；dry-run 不需 endpoint，真跑需要 base_url |
| 认领不到 job | 确认有 `status=QUEUED` 且 `model_id` 匹配的 inference_jobs |
| 推理全失败 `OPENAI_*` | 见 `docs/vllm_openai_compatible.md` |
| output 没写回 MinIO | 检查 `ROBOT_DH_S3_*`；output_uri 必须是 `s3://robot-lake/...` |
| AutoDL 数据丢失 | 不要把核心数据放临时盘，结果必须立即写回 MinIO；模型权重只当缓存 |
