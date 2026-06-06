# v1.9 vLLM / OpenAI-compatible endpoint 接入

## 1. 在 AutoDL GPU 容器内起 vLLM

```bash
cd workers/autodl_inference_worker
./run_vllm_example.sh        # 默认 Qwen/Qwen2.5-0.5B-Instruct, 0.0.0.0:8000
```

等价命令：

```bash
python -m vllm.entrypoints.openai.api_server \
  --model Qwen/Qwen2.5-0.5B-Instruct --host 0.0.0.0 --port 8000
```

注意：

- 这是 AutoDL GPU 容器内运行示例，无 GPU 环境不保证可运行。
- 不要在普通 AutoDL 容器里再跑 Docker（Docker-in-Docker）。
- 不要把核心数据放 AutoDL 临时盘；重要输出必须写回 MinIO。
- 可用 SSH tunnel 或安全组白名单访问 endpoint，不要把 PG / MinIO 管理端口暴露公网。

## 2. model registry 配置

注册一个 `backend=openai_compatible` 的模型；`endpoint_url` 留空时从 env 读：

```bash
export ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL=http://<autodl-host>:8000/v1
export ROBOT_DH_OPENAI_COMPATIBLE_MODEL=Qwen/Qwen2.5-0.5B-Instruct
# export ROBOT_DH_OPENAI_COMPATIBLE_API_KEY=...   # 无鉴权可不设

robot-dh model register \
  --model-id openai-compatible-chat-v1 \
  --model-name "OpenAI Compatible Chat" \
  --model-type llm --backend openai_compatible
robot-dh model health --model-id openai-compatible-chat-v1
```

## 3. robot-dh infer run 指定该 backend

`infer run` 通过 `--model-id` 选择模型，backend 由模型决定：

```bash
robot-dh infer run \
  --input file://$PWD/runs/lake/ml-ready/demo/v1 \
  --model-id openai-compatible-chat-v1 \
  --output file://$PWD/runs/lake/infer/vllm/demo/v1 \
  --limit 10
```

## 4. 常见故障

| error_type / 现象 | 根因 | 处理 |
| --- | --- | --- |
| `OPENAI_ENDPOINT_UNAVAILABLE` | base_url 未配置 / endpoint 不通 | 确认 vLLM 已起、`ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL` 正确、网络可达 |
| `OPENAI_BAD_RESPONSE`（401/403） | API key 错误 | 核对 `ROBOT_DH_OPENAI_COMPATIBLE_API_KEY` |
| `OPENAI_TIMEOUT` | endpoint 过载 / 超时太短 | 调大 `--timeout-sec` / `--retry`；降并发 |
| `OPENAI_BAD_RESPONSE`（缺字段） | 响应不符合 OpenAI schema | 确认 endpoint 真为 OpenAI-compatible（/chat/completions、/embeddings） |
| output 没写回 MinIO | `ROBOT_DH_S3_*` 未配 / output 非 s3:// | 配 MinIO 凭据，output 用 `s3://robot-lake/...` |
| AutoDL 数据丢失 | 结果只留在临时盘 | 推理结果必须立即写回 MinIO，临时盘只当缓存 |
