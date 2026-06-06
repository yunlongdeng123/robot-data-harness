# v1.9 模型注册表与 backend 抽象

## 1. ModelSpec

`robot_dh.models.ModelSpec` 对应 `model_registry` 一行：

| 字段 | 说明 |
| --- | --- |
| model_id | 唯一 key，建议 `<name>-v1` |
| model_name | 人类可读名 |
| model_type | `caption` / `embedding` / `anomaly_scorer` / `vlm` / `llm` / `mock` |
| backend | `mock` / `local_cpu` / `openai_compatible` / `autodl_worker` / `http_json` |
| endpoint_url | http 类后端调用地址（mock/local_cpu 可空） |
| input_schema / output_schema | I/O 契约（dict） |
| max_batch_size / timeout_sec | 批大小 / 超时 |
| status | ACTIVE / DISABLED / INACTIVE / DEPRECATED |
| tags | 自由标签 |

## 2. 存储后端：PG 优先，本地 JSON 回退

`ModelRegistry`：

- DB 可用（`ROBOT_DH_DB_URI`）→ 读写 `model_registry` 表。
- DB 不可用 / `--local-only` → 读写 `.robot_dh/model_registry.json`。
- `register_from_config(configs/model_registry.yaml)` 批量初始化默认模型。
- **不打印任何 secret**；`api_key` 永不入库 / 不回显（仅走 env / Secret）。

```bash
robot-dh model register --model-id mock-captioner-v1 --model-name "Mock Captioner" \
  --model-type caption --backend mock --max-batch-size 32
robot-dh model register --config configs/model_registry.yaml
robot-dh model list
robot-dh model show --model-id mock-captioner-v1
robot-dh model health --model-id mock-captioner-v1
```

## 3. Backend 抽象

`BaseModelBackend`：

```python
class BaseModelBackend:
    def health(self, model: ModelSpec) -> BackendHealth: ...
    def predict_batch(self, samples: list[InferenceSample],
                      model: ModelSpec, config: dict) -> list[InferencePrediction]: ...
```

`get_backend(model)` 按 `model.backend` 分发。单样本失败不抛异常，返回 `status=FAILED` 的
`InferencePrediction`，由 runner 决定是否 fail-fast。

| backend | GPU | endpoint | 说明 |
| --- | --- | --- | --- |
| mock | 否 | 否 | 确定性假结果，CI / 联调 |
| local_cpu | 否 | 否 | 纯标准库：anomaly 规则打分 / feature-hashing embedding |
| openai_compatible | 否（远端 GPU 由对端承担） | 是 | 调 OpenAI 协议 /chat/completions、/embeddings |
| autodl_worker | 是 | 否 | 由 `workers/autodl_inference_worker` 执行，主项目不直接调用 |
| http_json | 视对端 | 是 | 预留通用 HTTP+JSON |

`model health`：mock / local_cpu 直接 PASS；openai_compatible 探测 `{base_url}/models`，
不可达返回 FAIL（错误码 `OPENAI_ENDPOINT_UNAVAILABLE` 等）。

## 4. OpenAI-compatible 配置

env（也可写进 model_registry 的 `endpoint_url`）：

```bash
export ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL=http://<host>:8000/v1
export ROBOT_DH_OPENAI_COMPATIBLE_API_KEY=<token-if-any>   # 可空
export ROBOT_DH_OPENAI_COMPATIBLE_MODEL=Qwen/Qwen2.5-0.5B-Instruct
```

失败分类（写入 `inference_failures.error_type`）：

- `OPENAI_ENDPOINT_UNAVAILABLE`：base_url 未配置 / 连接被拒（可重试）
- `OPENAI_TIMEOUT`：请求超时（可重试）
- `OPENAI_BAD_RESPONSE`：HTTP 非 2xx / JSON 解析失败 / 缺字段（默认不重试）

> 实现仅用标准库 `urllib`，不引入 httpx / requests；测试用 monkeypatch 替换
> `robot_dh.models.backends.openai_compatible._http_post_json`，不依赖真实 endpoint。
