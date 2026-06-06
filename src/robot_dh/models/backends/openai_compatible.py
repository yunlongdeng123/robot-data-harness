"""OpenAICompatibleBackend：调用任意 OpenAI 协议 endpoint（vLLM / 兼容服务）。

只用标准库 urllib 发请求，不引入 httpx / requests，避免新依赖。HTTP 调用统一走模块级
``_http_post_json`` / ``_http_get_json``，测试用 monkeypatch 替换，不依赖真实 endpoint。

失败分类（写到 InferencePrediction.error_message 前缀 + inference_failures.error_type）：
- OPENAI_ENDPOINT_UNAVAILABLE  base_url 未配置 / 连接被拒
- OPENAI_TIMEOUT               请求超时
- OPENAI_BAD_RESPONSE          HTTP 非 2xx / JSON 解析失败 / 缺字段
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any

from robot_dh.models.backends.base import BaseModelBackend
from robot_dh.models.schemas import (
    BackendHealth,
    InferencePrediction,
    InferenceSample,
    ModelSpec,
    PREDICTION_FAILED,
    PREDICTION_OK,
)

ERR_ENDPOINT_UNAVAILABLE = "OPENAI_ENDPOINT_UNAVAILABLE"
ERR_TIMEOUT = "OPENAI_TIMEOUT"
ERR_BAD_RESPONSE = "OPENAI_BAD_RESPONSE"


class OpenAIBackendError(RuntimeError):
    """带 error_type 的 backend 调用异常。"""

    def __init__(self, error_type: str, message: str) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.message = message


def _http_post_json(
    url: str,
    payload: dict[str, Any],
    *,
    headers: dict[str, str],
    timeout: float,
) -> dict[str, Any]:
    """POST JSON 并解析 JSON 响应。测试通过 monkeypatch 替换本函数。"""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as err:  # 非 2xx
        raise OpenAIBackendError(ERR_BAD_RESPONSE, f"HTTP {err.code}: {err.reason}") from err
    except TimeoutError as err:
        raise OpenAIBackendError(ERR_TIMEOUT, f"request timed out after {timeout}s") from err
    except urllib.error.URLError as err:
        reason = getattr(err, "reason", err)
        if isinstance(reason, TimeoutError):
            raise OpenAIBackendError(ERR_TIMEOUT, f"request timed out after {timeout}s") from err
        raise OpenAIBackendError(ERR_ENDPOINT_UNAVAILABLE, f"endpoint unreachable: {reason}") from err
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError) as err:
        raise OpenAIBackendError(ERR_BAD_RESPONSE, f"response is not valid JSON: {err}") from err
    if not isinstance(parsed, dict):
        raise OpenAIBackendError(ERR_BAD_RESPONSE, "response JSON is not an object")
    return parsed


def _http_get_json(url: str, *, headers: dict[str, str], timeout: float) -> dict[str, Any]:
    """GET JSON（用于 health 探测 {base_url}/models）。"""
    req = urllib.request.Request(url, method="GET")
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as err:
        raise OpenAIBackendError(ERR_BAD_RESPONSE, f"HTTP {err.code}: {err.reason}") from err
    except TimeoutError as err:
        raise OpenAIBackendError(ERR_TIMEOUT, f"request timed out after {timeout}s") from err
    except urllib.error.URLError as err:
        reason = getattr(err, "reason", err)
        if isinstance(reason, TimeoutError):
            raise OpenAIBackendError(ERR_TIMEOUT, f"request timed out after {timeout}s") from err
        raise OpenAIBackendError(ERR_ENDPOINT_UNAVAILABLE, f"endpoint unreachable: {reason}") from err
    try:
        return json.loads(raw)
    except (TypeError, ValueError) as err:
        raise OpenAIBackendError(ERR_BAD_RESPONSE, f"response is not valid JSON: {err}") from err


def _resolve_base_url(model: ModelSpec) -> str | None:
    return model.endpoint_url or os.environ.get("ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL") or None


def _resolve_api_key() -> str | None:
    return os.environ.get("ROBOT_DH_OPENAI_COMPATIBLE_API_KEY") or None


def _resolve_remote_model(model: ModelSpec) -> str:
    return (
        os.environ.get("ROBOT_DH_OPENAI_COMPATIBLE_MODEL")
        or str(model.tags.get("remote_model") or "")
        or model.model_name
        or model.model_id
    )


def _auth_headers() -> dict[str, str]:
    api_key = _resolve_api_key()
    return {"Authorization": f"Bearer {api_key}"} if api_key else {}


class OpenAICompatibleBackend(BaseModelBackend):
    name = "openai_compatible"

    def health(self, model: ModelSpec) -> BackendHealth:
        base_url = _resolve_base_url(model)
        if not base_url:
            return BackendHealth(
                status="FAIL",
                backend=self.name,
                model_id=model.model_id,
                detail="未配置 endpoint",
                error=f"{ERR_ENDPOINT_UNAVAILABLE}: 设置 ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL 或 model.endpoint_url",
            )
        started = time.perf_counter()
        try:
            _http_get_json(
                base_url.rstrip("/") + "/models",
                headers=_auth_headers(),
                timeout=5.0,
            )
        except OpenAIBackendError as err:
            return BackendHealth(
                status="FAIL",
                backend=self.name,
                model_id=model.model_id,
                detail=f"探测 {base_url} 失败",
                error=f"{err.error_type}: {err.message}",
                latency_ms=(time.perf_counter() - started) * 1000.0,
            )
        return BackendHealth(
            status="PASS",
            backend=self.name,
            model_id=model.model_id,
            detail=f"endpoint 可达：{base_url}",
            latency_ms=(time.perf_counter() - started) * 1000.0,
        )

    def predict_batch(
        self,
        samples: list[InferenceSample],
        model: ModelSpec,
        config: dict[str, Any],
    ) -> list[InferencePrediction]:
        base_url = _resolve_base_url(model)
        timeout = float(config.get("timeout_sec") or model.timeout_sec or 60)
        retry = int(config.get("retry") or 0)
        prediction_type = "embedding" if model.model_type == "embedding" else "caption"

        out: list[InferencePrediction] = []
        for sample in samples:
            if not base_url:
                out.append(
                    self._failed(sample, prediction_type, ERR_ENDPOINT_UNAVAILABLE, "base_url 未配置")
                )
                continue
            out.append(self._predict_one(sample, model, base_url, prediction_type, timeout, retry))
        return out

    def _predict_one(
        self,
        sample: InferenceSample,
        model: ModelSpec,
        base_url: str,
        prediction_type: str,
        timeout: float,
        retry: int,
    ) -> InferencePrediction:
        attempts = max(1, retry + 1)
        last: OpenAIBackendError | None = None
        started = time.perf_counter()
        for _ in range(attempts):
            try:
                if prediction_type == "embedding":
                    pred = self._call_embedding(sample, model, base_url, timeout)
                else:
                    pred = self._call_chat(sample, model, base_url, timeout)
                pred.latency_ms = (time.perf_counter() - started) * 1000.0
                return pred
            except OpenAIBackendError as err:
                last = err
                # 仅对超时 / 连接类错误重试；BAD_RESPONSE 通常是契约问题，不重试。
                if err.error_type == ERR_BAD_RESPONSE:
                    break
        assert last is not None
        pred = self._failed(sample, prediction_type, last.error_type, last.message)
        pred.latency_ms = (time.perf_counter() - started) * 1000.0
        return pred

    def _call_chat(
        self,
        sample: InferenceSample,
        model: ModelSpec,
        base_url: str,
        timeout: float,
    ) -> InferencePrediction:
        prompt = self._build_prompt(sample)
        payload = {
            "model": _resolve_remote_model(model),
            "messages": [
                {"role": "system", "content": "You describe robot manipulation episodes concisely."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.0,
        }
        resp = _http_post_json(
            base_url.rstrip("/") + "/chat/completions",
            payload,
            headers=_auth_headers(),
            timeout=timeout,
        )
        try:
            choice = resp["choices"][0]
            content = choice["message"]["content"]
        except (KeyError, IndexError, TypeError) as err:
            raise OpenAIBackendError(ERR_BAD_RESPONSE, f"缺少 choices[0].message.content: {err}") from err
        usage = resp.get("usage") or {}
        token_count = usage.get("total_tokens")
        return InferencePrediction(
            sample_id=sample.sample_id,
            prediction_type="caption",
            prediction_json={"text": content},
            confidence=None,
            token_count=int(token_count) if isinstance(token_count, (int, float)) else None,
            status=PREDICTION_OK,
        )

    def _call_embedding(
        self,
        sample: InferenceSample,
        model: ModelSpec,
        base_url: str,
        timeout: float,
    ) -> InferencePrediction:
        text = sample.input_text or sample.input_uri or sample.sample_id
        payload = {"model": _resolve_remote_model(model), "input": text}
        resp = _http_post_json(
            base_url.rstrip("/") + "/embeddings",
            payload,
            headers=_auth_headers(),
            timeout=timeout,
        )
        try:
            vec = resp["data"][0]["embedding"]
        except (KeyError, IndexError, TypeError) as err:
            raise OpenAIBackendError(ERR_BAD_RESPONSE, f"缺少 data[0].embedding: {err}") from err
        if not isinstance(vec, list):
            raise OpenAIBackendError(ERR_BAD_RESPONSE, "embedding 不是数组")
        return InferencePrediction(
            sample_id=sample.sample_id,
            prediction_type="embedding",
            prediction_json={"embedding": vec, "dim": len(vec)},
            status=PREDICTION_OK,
        )

    @staticmethod
    def _build_prompt(sample: InferenceSample) -> str:
        if sample.input_text:
            return sample.input_text
        parts = [f"Describe robot episode {sample.episode_id or '?'}"]
        if sample.dataset_id:
            parts.append(f"from dataset {sample.dataset_id}")
        return " ".join(parts) + "."

    @staticmethod
    def _failed(
        sample: InferenceSample,
        prediction_type: str,
        error_type: str,
        message: str,
    ) -> InferencePrediction:
        return InferencePrediction(
            sample_id=sample.sample_id,
            prediction_type=prediction_type,
            prediction_json={},
            status=PREDICTION_FAILED,
            error_message=f"{error_type}: {message}",
        )
