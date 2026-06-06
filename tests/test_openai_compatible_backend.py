"""v1.9 OpenAICompatibleBackend 测试：monkeypatch HTTP，不依赖真实 endpoint。"""

from __future__ import annotations

import pytest

from robot_dh.models import ModelSpec
from robot_dh.models.backends import openai_compatible as oc
from robot_dh.models.backends.openai_compatible import (
    ERR_BAD_RESPONSE,
    ERR_TIMEOUT,
    OpenAIBackendError,
    OpenAICompatibleBackend,
)
from robot_dh.models.schemas import InferenceSample


def _spec() -> ModelSpec:
    return ModelSpec(
        model_id="openai-compatible-chat-v1",
        model_name="chat",
        model_type="llm",
        backend="openai_compatible",
        endpoint_url="http://fake-endpoint:8000/v1",
    )


def _sample() -> InferenceSample:
    return InferenceSample(sample_id="s1", dataset_id="demo", episode_id="e0")


def test_chat_success(monkeypatch) -> None:
    def fake_post(url, payload, *, headers, timeout):
        assert url.endswith("/chat/completions")
        return {"choices": [{"message": {"content": "a caption"}}], "usage": {"total_tokens": 5}}

    monkeypatch.setattr(oc, "_http_post_json", fake_post)
    be = OpenAICompatibleBackend()
    pred = be.predict_batch([_sample()], _spec(), {})[0]
    assert pred.status == "OK"
    assert pred.prediction_json["text"] == "a caption"
    assert pred.token_count == 5


def test_timeout_classified(monkeypatch) -> None:
    def fake_post(url, payload, *, headers, timeout):
        raise OpenAIBackendError(ERR_TIMEOUT, "request timed out")

    monkeypatch.setattr(oc, "_http_post_json", fake_post)
    be = OpenAICompatibleBackend()
    pred = be.predict_batch([_sample()], _spec(), {"retry": 0})[0]
    assert pred.status == "FAILED"
    assert pred.error_message.startswith(ERR_TIMEOUT)


def test_bad_response_classified(monkeypatch) -> None:
    def fake_post(url, payload, *, headers, timeout):
        return {"unexpected": "shape"}  # 缺 choices -> KeyError -> BAD_RESPONSE

    monkeypatch.setattr(oc, "_http_post_json", fake_post)
    be = OpenAICompatibleBackend()
    pred = be.predict_batch([_sample()], _spec(), {})[0]
    assert pred.status == "FAILED"
    assert pred.error_message.startswith(ERR_BAD_RESPONSE)


def test_health_fail_without_endpoint(monkeypatch) -> None:
    monkeypatch.delenv("ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL", raising=False)
    spec = ModelSpec(model_id="x", model_name="x", model_type="llm", backend="openai_compatible")
    be = OpenAICompatibleBackend()
    h = be.health(spec)
    assert h.status == "FAIL"
    assert "OPENAI_ENDPOINT_UNAVAILABLE" in (h.error or "")


def test_embedding_endpoint(monkeypatch) -> None:
    def fake_post(url, payload, *, headers, timeout):
        assert url.endswith("/embeddings")
        return {"data": [{"embedding": [0.1, 0.2, 0.3]}]}

    monkeypatch.setattr(oc, "_http_post_json", fake_post)
    spec = ModelSpec(model_id="e", model_name="e", model_type="embedding",
                     backend="openai_compatible", endpoint_url="http://x:8000/v1")
    be = OpenAICompatibleBackend()
    pred = be.predict_batch([_sample()], spec, {})[0]
    assert pred.status == "OK"
    assert pred.prediction_json["embedding"] == [0.1, 0.2, 0.3]
