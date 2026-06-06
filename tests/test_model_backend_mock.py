"""v1.9 MockBackend / LocalCPUBackend 确定性输出测试。"""

from __future__ import annotations

from robot_dh.models import ModelSpec, get_backend
from robot_dh.models.schemas import InferenceSample


def _sample(sid: str, **kw) -> InferenceSample:
    return InferenceSample(sample_id=sid, dataset_id=kw.get("dataset_id", "demo"),
                           episode_id=kw.get("episode_id", "e0"), metadata=kw.get("metadata", {}))


def test_mock_caption_deterministic() -> None:
    spec = ModelSpec(model_id="m", model_name="m", model_type="caption", backend="mock")
    be = get_backend(spec)
    p1 = be.predict_batch([_sample("s1", dataset_id="droid")], spec, {})[0]
    p2 = be.predict_batch([_sample("s1", dataset_id="droid")], spec, {})[0]
    assert p1.status == "OK"
    assert p1.prediction_json["text"] == "A robot manipulation episode from dataset droid."
    assert p1.prediction_json == p2.prediction_json
    assert p1.token_count == len(p1.prediction_json["text"].split())


def test_mock_embedding_deterministic_dim16() -> None:
    spec = ModelSpec(model_id="m", model_name="m", model_type="embedding", backend="mock")
    be = get_backend(spec)
    p1 = be.predict_batch([_sample("s1")], spec, {})[0]
    p2 = be.predict_batch([_sample("s1")], spec, {})[0]
    assert p1.prediction_type == "embedding"
    assert p1.prediction_json["dim"] == 16
    assert len(p1.prediction_json["embedding"]) == 16
    assert p1.prediction_json["embedding"] == p2.prediction_json["embedding"]
    # 不同 sample_id -> 不同向量
    p3 = be.predict_batch([_sample("s2")], spec, {})[0]
    assert p3.prediction_json["embedding"] != p1.prediction_json["embedding"]


def test_mock_anomaly_uses_quality_score() -> None:
    spec = ModelSpec(model_id="m", model_name="m", model_type="anomaly_scorer", backend="mock")
    be = get_backend(spec)
    low_q = be.predict_batch([_sample("s1", metadata={"quality_score": 0.1})], spec, {})[0]
    high_q = be.predict_batch([_sample("s2", metadata={"quality_score": 0.99})], spec, {})[0]
    assert low_q.prediction_json["anomaly_score"] > high_q.prediction_json["anomaly_score"]
    assert low_q.prediction_json["label"] == "anomaly"


def test_local_cpu_anomaly_and_embedding() -> None:
    anom = ModelSpec(model_id="r", model_name="r", model_type="anomaly_scorer", backend="local_cpu")
    be = get_backend(anom)
    p = be.predict_batch([_sample("s1", metadata={"quality_score": 0.2, "max_velocity_mps": 1.8})], anom, {})[0]
    assert p.status == "OK"
    assert 0.0 <= p.prediction_json["anomaly_score"] <= 1.0
    assert "factors" in p.prediction_json

    emb = ModelSpec(model_id="e", model_name="e", model_type="embedding", backend="local_cpu")
    be2 = get_backend(emb)
    pe = be2.predict_batch([_sample("s1")], emb, {})[0]
    assert pe.prediction_json["dim"] == 32
    assert pe.prediction_json["method"] == "feature_hashing"
