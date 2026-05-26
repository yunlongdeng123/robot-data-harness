"""QC probe 失败时必须暴露 ``error_type`` / ``cause``，停止吞成 "Max Retries Exceeded"。"""

from __future__ import annotations

import builtins
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from robot_dh.qc.hdf5_probe import probe_hdf5
from robot_dh.qc.parquet_probe import probe_parquet
from robot_dh.qc.profile import profile_dataset


def test_probe_parquet_corrupt_returns_error_type(tmp_path: Path) -> None:
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"NotAParquetFile\x00\x00\x00")
    out = probe_parquet(bad)
    assert out["readable"] is False
    assert out["error_type"]
    assert "error" in out


def test_probe_hdf5_corrupt_returns_error_type(tmp_path: Path) -> None:
    bad = tmp_path / "bad.hdf5"
    bad.write_bytes(b"\x00not-h5")
    out = probe_hdf5(bad)
    assert out["readable"] is False
    assert out["error_type"]


def test_profile_dataset_marks_warn_when_probe_fails(tmp_path: Path) -> None:
    root = tmp_path / "mixed"
    root.mkdir()
    df = pa.table({"a": [1, 2, 3], "b": [0.1, 0.2, 0.3]})
    pq.write_table(df, root / "ok.parquet")
    (root / "bad.parquet").write_bytes(b"junk")
    profile = profile_dataset(dataset_uri=root.as_posix(), dataset_id="mix", version="v1")
    assert profile.status == "WARN"
    parquet_probes = profile.profile["parquet"]
    assert len(parquet_probes) == 2
    failed = [p for p in parquet_probes if not p.get("readable")]
    assert failed and failed[0].get("error_type")
    assert profile.profile["probe_failure_count"] == 1


def test_probe_hdf5_uri_does_not_delete_local_source(tmp_path: Path) -> None:
    """v1.7（本地 file:// URI）：``_materialize_local`` 对本地 URI 直接返回原 path，
    ``_probe_hdf5_uri`` 必须**不能** unlink，否则把 raw 源数据自身删了。
    本测试是 v1.7 local devscale workflow robomimic-normalize 不再凭空丢失 hdf5
    的强守门——回归一次整 raw 数据就报废，CI 必挂。
    """
    import h5py
    from robot_dh.qc.profile import _probe_hdf5_uri

    src = tmp_path / "raw" / "robomimic_fake" / "v1" / "low_dim_v15.hdf5"
    src.parent.mkdir(parents=True)
    with h5py.File(src, "w") as f:
        data_grp = f.create_group("data")
        demo = data_grp.create_group("demo_0")
        demo.create_dataset("actions", data=[[0.0] * 7] * 8)
    assert src.is_file()

    uri = f"file://{src.as_posix()}"
    out = _probe_hdf5_uri(uri, tmp_path / "qc-tmp")

    assert out.get("readable") is True
    # 关键断言：probe 完源文件还在
    assert src.is_file(), "probe_hdf5_uri must NOT unlink local source files"


def test_probe_video_uri_does_not_delete_local_source(tmp_path: Path) -> None:
    """同款守门：本地 file:// mp4 不能被 probe 完 unlink。"""
    from unittest.mock import patch

    from robot_dh.qc.profile import _probe_video_uri

    src = tmp_path / "raw" / "v1" / "videos" / "obs.mp4"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"\x00\x00\x00\x18ftypisom")
    assert src.is_file()

    uri = f"file://{src.as_posix()}"
    # mock probe_video 避免依赖真实 ffmpeg，仅验证 unlink 不发生
    with patch("robot_dh.qc.profile.probe_video", return_value={"readable": True, "fps": 30}):
        out = _probe_video_uri(uri, tmp_path / "qc-tmp")

    assert out.get("readable") is True
    assert src.is_file(), "probe_video_uri must NOT unlink local source files"


def test_summarize_exception_falls_back_to_context_when_cause_missing() -> None:
    """v1.6.7：botocore.exceptions.RetriesExceededError 这种 ``raise`` 不带 ``from``
    的异常，``__cause__`` 永远是 None；fallback 到 ``__context__`` 才能拿到根因。
    本测试用 ConnectionResetError 模拟一次 ``raise X`` 不带 ``from``，验证 cause_type
    是 ConnectionResetError 而不是 None；同时 ``traceback`` 字段非空。
    """
    from robot_dh.qc.parquet_probe import _summarize_exception

    try:
        try:
            raise ConnectionResetError("simulated underlying network reset")
        except ConnectionResetError:
            raise RuntimeError("Max Retries Exceeded")
    except RuntimeError as err:
        out = _summarize_exception(err)

    assert out["error_type"] == "RuntimeError"
    assert out["cause_type"] == "ConnectionResetError", (
        f"expected fallback to __context__ when __cause__ is None; got {out['cause_type']}"
    )
    assert out["cause"] == "simulated underlying network reset"
    assert out["traceback"]


def test_summarize_exception_no_cause_keeps_repr_and_traceback() -> None:
    """``__cause__`` 与 ``__context__`` 都为 None 时（直接 raise 的根因异常），
    cause_type=None 但 ``cause = repr(err)`` 不能为空字符串，且 ``traceback`` 必须存在。
    """
    from robot_dh.qc.parquet_probe import _summarize_exception

    try:
        raise ValueError("naked failure with no context")
    except ValueError as err:
        out = _summarize_exception(err)

    assert out["error_type"] == "ValueError"
    assert out["cause_type"] is None
    assert "ValueError" in out["cause"]
    assert "naked failure" in out["cause"]
    assert out["traceback"]


def test_probe_hdf5_failed_download_uri_surfaces_cause(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """**重复要求第三次（fhkvr §5.1.2 → qptk9 §5.1 → ddbfb R2）**：

    profile.py 的 ``_probe_hdf5_uri`` 在 download 失败时返回的 dict 中
    ``cause_type`` 不能是 None（必须 fallback ``__context__`` 或暴露 traceback）。
    模拟一次 download_file 抛"无 cause"的 RuntimeError 场景，验证 probe dict 的
    ``cause_type`` / ``traceback`` 至少有一项可见。
    """
    from robot_dh.qc import profile as profile_mod

    def _fake_materialize(uri: str, target: Path, **_kwargs) -> Path:
        # 模拟 botocore RetriesExceededError 这种 raise 不带 from 的链：
        # 内层先抛网络错误，外层不 chain 直接抛 RuntimeError
        try:
            raise ConnectionResetError("Connection reset by peer (simulated)")
        except ConnectionResetError:
            raise RuntimeError("Max Retries Exceeded")

    monkeypatch.setattr(profile_mod, "_materialize_local", _fake_materialize)

    out = profile_mod._probe_hdf5_uri("s3://nope/x.hdf5", tmp_path)

    assert out["readable"] is False
    assert out["error_type"] == "RuntimeError"
    # 防止再次回归 cause_type=None
    assert out["cause_type"] is not None, (
        "hdf5 probe must surface ``__context__`` when ``__cause__`` is None; "
        "ddbfb R2 require this for botocore RetriesExceededError"
    )
    assert out["cause_type"] == "ConnectionResetError"
    assert out.get("traceback")


# ---------- v1.6.8（fvx5z F2）CI 强制门 ----------


def test_summarize_exception_skips_same_type_self_reference() -> None:
    """fvx5z 现场：botocore download_file (s3transfer + 内层 client) 互相把
    ``RetriesExceededError`` 包成 ``__context__``，导致 ``cause_type == error_type``
    自引用，排障毫无信息量。``_summarize_exception`` 必须沿链回溯，**跳过同类型
    祖先**找到第一个真正不同类型的根因。
    """
    from robot_dh.qc.parquet_probe import _summarize_exception

    class FakeRetriesExceeded(RuntimeError):
        pass

    try:
        try:
            try:
                raise ConnectionResetError("real underlying network reset")
            except ConnectionResetError:
                raise FakeRetriesExceeded("Max Retries Exceeded (inner client layer)")
        except FakeRetriesExceeded:
            raise FakeRetriesExceeded("Max Retries Exceeded (outer transfer layer)")
    except FakeRetriesExceeded as err:
        out = _summarize_exception(err)

    assert out["error_type"] == "FakeRetriesExceeded"
    assert out["cause_type"] != out["error_type"], (
        f"cause_type leaks error_type ({out['cause_type']} == {out['error_type']}); "
        "_summarize_exception must walk past same-type ancestors via __context__ chain"
    )
    assert out["cause_type"] == "ConnectionResetError"


def test_summarize_exception_avoids_infinite_loop_on_self_reference() -> None:
    """``__context__`` 指回自身的极端情况下，回溯必须有 visited 短路兜底，
    否则 ``cause_type=None`` 比死循环可接受。"""
    from robot_dh.qc.parquet_probe import _summarize_exception

    err = RuntimeError("self-loop")
    # 显式让 __context__ 指回自己——理论上 CPython 不会这么做，但写防御。
    err.__context__ = err
    out = _summarize_exception(err)
    assert out["error_type"] == "RuntimeError"
    assert out["cause_type"] is None


def test_probe_hdf5_uri_uses_fast_boto_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """fvx5z F2：hdf5 probe materialize-first 必须走 ``get_s3_boto_client_fast``，
    不能再退回默认 ``get_s3_boto_client``（read_timeout=300s × 10 → 50 min/file）。
    """
    from robot_dh.qc import profile as profile_mod
    from robot_dh.lake import s3_fs as s3_fs_mod

    captured: dict[str, bool] = {"fast_called": False, "default_called": False}

    class _FakeClient:
        def download_file(self, bucket: str, key: str, target: str) -> None:
            Path(target).write_bytes(b"")

    def _fake_fast(*, read_timeout: float = 60.0, region=None):
        captured["fast_called"] = True
        return _FakeClient()

    def _fake_default(*, region=None):
        captured["default_called"] = True
        return _FakeClient()

    monkeypatch.setattr(s3_fs_mod, "get_s3_boto_client_fast", _fake_fast)
    monkeypatch.setattr(s3_fs_mod, "get_s3_boto_client", _fake_default)
    monkeypatch.setenv("ROBOT_DH_S3_ENDPOINT_URL", "http://fake:9000")
    monkeypatch.setenv("ROBOT_DH_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("ROBOT_DH_S3_SECRET_KEY", "sk")

    profile_mod._probe_hdf5_uri("s3://bucket/key/x.hdf5", tmp_path)

    assert captured["fast_called"], "hdf5 probe must call get_s3_boto_client_fast"
    assert not captured["default_called"], (
        "hdf5 probe must NOT fall back to default get_s3_boto_client (300s × 10 retries)"
    )


def test_profile_hdf5_uses_boto3_download_file_not_fsspec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """防回归：HDF5 profile 必须用 boto3 ``download_file`` 整文件落地。

    这是 fvx5z §8 点名的 required check。测试期间把 ``fsspec`` / ``s3fs`` import
    设成失败，确保 HDF5 probe 不会退回 remote file handle + h5py 的慢路径。
    """
    from robot_dh.qc import profile as profile_mod
    from robot_dh.lake import s3_fs as s3_fs_mod

    called: dict[str, object] = {}

    class _FakeBotoClient:
        def download_file(self, bucket: str, key: str, target: str) -> None:
            called["bucket"] = bucket
            called["key"] = key
            called["target"] = target
            import h5py

            with h5py.File(target, "w") as handle:
                data = handle.create_group("data")
                demo = data.create_group("demo_0")
                demo.create_dataset("actions", data=[[0.0] * 7, [1.0] * 7])

    real_import = builtins.__import__

    def _deny_fsspec_import(name, globals=None, locals=None, fromlist=(), level=0):  # type: ignore[no-untyped-def]
        if name in {"fsspec", "s3fs"} or name.startswith(("fsspec.", "s3fs.")):
            raise AssertionError(f"HDF5 probe must not import remote FS reader: {name}")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _deny_fsspec_import)
    monkeypatch.setattr(s3_fs_mod, "get_s3_boto_client_fast", lambda **_kwargs: _FakeBotoClient())
    monkeypatch.setenv("ROBOT_DH_S3_ENDPOINT_URL", "http://fake:9000")
    monkeypatch.setenv("ROBOT_DH_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("ROBOT_DH_S3_SECRET_KEY", "sk")

    out = profile_mod._probe_hdf5_uri("s3://robot-datasets/raw/demo.hdf5", tmp_path)

    assert called["bucket"] == "robot-datasets"
    assert called["key"] == "raw/demo.hdf5"
    assert Path(str(called["target"])).name == "demo.hdf5"
    assert out["readable"] is True
    assert out["episode_lens"] == [2]
