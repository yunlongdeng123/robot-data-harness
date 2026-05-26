"""v1.7：本地 file URI 端到端跑 universal contract，不访问任何远端服务。

覆盖：
1. ``qc contract run`` CLI 在 ``file:///abs/...`` 下完成 profile + contract + 写报告；
2. ``--max-workers / --probe-timeout-sec / --disable-remote-lazy / --fail-fast``
   被翻译成对应 env，子进程内部 ``profile_dataset`` 能感知（这里只断言 env 已落，
   细节由 ``robomimic`` / ``bridge`` 自己的测试覆盖）；
3. 写出的 ``contract_report.json`` / ``asset_profile.json`` 在本地落到 lake 子目录。
"""

from __future__ import annotations

import io
import json
import os
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import pytest

from robot_dh.cli import _main_impl as cli_main
from robot_dh.lake.uri import to_file_uri


def _run_cli(args: list[str]) -> tuple[int, str]:
    buf_out = io.StringIO()
    buf_err = io.StringIO()
    with redirect_stdout(buf_out), redirect_stderr(buf_err):
        code = cli_main(args)
    return code, buf_out.getvalue()


@pytest.fixture(autouse=True)
def _reset_qc_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in (
        "ROBOT_DH_QC_PROBE_CONCURRENCY",
        "ROBOT_DH_QC_FILE_TIMEOUT_SEC",
        "ROBOT_DH_QC_PROBE_TIMEOUT_SEC",
        "ROBOT_DH_QC_MAX_RETRIES",
        "ROBOT_DH_QC_DISABLE_REMOTE_LAZY",
        "ROBOT_DH_QC_FAIL_FAST",
    ):
        monkeypatch.delenv(k, raising=False)


def _make_universal_demo(root: Path) -> None:
    """造一个最小 universal 数据集：随便几个 .parquet / .json 让 profile 能 list。"""
    (root / "meta").mkdir(parents=True, exist_ok=True)
    (root / "meta" / "info.json").write_text(json.dumps({"robot_type": "demo", "fps": 30}))
    (root / "data").mkdir(parents=True, exist_ok=True)
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"episode_index": [0, 0, 1], "frame_index": [0, 1, 0]})
    pq.write_table(table, root / "data" / "shard_000.parquet")


def test_local_qc_contract_run_end_to_end(tmp_path: Path) -> None:
    src = tmp_path / "demo_local_dev"
    _make_universal_demo(src)
    out_dir = tmp_path / "lake" / "qc" / "demo_local_dev" / "v1"

    code, stdout = _run_cli([
        "qc", "contract", "run",
        "--dataset-family", "universal",
        "--dataset-uri", to_file_uri(src),
        "--dataset-id", "demo_local_dev",
        "--version", "v1",
        "--output", to_file_uri(out_dir),
        "--contract", "configs/qc/universal.yaml",
        "--log-format", "json",
    ])
    assert code in (0, 1), stdout  # 数据极小可能 WARN/FAIL，但不应 crash
    # stdout 含 runner_boot + 一段格式化 JSON 报告；按 "{...}" 块抓最后一个含 run_id 的对象
    payloads: list[dict] = []
    buf = ""
    depth = 0
    for ch in stdout:
        if ch == "{":
            depth += 1
        if depth > 0:
            buf += ch
        if ch == "}":
            depth -= 1
            if depth == 0 and buf:
                try:
                    payloads.append(json.loads(buf))
                except json.JSONDecodeError:
                    pass
                buf = ""
    report_obj = next((p for p in payloads if "run_id" in p), None)
    assert report_obj is not None, f"no contract report JSON in stdout: {stdout!r}"
    assert "contract_id" in report_obj
    assert report_obj["status"] in {"OK", "PASS", "WARN", "FAIL"}
    # contract_report.json 落本地
    report = out_dir / "contract_report.json"
    profile = out_dir / "asset_profile.json"
    assert report.exists(), list(out_dir.iterdir())
    assert profile.exists()
    parsed_report = json.loads(report.read_text())
    assert parsed_report["dataset_uri"].startswith("file://")
    assert parsed_report["dataset_id"] == "demo_local_dev"


def test_local_qc_contract_cli_translates_flags_to_env(tmp_path: Path) -> None:
    """断言 CLI flag → env 翻译生效，且 run 退出后 env 仍是 CLI 设置的值。"""
    src = tmp_path / "demo_env"
    _make_universal_demo(src)
    out_dir = tmp_path / "lake" / "qc" / "demo_env" / "v1"

    _run_cli([
        "qc", "contract", "run",
        "--dataset-family", "universal",
        "--dataset-uri", to_file_uri(src),
        "--dataset-id", "demo_env",
        "--version", "v1",
        "--output", to_file_uri(out_dir),
        "--contract", "configs/qc/universal.yaml",
        "--max-workers", "2",
        "--file-timeout-sec", "60",
        "--probe-timeout-sec", "30",
        "--max-retries", "1",
        "--disable-remote-lazy",
        "--fail-fast",
        "--log-format", "json",
    ])
    assert os.environ.get("ROBOT_DH_QC_PROBE_CONCURRENCY") == "2"
    assert os.environ.get("ROBOT_DH_QC_FILE_TIMEOUT_SEC") == "60.0"
    assert os.environ.get("ROBOT_DH_QC_PROBE_TIMEOUT_SEC") == "30.0"
    assert os.environ.get("ROBOT_DH_QC_MAX_RETRIES") == "1"
    assert os.environ.get("ROBOT_DH_QC_DISABLE_REMOTE_LAZY") == "1"
    assert os.environ.get("ROBOT_DH_QC_FAIL_FAST") == "1"


def test_local_qc_contract_does_not_touch_s3(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """硬约束：本地 file URI 不能触发任何 boto3 / s3fs / mc 子进程。

    做法：把 ``boto3.client`` 和 ``s3fs.S3FileSystem`` 都替换成抛 AssertionError，
    一旦被调用就立即失败。
    """
    src = tmp_path / "demo_no_s3"
    _make_universal_demo(src)
    out_dir = tmp_path / "lake" / "qc" / "demo_no_s3" / "v1"

    def _explode(*args, **kwargs):  # noqa: ANN001
        raise AssertionError("S3 client was invoked on a local-only QC contract run")

    monkeypatch.setattr("boto3.client", _explode, raising=False)
    monkeypatch.setattr("s3fs.S3FileSystem", _explode, raising=False)

    code, _ = _run_cli([
        "qc", "contract", "run",
        "--dataset-family", "universal",
        "--dataset-uri", to_file_uri(src),
        "--dataset-id", "demo_no_s3",
        "--version", "v1",
        "--output", to_file_uri(out_dir),
        "--contract", "configs/qc/universal.yaml",
        "--log-format", "json",
    ])
    assert code in (0, 1)
    assert (out_dir / "contract_report.json").exists()
