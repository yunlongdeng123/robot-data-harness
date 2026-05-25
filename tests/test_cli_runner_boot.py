"""CLI runner_boot 兜底：第一行 print + BaseException traceback。

针对 Argo droid-qc 0B archive log 现场——只要 _emit_runner_boot 跑到，archive log
就必然非空，可以排除"业务进程在第一行 print 前就被 SIGKILL"这种情况。
"""

from __future__ import annotations

import json
import os

import pytest


def test_emit_runner_boot_writes_json_first_line_on_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from robot_dh.cli import _emit_runner_boot

    _emit_runner_boot(["qc", "contract", "run"])
    captured = capsys.readouterr()
    # 必须走 stderr，避免破坏 stdout=JSON 的下游契约
    assert captured.out == ""
    err_lines = captured.err.strip().splitlines()
    assert err_lines, "runner_boot 必须至少打一行到 stderr"
    payload = json.loads(err_lines[0])
    assert payload["event"] == "runner_boot"
    assert payload["argv"] == ["qc", "contract", "run"]
    assert "python" in payload
    assert "ts" in payload


def test_emit_runner_boot_includes_robot_dh_env_keys(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from robot_dh.cli import _emit_runner_boot

    monkeypatch.setenv("ROBOT_DH_TEST_KEY", "1")
    monkeypatch.setenv("ROBOT_DH_S3_ENDPOINT_URL", "http://fake:9000")
    _emit_runner_boot([])
    payload = json.loads(capsys.readouterr().err.strip().splitlines()[0])
    assert "ROBOT_DH_TEST_KEY" in payload["env_keys"]
    assert "ROBOT_DH_S3_ENDPOINT_URL" in payload["env_keys"]


def test_main_emits_runner_boot_then_propagates_traceback(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """业务侧抛 BaseException 时仍能在 archive log 留下 runner_boot + traceback。"""
    from robot_dh import cli

    def _boom(*_args: object, **_kwargs: object) -> int:
        raise RuntimeError("simulated import failure")

    monkeypatch.setattr(cli, "_main_impl", _boom)
    with pytest.raises(RuntimeError, match="simulated import failure"):
        cli.main(["validate", "--dataset", "/dev/null"])
    captured = capsys.readouterr()
    err_lines = captured.err.strip().splitlines()
    assert err_lines, "stderr 必须至少有 runner_boot + traceback"
    boot_line = err_lines[0]
    assert json.loads(boot_line)["event"] == "runner_boot"
    assert "RuntimeError" in captured.err
    assert "simulated import failure" in captured.err
    # stdout 保持干净
    assert captured.out == ""


def test_main_does_not_swallow_systemexit(monkeypatch: pytest.MonkeyPatch) -> None:
    """正常 SystemExit（argparse --help 等）必须原样冒上去，不被 traceback 兜底吃掉。"""
    from robot_dh import cli

    def _exit(*_args: object, **_kwargs: object) -> int:
        raise SystemExit(0)

    monkeypatch.setattr(cli, "_main_impl", _exit)
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["--version"])
    assert excinfo.value.code == 0
