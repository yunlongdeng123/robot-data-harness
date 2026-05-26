"""v1.7：本地 file URI / 裸路径不走 download；normalize 打出明确"local direct"日志。"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from robot_dh.etl.normalize import _materialize_input
from robot_dh.lake.uri import is_file_uri, is_local_uri, parse_uri, to_file_uri, to_local_path


def test_to_file_uri_and_back(tmp_path: Path) -> None:
    p = tmp_path / "foo" / "bar"
    p.mkdir(parents=True)
    uri = to_file_uri(p)
    assert uri.startswith("file:///")
    assert is_file_uri(uri)
    assert is_local_uri(uri)
    parsed = parse_uri(uri)
    assert parsed.is_local
    assert to_local_path(uri) == p.resolve()


def test_is_file_uri_only_matches_file_scheme() -> None:
    assert is_file_uri("file:///x") is True
    assert is_file_uri("s3://b/k") is False
    assert is_file_uri("/abs/path") is False
    assert is_file_uri("./relative") is False


def test_materialize_input_local_does_not_copy(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    src = tmp_path / "ds"
    src.mkdir()
    big = src / "big.bin"
    big.write_bytes(b"x" * 4096)
    work_dir = tmp_path / "work"
    work_dir.mkdir()

    with caplog.at_level(logging.INFO, logger="robot_dh.etl.normalize"):
        out = _materialize_input(str(src), work_dir)
    # 直接返回源路径，不应在 work_dir 下复制
    assert out == src.resolve()
    assert not (work_dir / "input").exists() or not any((work_dir / "input").iterdir()) \
        if (work_dir / "input").exists() else True
    # 关键日志：v1.7 要求 normalize 在本地路径时明确给出 "local direct"
    assert any("using local direct input" in r.message for r in caplog.records), \
        [r.message for r in caplog.records]


def test_materialize_input_file_uri(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    src = tmp_path / "ds2"
    src.mkdir()
    (src / "a.parquet").write_bytes(b"\x00" * 100)
    work_dir = tmp_path / "work2"
    work_dir.mkdir()
    uri = to_file_uri(src)
    with caplog.at_level(logging.INFO, logger="robot_dh.etl.normalize"):
        out = _materialize_input(uri, work_dir)
    assert out == src.resolve()
    assert any("local direct input" in r.message for r in caplog.records)
