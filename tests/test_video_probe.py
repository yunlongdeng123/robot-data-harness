"""video probe：空文件 -> readable=False，不抛。"""

from __future__ import annotations

from pathlib import Path

from robot_dh.qc.video_probe import probe_video


def test_video_probe_empty_file(tmp_path: Path) -> None:
    p = tmp_path / "empty.mp4"
    p.write_bytes(b"")
    out = probe_video(p)
    assert out["readable"] is False
    assert out["size_bytes"] == 0


def test_video_probe_garbage_file_returns_unreadable(tmp_path: Path) -> None:
    p = tmp_path / "garbage.mp4"
    p.write_bytes(b"\x00" * 64)
    out = probe_video(p)
    assert out["readable"] is False
