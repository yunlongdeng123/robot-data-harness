from __future__ import annotations

from pathlib import Path

from robot_dh.artifacts import LocalArtifactStore


def test_local_artifact_store_put_file_and_exists(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path / "artifacts")
    source = tmp_path / "source.txt"
    source.write_text("hello artifact\n", encoding="utf-8")

    artifact_uri = store.put_file(source, "files/source.txt")

    assert artifact_uri.startswith("file://")
    assert store.exists(artifact_uri)
    assert (tmp_path / "artifacts" / "files" / "source.txt").read_text(encoding="utf-8") == "hello artifact\n"


def test_local_artifact_store_put_dir(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path / "artifacts")
    source_dir = tmp_path / "plots"
    source_dir.mkdir()
    (source_dir / "a.txt").write_text("a", encoding="utf-8")
    (source_dir / "b.txt").write_text("b", encoding="utf-8")

    uploaded = store.put_dir(source_dir, "plots")

    assert set(uploaded) == {"a.txt", "b.txt"}
    assert store.exists(uploaded["a.txt"])
    assert store.exists(uploaded["b.txt"])
