"""checkpoint：写入 / 读取 / 更新；与 _manifest.json 互补。"""

from __future__ import annotations

from pathlib import Path

from robot_dh.progress.checkpoint import (
    CHECKPOINT_FILENAME,
    Checkpoint,
    CheckpointFile,
    CheckpointStore,
    load_checkpoint,
    save_checkpoint,
)


def test_checkpoint_save_and_load(tmp_path: Path) -> None:
    out = tmp_path / "ods/demo/v1"
    out.mkdir(parents=True, exist_ok=True)
    ckpt = Checkpoint(
        dataset_id="demo",
        version="v1",
        phase="normalize",
        source_uri=tmp_path.as_posix(),
        output_uri=out.as_posix(),
    )
    ckpt.mark_step("materialize_input")
    ckpt.upsert_file(CheckpointFile(name="pose.parquet", status="STAGED"))
    save_checkpoint(out.as_posix(), ckpt)

    assert (out / CHECKPOINT_FILENAME).is_file()
    loaded = load_checkpoint(out.as_posix())
    assert loaded is not None
    assert loaded.dataset_id == "demo"
    assert loaded.version == "v1"
    assert loaded.has_step("materialize_input")
    assert loaded.file_status("pose.parquet") == "STAGED"


def test_checkpoint_update_persists_steps(tmp_path: Path) -> None:
    out = tmp_path / "ods/demo/v1"
    out.mkdir(parents=True, exist_ok=True)
    store = CheckpointStore(output_uri=out.as_posix())

    ckpt = Checkpoint(
        dataset_id="demo", version="v1", phase="normalize",
        source_uri="s3://bucket/raw/demo", output_uri=out.as_posix(),
    )
    store.save(ckpt)
    ckpt.mark_step("materialize_input")
    store.save(ckpt)
    ckpt.mark_step("load_bundles")
    store.save(ckpt)

    loaded = store.load()
    assert loaded is not None
    assert "materialize_input" in loaded.completed_steps
    assert "load_bundles" in loaded.completed_steps


def test_load_checkpoint_returns_none_when_missing(tmp_path: Path) -> None:
    assert load_checkpoint((tmp_path / "no_such").as_posix()) is None
