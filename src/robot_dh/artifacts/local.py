from __future__ import annotations

import shutil
from pathlib import Path

from robot_dh.artifacts.base import ArtifactStore


class LocalArtifactStore(ArtifactStore):
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir.expanduser().resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _destination(self, artifact_path: str) -> Path:
        destination = self.base_dir / artifact_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        return destination

    def put_file(self, local_path: Path, artifact_path: str) -> str:
        local_path = local_path.expanduser().resolve()
        destination = self._destination(artifact_path)
        if local_path != destination:
            shutil.copy2(local_path, destination)
        return destination.as_uri()

    def put_dir(self, local_dir: Path, artifact_prefix: str) -> dict[str, str]:
        local_dir = local_dir.expanduser().resolve()
        if not local_dir.exists() or not local_dir.is_dir():
            raise FileNotFoundError(f"Artifact directory not found: {local_dir}")
        uploaded: dict[str, str] = {}
        for source_path in sorted(local_dir.rglob("*")):
            if not source_path.is_file():
                continue
            relative_path = source_path.relative_to(local_dir).as_posix()
            uploaded[relative_path] = self.put_file(source_path, f"{artifact_prefix}/{relative_path}")
        return uploaded

    def get_file(self, artifact_uri: str, local_path: Path) -> Path:
        source_path = Path(artifact_uri.removeprefix("file://")).expanduser().resolve()
        if not source_path.exists():
            raise FileNotFoundError(f"Artifact not found: {artifact_uri}")
        local_path = local_path.expanduser().resolve()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, local_path)
        return local_path

    def exists(self, artifact_uri: str) -> bool:
        return Path(artifact_uri.removeprefix("file://")).expanduser().resolve().exists()
