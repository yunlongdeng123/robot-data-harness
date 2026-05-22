from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class ArtifactStore(ABC):
    @abstractmethod
    def put_file(self, local_path: Path, artifact_path: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def put_dir(self, local_dir: Path, artifact_prefix: str) -> dict[str, str]:
        raise NotImplementedError

    @abstractmethod
    def get_file(self, artifact_uri: str, local_path: Path) -> Path:
        raise NotImplementedError

    @abstractmethod
    def exists(self, artifact_uri: str) -> bool:
        raise NotImplementedError
