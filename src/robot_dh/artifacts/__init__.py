"""robot_dh 制品存储抽象。"""

from robot_dh.artifacts.base import ArtifactStore
from robot_dh.artifacts.factory import create_artifact_store, resolve_artifact_store_type
from robot_dh.artifacts.local import LocalArtifactStore
from robot_dh.artifacts.s3 import S3ArtifactStore

__all__ = [
	"ArtifactStore",
	"LocalArtifactStore",
	"S3ArtifactStore",
	"create_artifact_store",
	"resolve_artifact_store_type",
]
