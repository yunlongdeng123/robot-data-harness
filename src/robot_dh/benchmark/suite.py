"""benchmark suite 加载器（薄包装，复用 models.BenchmarkSuite.from_yaml）。"""

from __future__ import annotations

from pathlib import Path

from robot_dh.benchmark.models import BenchmarkSuite


def load_suite(path: Path) -> BenchmarkSuite:
    return BenchmarkSuite.from_yaml(Path(path).expanduser().resolve())
