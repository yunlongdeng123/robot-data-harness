from __future__ import annotations

from pathlib import Path

import pytest

from robot_dh.infra.doctor import parse_check_list, render_doctor_human, run_infra_doctor


def test_parse_check_list() -> None:
    assert parse_check_list("db,s3,redis") == ["db", "s3", "redis"]
    with pytest.raises(ValueError):
        parse_check_list("db,unknown")


def test_infra_doctor_sqlite_local_mode(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path / 'robot_dh.db'}")
    for name in (
        "ROBOT_DH_S3_ENDPOINT_URL",
        "ROBOT_DH_S3_ACCESS_KEY",
        "ROBOT_DH_S3_SECRET_KEY",
        "ROBOT_DH_REDIS_URL",
    ):
        monkeypatch.delenv(name, raising=False)

    payload = run_infra_doctor()
    results = {result["name"]: result for result in payload["results"]}

    assert payload["status"] == "PASS"
    assert results["db"]["status"] == "PASS"
    assert results["db"]["backend"] == "sqlite"
    assert results["s3"]["status"] == "SKIP"
    assert results["redis"]["status"] == "SKIP"
    assert "infra doctor: PASS" in render_doctor_human(payload)