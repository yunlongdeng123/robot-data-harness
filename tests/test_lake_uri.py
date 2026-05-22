from __future__ import annotations

import pytest

from robot_dh.lake.uri import is_local_uri, is_s3_uri, join_uri, parse_uri


@pytest.mark.parametrize(
    "uri,expected_scheme,expected_bucket,expected_key",
    [
        ("s3://robot-lake/ods/foo/v1", "s3", "robot-lake", "ods/foo/v1"),
        ("s3://robot-lake/", "s3", "robot-lake", ""),
        ("s3://robot-lake/ods//foo///v1/", "s3", "robot-lake", "ods/foo/v1"),
    ],
)
def test_parse_s3_uri(uri, expected_scheme, expected_bucket, expected_key) -> None:
    parsed = parse_uri(uri)
    assert parsed.scheme == expected_scheme
    assert parsed.bucket == expected_bucket
    assert parsed.key == expected_key
    assert parsed.is_s3
    assert not parsed.is_local


@pytest.mark.parametrize(
    "uri,expected_local",
    [
        ("./samples/foo", "./samples/foo"),
        ("/abs/path", "/abs/path"),
        ("file:///abs/path", "/abs/path"),
        ("file://samples/foo", "samples/foo"),
        ("runs/lake/ods/foo/v1", "runs/lake/ods/foo/v1"),
    ],
)
def test_parse_local_uri(uri, expected_local) -> None:
    parsed = parse_uri(uri)
    assert parsed.is_local
    assert parsed.local_path == expected_local


def test_parse_uri_empty_raises() -> None:
    with pytest.raises(ValueError):
        parse_uri("")


def test_parse_uri_s3_empty_bucket_raises() -> None:
    with pytest.raises(ValueError):
        parse_uri("s3:///key/only")


@pytest.mark.parametrize(
    "uri,expected_s3,expected_local",
    [
        ("s3://b/k", True, False),
        ("./a", False, True),
        ("file:///abs", False, True),
    ],
)
def test_is_uri_helpers(uri, expected_s3, expected_local) -> None:
    assert is_s3_uri(uri) is expected_s3
    assert is_local_uri(uri) is expected_local


def test_join_s3_uri() -> None:
    assert join_uri("s3://robot-lake/", "ods", "foo", "v1") == "s3://robot-lake/ods/foo/v1"
    assert join_uri("s3://robot-lake/ods/foo", "v1") == "s3://robot-lake/ods/foo/v1"
    assert join_uri("s3://robot-lake/", "/ods/", "/foo/", "v1/") == "s3://robot-lake/ods/foo/v1"


def test_join_local_uri() -> None:
    assert join_uri("runs/lake", "ods", "foo", "v1") == "runs/lake/ods/foo/v1"
    assert join_uri("./samples", "x") == "./samples/x"
    assert join_uri("file:///abs/path", "y") == "/abs/path/y"


def test_join_with_empty_parts() -> None:
    assert join_uri("s3://b/k", "", None, "x") == "s3://b/k/x"
    assert join_uri("runs/lake", None) == "runs/lake"


def test_round_trip_s3() -> None:
    base = "s3://robot-lake/ods"
    joined = join_uri(base, "foo", "v1")
    re_parsed = parse_uri(joined)
    assert re_parsed.bucket == "robot-lake"
    assert re_parsed.key == "ods/foo/v1"
