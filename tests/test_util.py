from __future__ import annotations

from enum import Enum
from pathlib import Path

import pytest

from duckplus import util


class SampleEnum(Enum):
    FOO = "foo"


def test_ensure_identifier_accepts_simple_names() -> None:
    assert util.ensure_identifier("valid_name") == "valid_name"


def test_ensure_identifier_rejects_invalid_names() -> None:
    with pytest.raises(ValueError):
        util.ensure_identifier("123abc")


def test_ensure_identifier_allows_quoted_names_when_enabled() -> None:
    quoted = '"Mixed Case"'
    assert util.ensure_identifier(quoted, allow_quoted=True) == quoted

    with pytest.raises(ValueError):
        util.ensure_identifier(quoted)


def test_normalize_columns_builds_casefold_lookup() -> None:
    columns, lookup = util.normalize_columns(["Foo", "Bar"])

    assert columns == ["Foo", "Bar"]
    assert lookup == {"foo": 0, "bar": 1}


def test_normalize_columns_rejects_duplicates_case_insensitive() -> None:
    with pytest.raises(ValueError):
        util.normalize_columns(["Foo", "foo"])


def test_resolve_columns_handles_case_insensitive_matches() -> None:
    available = ["Foo", "Bar"]
    resolved = util.resolve_columns(["foo", "BAR"], available)

    assert resolved == ["Foo", "Bar"]


def test_resolve_columns_missing_columns() -> None:
    with pytest.raises(KeyError):
        util.resolve_columns(["missing"], ["Foo"])


def test_resolve_columns_missing_ok_skips_missing() -> None:
    result = util.resolve_columns(["missing", "Foo"], ["Foo"], missing_ok=True)

    assert result == ["Foo"]


def test_coerce_scalar_handles_path_and_enum() -> None:
    path = Path("/tmp/example")

    assert util.coerce_scalar(path) == str(path)
    assert util.coerce_scalar(SampleEnum.FOO) == "foo"
