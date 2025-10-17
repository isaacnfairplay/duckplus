"""Regression tests for the ``duckplus.typed.ducktype`` convenience module."""

from __future__ import annotations

from duckplus import (
    Blob as TopLevelBlob,
    Boolean as TopLevelBoolean,
    Generic as TopLevelGeneric,
    Numeric as TopLevelNumeric,
    Varchar as TopLevelVarchar,
    ducktype as top_level_ducktype,
    select as top_level_select,
)
from duckplus.typed.ducktype import (
    Blob,
    Boolean,
    Generic,
    Numeric,
    Varchar,
    ducktype,
    select,
)
from duckplus.typed.expression import DuckTypeNamespace
from duckplus.typed.expression import ducktype as expression_ducktype
from duckplus.typed.types import NumericType, VarcharType


def test_ducktype_module_re_exports_namespace() -> None:
    assert isinstance(ducktype, DuckTypeNamespace)
    assert ducktype is expression_ducktype


def test_ducktype_module_factory_aliases_are_identical() -> None:
    assert Numeric is expression_ducktype.Numeric
    assert Varchar is expression_ducktype.Varchar
    assert Boolean is expression_ducktype.Boolean
    assert Blob is expression_ducktype.Blob
    assert Generic is expression_ducktype.Generic


def test_ducktype_module_select_helper() -> None:
    builder = select().column("1")
    expected = expression_ducktype.select().column("1")
    assert type(builder) is type(expected)
    assert builder.build() == expected.build()


def test_factory_type_metadata_matches_underlying_namespace() -> None:
    numeric_literal = Numeric.literal(42)
    varchar_literal = Varchar.literal("ok")

    assert isinstance(numeric_literal.duck_type, NumericType)
    assert numeric_literal.duck_type.category == "numeric"
    assert isinstance(varchar_literal.duck_type, VarcharType)
    assert varchar_literal.duck_type.render() == "VARCHAR"


def test_duckplus_module_re_exports_typed_factories() -> None:
    assert top_level_ducktype is expression_ducktype
    assert TopLevelNumeric is expression_ducktype.Numeric
    assert TopLevelVarchar is expression_ducktype.Varchar
    assert TopLevelBoolean is expression_ducktype.Boolean
    assert TopLevelBlob is expression_ducktype.Blob
    assert TopLevelGeneric is expression_ducktype.Generic

    builder = top_level_select().column("1")
    expected = expression_ducktype.select().column("1")
    assert builder.build() == expected.build()
