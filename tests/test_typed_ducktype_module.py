"""Regression tests for the ``duckplus.typed.ducktype`` convenience module."""

from __future__ import annotations

from duckplus import (
    Blob as TopLevelBlob,
    Boolean as TopLevelBoolean,
    Date as TopLevelDate,
    Datetime as TopLevelDatetime,
    Double as TopLevelDouble,
    Float as TopLevelFloat,
    Generic as TopLevelGeneric,
    Integer as TopLevelInteger,
    Numeric as TopLevelNumeric,
    Smallint as TopLevelSmallint,
    Tinyint as TopLevelTinyint,
    Timestamp as TopLevelTimestamp,
    Uinteger as TopLevelUinteger,
    Usmallint as TopLevelUsmallint,
    Utinyint as TopLevelUtinyint,
    Varchar as TopLevelVarchar,
    ducktype as top_level_ducktype,
    select as top_level_select,
)
from duckplus.typed.ducktype import (
    Blob,
    Boolean,
    Date,
    Datetime,
    Double,
    Float,
    Generic,
    Integer,
    Numeric,
    Smallint,
    Tinyint,
    Timestamp,
    Uinteger,
    Usmallint,
    Utinyint,
    Varchar,
    ducktype,
    select,
)
from duckplus.typed.expression import DuckTypeNamespace
from duckplus.typed.expression import ducktype as expression_ducktype
from duckplus.typed.types import IntegerType, NumericType, TemporalType, VarcharType


def test_ducktype_module_re_exports_namespace() -> None:
    assert isinstance(ducktype, DuckTypeNamespace)
    assert ducktype is expression_ducktype


def test_ducktype_module_factory_aliases_are_identical() -> None:
    assert Numeric is expression_ducktype.Numeric
    assert Varchar is expression_ducktype.Varchar
    assert Boolean is expression_ducktype.Boolean
    assert Blob is expression_ducktype.Blob
    assert Generic is expression_ducktype.Generic
    assert Tinyint is expression_ducktype.Tinyint
    assert Smallint is expression_ducktype.Smallint
    assert Integer is expression_ducktype.Integer
    assert Utinyint is expression_ducktype.Utinyint
    assert Usmallint is expression_ducktype.Usmallint
    assert Uinteger is expression_ducktype.Uinteger
    assert Float is expression_ducktype.Float
    assert Double is expression_ducktype.Double
    assert Date is expression_ducktype.Date
    assert Datetime is expression_ducktype.Datetime
    assert Timestamp is expression_ducktype.Timestamp


def test_ducktype_module_select_helper() -> None:
    builder = select().column("1")
    expected = expression_ducktype.select().column("1")
    assert type(builder) is type(expected)
    assert builder.build() == expected.build()


def test_factory_type_metadata_matches_underlying_namespace() -> None:
    numeric_literal = Numeric.literal(42)
    varchar_literal = Varchar.literal("ok")
    integer_literal = Integer.literal(7)
    date_literal = Date.literal("2024-01-01")

    assert isinstance(numeric_literal.duck_type, NumericType)
    assert numeric_literal.duck_type.category == "numeric"
    assert isinstance(varchar_literal.duck_type, VarcharType)
    assert varchar_literal.duck_type.render() == "VARCHAR"
    assert isinstance(integer_literal.duck_type, IntegerType)
    assert integer_literal.duck_type.render() == "INTEGER"
    assert isinstance(date_literal.duck_type, TemporalType)
    assert date_literal.duck_type.render() == "DATE"


def test_duckplus_module_re_exports_typed_factories() -> None:
    assert top_level_ducktype is expression_ducktype
    assert TopLevelNumeric is expression_ducktype.Numeric
    assert TopLevelVarchar is expression_ducktype.Varchar
    assert TopLevelBoolean is expression_ducktype.Boolean
    assert TopLevelBlob is expression_ducktype.Blob
    assert TopLevelGeneric is expression_ducktype.Generic
    assert TopLevelTinyint is expression_ducktype.Tinyint
    assert TopLevelSmallint is expression_ducktype.Smallint
    assert TopLevelInteger is expression_ducktype.Integer
    assert TopLevelUtinyint is expression_ducktype.Utinyint
    assert TopLevelUsmallint is expression_ducktype.Usmallint
    assert TopLevelUinteger is expression_ducktype.Uinteger
    assert TopLevelFloat is expression_ducktype.Float
    assert TopLevelDouble is expression_ducktype.Double
    assert TopLevelDate is expression_ducktype.Date
    assert TopLevelDatetime is expression_ducktype.Datetime
    assert TopLevelTimestamp is expression_ducktype.Timestamp

    builder = top_level_select().column("1")
    expected = expression_ducktype.select().column("1")
    assert builder.build() == expected.build()
