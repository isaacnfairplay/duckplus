"""Unit tests covering DuckDB type parsing and hierarchy."""

from duckplus.typed.types import (
    DecimalType,
    ListType,
    StructField,
    StructType,
    parse_type,
)


def test_decimal_type_parses_precision_and_scale() -> None:
    duck_type = parse_type("DECIMAL(10, 2)")
    assert isinstance(duck_type, DecimalType)
    assert duck_type.precision == 10
    assert duck_type.scale == 2


def test_list_type_wraps_nested_element() -> None:
    duck_type = parse_type("LIST(INTEGER)")
    assert isinstance(duck_type, ListType)
    assert duck_type.element_type.render() == "INTEGER"


def test_struct_type_preserves_fields() -> None:
    duck_type = parse_type("STRUCT(id INTEGER, name VARCHAR)")
    assert isinstance(duck_type, StructType)
    assert duck_type.fields == (
        StructField("id", parse_type("INTEGER")),
        StructField("name", parse_type("VARCHAR")),
    )
