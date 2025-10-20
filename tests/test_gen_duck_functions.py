"""Unit tests for the DuckDB function generation utilities."""

from __future__ import annotations

from tools.gen_duck_functions import (
    DuckDBFunctionRecord,
    partition_functions,
)


def _record(
    *,
    name: str,
    function_type: str,
    family: str,
    schema: str = "main",
) -> DuckDBFunctionRecord:
    """Convenience factory mirroring the DuckDB catalog response."""

    return DuckDBFunctionRecord(
        schema_name=schema,
        function_name=name,
        function_type=function_type,
        return_type="INTEGER",
        parameter_types=("INTEGER",),
        parameters=("value",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition=None,
        family=family,
    )


def test_partition_functions_groups_by_namespace_and_type() -> None:
    scalar_numeric = _record(name="abs", function_type="scalar", family="numeric")
    scalar_boolean = _record(
        name="isfinite", function_type="scalar", family="boolean", schema="pg"
    )
    aggregate_numeric = _record(
        name="sum", function_type="aggregate", family="numeric"
    )
    aggregate_temporal = _record(
        name="min", function_type="aggregate", family="temporal"
    )
    ignored_window = _record(name="row_number", function_type="window", family="generic")

    scalar_map, aggregate_map, window_map = partition_functions(
        [
            scalar_numeric,
            aggregate_temporal,
            scalar_boolean,
            ignored_window,
            aggregate_numeric,
        ]
    )

    assert list(scalar_map) == ["Boolean", "Numeric"]
    assert list(scalar_map["Numeric"].keys()) == ["abs"]
    assert scalar_map["Numeric"]["abs"] == [scalar_numeric]
    assert list(scalar_map["Boolean"].keys()) == ["isfinite"]
    assert scalar_map["Boolean"]["isfinite"] == [scalar_boolean]

    assert list(aggregate_map) == ["Numeric", "Temporal"]
    assert list(aggregate_map["Numeric"].keys()) == ["sum"]
    assert aggregate_map["Numeric"]["sum"] == [aggregate_numeric]
    assert list(aggregate_map["Temporal"].keys()) == ["min"]
    assert aggregate_map["Temporal"]["min"] == [aggregate_temporal]

    assert "Generic" not in scalar_map
    assert "Generic" not in aggregate_map

    assert window_map == {"Generic": {"row_number": [ignored_window]}}


def test_partition_functions_sorts_function_names() -> None:
    scalar_records = [
        _record(name="lower", function_type="scalar", family="varchar"),
        _record(name="upper", function_type="scalar", family="varchar"),
        _record(name="ascii", function_type="scalar", family="varchar"),
    ]

    scalar_map, _, window_map = partition_functions(reversed(scalar_records))

    assert list(scalar_map) == ["Varchar"]
    assert list(scalar_map["Varchar"].keys()) == ["ascii", "lower", "upper"]
    assert window_map == {}
