from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest

from datetime import date, datetime
from pathlib import Path
from typing import NamedTuple

from duckplus.core import (
    AggregateExpression,
    AsofOrder,
    AsofSpec,
    ColumnDefinition,
    DuckRel,
    DuckSchema,
    FilterExpression,
    ExpressionPredicate,
    JoinProjection,
    JoinSpec,
    PartitionSpec,
    col,
    column,
    equals,
)
from duckplus import ducktypes
import duckplus.util as util_module
from duckplus.materialize import ParquetMaterializeStrategy
from typing_extensions import assert_type


def table_rows(table: pa.Table) -> list[tuple[object, ...]]:
    columns = [table.column(i).to_pylist() for i in range(table.num_columns)]
    if not columns:
        return [tuple() for _ in range(table.num_rows)]
    return list(zip(*columns, strict=True))


@pytest.fixture()
def connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def sample_rel(connection: duckdb.DuckDBPyConnection) -> DuckRel:
    base = connection.sql(
        """
        SELECT *
        FROM (VALUES
            (1, 'Alpha', 10),
            (2, 'Beta', 5),
            (3, 'Gamma', 8)
        ) AS t(id, Name, score)
        """
    )
    return DuckRel(base)


@pytest.fixture()
def sales_rel(connection: duckdb.DuckDBPyConnection) -> DuckRel:
    relation = connection.sql(
        """
        SELECT *
        FROM (VALUES
            ('north', 50, DATE '2024-01-03'),
            ('north', 60, DATE '2024-01-02'),
            ('south', 30, DATE '2024-01-01'),
            ('east', 20, DATE '2024-01-04'),
            ('west', 70, DATE '2024-01-05')
        ) AS t(region, amount, sale_date)
        """
    )
    return DuckRel(relation)


def test_columns_metadata_preserves_case(sample_rel: DuckRel) -> None:
    assert sample_rel.columns == ["id", "Name", "score"]
    assert sample_rel.columns_lower == ["id", "name", "score"]
    assert sample_rel.columns_lower_set == frozenset({"id", "name", "score"})


def test_column_types_metadata(sample_rel: DuckRel) -> None:
    assert sample_rel.column_types == ["INTEGER", "VARCHAR", "INTEGER"]


def test_schema_exposes_column_definitions(sample_rel: DuckRel) -> None:
    schema = sample_rel.schema
    assert isinstance(schema, DuckSchema)
    assert schema.column_names == ("id", "Name", "score")

    name_def = schema.column("Name")
    assert isinstance(name_def, ColumnDefinition)
    assert name_def.name == "Name"
    assert name_def.duckdb_type == "VARCHAR"
    assert schema.duckdb_type("id") == "INTEGER"
    assert schema.marker("score") is schema.column("score").duck_type


def test_schema_declared_marker_validation(sample_rel: DuckRel) -> None:
    with pytest.raises(TypeError):
        sample_rel.project({"bad": column("Name", duck_type=ducktypes.Integer)})


def test_schema_declared_marker_supports_custom_subclasses() -> None:
    class CustomInteger(ducktypes.Integer):
        """Custom duck type used to verify subclass compatibility."""

    schema = DuckSchema.from_components(
        ["value"],
        ["INTEGER"],
        duck_types=[CustomInteger],
    )

    schema.ensure_declared_marker(
        column="value",
        declared=CustomInteger,
        context="custom",
    )


def test_schema_declared_marker_falls_back_to_lookup() -> None:
    schema = DuckSchema.from_components(["value"], ["INTEGER"])

    schema.ensure_declared_marker(
        column="value",
        declared=ducktypes.Integer,
        context="lookup",
    )


def test_schema_declared_marker_returns_declared_type(sample_rel: DuckRel) -> None:
    marker = sample_rel.schema.ensure_declared_marker(
        column="id",
        declared=ducktypes.Integer,
        context="test",
    )

    assert marker is ducktypes.Integer
    assert_type(marker, type[ducktypes.Integer])


def test_schema_declared_marker_unknown_preserves_runtime_marker(sample_rel: DuckRel) -> None:
    marker = sample_rel.schema.ensure_declared_marker(
        column="id",
        declared=ducktypes.Unknown,
        context="test",
    )

    assert marker is ducktypes.Integer
    assert_type(marker, type[ducktypes.DuckType])


def test_row_count_returns_int(sample_rel: DuckRel) -> None:
    count = sample_rel.row_count()
    assert isinstance(count, int)
    assert count == 3


def test_row_count_empty_relation(sample_rel: DuckRel) -> None:
    empty = sample_rel.filter("1 = 0")
    count = empty.row_count()
    assert isinstance(count, int)
    assert count == 0


def test_show_passthrough_returns_relation(sample_rel: DuckRel, capsys: pytest.CaptureFixture[str]) -> None:
    rel = sample_rel.show()

    captured = capsys.readouterr()
    assert "Name" in captured.out
    assert "Alpha" in captured.out
    assert rel is sample_rel


def test_project_columns_case_insensitive(sample_rel: DuckRel) -> None:
    selected = sample_rel.project_columns("name", "ID")
    assert selected.columns == ["Name", "id"]
    assert selected.column_types == ["VARCHAR", "INTEGER"]
    assert table_rows(selected.materialize().require_table()) == [
        ("Alpha", 1),
        ("Beta", 2),
        ("Gamma", 3),
    ]


def test_project_columns_missing_ok_returns_original(sample_rel: DuckRel) -> None:
    rel = sample_rel.project_columns("missing", missing_ok=True)
    assert rel is sample_rel


def test_column_python_annotations_use_schema(sample_rel: DuckRel) -> None:
    typed = sample_rel.project(
        {
            "id": col("id", duck_type=ducktypes.Integer),
            "score": col("score", duck_type=ducktypes.Integer),
        }
    )

    assert typed.column_python_annotations == [int, int]


def test_schema_row_type_alias_uses_python_annotations(sample_rel: DuckRel) -> None:
    typed = sample_rel.project(
        {
            "id": col("id", duck_type=ducktypes.Integer),
            "name": col("Name", duck_type=ducktypes.Varchar),
            "score": col("score", duck_type=ducktypes.Integer),
        }
    )

    row_alias = typed.schema.row_type
    assert issubclass(row_alias, tuple)
    assert row_alias.__annotations__ == {"id": int, "name": str, "score": int}


class SampleRow(NamedTuple):
    id: int
    name: str
    score: int


def test_duckrel_typed_returns_namedtuple_rows(sample_rel: DuckRel) -> None:
    typed = sample_rel.project(
        {
            "id": col("id", duck_type=ducktypes.Integer),
            "name": col("Name", duck_type=ducktypes.Varchar),
            "score": col("score", duck_type=ducktypes.Integer),
        }
    )

    typed_rel = typed.typed(SampleRow)
    rows = typed_rel.fetch_typed()
    assert_type(rows, list[SampleRow])
    assert rows[0] == SampleRow(1, "Alpha", 10)


def test_drop_columns_excludes_requested(sample_rel: DuckRel) -> None:
    reduced = sample_rel.drop("score")

    assert reduced.columns == ["id", "Name"]
    assert table_rows(reduced.materialize().require_table()) == [
        (1, "Alpha"),
        (2, "Beta"),
        (3, "Gamma"),
    ]


def test_drop_columns_missing_raises(sample_rel: DuckRel) -> None:
    with pytest.raises(KeyError):
        sample_rel.drop("missing")


def test_drop_columns_missing_ok(sample_rel: DuckRel) -> None:
    reduced = sample_rel.drop("id", "missing", missing_ok=True)

    assert reduced.columns == ["Name", "score"]
    assert table_rows(reduced.materialize().require_table()) == [
        ("Alpha", 10),
        ("Beta", 5),
        ("Gamma", 8),
    ]


def test_drop_columns_missing_ok_returns_original(sample_rel: DuckRel) -> None:
    rel = sample_rel.drop("missing", missing_ok=True)

    assert rel is sample_rel


def test_project_allows_computed_columns(sample_rel: DuckRel) -> None:
    projected = sample_rel.project({"id": '"id"', "label": 'upper("Name")', "score": '"score"'})
    assert projected.columns == ["id", "label", "score"]
    assert projected.column_types == ["INTEGER", "VARCHAR", "INTEGER"]
    assert table_rows(projected.materialize().require_table())[0] == (1, "ALPHA", 10)


def test_rename_columns_uses_star_modifier(sample_rel: DuckRel) -> None:
    renamed = sample_rel.rename_columns(identifier="id", label="Name")

    assert renamed.columns == ["identifier", "label", "score"]
    assert table_rows(renamed.materialize().require_table())[0] == (1, "Alpha", 10)


def test_transform_columns_with_template(sample_rel: DuckRel) -> None:
    transformed = sample_rel.transform_columns(score="{col} * 2")

    assert transformed.columns == ["id", "Name", "score"]
    assert table_rows(transformed.materialize().require_table())[0] == (1, "Alpha", 20)


def test_add_columns_appends_new_columns(sample_rel: DuckRel) -> None:
    extended = sample_rel.add_columns(double_score='"score" * 2')

    assert extended.columns == ["id", "Name", "score", "double_score"]
    assert table_rows(extended.materialize().require_table())[0] == (1, "Alpha", 10, 20)


def test_rename_columns_missing_source(sample_rel: DuckRel) -> None:
    with pytest.raises(KeyError):
        sample_rel.rename_columns(label="missing")


def test_transform_columns_missing_target(sample_rel: DuckRel) -> None:
    with pytest.raises(KeyError):
        sample_rel.transform_columns(missing="{col} * 2")


def test_add_columns_rejects_duplicate_name(sample_rel: DuckRel) -> None:
    with pytest.raises(ValueError):
        sample_rel.add_columns(Name="1")


def test_filter_supports_parameters(sample_rel: DuckRel) -> None:
    filtered = sample_rel.filter('"id" = ? AND "Name" = ?', 2, "Beta")
    assert table_rows(filtered.materialize().require_table()) == [(2, "Beta", 5)]


def test_filter_expression_with_column_builder(sample_rel: DuckRel) -> None:
    condition = col("score") >= 8
    filtered = sample_rel.filter(condition)

    assert table_rows(filtered.materialize().require_table()) == [
        (1, "Alpha", 10),
        (3, "Gamma", 8),
    ]


def test_filter_expression_combination(sample_rel: DuckRel) -> None:
    condition = (col("id") == 1) | (col("Name") == "Beta")
    filtered = sample_rel.filter(condition)

    assert table_rows(filtered.materialize().require_table()) == [
        (1, "Alpha", 10),
        (2, "Beta", 5),
    ]


def test_filter_expression_column_comparison(connection: duckdb.DuckDBPyConnection) -> None:
    rel = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES (1, 1), (2, 3)) AS t(left_val, right_val)
            """
        )
    )

    filtered = rel.filter(col("left_val") == col("right_val"))
    assert table_rows(filtered.materialize().require_table()) == [(1, 1)]


def test_filter_expression_keyword_helpers(sample_rel: DuckRel) -> None:
    condition = equals(id=2, Name="Beta")
    filtered = sample_rel.filter(condition)

    assert table_rows(filtered.materialize().require_table()) == [(2, "Beta", 5)]


def test_filter_expression_missing_column(sample_rel: DuckRel) -> None:
    with pytest.raises(KeyError):
        sample_rel.filter(col("missing") == 1)


def test_filter_expression_raw(sample_rel: DuckRel) -> None:
    condition = FilterExpression.raw('"id" > 2')
    filtered = sample_rel.filter(condition)

    assert table_rows(filtered.materialize().require_table()) == [(3, "Gamma", 8)]


def test_filter_expression_rejects_parameters(sample_rel: DuckRel) -> None:
    condition = col("id") == 1
    with pytest.raises(TypeError):
        sample_rel.filter(condition, 1)


def test_split_returns_matching_and_remainder(sample_rel: DuckRel) -> None:
    matching, remainder = sample_rel.split('"score" >= ?', 8)

    assert table_rows(matching.materialize().require_table()) == [
        (1, "Alpha", 10),
        (3, "Gamma", 8),
    ]
    assert table_rows(remainder.materialize().require_table()) == [(2, "Beta", 5)]


def test_split_treats_nulls_as_non_matching(connection: duckdb.DuckDBPyConnection) -> None:
    rel = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 10),
                (2, NULL),
                (3, -1)
            ) AS t(id, score)
            """
        )
    )

    matching, remainder = rel.split('"score" > ?', 0)

    assert table_rows(matching.materialize().require_table()) == [(1, 10)]
    assert table_rows(remainder.materialize().require_table()) == [
        (2, None),
        (3, -1),
    ]


def test_split_accepts_filter_expression(sample_rel: DuckRel) -> None:
    matches, remainder = sample_rel.split(col("score") > 5)

    assert table_rows(matches.materialize().require_table()) == [
        (1, "Alpha", 10),
        (3, "Gamma", 8),
    ]
    assert table_rows(remainder.materialize().require_table()) == [(2, "Beta", 5)]


def test_aggregate_with_grouping(connection: duckdb.DuckDBPyConnection) -> None:
    rel = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', 10),
                ('A', 15),
                ('B', 7)
            ) AS t(category, amount)
            """
        )
    )

    aggregated = rel.aggregate(
        "category",
        aggregates={"total_amount": AggregateExpression.sum("amount")},
        order_count=AggregateExpression.count(),
    )

    table = aggregated.materialize().require_table()
    assert table.schema.names == ["category", "total_amount", "order_count"]
    assert sorted(table_rows(table)) == [
        ("A", 25, 2),
        ("B", 7, 1),
    ]


def test_aggregate_having_filter(connection: duckdb.DuckDBPyConnection) -> None:
    rel = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', 10),
                ('A', 15),
                ('B', 7)
            ) AS t(category, amount)
            """
        )
    )

    filtered = rel.aggregate(
        "category",
        total=AggregateExpression.sum("amount"),
        having_expressions=[FilterExpression.raw('SUM("amount") > 10')],
    )

    assert table_rows(filtered.materialize().require_table()) == [("A", 25)]


def test_aggregate_rejects_alias_collision(connection: duckdb.DuckDBPyConnection) -> None:
    rel = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', 10),
                ('B', 7)
            ) AS t(category, amount)
            """
        )
    )

    with pytest.raises(ValueError):
        rel.aggregate("category", category=AggregateExpression.sum("amount"))


def test_aggregate_supports_filter_and_order(connection: duckdb.DuckDBPyConnection) -> None:
    rel = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('north', 10),
                ('south', 15),
                ('east', 12),
                ('north', 5)
            ) AS t(region, amount)
            """
        )
    )

    aggregated = rel.aggregate(
        total=AggregateExpression.sum("amount", filter=col("region") != "north"),
        unique_regions=AggregateExpression.count("region", distinct=True),
        ordered_regions=AggregateExpression.function(
            "STRING_AGG",
            AggregateExpression.column("region"),
            AggregateExpression.literal(", "),
            order_by=[("amount", "desc")],
        ),
    )

    table = aggregated.materialize().require_table()
    assert table.schema.names == ["total", "unique_regions", "ordered_regions"]
    assert table_rows(table) == [(27, 3, "south, east, north, north")]


def test_aggregate_sum_amount(sales_rel: DuckRel) -> None:
    aggregated = sales_rel.aggregate(total_amount=AggregateExpression.sum("amount"))

    assert table_rows(aggregated.materialize().require_table()) == [(230,)]


def test_aggregate_group_by_region(sales_rel: DuckRel) -> None:
    grouped = sales_rel.aggregate("region", total_amount=AggregateExpression.sum("amount"))

    assert sorted(table_rows(grouped.materialize().require_table())) == [
        ("east", 20),
        ("north", 110),
        ("south", 30),
        ("west", 70),
    ]


def test_aggregate_having_sum_greater_than_100(sales_rel: DuckRel) -> None:
    filtered = sales_rel.aggregate(
        "region",
        total_amount=AggregateExpression.sum("amount"),
        having_expressions=[FilterExpression.raw('SUM("amount") > 100')],
    )

    assert table_rows(filtered.materialize().require_table()) == [("north", 110)]


def test_aggregate_distinct_region_count(sales_rel: DuckRel) -> None:
    result = sales_rel.aggregate(
        unique_regions=AggregateExpression.count("region", distinct=True)
    )

    assert table_rows(result.materialize().require_table()) == [(4,)]


def test_aggregate_filtered_sum(sales_rel: DuckRel) -> None:
    aggregates = sales_rel.aggregate(
        total_amount=AggregateExpression.sum("amount"),
        non_north_total=AggregateExpression.sum(
            "amount", filter=col("region") != "north"
        ),
    )

    assert table_rows(aggregates.materialize().require_table()) == [(230, 120)]


def test_aggregate_list_ordered_regions(sales_rel: DuckRel) -> None:
    result = sales_rel.aggregate(
        ordered_regions=AggregateExpression.function(
            "LIST",
            AggregateExpression.column("region"),
            order_by=[("amount", "desc")],
        )
    )

    assert table_rows(result.materialize().require_table()) == [
        (["west", "north", "north", "south", "east"],)
    ]


def test_aggregate_first_amount_ordered_by_date(sales_rel: DuckRel) -> None:
    result = sales_rel.aggregate(
        first_sale_amount=AggregateExpression.function(
            "FIRST",
            AggregateExpression.column("amount"),
            order_by=[("sale_date", "asc")],
        )
    )

    assert table_rows(result.materialize().require_table()) == [(30,)]


@pytest.mark.parametrize(
    "values, error",
    [
        ((), ValueError),
        ((1,), ValueError),
    ],
)
def test_filter_parameter_validation(values: tuple[object, ...], error: type[Exception], sample_rel: DuckRel) -> None:
    with pytest.raises(error):
        sample_rel.filter('"id" = ? AND "Name" = ?', *values)
    with pytest.raises(error):
        sample_rel.split('"id" = ? AND "Name" = ?', *values)


def test_natural_inner_defaults_to_shared_keys(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'L1'),
                (2, 'L2')
            ) AS t(id, left_val)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'R1'),
                (3, 'R3')
            ) AS t(ID, right_val)
            """
        )
    )

    joined = left.natural_inner(right)
    assert joined.columns == ["id", "left_val", "right_val"]
    assert joined.column_types == ["INTEGER", "VARCHAR", "VARCHAR"]
    assert table_rows(joined.materialize().require_table()) == [(1, "L1", "R1")]


def test_natural_left_with_missing_rows(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'L1'), (2, 'L2')) AS t(id, left_val)"))
    right = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'R1')) AS t(id, right_val)"))

    joined = left.natural_left(right)
    assert joined.column_types == ["INTEGER", "VARCHAR", "VARCHAR"]
    assert table_rows(joined.materialize().require_table()) == [(1, "L1", "R1"), (2, "L2", None)]


def test_natural_join_with_alias(connection: duckdb.DuckDBPyConnection) -> None:
    orders = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 100),
                (2, 200)
            ) AS t(order_id, customer_ref)
            """
        )
    )
    customers = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (100, 'Alice'),
                (300, 'Charlie')
            ) AS t(id, name)
            """
        )
    )

    joined = orders.natural_inner(customers, customer_ref="id")
    assert joined.columns == ["order_id", "customer_ref", "name"]
    assert table_rows(joined.materialize().require_table()) == [(1, 100, "Alice")]


def test_inspect_partitions_counts(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('north', 'L1'),
                ('north', 'L2'),
                ('south', 'L3')
            ) AS t(segment, payload)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('north', 'R1'),
                ('west', 'R2'),
                ('west', 'R3')
            ) AS t(segment, marker)
            """
        )
    )

    inspection = left.inspect_partitions(right, PartitionSpec.of_columns("segment")).order_by(
        segment="asc"
    )
    assert inspection.columns == [
        "segment",
        "left_count",
        "right_count",
        "pair_count",
        "shared_partition",
    ]
    rows = table_rows(inspection.materialize().require_table())
    assert rows == [
        ("north", 2, 1, 2, True),
        ("south", 1, 0, 0, False),
        ("west", 0, 2, 0, False),
    ]


def test_partitioned_inner_join_limits_matches(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'A', 'left-a'),
                (1, 'B', 'left-b')
            ) AS t(id, segment, payload)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'A', 'right-a'),
                (1, 'B', 'right-b')
            ) AS t(id, segment, marker)
            """
        )
    )

    spec = JoinSpec(equal_keys=[("id", "id")])
    joined = left.partitioned_inner(right, PartitionSpec.of_columns("segment"), spec).order_by(
        segment="asc"
    )
    rows = table_rows(joined.materialize().require_table())
    assert rows == [
        (1, "A", "left-a", "right-a"),
        (1, "B", "left-b", "right-b"),
    ]


def test_partitioned_join_conflict_raises(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'north'),
                (2, 'south')
            ) AS t(id, region)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'north', 'east'),
                (2, 'south', 'west')
            ) AS t(id, partition_region, other_region)
            """
        )
    )

    partition = PartitionSpec.from_mapping({"region": "partition_region"})
    spec = JoinSpec(equal_keys=[("region", "other_region")])

    with pytest.raises(ValueError):
        left.partitioned_inner(right, partition, spec)


def test_join_missing_shared_keys_raises(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT 1 AS a"))
    right = DuckRel(connection.sql("SELECT 1 AS b"))
    with pytest.raises(ValueError):
        left.natural_inner(right)


def test_explicit_join_requires_matching_right_columns(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'L')) AS t(id, value)"))
    right = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'R')) AS t(id_right, value)"))

    spec = JoinSpec(equal_keys=[("id", "id")])
    with pytest.raises(KeyError):
        left.inner_join(right, spec)


def test_join_projection_raises_on_collision_without_suffix(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'L')) AS t(id, value)"))
    right = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'R')) AS t(id, value)"))

    spec = JoinSpec(equal_keys=[("id", "id")])
    with pytest.raises(ValueError):
        left.inner_join(right, spec)


def test_join_projection_suffix_override(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'L')) AS t(id, value)"))
    right = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'R')) AS t(id, value)"))

    spec = JoinSpec(equal_keys=[("id", "id")])
    joined = left.inner_join(
        right, spec, project=JoinProjection(allow_collisions=True, suffixes=("_left", "_right"))
    )
    assert joined.columns == ["id", "value_left", "value_right"]
    assert table_rows(joined.materialize().require_table()) == [(1, "L", "R")]


def test_join_spec_rejects_invalid_key_sequence() -> None:
    with pytest.raises(TypeError):
        JoinSpec(equal_keys="id")  # type: ignore[arg-type]


def test_join_spec_rejects_non_predicate_entries() -> None:
    with pytest.raises(TypeError):
        JoinSpec(
            equal_keys=[("id", "id")],
            predicates=("unsupported",),  # type: ignore[arg-type]
        )


def test_explicit_join_with_predicate(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, DATE '2024-01-01'),
                (2, DATE '2024-02-01')
            ) AS t(order_id, order_date)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, DATE '2023-12-15', 'A'),
                (1, DATE '2024-01-15', 'B')
            ) AS t(customer_id, customer_since, tier)
            """
        )
    )

    spec = JoinSpec(
        equal_keys=[("order_id", "customer_id")],
        predicates=[column("order_date") >= column("customer_since")],
    )
    joined = left.left_outer(right, spec, project=JoinProjection(allow_collisions=True))
    assert joined.columns == ["order_id", "order_date", "customer_since", "tier"]
    assert table_rows(joined.materialize().require_table()) == [
        (1, date(2024, 1, 1), date(2023, 12, 15), "A"),
        (2, date(2024, 2, 1), None, None),
    ]


def test_join_predicate_requires_disambiguation(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 100, 'A')
            ) AS t(id, shared, left_label)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 200, 'B')
            ) AS t(id, shared, right_label)
            """
        )
    )

    spec = JoinSpec(
        equal_keys=[("id", "id")],
        predicates=[column("shared") >= column("right_label")],
    )

    with pytest.raises(ValueError, match="found in both relations"):
        left.inner_join(right, spec)


def test_join_predicate_missing_column(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 10)
            ) AS t(id, left_only)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 20)
            ) AS t(id, right_only)
            """
        )
    )

    spec = JoinSpec(
        equal_keys=[("id", "id")],
        predicates=[column("left_only") >= column("missing")],
    )

    with pytest.raises(KeyError, match="missing"):
        left.inner_join(right, spec)


def test_explicit_join_with_expression_predicate(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'keep'),
                (2, 'also_keep')
            ) AS t(id, label)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'primary', TRUE),
                (1, 'secondary', FALSE),
                (2, 'primary', TRUE)
            ) AS t(id, status, is_primary)
            """
        )
    )

    spec = JoinSpec(
        equal_keys=[("id", "id")],
        predicates=[ExpressionPredicate('r."is_primary"')],
    )
    joined = left.inner_join(
        right,
        spec,
        project=JoinProjection(allow_collisions=True, suffixes=("_left", "_right")),
    )

    assert joined.columns == ["id", "label", "status", "is_primary"]
    assert table_rows(joined.materialize().require_table()) == [
        (1, "keep", "primary", True),
        (2, "also_keep", "primary", True),
    ]


def test_natural_asof_backward(connection: duckdb.DuckDBPyConnection) -> None:
    trades = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:03', 10),
                ('A', TIMESTAMP '2024-01-01 00:00:06', 12),
                ('B', TIMESTAMP '2024-01-01 00:00:04', 15)
            ) AS t(symbol, trade_ts, price)
            """
        )
    )
    quotes = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:01', 9),
                ('A', TIMESTAMP '2024-01-01 00:00:05', 11),
                ('B', TIMESTAMP '2024-01-01 00:00:02', 14)
            ) AS t(symbol, quote_ts, quote_price)
            """
        )
    )

    joined = trades.natural_asof(quotes, order=AsofOrder(left="trade_ts", right="quote_ts"))
    assert joined.columns == ["symbol", "trade_ts", "price", "quote_ts", "quote_price"]
    assert table_rows(joined.materialize().require_table()) == [
        ("A", datetime(2024, 1, 1, 0, 0, 3), 10, datetime(2024, 1, 1, 0, 0, 1), 9),
        ("A", datetime(2024, 1, 1, 0, 0, 6), 12, datetime(2024, 1, 1, 0, 0, 5), 11),
        ("B", datetime(2024, 1, 1, 0, 0, 4), 15, datetime(2024, 1, 1, 0, 0, 2), 14),
    ]


def test_explicit_right_join_keeps_unmatched_rows(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'left')
            ) AS t(order_ref, left_val)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'match'),
                (2, 'orphan')
            ) AS t(order_ref, right_val)
            """
        )
    )

    spec = JoinSpec(equal_keys=[("order_ref", "order_ref")])
    joined = left.left_right(right, spec).order_by(right_val="asc")

    assert joined.columns == ["order_ref", "left_val", "order_ref_2", "right_val"]
    assert table_rows(joined.materialize().require_table()) == [
        (1, "left", 1, "match"),
        (None, None, 2, "orphan"),
    ]


def test_full_join_preserves_right_keys(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'left'),
                (3, 'only_left')
            ) AS t(order_ref, left_val)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'match'),
                (2, 'orphan')
            ) AS t(order_ref, right_val)
            """
        )
    )

    spec = JoinSpec(equal_keys=[("order_ref", "order_ref")])
    joined = left.outer_join(right, spec).order_by(order_ref="asc", order_ref_2="asc")

    assert joined.columns == ["order_ref", "left_val", "order_ref_2", "right_val"]
    assert table_rows(joined.materialize().require_table()) == [
        (1, "left", 1, "match"),
        (3, "only_left", None, None),
        (None, None, 2, "orphan"),
    ]


def test_right_join_collision_suffix_defaults(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (100, 'left_only'),
                (200, 'matched_left')
            ) AS t(customer_ref, status)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (200, 'matched_right', 'R-200'),
                (300, 'right_only', 'R-300')
            ) AS t(customer_ref, status, shipment_code)
            """
        )
    )

    spec = JoinSpec(equal_keys=[("customer_ref", "customer_ref")])
    projection = JoinProjection(allow_collisions=True)
    joined = (
        left.left_right(right, spec, project=projection)
        .order_by(customer_ref_2="asc")
    )

    assert joined.columns == [
        "customer_ref",
        "status_1",
        "customer_ref_2",
        "status_2",
        "shipment_code",
    ]
    assert table_rows(joined.materialize().require_table()) == [
        (200, "matched_left", 200, "matched_right", "R-200"),
        (None, None, 300, "right_only", "R-300"),
    ]


def test_right_join_custom_suffixes_with_join_keys(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (200, 'matched_left')
            ) AS t(customer_ref, status)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (200, 'matched_right', 'R-200')
            ) AS t(customer_ref, status, shipment_code)
            """
        )
    )

    spec = JoinSpec(equal_keys=[("customer_ref", "customer_ref")])
    projection = JoinProjection(suffixes=("_left", "_right"))
    joined = left.left_right(right, spec, project=projection)

    assert joined.columns == [
        "customer_ref_left",
        "status_left",
        "customer_ref_right",
        "status_right",
        "shipment_code",
    ]
    assert table_rows(joined.materialize().require_table()) == [
        (200, "matched_left", 200, "matched_right", "R-200"),
    ]


def test_natural_asof_forward(connection: duckdb.DuckDBPyConnection) -> None:
    trades = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:03', 10),
                ('A', TIMESTAMP '2024-01-01 00:00:06', 12),
                ('B', TIMESTAMP '2024-01-01 00:00:04', 15)
            ) AS t(symbol, trade_ts, price)
            """
        )
    )
    quotes = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:04', 13),
                ('A', TIMESTAMP '2024-01-01 00:00:07', 15),
                ('B', TIMESTAMP '2024-01-01 00:00:06', 18)
            ) AS t(symbol, quote_ts, quote_price)
            """
        )
    )

    joined = trades.natural_asof(
        quotes,
        order=AsofOrder(left="trade_ts", right="quote_ts"),
        direction="forward",
    )
    assert table_rows(joined.materialize().require_table()) == [
        ("A", datetime(2024, 1, 1, 0, 0, 3), 10, datetime(2024, 1, 1, 0, 0, 4), 13),
        ("A", datetime(2024, 1, 1, 0, 0, 6), 12, datetime(2024, 1, 1, 0, 0, 7), 15),
        ("B", datetime(2024, 1, 1, 0, 0, 4), 15, datetime(2024, 1, 1, 0, 0, 6), 18),
    ]


def test_asof_join_nearest_with_tolerance(connection: duckdb.DuckDBPyConnection) -> None:
    trades = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:06', 12),
                ('A', TIMESTAMP '2024-01-01 00:00:09', 14)
            ) AS t(symbol, trade_ts, price)
            """
        )
    )
    quotes = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:04', 10),
                ('A', TIMESTAMP '2024-01-01 00:00:08', 13),
                ('A', TIMESTAMP '2024-01-01 00:00:15', 16)
            ) AS t(symbol, quote_ts, quote_price)
            """
        )
    )

    spec = AsofSpec(
        equal_keys=[("symbol", "symbol")],
        order=AsofOrder(left="trade_ts", right="quote_ts"),
        direction="nearest",
        tolerance="3 seconds",
    )
    joined = trades.asof_join(quotes, spec)
    assert table_rows(joined.materialize().require_table()) == [
        ("A", datetime(2024, 1, 1, 0, 0, 6), 12, datetime(2024, 1, 1, 0, 0, 4), 10),
        ("A", datetime(2024, 1, 1, 0, 0, 9), 14, datetime(2024, 1, 1, 0, 0, 8), 13),
    ]


def test_semi_and_anti_join(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(id)"))
    right = DuckRel(connection.sql("SELECT * FROM (VALUES (2), (3)) AS t(id)"))

    semi = left.semi_join(right)
    anti = left.anti_join(right)

    assert table_rows(semi.materialize().require_table()) == [(2,), (3,)]
    assert table_rows(anti.materialize().require_table()) == [(1,)]


def test_cast_columns_updates_types(sample_rel: DuckRel) -> None:
    casted = sample_rel.cast_columns(id="UTINYINT")
    assert casted.column_types == ["UTINYINT", "VARCHAR", "INTEGER"]
    assert table_rows(casted.materialize().require_table())[0] == (1, "Alpha", 10)


def test_cast_columns_requires_targets(sample_rel: DuckRel) -> None:
    with pytest.raises(ValueError):
        sample_rel.cast_columns()


def test_cast_columns_rejects_unknown_type(sample_rel: DuckRel) -> None:
    with pytest.raises(ValueError):
        sample_rel.cast_columns({"id": "UNKNOWN"})  # type: ignore[arg-type]


def test_try_cast_columns_handles_invalid_values(sample_rel: DuckRel) -> None:
    casted = sample_rel.try_cast_columns({"Name": "UTINYINT"})
    assert casted.column_types == ["INTEGER", "UTINYINT", "INTEGER"]
    assert table_rows(casted.materialize().require_table())[0] == (1, None, 10)


def test_order_by_and_limit(sample_rel: DuckRel) -> None:
    ordered = sample_rel.order_by(score="desc").limit(2)
    assert table_rows(ordered.materialize().require_table()) == [(1, "Alpha", 10), (3, "Gamma", 8)]


def test_order_by_rejects_invalid_direction(sample_rel: DuckRel) -> None:
    with pytest.raises(ValueError):
        sample_rel.order_by(score="up")


def test_materialize_returns_arrow_table(sample_rel: DuckRel) -> None:
    result = sample_rel.materialize()
    table = result.require_table()
    assert isinstance(table, pa.Table)
    assert result.relation is None
    assert result.path is None


def test_materialize_into_new_connection(sample_rel: DuckRel) -> None:
    other = duckdb.connect()
    try:
        result = sample_rel.materialize(into=other)
        relation = result.require_relation()
        rows = table_rows(relation.materialize().require_table())
        assert rows == table_rows(sample_rel.materialize().require_table())
    finally:
        other.close()


def test_materialize_parquet_strategy(sample_rel: DuckRel, tmp_path: Path) -> None:
    path = tmp_path / "dataset.parquet"
    strategy = ParquetMaterializeStrategy(path)
    result = sample_rel.materialize(strategy=strategy)
    table = result.require_table()
    assert result.path == path
    assert path.exists()
    other = duckdb.connect()
    try:
        moved = sample_rel.materialize(strategy=strategy, into=other)
        relation = moved.require_relation()
        assert table_rows(relation.materialize().require_table()) == table_rows(table)
    finally:
        other.close()


def test_duckrel_df_requires_optional_dependency(
    monkeypatch: pytest.MonkeyPatch, sample_rel: DuckRel
) -> None:
    original = util_module.import_module

    def fail_import(name: str, package: str | None = None) -> object:
        if name == "pandas":
            raise ModuleNotFoundError("No module named 'pandas'")
        return original(name, package)

    monkeypatch.setattr(util_module, "import_module", fail_import)

    with pytest.raises(ModuleNotFoundError, match=r"duckplus\[pandas\]"):
        sample_rel.df()


def test_duckrel_df_returns_dataframe(sample_rel: DuckRel) -> None:
    pd = pytest.importorskip("pandas")

    frame = sample_rel.df()

    assert isinstance(frame, pd.DataFrame)
    assert list(frame.columns) == ["id", "Name", "score"]
    assert frame.loc[0, "id"] == 1


def test_duckrel_pl_requires_optional_dependency(
    monkeypatch: pytest.MonkeyPatch, sample_rel: DuckRel
) -> None:
    original = util_module.import_module

    def fail_import(name: str, package: str | None = None) -> object:
        if name == "polars":
            raise ModuleNotFoundError("No module named 'polars'")
        return original(name, package)

    monkeypatch.setattr(util_module, "import_module", fail_import)

    with pytest.raises(ModuleNotFoundError, match=r"duckplus\[polars\]"):
        sample_rel.pl()


def test_duckrel_pl_returns_dataframe(sample_rel: DuckRel) -> None:
    pl = pytest.importorskip("polars")

    frame = sample_rel.pl()

    assert isinstance(frame, pl.DataFrame)
    assert frame.shape == (3, 3)
    assert frame["id"].to_list() == [1, 2, 3]


def test_duckrel_from_pandas_roundtrip(connection: duckdb.DuckDBPyConnection) -> None:
    pd = pytest.importorskip("pandas")

    frame = pd.DataFrame({"id": [1, 2], "name": ["Alpha", "Beta"]})

    rel = DuckRel.from_pandas(frame, connection=connection)

    assert rel.columns == ["id", "name"]
    assert rel.column_types == ["BIGINT", "VARCHAR"]
    assert table_rows(rel.materialize().require_table()) == [
        (1, "Alpha"),
        (2, "Beta"),
    ]


def test_duckrel_from_polars_roundtrip() -> None:
    pl = pytest.importorskip("polars")

    frame = pl.DataFrame({"idx": [10, 11], "value": ["x", "y"]})

    rel = DuckRel.from_polars(frame)

    assert rel.columns == ["idx", "value"]
    assert rel.column_types == ["BIGINT", "VARCHAR"]
    assert table_rows(rel.materialize().require_table()) == [
        (10, "x"),
        (11, "y"),
    ]
