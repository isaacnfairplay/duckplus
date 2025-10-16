from dataclasses import FrozenInstanceError
from pathlib import Path
import csv

import duckdb
import pytest

from duckplus import DuckCon, Relation, io as io_helpers
from duckplus.typed import ducktype


def _make_relation(manager: DuckCon, query: str) -> Relation:
    with manager as connection:
        duck_relation = connection.sql(query)
        return Relation.from_relation(manager, duck_relation)


_AGGREGATE_SOURCE_SQL = """
    SELECT * FROM (VALUES
        ('a'::VARCHAR, 1::INTEGER),
        ('a'::VARCHAR, 2::INTEGER),
        ('b'::VARCHAR, 3::INTEGER)
    ) AS data(category, amount)
""".strip()


_SINGLE_ROW_SQL = """
    SELECT * FROM (VALUES
        ('a'::VARCHAR, 1::INTEGER)
    ) AS data(category, amount)
""".strip()


def test_relation_is_immutable() -> None:
    manager = DuckCon()
    relation = _make_relation(manager, "SELECT 1 AS value")

    with pytest.raises(FrozenInstanceError):
        relation.columns = ("other",)

    with pytest.raises(FrozenInstanceError):
        relation.types = ("INTEGER",)


def test_relation_metadata_populated() -> None:
    manager = DuckCon()

    relation = _make_relation(
        manager,
        "SELECT 1::INTEGER AS value, 'text'::VARCHAR AS label",
    )

    assert relation.columns == ("value", "label")
    assert relation.types == ("INTEGER", "VARCHAR")


def test_relation_from_sql_uses_active_connection() -> None:
    manager = DuckCon()

    with manager:
        relation = Relation.from_sql(manager, "SELECT 42 AS answer")

    assert relation.columns == ("answer",)
    assert relation.types == ("INTEGER",)


def test_relation_from_sql_requires_active_connection() -> None:
    manager = DuckCon()

    with pytest.raises(RuntimeError):
        Relation.from_sql(manager, "SELECT 1")


def test_relation_from_odbc_query_requires_active_connection() -> None:
    manager = DuckCon()

    with pytest.raises(RuntimeError):
        Relation.from_odbc_query(manager, "Driver=sqlite", "SELECT 1")


def test_relation_from_odbc_table_requires_active_connection() -> None:
    manager = DuckCon()

    with pytest.raises(RuntimeError):
        Relation.from_odbc_table(manager, "Driver=sqlite", "example")


def test_relation_from_excel_requires_active_connection() -> None:
    manager = DuckCon()

    with pytest.raises(RuntimeError):
        Relation.from_excel(manager, "workbook.xlsx")


def test_relation_from_excel_loads_extension_and_projects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = DuckCon()
    called: dict[str, object] = {}

    def fake_load_excel(self: DuckCon, install: bool = True) -> None:
        called["install"] = install

    monkeypatch.setattr(DuckCon, "_load_excel", fake_load_excel)

    original_sql = duckdb.DuckDBPyConnection.sql

    def fake_sql(self: duckdb.DuckDBPyConnection, sql: str):
        called["sql"] = sql
        return original_sql(self, "SELECT 1::INTEGER AS value")

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "sql", fake_sql)

    with manager:
        relation = Relation.from_excel(
            manager,
            "data.xlsx",
            sheet="Sheet1",
            header=True,
            skip=2,
            limit=5,
            names=("a", "b"),
            dtype={"a": "INTEGER"},
            all_varchar=False,
        )
        rows = relation.relation.fetchall()

    assert called["install"] is True
    assert called["sql"] == (
        "SELECT * FROM read_excel('data.xlsx', sheet='Sheet1', header=TRUE, "
        "skip=2, limit=5, names=['a', 'b'], dtype={'a': 'INTEGER'}, "
        "all_varchar=FALSE)"
    )
    assert relation.columns == ("value",)
    assert rows == [(1,)]


def test_relation_from_excel_rejects_conflicting_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = DuckCon()
    monkeypatch.setattr(DuckCon, "_load_excel", lambda self, install=True: None)

    with manager:
        with pytest.raises(ValueError, match="skip"):
            Relation.from_excel(
                manager,
                "data.xlsx",
                skip=1,
                skiprows=2,
            )


def test_transform_replaces_column_values() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value, 2::INTEGER AS other"),
        )

        transformed = relation.transform(value="value + other")

        assert transformed.columns == ("value", "other")
        assert transformed.types == ("INTEGER", "INTEGER")
        assert transformed.relation.fetchall() == [(3, 2)]


def test_transform_supports_simple_casts() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        transformed = relation.transform(value=str)

        assert transformed.types == ("VARCHAR",)
        assert transformed.relation.fetchall() == [("1",)]


def test_transform_matches_columns_case_insensitively() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        transformed = relation.transform(VALUE="value + 1")

        assert transformed.relation.fetchall() == [(2,)]


def test_transform_rejects_unknown_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(KeyError):
            relation.transform(other="value")


def test_transform_validates_expression_references() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(ValueError, match="unknown columns"):
            relation.transform(value="missing + 1")


def test_transform_requires_replacements() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(ValueError):
            relation.transform()


def test_transform_rejects_unsupported_casts() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(TypeError):
            relation.transform(value=complex)


def test_transform_requires_open_connection() -> None:
    manager = DuckCon()
    relation = _make_relation(manager, "SELECT 1::INTEGER AS value")

    with pytest.raises(RuntimeError):
        relation.transform(value="value + 1")


def test_add_appends_new_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 2::INTEGER AS value"),
        )

        extended = relation.add(double="value * 2", triple="value * 3")

        assert extended.columns == ("value", "double", "triple")
        assert extended.types == ("INTEGER", "INTEGER", "INTEGER")
        assert extended.relation.fetchall() == [(2, 4, 6)]


def test_add_rejects_forward_references() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 3::INTEGER AS value"),
        )

        with pytest.raises(ValueError, match="unknown columns"):
            relation.add(double="quadruple * 2", quadruple="value * 4")


def test_add_rejects_dependent_expressions() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 3::INTEGER AS value"),
        )

        with pytest.raises(ValueError, match="unknown columns"):
            relation.add(double="value * 2", quadruple="double * 2")


def test_add_rejects_dependent_expressions_with_quoted_aliases() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 4::INTEGER AS value"),
        )

        expressions = dict(
            [
                ("spaced name", '"value" * 2'),
                ("other alias", '"spaced name" * 2'),
            ]
        )

        with pytest.raises(ValueError, match="unknown columns"):
            relation.add(**expressions)


def test_add_rejects_existing_columns_case_insensitively() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(ValueError, match="already exist"):
            relation.add(VALUE="value + 1")


def test_add_rejects_existing_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(ValueError, match="already exist"):
            relation.add(value="value + 1")


def test_add_rejects_invalid_expression_types() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(TypeError, match="SQL strings"):
            relation.add(double=123)


def test_add_rejects_blank_expressions() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(ValueError, match="cannot be empty"):
            relation.add(double="   ")


def test_add_accepts_typed_expressions() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 2::INTEGER AS value, 3::INTEGER AS other"),
        )

        extended = relation.add(
            total=ducktype.Numeric("value") + ducktype.Numeric("other"),
            delta=ducktype.Numeric.literal(10),
        )

        assert extended.columns == ("value", "other", "total", "delta")
        assert extended.types == ("INTEGER", "INTEGER", "INTEGER", "INTEGER")
        assert extended.relation.fetchall() == [(2, 3, 5, 10)]


def test_add_typed_expression_rejects_new_column_dependencies() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 3::INTEGER AS value"),
        )

        with pytest.raises(ValueError, match="unknown columns"):
            relation.add(
                double=ducktype.Numeric("value") * 2,
                quadruple=ducktype.Numeric("double") * 2,
            )


def test_add_requires_open_connection() -> None:
    manager = DuckCon()
    relation = _make_relation(manager, "SELECT 1::INTEGER AS value")

    with pytest.raises(RuntimeError):
        relation.add(double="value * 2")


def test_add_validates_expression_references() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(ValueError, match="unknown columns"):
            relation.add(double="missing + 1")


def test_rename_updates_column_names() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value, 2::INTEGER AS other"),
        )

        renamed = relation.rename(value="first", other="second")

        assert renamed.columns == ("first", "second")
        assert renamed.types == ("INTEGER", "INTEGER")
        assert renamed.relation.fetchall() == [(1, 2)]


def test_rename_matches_columns_case_insensitively() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        renamed = relation.rename(VALUE="first")

        assert renamed.columns == ("first",)


def test_rename_rejects_unknown_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(KeyError):
            relation.rename(other="value")


def test_rename_rejects_duplicate_targets() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value, 2::INTEGER AS other"),
        )

        with pytest.raises(ValueError, match="duplicate column names"):
            relation.rename(value="other")


def test_rename_rejects_invalid_targets() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(ValueError):
            relation.rename(value="")


def test_rename_requires_open_connection() -> None:
    manager = DuckCon()
    relation = _make_relation(
        manager,
        "SELECT 1::INTEGER AS value, 2::INTEGER AS other",
    )

    with pytest.raises(RuntimeError):
        relation.rename(value="first")


def test_rename_if_exists_skips_missing_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.warns(UserWarning, match="skipped"):
            renamed = relation.rename_if_exists(value="first", other="second")

        assert renamed.columns == ("first",)
        assert renamed.relation.fetchall() == [(1,)]


def test_rename_if_exists_returns_original_when_nothing_to_rename() -> None:
    manager = DuckCon()
    relation = _make_relation(manager, "SELECT 1::INTEGER AS value")

    with pytest.warns(UserWarning):
        result = relation.rename_if_exists(other="second")

    assert result is relation


def test_keep_projects_requested_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(
                "SELECT 1::INTEGER AS value, 2::INTEGER AS other, 3::INTEGER AS extra"
            ),
        )

        subset = relation.keep("OTHER", "value")

        assert subset.columns == ("other", "value")
        assert subset.relation.fetchall() == [(2, 1)]


def test_keep_rejects_unknown_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(KeyError):
            relation.keep("missing")


def test_keep_requires_columns() -> None:
    manager = DuckCon()
    relation = _make_relation(
        manager,
        "SELECT 1::INTEGER AS value, 2::INTEGER AS other",
    )

    with pytest.raises(ValueError):
        relation.keep()


def test_keep_requires_open_connection() -> None:
    manager = DuckCon()
    relation = _make_relation(
        manager,
        "SELECT 1::INTEGER AS value, 2::INTEGER AS other",
    )

    with pytest.raises(RuntimeError):
        relation.keep("value")


def test_keep_if_exists_skips_missing_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value, 2::INTEGER AS other"),
        )

        with pytest.warns(UserWarning, match="skipped"):
            subset = relation.keep_if_exists("value", "missing")

        assert subset.columns == ("value",)
        assert subset.relation.fetchall() == [(1,)]


def test_keep_if_exists_returns_original_when_nothing_to_keep() -> None:
    manager = DuckCon()
    relation = _make_relation(manager, "SELECT 1::INTEGER AS value")

    with pytest.warns(UserWarning):
        result = relation.keep_if_exists("missing")

    assert result is relation


def test_drop_removes_requested_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(
                "SELECT 1::INTEGER AS value, 2::INTEGER AS other, 3::INTEGER AS extra"
            ),
        )

        reduced = relation.drop("OTHER")

        assert reduced.columns == ("value", "extra")
        assert reduced.relation.fetchall() == [(1, 3)]


def test_drop_rejects_unknown_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )

        with pytest.raises(KeyError):
            relation.drop("missing")


def test_drop_requires_columns() -> None:
    manager = DuckCon()
    relation = _make_relation(manager, "SELECT 1::INTEGER AS value")

    with pytest.raises(ValueError):
        relation.drop()


def test_drop_requires_open_connection() -> None:
    manager = DuckCon()
    relation = _make_relation(
        manager,
        "SELECT 1::INTEGER AS value, 2::INTEGER AS other",
    )

    with pytest.raises(RuntimeError):
        relation.drop("value")


def test_drop_if_exists_skips_missing_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(
                "SELECT 1::INTEGER AS value, 2::INTEGER AS other, 3::INTEGER AS extra"
            ),
        )

        with pytest.warns(UserWarning, match="skipped"):
            reduced = relation.drop_if_exists("missing", "other")

        assert reduced.columns == ("value", "extra")
        assert reduced.relation.fetchall() == [(1, 3)]


def test_drop_if_exists_returns_original_when_nothing_to_drop() -> None:
    manager = DuckCon()
    relation = _make_relation(manager, "SELECT 1::INTEGER AS value")

    with pytest.warns(UserWarning):
        result = relation.drop_if_exists("missing")

    assert result is relation


def test_aggregate_groups_rows_and_computes_aggregates() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_AGGREGATE_SOURCE_SQL),
        )

        aggregated = relation.aggregate(
            ("category",),
            total="sum(amount)",
            average="avg(amount)",
        )

        ordered = aggregated.relation.order("category").fetchall()
        assert ordered == [("a", 3, 1.5), ("b", 3, 3.0)]
        assert aggregated.columns == ("category", "total", "average")


def test_aggregate_accepts_typed_expressions() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_AGGREGATE_SOURCE_SQL),
        )

        aggregated = relation.aggregate(
            ("CATEGORY",),
            total=ducktype.Numeric.Aggregate.sum("amount"),
        )

        assert aggregated.columns == ("category", "total")
        assert aggregated.relation.order("category").fetchall() == [
            ("a", 3),
            ("b", 3),
        ]


def test_aggregate_supports_filters() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_AGGREGATE_SOURCE_SQL),
        )

        string_filtered = relation.aggregate(
            ("category",),
            "amount > 1",
            total="sum(amount)",
        )

        typed_filtered = relation.aggregate(
            ("category",),
            ducktype.Boolean.raw("amount > 1", dependencies=["amount"]),
            total="sum(amount)",
        )

        expected = [("a", 2), ("b", 3)]
        assert string_filtered.relation.order("category").fetchall() == expected
        assert typed_filtered.relation.order("category").fetchall() == expected


def test_aggregate_rejects_unknown_group_by_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_SINGLE_ROW_SQL),
        )

        with pytest.raises(KeyError):
            relation.aggregate(("missing",), total="sum(amount)")


def test_aggregate_rejects_unknown_aggregation_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_SINGLE_ROW_SQL),
        )

        with pytest.raises(ValueError, match="unknown columns"):
            relation.aggregate(("category",), total="sum(missing)")


def test_aggregate_rejects_typed_expression_with_unknown_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_SINGLE_ROW_SQL),
        )

        with pytest.raises(ValueError, match="unknown columns"):
            relation.aggregate(
                ("category",),
                total=ducktype.Numeric.Aggregate.sum("missing"),
            )


def test_aggregate_rejects_non_boolean_filters() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_SINGLE_ROW_SQL),
        )

        with pytest.raises(TypeError):
            relation.aggregate(("category",), ducktype.Numeric("amount"), total="sum(amount)")


def test_aggregate_rejects_blank_filters() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_SINGLE_ROW_SQL),
        )

        with pytest.raises(ValueError):
            relation.aggregate(("category",), "   ", total="sum(amount)")


def test_aggregate_requires_aggregations() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_SINGLE_ROW_SQL),
        )

        with pytest.raises(ValueError):
            relation.aggregate(("category",))


def test_aggregate_requires_open_connection() -> None:
    manager = DuckCon()
    relation = _make_relation(manager, _SINGLE_ROW_SQL)

    with pytest.raises(RuntimeError):
        relation.aggregate(("category",), total="sum(amount)")


def test_aggregate_rejects_duplicate_aggregation_names() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_SINGLE_ROW_SQL),
        )

        with pytest.raises(ValueError, match="specified multiple times"):
            relation.aggregate(("category",), total="sum(amount)", TOTAL="avg(amount)")


def test_aggregate_rejects_mismatched_alias_typed_expressions() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_SINGLE_ROW_SQL),
        )

        expression = ducktype.Numeric.Aggregate.sum("amount").alias("other")

        with pytest.raises(ValueError, match="aggregate must use the same alias"):
            relation.aggregate(("category",), total=expression)


def test_filter_applies_multiple_conditions() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_AGGREGATE_SOURCE_SQL),
        )

        filtered = relation.filter(
            "amount > 1",
            ducktype.Boolean.raw("category = 'b'", dependencies=["category"]),
        )

        assert filtered.columns == relation.columns
        assert filtered.relation.order("category").fetchall() == [("b", 3)]


def test_filter_rejects_unknown_columns_in_strings() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_AGGREGATE_SOURCE_SQL),
        )

        with pytest.raises(ValueError, match="unknown columns"):
            relation.filter("missing > 1")


def test_filter_rejects_unknown_columns_in_typed_expressions() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_AGGREGATE_SOURCE_SQL),
        )

        with pytest.raises(ValueError, match="unknown columns"):
            relation.filter(
                ducktype.Boolean.raw(
                    "missing > 1",
                    dependencies=["missing"],
                )
            )


def test_filter_rejects_non_boolean_typed_conditions() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_AGGREGATE_SOURCE_SQL),
        )

        with pytest.raises(TypeError):
            relation.filter(ducktype.Numeric("amount"))


def test_filter_rejects_blank_conditions() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_AGGREGATE_SOURCE_SQL),
        )

        with pytest.raises(ValueError):
            relation.filter("   ")


def test_filter_requires_conditions() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql(_AGGREGATE_SOURCE_SQL),
        )

        with pytest.raises(ValueError):
            relation.filter()


def test_filter_requires_open_connection() -> None:
    manager = DuckCon()
    relation = _make_relation(manager, _AGGREGATE_SOURCE_SQL)

    with pytest.raises(RuntimeError):
        relation.filter("amount > 1")


def test_join_uses_shared_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        left = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER, 'north'::VARCHAR),
                    (2::INTEGER, 'south'::VARCHAR)
                ) AS data(id, region)
                """
            ),
        )
        right = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER, 'north'::VARCHAR, 100::INTEGER),
                    (1::INTEGER, 'north'::VARCHAR, 200::INTEGER),
                    (3::INTEGER, 'east'::VARCHAR, 300::INTEGER)
                ) AS data(id, region, amount)
                """
            ),
        )

        joined = left.join(right)

        assert joined.columns == ("id", "region", "amount")
        assert joined.relation.order("id").fetchall() == [
            (1, "north", 100),
            (1, "north", 200),
        ]


def test_join_supports_explicit_pairs() -> None:
    manager = DuckCon()
    with manager as connection:
        customers = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER, 'north'::VARCHAR),
                    (2::INTEGER, 'south'::VARCHAR)
                ) AS data(customer_id, region)
                """
            ),
        )
        orders = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    ('north'::VARCHAR, 1::INTEGER, 500::INTEGER),
                    ('south'::VARCHAR, 2::INTEGER, 700::INTEGER)
                ) AS data(region, order_customer_id, total)
                """
            ),
        )

        joined = customers.join(orders, on={"customer_id": "order_customer_id"})

        assert joined.columns == (
            "customer_id",
            "region",
            "order_customer_id",
            "total",
        )
        assert joined.relation.order("customer_id").fetchall() == [
            (1, "north", 1, 500),
            (2, "south", 2, 700),
        ]


def test_join_requires_join_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        left = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS value"),
        )
        right = Relation.from_relation(
            manager,
            connection.sql("SELECT 2::INTEGER AS other"),
        )

        with pytest.raises(ValueError, match="requires at least one"):
            left.join(right)


def test_join_rejects_unknown_explicit_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        left = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS id"),
        )
        right = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS other_id"),
        )

        with pytest.raises(KeyError):
            left.join(right, on={"missing": "other_id"})


def test_join_requires_matching_duckcon() -> None:
    left_manager = DuckCon()
    right_manager = DuckCon()
    left = _make_relation(left_manager, "SELECT 1::INTEGER AS id")
    right = _make_relation(right_manager, "SELECT 1::INTEGER AS id")

    with left_manager:
        with pytest.raises(ValueError, match="same DuckCon"):
            left.join(right)


def test_left_join_retains_unmatched_rows() -> None:
    manager = DuckCon()
    with manager as connection:
        left = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER, 'north'::VARCHAR),
                    (2::INTEGER, 'south'::VARCHAR)
                ) AS data(id, region)
                """
            ),
        )
        right = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER, 100::INTEGER)
                ) AS data(id, amount)
                """
            ),
        )

        joined = left.left_join(right)

        assert joined.columns == ("id", "region", "amount")
        assert joined.relation.order("id").fetchall() == [
            (1, "north", 100),
            (2, "south", None),
        ]


def test_semi_join_returns_only_left_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        left = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER, 'north'::VARCHAR),
                    (2::INTEGER, 'south'::VARCHAR),
                    (3::INTEGER, 'east'::VARCHAR)
                ) AS data(id, region)
                """
            ),
        )
        right = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER),
                    (3::INTEGER)
                ) AS data(id)
                """
            ),
        )

        joined = left.semi_join(right)

        assert joined.columns == ("id", "region")
        assert joined.relation.order("id").fetchall() == [
            (1, "north"),
            (3, "east"),
        ]


def test_join_requires_open_connection() -> None:
    manager = DuckCon()
    left = _make_relation(manager, "SELECT 1::INTEGER AS id")
    right = _make_relation(manager, "SELECT 1::INTEGER AS id")

    with pytest.raises(RuntimeError):
        left.join(right)


def test_asof_join_matches_previous_rows() -> None:
    manager = DuckCon()
    with manager as connection:
        trades = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER, 10::INTEGER),
                    (2::INTEGER, 20::INTEGER),
                    (1::INTEGER, 35::INTEGER)
                ) AS data(symbol, event_ts)
                """
            ),
        )
        quotes = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER, 5::INTEGER, 100::INTEGER),
                    (1::INTEGER, 30::INTEGER, 110::INTEGER),
                    (2::INTEGER, 19::INTEGER, 90::INTEGER)
                ) AS data(symbol, quote_ts, price)
                """
            ),
        )

        joined = trades.asof_join(
            quotes,
            on={"symbol": "symbol"},
            order=("event_ts", "quote_ts"),
        )

        assert joined.columns == ("symbol", "event_ts", "quote_ts", "price")
        assert joined.relation.order("event_ts").fetchall() == [
            (1, 10, 5, 100),
            (2, 20, 19, 90),
            (1, 35, 30, 110),
        ]


def test_asof_join_respects_tolerance() -> None:
    manager = DuckCon()
    with manager as connection:
        left = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER, 10::INTEGER),
                    (1::INTEGER, 50::INTEGER)
                ) AS data(symbol, event_ts)
                """
            ),
        )
        right = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER, 3::INTEGER, 100::INTEGER),
                    (1::INTEGER, 30::INTEGER, 200::INTEGER)
                ) AS data(symbol, quote_ts, price)
                """
            ),
        )

        joined = left.asof_join(
            right,
            on={"symbol": "symbol"},
            order=("event_ts", "quote_ts"),
            tolerance=15,
        )

        assert joined.relation.fetchall() == [(1, 10, 3, 100)]


def test_asof_join_supports_typed_operands() -> None:
    manager = DuckCon()
    with manager as connection:
        events = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER, 10::INTEGER, 7::INTEGER),
                    (1::INTEGER, 20::INTEGER, 3::INTEGER),
                    (1::INTEGER, 50::INTEGER, 10::INTEGER)
                ) AS data(symbol, event_ts, max_gap)
                """
            ),
        )
        snapshots = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT * FROM (VALUES
                    (1::INTEGER, 4::INTEGER, 100::INTEGER),
                    (1::INTEGER, 18::INTEGER, 200::INTEGER),
                    (1::INTEGER, 45::INTEGER, 300::INTEGER)
                ) AS data(symbol, quote_ts, price)
                """
            ),
        )

        joined = events.asof_join(
            snapshots,
            on={"symbol": "symbol"},
            order=(
                ducktype.Numeric.coerce(("left", "event_ts")),
                ducktype.Numeric.coerce(("right", "quote_ts")),
            ),
            tolerance=ducktype.Numeric.coerce(("left", "max_gap")),
        )

        assert joined.relation.order("event_ts").fetchall() == [
            (1, 10, 7, 4, 100),
            (1, 20, 3, 18, 200),
            (1, 50, 10, 45, 300),
        ]


def test_asof_join_rejects_unknown_order_column() -> None:
    manager = DuckCon()
    with manager as connection:
        left = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS id, 10::INTEGER AS event_ts"),
        )
        right = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS id, 5::INTEGER AS quote_ts"),
        )

        with pytest.raises(KeyError):
            left.asof_join(right, on={"id": "id"}, order=("missing", "quote_ts"))


def test_asof_join_requires_open_connection() -> None:
    manager = DuckCon()
    left = _make_relation(manager, "SELECT 1::INTEGER AS id, 1::INTEGER AS value")
    right = _make_relation(manager, "SELECT 1::INTEGER AS id, 1::INTEGER AS other")

    with pytest.raises(RuntimeError):
        left.asof_join(right, on={"id": "id"}, order=("value", "other"))


def test_asof_join_rejects_invalid_direction() -> None:
    manager = DuckCon()
    with manager as connection:
        left = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS id, 1::INTEGER AS event_ts"),
        )
        right = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS id, 1::INTEGER AS quote_ts"),
        )

        with pytest.raises(ValueError, match="direction"):
            left.asof_join(
                right,
                on={"id": "id"},
                order=("event_ts", "quote_ts"),
                direction="nearest",  # type: ignore[arg-type]
            )


def test_relation_append_csv_writes_rows(tmp_path: Path) -> None:
    target = tmp_path / "data.csv"

    manager = DuckCon()
    with manager:
        relation = Relation.from_sql(
            manager,
            """
            SELECT * FROM (VALUES
                (1::INTEGER, 'north'::VARCHAR),
                (2::INTEGER, 'south'::VARCHAR)
            ) AS data(id, region)
            """.strip(),
        )

        result = relation.append_csv(target)
        assert result.columns == ("id", "region")

    with target.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    assert rows == [["id", "region"], ["1", "north"], ["2", "south"]]


def test_relation_append_csv_handles_large_batches(tmp_path: Path) -> None:
    target = tmp_path / "large.csv"

    manager = DuckCon()
    with manager as connection:
        large_relation = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT
                    range AS id,
                    CONCAT('value_', range::VARCHAR) AS label
                FROM range(0, 2000) AS data(range)
                """.strip(),
            ),
        )

        large_relation.append_csv(target)

    with target.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    assert rows[0] == ["id", "label"]
    assert len(rows) == 2001
    assert rows[1] == ["0", "value_0"]
    assert rows[-1] == ["1999", "value_1999"]


def test_relation_append_csv_match_all_columns_deduplicates_large_files(
    tmp_path: Path,
) -> None:
    target = tmp_path / "dedupe.csv"

    manager = DuckCon()
    with manager as connection:
        initial = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT
                    range AS id,
                    CONCAT('value_', range::VARCHAR) AS label
                FROM range(0, 1000) AS data(range)
                """.strip(),
            ),
        )
        initial.append_csv(target)

        update = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT
                    range AS id,
                    CONCAT('value_', range::VARCHAR) AS label
                FROM range(500, 1500) AS data(range)
                """.strip(),
            ),
        )
        update.append_csv(target, match_all_columns=True)

    with target.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    assert rows[0] == ["id", "label"]
    data_rows = rows[1:]
    ids = {int(row[0]) for row in data_rows}
    assert len(data_rows) == 1500
    assert ids == set(range(1500))


def test_relation_append_csv_unique_id_skips_duplicates(tmp_path: Path) -> None:
    target = tmp_path / "data.csv"
    target.write_text("id,region\n1,north\n", encoding="utf-8")

    manager = DuckCon()
    with manager:
        relation = Relation.from_sql(
            manager,
            """
            SELECT * FROM (VALUES
                (1::INTEGER, 'north'::VARCHAR),
                (2::INTEGER, 'south'::VARCHAR)
            ) AS data(id, region)
            """.strip(),
        )

        relation.append_csv(target, unique_id_column="id")

    with target.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    assert rows == [["id", "region"], ["1", "north"], ["2", "south"]]


def test_relation_append_csv_mutate_false_leaves_file_unchanged(tmp_path: Path) -> None:
    target = tmp_path / "data.csv"
    target.write_text("id\n1\n", encoding="utf-8")

    manager = DuckCon()
    with manager:
        relation = Relation.from_sql(
            manager,
            "SELECT * FROM (VALUES (1::INTEGER), (2::INTEGER)) AS data(id)",
        )

        result = relation.append_csv(
            target,
            unique_id_column="id",
            mutate=False,
        )

        assert result.relation.fetchall() == [(2,)]

    assert target.read_text(encoding="utf-8") == "id\n1\n"


def test_relation_append_parquet_appends_rows(tmp_path: Path) -> None:
    target = tmp_path / "data.parquet"
    connection = duckdb.connect()
    try:
        connection.sql(
            "SELECT 1::INTEGER AS id, 'north'::VARCHAR AS region"
        ).write_parquet(str(target), overwrite=True)
    finally:
        connection.close()

    manager = DuckCon()
    with manager:
        relation = Relation.from_sql(
            manager,
            """
            SELECT * FROM (VALUES
                (1::INTEGER, 'north'::VARCHAR),
                (2::INTEGER, 'south'::VARCHAR)
            ) AS data(id, region)
            """.strip(),
        )

        relation.append_parquet(target, unique_id_column="id", mutate=True)

    rows = duckdb.read_parquet(str(target)).order("id").fetchall()
    assert rows == [(1, "north"), (2, "south")]


def test_relation_append_parquet_mutate_false_returns_rows(tmp_path: Path) -> None:
    target = tmp_path / "data.parquet"
    connection = duckdb.connect()
    try:
        connection.sql(
            "SELECT 1::INTEGER AS id, 'north'::VARCHAR AS region"
        ).write_parquet(str(target), overwrite=True)
    finally:
        connection.close()

    manager = DuckCon()
    with manager:
        relation = Relation.from_sql(
            manager,
            "SELECT * FROM (VALUES (1::INTEGER, 'north'::VARCHAR), (2, 'south')) AS data(id, region)",
        )

        result = relation.append_parquet(
            target,
            unique_id_column="id",
            mutate=False,
        )

        assert result.relation.fetchall() == [(2, "south")]

    rows = duckdb.read_parquet(str(target)).fetchall()
    assert rows == [(1, "north")]


def test_relation_append_parquet_large_batches_use_temp_directory(tmp_path: Path) -> None:
    target = tmp_path / "large.parquet"
    temp_directory = tmp_path / "staging"

    manager = DuckCon()
    with manager as connection:
        initial = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT
                    range AS id,
                    CONCAT('value_', range::VARCHAR) AS label
                FROM range(0, 750) AS data(range)
                """.strip(),
            ),
        )
        initial.append_parquet(target, mutate=True)

        update = Relation.from_relation(
            manager,
            connection.sql(
                """
                SELECT
                    range AS id,
                    CONCAT('value_', range::VARCHAR) AS label
                FROM range(500, 1250) AS data(range)
                """.strip(),
            ),
        )
        update.append_parquet(
            target,
            unique_id_column="id",
            mutate=True,
            temp_directory=temp_directory,
        )

    rows = duckdb.read_parquet(str(target)).order("id").fetchall()
    assert len(rows) == 1250
    assert rows[0] == (0, "value_0")
    assert rows[-1] == (1249, "value_1249")
    assert target.exists()
    assert temp_directory.exists()
    assert list(temp_directory.iterdir()) == []


def test_relation_append_parquet_rejects_directory(tmp_path: Path) -> None:
    manager = DuckCon()
    with manager:
        relation = Relation.from_sql(
            manager,
            "SELECT 1::INTEGER AS id",
        )

        with pytest.raises(ValueError, match="Parquet file"):
            relation.append_parquet(tmp_path)


def test_relation_write_parquet_dataset_partition_actions(tmp_path: Path) -> None:
    dataset = tmp_path / "dataset"
    dataset.mkdir()

    connection = duckdb.connect()
    try:
        connection.sql(
            "SELECT 'prefix_0' AS partition_key, 1 AS value"
        ).write_parquet(str(dataset / "prefix_0.parquet"), overwrite=True)
        connection.sql(
            "SELECT '1' AS partition_key, 5 AS value"
        ).write_parquet(str(dataset / "1.parquet"), overwrite=True)
    finally:
        connection.close()

    manager = DuckCon()
    with manager:
        relation = Relation.from_sql(
            manager,
            """
            SELECT * FROM (
                VALUES
                    ('prefix_0'::VARCHAR, 2::INTEGER),
                    ('1'::VARCHAR, 10::INTEGER)
            ) AS data(partition_key, value)
            """.strip(),
        )

        relation.write_parquet_dataset(
            dataset,
            partition_column="partition_key",
            partition_actions={"prefix_0": "append", "1": "overwrite"},
        )

        dataset_relation = io_helpers.read_parquet(
            manager,
            dataset,
            directory=True,
            partition_id_column="partition_id",
        )
        partition_idx = dataset_relation.columns.index("partition_id")
        value_idx = dataset_relation.columns.index("value")
        grouped: dict[str, list[int]] = {}
        for row in dataset_relation.relation.fetchall():
            key = row[partition_idx]
            grouped.setdefault(key, []).append(row[value_idx])

    assert sorted(grouped["prefix_0"]) == [1, 2]
    assert grouped["1"] == [10]


def test_relation_write_parquet_dataset_immutable_enforces_new_partitions(
    tmp_path: Path,
) -> None:
    dataset = tmp_path / "immutable"

    manager = DuckCon()
    with manager:
        relation = Relation.from_sql(
            manager,
            "SELECT 'fresh'::VARCHAR AS partition_key, 42::INTEGER AS value",
        )

        relation.write_parquet_dataset(
            dataset,
            partition_column="partition_key",
            immutable=True,
        )

        stored_rows = duckdb.read_parquet(str(dataset / "fresh.parquet")).fetchall()
        assert stored_rows == [("fresh", 42)]

        with pytest.raises(ValueError, match="immutable"):
            relation.write_parquet_dataset(
                dataset,
                partition_column="partition_key",
                immutable=True,
            )
