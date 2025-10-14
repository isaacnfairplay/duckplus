from dataclasses import FrozenInstanceError

import pytest

from duckplus import DuckCon, Relation


def _make_relation(manager: DuckCon, query: str) -> Relation:
    with manager as connection:
        duck_relation = connection.sql(query)
        return Relation.from_relation(manager, duck_relation)


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
