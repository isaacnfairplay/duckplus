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
