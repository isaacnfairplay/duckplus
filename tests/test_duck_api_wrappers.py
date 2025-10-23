from __future__ import annotations

from typing import SupportsInt

from pathlib import Path

from typing import Callable

import duckdb
from duckdb import sqltypes
import pytest
from pytest import approx

from duckplus import DuckCon
from duckplus.relation import Relation
from duckplus.static_typed.types import parse_type


def test_duckcon_execute_and_sql_roundtrip() -> None:
    duckcon = DuckCon()
    with duckcon:
        duckcon.execute("CREATE TABLE numbers(n INTEGER)")
        duckcon.execute("INSERT INTO numbers VALUES (1), (2)")
        relation = duckcon.sql("SELECT n FROM numbers ORDER BY n")
        assert isinstance(relation, Relation)
        assert relation.fetchall() == [(1,), (2,)]


def test_duckcon_transaction_helpers() -> None:
    duckcon = DuckCon()
    with duckcon:
        duckcon.begin()
        duckcon.execute("CREATE TABLE tx_values(v INTEGER)")
        duckcon.rollback()
        relation = duckcon.sql(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'tx_values'"
        )
        assert relation.fetchone() == (0,)


def test_duckcon_close_idempotent() -> None:
    duckcon = DuckCon()
    with duckcon:
        assert duckcon.is_open
        duckcon.close()
        assert not duckcon.is_open
        with pytest.raises(RuntimeError):
            duckcon.execute("SELECT 1")
    assert not duckcon.is_open


def test_duckcon_fetch_helpers() -> None:
    duckcon = DuckCon()
    with duckcon:
        duckcon.execute("CREATE TABLE conn_fetch(v INTEGER)")
        duckcon.execute("INSERT INTO conn_fetch VALUES (0), (1), (2)")

        duckcon.execute("SELECT v FROM conn_fetch ORDER BY v")
        assert duckcon.fetchone() == (0,)
        assert duckcon.fetchmany(1) == [(1,)]
        assert duckcon.fetchall() == [(2,)]


def test_duckcon_fetchmany_validates_input() -> None:
    duckcon = DuckCon()
    with duckcon:
        duckcon.execute("SELECT 1")
        with pytest.raises(TypeError):
            duckcon.fetchmany(True)
        with pytest.raises(ValueError):
            duckcon.fetchmany(0)


def test_duckcon_dataframe_fetch_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, object]] = []

    def fake_require_module(module: str, helper: str, install_hint: str) -> object:
        calls.append((module, helper))
        return object()

    def fake_fetch_df(self: duckdb.DuckDBPyConnection, *, date_as_object: bool = False) -> object:
        calls.append(("fetch_df", date_as_object))
        return {"df": date_as_object}

    def fake_fetch_df_chunk(
        self: duckdb.DuckDBPyConnection,
        vectors_per_chunk: SupportsInt = 1,
        *,
        date_as_object: bool = False,
    ) -> object:
        calls.append(("fetch_df_chunk", (int(vectors_per_chunk), date_as_object)))
        return {"chunk": int(vectors_per_chunk)}

    monkeypatch.setattr(DuckCon, "_require_module", staticmethod(fake_require_module))
    monkeypatch.setattr(
        duckdb.DuckDBPyConnection,
        "fetch_df",
        fake_fetch_df,
        raising=False,
    )
    monkeypatch.setattr(
        duckdb.DuckDBPyConnection,
        "fetch_df_chunk",
        fake_fetch_df_chunk,
        raising=False,
    )

    duckcon = DuckCon()
    with duckcon:
        duckcon.execute("SELECT 1")
        assert duckcon.fetch_df() == {"df": False}
        assert duckcon.fetchdf(date_as_object=True) == {"df": True}
        assert duckcon.fetch_df_chunk() == {"chunk": 1}
        assert duckcon.fetch_df_chunk(5, date_as_object=True) == {"chunk": 5}

    assert ("pandas", "DuckCon.fetch_df") in calls
    assert ("pandas", "DuckCon.fetch_df_chunk") in calls
    assert ("fetch_df", False) in calls
    assert ("fetch_df", True) in calls
    assert ("fetch_df_chunk", (1, False)) in calls
    assert ("fetch_df_chunk", (5, True)) in calls


def test_duckcon_numpy_and_arrow_fetch_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []

    def fake_require_module(module: str, helper: str, install_hint: str) -> object:
        calls.append((module, helper))
        return object()

    def fake_fetchnumpy(self: duckdb.DuckDBPyConnection) -> dict[str, list[int]]:
        return {"v": [1, 2]}

    def fake_fetch_arrow_table(
        self: duckdb.DuckDBPyConnection, rows_per_batch: SupportsInt = 1
    ) -> str:
        return f"table:{int(rows_per_batch)}"

    def fake_fetch_record_batch(
        self: duckdb.DuckDBPyConnection, rows_per_batch: SupportsInt = 1
    ) -> str:
        return f"record:{int(rows_per_batch)}"

    monkeypatch.setattr(DuckCon, "_require_module", staticmethod(fake_require_module))
    monkeypatch.setattr(
        duckdb.DuckDBPyConnection,
        "fetchnumpy",
        fake_fetchnumpy,
        raising=False,
    )
    monkeypatch.setattr(
        duckdb.DuckDBPyConnection,
        "fetch_arrow_table",
        fake_fetch_arrow_table,
        raising=False,
    )
    monkeypatch.setattr(
        duckdb.DuckDBPyConnection,
        "fetch_record_batch",
        fake_fetch_record_batch,
        raising=False,
    )

    duckcon = DuckCon()
    with duckcon:
        duckcon.execute("SELECT 1")
        numpy_result = duckcon.fetchnumpy()
        arrow_default = duckcon.fetch_arrow_table()
        arrow_chunked = duckcon.fetch_arrow_table(42)
        record_default = duckcon.fetch_record_batch()
        record_chunked = duckcon.fetch_record_batch(7)

    assert numpy_result == {"v": [1, 2]}
    assert arrow_default == "table:1"
    assert arrow_chunked == "table:42"
    assert record_default == "record:1"
    assert record_chunked == "record:7"

    assert ("numpy", "DuckCon.fetchnumpy") in calls
    assert ("pyarrow", "DuckCon.fetch_arrow_table") in calls
    assert ("pyarrow", "DuckCon.fetch_record_batch") in calls


def test_duckcon_cursor_extension_and_registration_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, object, object | None]] = []

    def fake_install(
        self: duckdb.DuckDBPyConnection, name: str, *, force_install: bool = False
    ) -> duckdb.DuckDBPyConnection:
        calls.append(("install", name, force_install))
        return self

    def fake_load(
        self: duckdb.DuckDBPyConnection, name: str
    ) -> duckdb.DuckDBPyConnection:
        calls.append(("load", name, None))
        return self

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "install_extension", fake_install)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "load_extension", fake_load)

    duckcon = DuckCon()
    with duckcon:
        cursor = duckcon.cursor()
        assert isinstance(cursor, duckdb.DuckDBPyConnection)
        duckcon.interrupt()  # no active query, but should be a no-op

        duckcon.install_extension("  spatial  ", force_install=True)
        duckcon.load_extension("json")
        assert calls == [("install", "spatial", True), ("load", "json", None)]

        source = duckcon.sql("SELECT 1 AS value")
        duckcon.register("temp_values", source.relation)
        registered = duckcon.sql("SELECT value FROM temp_values")
        assert registered.fetchall() == [(1,)]

        duckcon.unregister("temp_values")
        with pytest.raises(duckdb.CatalogException):
            duckcon.connection.sql("SELECT * FROM temp_values")


def test_duckcon_extension_helper_validation() -> None:
    duckcon = DuckCon()
    with duckcon:
        with pytest.raises(TypeError):
            duckcon.install_extension(123)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            duckcon.install_extension("   ")
        with pytest.raises(TypeError):
            duckcon.load_extension(123)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            duckcon.load_extension("   ")
        with pytest.raises(TypeError):
            duckcon.register(123, object())  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            duckcon.unregister("   ")


def test_duckcon_type_helpers_normalise_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    reference = duckdb.connect()
    try:
        type_map = {
            "INTEGER": reference.type("INTEGER"),
            "VARCHAR": reference.type("VARCHAR"),
            "BIGINT": reference.type("BIGINT"),
        }
    finally:
        reference.close()

    recorded_specs: list[str] = []

    def fake_type(self: duckdb.DuckDBPyConnection, spec: str) -> sqltypes.DuckDBPyType:
        recorded_specs.append(spec)
        return type_map[spec]

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "type", fake_type, raising=False)

    duckcon = DuckCon()
    with duckcon:
        integer_type = duckcon.type("INTEGER")
        assert integer_type is type_map["INTEGER"]
        assert recorded_specs == ["INTEGER"]

        recorded_specs.clear()
        bigint_type = duckcon.sqltype(parse_type("BIGINT"))
        assert bigint_type is type_map["BIGINT"]
        assert recorded_specs == ["BIGINT"]

        recorded_specs.clear()
        passthrough = duckcon.type(type_map["VARCHAR"])
        assert passthrough is type_map["VARCHAR"]
        assert recorded_specs == []


def test_duckcon_collection_type_helpers_use_normalised_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference = duckdb.connect()
    try:
        integer_type = reference.type("INTEGER")
        varchar_type = reference.type("VARCHAR")
        list_result = reference.list_type(integer_type)
        array_result = reference.array_type(integer_type, 4)
        map_result = reference.map_type(integer_type, varchar_type)
        struct_result = reference.struct_type({"id": integer_type, "name": varchar_type})
        row_result = reference.row_type({"id": integer_type})
        union_result = reference.union_type({"a": integer_type, "b": varchar_type})
    finally:
        reference.close()

    type_map = {"INTEGER": integer_type, "VARCHAR": varchar_type}
    recorded_specs: list[str] = []
    recorded_list: list[sqltypes.DuckDBPyType] = []
    recorded_array: list[tuple[sqltypes.DuckDBPyType, int]] = []
    recorded_map: list[tuple[sqltypes.DuckDBPyType, sqltypes.DuckDBPyType]] = []
    recorded_struct: list[dict[str, sqltypes.DuckDBPyType]] = []
    recorded_row: list[dict[str, sqltypes.DuckDBPyType]] = []
    recorded_union: list[dict[str, sqltypes.DuckDBPyType]] = []

    def fake_type(self: duckdb.DuckDBPyConnection, spec: str) -> sqltypes.DuckDBPyType:
        recorded_specs.append(spec)
        return type_map[spec]

    def fake_list_type(
        self: duckdb.DuckDBPyConnection, element: sqltypes.DuckDBPyType
    ) -> sqltypes.DuckDBPyType:
        recorded_list.append(element)
        return list_result

    def fake_array_type(
        self: duckdb.DuckDBPyConnection,
        element: sqltypes.DuckDBPyType,
        size: SupportsInt,
    ) -> sqltypes.DuckDBPyType:
        recorded_array.append((element, int(size)))
        return array_result

    def fake_map_type(
        self: duckdb.DuckDBPyConnection,
        key: sqltypes.DuckDBPyType,
        value: sqltypes.DuckDBPyType,
    ) -> sqltypes.DuckDBPyType:
        recorded_map.append((key, value))
        return map_result

    def fake_struct_type(
        self: duckdb.DuckDBPyConnection, fields: dict[str, sqltypes.DuckDBPyType]
    ) -> sqltypes.DuckDBPyType:
        recorded_struct.append(fields)
        return struct_result

    def fake_row_type(
        self: duckdb.DuckDBPyConnection, fields: dict[str, sqltypes.DuckDBPyType]
    ) -> sqltypes.DuckDBPyType:
        recorded_row.append(fields)
        return row_result

    def fake_union_type(
        self: duckdb.DuckDBPyConnection, members: dict[str, sqltypes.DuckDBPyType]
    ) -> sqltypes.DuckDBPyType:
        recorded_union.append(members)
        return union_result

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "type", fake_type, raising=False)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "list_type", fake_list_type, raising=False)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "array_type", fake_array_type, raising=False)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "map_type", fake_map_type, raising=False)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "struct_type", fake_struct_type, raising=False)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "row_type", fake_row_type, raising=False)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "union_type", fake_union_type, raising=False)

    duckcon = DuckCon()
    with duckcon:
        list_type_value = duckcon.list_type("INTEGER")
        assert list_type_value is list_result
        assert recorded_list == [integer_type]

        array_type_value = duckcon.array_type("INTEGER", 4)
        assert array_type_value is array_result
        assert recorded_array == [(integer_type, 4)]

        map_type_value = duckcon.map_type("INTEGER", "VARCHAR")
        assert map_type_value is map_result
        assert recorded_map == [(integer_type, varchar_type)]

        struct_type_value = duckcon.struct_type({"id": "INTEGER", " name ": "VARCHAR"})
        assert struct_type_value is struct_result
        assert recorded_struct == [{"id": integer_type, "name": varchar_type}]

        row_type_value = duckcon.row_type([(" row_id ", "INTEGER")])
        assert row_type_value is row_result
        assert recorded_row == [{"row_id": integer_type}]

        union_type_value = duckcon.union_type((("a", "INTEGER"), ("b", "VARCHAR")))
        assert union_type_value is union_result
        assert recorded_union == [{"a": integer_type, "b": varchar_type}]

    assert recorded_specs == [
        "INTEGER",
        "INTEGER",
        "INTEGER",
        "VARCHAR",
        "INTEGER",
        "VARCHAR",
        "INTEGER",
        "INTEGER",
        "VARCHAR",
    ]


def test_duckcon_type_helper_validation_errors() -> None:
    duckcon = DuckCon()
    with duckcon:
        with pytest.raises(TypeError):
            duckcon.array_type("INTEGER", True)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            duckcon.array_type("INTEGER", 0)
        with pytest.raises(TypeError):
            duckcon.string_type(123)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            duckcon.decimal_type(3, 4)
        with pytest.raises(ValueError):
            duckcon.decimal_type(4, -1)
        with pytest.raises(TypeError):
            duckcon.enum_type(123, "VARCHAR", ["a"])  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            duckcon.enum_type("status", 123, ["a"])  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            duckcon.enum_type("status", "VARCHAR", [])
        with pytest.raises(ValueError):
            duckcon.enum_type("status", "VARCHAR", ["open", "open"])
        with pytest.raises(ValueError):
            duckcon.enum_type("   ", "VARCHAR", ["open"])
        with pytest.raises(ValueError):
            duckcon.struct_type({})
        with pytest.raises(ValueError):
            duckcon.struct_type([("id", "INTEGER"), ("id", "VARCHAR")])
        with pytest.raises(TypeError):
            duckcon.struct_type([("id", object())])


def test_relation_alias_and_describe() -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql("SELECT 42 AS value")
        described = relation.describe()
        assert isinstance(described, Relation)
        assert "value" in described.columns
        assert "aggr" in described.columns
        assert isinstance(relation.alias, str)
        assert relation.alias


def test_relation_apply_value_counts_and_selectors() -> None:
    duckcon = DuckCon()
    with duckcon:
        duckcon.execute(
            """
            CREATE TABLE metrics(cat VARCHAR, amount INTEGER);
            INSERT INTO metrics VALUES
                ('a', 10),
                ('a', 20),
                ('b', 5);
            """
        )
        relation = duckcon.sql("SELECT cat, amount FROM metrics")

        aggregated = relation.apply(
            "sum",
            "amount",
            groups=("cat",),
            projected_columns=("cat",),
        )
        assert sorted(aggregated.fetchall()) == [("a", 30), ("b", 5)]
        assert aggregated.columns == ("cat", "sum(amount)")

        counts = relation.value_counts("cat")
        assert sorted(counts.fetchall()) == [("a", 2), ("b", 1)]

        grouped = relation.value_counts("amount", groups=("cat", "amount"))
        assert sorted(grouped.fetchall()) == [(5, 1), (10, 1), (20, 1)]

        distinct = relation.unique("cat")
        assert set(distinct.fetchall()) == {("a",), ("b",)}

        numeric_only = relation.select_types(["INTEGER"])
        assert numeric_only.columns == ("amount",)

        varchar_only = relation.select_dtypes(["VARCHAR"])
        assert varchar_only.columns == ("cat",)


def test_relation_query_execution_and_alias(capfd: pytest.CaptureFixture[str]) -> None:
    duckcon = DuckCon()
    with duckcon:
        duckcon.execute(
            """
            CREATE TABLE people(id INTEGER, name VARCHAR);
            INSERT INTO people VALUES (1, 'Ada'), (2, 'Bob');
            """
        )
        relation = duckcon.sql("SELECT id, name FROM people")

        sql_text = relation.sql_query()
        assert "FROM" in sql_text

        plan = relation.explain()
        assert "SEQ_SCAN" in plan

        executed = relation.execute()
        assert executed.fetchall() == [(1, "Ada"), (2, "Bob")]

        executed.close()

        relation.show(max_rows=1)
        out, _ = capfd.readouterr()
        assert "id" in out
        assert "name" in out

        aliased = relation.set_alias("ppl")
        assert aliased.alias == "ppl"

        filtered = relation.query(
            "people_view",
            "SELECT name FROM people_view WHERE id = 2",
        )
        assert filtered.fetchall() == [("Bob",)]


def test_relation_create_insert_update_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    duckcon = DuckCon()
    with duckcon:
        duckcon.execute(
            """
            CREATE TABLE source_people(id INTEGER, name VARCHAR);
            INSERT INTO source_people VALUES (1, 'Ada'), (2, 'Bob');
            """
        )
        relation = duckcon.sql("SELECT id, name FROM source_people")

        relation.create("people_copy")
        assert duckcon.sql("SELECT COUNT(*) FROM people_copy").fetchone() == (2,)

        relation.create_view("people_view", replace=True)
        assert duckcon.sql("SELECT COUNT(*) FROM people_view").fetchone() == (2,)

        duckcon.execute("CREATE TABLE sink_people(id INTEGER, name VARCHAR)")
        relation.insert_into("sink_people")
        assert duckcon.sql("SELECT COUNT(*) FROM sink_people").fetchone() == (2,)

        captured_insert: list[object] = []

        def fake_insert(self: duckdb.DuckDBPyRelation, values: object) -> None:
            captured_insert.append(values)

        monkeypatch.setattr(
            duckdb.DuckDBPyRelation, "insert", fake_insert, raising=False
        )
        target = duckcon.sql("SELECT id, name FROM sink_people")
        payload = [("Carol", 3)]
        target.insert(payload)
        assert captured_insert == [payload]

        captured_updates: list[tuple[object, object | None]] = []

        def fake_update(
            self: duckdb.DuckDBPyRelation,
            assignments: object,
            *,
            condition: object | None = None,
        ) -> None:
            captured_updates.append((assignments, condition))

        monkeypatch.setattr(
            duckdb.DuckDBPyRelation, "update", fake_update, raising=False
        )

        target.update({"name": "UPPER(name)"}, condition="id = 1")
        target.update({"name": "name"})
        assert captured_updates == [
            ({"name": "UPPER(name)"}, "id = 1"),
            ({"name": "name"}, None),
        ]


def test_relation_map_and_tf_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql("SELECT 1 AS value")

        captured_map: list[tuple[object, object | None]] = []

        def fake_map(
            self: duckdb.DuckDBPyRelation,
            func: Callable[[duckdb.DuckDBPyRelation], object],
            *,
            schema: object | None = None,
        ) -> duckdb.DuckDBPyRelation:
            captured_map.append((func, schema))
            return duckcon.connection.sql("SELECT 99 AS value")

        monkeypatch.setattr(duckdb.DuckDBPyRelation, "map", fake_map, raising=False)

        def transformer(inner: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
            return inner

        mapped = relation.map(transformer, schema={"value": "INT"})
        assert mapped.fetchall() == [(99,)]
        assert captured_map == [(transformer, {"value": "INT"})]

        tf_calls: list[object] = []

        def fake_require_module(module: str, helper: str, install_hint: str) -> object:
            tf_calls.append((module, helper, install_hint))
            return object()

        def fake_tf(self: duckdb.DuckDBPyRelation) -> object:
            tf_calls.append("tf_called")
            return {"tensor": True}

        monkeypatch.setattr(Relation, "_require_module", staticmethod(fake_require_module))
        monkeypatch.setattr(duckdb.DuckDBPyRelation, "tf", fake_tf, raising=False)

        assert relation.tf() == {"tensor": True}
        assert tf_calls[0][0] == "tensorflow"
        assert "tf_called" in tf_calls


def test_relation_fetch_helpers() -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql(
            "SELECT generate_series AS value FROM range(3) t(generate_series) ORDER BY value"
        )
        assert relation.fetchone() == (0,)
        assert relation.fetchmany(2) == [(1,), (2,)]
        assert relation.fetchall() == []


def test_relation_fetchmany_validates_input() -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql("SELECT 1")
        with pytest.raises(TypeError):
            relation.fetchmany(True)
        with pytest.raises(ValueError):
            relation.fetchmany(0)


def test_relation_dataframe_fetch_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, object]] = []

    def fake_require_module(module: str, helper: str, install_hint: str) -> object:
        calls.append((module, helper))
        return object()

    def fake_fetchdf(
        self: duckdb.DuckDBPyRelation, *, date_as_object: bool = False
    ) -> object:
        calls.append(("fetchdf", date_as_object))
        return {"df": date_as_object}

    def fake_fetch_df_chunk(
        self: duckdb.DuckDBPyRelation,
        vectors_per_chunk: SupportsInt = 1,
        *,
        date_as_object: bool = False,
    ) -> object:
        calls.append(("fetch_df_chunk", (int(vectors_per_chunk), date_as_object)))
        return {"chunk": int(vectors_per_chunk)}

    monkeypatch.setattr(Relation, "_require_module", staticmethod(fake_require_module))
    monkeypatch.setattr(
        duckdb.DuckDBPyRelation,
        "fetchdf",
        fake_fetchdf,
        raising=False,
    )
    monkeypatch.setattr(
        duckdb.DuckDBPyRelation,
        "fetch_df_chunk",
        fake_fetch_df_chunk,
        raising=False,
    )

    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql("SELECT 1")
        assert relation.fetchdf() == {"df": False}
        assert relation.fetchdf(date_as_object=True) == {"df": True}
        assert relation.fetch_df_chunk() == {"chunk": 1}
        assert relation.fetch_df_chunk(3, date_as_object=True) == {"chunk": 3}

    assert ("pandas", "Relation.fetchdf") in calls
    assert ("pandas", "Relation.fetch_df_chunk") in calls
    assert ("fetchdf", False) in calls
    assert ("fetchdf", True) in calls
    assert ("fetch_df_chunk", (1, False)) in calls
    assert ("fetch_df_chunk", (3, True)) in calls


def test_relation_numpy_and_arrow_fetch_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []

    def fake_require_module(module: str, helper: str, install_hint: str) -> object:
        calls.append((module, helper))
        return object()

    def fake_fetchnumpy(self: duckdb.DuckDBPyRelation) -> dict[str, list[int]]:
        return {"v": [1, 2]}

    def fake_fetch_arrow_table(
        self: duckdb.DuckDBPyRelation, batch_size: SupportsInt = 1
    ) -> str:
        return f"table:{int(batch_size)}"

    def fake_fetch_arrow_reader(
        self: duckdb.DuckDBPyRelation, batch_size: SupportsInt = 1
    ) -> str:
        return f"reader:{int(batch_size)}"

    def fake_fetch_record_batch(
        self: duckdb.DuckDBPyRelation, rows_per_batch: SupportsInt = 1
    ) -> str:
        return f"record:{int(rows_per_batch)}"

    def fake_arrow(
        self: duckdb.DuckDBPyRelation, batch_size: SupportsInt = 1
    ) -> str:
        return f"arrow:{int(batch_size)}"

    monkeypatch.setattr(Relation, "_require_module", staticmethod(fake_require_module))
    monkeypatch.setattr(
        duckdb.DuckDBPyRelation,
        "fetchnumpy",
        fake_fetchnumpy,
        raising=False,
    )
    monkeypatch.setattr(
        duckdb.DuckDBPyRelation,
        "fetch_arrow_table",
        fake_fetch_arrow_table,
        raising=False,
    )
    monkeypatch.setattr(
        duckdb.DuckDBPyRelation,
        "fetch_arrow_reader",
        fake_fetch_arrow_reader,
        raising=False,
    )
    monkeypatch.setattr(
        duckdb.DuckDBPyRelation,
        "fetch_record_batch",
        fake_fetch_record_batch,
        raising=False,
    )
    monkeypatch.setattr(
        duckdb.DuckDBPyRelation,
        "arrow",
        fake_arrow,
        raising=False,
    )

    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql("SELECT 1")
        numpy_result = relation.fetchnumpy()
        table_default = relation.fetch_arrow_table()
        table_chunked = relation.fetch_arrow_table(8)
        reader_default = relation.fetch_arrow_reader()
        reader_chunked = relation.fetch_arrow_reader(4)
        record_default = relation.fetch_record_batch()
        record_chunked = relation.fetch_record_batch(2)
        arrow_default = relation.arrow()
        arrow_chunked = relation.arrow(6)

    assert numpy_result == {"v": [1, 2]}
    assert table_default == "table:1"
    assert table_chunked == "table:8"
    assert reader_default == "reader:1"
    assert reader_chunked == "reader:4"
    assert record_default == "record:1"
    assert record_chunked == "record:2"
    assert arrow_default == "arrow:1"
    assert arrow_chunked == "arrow:6"

    assert ("numpy", "Relation.fetchnumpy") in calls
    assert ("pyarrow", "Relation.fetch_arrow_table") in calls
    assert ("pyarrow", "Relation.fetch_arrow_reader") in calls
    assert ("pyarrow", "Relation.fetch_record_batch") in calls
    assert ("pyarrow", "Relation.arrow") in calls


def test_relation_set_operations_and_project() -> None:
    duckcon = DuckCon()
    with duckcon:
        left = duckcon.sql(
            "SELECT * FROM (VALUES (1), (2)) AS t(v) ORDER BY v"
        )
        right = duckcon.sql(
            "SELECT * FROM (VALUES (2), (3)) AS t(v) ORDER BY v"
        )

        intersection = left.intersect(right)
        assert intersection.fetchall() == [(2,)]

        difference = left.except_(right)
        assert difference.fetchall() == [(1,)]

        cross = left.cross(right)
        assert cross.columns == ("v", "v")
        assert len(cross.fetchall()) == 4

        projected = left.project("v + 1 AS inc")
        assert projected.columns == ("inc",)
        assert projected.fetchall() == [(2,), (3,)]

        sorted_relation = projected.sort("inc DESC")
        assert sorted_relation.fetchall() == [(3,), (2,)]


@pytest.mark.parametrize(
    ("method", "args", "kwargs", "expected"),
    (
        ("sum", ("v",), {}, 6),
        ("avg", ("v",), {}, approx(2.0)),
        ("count", ("v",), {}, 3),
        ("min", ("v",), {}, 1),
        ("max", ("v",), {}, 3),
        ("mean", ("v",), {}, approx(2.0)),
        ("median", ("v",), {}, approx(2.0)),
        ("std", ("v",), {}, approx(1.0)),
        ("stddev", ("v",), {}, approx(1.0)),
        ("stddev_pop", ("v",), {}, approx(0.8164965809)),
        ("stddev_samp", ("v",), {}, approx(1.0)),
        ("var", ("v",), {}, approx(1.0)),
        ("var_pop", ("v",), {}, approx(2 / 3)),
        ("var_samp", ("v",), {}, approx(1.0)),
        ("variance", ("v",), {}, approx(1.0)),
        ("product", ("v",), {}, approx(6.0)),
        ("any_value", ("v",), {}, 1),
        ("bool_and", ("flag",), {}, False),
        ("bool_or", ("flag",), {}, True),
        ("first", ("v",), {}, 1),
        ("last", ("v",), {}, 3),
        ("bit_and", ("v",), {}, 0),
        ("bit_or", ("v",), {}, 3),
        ("bit_xor", ("v",), {}, 0),
        ("favg", ("v",), {}, approx(2.0)),
        ("fsum", ("v",), {}, approx(6.0)),
        ("geomean", ("v",), {}, approx(1.8171205928)),
        ("mode", ("v",), {}, 1),
        ("quantile", ("v",), {}, 2),
        ("quantile_disc", ("v",), {}, 2),
        ("quantile_cont", ("v",), {}, approx(2.0)),
    ),
)
def test_relation_basic_aggregates(
    method: str, args: tuple[object, ...], kwargs: dict[str, object], expected: object
) -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql(
            "SELECT * FROM (VALUES (1, TRUE), (2, TRUE), (3, FALSE)) AS t(v, flag)"
        )
        aggregate = getattr(relation, method)
        result = aggregate(*args, **kwargs)
        assert isinstance(result, Relation)
        rows = result.fetchall()
        assert len(rows) == 1
        value = rows[0][0]
        assert value == expected


def test_relation_aggregate_validation() -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql("SELECT 1 AS value")
        with pytest.raises(TypeError):
            relation.sum(123)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            relation.sum("   ")
        result = relation.sum("value", groups=None, window_spec=None, projected_columns=None)
        assert isinstance(result, Relation)


def test_relation_list_and_histogram_helpers() -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql(
            "SELECT * FROM (VALUES (1, TRUE), (2, TRUE), (3, FALSE)) AS t(v, flag)"
        )
        listed = relation.list("v")
        histogram = relation.histogram("v")
        assert listed.fetchall() == [([1, 2, 3],)]
        assert histogram.fetchall() == [({1: 1, 2: 1, 3: 1},)]


def test_relation_string_agg_helpers() -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(v)")
        concatenated = relation.string_agg("v::VARCHAR", sep="|")
        assert concatenated.fetchall() == [("1|2|3",)]
        with pytest.raises(TypeError):
            relation.string_agg("v", sep=123)  # type: ignore[arg-type]


def test_relation_quantile_helpers_support_sequences() -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql("SELECT * FROM (VALUES (1), (2), (3), (4)) AS t(v)")
        quantiles = relation.quantile("v", q=[0.25, 0.75])
        assert quantiles.fetchall() == [([1, 3],)]
        with pytest.raises(ValueError):
            relation.quantile("v", q=1.5)
        with pytest.raises(ValueError):
            relation.quantile("v", q=[])
        with pytest.raises(TypeError):
            relation.quantile("v", q=[0.25, "bad"])  # type: ignore[list-item]


def test_relation_bitstring_agg_helper() -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql(
            "SELECT * FROM (VALUES (1), (2), (3)) AS t(v)"
        )
        bitstring = relation.bitstring_agg("v", minimum=1, maximum=3)
        assert bitstring.fetchall() == [("111",)]
        with pytest.raises(TypeError):
            relation.bitstring_agg("v", minimum=True)  # type: ignore[arg-type]


def test_relation_window_rank_helpers() -> None:
    duckcon = DuckCon()
    with duckcon:
        duckcon.execute("CREATE TABLE win(g VARCHAR, v INTEGER)")
        duckcon.execute(
            "INSERT INTO win VALUES ('a', 1), ('a', 2), ('b', 1), ('b', 2)"
        )
        relation = duckcon.sql("SELECT g, v FROM win ORDER BY g, v")
        window_spec = "OVER (PARTITION BY g ORDER BY v)"

        def extract_values(result: Relation) -> list[object]:
            return [row[0] for row in result.fetchall()]

        assert extract_values(relation.rank(window_spec)) == [1, 2, 1, 2]
        assert extract_values(relation.dense_rank(window_spec)) == [1, 2, 1, 2]
        assert extract_values(relation.rank_dense(window_spec)) == [1, 2, 1, 2]
        assert extract_values(relation.row_number(window_spec)) == [1, 2, 1, 2]
        assert extract_values(relation.percent_rank(window_spec)) == [
            0.0,
            1.0,
            0.0,
            1.0,
        ]
        assert extract_values(relation.cume_dist(window_spec)) == [
            pytest.approx(0.5),
            pytest.approx(1.0),
            pytest.approx(0.5),
            pytest.approx(1.0),
        ]
        assert extract_values(relation.n_tile(window_spec, 2)) == [1, 2, 1, 2]

        with pytest.raises(ValueError):
            relation.n_tile(window_spec, 0)
        with pytest.raises(TypeError):
            relation.n_tile(window_spec, True)  # type: ignore[arg-type]


def test_relation_window_value_helpers() -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql(
            "SELECT v FROM (VALUES (10), (20), (30)) AS t(v) ORDER BY v"
        )
        window_spec = "OVER (ORDER BY v)"

        assert relation.lag("v", window_spec).fetchall() == [
            (None,),
            (10,),
            (20,),
        ]
        assert relation.lead("v", window_spec).fetchall() == [
            (20,),
            (30,),
            (None,),
        ]
        assert relation.first_value("v", window_spec).fetchall() == [
            (10,),
            (10,),
            (10,),
        ]
        assert relation.last_value("v", window_spec).fetchall() == [
            (10,),
            (20,),
            (30,),
        ]
        assert relation.nth_value("v", window_spec, 2).fetchall() == [
            (None,),
            (20,),
            (20,),
        ]

        with pytest.raises(TypeError):
            relation.lag(123, window_spec)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            relation.lag("v", "  ")
        with pytest.raises(ValueError):
            relation.lag("v", window_spec, 0)
        with pytest.raises(TypeError):
            relation.lag("v", window_spec, default_value=123)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            relation.lag("v", window_spec, default_value="   ")
        with pytest.raises(TypeError):
            relation.lag("v", window_spec, ignore_nulls="no")  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            relation.nth_value("v", window_spec, 0)
        with pytest.raises(TypeError):
            relation.nth_value("v", window_spec, 1, ignore_nulls="no")  # type: ignore[arg-type]


def test_relation_arg_extrema_helpers() -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql(
            "SELECT * FROM (VALUES (1, TRUE), (2, TRUE), (3, FALSE)) AS t(v, flag)"
        )
        assert relation.arg_max("flag", "v").fetchall() == [(False,)]
        assert relation.arg_min("flag", "v").fetchall() == [(True,)]

def test_relation_metadata_properties() -> None:
    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql("SELECT 1::INTEGER AS value, 'x'::VARCHAR AS label")

        description = relation.description
        assert isinstance(description, tuple)
        assert description[0][0] == "value"

        dtypes = relation.dtypes
        assert tuple(str(dtype) for dtype in dtypes) == ("INTEGER", "VARCHAR")

        assert relation.shape == (1, 2)
        assert relation.type == relation.relation.type


def test_relation_materializers(monkeypatch: pytest.MonkeyPatch) -> None:
    module_calls: list[tuple[str, str]] = []
    method_calls: list[tuple[str, object]] = []

    def fake_require_module(module: str, helper: str, install_hint: str) -> object:
        module_calls.append((module, helper))
        return object()

    def fake_df(self: duckdb.DuckDBPyRelation, *, date_as_object: bool = False) -> object:
        method_calls.append(("df", date_as_object))
        return {"df": date_as_object}

    def fake_fetch_arrow_table(
        self: duckdb.DuckDBPyRelation, batch_size: int = 1_000_000
    ) -> object:
        method_calls.append(("fetch_arrow_table", int(batch_size)))
        return {"arrow": int(batch_size)}

    def fake_record_batch(
        self: duckdb.DuckDBPyRelation, batch_size: int = 1_000_000
    ) -> object:
        method_calls.append(("record_batch", int(batch_size)))
        return {"record": int(batch_size)}

    def fake_pl(
        self: duckdb.DuckDBPyRelation, batch_size: int = 1_000_000, *, lazy: bool = False
    ) -> object:
        method_calls.append(("pl", (int(batch_size), lazy)))
        return {"pl": (int(batch_size), lazy)}

    def fake_torch(self: duckdb.DuckDBPyRelation) -> object:
        method_calls.append(("torch", True))
        return {"torch": True}

    monkeypatch.setattr(Relation, "_require_module", staticmethod(fake_require_module))
    monkeypatch.setattr(duckdb.DuckDBPyRelation, "df", fake_df, raising=False)
    monkeypatch.setattr(
        duckdb.DuckDBPyRelation,
        "fetch_arrow_table",
        fake_fetch_arrow_table,
        raising=False,
    )
    monkeypatch.setattr(
        duckdb.DuckDBPyRelation,
        "record_batch",
        fake_record_batch,
        raising=False,
    )
    monkeypatch.setattr(duckdb.DuckDBPyRelation, "pl", fake_pl, raising=False)
    monkeypatch.setattr(duckdb.DuckDBPyRelation, "torch", fake_torch, raising=False)

    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql("SELECT 1 AS v")

        assert relation.df() == {"df": False}
        assert relation.to_df(date_as_object=True) == {"df": True}

        assert relation.to_arrow_table() == {"arrow": 1_000_000}
        assert relation.to_arrow_table(256) == {"arrow": 256}

        assert relation.record_batch() == {"record": 1_000_000}
        assert relation.record_batch(128) == {"record": 128}

        assert relation.pl() == {"pl": (1_000_000, False)}
        assert relation.pl(512, lazy=True) == {"pl": (512, True)}

        assert relation.torch() == {"torch": True}

    assert ("pandas", "Relation.df") in module_calls
    assert ("pyarrow", "Relation.fetch_arrow_table") in module_calls
    assert ("pyarrow", "Relation.record_batch") in module_calls
    assert ("polars", "Relation.pl") in module_calls
    assert ("torch", "Relation.torch") in module_calls

    assert ("df", False) in method_calls
    assert ("df", True) in method_calls
    assert ("fetch_arrow_table", 1_000_000) in method_calls
    assert ("fetch_arrow_table", 256) in method_calls
    assert ("record_batch", 1_000_000) in method_calls
    assert ("record_batch", 128) in method_calls
    assert ("pl", (1_000_000, False)) in method_calls
    assert ("pl", (512, True)) in method_calls
    assert ("torch", True) in method_calls


def test_relation_file_export_helpers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    csv_calls: list[tuple[str, dict[str, object]]] = []
    parquet_calls: list[tuple[str, dict[str, object]]] = []
    table_calls: list[str] = []
    view_calls: list[tuple[str, bool]] = []

    def fake_to_csv(self: duckdb.DuckDBPyRelation, file_name: str, **kwargs: object) -> None:
        csv_calls.append((file_name, dict(kwargs)))

    def fake_to_parquet(
        self: duckdb.DuckDBPyRelation, file_name: str, **kwargs: object
    ) -> None:
        parquet_calls.append((file_name, dict(kwargs)))

    def fake_to_table(self: duckdb.DuckDBPyRelation, table_name: str) -> None:
        table_calls.append(table_name)

    def fake_to_view(
        self: duckdb.DuckDBPyRelation, view_name: str, replace: bool = True
    ) -> duckdb.DuckDBPyRelation:
        view_calls.append((view_name, replace))
        return self

    monkeypatch.setattr(duckdb.DuckDBPyRelation, "to_csv", fake_to_csv, raising=False)
    monkeypatch.setattr(
        duckdb.DuckDBPyRelation,
        "write_csv",
        fake_to_csv,
        raising=False,
    )
    monkeypatch.setattr(
        duckdb.DuckDBPyRelation,
        "to_parquet",
        fake_to_parquet,
        raising=False,
    )
    monkeypatch.setattr(
        duckdb.DuckDBPyRelation,
        "write_parquet",
        fake_to_parquet,
        raising=False,
    )
    monkeypatch.setattr(duckdb.DuckDBPyRelation, "to_table", fake_to_table, raising=False)
    monkeypatch.setattr(duckdb.DuckDBPyRelation, "to_view", fake_to_view, raising=False)

    duckcon = DuckCon()
    with duckcon:
        relation = duckcon.sql("SELECT 42 AS v")

        relation.to_csv(
            tmp_path / "data.csv",
            sep="|",
            partition_by=["v"],
            overwrite=True,
        )
        relation.write_csv(
            tmp_path / "other.csv",
            header=False,
            partition_by="v",
        )

        relation.to_parquet(
            tmp_path / "data.parquet",
            row_group_size=10,
            partition_by=("v",),
        )
        relation.write_parquet(
            tmp_path / "other.parquet",
            field_ids=[1],
            append=True,
        )

        relation.to_table("  test_table  ")
        new_relation = relation.to_view("  test_view  ", replace=False)

        with pytest.raises(TypeError):
            relation.to_table(123)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            relation.to_table("   ")

    assert len(csv_calls) == 2
    csv_target, csv_kwargs = csv_calls[0]
    assert csv_target.endswith("data.csv")
    assert csv_kwargs["sep"] == "|"
    assert csv_kwargs["partition_by"] == ["v"]
    assert csv_kwargs["overwrite"] is True

    _, write_csv_kwargs = csv_calls[1]
    assert write_csv_kwargs["header"] is False
    assert write_csv_kwargs["partition_by"] == ["v"]

    assert len(parquet_calls) == 2
    parquet_target, parquet_kwargs = parquet_calls[0]
    assert parquet_target.endswith("data.parquet")
    assert parquet_kwargs["row_group_size"] == 10
    assert parquet_kwargs["partition_by"] == ["v"]

    _, write_parquet_kwargs = parquet_calls[1]
    assert write_parquet_kwargs["field_ids"] == [1]
    assert write_parquet_kwargs["append"] is True

    assert table_calls == ["test_table"]
    assert view_calls == [("test_view", False)]
    assert isinstance(new_relation, Relation)
