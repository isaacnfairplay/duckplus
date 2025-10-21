"""Decorator-backed overrides for PostgreSQL privilege macros."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from duckplus.static_typed.expression import BooleanExpression
from duckplus.static_typed.functions import (
    DuckDBFunctionDefinition,
    call_duckdb_function,
    duckdb_function,
)
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        ScalarBooleanFunctions,
    )


_HAS_ANY_COLUMN_PRIVILEGE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_any_column_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None),
        parameters=("table", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_any_column_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None),
        parameters=("user", "table", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@duckdb_function("has_any_column_privilege")
def has_any_column_privilege(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` for PostgreSQL column privilege checks.

    Overloads:
    - pg_catalog.has_any_column_privilege(ANY table, ANY privilege) -> BOOLEAN
    - pg_catalog.has_any_column_privilege(ANY user, ANY table, ANY privilege) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        call_duckdb_function(
            _HAS_ANY_COLUMN_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_HAS_COLUMN_PRIVILEGE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_column_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None),
        parameters=("table", "column", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_column_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None, None),
        parameters=("user", "table", "column", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@duckdb_function("has_column_privilege")
def has_column_privilege(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` for PostgreSQL column-level privilege checks.

    Overloads:
    - pg_catalog.has_column_privilege(ANY table, ANY column, ANY privilege) -> BOOLEAN
    - pg_catalog.has_column_privilege(ANY user, ANY table, ANY column, ANY privilege) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        call_duckdb_function(
            _HAS_COLUMN_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_HAS_DATABASE_PRIVILEGE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_database_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None),
        parameters=("database", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_database_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None),
        parameters=("user", "database", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@duckdb_function("has_database_privilege")
def has_database_privilege(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` for PostgreSQL database privilege checks.

    Overloads:
    - pg_catalog.has_database_privilege(ANY database, ANY privilege) -> BOOLEAN
    - pg_catalog.has_database_privilege(ANY user, ANY database, ANY privilege) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        call_duckdb_function(
            _HAS_DATABASE_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_HAS_FOREIGN_DATA_WRAPPER_PRIVILEGE_SIGNATURES: tuple[
    DuckDBFunctionDefinition, ...
] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_foreign_data_wrapper_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None),
        parameters=("fdw", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_foreign_data_wrapper_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None),
        parameters=("user", "fdw", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@duckdb_function("has_foreign_data_wrapper_privilege")
def has_foreign_data_wrapper_privilege(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` for PostgreSQL foreign data wrapper privilege checks.

    Overloads:
    - pg_catalog.has_foreign_data_wrapper_privilege(ANY fdw, ANY privilege) -> BOOLEAN
    - pg_catalog.has_foreign_data_wrapper_privilege(ANY user, ANY fdw, ANY privilege) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        call_duckdb_function(
            _HAS_FOREIGN_DATA_WRAPPER_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_HAS_FUNCTION_PRIVILEGE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_function_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None),
        parameters=("function", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_function_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None),
        parameters=("user", "function", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@duckdb_function("has_function_privilege")
def has_function_privilege(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` for PostgreSQL function privilege checks.

    Overloads:
    - pg_catalog.has_function_privilege(ANY function, ANY privilege) -> BOOLEAN
    - pg_catalog.has_function_privilege(ANY user, ANY function, ANY privilege) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        call_duckdb_function(
            _HAS_FUNCTION_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_HAS_LANGUAGE_PRIVILEGE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_language_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None),
        parameters=("language", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_language_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None),
        parameters=("user", "language", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@duckdb_function("has_language_privilege")
def has_language_privilege(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` for PostgreSQL language privilege checks.

    Overloads:
    - pg_catalog.has_language_privilege(ANY language, ANY privilege) -> BOOLEAN
    - pg_catalog.has_language_privilege(ANY user, ANY language, ANY privilege) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        call_duckdb_function(
            _HAS_LANGUAGE_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_HAS_SCHEMA_PRIVILEGE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_schema_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None),
        parameters=("schema", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_schema_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None),
        parameters=("user", "schema", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@duckdb_function("has_schema_privilege")
def has_schema_privilege(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` for PostgreSQL schema privilege checks.

    Overloads:
    - pg_catalog.has_schema_privilege(ANY schema, ANY privilege) -> BOOLEAN
    - pg_catalog.has_schema_privilege(ANY user, ANY schema, ANY privilege) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        call_duckdb_function(
            _HAS_SCHEMA_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_HAS_SEQUENCE_PRIVILEGE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_sequence_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None),
        parameters=("sequence", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_sequence_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None),
        parameters=("user", "sequence", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@duckdb_function("has_sequence_privilege")
def has_sequence_privilege(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` for PostgreSQL sequence privilege checks.

    Overloads:
    - pg_catalog.has_sequence_privilege(ANY sequence, ANY privilege) -> BOOLEAN
    - pg_catalog.has_sequence_privilege(ANY user, ANY sequence, ANY privilege) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        call_duckdb_function(
            _HAS_SEQUENCE_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_HAS_SERVER_PRIVILEGE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_server_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None),
        parameters=("server", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_server_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None),
        parameters=("user", "server", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@duckdb_function("has_server_privilege")
def has_server_privilege(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` for PostgreSQL server privilege checks.

    Overloads:
    - pg_catalog.has_server_privilege(ANY server, ANY privilege) -> BOOLEAN
    - pg_catalog.has_server_privilege(ANY user, ANY server, ANY privilege) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        call_duckdb_function(
            _HAS_SERVER_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_HAS_TABLE_PRIVILEGE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_table_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None),
        parameters=("table", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_table_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None),
        parameters=("user", "table", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@duckdb_function("has_table_privilege")
def has_table_privilege(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` for PostgreSQL table privilege checks.

    Overloads:
    - pg_catalog.has_table_privilege(ANY table, ANY privilege) -> BOOLEAN
    - pg_catalog.has_table_privilege(ANY user, ANY table, ANY privilege) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        call_duckdb_function(
            _HAS_TABLE_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_HAS_TABLESPACE_PRIVILEGE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_tablespace_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None),
        parameters=("tablespace", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="has_tablespace_privilege",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None),
        parameters=("user", "tablespace", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@duckdb_function("has_tablespace_privilege")
def has_tablespace_privilege(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` for PostgreSQL tablespace privilege checks.

    Overloads:
    - pg_catalog.has_tablespace_privilege(ANY tablespace, ANY privilege) -> BOOLEAN
    - pg_catalog.has_tablespace_privilege(ANY user, ANY tablespace, ANY privilege) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        call_duckdb_function(
            _HAS_TABLESPACE_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )
