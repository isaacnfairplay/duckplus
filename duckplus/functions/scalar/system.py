"""Decorator-backed helpers for DuckDB session and catalog macros."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from duckplus.functions._base import (
    invoke_duckdb_function,
    register_duckdb_function,
)
from duckplus.static_typed.expression import VarcharExpression
from duckplus.static_typed.functions import DuckDBFunctionDefinition
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        ScalarVarcharFunctions,
    )


_CURRENT_CATALOG_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="current_catalog",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="main.current_database()",
    ),
)


@register_duckdb_function("current_catalog")
def current_catalog(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Return the name of the catalog for the active connection."""

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _CURRENT_CATALOG_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_CURRENT_DATABASE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="current_database",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description="Returns the name of the currently active database",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="current_database",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description=None,
        comment=None,
        macro_definition='"system".main.current_database()',
    ),
)


@register_duckdb_function("current_database")
def current_database(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Return the active database name for the current session."""

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _CURRENT_DATABASE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_CURRENT_QUERY_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="current_query",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description="Returns the current query as a string",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="current_query",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description=None,
        comment=None,
        macro_definition='"system".main.current_query()',
    ),
)


@register_duckdb_function("current_query")
def current_query(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Return the SQL text of the query currently executing."""

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _CURRENT_QUERY_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_CURRENT_ROLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="current_role",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="'duckdb'",
    ),
)


@register_duckdb_function("current_role")
def current_role(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Return the name of the active role (DuckDB always reports ``duckdb``)."""

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _CURRENT_ROLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_CURRENT_SCHEMA_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="current_schema",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description="Returns the name of the currently active schema. Default is main",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="current_schema",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description=None,
        comment=None,
        macro_definition='"system".main.current_schema()',
    ),
)


@register_duckdb_function("current_schema")
def current_schema(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Return the name of the default schema for new relations."""

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _CURRENT_SCHEMA_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_CURRENT_SCHEMAS_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="current_schemas",
        function_type="scalar",
        return_type=parse_type("VARCHAR[]"),
        parameter_types=(parse_type("BOOLEAN"),),
        parameters=("include_implicit",),
        varargs=None,
        description="Returns list of schemas. Pass a parameter of True to include implicit schemas",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="current_schemas",
        function_type="scalar",
        return_type=parse_type("VARCHAR[]"),
        parameter_types=(None,),
        parameters=("include_implicit",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition='"system".main.current_schemas(include_implicit)',
    ),
)


@register_duckdb_function("current_schemas")
def current_schemas(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Return the visible schema search path as an array of names."""

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _CURRENT_SCHEMAS_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_CURRENT_USER_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="current_user",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="'duckdb'",
    ),
)


@register_duckdb_function("current_user")
def current_user(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Return the name of the authenticated user (always ``duckdb``)."""

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _CURRENT_USER_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_SESSION_USER_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="session_user",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="'duckdb'",
    ),
)


@register_duckdb_function("session_user")
def session_user(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Return the session user (DuckDB always reports ``duckdb``)."""

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _SESSION_USER_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_GET_CONSTRAINTDEF_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_get_constraintdef",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(None,),
        parameters=("constraint_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition=(
            "(SELECT constraint_text FROM duckdb_constraints() AS d_constraint "
            "WHERE ((d_constraint.table_oid = (constraint_oid // 1000000)) "
            "AND (d_constraint.constraint_index = (constraint_oid % 1000000))))"
        ),
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_get_constraintdef",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(None, None),
        parameters=("constraint_oid", "pretty_bool"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="pg_get_constraintdef(constraint_oid)",
    ),
)


@register_duckdb_function("pg_get_constraintdef")
def pg_get_constraintdef(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Render the definition of a constraint from DuckDB's catalog."""

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _PG_GET_CONSTRAINTDEF_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_GET_VIEWDEF_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_get_viewdef",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(None,),
        parameters=("oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition='(SELECT "sql" FROM duckdb_views() AS v WHERE (v.view_oid = oid))',
    ),
)


@register_duckdb_function("pg_get_viewdef")
def pg_get_viewdef(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Return the SQL definition of the view identified by ``oid``."""

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _PG_GET_VIEWDEF_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_SIZE_PRETTY_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_size_pretty",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(None,),
        parameters=("bytes",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="format_bytes(bytes)",
    ),
)


@register_duckdb_function("pg_size_pretty")
def pg_size_pretty(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Format a byte count using PostgreSQL's pretty-printing convention."""

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _PG_SIZE_PRETTY_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_TYPEOF_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_typeof",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(None,),
        parameters=("expression",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="lower(typeof(expression))",
    ),
)


@register_duckdb_function("pg_typeof")
def pg_typeof(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Report the DuckDB logical type of ``expression`` in PostgreSQL form."""

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _PG_TYPEOF_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


def _register() -> None:
    """Attach scalar catalog macro helpers onto the varchar namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        ScalarVarcharFunctions,
    )

    namespace: Any = ScalarVarcharFunctions

    namespace._CURRENT_CATALOG_SIGNATURES = _CURRENT_CATALOG_SIGNATURES
    namespace.current_catalog = cast(Any, current_catalog)
    namespace._register_function(
        "current_catalog",
        names=getattr(current_catalog, "__duckdb_identifiers__", ()),
        symbols=getattr(current_catalog, "__duckdb_symbols__", ()),
    )

    namespace._CURRENT_DATABASE_SIGNATURES = _CURRENT_DATABASE_SIGNATURES
    namespace.current_database = cast(Any, current_database)
    namespace._register_function(
        "current_database",
        names=getattr(current_database, "__duckdb_identifiers__", ()),
        symbols=getattr(current_database, "__duckdb_symbols__", ()),
    )

    namespace._CURRENT_QUERY_SIGNATURES = _CURRENT_QUERY_SIGNATURES
    namespace.current_query = cast(Any, current_query)
    namespace._register_function(
        "current_query",
        names=getattr(current_query, "__duckdb_identifiers__", ()),
        symbols=getattr(current_query, "__duckdb_symbols__", ()),
    )

    namespace._CURRENT_ROLE_SIGNATURES = _CURRENT_ROLE_SIGNATURES
    namespace.current_role = cast(Any, current_role)
    namespace._register_function(
        "current_role",
        names=getattr(current_role, "__duckdb_identifiers__", ()),
        symbols=getattr(current_role, "__duckdb_symbols__", ()),
    )

    namespace._CURRENT_SCHEMA_SIGNATURES = _CURRENT_SCHEMA_SIGNATURES
    namespace.current_schema = cast(Any, current_schema)
    namespace._register_function(
        "current_schema",
        names=getattr(current_schema, "__duckdb_identifiers__", ()),
        symbols=getattr(current_schema, "__duckdb_symbols__", ()),
    )

    namespace._CURRENT_SCHEMAS_SIGNATURES = _CURRENT_SCHEMAS_SIGNATURES
    namespace.current_schemas = cast(Any, current_schemas)
    namespace._register_function(
        "current_schemas",
        names=getattr(current_schemas, "__duckdb_identifiers__", ()),
        symbols=getattr(current_schemas, "__duckdb_symbols__", ()),
    )

    namespace._CURRENT_USER_SIGNATURES = _CURRENT_USER_SIGNATURES
    namespace.current_user = cast(Any, current_user)
    namespace._register_function(
        "current_user",
        names=getattr(current_user, "__duckdb_identifiers__", ()),
        symbols=getattr(current_user, "__duckdb_symbols__", ()),
    )

    namespace._SESSION_USER_SIGNATURES = _SESSION_USER_SIGNATURES
    namespace.session_user = cast(Any, session_user)
    namespace._register_function(
        "session_user",
        names=getattr(session_user, "__duckdb_identifiers__", ()),
        symbols=getattr(session_user, "__duckdb_symbols__", ()),
    )

    namespace._PG_GET_CONSTRAINTDEF_SIGNATURES = _PG_GET_CONSTRAINTDEF_SIGNATURES
    namespace.pg_get_constraintdef = cast(Any, pg_get_constraintdef)
    namespace._register_function(
        "pg_get_constraintdef",
        names=getattr(pg_get_constraintdef, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_get_constraintdef, "__duckdb_symbols__", ()),
    )

    namespace._PG_GET_VIEWDEF_SIGNATURES = _PG_GET_VIEWDEF_SIGNATURES
    namespace.pg_get_viewdef = cast(Any, pg_get_viewdef)
    namespace._register_function(
        "pg_get_viewdef",
        names=getattr(pg_get_viewdef, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_get_viewdef, "__duckdb_symbols__", ()),
    )

    namespace._PG_SIZE_PRETTY_SIGNATURES = _PG_SIZE_PRETTY_SIGNATURES
    namespace.pg_size_pretty = cast(Any, pg_size_pretty)
    namespace._register_function(
        "pg_size_pretty",
        names=getattr(pg_size_pretty, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_size_pretty, "__duckdb_symbols__", ()),
    )

    namespace._PG_TYPEOF_SIGNATURES = _PG_TYPEOF_SIGNATURES
    namespace.pg_typeof = cast(Any, pg_typeof)
    namespace._register_function(
        "pg_typeof",
        names=getattr(pg_typeof, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_typeof, "__duckdb_symbols__", ()),
    )


# Preserve provenance for introspection tests that assert module origins.
current_catalog.__module__ = "duckplus.functions.scalar.system"
current_database.__module__ = "duckplus.functions.scalar.system"
current_query.__module__ = "duckplus.functions.scalar.system"
current_role.__module__ = "duckplus.functions.scalar.system"
current_schema.__module__ = "duckplus.functions.scalar.system"
current_schemas.__module__ = "duckplus.functions.scalar.system"
current_user.__module__ = "duckplus.functions.scalar.system"
session_user.__module__ = "duckplus.functions.scalar.system"
pg_get_constraintdef.__module__ = "duckplus.functions.scalar.system"
pg_get_viewdef.__module__ = "duckplus.functions.scalar.system"
pg_size_pretty.__module__ = "duckplus.functions.scalar.system"
pg_typeof.__module__ = "duckplus.functions.scalar.system"


_register()


__all__ = [
    "current_catalog",
    "current_database",
    "current_query",
    "current_role",
    "current_schema",
    "current_schemas",
    "current_user",
    "session_user",
    "pg_get_constraintdef",
    "pg_get_viewdef",
    "pg_size_pretty",
    "pg_typeof",
]
