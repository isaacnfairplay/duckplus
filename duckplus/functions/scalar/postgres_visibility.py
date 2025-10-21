"""PostgreSQL visibility macros exposed as boolean helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from duckplus.functions._base import (
    invoke_duckdb_function,
    register_duckdb_function,
)
from duckplus.static_typed.expression import BooleanExpression
from duckplus.static_typed.functions import DuckDBFunctionDefinition
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        ScalarBooleanFunctions,
    )


_PG_COLLATION_IS_VISIBLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_collation_is_visible",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None,),
        parameters=("collation_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_collation_is_visible")
def pg_collation_is_visible(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the collation identifier is visible."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_COLLATION_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_CONVERSION_IS_VISIBLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_conversion_is_visible",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None,),
        parameters=("conversion_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_conversion_is_visible")
def pg_conversion_is_visible(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the conversion identifier is visible."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_CONVERSION_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_FUNCTION_IS_VISIBLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_function_is_visible",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None,),
        parameters=("function_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_function_is_visible")
def pg_function_is_visible(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the function identifier is visible."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_FUNCTION_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_HAS_ROLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_has_role",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None),
        parameters=("role", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_has_role",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None, None, None),
        parameters=("user", "role", "privilege"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_has_role")
def pg_has_role(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the requested role privilege is available."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_HAS_ROLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_OPCLASS_IS_VISIBLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_opclass_is_visible",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None,),
        parameters=("opclass_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_opclass_is_visible")
def pg_opclass_is_visible(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the operator class identifier is visible."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_OPCLASS_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_OPERATOR_IS_VISIBLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_operator_is_visible",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None,),
        parameters=("operator_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_operator_is_visible")
def pg_operator_is_visible(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the operator identifier is visible."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_OPERATOR_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_OPFAMILY_IS_VISIBLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_opfamily_is_visible",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None,),
        parameters=("opclass_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_opfamily_is_visible")
def pg_opfamily_is_visible(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the operator family identifier is visible."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_OPFAMILY_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_TABLE_IS_VISIBLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_table_is_visible",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None,),
        parameters=("table_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_table_is_visible")
def pg_table_is_visible(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the table identifier is visible."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_TABLE_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_TS_CONFIG_IS_VISIBLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_ts_config_is_visible",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None,),
        parameters=("config_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_ts_config_is_visible")
def pg_ts_config_is_visible(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the text search config identifier is visible."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_TS_CONFIG_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_TS_DICT_IS_VISIBLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_ts_dict_is_visible",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None,),
        parameters=("dict_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_ts_dict_is_visible")
def pg_ts_dict_is_visible(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the text search dictionary identifier is visible."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_TS_DICT_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_TS_PARSER_IS_VISIBLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_ts_parser_is_visible",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None,),
        parameters=("parser_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_ts_parser_is_visible")
def pg_ts_parser_is_visible(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the text search parser identifier is visible."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_TS_PARSER_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_TS_TEMPLATE_IS_VISIBLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_ts_template_is_visible",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None,),
        parameters=("template_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_ts_template_is_visible")
def pg_ts_template_is_visible(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the text search template identifier is visible."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_TS_TEMPLATE_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_PG_TYPE_IS_VISIBLE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="pg_catalog",
        function_name="pg_type_is_visible",
        function_type="scalar",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(None,),
        parameters=("type_oid",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="CAST('t' AS BOOLEAN)",
    ),
)


@register_duckdb_function("pg_type_is_visible")
def pg_type_is_visible(
    self: "ScalarBooleanFunctions",
    *operands: object,
) -> BooleanExpression:
    """Return ``TRUE`` when the type identifier is visible."""

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _PG_TYPE_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


__all__ = [
    "pg_collation_is_visible",
    "pg_conversion_is_visible",
    "pg_function_is_visible",
    "pg_has_role",
    "pg_opclass_is_visible",
    "pg_operator_is_visible",
    "pg_opfamily_is_visible",
    "pg_table_is_visible",
    "pg_ts_config_is_visible",
    "pg_ts_dict_is_visible",
    "pg_ts_parser_is_visible",
    "pg_ts_template_is_visible",
    "pg_type_is_visible",
]


def _register() -> None:
    """Attach PostgreSQL visibility macros to the boolean namespace."""

    from duckplus.static_typed._generated_function_namespaces import (  # noqa: WPS433
        ScalarBooleanFunctions,
    )

    namespace: Any = ScalarBooleanFunctions

    namespace._PG_COLLATION_IS_VISIBLE_SIGNATURES = (  # type: ignore[attr-defined]
        _PG_COLLATION_IS_VISIBLE_SIGNATURES
    )
    namespace.pg_collation_is_visible = cast(Any, pg_collation_is_visible)
    namespace._register_function(
        "pg_collation_is_visible",
        names=getattr(pg_collation_is_visible, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_collation_is_visible, "__duckdb_symbols__", ()),
    )

    namespace._PG_CONVERSION_IS_VISIBLE_SIGNATURES = (  # type: ignore[attr-defined]
        _PG_CONVERSION_IS_VISIBLE_SIGNATURES
    )
    namespace.pg_conversion_is_visible = cast(Any, pg_conversion_is_visible)
    namespace._register_function(
        "pg_conversion_is_visible",
        names=getattr(pg_conversion_is_visible, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_conversion_is_visible, "__duckdb_symbols__", ()),
    )

    namespace._PG_FUNCTION_IS_VISIBLE_SIGNATURES = (  # type: ignore[attr-defined]
        _PG_FUNCTION_IS_VISIBLE_SIGNATURES
    )
    namespace.pg_function_is_visible = cast(Any, pg_function_is_visible)
    namespace._register_function(
        "pg_function_is_visible",
        names=getattr(pg_function_is_visible, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_function_is_visible, "__duckdb_symbols__", ()),
    )

    namespace._PG_HAS_ROLE_SIGNATURES = _PG_HAS_ROLE_SIGNATURES  # type: ignore[attr-defined]
    namespace.pg_has_role = cast(Any, pg_has_role)
    namespace._register_function(
        "pg_has_role",
        names=getattr(pg_has_role, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_has_role, "__duckdb_symbols__", ()),
    )

    namespace._PG_OPCLASS_IS_VISIBLE_SIGNATURES = (  # type: ignore[attr-defined]
        _PG_OPCLASS_IS_VISIBLE_SIGNATURES
    )
    namespace.pg_opclass_is_visible = cast(Any, pg_opclass_is_visible)
    namespace._register_function(
        "pg_opclass_is_visible",
        names=getattr(pg_opclass_is_visible, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_opclass_is_visible, "__duckdb_symbols__", ()),
    )

    namespace._PG_OPERATOR_IS_VISIBLE_SIGNATURES = (  # type: ignore[attr-defined]
        _PG_OPERATOR_IS_VISIBLE_SIGNATURES
    )
    namespace.pg_operator_is_visible = cast(Any, pg_operator_is_visible)
    namespace._register_function(
        "pg_operator_is_visible",
        names=getattr(pg_operator_is_visible, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_operator_is_visible, "__duckdb_symbols__", ()),
    )

    namespace._PG_OPFAMILY_IS_VISIBLE_SIGNATURES = (  # type: ignore[attr-defined]
        _PG_OPFAMILY_IS_VISIBLE_SIGNATURES
    )
    namespace.pg_opfamily_is_visible = cast(Any, pg_opfamily_is_visible)
    namespace._register_function(
        "pg_opfamily_is_visible",
        names=getattr(pg_opfamily_is_visible, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_opfamily_is_visible, "__duckdb_symbols__", ()),
    )

    namespace._PG_TABLE_IS_VISIBLE_SIGNATURES = (  # type: ignore[attr-defined]
        _PG_TABLE_IS_VISIBLE_SIGNATURES
    )
    namespace.pg_table_is_visible = cast(Any, pg_table_is_visible)
    namespace._register_function(
        "pg_table_is_visible",
        names=getattr(pg_table_is_visible, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_table_is_visible, "__duckdb_symbols__", ()),
    )

    namespace._PG_TS_CONFIG_IS_VISIBLE_SIGNATURES = (  # type: ignore[attr-defined]
        _PG_TS_CONFIG_IS_VISIBLE_SIGNATURES
    )
    namespace.pg_ts_config_is_visible = cast(Any, pg_ts_config_is_visible)
    namespace._register_function(
        "pg_ts_config_is_visible",
        names=getattr(pg_ts_config_is_visible, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_ts_config_is_visible, "__duckdb_symbols__", ()),
    )

    namespace._PG_TS_DICT_IS_VISIBLE_SIGNATURES = (  # type: ignore[attr-defined]
        _PG_TS_DICT_IS_VISIBLE_SIGNATURES
    )
    namespace.pg_ts_dict_is_visible = cast(Any, pg_ts_dict_is_visible)
    namespace._register_function(
        "pg_ts_dict_is_visible",
        names=getattr(pg_ts_dict_is_visible, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_ts_dict_is_visible, "__duckdb_symbols__", ()),
    )

    namespace._PG_TS_PARSER_IS_VISIBLE_SIGNATURES = (  # type: ignore[attr-defined]
        _PG_TS_PARSER_IS_VISIBLE_SIGNATURES
    )
    namespace.pg_ts_parser_is_visible = cast(Any, pg_ts_parser_is_visible)
    namespace._register_function(
        "pg_ts_parser_is_visible",
        names=getattr(pg_ts_parser_is_visible, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_ts_parser_is_visible, "__duckdb_symbols__", ()),
    )

    namespace._PG_TS_TEMPLATE_IS_VISIBLE_SIGNATURES = (  # type: ignore[attr-defined]
        _PG_TS_TEMPLATE_IS_VISIBLE_SIGNATURES
    )
    namespace.pg_ts_template_is_visible = cast(Any, pg_ts_template_is_visible)
    namespace._register_function(
        "pg_ts_template_is_visible",
        names=getattr(pg_ts_template_is_visible, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_ts_template_is_visible, "__duckdb_symbols__", ()),
    )

    namespace._PG_TYPE_IS_VISIBLE_SIGNATURES = (  # type: ignore[attr-defined]
        _PG_TYPE_IS_VISIBLE_SIGNATURES
    )
    namespace.pg_type_is_visible = cast(Any, pg_type_is_visible)
    namespace._register_function(
        "pg_type_is_visible",
        names=getattr(pg_type_is_visible, "__duckdb_identifiers__", ()),
        symbols=getattr(pg_type_is_visible, "__duckdb_symbols__", ()),
    )


_register()
