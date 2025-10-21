"""Static-typed overrides for scalar string macros."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from duckplus.static_typed.expression import VarcharExpression
from duckplus.static_typed.functions import (
    DuckDBFunctionDefinition,
    call_duckdb_function,
    duckdb_function,
)
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        ScalarVarcharFunctions,
    )


_SPLIT_PART_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="split_part",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(None, None, None),
        parameters=("string", "delimiter", "position"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition=(
            "CASE  WHEN (((string IS NOT NULL) AND (\"delimiter\" IS NOT NULL) "
            "AND (\"position\" IS NOT NULL))) "
            "THEN (COALESCE(string_split(string, \"delimiter\")[\"position\"], '')) "
            "ELSE NULL END"
        ),
    ),
)


@duckdb_function("split_part")
def split_part(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Split a string by delimiter and return the 1-indexed part.

    Overloads:
    - main.split_part(ANY string, ANY delimiter, ANY position) -> VARCHAR
    """

    return cast(
        VarcharExpression,
        call_duckdb_function(
            _SPLIT_PART_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_ARRAY_TO_STRING_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="array_to_string",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(None, None),
        parameters=("arr", "sep"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="list_aggr(CAST(arr AS VARCHAR[]), 'string_agg', sep)",
    ),
)


@duckdb_function("array_to_string")
def array_to_string(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Join array elements into a string using the supplied separator.

    Overloads:
    - main.array_to_string(ANY arr, ANY sep) -> VARCHAR
    """

    return cast(
        VarcharExpression,
        call_duckdb_function(
            _ARRAY_TO_STRING_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_ARRAY_TO_STRING_COMMA_DEFAULT_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="array_to_string_comma_default",
        function_type="scalar",
        return_type=parse_type("VARCHAR"),
        parameter_types=(None, None),
        parameters=("arr", "sep"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="list_aggr(CAST(arr AS VARCHAR[]), 'string_agg', sep)",
    ),
)


@duckdb_function("array_to_string_comma_default")
def array_to_string_comma_default(
    self: "ScalarVarcharFunctions",
    *operands: object,
) -> VarcharExpression:
    """Join array elements with a comma separator when none is provided.

    Overloads:
    - main.array_to_string_comma_default(ANY arr, ANY sep) -> VARCHAR
    """

    return cast(
        VarcharExpression,
        call_duckdb_function(
            _ARRAY_TO_STRING_COMMA_DEFAULT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


def _register() -> None:
    """Attach scalar string macro helpers onto the scalar namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        ScalarVarcharFunctions,
    )

    namespace: Any = ScalarVarcharFunctions

    namespace._SPLIT_PART_SIGNATURES = _SPLIT_PART_SIGNATURES
    namespace.split_part = cast(Any, split_part)
    namespace._register_function(
        "split_part",
        names=getattr(split_part, "__duckdb_identifiers__", ()),
        symbols=getattr(split_part, "__duckdb_symbols__", ()),
    )

    namespace._ARRAY_TO_STRING_SIGNATURES = _ARRAY_TO_STRING_SIGNATURES
    namespace.array_to_string = cast(Any, array_to_string)
    namespace._register_function(
        "array_to_string",
        names=getattr(array_to_string, "__duckdb_identifiers__", ()),
        symbols=getattr(array_to_string, "__duckdb_symbols__", ()),
    )

    namespace._ARRAY_TO_STRING_COMMA_DEFAULT_SIGNATURES = (
        _ARRAY_TO_STRING_COMMA_DEFAULT_SIGNATURES
    )
    namespace.array_to_string_comma_default = cast(
        Any, array_to_string_comma_default
    )
    namespace._register_function(
        "array_to_string_comma_default",
        names=getattr(array_to_string_comma_default, "__duckdb_identifiers__", ()),
        symbols=getattr(array_to_string_comma_default, "__duckdb_symbols__", ()),
    )


# Preserve the original provenance for introspection tests that assert the
# module origin of these helpers through ``ScalarVarcharFunctions``.
split_part.__module__ = "duckplus.functions.scalar.string"
array_to_string.__module__ = "duckplus.functions.scalar.string"
array_to_string_comma_default.__module__ = "duckplus.functions.scalar.string"


_register()


__all__ = [
    "split_part",
    "array_to_string",
    "array_to_string_comma_default",
]
