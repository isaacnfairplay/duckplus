"""Decorator-backed overrides for scalar array/list macros."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from duckplus.static_typed.expression import GenericExpression
from duckplus.static_typed.functions import (
    DuckDBFunctionDefinition,
    call_duckdb_function,
    duckdb_function,
)
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        ScalarGenericFunctions,
    )


_ARRAY_APPEND_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="array_append",
        function_type="scalar",
        return_type=parse_type('"NULL"[]'),
        parameter_types=(None, None),
        parameters=("arr", "el"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="list_append(arr, el)",
    ),
)


@duckdb_function("array_append")
def array_append(
    self: "ScalarGenericFunctions",
    *operands: object,
) -> GenericExpression:
    """Append an element to the end of an array."""

    return cast(
        GenericExpression,
        call_duckdb_function(
            _ARRAY_APPEND_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_ARRAY_INTERSECT_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="array_intersect",
        function_type="scalar",
        return_type=parse_type('"NULL"'),
        parameter_types=(None, None),
        parameters=("l1", "l2"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="list_intersect(l1, l2)",
    ),
)


@duckdb_function("array_intersect")
def array_intersect(
    self: "ScalarGenericFunctions",
    *operands: object,
) -> GenericExpression:
    """Return the intersection of two arrays."""

    return cast(
        GenericExpression,
        call_duckdb_function(
            _ARRAY_INTERSECT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_ARRAY_POP_BACK_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="array_pop_back",
        function_type="scalar",
        return_type=parse_type('"NULL"'),
        parameter_types=(None,),
        parameters=("arr",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="arr[:(len(arr) - 1)]",
    ),
)


@duckdb_function("array_pop_back")
def array_pop_back(
    self: "ScalarGenericFunctions",
    *operands: object,
) -> GenericExpression:
    """Drop the final element from an array."""

    return cast(
        GenericExpression,
        call_duckdb_function(
            _ARRAY_POP_BACK_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_ARRAY_POP_FRONT_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="array_pop_front",
        function_type="scalar",
        return_type=parse_type('"NULL"'),
        parameter_types=(None,),
        parameters=("arr",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="arr[2:]",
    ),
)


@duckdb_function("array_pop_front")
def array_pop_front(
    self: "ScalarGenericFunctions",
    *operands: object,
) -> GenericExpression:
    """Drop the first element from an array."""

    return cast(
        GenericExpression,
        call_duckdb_function(
            _ARRAY_POP_FRONT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_ARRAY_PREPEND_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="array_prepend",
        function_type="scalar",
        return_type=parse_type('"NULL"[]'),
        parameter_types=(None, None),
        parameters=("el", "arr"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="list_prepend(el, arr)",
    ),
)


@duckdb_function("array_prepend")
def array_prepend(
    self: "ScalarGenericFunctions",
    *operands: object,
) -> GenericExpression:
    """Insert an element at the beginning of an array."""

    return cast(
        GenericExpression,
        call_duckdb_function(
            _ARRAY_PREPEND_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_ARRAY_PUSH_BACK_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="array_push_back",
        function_type="scalar",
        return_type=parse_type('"NULL"[]'),
        parameter_types=(None, None),
        parameters=("arr", "e"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="list_concat(arr, list_value(e))",
    ),
)


@duckdb_function("array_push_back")
def array_push_back(
    self: "ScalarGenericFunctions",
    *operands: object,
) -> GenericExpression:
    """Push an element onto the end of an array."""

    return cast(
        GenericExpression,
        call_duckdb_function(
            _ARRAY_PUSH_BACK_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_ARRAY_PUSH_FRONT_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="array_push_front",
        function_type="scalar",
        return_type=parse_type('"NULL"[]'),
        parameter_types=(None, None),
        parameters=("arr", "e"),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="list_concat(list_value(e), arr)",
    ),
)


@duckdb_function("array_push_front")
def array_push_front(
    self: "ScalarGenericFunctions",
    *operands: object,
) -> GenericExpression:
    """Push an element onto the front of an array."""

    return cast(
        GenericExpression,
        call_duckdb_function(
            _ARRAY_PUSH_FRONT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


_ARRAY_REVERSE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="array_reverse",
        function_type="scalar",
        return_type=parse_type('"NULL"'),
        parameter_types=(None,),
        parameters=("l",),
        varargs=None,
        description=None,
        comment=None,
        macro_definition="list_reverse(l)",
    ),
)


@duckdb_function("array_reverse")
def array_reverse(
    self: "ScalarGenericFunctions",
    *operands: object,
) -> GenericExpression:
    """Reverse the order of elements within an array."""

    return cast(
        GenericExpression,
        call_duckdb_function(
            _ARRAY_REVERSE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        ),
    )


def _register() -> None:
    """Attach scalar list macro helpers onto the generic namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        ScalarGenericFunctions,
    )

    namespace: Any = ScalarGenericFunctions

    namespace._ARRAY_APPEND_SIGNATURES = _ARRAY_APPEND_SIGNATURES
    namespace.array_append = cast(Any, array_append)
    namespace._register_function(
        "array_append",
        names=getattr(array_append, "__duckdb_identifiers__", ()),
        symbols=getattr(array_append, "__duckdb_symbols__", ()),
    )

    namespace._ARRAY_INTERSECT_SIGNATURES = _ARRAY_INTERSECT_SIGNATURES
    namespace.array_intersect = cast(Any, array_intersect)
    namespace._register_function(
        "array_intersect",
        names=getattr(array_intersect, "__duckdb_identifiers__", ()),
        symbols=getattr(array_intersect, "__duckdb_symbols__", ()),
    )

    namespace._ARRAY_POP_BACK_SIGNATURES = _ARRAY_POP_BACK_SIGNATURES
    namespace.array_pop_back = cast(Any, array_pop_back)
    namespace._register_function(
        "array_pop_back",
        names=getattr(array_pop_back, "__duckdb_identifiers__", ()),
        symbols=getattr(array_pop_back, "__duckdb_symbols__", ()),
    )

    namespace._ARRAY_POP_FRONT_SIGNATURES = _ARRAY_POP_FRONT_SIGNATURES
    namespace.array_pop_front = cast(Any, array_pop_front)
    namespace._register_function(
        "array_pop_front",
        names=getattr(array_pop_front, "__duckdb_identifiers__", ()),
        symbols=getattr(array_pop_front, "__duckdb_symbols__", ()),
    )

    namespace._ARRAY_PREPEND_SIGNATURES = _ARRAY_PREPEND_SIGNATURES
    namespace.array_prepend = cast(Any, array_prepend)
    namespace._register_function(
        "array_prepend",
        names=getattr(array_prepend, "__duckdb_identifiers__", ()),
        symbols=getattr(array_prepend, "__duckdb_symbols__", ()),
    )

    namespace._ARRAY_PUSH_BACK_SIGNATURES = _ARRAY_PUSH_BACK_SIGNATURES
    namespace.array_push_back = cast(Any, array_push_back)
    namespace._register_function(
        "array_push_back",
        names=getattr(array_push_back, "__duckdb_identifiers__", ()),
        symbols=getattr(array_push_back, "__duckdb_symbols__", ()),
    )

    namespace._ARRAY_PUSH_FRONT_SIGNATURES = _ARRAY_PUSH_FRONT_SIGNATURES
    namespace.array_push_front = cast(Any, array_push_front)
    namespace._register_function(
        "array_push_front",
        names=getattr(array_push_front, "__duckdb_identifiers__", ()),
        symbols=getattr(array_push_front, "__duckdb_symbols__", ()),
    )

    namespace._ARRAY_REVERSE_SIGNATURES = _ARRAY_REVERSE_SIGNATURES
    namespace.array_reverse = cast(Any, array_reverse)
    namespace._register_function(
        "array_reverse",
        names=getattr(array_reverse, "__duckdb_identifiers__", ()),
        symbols=getattr(array_reverse, "__duckdb_symbols__", ()),
    )


# Preserve the original provenance for introspection tests that assert the
# module origin of these helpers through ``ScalarGenericFunctions``.
array_append.__module__ = "duckplus.functions.scalar.list"
array_intersect.__module__ = "duckplus.functions.scalar.list"
array_pop_back.__module__ = "duckplus.functions.scalar.list"
array_pop_front.__module__ = "duckplus.functions.scalar.list"
array_prepend.__module__ = "duckplus.functions.scalar.list"
array_push_back.__module__ = "duckplus.functions.scalar.list"
array_push_front.__module__ = "duckplus.functions.scalar.list"
array_reverse.__module__ = "duckplus.functions.scalar.list"


_register()


__all__ = [
    "array_append",
    "array_intersect",
    "array_pop_back",
    "array_pop_front",
    "array_prepend",
    "array_push_back",
    "array_push_front",
    "array_reverse",
]
