"""Compatibility tests for typed function namespace shims."""

from __future__ import annotations

import inspect
import pickle

from duckplus.typed.expression import DuckTypeNamespace, GenericExpression
from duckplus.typed._generated_function_namespaces import AggregateNumericFunctions
from duckplus.typed.functions import _StaticFunctionNamespace, duckdb_function


class _LegacyNamespace(_StaticFunctionNamespace[GenericExpression]):
    function_type = "scalar"
    return_category = "generic"
    _IDENTIFIER_FUNCTIONS = {"legacy": "legacy"}
    _SYMBOLIC_FUNCTIONS = {"??": "legacy_symbol"}

    def legacy(self) -> str:  # pragma: no cover - trivial shim
        return "legacy"

    def legacy_symbol(self) -> str:  # pragma: no cover - trivial shim
        return "legacy-symbol"

    @duckdb_function("modern")
    def modern(self) -> str:
        return "modern"

    @duckdb_function(symbols=("!!",))
    def modern_symbol(self) -> str:
        return "modern-symbol"


def test_static_namespace_preserves_legacy_mappings() -> None:
    namespace = _LegacyNamespace()

    assert namespace.get("legacy") is not None
    assert namespace.get("modern") is not None

    assert "legacy" in namespace
    assert "modern" in namespace

    assert "legacy" in dir(namespace)
    assert "modern" in dir(namespace)

    assert "??" in namespace.symbols
    assert "!!" in namespace.symbols

    assert namespace._IDENTIFIER_FUNCTIONS["legacy"] == "legacy"
    assert namespace._IDENTIFIER_FUNCTIONS["modern"] == "modern"
    assert namespace._SYMBOLIC_FUNCTIONS["??"] == "legacy_symbol"
    assert namespace._SYMBOLIC_FUNCTIONS["!!"] == "modern_symbol"


def test_ducktype_namespace_register_decimal_factories_shim() -> None:
    namespace = DuckTypeNamespace()
    # Clearing mirrors legacy behaviour where callers could rebuild factories.
    namespace._decimal_names.clear()

    namespace._register_decimal_factories()

    assert "Decimal_1_0" in namespace.decimal_factory_names
    assert hasattr(namespace, "Decimal_1_0")


def test_aggregate_namespace_preserves_filter_aliases() -> None:
    namespace = AggregateNumericFunctions()

    assert "sum" in namespace._IDENTIFIER_FUNCTIONS
    assert "sum_filter" in namespace._IDENTIFIER_FUNCTIONS
    assert namespace.get("sum") is not None
    assert namespace.get("sum_filter") is not None


def test_aggregate_namespace_methods_are_introspectable() -> None:
    namespace = AggregateNumericFunctions()
    method = type(namespace).__dict__["arg_max"]

    assert method.__module__.startswith("duckplus.typed._generated_function_namespaces")
    assert method.__qualname__.startswith("AggregateNumericFunctions.")

    doc = inspect.getdoc(method)
    assert doc and "Call DuckDB function ``arg_max``." in doc

    signature = inspect.signature(method)
    assert "operands" in signature.parameters
    assert "order_by" in signature.parameters

    assert pickle.loads(pickle.dumps(method)) is method

