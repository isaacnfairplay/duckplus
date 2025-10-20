"""Compatibility tests for typed function namespace shims."""

from __future__ import annotations

from decimal import Decimal
import importlib
import inspect
import pickle

import pytest

from duckplus.typed import ExpressionDependency
from duckplus.typed.expression import DuckTypeNamespace, GenericExpression
from duckplus.typed.expressions import decimal as decimal_module
from duckplus.typed._generated_function_namespaces import (
    AggregateGenericFunctions,
    AggregateNumericFunctions,
)
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


def test_decimal_factories_expose_metadata_at_import_time() -> None:
    module = importlib.reload(decimal_module)

    expression = module.Decimal_10_2("balance")
    assert expression.render() == '"balance"'
    assert expression.dependencies == {ExpressionDependency.column("balance")}
    assert expression.duck_type.render() == "DECIMAL(10, 2)"

    literal = module.Decimal_4_3.literal(Decimal("1.234"))
    assert literal.render() == "1.234"
    assert literal.dependencies == frozenset()
    assert literal.duck_type.render() == "DECIMAL(4, 3)"

    namespace = DuckTypeNamespace()
    assert namespace.decimal_factory_names == module.DECIMAL_FACTORY_NAMES
    namespace_factory = getattr(type(namespace), "Decimal_10_2")
    assert namespace_factory.expression_type.default_type().render() == "DECIMAL(10, 2)"


def test_decimal_registration_rejects_duplicate_factories(monkeypatch: pytest.MonkeyPatch) -> None:
    duplicate_items = (
        ("Decimal_1_0", decimal_module.Decimal_1_0),
        ("Decimal_1_0", decimal_module.Decimal_1_0),
    )
    monkeypatch.setattr(decimal_module, "_DECIMAL_FACTORY_ITEMS", duplicate_items, raising=False)

    with pytest.raises(ValueError, match="Duplicate decimal factory name"):
        register_target = type("DuplicateNamespace", (object,), {})
        decimal_module.register_decimal_factories(register_target)


def test_decimal_registration_requires_decimal_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    class BadExpression(decimal_module.NumericExpression):
        __slots__ = ()

    bad_factory = decimal_module.NumericFactory(BadExpression)

    monkeypatch.setattr(
        decimal_module,
        "_DECIMAL_FACTORY_ITEMS",
        (("Decimal_2_1", bad_factory),),
        raising=False,
    )

    with pytest.raises(ValueError, match="must expose DecimalType metadata"):
        register_target = type("MetadataNamespace", (object,), {})
        decimal_module.register_decimal_factories(register_target)


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


def test_aggregate_approximation_helpers_are_module_scoped() -> None:
    namespace = AggregateNumericFunctions()
    method = type(namespace).__dict__["approx_count_distinct"]

    assert method.__module__ == "duckplus.functions.aggregate.approximation"
    assert method.__qualname__.startswith("approx_count_distinct")

    doc = inspect.getdoc(method)
    assert doc and "HyperLogLog" in doc

    signature = inspect.signature(method)
    assert "operands" in signature.parameters
    assert "order_by" in signature.parameters

    filter_method = type(namespace).__dict__["approx_count_distinct_filter"]
    assert filter_method.__module__ == "duckplus.functions.aggregate.approximation"
    filter_doc = inspect.getdoc(filter_method)
    assert filter_doc and "HyperLogLog" in filter_doc


def test_generic_approximation_helpers_are_module_scoped() -> None:
    namespace = AggregateGenericFunctions()

    approx_quantile = type(namespace).__dict__["approx_quantile"]
    assert approx_quantile.__module__ == "duckplus.functions.aggregate.approximation"
    approx_quantile_doc = inspect.getdoc(approx_quantile)
    assert approx_quantile_doc and "T-Digest" in approx_quantile_doc

    approx_quantile_filter = type(namespace).__dict__["approx_quantile_filter"]
    assert approx_quantile_filter.__module__ == "duckplus.functions.aggregate.approximation"
    approx_quantile_filter_doc = inspect.getdoc(approx_quantile_filter)
    assert approx_quantile_filter_doc and "T-Digest" in approx_quantile_filter_doc

    approx_top_k = type(namespace).__dict__["approx_top_k"]
    assert approx_top_k.__module__ == "duckplus.functions.aggregate.approximation"
    approx_top_k_doc = inspect.getdoc(approx_top_k)
    assert approx_top_k_doc and "approximately most occurring" in approx_top_k_doc

    histogram_method = type(namespace).__dict__["histogram"]
    assert histogram_method.__module__ == "duckplus.functions.aggregate.approximation"
    histogram_doc = inspect.getdoc(histogram_method)
    assert histogram_doc and "bucket and count" in histogram_doc

    histogram_exact_method = type(namespace).__dict__["histogram_exact"]
    assert histogram_exact_method.__module__ == "duckplus.functions.aggregate.approximation"
    histogram_exact_doc = inspect.getdoc(histogram_exact_method)
    assert histogram_exact_doc and "matching the buckets exactly" in histogram_exact_doc


def test_numeric_quantile_helpers_are_module_scoped() -> None:
    namespace = AggregateNumericFunctions()

    approx_quantile = type(namespace).__dict__["approx_quantile"]
    assert approx_quantile.__module__ == "duckplus.functions.aggregate.approximation"
    approx_quantile_doc = inspect.getdoc(approx_quantile)
    assert approx_quantile_doc and "T-Digest" in approx_quantile_doc

    approx_quantile_filter = type(namespace).__dict__["approx_quantile_filter"]
    assert approx_quantile_filter.__module__ == "duckplus.functions.aggregate.approximation"
    approx_quantile_filter_doc = inspect.getdoc(approx_quantile_filter)
    assert approx_quantile_filter_doc and "T-Digest" in approx_quantile_filter_doc

