"""Compatibility tests for typed function namespace shims."""

from __future__ import annotations

from decimal import Decimal
import importlib
import inspect
import pickle

import pytest

from duckplus.static_typed import ExpressionDependency
from duckplus.static_typed.expression import DuckTypeNamespace, GenericExpression
from duckplus.static_typed.expressions import decimal as decimal_module
from duckplus.static_typed._generated_function_namespaces import (
    AggregateBlobFunctions,
    AggregateBooleanFunctions,
    AggregateGenericFunctions,
    AggregateNumericFunctions,
    AggregateVarcharFunctions,
    ScalarBooleanFunctions,
    ScalarGenericFunctions,
    ScalarVarcharFunctions,
)
from duckplus.static_typed.functions import (
    _StaticFunctionNamespace,
    duckdb_function,
)


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

    assert method.__module__ == "duckplus.functions.aggregate.arg_extrema"
    assert method.__qualname__ == "arg_max"

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


def test_boolean_aggregate_helpers_are_module_scoped() -> None:
    namespace = AggregateBooleanFunctions()

    bool_and_method = type(namespace).__dict__["bool_and"]
    assert bool_and_method.__module__ == "duckplus.functions.aggregate.boolean"
    bool_and_doc = inspect.getdoc(bool_and_method)
    assert bool_and_doc and "every input value is TRUE" in bool_and_doc

    bool_or_filter = type(namespace).__dict__["bool_or_filter"]
    assert bool_or_filter.__module__ == "duckplus.functions.aggregate.boolean"
    bool_or_filter_doc = inspect.getdoc(bool_or_filter)
    assert bool_or_filter_doc and "any input value is TRUE" in bool_or_filter_doc

    max_method = type(namespace).__dict__["max"]
    assert max_method.__module__ == "duckplus.functions.aggregate.extrema"
    max_doc = inspect.getdoc(max_method)
    assert max_doc and "maximum value present" in max_doc

    min_filter_method = type(namespace).__dict__["min_filter"]
    assert min_filter_method.__module__ == (
        "duckplus.functions.aggregate.extrema"
    )
    min_filter_doc = inspect.getdoc(min_filter_method)
    assert min_filter_doc and "minimum value present" in min_filter_doc


def test_counting_aggregate_helpers_are_module_scoped() -> None:
    namespace = AggregateNumericFunctions()

    count_method = type(namespace).__dict__["count"]
    assert count_method.__module__ == "duckplus.functions.aggregate.counting"
    count_doc = inspect.getdoc(count_method)
    assert count_doc and "non-NULL values" in count_doc

    count_if_filter = type(namespace).__dict__["count_if_filter"]
    assert count_if_filter.__module__ == "duckplus.functions.aggregate.counting"
    count_if_doc = inspect.getdoc(count_if_filter)
    assert count_if_doc and "TRUE values" in count_if_doc

    count_star = type(namespace).__dict__["count_star"]
    assert count_star.__module__ == "duckplus.functions.aggregate.counting"
    count_star_doc = inspect.getdoc(count_star)
    assert count_star_doc and "count_star" in count_star_doc


def test_bitwise_aggregate_helpers_are_module_scoped() -> None:
    namespace = AggregateNumericFunctions()

    bit_and_method = type(namespace).__dict__["bit_and"]
    assert bit_and_method.__module__ == "duckplus.functions.aggregate.bitwise"
    bit_and_doc = inspect.getdoc(bit_and_method)
    assert bit_and_doc and "bitwise AND" in bit_and_doc

    bit_xor_filter = type(namespace).__dict__["bit_xor_filter"]
    assert bit_xor_filter.__module__ == "duckplus.functions.aggregate.bitwise"
    bit_xor_doc = inspect.getdoc(bit_xor_filter)
    assert bit_xor_doc and "bitwise XOR" in bit_xor_doc


def test_statistical_aggregate_helpers_are_module_scoped() -> None:
    namespace = AggregateNumericFunctions()

    skewness_method = type(namespace).__dict__["skewness"]
    assert skewness_method.__module__ == "duckplus.functions.aggregate.statistics"
    skewness_doc = inspect.getdoc(skewness_method)
    assert skewness_doc and "skewness of all input values" in skewness_doc

    skewness_filter = type(namespace).__dict__["skewness_filter"]
    assert skewness_filter.__module__ == "duckplus.functions.aggregate.statistics"
    skewness_filter_doc = inspect.getdoc(skewness_filter)
    assert skewness_filter_doc and "skewness of all input values" in skewness_filter_doc


def test_regression_aggregate_helpers_are_module_scoped() -> None:
    namespace = AggregateNumericFunctions()

    covar_pop_method = type(namespace).__dict__["covar_pop"]
    assert covar_pop_method.__module__ == "duckplus.functions.aggregate.regression"
    covar_pop_doc = inspect.getdoc(covar_pop_method)
    assert covar_pop_doc and "population covariance" in covar_pop_doc

    regr_avgx_method = type(namespace).__dict__["regr_avgx"]
    assert regr_avgx_method.__module__ == "duckplus.functions.aggregate.regression"
    regr_avgx_doc = inspect.getdoc(regr_avgx_method)
    assert regr_avgx_doc and "independent variable" in regr_avgx_doc

    regr_count_filter_method = type(namespace).__dict__["regr_count_filter"]
    assert regr_count_filter_method.__module__ == (
        "duckplus.functions.aggregate.regression"
    )
    regr_count_doc = inspect.getdoc(regr_count_filter_method)
    assert regr_count_doc and "non-NULL number pairs" in regr_count_doc

    regr_sxy_method = type(namespace).__dict__["regr_sxy"]
    assert regr_sxy_method.__module__ == "duckplus.functions.aggregate.regression"
    regr_sxy_doc = inspect.getdoc(regr_sxy_method)
    assert regr_sxy_doc and "population covariance" in regr_sxy_doc


def test_generic_aggregate_helpers_are_module_scoped() -> None:
    namespace = AggregateGenericFunctions()

    any_value_method = type(namespace).__dict__["any_value"]
    assert any_value_method.__module__ == "duckplus.functions.aggregate.generic"
    any_value_doc = inspect.getdoc(any_value_method)
    assert any_value_doc and "first non-NULL value" in any_value_doc

    any_value_filter = type(namespace).__dict__["any_value_filter"]
    assert any_value_filter.__module__ == "duckplus.functions.aggregate.generic"
    any_value_filter_doc = inspect.getdoc(any_value_filter)
    assert any_value_filter_doc and "first non-NULL value" in any_value_filter_doc

    list_method = type(namespace).__dict__["list"]
    assert list_method.__module__ == "duckplus.functions.aggregate.list"
    list_doc = inspect.getdoc(list_method)
    assert list_doc and "LIST containing all the values" in list_doc

    list_filter_method = type(namespace).__dict__["list_filter"]
    assert list_filter_method.__module__ == "duckplus.functions.aggregate.list"
    list_filter_doc = inspect.getdoc(list_filter_method)
    assert list_filter_doc and "LIST containing all the values" in list_filter_doc

    map_method = type(namespace).__dict__["map"]
    assert map_method.__module__ == "duckplus.functions.aggregate.map"
    map_doc = inspect.getdoc(map_method)
    assert map_doc and "Creates a map from a set of keys and values" in map_doc

    median_method = type(namespace).__dict__["median"]
    assert median_method.__module__ == "duckplus.functions.aggregate.median"
    median_doc = inspect.getdoc(median_method)
    assert median_doc and "middle value of the set" in median_doc

    median_filter_method = type(namespace).__dict__["median_filter"]
    assert median_filter_method.__module__ == "duckplus.functions.aggregate.median"
    median_filter_doc = inspect.getdoc(median_filter_method)
    assert median_filter_doc and "middle value of the set" in median_filter_doc

    mode_method = type(namespace).__dict__["mode"]
    assert mode_method.__module__ == "duckplus.functions.aggregate.mode"
    mode_doc = inspect.getdoc(mode_method)
    assert mode_doc and "most frequent value" in mode_doc

    mode_filter_method = type(namespace).__dict__["mode_filter"]
    assert mode_filter_method.__module__ == "duckplus.functions.aggregate.mode"
    mode_filter_doc = inspect.getdoc(mode_filter_method)
    assert mode_filter_doc and "most frequent value" in mode_filter_doc

    bitstring_method = type(namespace).__dict__["bitstring_agg"]
    assert bitstring_method.__module__ == "duckplus.functions.aggregate.bitstring"
    bitstring_doc = inspect.getdoc(bitstring_method)
    assert bitstring_doc and "bitstring with bits set" in bitstring_doc

    bitstring_filter_method = type(namespace).__dict__["bitstring_agg_filter"]
    assert (
        bitstring_filter_method.__module__
        == "duckplus.functions.aggregate.bitstring"
    )
    bitstring_filter_doc = inspect.getdoc(bitstring_filter_method)
    assert bitstring_filter_doc and "bitstring with bits set" in bitstring_filter_doc

    first_method = type(namespace).__dict__["first"]
    assert first_method.__module__ == "duckplus.functions.aggregate.ordering"
    first_doc = inspect.getdoc(first_method)
    assert first_doc and "first value (NULL or non-NULL)" in first_doc

    first_filter_method = type(namespace).__dict__["first_filter"]
    assert first_filter_method.__module__ == "duckplus.functions.aggregate.ordering"
    first_filter_doc = inspect.getdoc(first_filter_method)
    assert first_filter_doc and "first value (NULL or non-NULL)" in first_filter_doc

    avg_method = type(namespace).__dict__["avg"]
    assert avg_method.__module__ == "duckplus.functions.aggregate.averages"
    avg_doc = inspect.getdoc(avg_method)
    assert avg_doc and "average value" in avg_doc

    avg_filter_method = type(namespace).__dict__["avg_filter"]
    assert avg_filter_method.__module__ == "duckplus.functions.aggregate.averages"
    avg_filter_doc = inspect.getdoc(avg_filter_method)
    assert avg_filter_doc and "average value" in avg_filter_doc

    mean_method = type(namespace).__dict__["mean"]
    assert mean_method.__module__ == "duckplus.functions.aggregate.averages"
    mean_doc = inspect.getdoc(mean_method)
    assert mean_doc and "average value" in mean_doc

    mean_filter_method = type(namespace).__dict__["mean_filter"]
    assert mean_filter_method.__module__ == "duckplus.functions.aggregate.averages"
    mean_filter_doc = inspect.getdoc(mean_filter_method)
    assert mean_filter_doc and "average value" in mean_filter_doc

    sum_method = type(namespace).__dict__["sum"]
    assert sum_method.__module__ == "duckplus.functions.aggregate.summation"
    sum_doc = inspect.getdoc(sum_method)
    assert sum_doc and "Calculates the sum value" in sum_doc

    sum_filter_method = type(namespace).__dict__["sum_filter"]
    assert sum_filter_method.__module__ == "duckplus.functions.aggregate.summation"
    sum_filter_doc = inspect.getdoc(sum_filter_method)
    assert sum_filter_doc and "Calculates the sum value" in sum_filter_doc

    max_method = type(namespace).__dict__["max"]
    assert max_method.__module__ == "duckplus.functions.aggregate.extrema"
    max_doc = inspect.getdoc(max_method)
    assert max_doc and "maximum value present" in max_doc

    min_filter_method = type(namespace).__dict__["min_filter"]
    assert min_filter_method.__module__ == (
        "duckplus.functions.aggregate.extrema"
    )
    min_filter_doc = inspect.getdoc(min_filter_method)
    assert min_filter_doc and "minimum value present" in min_filter_doc


def test_varchar_string_aggregate_helpers_are_module_scoped() -> None:
    namespace = AggregateVarcharFunctions()

    string_agg_method = type(namespace).__dict__["string_agg"]
    assert string_agg_method.__module__ == "duckplus.functions.aggregate.string"
    string_agg_doc = inspect.getdoc(string_agg_method)
    assert string_agg_doc and "optional separator" in string_agg_doc

    string_agg_filter_method = type(namespace).__dict__["string_agg_filter"]
    assert (
        string_agg_filter_method.__module__
        == "duckplus.functions.aggregate.string"
    )
    string_agg_filter_doc = inspect.getdoc(string_agg_filter_method)
    assert string_agg_filter_doc and "optional separator" in string_agg_filter_doc

    max_method = type(namespace).__dict__["max"]
    assert max_method.__module__ == "duckplus.functions.aggregate.extrema"
    max_doc = inspect.getdoc(max_method)
    assert max_doc and "maximum value present" in max_doc

    min_filter_method = type(namespace).__dict__["min_filter"]
    assert min_filter_method.__module__ == (
        "duckplus.functions.aggregate.extrema"
    )
    min_filter_doc = inspect.getdoc(min_filter_method)
    assert min_filter_doc and "minimum value present" in min_filter_doc


def test_varchar_string_macros_are_module_scoped() -> None:
    namespace = ScalarVarcharFunctions()

    split_part_method = type(namespace).__dict__["split_part"]
    assert split_part_method.__module__ == "duckplus.functions.scalar.string"
    split_part_doc = inspect.getdoc(split_part_method)
    assert split_part_doc and "Split a string" in split_part_doc

    array_to_string_method = type(namespace).__dict__["array_to_string"]
    assert array_to_string_method.__module__ == "duckplus.functions.scalar.string"
    array_to_string_doc = inspect.getdoc(array_to_string_method)
    assert array_to_string_doc and "Join array elements" in array_to_string_doc

    array_to_string_comma_default_method = type(namespace).__dict__[
        "array_to_string_comma_default"
    ]
    assert array_to_string_comma_default_method.__module__ == (
        "duckplus.functions.scalar.string"
    )
    array_to_string_comma_default_doc = inspect.getdoc(
        array_to_string_comma_default_method
    )
    assert (
        array_to_string_comma_default_doc
        and "comma separator" in array_to_string_comma_default_doc
    )


def test_varchar_system_macros_are_module_scoped() -> None:
    namespace = ScalarVarcharFunctions()

    current_catalog_method = type(namespace).__dict__["current_catalog"]
    assert current_catalog_method.__module__ == (
        "duckplus.functions.scalar.system"
    )
    current_catalog_doc = inspect.getdoc(current_catalog_method)
    assert current_catalog_doc and "catalog for the active connection" in current_catalog_doc

    current_user_method = type(namespace).__dict__["current_user"]
    assert current_user_method.__module__ == (
        "duckplus.functions.scalar.system"
    )
    current_user_doc = inspect.getdoc(current_user_method)
    assert current_user_doc and "authenticated user" in current_user_doc

    pg_get_viewdef_method = type(namespace).__dict__["pg_get_viewdef"]
    assert pg_get_viewdef_method.__module__ == (
        "duckplus.functions.scalar.system"
    )
    pg_get_viewdef_doc = inspect.getdoc(pg_get_viewdef_method)
    assert pg_get_viewdef_doc and "SQL definition" in pg_get_viewdef_doc


def test_boolean_postgres_privilege_macros_are_module_scoped() -> None:
    namespace = ScalarBooleanFunctions()

    has_any_column_privilege_method = type(namespace).__dict__["has_any_column_privilege"]
    assert has_any_column_privilege_method.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    has_any_column_privilege_doc = inspect.getdoc(has_any_column_privilege_method)
    assert (
        has_any_column_privilege_doc
        and "column privilege checks" in has_any_column_privilege_doc
    )

    has_tablespace_privilege_method = type(namespace).__dict__["has_tablespace_privilege"]
    assert has_tablespace_privilege_method.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    has_tablespace_privilege_doc = inspect.getdoc(has_tablespace_privilege_method)
    assert (
        has_tablespace_privilege_doc
        and "tablespace privilege" in has_tablespace_privilege_doc
    )


def test_boolean_postgres_visibility_macros_are_module_scoped() -> None:
    namespace = ScalarBooleanFunctions()

    pg_collation_is_visible_method = type(namespace).__dict__["pg_collation_is_visible"]
    assert pg_collation_is_visible_method.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    pg_collation_is_visible_doc = inspect.getdoc(pg_collation_is_visible_method)
    assert (
        pg_collation_is_visible_doc
        and "collation identifier is visible" in pg_collation_is_visible_doc
    )

    pg_has_role_method = type(namespace).__dict__["pg_has_role"]
    assert pg_has_role_method.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    pg_has_role_doc = inspect.getdoc(pg_has_role_method)
    assert pg_has_role_doc and "role privilege" in pg_has_role_doc

    pg_ts_parser_is_visible_method = type(namespace).__dict__["pg_ts_parser_is_visible"]
    assert pg_ts_parser_is_visible_method.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    pg_ts_parser_is_visible_doc = inspect.getdoc(pg_ts_parser_is_visible_method)
    assert (
        pg_ts_parser_is_visible_doc
        and "parser identifier is visible" in pg_ts_parser_is_visible_doc
    )


def test_generic_array_macros_are_module_scoped() -> None:
    namespace = ScalarGenericFunctions()

    array_append_method = type(namespace).__dict__["array_append"]
    assert array_append_method.__module__ == "duckplus.functions.scalar.list"
    array_append_doc = inspect.getdoc(array_append_method)
    assert array_append_doc and "Append an element" in array_append_doc

    array_pop_front_method = type(namespace).__dict__["array_pop_front"]
    assert array_pop_front_method.__module__ == "duckplus.functions.scalar.list"
    array_pop_front_doc = inspect.getdoc(array_pop_front_method)
    assert array_pop_front_doc and "Drop the first element" in array_pop_front_doc


def test_numeric_summation_helpers_are_module_scoped() -> None:
    namespace = AggregateNumericFunctions()

    avg_method = type(namespace).__dict__["avg"]
    assert avg_method.__module__ == "duckplus.functions.aggregate.averages"
    avg_doc = inspect.getdoc(avg_method)
    assert avg_doc and "average value" in avg_doc

    avg_filter_method = type(namespace).__dict__["avg_filter"]
    assert avg_filter_method.__module__ == "duckplus.functions.aggregate.averages"
    avg_filter_doc = inspect.getdoc(avg_filter_method)
    assert avg_filter_doc and "average value" in avg_filter_doc

    mean_method = type(namespace).__dict__["mean"]
    assert mean_method.__module__ == "duckplus.functions.aggregate.averages"
    mean_doc = inspect.getdoc(mean_method)
    assert mean_doc and "average value" in mean_doc

    mean_filter_method = type(namespace).__dict__["mean_filter"]
    assert mean_filter_method.__module__ == "duckplus.functions.aggregate.averages"
    mean_filter_doc = inspect.getdoc(mean_filter_method)
    assert mean_filter_doc and "average value" in mean_filter_doc

    sum_method = type(namespace).__dict__["sum"]
    assert sum_method.__module__ == "duckplus.functions.aggregate.summation"
    sum_doc = inspect.getdoc(sum_method)
    assert sum_doc and "Calculates the sum value" in sum_doc

    sum_filter_method = type(namespace).__dict__["sum_filter"]
    assert sum_filter_method.__module__ == "duckplus.functions.aggregate.summation"
    sum_filter_doc = inspect.getdoc(sum_filter_method)
    assert sum_filter_doc and "Calculates the sum value" in sum_filter_doc

    product_method = type(namespace).__dict__["product"]
    assert product_method.__module__ == "duckplus.functions.aggregate.summation"
    product_doc = inspect.getdoc(product_method)
    assert product_doc and "Calculates the product" in product_doc

    product_filter_method = type(namespace).__dict__["product_filter"]
    assert product_filter_method.__module__ == "duckplus.functions.aggregate.summation"
    product_filter_doc = inspect.getdoc(product_filter_method)
    assert product_filter_doc and "Calculates the product" in product_filter_doc


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


def test_generic_quantile_helpers_are_module_scoped() -> None:
    namespace = AggregateGenericFunctions()

    quantile_method = type(namespace).__dict__["quantile"]
    assert quantile_method.__module__ == "duckplus.functions.aggregate.quantiles"
    quantile_doc = inspect.getdoc(quantile_method)
    assert quantile_doc and "exact quantile number between 0 and 1" in quantile_doc

    quantile_filter_method = type(namespace).__dict__["quantile_filter"]
    assert quantile_filter_method.__module__ == "duckplus.functions.aggregate.quantiles"
    quantile_filter_doc = inspect.getdoc(quantile_filter_method)
    assert quantile_filter_doc and "exact quantile number between 0 and 1" in quantile_filter_doc

    quantile_cont_method = type(namespace).__dict__["quantile_cont"]
    assert quantile_cont_method.__module__ == "duckplus.functions.aggregate.quantiles"
    quantile_cont_doc = inspect.getdoc(quantile_cont_method)
    assert quantile_cont_doc and "interpolated quantile number between 0 and 1" in quantile_cont_doc

    quantile_cont_filter_method = type(namespace).__dict__["quantile_cont_filter"]
    assert quantile_cont_filter_method.__module__ == "duckplus.functions.aggregate.quantiles"
    quantile_cont_filter_doc = inspect.getdoc(quantile_cont_filter_method)
    assert (
        quantile_cont_filter_doc
        and "interpolated quantile number between 0 and 1"
        in quantile_cont_filter_doc
    )

    quantile_disc_method = type(namespace).__dict__["quantile_disc"]
    assert quantile_disc_method.__module__ == "duckplus.functions.aggregate.quantiles"
    quantile_disc_doc = inspect.getdoc(quantile_disc_method)
    assert quantile_disc_doc and "exact quantile number between 0 and 1" in quantile_disc_doc

    quantile_disc_filter_method = type(namespace).__dict__["quantile_disc_filter"]
    assert quantile_disc_filter_method.__module__ == "duckplus.functions.aggregate.quantiles"
    quantile_disc_filter_doc = inspect.getdoc(quantile_disc_filter_method)
    assert (
        quantile_disc_filter_doc
        and "exact quantile number between 0 and 1"
        in quantile_disc_filter_doc
    )


def test_blob_arg_extrema_helpers_are_module_scoped() -> None:
    namespace = AggregateBlobFunctions()

    arg_max_method = type(namespace).__dict__["arg_max"]
    assert arg_max_method.__module__ == "duckplus.functions.aggregate.arg_extrema"
    arg_max_doc = inspect.getdoc(arg_max_method)
    assert arg_max_doc and "maximum val" in arg_max_doc

    arg_min_null_filter = type(namespace).__dict__["arg_min_null_filter"]
    assert arg_min_null_filter.__module__ == "duckplus.functions.aggregate.arg_extrema"
    arg_min_null_filter_doc = inspect.getdoc(arg_min_null_filter)
    assert arg_min_null_filter_doc and "minimum val" in arg_min_null_filter_doc


def test_blob_extremum_by_value_helpers_are_module_scoped() -> None:
    namespace = AggregateBlobFunctions()

    max_by_method = type(namespace).__dict__["max_by"]
    assert max_by_method.__module__ == (
        "duckplus.functions.aggregate.extremum_by_value"
    )
    max_by_doc = inspect.getdoc(max_by_method)
    assert max_by_doc and "maximum val" in max_by_doc

    min_by_filter_method = type(namespace).__dict__["min_by_filter"]
    assert min_by_filter_method.__module__ == (
        "duckplus.functions.aggregate.extremum_by_value"
    )
    min_by_filter_doc = inspect.getdoc(min_by_filter_method)
    assert min_by_filter_doc and "minimum val" in min_by_filter_doc

    max_method = type(namespace).__dict__["max"]
    assert max_method.__module__ == "duckplus.functions.aggregate.extrema"
    max_doc = inspect.getdoc(max_method)
    assert max_doc and "maximum value present" in max_doc

    min_filter_method = type(namespace).__dict__["min_filter"]
    assert min_filter_method.__module__ == (
        "duckplus.functions.aggregate.extrema"
    )
    min_filter_doc = inspect.getdoc(min_filter_method)
    assert min_filter_doc and "minimum value present" in min_filter_doc


def test_varchar_arg_extrema_helpers_are_module_scoped() -> None:
    namespace = AggregateVarcharFunctions()

    arg_max_method = type(namespace).__dict__["arg_max"]
    assert arg_max_method.__module__ == "duckplus.functions.aggregate.arg_extrema"
    arg_max_doc = inspect.getdoc(arg_max_method)
    assert arg_max_doc and "maximum val" in arg_max_doc

    arg_min_null_filter = type(namespace).__dict__["arg_min_null_filter"]
    assert arg_min_null_filter.__module__ == "duckplus.functions.aggregate.arg_extrema"
    arg_min_null_filter_doc = inspect.getdoc(arg_min_null_filter)
    assert arg_min_null_filter_doc and "minimum val" in arg_min_null_filter_doc


def test_varchar_extremum_by_value_helpers_are_module_scoped() -> None:
    namespace = AggregateVarcharFunctions()

    max_by_method = type(namespace).__dict__["max_by"]
    assert max_by_method.__module__ == (
        "duckplus.functions.aggregate.extremum_by_value"
    )
    max_by_doc = inspect.getdoc(max_by_method)
    assert max_by_doc and "maximum val" in max_by_doc

    min_by_filter_method = type(namespace).__dict__["min_by_filter"]
    assert min_by_filter_method.__module__ == (
        "duckplus.functions.aggregate.extremum_by_value"
    )
    min_by_filter_doc = inspect.getdoc(min_by_filter_method)
    assert min_by_filter_doc and "minimum val" in min_by_filter_doc

    max_method = type(namespace).__dict__["max"]
    assert max_method.__module__ == "duckplus.functions.aggregate.extrema"
    max_doc = inspect.getdoc(max_method)
    assert max_doc and "maximum value present" in max_doc

    min_filter_method = type(namespace).__dict__["min_filter"]
    assert min_filter_method.__module__ == (
        "duckplus.functions.aggregate.extrema"
    )
    min_filter_doc = inspect.getdoc(min_filter_method)
    assert min_filter_doc and "minimum value present" in min_filter_doc


def test_numeric_arg_extrema_helpers_are_module_scoped() -> None:
    namespace = AggregateNumericFunctions()

    arg_max_method = type(namespace).__dict__["arg_max"]
    assert arg_max_method.__module__ == "duckplus.functions.aggregate.arg_extrema"
    arg_max_doc = inspect.getdoc(arg_max_method)
    assert arg_max_doc and "maximum val" in arg_max_doc

    arg_min_null_filter = type(namespace).__dict__["arg_min_null_filter"]
    assert arg_min_null_filter.__module__ == "duckplus.functions.aggregate.arg_extrema"
    arg_min_null_filter_doc = inspect.getdoc(arg_min_null_filter)
    assert arg_min_null_filter_doc and "minimum val" in arg_min_null_filter_doc


def test_numeric_extremum_by_value_helpers_are_module_scoped() -> None:
    namespace = AggregateNumericFunctions()

    max_by_method = type(namespace).__dict__["max_by"]
    assert max_by_method.__module__ == (
        "duckplus.functions.aggregate.extremum_by_value"
    )
    max_by_doc = inspect.getdoc(max_by_method)
    assert max_by_doc and "maximum val" in max_by_doc

    min_by_filter_method = type(namespace).__dict__["min_by_filter"]
    assert min_by_filter_method.__module__ == (
        "duckplus.functions.aggregate.extremum_by_value"
    )
    min_by_filter_doc = inspect.getdoc(min_by_filter_method)
    assert min_by_filter_doc and "minimum val" in min_by_filter_doc


def test_generic_arg_extrema_helpers_are_module_scoped() -> None:
    namespace = AggregateGenericFunctions()

    arg_max_method = type(namespace).__dict__["arg_max"]
    assert arg_max_method.__module__ == "duckplus.functions.aggregate.arg_extrema"
    arg_max_doc = inspect.getdoc(arg_max_method)
    assert arg_max_doc and "maximum val" in arg_max_doc
    assert "ANY[]" in arg_max_doc

    arg_min_null_filter = type(namespace).__dict__["arg_min_null_filter"]
    assert arg_min_null_filter.__module__ == "duckplus.functions.aggregate.arg_extrema"
    arg_min_null_filter_doc = inspect.getdoc(arg_min_null_filter)
    assert arg_min_null_filter_doc and "minimum val" in arg_min_null_filter_doc


def test_generic_extremum_by_value_helpers_are_module_scoped() -> None:
    namespace = AggregateGenericFunctions()

    max_by_method = type(namespace).__dict__["max_by"]
    assert max_by_method.__module__ == (
        "duckplus.functions.aggregate.extremum_by_value"
    )
    max_by_doc = inspect.getdoc(max_by_method)
    assert max_by_doc and "maximum val" in max_by_doc
    assert "ANY[]" in max_by_doc

    min_by_filter_method = type(namespace).__dict__["min_by_filter"]
    assert min_by_filter_method.__module__ == (
        "duckplus.functions.aggregate.extremum_by_value"
    )
    min_by_filter_doc = inspect.getdoc(min_by_filter_method)
    assert min_by_filter_doc and "minimum val" in min_by_filter_doc


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

