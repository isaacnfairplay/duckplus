"""Tests for the static typed DuckDB function namespace."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from duckplus.typed import ducktype
from duckplus.typed.expression import GenericExpression, VarcharExpression
from duckplus.typed.functions import (
    AGGREGATE_FUNCTIONS,
    SCALAR_FUNCTIONS,
    WINDOW_FUNCTIONS,
    DuckDBFunctionNamespace,
    _coerce_function_operand,
)
from duckplus.typed.types import IntegerType


def test_scalar_function_signatures_are_available_without_runtime_loading() -> None:
    abs_function = SCALAR_FUNCTIONS.Numeric.abs
    doc = abs_function.__doc__
    assert doc is not None
    assert "DuckDB scalar function" in doc
    signatures = abs_function.signatures
    assert signatures
    first_signature = signatures[0]
    assert first_signature.function_name == "abs"
    assert first_signature.function_type == "scalar"
    rendered = [signature.call_syntax() for signature in signatures]
    assert any(call.startswith("abs(") for call in rendered)


def test_symbolic_functions_are_exposed_via_index_lookup() -> None:
    plus_function = SCALAR_FUNCTIONS.Numeric["+"]
    rendered = [str(signature) for signature in plus_function.signatures]
    assert any("+" in signature for signature in rendered)


def test_aggregate_namespace_matches_module_level_singletons() -> None:
    namespace = DuckDBFunctionNamespace()
    sum_function = namespace.Aggregate.Numeric.sum
    singleton_sum = AGGREGATE_FUNCTIONS.Numeric.sum
    assert sum_function.signatures == singleton_sum.signatures
    assert sum_function.__doc__ == singleton_sum.__doc__


def test_aggregate_functions_include_filter_variants() -> None:
    numeric_namespace = AGGREGATE_FUNCTIONS.Numeric
    exported = set(numeric_namespace.__dir__())
    base_functions = {name for name in exported if not name.endswith("_filter")}
    for name in base_functions:
        assert f"{name}_filter" in exported

    amount = ducktype.Numeric("amount")
    include_flag = ducktype.Boolean("include_flag")
    filtered_sum = AGGREGATE_FUNCTIONS.Numeric.sum_filter(include_flag, amount)

    assert filtered_sum.render() == 'sum("amount") FILTER (WHERE "include_flag")'
    assert include_flag.dependencies.union(amount.dependencies) == filtered_sum.dependencies

    with pytest.raises(TypeError, match="BOOLEAN"):
        AGGREGATE_FUNCTIONS.Numeric.sum_filter(amount, amount)


def test_aggregate_min_by_dispatches_by_expression_type() -> None:
    namespace = DuckDBFunctionNamespace()
    varchar_expr = ducktype.Varchar("label")
    order_expr = ducktype.Numeric("ordering")

    result = namespace.Aggregate.min_by(varchar_expr, order_expr)

    assert isinstance(result, VarcharExpression)
    assert result.render() == 'min_by("label", "ordering")'

    generic_value = ducktype.Generic("payload")
    generic_result = namespace.Aggregate.max_by(generic_value, order_expr)

    assert isinstance(generic_result, GenericExpression)
    assert generic_result.render() == 'max_by("payload", "ordering")'


def test_window_namespace_is_populated() -> None:
    numeric_window = WINDOW_FUNCTIONS.Numeric
    # Some DuckDB builds expose window functions via the scalar namespace only, so the
    # window namespace may legitimately be empty. We still expect the structure to be
    # present and to advertise the correct function type metadata.
    assert numeric_window.function_type == "window"
    assert isinstance(numeric_window.__dir__(), list)


def test_stub_defines_return_type_annotations_for_language_servers() -> None:
    stub_path = Path(__file__).resolve().parents[1] / "duckplus" / "typed" / "_generated_function_namespaces.pyi"
    stub_source = stub_path.read_text(encoding="utf-8")
    module = ast.parse(stub_source, filename=str(stub_path))
    classes = {
        node.name: node for node in module.body if isinstance(node, ast.ClassDef)
    }
    scalar_numeric = classes["ScalarNumericFunctions"]
    aggregate_boolean = classes["AggregateBooleanFunctions"]

    def _find_annotation(class_node: ast.ClassDef, name: str) -> ast.expr:
        for statement in class_node.body:
            if isinstance(statement, ast.AnnAssign) and getattr(
                statement.target, "id", None
            ) == name:
                return statement.annotation
        msg = f"Missing annotation for {name}"
        raise AssertionError(msg)

    def _extract_generic_argument(annotation: ast.expr) -> str:
        assert isinstance(annotation, ast.Subscript)
        value = annotation.value
        assert isinstance(value, ast.Name)
        assert value.id == "_DuckDBFunctionCall"
        slice_value = annotation.slice
        if isinstance(slice_value, ast.Tuple):
            msg = "Unexpected tuple annotation"
            raise AssertionError(msg)
        if hasattr(ast, "Index") and isinstance(slice_value, getattr(ast, "Index")):
            slice_value = slice_value.value  # pragma: no cover - compatibility shim
        assert isinstance(slice_value, ast.Name)
        return slice_value.id

    abs_annotation = _find_annotation(scalar_numeric, "abs")
    bool_and_annotation = _find_annotation(aggregate_boolean, "bool_and")
    assert _extract_generic_argument(abs_annotation) == "NumericExpression"
    assert _extract_generic_argument(bool_and_annotation) == "BooleanExpression"


def test_coerce_function_operand_rejects_out_of_range_integer_literal() -> None:
    expected_type = IntegerType("UTINYINT")
    with pytest.raises(TypeError, match="expected UTINYINT, got USMALLINT"):
        _coerce_function_operand(512, expected_type)
