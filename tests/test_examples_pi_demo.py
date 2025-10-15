"""Tests covering the Pi-themed typed expression demo."""

import builtins

import pytest

from duckplus.examples import pi_demo
from duckplus.typed import ExpressionDependency


def test_build_circle_expressions_uses_numeric_dependencies() -> None:
    expressions = pi_demo.build_circle_expressions("r")
    assert expressions.radius.render() == '"r"'
    assert expressions.area.render() == '((3.141592653589793::DOUBLE * "r") * "r")'
    assert expressions.area.dependencies == {ExpressionDependency.column("r")}
    assert expressions.circumference.render() == '((3.141592653589793::DOUBLE * "r") * 2)'


def test_project_circle_metrics_aliases_columns() -> None:
    projection = pi_demo.project_circle_metrics("r")
    rendered = [expression.render() for expression in projection]
    assert rendered == [
        '"r" AS "radius"',
        '((3.141592653589793::DOUBLE * "r") * "r") AS "area"',
        '((3.141592653589793::DOUBLE * "r") * 2) AS "circumference"',
    ]


def test_summarise_circle_metrics_renders_aggregations() -> None:
    summary = pi_demo.summarise_circle_metrics("r")
    rendered = [expression.render() for expression in summary]
    assert rendered == [
        'sum(((3.141592653589793::DOUBLE * "r") * "r")) AS "total_area"',
        'sum(((3.141592653589793::DOUBLE * "r") * 2)) AS "total_circumference"',
    ]


def test_run_duckdb_demo_requires_duckdb(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_import(name: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        if name == "duckdb":
            raise ModuleNotFoundError
        return original_import(name, *args, **kwargs)

    original_import = builtins.__import__
    monkeypatch.setattr(builtins, "__import__", fake_import, raising=True)

    with pytest.raises(RuntimeError) as exc_info:
        pi_demo.run_duckdb_demo()
    assert "Install it with 'pip install duckdb'" in str(exc_info.value)


def test_build_demo_queries_uses_projection_and_summary() -> None:
    queries = pi_demo.build_demo_queries("r")
    assert "projection" in queries
    assert "summary" in queries
    assert queries["projection"].startswith("SELECT ")
    assert '"r" AS "radius"' in queries["projection"]
    assert 'sum(((3.141592653589793::DOUBLE * "r") * 2))' in queries["summary"]
