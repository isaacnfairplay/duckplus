"""Integration tests ensuring mypy spots invalid typed expression usage."""

from __future__ import annotations

from pathlib import Path

from mypy import api as mypy_api


def test_generic_sum_is_rejected(tmp_path: Path) -> None:
    source = """
from duckplus.static_typed import ducktype

def demo() -> None:
    expr = ducktype.Generic("customer")
    expr.sum()
"""
    path = tmp_path / "invalid_generic_sum.py"
    path.write_text(source, encoding="utf-8")

    stdout, stderr, status = mypy_api.run([str(path)])
    assert status != 0
    assert "error" in stdout.lower()
    assert "has no attribute \"sum\"" in stdout
    assert stderr == ""


def test_ducktype_convenience_module_is_mypy_clean(tmp_path: Path) -> None:
    source = """
from duckplus.static_typed.ducktype import Numeric, select


def demo() -> None:
    builder = select()
    expr = Numeric.literal(1)
    builder = builder.column(expr, alias="value")
    builder.build()
"""
    path = tmp_path / "typed_ducktype_usage.py"
    path.write_text(source, encoding="utf-8")

    stdout, stderr, status = mypy_api.run([str(path)])
    assert status == 0
    assert stdout.strip().startswith("Success: no issues found")
    assert stderr == ""
