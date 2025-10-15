"""Integration tests ensuring mypy spots invalid typed expression usage."""

from __future__ import annotations

from pathlib import Path

from mypy import api as mypy_api


def test_generic_sum_is_rejected(tmp_path: Path) -> None:
    source = """
from duckplus.typed import ducktype

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
