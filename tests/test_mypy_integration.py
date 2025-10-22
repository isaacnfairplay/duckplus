"""Integration tests ensuring mypy spots invalid typed expression usage."""

from __future__ import annotations

from pathlib import Path

import pytest
from mypy import api as mypy_api

from .typecheck_cases import CheckerExpectation, TypeCheckCase, cases_for


MYPY_CASES = cases_for("mypy")


def _evaluate_mypy(case: TypeCheckCase, expectation: CheckerExpectation, stdout: str, stderr: str, status: int) -> None:
    output = (stdout + stderr).lower()
    if expectation.ok:
        assert status == 0, f"mypy should succeed for {case.name}: {stdout}{stderr}"
        assert stdout.strip().startswith("Success: no issues found")
        assert stderr == ""
        return

    assert status != 0, f"mypy should fail for {case.name}"  # pragma: no branch - enforced below
    diagnostic = expectation.normalised_diagnostic()
    if diagnostic:
        assert diagnostic in output, f"expected {diagnostic!r} in mypy output for {case.name}"


@pytest.mark.parametrize("case", MYPY_CASES, ids=lambda case: case.name)
def test_mypy_contract(tmp_path: Path, case: TypeCheckCase) -> None:
    path = tmp_path / case.filename
    path.write_text(case.rendered_source(), encoding="utf-8")

    expectation = case.expectation_for("mypy")
    assert expectation is not None  # pragma: no branch - defended by cases_for

    stdout, stderr, status = mypy_api.run([str(path)])
    _evaluate_mypy(case, expectation, stdout, stderr, status)
