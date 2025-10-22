"""Integration tests ensuring ty spots invalid typed expression usage."""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from ._project_metadata import project_version_tuple
from .typecheck_cases import CheckerExpectation, TypeCheckCase, cases_for


TY_CASES = cases_for("ty")


@dataclass
class TyResult:
    name: str
    passed: bool
    details: str | None = None


class TyContractAccumulator:
    def __init__(self) -> None:
        self._results: list[TyResult] = []

    def record(self, name: str, passed: bool, details: str | None) -> None:
        self._results.append(TyResult(name=name, passed=passed, details=details))

    def summary(self) -> tuple[int, list[TyResult]]:
        total = len(self._results)
        failures = [result for result in self._results if not result.passed]
        return total, failures


def _run_ty(args: list[str]) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, "-m", "ty", "check", *args]
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _evaluate_expectation(
    case: TypeCheckCase, expectation: CheckerExpectation, result: subprocess.CompletedProcess[str]
) -> tuple[bool, str | None]:
    output = (result.stdout + result.stderr).strip()
    if expectation.ok:
        if result.returncode == 0:
            return True, None
        message = (
            f"ty should succeed for {case.name}, return code {result.returncode}\n{output}"
        )
        return False, message

    if result.returncode == 0:
        message = f"ty should fail for {case.name} but succeeded\n{output}"
        return False, message

    diagnostic = expectation.normalised_diagnostic()
    if diagnostic and diagnostic not in output.lower():
        message = (
            f"ty diagnostics for {case.name} should mention {diagnostic!r}, got:\n{output}"
        )
        return False, message

    return True, None


@pytest.fixture(scope="session")
def _ty_contract_accumulator() -> TyContractAccumulator:
    return TyContractAccumulator()


@pytest.fixture(scope="session")
def _ty_available() -> None:
    pytest.importorskip("ty")


@pytest.mark.parametrize("case", TY_CASES, ids=lambda case: case.name)
def test_ty_contract_case(
    case: TypeCheckCase,
    tmp_path_factory: pytest.TempPathFactory,
    _ty_contract_accumulator: TyContractAccumulator,
    _ty_available: None,
) -> None:
    temp_dir = tmp_path_factory.mktemp(f"ty-case-{case.name}")
    path = temp_dir / case.filename
    path.write_text(case.rendered_source(), encoding="utf-8")

    expectation = case.expectation_for("ty")
    assert expectation is not None  # pragma: no branch - defended by cases_for

    result = _run_ty([str(path)])
    passed, details = _evaluate_expectation(case, expectation, result)
    _ty_contract_accumulator.record(case.name, passed, details)


def test_ty_contract_threshold(_ty_contract_accumulator: TyContractAccumulator, _ty_available: None) -> None:
    version = project_version_tuple()
    total, failures = _ty_contract_accumulator.summary()

    if total == 0:  # pragma: no cover - defensive guard
        pytest.skip("No ty contract checks defined")

    passed = total - len(failures)
    ratio = passed / total

    if not failures:
        return

    formatted = "; ".join(
        f"{failure.name}: {failure.details}" if failure.details else failure.name
        for failure in failures
    )

    if version < (1, 4, 6):
        pytest.xfail("ty contract is optional before 1.4.6: " + formatted)
        return

    if version < (1, 4, 7):
        if ratio < 0.8:
            pytest.fail(
                "ty contract must be at least 80% satisfied in 1.4.6: " + formatted
            )
        pytest.xfail("ty contract not fully satisfied yet: " + formatted)
        return

    pytest.fail("ty contract failed: " + formatted)
