"""Contract tests for unsigned integer types and factories."""

from __future__ import annotations

from importlib import import_module
from typing import Callable, Iterable

import pytest

from ._project_metadata import project_version_tuple

Check = Callable[[], None]


def _unsigned_type_exports() -> None:
    module = import_module("duckplus.static_typed.types")
    integer_type = getattr(module, "IntegerType")
    expected: dict[str, str] = {
        "UtinyintType": "UTINYINT",
        "UsmallintType": "USMALLINT",
        "UintegerType": "UINTEGER",
    }
    missing: list[str] = []
    wrong_category: list[str] = []
    for name, render in expected.items():
        attr = getattr(module, name, None)
        if attr is None:
            missing.append(name)
            continue
        if not isinstance(attr, type):
            missing.append(name)
            continue
        if not issubclass(attr, integer_type):
            wrong_category.append(name)
            continue
        instance = attr()
        if instance.render() != render:
            wrong_category.append(name)
    if missing:
        raise AssertionError(f"Missing unsigned integer types: {', '.join(sorted(missing))}")
    if wrong_category:
        raise AssertionError(
            "Unsigned integer types must inherit from IntegerType and render correctly: "
            + ", ".join(sorted(wrong_category))
        )


def _factory_cast_supports_unsigned_numeric() -> None:
    module = import_module("duckplus.static_typed")
    varchar = getattr(module, "Varchar")
    uinteger = getattr(module, "Uinteger")
    expression = varchar.literal("123")
    cast_expression = expression.try_cast(uinteger)
    duck_type = cast_expression.duck_type
    if duck_type.render() != "UINTEGER":
        raise AssertionError("Casting via factory should produce a UINTEGER duck type")


def _run_contract(checks: Iterable[tuple[str, Check]]) -> tuple[int, list[str]]:
    failures: list[str] = []
    total = 0
    for name, check in checks:
        total += 1
        try:
            check()
        except Exception as exc:  # pragma: no cover - exercised in future versions
            failures.append(f"{name}: {exc}")
    return total, failures


@pytest.mark.parametrize("_sentinel", [None])
def test_unsigned_integer_types_contract(_sentinel: None) -> None:
    checks = (
        ("unsigned-type-exports", _unsigned_type_exports),
        ("factory-cast-support", _factory_cast_supports_unsigned_numeric),
    )
    total, failures = _run_contract(checks)
    version = project_version_tuple()

    if total == 0:  # pragma: no cover - defensive guard
        pytest.skip("No unsigned integer contract checks defined")

    passed = total - len(failures)
    ratio = passed / total

    if version < (1, 4, 6):
        if failures:
            pytest.xfail(
                "Unsigned integer types contract is optional before 1.4.6: "
                + "; ".join(failures)
            )
        return

    if version < (1, 4, 7):
        if ratio < 0.8:
            pytest.fail(
                "Unsigned integer types contract must be at least 80% satisfied in 1.4.6: "
                + "; ".join(failures)
            )
        if failures:
            pytest.xfail(
                "Unsigned integer types contract not fully satisfied yet: "
                + "; ".join(failures)
            )
        return

    if failures:
        pytest.fail("Unsigned integer types contract failed: " + "; ".join(failures))
