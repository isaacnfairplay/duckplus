"""Compatibility checks for the deprecated ``duckplus.typed`` namespace."""

from __future__ import annotations

import importlib
import sys
import warnings

import pytest

from duckplus import ducktype as package_ducktype
from duckplus import static_typed


def test_package_ducktype_defaults_to_static_namespace() -> None:
    """Importing ``ducktype`` from the package root yields the static namespace."""

    assert package_ducktype is static_typed.ducktype


def test_deprecated_module_alias() -> None:
    """Importing ``duckplus.typed`` emits a deprecation warning and aliases static typed."""

    with warnings.catch_warnings():
        warnings.simplefilter("always", DeprecationWarning)
        with pytest.deprecated_call():
            module = importlib.import_module("duckplus.typed")
        with pytest.deprecated_call():
            importlib.reload(module)
    assert module.ducktype is static_typed.ducktype


def test_submodule_aliases_follow_static_namespace() -> None:
    """Submodules resolve to the static typed implementation."""

    dependencies = importlib.import_module("duckplus.typed.dependencies")
    static_dependencies = importlib.import_module("duckplus.static_typed.dependencies")
    assert dependencies is static_dependencies


def test_decimal_factories_exported() -> None:
    """Static typed module exposes decimal factories for backward compat imports."""

    module = sys.modules.get("duckplus.typed")
    if module is None:
        with pytest.deprecated_call():
            module = importlib.import_module("duckplus.typed")
    column = module.Decimal_18_2("amount")
    assert column.duck_type.render() == "DECIMAL(18, 2)"
