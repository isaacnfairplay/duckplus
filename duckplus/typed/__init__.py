"""Compatibility wrapper redirecting to :mod:`duckplus.static_typed`."""

from __future__ import annotations

import importlib
import sys
import warnings
from types import ModuleType
from typing import Any

from duckplus import static_typed as _static_typed

warnings.warn(
    "'duckplus.typed' is deprecated; import from 'duckplus.static_typed' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = list(_static_typed.__all__)
__path__ = _static_typed.__path__  # type: ignore[attr-defined]


def __getattr__(name: str) -> Any:
    """Proxy attribute lookups to :mod:`duckplus.static_typed`."""

    if hasattr(_static_typed, name):
        value = getattr(_static_typed, name)
        globals()[name] = value
        return value

    module = importlib.import_module(f"duckplus.static_typed.{name}")
    sys.modules[f"{__name__}.{name}"] = module
    return module


def __dir__() -> list[str]:
    """Expose static typed exports to dir() callers."""

    return sorted(__all__)


def _alias_module(name: str, module: ModuleType) -> None:
    """Register ``module`` under the deprecated package name."""

    qualified = f"{__name__}.{name}"
    sys.modules.setdefault(qualified, module)


for _submodule_name in [
    "ducktype",
    "expression",
    "expressions",
    "dependencies",
    "functions",
    "select",
    "types",
    "_generated_function_namespaces",
]:
    try:
        _module = importlib.import_module(f"duckplus.static_typed.{_submodule_name}")
    except ModuleNotFoundError:
        continue
    _alias_module(_submodule_name, _module)


for _export_name in __all__:
    globals()[_export_name] = getattr(_static_typed, _export_name)


del _alias_module, _module, _submodule_name, _export_name
