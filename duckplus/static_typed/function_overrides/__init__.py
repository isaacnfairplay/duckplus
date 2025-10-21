"""Import-time overrides for generated function namespaces."""

from __future__ import annotations

from importlib import import_module

_SIDE_EFFECT_MODULES: tuple[str, ...] = (
    "duckplus.static_typed.function_overrides.scalar_string",
    "duckplus.static_typed.function_overrides.scalar_generic",
    "duckplus.static_typed.function_overrides.scalar_system",
    "duckplus.static_typed.function_overrides.scalar_postgres_privilege",
    "duckplus.static_typed.function_overrides.scalar_postgres_visibility",
)

for module_name in _SIDE_EFFECT_MODULES:
    import_module(module_name)


__all__ = ["_SIDE_EFFECT_MODULES"]
