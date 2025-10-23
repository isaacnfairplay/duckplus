"""Typed expression base classes and metaclass support."""

from __future__ import annotations

from typing import Any, Callable, Mapping


class ExprMeta(type):
    """Metaclass that attaches helper methods based on a specification."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> "ExprMeta":
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        spec: Mapping[str, Mapping[str, Any]] | None = getattr(cls, "__duckplus_spec__", None)
        if spec:
            for method_name, method_spec in spec.items():
                if hasattr(cls, method_name):
                    continue

                def make_method(spec_map: Mapping[str, Any]) -> Callable[..., Any]:
                    def _method(
                        self: "TypedExpression", *args: Any, **kwargs: Any
                    ) -> "TypedExpression":
                        return self.invoke(spec_map["name"], *args, **kwargs)

                    _method.__name__ = str(spec_map["name"])
                    _method.__doc__ = spec_map.get("doc")
                    return _method

                runtime_spec = dict(method_spec)
                runtime_spec.setdefault("name", method_name)
                setattr(cls, method_name, make_method(runtime_spec))
        return cls


class TypedExpression(metaclass=ExprMeta):
    """Base class for all typed expressions."""

    __slots__ = ("_expression",)
    __duckplus_spec__: Mapping[str, Mapping[str, Any]] | None = None

    def __init__(self, expression: str) -> None:
        self._expression = expression

    def invoke(self, name: str, *args: Any, **kwargs: Any) -> "TypedExpression":
        """Invoke a runtime helper defined by the backend implementation."""
        return self._invoke(name, *args, **kwargs)

    def _invoke(self, name: str, *args: Any, **kwargs: Any) -> "TypedExpression":
        raise NotImplementedError("Expression invocation is backend specific")

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"{self.__class__.__name__}({self._expression!r})"
