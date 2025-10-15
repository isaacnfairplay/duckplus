"""Builder helpers for composing CASE expressions."""

from __future__ import annotations

from typing import Callable, Generic, TypeVar, cast

from ..dependencies import ExpressionDependency
from .base import AliasedExpression, BooleanExpression, TypedExpression

ResultExpressionT = TypeVar("ResultExpressionT", bound=TypedExpression)


class CaseExpressionBuilder(Generic[ResultExpressionT]):
    """Fluent builder that assembles typed CASE expressions."""

    __slots__ = (
        "_result_coercer",
        "_condition_coercer",
        "_when_clauses",
        "_else_clause",
        "_finalised",
    )

    def __init__(
        self,
        *,
        result_coercer: Callable[[object], ResultExpressionT],
        condition_coercer: Callable[[object], BooleanExpression],
    ) -> None:
        self._result_coercer = result_coercer
        self._condition_coercer = condition_coercer
        self._when_clauses: list[tuple[BooleanExpression, ResultExpressionT]] = []
        self._else_clause: ResultExpressionT | None = None
        self._finalised = False

    def when(self, condition: object, result: object) -> "CaseExpressionBuilder[ResultExpressionT]":
        """Append a WHEN clause to the CASE expression."""

        self._ensure_mutable()
        condition_expression = self._condition_coercer(condition)
        result_expression = self._result_coercer(result)
        self._when_clauses.append((condition_expression, result_expression))
        return self

    def else_(self, result: object) -> "CaseExpressionBuilder[ResultExpressionT]":
        """Define the optional ELSE branch for the CASE expression."""

        self._ensure_mutable()
        if self._else_clause is not None:
            msg = "CASE expression already defines an ELSE clause"
            raise ValueError(msg)
        self._else_clause = self._result_coercer(result)
        return self

    def end(self) -> ResultExpressionT:
        """Finalize the CASE expression and return the typed result."""

        self._ensure_mutable()
        if not self._when_clauses:
            msg = "CASE expressions require at least one WHEN clause"
            raise ValueError(msg)

        self._finalised = True

        parts = ["CASE"]
        dependencies: set[ExpressionDependency] = set()

        for condition_expression, result_expression in self._when_clauses:
            parts.append(
                f"WHEN {condition_expression.render()} THEN {result_expression.render()}"
            )
            dependencies.update(condition_expression.dependencies)
            dependencies.update(result_expression.dependencies)

        if self._else_clause is not None:
            parts.append(f"ELSE {self._else_clause.render()}")
            dependencies.update(self._else_clause.dependencies)

        parts.append("END")
        sql = " ".join(parts)

        template = self._select_template_expression()
        cloned = template.clone_with_sql(sql, dependencies=frozenset(dependencies))
        return cast(ResultExpressionT, cloned)

    def _select_template_expression(self) -> ResultExpressionT:
        template = self._when_clauses[0][1]
        if isinstance(template, AliasedExpression):
            return cast(ResultExpressionT, template.base)
        return template

    def _ensure_mutable(self) -> None:
        if self._finalised:
            msg = "CASE expression has already been finalised"
            raise RuntimeError(msg)
