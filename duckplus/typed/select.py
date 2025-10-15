"""Helpers for assembling SELECT statements from typed expressions."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from .expressions.base import AliasedExpression, TypedExpression
from .expressions.utils import quote_identifier


class SelectStatementBuilder:
    """Fluent builder for composing SQL SELECT statements."""

    __slots__ = ("_columns", "_from_clause", "_finalised")

    def __init__(self) -> None:
        self._columns: list[str] = []
        self._from_clause: str | None = None
        self._finalised = False

    def column(
        self,
        expression: object,
        *,
        alias: str | None = None,
    ) -> "SelectStatementBuilder":
        """Append a column expression to the SELECT list."""

        self._ensure_mutable()
        expression_sql, default_alias = self._coerce_expression(expression)

        alias_name: str | None
        if alias is not None:
            alias_name = alias.strip()
            if not alias_name:
                msg = "Column alias cannot be empty"
                raise ValueError(msg)
        else:
            alias_name = default_alias

        if alias_name is not None:
            aliased_sql = f"{expression_sql} AS {quote_identifier(alias_name)}"
            self._columns.append(aliased_sql)
        else:
            self._columns.append(expression_sql)

        return self

    def star(
        self,
        *,
        exclude: Iterable[str] | None = None,
        replace: (
            Mapping[str, object]
            | Iterable[tuple[str, object]]
            | Iterable[AliasedExpression]
            | None
        ) = None,
    ) -> "SelectStatementBuilder":
        """Append a ``*`` expression with optional modifiers."""

        self._ensure_mutable()

        clause_parts: list[str] = ["*"]

        replace_clauses = self._normalise_replace_clauses(replace)
        if replace_clauses:
            clause_parts.append(f"REPLACE ({', '.join(replace_clauses)})")

        exclude_clauses = self._normalise_exclude_identifiers(exclude)
        if exclude_clauses:
            clause_parts.append(f"EXCLUDE ({', '.join(exclude_clauses)})")

        self._columns.append(" ".join(clause_parts))
        return self

    def from_(self, source: str) -> "SelectStatementBuilder":
        """Define the FROM clause for the SELECT statement."""

        self._ensure_mutable()
        if self._from_clause is not None:
            msg = "SELECT statement already defines a FROM clause"
            raise ValueError(msg)

        source_sql = source.strip()
        if not source_sql:
            msg = "FROM clause cannot be empty"
            raise ValueError(msg)

        self._from_clause = source_sql
        return self

    def build(self) -> str:
        """Render the accumulated SQL statement."""

        select_list = self.build_select_list()
        sql = f"SELECT {select_list}"
        if self._from_clause is not None:
            sql = f"{sql} FROM {self._from_clause}"
        return sql

    def build_select_list(self) -> str:
        """Render only the SELECT list for use with ``Relation.project``."""

        self._ensure_mutable()
        if not self._columns:
            msg = "SELECT statement requires at least one column"
            raise ValueError(msg)

        self._finalised = True
        return ", ".join(self._columns)

    def _coerce_expression(self, expression: object) -> tuple[str, str | None]:
        if isinstance(expression, AliasedExpression):
            return expression.base.render(), expression.alias_name
        if isinstance(expression, TypedExpression):
            return expression.render(), None
        if isinstance(expression, str):
            sql = expression.strip()
            if not sql:
                msg = "Column expression cannot be empty"
                raise ValueError(msg)
            return sql, None
        msg = "Columns must be SQL strings or typed expressions"
        raise TypeError(msg)

    def _ensure_mutable(self) -> None:
        if self._finalised:
            msg = "SELECT statement has already been built"
            raise RuntimeError(msg)

    def _normalise_replace_clauses(
        self,
        replace: (
            Mapping[str, object]
            | Iterable[tuple[str, object]]
            | Iterable[AliasedExpression]
            | None
        ),
    ) -> list[str]:
        if replace is None:
            return []

        if isinstance(replace, Mapping):
            items_iterable: Iterable[tuple[str | None, object]] = replace.items()
        else:

            def iter_replace() -> Iterable[tuple[str | None, object]]:
                for entry in replace:
                    if isinstance(entry, AliasedExpression):
                        yield None, entry
                    elif isinstance(entry, tuple) and len(entry) == 2:
                        alias_candidate, expression = entry
                        if alias_candidate is None:
                            yield None, expression
                        elif isinstance(alias_candidate, str):
                            yield alias_candidate, expression
                        else:
                            msg = "Replace aliases must be strings"
                            raise TypeError(msg)
                    else:
                        msg = (
                            "Replace clauses must be provided as aliased expressions or "
                            "(alias, expression) pairs"
                        )
                        raise TypeError(msg)

            items_iterable = iter_replace()

        clauses: list[str] = []
        for alias_candidate, expression in items_iterable:
            alias_name: str | None
            if alias_candidate is not None:
                alias_name = alias_candidate.strip()
                if not alias_name:
                    msg = "Replace alias cannot be empty"
                    raise ValueError(msg)
            else:
                alias_name = None

            expression_sql, default_alias = self._coerce_expression(expression)
            final_alias = alias_name or default_alias
            if final_alias is None:
                msg = "Replace expressions must define an alias"
                raise ValueError(msg)

            clauses.append(f"{expression_sql} AS {quote_identifier(final_alias)}")

        return clauses

    def _normalise_exclude_identifiers(
        self, exclude: Iterable[str] | None
    ) -> list[str]:
        if exclude is None:
            return []

        identifiers: list[str] = []
        for identifier in exclude:
            if not isinstance(identifier, str):
                msg = "Exclude targets must be strings"
                raise TypeError(msg)
            name = identifier.strip()
            if not name:
                msg = "Exclude target cannot be empty"
                raise ValueError(msg)
            identifiers.append(quote_identifier(name))

        return identifiers
