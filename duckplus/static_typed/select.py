"""Helpers for assembling SELECT statements from typed expressions."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from .dependencies import ExpressionDependency
from .expressions.base import AliasedExpression, TypedExpression
from .expressions.utils import quote_identifier


@dataclass(frozen=True, slots=True)
class _ColumnEntry:
    sql: str
    optional: bool
    required_columns: frozenset[str] | None

    def render(self, available: frozenset[str] | None) -> str | None:
        if not self.optional:
            return self.sql
        required = _require_columns(self.required_columns)
        available_columns = _require_available_columns(available)
        if required.issubset(available_columns):
            return self.sql
        return None


@dataclass(frozen=True, slots=True)
class _ReplaceEntry:
    sql: str
    optional: bool
    required_columns: frozenset[str] | None

    def render(self, available: frozenset[str] | None) -> str | None:
        if not self.optional:
            return self.sql
        required = _require_columns(self.required_columns)
        available_columns = _require_available_columns(available)
        if required.issubset(available_columns):
            return self.sql
        return None


@dataclass(frozen=True, slots=True)
class _ExcludeEntry:
    identifier: str
    sql: str
    optional: bool

    def render(self, available: frozenset[str] | None) -> str | None:
        if not self.optional:
            return self.sql
        available_columns = _require_available_columns(available)
        if self.identifier in available_columns:
            return self.sql
        return None


@dataclass(frozen=True, slots=True)
class _StarEntry:
    replace_entries: tuple[_ReplaceEntry, ...]
    exclude_entries: tuple[_ExcludeEntry, ...]

    def render(self, available: frozenset[str] | None) -> str:
        replace_clauses = []
        for entry in self.replace_entries:
            if (clause := entry.render(available)) is not None:
                replace_clauses.append(clause)

        exclude_clauses = []
        for exclude_entry in self.exclude_entries:
            if (clause := exclude_entry.render(available)) is not None:
                exclude_clauses.append(clause)

        components = ["*"]
        if replace_clauses:
            components.append(f"REPLACE ({', '.join(replace_clauses)})")
        if exclude_clauses:
            components.append(f"EXCLUDE ({', '.join(exclude_clauses)})")
        return " ".join(components)


_SelectEntry = _ColumnEntry | _StarEntry


def _require_available_columns(
    available: frozenset[str] | None,
) -> frozenset[str]:
    if available is None:
        msg = (
            "SelectStatementBuilder.build_select_list requires available_columns "
            "when if_exists clauses are used"
        )
        raise RuntimeError(msg)
    return available


def _require_columns(required: frozenset[str] | None) -> frozenset[str]:
    if required is None:
        msg = "if_exists clauses must provide dependency metadata"
        raise ValueError(msg)
    if not required:
        msg = "if_exists clauses require at least one column dependency"
        raise ValueError(msg)
    return required


def _resolve_required_columns(
    dependencies: frozenset[ExpressionDependency],
) -> frozenset[str]:
    required: set[str] = set()
    for dependency in dependencies:
        column = dependency.column_name
        if column is None:
            msg = (
                "if_exists clauses do not support table-level dependencies; "
                "provide explicit column dependencies"
            )
            raise ValueError(msg)
        if dependency.table_name is not None:
            msg = (
                "if_exists clauses require unqualified column dependencies; "
                "remove table qualifiers"
            )
            raise ValueError(msg)
        required.add(column)
    if not required:
        msg = "if_exists clauses require at least one column dependency"
        raise ValueError(msg)
    return frozenset(required)


class SelectStatementBuilder:
    """Fluent builder for composing SQL SELECT statements."""

    __slots__ = ("_columns", "_from_clause", "_finalised")

    def __init__(self) -> None:
        self._columns: list[_SelectEntry] = []
        self._from_clause: str | None = None
        self._finalised = False

    def column(
        self,
        expression: object,
        *,
        alias: str | None = None,
        if_exists: bool = False,
    ) -> "SelectStatementBuilder":
        """Append a column expression to the SELECT list."""

        self._ensure_mutable()
        expression_sql, default_alias, dependencies = self._coerce_expression(
            expression
        )

        alias_name: str | None
        if alias is not None:
            alias_name = alias.strip()
            if not alias_name:
                msg = "Column alias cannot be empty"
                raise ValueError(msg)
        else:
            alias_name = default_alias

        if dependencies is None and if_exists:
            msg = "if_exists columns require typed expressions with dependencies"
            raise TypeError(msg)

        required_columns: frozenset[str] | None
        if if_exists:
            assert dependencies is not None  # narrow type for mypy
            required_columns = _resolve_required_columns(dependencies)
        else:
            required_columns = None

        if alias_name is not None:
            aliased_sql = f"{expression_sql} AS {quote_identifier(alias_name)}"
            entry_sql = aliased_sql
        else:
            entry_sql = expression_sql

        entry = _ColumnEntry(
            entry_sql,
            optional=if_exists,
            required_columns=required_columns,
        )
        self._columns.append(entry)

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
        exclude_if_exists: Iterable[str] | None = None,
        replace_if_exists: (
            Mapping[str, object]
            | Iterable[tuple[str, object]]
            | Iterable[AliasedExpression]
            | None
        ) = None,
    ) -> "SelectStatementBuilder":
        """Append a ``*`` expression with optional modifiers."""

        self._ensure_mutable()

        replace_entries = []
        replace_entries.extend(
            self._normalise_replace_clauses(replace, optional=False)
        )
        replace_entries.extend(
            self._normalise_replace_clauses(replace_if_exists, optional=True)
        )

        exclude_entries = []
        exclude_entries.extend(
            self._normalise_exclude_identifiers(exclude, optional=False)
        )
        exclude_entries.extend(
            self._normalise_exclude_identifiers(exclude_if_exists, optional=True)
        )

        star_entry = _StarEntry(
            replace_entries=tuple(replace_entries),
            exclude_entries=tuple(exclude_entries),
        )
        self._columns.append(star_entry)
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

    def build(self, *, available_columns: Iterable[str] | None = None) -> str:
        """Render the accumulated SQL statement."""

        select_list = self.build_select_list(
            available_columns=available_columns
        )
        sql = f"SELECT {select_list}"
        if self._from_clause is not None:
            sql = f"{sql} FROM {self._from_clause}"
        return sql

    def build_select_list(
        self, *, available_columns: Iterable[str] | None = None
    ) -> str:
        """Render only the SELECT list for use with ``Relation.project``."""

        self._ensure_mutable()
        if not self._columns:
            msg = "SELECT statement requires at least one column"
            raise ValueError(msg)

        available: frozenset[str] | None
        if available_columns is None:
            available = None
        else:
            available = frozenset(available_columns)

        rendered: list[str] = []
        for entry in self._columns:
            clause = entry.render(available)
            if clause is None:
                continue
            rendered.append(clause)

        if not rendered:
            msg = "SELECT statement requires at least one column"
            raise ValueError(msg)

        self._finalised = True
        return ", ".join(rendered)

    def _coerce_expression(
        self, expression: object
    ) -> tuple[str, str | None, frozenset[ExpressionDependency] | None]:
        if isinstance(expression, AliasedExpression):
            return (
                expression.base.render(),
                expression.alias_name,
                expression.dependencies,
            )
        if isinstance(expression, TypedExpression):
            return expression.render(), None, expression.dependencies
        if isinstance(expression, str):
            sql = expression.strip()
            if not sql:
                msg = "Column expression cannot be empty"
                raise ValueError(msg)
            return sql, None, None
        msg = "Columns must be SQL strings or typed expressions"
        raise TypeError(msg)

    def _ensure_mutable(self) -> None:
        if self._finalised:
            msg = "SELECT statement has already been built"
            raise RuntimeError(msg)

    # pylint: disable=too-many-locals
    def _normalise_replace_clauses(
        self,
        replace: (
            Mapping[str, object]
            | Iterable[tuple[str, object]]
            | Iterable[AliasedExpression]
            | None
        ),
        *,
        optional: bool,
    ) -> list[_ReplaceEntry]:
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

        clauses: list[_ReplaceEntry] = []
        for alias_candidate, expression in items_iterable:
            alias_name: str | None
            if alias_candidate is not None:
                alias_name = alias_candidate.strip()
                if not alias_name:
                    msg = "Replace alias cannot be empty"
                    raise ValueError(msg)
            else:
                alias_name = None

            (
                expression_sql,
                default_alias,
                dependencies,
            ) = self._coerce_expression(expression)
            final_alias = alias_name or default_alias
            if final_alias is None:
                msg = "Replace expressions must define an alias"
                raise ValueError(msg)

            if dependencies is None and optional:
                msg = (
                    "replace_if_exists expressions require typed expressions "
                    "with dependencies"
                )
                raise TypeError(msg)

            required_columns: frozenset[str] | None
            if optional:
                assert dependencies is not None  # narrow type
                required_columns = _resolve_required_columns(dependencies)
            else:
                required_columns = None

            clause_sql = f"{expression_sql} AS {quote_identifier(final_alias)}"
            clauses.append(
                _ReplaceEntry(
                    clause_sql,
                    optional=optional,
                    required_columns=required_columns,
                )
            )

        return clauses

    def _normalise_exclude_identifiers(
        self,
        exclude: Iterable[str] | None,
        *,
        optional: bool,
    ) -> list[_ExcludeEntry]:
        if exclude is None:
            return []

        identifiers: list[_ExcludeEntry] = []
        for identifier in exclude:
            if not isinstance(identifier, str):
                msg = "Exclude targets must be strings"
                raise TypeError(msg)
            name = identifier.strip()
            if not name:
                msg = "Exclude target cannot be empty"
                raise ValueError(msg)
            identifiers.append(
                _ExcludeEntry(
                    identifier=name,
                    sql=quote_identifier(name),
                    optional=optional,
                )
            )

        return identifiers
