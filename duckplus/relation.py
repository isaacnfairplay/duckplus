"""Utilities for working with DuckDB relations."""

# pylint: disable=import-error

from __future__ import annotations

from dataclasses import dataclass, field
import warnings
from typing import Iterable, Mapping, TypeVar, overload

import duckdb  # type: ignore[import-not-found]

from .duckcon import DuckCon
from .typed.select import SelectStatementBuilder
from .typed.dependencies import ExpressionDependency
from .typed.expressions.base import AliasedExpression, BooleanExpression, TypedExpression
from .typed.types import BooleanType


T = TypeVar("T")


@dataclass(frozen=True)
class Relation:
    """Immutable wrapper around a DuckDB relation.

    The wrapper keeps track of the :class:`~duckplus.duckcon.DuckCon` that
    produced the relation together with cached metadata describing the
    relation's column names and DuckDB data types.
    """

    duckcon: DuckCon
    _relation: duckdb.DuckDBPyRelation
    _columns: tuple[str, ...] = field(init=False, repr=False)
    _types: tuple[str, ...] = field(init=False, repr=False)
    _casefolded_columns: dict[str, tuple[str, ...]] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        columns = tuple(self._relation.columns)
        # DuckDB returns custom type objects in ``relation.types`` so we cast
        # them to their string representation for stable comparison.
        types = tuple(str(type_) for type_ in self._relation.types)
        casefolded: dict[str, list[str]] = {}
        for column in columns:
            key = column.casefold()
            casefolded.setdefault(key, []).append(column)
        object.__setattr__(self, "_columns", columns)
        object.__setattr__(self, "_types", types)
        object.__setattr__(self, "_casefolded_columns", {
            key: tuple(values) for key, values in casefolded.items()
        })

    @property
    def columns(self) -> tuple[str, ...]:
        """Return the column names of the wrapped relation."""

        return self._columns

    @property
    def types(self) -> tuple[str, ...]:
        """Return the DuckDB data types associated with the relation."""

        return self._types

    @property
    def relation(self) -> duckdb.DuckDBPyRelation:
        """Expose the underlying DuckDB relation."""

        return self._relation

    @classmethod
    def from_relation(cls, duckcon: DuckCon, relation: duckdb.DuckDBPyRelation) -> "Relation":
        """Create a :class:`Relation` from an existing DuckDB relation."""

        return cls(duckcon=duckcon, _relation=relation)

    @classmethod
    def from_sql(cls, duckcon: DuckCon, query: str) -> "Relation":
        """Create a relation from a SQL query executed on a managed connection."""

        connection = duckcon.connection
        relation = connection.sql(query)
        return cls.from_relation(duckcon, relation)

    @overload
    def transform(self, **replacements: str) -> "Relation":
        ...

    @overload
    def transform(
        self,
        **replacements: type[int] | type[float] | type[str] | type[bool] | type[bytes],
    ) -> "Relation":
        ...

    def transform(self, **replacements: object) -> "Relation":
        """Return a new relation with selected columns replaced.

        The helper issues a ``SELECT * REPLACE`` statement against the underlying
        DuckDB relation and validates that referenced columns exist. Replacement
        expressions can be provided directly as SQL snippets or as simple Python
        types (``int``, ``float``, ``str``, ``bool``, ``bytes``) which will be
        translated into DuckDB casts, e.g. ``relation.transform(total=int)``.
        """

        if not replacements:
            msg = "transform requires at least one replacement"
            raise ValueError(msg)

        if not self.duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call transform. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        resolved_items, missing = self._resolve_column_items(replacements.items())
        if missing:
            formatted = self._format_column_list(missing)
            msg = f"Columns do not exist on relation: {formatted}"
            raise KeyError(msg)

        replace_clauses = []
        for column, value in resolved_items:
            expression = self._normalise_transform_value(column, value)
            alias = self._quote_identifier(column)
            replace_clauses.append(f"{expression} AS {alias}")

        select_list = f"* REPLACE ({', '.join(replace_clauses)})"

        try:
            relation = self._relation.project(select_list)
        except duckdb.BinderException as error:
            msg = "transform expression references unknown columns"
            raise ValueError(msg) from error

        return type(self).from_relation(self.duckcon, relation)

    @overload
    def add(self, **expressions: str) -> "Relation":  # pylint: disable=too-many-locals
        ...

    @overload
    def add(self, **expressions: TypedExpression) -> "Relation":
        ...

    def add(self, **expressions: object) -> "Relation":  # pylint: disable=too-many-locals
        """Return a new relation with additional computed columns.

        Expressions can be provided either as raw SQL strings or as typed
        expressions from :mod:`duckplus.typed`. Typed expressions carry
        dependency metadata, allowing the helper to validate that references
        only target columns present on the original relation.
        """

        if not expressions:
            msg = "add requires at least one expression"
            raise ValueError(msg)

        if not self.duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call add. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        existing_matches = {
            column
            for alias in expressions
            for column in self._casefolded_columns.get(alias.casefold(), ())
        }
        if existing_matches:
            formatted = self._format_column_list(existing_matches)
            msg = f"Columns already exist on relation: {formatted}"
            raise ValueError(msg)

        seen_aliases: set[str] = set()
        prepared: list[tuple[str, str]] = []
        for alias, expression in expressions.items():
            if not isinstance(alias, str):
                msg = "Column names must be strings"
                raise TypeError(msg)

            if not alias.strip():
                msg = "Column name for new column cannot be empty"
                raise ValueError(msg)

            alias_key = alias.casefold()
            if alias_key in seen_aliases:
                msg = f"Column '{alias}' specified multiple times"
                raise ValueError(msg)
            seen_aliases.add(alias_key)

            expression_sql, dependencies = self._normalise_add_expression(
                alias, expression
            )
            if dependencies is not None:
                self._assert_add_dependencies(alias, dependencies)

            validation_builder = SelectStatementBuilder().star()
            validation_builder.column(expression_sql, alias=alias)

            try:
                self._relation.project(validation_builder.build_select_list())
            except duckdb.BinderException as error:
                msg = (
                    "add expression for column "
                    f"'{alias}' references unknown columns"
                )
                raise ValueError(msg) from error

            prepared.append((alias, expression_sql))

        builder = SelectStatementBuilder().star()
        for alias, expression_sql in prepared:
            builder.column(expression_sql, alias=alias)

        select_list = builder.build_select_list()

        try:
            relation = self._relation.project(select_list)
        except duckdb.BinderException as error:
            msg = "add expressions reference unknown columns"
            raise ValueError(msg) from error

        return type(self).from_relation(self.duckcon, relation)

    def aggregate(  # pylint: disable=too-many-locals,keyword-arg-before-vararg
        self,
        group_by: Iterable[str] | str | None = None,
        *filters: object,
        **aggregations: object,
    ) -> "Relation":
        """Return a grouped relation with computed aggregate columns."""

        if not aggregations:
            msg = "aggregate requires at least one aggregation expression"
            raise ValueError(msg)

        resolved_group_by = self._normalise_group_by(group_by)
        filter_clauses = self._normalise_filter_clauses(filters)

        if not self.duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call aggregate. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        working_relation = self._relation
        for clause_sql, dependencies, label in filter_clauses:
            if dependencies is not None:
                self._assert_dependencies_exist(
                    dependencies,
                    error_prefix=label,
                )
            working_relation = working_relation.filter(clause_sql)

        seen_aliases: set[str] = set()
        prepared: list[str] = []
        for alias, expression in aggregations.items():
            name = alias.strip()
            if not name:
                msg = "Aggregation name cannot be empty"
                raise ValueError(msg)

            alias_key = name.casefold()
            if alias_key in seen_aliases:
                msg = f"Aggregation '{alias}' specified multiple times"
                raise ValueError(msg)
            seen_aliases.add(alias_key)

            expression_sql, dependencies = self._normalise_aggregate_expression(
                name, expression
            )
            if dependencies is not None:
                self._assert_dependencies_exist(
                    dependencies,
                    error_prefix=f"aggregate expression for column '{name}'",
                )

            quoted_alias = self._quote_identifier(name)
            prepared.append(f"{expression_sql} AS {quoted_alias}")

        select_entries = [
            self._quote_identifier(column) for column in resolved_group_by
        ]
        select_entries.extend(prepared)
        aggregate_sql = ", ".join(select_entries)
        group_clause = ", ".join(resolved_group_by)

        try:
            relation = working_relation.aggregate(aggregate_sql, group_clause)
        except duckdb.BinderException as error:
            msg = "aggregate expressions reference unknown columns"
            raise ValueError(msg) from error

        return type(self).from_relation(self.duckcon, relation)

    def rename(self, **renames: str) -> "Relation":
        """Return a new relation with selected columns renamed."""

        if not renames:
            msg = "rename requires at least one column to rename"
            raise ValueError(msg)

        return self._rename(renames, skip_missing=False)

    def rename_if_exists(self, **renames: str) -> "Relation":
        """Return a new relation renaming columns and skipping missing ones."""

        if not renames:
            return self

        return self._rename(renames, skip_missing=True)

    def keep(self, *columns: str) -> "Relation":
        """Return a new relation containing only the requested columns."""

        if not columns:
            msg = "keep requires at least one column to retain"
            raise ValueError(msg)

        resolved = self._resolve_subset(columns, skip_missing=False, operation="keep")

        if not self.duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call keep. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        select_list = ", ".join(self._quote_identifier(column) for column in resolved)
        relation = self._relation.project(select_list)
        return type(self).from_relation(self.duckcon, relation)

    def keep_if_exists(self, *columns: str) -> "Relation":
        """Return a new relation keeping available columns and skipping missing ones."""

        if not columns:
            return self

        resolved = self._resolve_subset(
            columns,
            skip_missing=True,
            operation="keep_if_exists",
        )
        if not resolved:
            return self

        if not self.duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call keep_if_exists. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        select_list = ", ".join(self._quote_identifier(column) for column in resolved)
        relation = self._relation.project(select_list)
        return type(self).from_relation(self.duckcon, relation)

    def drop(self, *columns: str) -> "Relation":
        """Return a new relation without the specified columns."""

        if not columns:
            msg = "drop requires at least one column to remove"
            raise ValueError(msg)

        resolved = self._resolve_subset(columns, skip_missing=False, operation="drop")

        if not self.duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call drop. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        builder = SelectStatementBuilder().star(exclude=resolved)
        select_list = builder.build_select_list()
        relation = self._relation.project(select_list)
        return type(self).from_relation(self.duckcon, relation)

    def drop_if_exists(self, *columns: str) -> "Relation":
        """Return a new relation dropping available columns and skipping missing ones."""

        if not columns:
            return self

        resolved = self._resolve_subset(
            columns,
            skip_missing=True,
            operation="drop_if_exists",
        )
        if not resolved:
            return self

        if not self.duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call drop_if_exists. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        builder = SelectStatementBuilder().star(exclude=resolved)
        select_list = builder.build_select_list()
        relation = self._relation.project(select_list)
        return type(self).from_relation(self.duckcon, relation)

    def _rename(self, renames: Mapping[str, str], *, skip_missing: bool) -> "Relation":
        validated = self._prepare_renames(renames, skip_missing=skip_missing)
        if not validated:
            return self

        if not self.duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call rename helpers. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        self._assert_no_conflicts(validated)

        builder = SelectStatementBuilder()
        for column in self.columns:
            quoted = self._quote_identifier(column)
            if column in validated:
                builder.column(quoted, alias=validated[column])
            else:
                builder.column(quoted)
        select_list = builder.build_select_list()
        relation = self._relation.project(select_list)
        return type(self).from_relation(self.duckcon, relation)

    def _prepare_renames(
        self, renames: Mapping[str, str], *, skip_missing: bool
    ) -> dict[str, str]:
        resolved_items, missing = self._resolve_column_items(renames.items())
        if missing:
            formatted = self._format_column_list(missing)
            if skip_missing:
                warnings.warn(
                    "Columns do not exist on relation and were skipped: " + formatted,
                    stacklevel=2,
                )
            else:
                msg = f"Columns do not exist on relation: {formatted}"
                raise KeyError(msg)

        validated: dict[str, str] = {}
        for column, new_name in resolved_items:
            if not isinstance(new_name, str):
                msg = (
                    "rename targets must be strings representing the new column name "
                    f"(got {type(new_name)!r} for column '{column}')"
                )
                raise TypeError(msg)

            if not new_name.strip():
                msg = f"New column name for '{column}' cannot be empty"
                raise ValueError(msg)

            validated[column] = new_name

        return validated

    def _assert_no_conflicts(self, renames: Mapping[str, str]) -> None:
        final_names = [renames.get(column, column) for column in self.columns]

        seen: dict[str, str] = {}
        duplicates: set[str] = set()
        for name in final_names:
            key = name.casefold()
            if key in seen:
                duplicates.add(seen[key])
                duplicates.add(name)
            else:
                seen[key] = name

        if duplicates:
            formatted = self._format_column_list(duplicates)
            msg = f"Renaming results in duplicate column names: {formatted}"
            raise ValueError(msg)

    def _resolve_subset(
        self,
        columns: tuple[str, ...],
        *,
        skip_missing: bool,
        operation: str,
    ) -> list[str]:
        entries: list[tuple[str, None]] = []
        for column in columns:
            if not isinstance(column, str):
                msg = f"{operation} column names must be strings"
                raise TypeError(msg)
            if not column.strip():
                msg = f"Column name for {operation} cannot be empty"
                raise ValueError(msg)
            entries.append((column, None))

        resolved_items, missing = self._resolve_column_items(entries)
        resolved = [column for column, _ in resolved_items]

        if missing:
            formatted = self._format_column_list(missing)
            if skip_missing:
                warnings.warn(
                    "Columns do not exist on relation and were skipped: " + formatted,
                    stacklevel=2,
                )
            else:
                msg = f"Columns do not exist on relation: {formatted}"
                raise KeyError(msg)

        return resolved

    def _normalise_group_by(
        self, group_by: Iterable[str] | str | None
    ) -> list[str]:
        if group_by is None:
            return []
        columns: tuple[str, ...]
        if isinstance(group_by, str):
            columns = (group_by,)
        else:
            columns = tuple(group_by)
        return self._resolve_subset(
            columns,
            skip_missing=False,
            operation="aggregate group_by",
        )

    def _normalise_filter_clauses(
        self, filters: tuple[object, ...]
    ) -> list[tuple[str, frozenset[ExpressionDependency] | None, str]]:
        clauses: list[tuple[str, frozenset[ExpressionDependency] | None, str]] = []
        for index, condition in enumerate(filters, start=1):
            label = f"aggregate filter condition {index}"
            if isinstance(condition, str):
                sql = condition.strip()
                if not sql:
                    msg = f"{label} cannot be empty"
                    raise ValueError(msg)
                clauses.append((sql, None, label))
                continue

            if isinstance(condition, TypedExpression):
                boolean_expression = self._unwrap_boolean_expression(condition)
                clauses.append(
                    (
                        boolean_expression.render(),
                        boolean_expression.dependencies,
                        label,
                    )
                )
                continue

            msg = (
                "Aggregate filters must be SQL strings or boolean typed expressions"
            )
            raise TypeError(msg)

        return clauses

    def _resolve_column(self, column: str) -> str | None:
        matches = self._casefolded_columns.get(column.casefold())
        if matches is None:
            return None
        if len(matches) > 1:
            formatted = self._format_column_list(matches)
            msg = (
                f"Column reference '{column}' is ambiguous; multiple columns match ignoring "
                f"case: {formatted}"
            )
            raise ValueError(msg)
        return matches[0]

    def _resolve_column_items(
        self, items: Iterable[tuple[str, T]]
    ) -> tuple[list[tuple[str, T]], list[str]]:
        resolved: list[tuple[str, T]] = []
        missing: list[str] = []
        seen: set[str] = set()

        for column, payload in items:
            resolved_name = self._resolve_column(column)
            if resolved_name is None:
                missing.append(column)
                continue

            if resolved_name in seen:
                msg = f"Column '{resolved_name}' referenced multiple times"
                raise ValueError(msg)
            seen.add(resolved_name)
            resolved.append((resolved_name, payload))

        return resolved, missing

    @staticmethod
    def _format_column_list(columns: Iterable[str]) -> str:
        unique = sorted(set(columns), key=str.casefold)
        return ", ".join(unique)

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        escaped = identifier.replace("\"", "\"\"")
        return f'"{escaped}"'

    @classmethod
    def _normalise_transform_value(cls, column: str, value: object) -> str:
        if isinstance(value, str):
            expression = value.strip()
            if not expression:
                msg = f"Replacement for column '{column}' cannot be empty"
                raise ValueError(msg)
            return expression

        if isinstance(value, type):
            duck_type = cls._python_type_to_duckdb(value)
            identifier = cls._quote_identifier(column)
            return f"{identifier}::{duck_type}"

        msg = (
            "transform replacements must be SQL strings or simple Python types "
            f"(got {type(value)!r})"
        )
        raise TypeError(msg)

    @staticmethod
    def _python_type_to_duckdb(python_type: type[object]) -> str:
        mapping: Mapping[type[object], str]
        mapping = {
            int: "INTEGER",
            float: "DOUBLE",
            str: "VARCHAR",
            bool: "BOOLEAN",
            bytes: "BLOB",
        }

        try:
            return mapping[python_type]
        except KeyError as error:
            msg = f"Unsupported cast target for transform: {python_type!r}"
            raise TypeError(msg) from error

    def _normalise_add_expression(
        self, alias: str, expression: object
    ) -> tuple[str, frozenset[ExpressionDependency] | None]:
        if isinstance(expression, str):
            expression_sql = expression.strip()
            if not expression_sql:
                msg = f"Expression for column '{alias}' cannot be empty"
                raise ValueError(msg)
            return expression_sql, None

        if isinstance(expression, TypedExpression):
            typed_expression = self._unwrap_expression_for_alias(
                alias,
                expression,
                context=(
                    "Aliased expressions passed to add must use the same alias as "
                    "the target column"
                ),
            )
            return typed_expression.render(), typed_expression.dependencies

        msg = (
            "add expressions must be SQL strings or typed expressions representing the new "
            f"column definition (got {type(expression)!r})"
        )
        raise TypeError(msg)

    def _normalise_aggregate_expression(
        self, alias: str, expression: object
    ) -> tuple[str, frozenset[ExpressionDependency] | None]:
        if isinstance(expression, str):
            expression_sql = expression.strip()
            if not expression_sql:
                msg = f"Expression for aggregation '{alias}' cannot be empty"
                raise ValueError(msg)
            return expression_sql, None

        if isinstance(expression, TypedExpression):
            typed_expression = self._unwrap_expression_for_alias(
                alias,
                expression,
                context=(
                    "Aliased expressions passed to aggregate must use the same alias as "
                    "the target column"
                ),
            )
            return typed_expression.render(), typed_expression.dependencies

        msg = (
            "aggregate expressions must be SQL strings or typed expressions representing the "
            f"aggregation (got {type(expression)!r})"
        )
        raise TypeError(msg)

    @staticmethod
    def _unwrap_expression_for_alias(
        alias: str,
        expression: TypedExpression,
        *,
        context: str,
    ) -> TypedExpression:
        current = expression
        while isinstance(current, AliasedExpression):
            alias_name = current.alias_name
            if alias_name.casefold() != alias.casefold():
                msg = (
                    f"{context} ('{alias_name}' vs '{alias}')"
                )
                raise ValueError(msg)
            current = current.base
        return current

    def _unwrap_boolean_expression(
        self, expression: TypedExpression
    ) -> BooleanExpression:
        current = expression
        while isinstance(current, AliasedExpression):
            current = current.base
        if not isinstance(current, BooleanExpression) and not isinstance(
            current.duck_type, BooleanType
        ):
            msg = "Aggregate filters must be boolean expressions"
            raise TypeError(msg)
        if isinstance(current, BooleanExpression):
            return current
        return BooleanExpression(
            current.render(),
            dependencies=current.dependencies,
        )

    def _assert_add_dependencies(
        self,
        alias: str,
        dependencies: frozenset[ExpressionDependency],
    ) -> None:
        self._assert_dependencies_exist(
            dependencies,
            error_prefix=f"add expression for column '{alias}'",
        )

    def _assert_dependencies_exist(
        self,
        dependencies: frozenset[ExpressionDependency],
        *,
        error_prefix: str,
    ) -> None:
        for dependency in dependencies:
            column = dependency.column_name
            if column is None:
                continue
            try:
                resolved = self._resolve_column(column)
            except ValueError as error:  # pragma: no cover - defensive
                msg = (
                    f"{error_prefix} references ambiguous column '{column}'"
                )
                raise ValueError(msg) from error
            if resolved is None:
                msg = f"{error_prefix} references unknown columns"
                raise ValueError(msg)
