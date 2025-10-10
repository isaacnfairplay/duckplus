"""Immutable relational wrapper for Duck+."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

import duckdb

from . import util
from .materialize import (
    ArrowMaterializeStrategy,
    MaterializeStrategy,
    Materialized,
)


def _quote_identifier(identifier: str) -> str:
    """Return *identifier* quoted for SQL usage."""

    escaped = identifier.replace("\"", "\"\"")
    return f'"{escaped}"'


def _qualify(alias: str, column: str) -> str:
    """Return a qualified column reference."""

    return f"{alias}.{_quote_identifier(column)}"


def _alias(expression: str, alias: str) -> str:
    """Return a SQL alias expression."""

    return f"{expression} AS {_quote_identifier(alias)}"


def _relation_types(relation: duckdb.DuckDBPyRelation) -> list[str]:
    """Return the DuckDB type names for *relation* columns."""

    return [str(type_name) for type_name in relation.types]


def _format_projection(columns: Sequence[str], *, alias: str | None = None) -> list[str]:
    """Return projection expressions for *columns* optionally qualified."""

    qualifier = alias or ""
    expressions: list[str] = []
    for column in columns:
        source = _quote_identifier(column) if not qualifier else _qualify(qualifier, column)
        expressions.append(_alias(source, column))
    return expressions


def _format_join_condition(pairs: Sequence[tuple[str, str]], *, left_alias: str, right_alias: str) -> str:
    """Return the join condition for the provided column *pairs*."""

    comparisons = [
        f"{_qualify(left_alias, left)} = {_qualify(right_alias, right)}" for left, right in pairs
    ]
    return " AND ".join(comparisons)


def _format_value(value: Any) -> str:
    """Render *value* as a SQL literal."""

    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, bytes):
        return "X'" + value.hex() + "'"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    from datetime import date, datetime, time, timedelta
    from decimal import Decimal

    if isinstance(value, (date, datetime, time)):
        return f"'{value.isoformat()}'"
    if isinstance(value, timedelta):
        total_seconds = value.total_seconds()
        return repr(total_seconds)
    if isinstance(value, Decimal):
        return format(value, "f")

    raise TypeError(f"Unsupported filter parameter type: {type(value)!r}")


def _inject_parameters(expression: str, parameters: Sequence[Any]) -> str:
    """Return *expression* with positional ``?`` placeholders replaced."""

    parts = expression.split("?")
    placeholder_count = len(parts) - 1
    if placeholder_count == 0:
        if parameters:
            raise ValueError("Number of parameters does not match placeholders")
        return expression

    if placeholder_count != len(parameters):
        raise ValueError("Number of parameters does not match placeholders")

    result: list[str] = []
    for index, segment in enumerate(parts[:-1]):
        result.append(segment)
        value = _format_value(parameters[index])
        result.append(value)
    result.append(parts[-1])
    return "".join(result)


class DuckRel:
    """Immutable wrapper around :class:`duckdb.DuckDBPyRelation`."""

    __slots__ = ("_relation", "_columns", "_lookup", "_types")
    _relation: duckdb.DuckDBPyRelation
    _columns: tuple[str, ...]
    _lookup: dict[str, int]
    _types: tuple[str, ...]

    def __setattr__(self, name: str, value: Any) -> None:  # pragma: no cover - defensive
        if name in self.__slots__ and hasattr(self, name):
            raise AttributeError("DuckRel is immutable")
        super().__setattr__(name, value)

    def __init__(
        self,
        relation: duckdb.DuckDBPyRelation,
        *,
        columns: Sequence[str] | None = None,
        types: Sequence[str] | None = None,
    ) -> None:
        super().__setattr__("_relation", relation)
        raw_columns = list(relation.columns if columns is None else columns)
        normalized, lookup = util.normalize_columns(raw_columns)
        super().__setattr__("_columns", tuple(normalized))
        super().__setattr__("_lookup", dict(lookup))
        raw_types = list(_relation_types(relation) if types is None else types)
        if len(raw_types) != len(normalized):
            raise ValueError("Number of types does not match number of columns")
        super().__setattr__("_types", tuple(raw_types))

    @property
    def relation(self) -> duckdb.DuckDBPyRelation:
        """Return the underlying relation."""

        return self._relation

    @property
    def columns(self) -> list[str]:
        """Return the projected column names preserving case."""

        return list(self._columns)

    @property
    def columns_lower(self) -> list[str]:
        """Return lower-cased column names in projection order."""

        return [name.casefold() for name in self._columns]

    @property
    def columns_lower_set(self) -> frozenset[str]:
        """Return a casefolded set of projected column names."""

        return frozenset(self._lookup)

    @property
    def column_types(self) -> list[str]:
        """Return the DuckDB type name for each projected column."""

        return list(self._types)

    def project_columns(self, *columns: str, missing_ok: bool = False) -> DuckRel:
        """Return a relation containing only the requested *columns*."""

        if not columns:
            raise ValueError("At least one column must be provided")

        resolved = util.resolve_columns(columns, self._columns, missing_ok=missing_ok)
        if not resolved:
            if missing_ok:
                return self
            raise KeyError("No columns resolved from projection request")
        projection = _format_projection(resolved)
        relation = self._relation.project(", ".join(projection))
        types = [self._types[self._lookup[name.casefold()]] for name in resolved]
        return type(self)(relation, columns=resolved, types=types)

    def project(self, expressions: Mapping[str, str]) -> DuckRel:
        """Project explicit *expressions* keyed by output column name."""

        if not expressions:
            raise ValueError("Projection requires at least one expression")

        alias_candidates = list(expressions.keys())
        aliases, _ = util.normalize_columns(alias_candidates)
        compiled: list[str] = []
        for alias in aliases:
            expression = expressions[alias]
            if not isinstance(expression, str):
                raise TypeError("Projection expressions must be strings")
            compiled.append(_alias(expression, alias))
        relation = self._relation.project(", ".join(compiled))
        return type(self)(relation, columns=aliases, types=_relation_types(relation))

    def filter(self, expression: str, /, *args: Any) -> DuckRel:
        """Filter the relation using a SQL *expression* with optional parameters."""

        if not isinstance(expression, str):
            raise TypeError("Filter expression must be a string")

        parameters = [util.coerce_scalar(arg) for arg in args]
        rendered = _inject_parameters(expression, parameters)
        relation = self._relation.filter(rendered)
        return type(self)(relation, columns=self._columns, types=self._types)

    def inner_join(self, other: DuckRel, *, on: Sequence[str] | None = None) -> DuckRel:
        """Perform an inner join with *other* on shared or explicit keys."""

        return self._join(other, how="inner", on=on)

    def left_join(self, other: DuckRel, *, on: Sequence[str] | None = None) -> DuckRel:
        """Perform a left join with *other* on shared or explicit keys."""

        return self._join(other, how="left", on=on)

    def semi_join(self, other: DuckRel, *, on: Sequence[str] | None = None) -> DuckRel:
        """Perform a semi join with *other* on shared or explicit keys."""

        return self._join(other, how="semi", on=on)

    def anti_join(self, other: DuckRel, *, on: Sequence[str] | None = None) -> DuckRel:
        """Perform an anti join with *other* on shared or explicit keys."""

        return self._join(other, how="anti", on=on)

    def order_by(self, **orders: Literal["asc", "desc", "ASC", "DESC"]) -> DuckRel:
        """Return a relation ordered by the specified *orders* mapping."""

        if not orders:
            raise ValueError("At least one column required for ordering")

        order_clauses: list[str] = []
        for column, direction in orders.items():
            resolved = util.resolve_columns([column], self._columns)[0]
            normalized = direction.lower()
            if normalized not in {"asc", "desc"}:
                raise ValueError("Ordering direction must be 'asc' or 'desc'")
            clause = f"{_quote_identifier(resolved)} {normalized.upper()}"
            order_clauses.append(clause)
        relation = self._relation.order(", ".join(order_clauses))
        return type(self)(relation, columns=self._columns, types=self._types)

    def limit(self, count: int) -> DuckRel:
        """Limit the relation to *count* rows."""

        if not isinstance(count, int):
            raise TypeError("Limit count must be an integer")
        if count < 0:
            raise ValueError("Limit must be non-negative")
        relation = self._relation.limit(count)
        return type(self)(relation, columns=self._columns, types=self._types)

    def cast_columns(
        self,
        mapping: Mapping[str, util.DuckDBType] | None = None,
        /,
        **casts: util.DuckDBType,
    ) -> DuckRel:
        """Return a relation with specified columns ``CAST`` to DuckDB types."""

        return self._cast_columns("CAST", mapping, casts)

    def try_cast_columns(
        self,
        mapping: Mapping[str, util.DuckDBType] | None = None,
        /,
        **casts: util.DuckDBType,
    ) -> DuckRel:
        """Return a relation with specified columns ``TRY_CAST`` to DuckDB types."""

        return self._cast_columns("TRY_CAST", mapping, casts)

    def materialize(
        self,
        *,
        strategy: MaterializeStrategy | None = None,
        into: duckdb.DuckDBPyConnection | None = None,
    ) -> Materialized:
        """Materialize the relation using *strategy* and optional target *into*.

        When *into* is provided the materialized data is registered on the
        supplied connection and wrapped in a new :class:`DuckRel` instance.
        The default strategy materializes via Arrow tables and retains the
        in-memory table.
        """

        runner = strategy or ArrowMaterializeStrategy()
        result = runner.materialize(self._relation, self._columns, into=into)

        if into is not None and result.relation is None:
            raise ValueError("Materialization strategy did not yield a relation for the target connection")

        if into is None and result.table is None and result.path is None:
            raise ValueError("Materialization strategy did not produce any artefact")

        wrapped: DuckRel | None = None
        if result.relation is not None:
            resolved_columns = (
                tuple(result.columns)
                if result.columns is not None
                else tuple(result.relation.columns)
            )
            wrapped = type(self)(
                result.relation,
                columns=resolved_columns,
                types=_relation_types(result.relation),
            )

        return Materialized(
            table=result.table,
            relation=wrapped,
            path=result.path,
        )

    # Internal helpers -------------------------------------------------

    def _cast_columns(
        self,
        function: Literal["CAST", "TRY_CAST"],
        mapping: Mapping[str, util.DuckDBType] | None,
        casts: Mapping[str, util.DuckDBType],
    ) -> DuckRel:
        provided: dict[str, util.DuckDBType] = {}
        if mapping:
            provided.update(mapping)
        provided.update(casts)

        if not provided:
            raise ValueError("At least one column must be provided for casting")

        resolved: dict[str, str] = {}
        for requested, type_name in provided.items():
            if type_name not in util.DUCKDB_TYPE_SET:
                raise ValueError(f"Unsupported DuckDB type: {type_name!r}")
            resolved_name = util.resolve_columns([requested], self._columns)[0]
            resolved[resolved_name] = str(type_name)

        expressions: list[str] = []
        updated_types: list[str] = []
        for column, current_type in zip(self._columns, self._types, strict=True):
            if column not in resolved:
                expressions.append(_alias(_quote_identifier(column), column))
                updated_types.append(current_type)
                continue

            cast_type = resolved[column]
            expression = f"{function}({_quote_identifier(column)} AS {cast_type})"
            expressions.append(_alias(expression, column))
            updated_types.append(cast_type)

        relation = self._relation.project(", ".join(expressions))
        return type(self)(relation, columns=self._columns, types=updated_types)

    def _join(
        self,
        other: DuckRel,
        *,
        how: str,
        on: Sequence[str] | None,
    ) -> DuckRel:
        if on is None:
            pairs = self._resolve_shared_keys(other)
            if not pairs:
                raise ValueError("No shared columns to join on")
        else:
            pairs = self._resolve_explicit_keys(other, on)

        left_alias = self._relation.set_alias("l")
        right_alias = other._relation.set_alias("r")
        condition = _format_join_condition(pairs, left_alias="l", right_alias="r")
        joined = left_alias.join(right_alias, condition, how=how)

        if how in {"semi", "anti"}:
            projection = _format_projection(self._columns, alias="l")
            relation = joined.project(", ".join(projection))
            return type(self)(relation, columns=self._columns, types=self._types)

        right_key_set = {right.casefold() for _, right in pairs}
        right_columns = [
            column for column in other._columns if column.casefold() not in right_key_set
        ]
        collisions = [
            column for column in right_columns if column.casefold() in self._lookup
        ]
        if collisions:
            duplicates = ", ".join(sorted({name for name in collisions}))
            raise ValueError(
                f"Join would produce duplicate columns: {duplicates}"
            )
        projection = _format_projection(self._columns, alias="l")
        projection.extend(_format_projection(right_columns, alias="r"))
        relation = joined.project(", ".join(projection))
        merged_columns = list(self._columns) + right_columns
        right_types = [
            type_name
            for column, type_name in zip(other._columns, other._types, strict=True)
            if column.casefold() not in right_key_set
        ]
        merged_types = list(self._types) + right_types
        return type(self)(relation, columns=merged_columns, types=merged_types)

    def _resolve_shared_keys(self, other: DuckRel) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        for column in self._columns:
            other_index = other._lookup.get(column.casefold())
            if other_index is not None:
                pairs.append((column, other._columns[other_index]))
        return pairs

    def _resolve_explicit_keys(
        self, other: DuckRel, requested: Sequence[str]
    ) -> list[tuple[str, str]]:
        left_columns = util.resolve_columns(requested, self._columns)
        pairs: list[tuple[str, str]] = []
        for left in left_columns:
            right_index = other._lookup.get(left.casefold())
            if right_index is None:
                raise KeyError(f"Column {left!r} not found in right relation")
            pairs.append((left, other._columns[right_index]))
        return pairs


