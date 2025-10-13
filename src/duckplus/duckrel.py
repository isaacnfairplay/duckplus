"""Immutable relational wrapper for Duck+."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, NamedTuple

import duckdb

from . import util
from .filters import FilterExpression
from ._core_specs import (
    AsofOrder,
    AsofSpec,
    ColumnPredicate,
    JoinProjection,
    JoinSpec,
    PartitionSpec,
)
from .materialize import (
    ArrowMaterializeStrategy,
    MaterializeStrategy,
    Materialized,
)


def _quote_identifier(identifier: str) -> str:
    """Return *identifier* quoted for SQL usage."""

    return util.quote_identifier(identifier)


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


_TEMPORAL_PREFIXES = ("TIMESTAMP", "DATE", "TIME")


def _is_temporal_type(type_name: str) -> bool:
    """Return ``True`` when *type_name* refers to a temporal DuckDB type."""

    normalized = type_name.upper()
    return any(normalized.startswith(prefix) for prefix in _TEMPORAL_PREFIXES)


class _ResolvedJoinSpec(NamedTuple):
    """Internal representation of a resolved join specification."""

    pairs: list[tuple[str, str]]
    left_keys: frozenset[str]
    right_keys: frozenset[str]
    predicates: list[str]


class _ResolvedAsofSpec(NamedTuple):
    """Internal representation of a resolved ASOF specification."""

    join: _ResolvedJoinSpec
    order_left: str
    order_right: str
    left_type: str
    right_type: str
    direction: Literal["backward", "forward", "nearest"]
    tolerance: str | None


def _inject_parameters(expression: str, parameters: Sequence[Any]) -> str:
    """Return *expression* with positional ``?`` placeholders replaced."""

    parts = expression.split("?")
    placeholder_count = len(parts) - 1
    if placeholder_count == 0:
        if parameters:
            raise ValueError(
                "Filter expression contains no '?' placeholders but "
                f"received {len(parameters)} parameter(s)."
            )
        return expression

    if placeholder_count != len(parameters):
        raise ValueError(
            "Mismatch between '?' placeholders and provided parameters; "
            f"expected {placeholder_count} parameter(s) but received {len(parameters)}."
        )

    result: list[str] = []
    for index, segment in enumerate(parts[:-1]):
        result.append(segment)
        value = util.format_sql_literal(parameters[index])
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
            raise ValueError(
                "Number of column types does not match the projected columns; "
                f"expected {len(normalized)} types but received {len(raw_types)}."
            )
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

    def show(self) -> DuckRel:
        """Render the relation using DuckDB's pretty printer and return ``self``."""

        self._relation.show()
        return self

    def project_columns(self, *columns: str, missing_ok: bool = False) -> DuckRel:
        """Return a relation containing only the requested *columns*."""

        if not columns:
            raise ValueError("project_columns() requires at least one column name.")

        resolved = util.resolve_columns(columns, self._columns, missing_ok=missing_ok)
        if not resolved:
            if missing_ok:
                return self
            requested = ", ".join(repr(column) for column in columns)
            raise KeyError(
                "None of the requested columns could be resolved from the relation; "
                f"requested {requested}."
            )
        projection = _format_projection(resolved)
        relation = self._relation.project(", ".join(projection))
        types = [self._types[self._lookup[name.casefold()]] for name in resolved]
        return type(self)(relation, columns=resolved, types=types)

    def drop(self, *columns: str, missing_ok: bool = False) -> DuckRel:
        """Return a relation excluding the specified *columns*."""

        if not columns:
            raise ValueError("drop() requires at least one column name.")

        resolved = util.resolve_columns(columns, self._columns, missing_ok=missing_ok)
        if not resolved:
            if missing_ok:
                return self
            requested = ", ".join(repr(column) for column in columns)
            raise KeyError(
                "None of the requested columns could be resolved from the relation; "
                f"requested {requested}."
            )

        drop_keys = {name.casefold() for name in resolved}
        remaining = [column for column in self._columns if column.casefold() not in drop_keys]

        if not remaining:
            raise ValueError("drop() would remove all columns from the relation.")

        projection = _format_projection(remaining)
        relation = self._relation.project(", ".join(projection))
        types = [self._types[self._lookup[name.casefold()]] for name in remaining]
        return type(self)(relation, columns=remaining, types=types)

    def project(self, expressions: Mapping[str, str]) -> DuckRel:
        """Project explicit *expressions* keyed by output column name."""

        if not expressions:
            raise ValueError("project() requires at least one expression mapping.")

        alias_candidates = list(expressions.keys())
        aliases, _ = util.normalize_columns(alias_candidates)
        compiled: list[str] = []
        for alias in aliases:
            expression = expressions[alias]
            if not isinstance(expression, str):
                raise TypeError(
                    "Projection expressions must be provided as strings; "
                    f"alias {alias!r} mapped to {type(expression).__name__}."
                )
            compiled.append(_alias(expression, alias))
        relation = self._relation.project(", ".join(compiled))
        return type(self)(relation, columns=aliases, types=_relation_types(relation))

    def rename_columns(self, **mappings: str) -> DuckRel:
        """Rename columns using DuckDB's ``RENAME`` star modifier."""

        if not mappings:
            raise ValueError("rename_columns() requires at least one column mapping.")

        resolved: list[tuple[str, str]] = []
        seen_sources: set[str] = set()
        seen_targets: set[str] = set()
        for new_name, original in mappings.items():
            if not isinstance(original, str):
                raise TypeError(
                    "rename_columns() expects string column names; "
                    f"received {type(original).__name__} for {new_name!r}."
                )
            resolved_name = util.resolve_columns([original], self._columns)[0]
            source_key = resolved_name.casefold()
            if source_key in seen_sources:
                raise ValueError(
                    "rename_columns() received duplicate source column names; "
                    f"column {resolved_name!r} mapped multiple times."
                )
            seen_sources.add(source_key)

            target_key = new_name.casefold()
            if target_key in seen_targets:
                raise ValueError(
                    "rename_columns() received duplicate target column names; "
                    f"column {new_name!r} mapped multiple times."
                )
            seen_targets.add(target_key)

            resolved.append((resolved_name, new_name))

        return self._apply_star_projection(rename_entries=resolved)

    def transform_columns(self, **expressions: str) -> DuckRel:
        """Replace columns using DuckDB's ``REPLACE`` star modifier."""

        if not expressions:
            raise ValueError("transform_columns() requires at least one column expression.")

        resolved: list[tuple[str, str]] = []
        seen: set[str] = set()
        for target, expression in expressions.items():
            if not isinstance(expression, str):
                raise TypeError(
                    "transform_columns() expects SQL expressions as strings; "
                    f"received {type(expression).__name__} for {target!r}."
                )
            if not expression.strip():
                raise ValueError(
                    "transform_columns() expressions must not be empty; "
                    f"column {target!r} received an empty expression."
                )

            resolved_name = util.resolve_columns([target], self._columns)[0]
            key = resolved_name.casefold()
            if key in seen:
                raise ValueError(
                    "transform_columns() received duplicate column names; "
                    f"column {resolved_name!r} mapped multiple times."
                )
            seen.add(key)

            quoted = _quote_identifier(resolved_name)
            templated = expression.replace("{column}", quoted).replace("{col}", quoted)
            resolved.append((resolved_name, templated))

        return self._apply_star_projection(transform_entries=resolved)

    def add_columns(self, **expressions: str) -> DuckRel:
        """Add computed columns to the relation using ``*`` projection syntax."""

        if not expressions:
            raise ValueError("add_columns() requires at least one expression mapping.")

        resolved: list[tuple[str, str]] = []
        seen: set[str] = set()
        for name, expression in expressions.items():
            if not isinstance(expression, str):
                raise TypeError(
                    "add_columns() expects SQL expressions as strings; "
                    f"received {type(expression).__name__} for {name!r}."
                )
            if not expression.strip():
                raise ValueError(
                    "add_columns() expressions must not be empty; "
                    f"column {name!r} received an empty expression."
                )
            key = name.casefold()
            if key in seen:
                raise ValueError(
                    "add_columns() received duplicate column names; "
                    f"column {name!r} mapped multiple times."
                )
            seen.add(key)
            resolved.append((name, expression))

        return self._apply_star_projection(add_entries=resolved)

    def _render_filter_expression(
        self, expression: str | FilterExpression, args: Sequence[Any]
    ) -> str:
        """Return *expression* with *args* coerced and injected."""

        if isinstance(expression, FilterExpression):
            if args:
                raise TypeError("FilterExpression does not accept positional parameters.")
            return expression.render(self._columns)

        if not isinstance(expression, str):
            raise TypeError(
                "Filter expression must be a string or FilterExpression; "
                f"received {type(expression).__name__}."
            )

        parameters = [util.coerce_scalar(arg) for arg in args]
        return _inject_parameters(expression, parameters)

    def filter(self, expression: str | FilterExpression, /, *args: Any) -> DuckRel:
        """Filter the relation using a SQL *expression* with optional parameters."""

        rendered = self._render_filter_expression(expression, args)
        relation = self._relation.filter(rendered)
        return type(self)(relation, columns=self._columns, types=self._types)

    def split(
        self, expression: str | FilterExpression, /, *args: Any
    ) -> tuple[DuckRel, DuckRel]:
        """Split the relation into matching and non-matching partitions.

        Returns a ``(matching, remainder)`` tuple where the first relation
        contains rows satisfying the provided *expression* and the second holds
        rows where the expression evaluates to ``FALSE`` or ``NULL``.
        """

        rendered = self._render_filter_expression(expression, args)
        matches = type(self)(
            self._relation.filter(rendered),
            columns=self._columns,
            types=self._types,
        )
        remainder_expression = f"NOT (COALESCE(({rendered}), FALSE))"
        remainder = type(self)(
            self._relation.filter(remainder_expression),
            columns=self._columns,
            types=self._types,
        )
        return matches, remainder

    def natural_inner(
        self,
        other: DuckRel,
        /,
        *,
        strict: bool = True,
        allow_collisions: bool = False,
        suffixes: tuple[str, str] | None = None,
        **key_aliases: str,
    ) -> DuckRel:
        """Perform a natural inner join using shared columns and optional aliases."""

        projection = self._build_projection(allow_collisions=allow_collisions, suffixes=suffixes)
        resolved = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        return self._execute_join(other, how="inner", resolved=resolved, projection=projection)

    def natural_left(
        self,
        other: DuckRel,
        /,
        *,
        strict: bool = True,
        allow_collisions: bool = False,
        suffixes: tuple[str, str] | None = None,
        **key_aliases: str,
    ) -> DuckRel:
        """Perform a natural left join using shared columns and optional aliases."""

        projection = self._build_projection(allow_collisions=allow_collisions, suffixes=suffixes)
        resolved = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        return self._execute_join(other, how="left", resolved=resolved, projection=projection)

    def natural_right(
        self,
        other: DuckRel,
        /,
        *,
        strict: bool = True,
        allow_collisions: bool = False,
        suffixes: tuple[str, str] | None = None,
        **key_aliases: str,
    ) -> DuckRel:
        """Perform a natural right join using shared columns and optional aliases."""

        projection = self._build_projection(allow_collisions=allow_collisions, suffixes=suffixes)
        resolved = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        return self._execute_join(other, how="right", resolved=resolved, projection=projection)

    def natural_full(
        self,
        other: DuckRel,
        /,
        *,
        strict: bool = True,
        allow_collisions: bool = False,
        suffixes: tuple[str, str] | None = None,
        **key_aliases: str,
    ) -> DuckRel:
        """Perform a natural full join using shared columns and optional aliases."""

        projection = self._build_projection(allow_collisions=allow_collisions, suffixes=suffixes)
        resolved = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        return self._execute_join(other, how="outer", resolved=resolved, projection=projection)

    def natural_asof(
        self,
        other: DuckRel,
        /,
        *,
        order: AsofOrder,
        direction: Literal["backward", "forward", "nearest"] = "backward",
        tolerance: str | None = None,
        strict: bool = True,
        allow_collisions: bool = False,
        suffixes: tuple[str, str] | None = None,
        **key_aliases: str,
    ) -> DuckRel:
        """Perform a natural ASOF join with explicit ordering."""

        projection = self._build_projection(allow_collisions=allow_collisions, suffixes=suffixes)
        base = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        resolved = self._resolve_asof_spec(
            other,
            AsofSpec(equal_keys=base.pairs, predicates=(), order=order, direction=direction, tolerance=tolerance),
        )
        return self._execute_asof_join(other, resolved=resolved, projection=projection)

    def inspect_partitions(
        self,
        other: DuckRel,
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
    ) -> DuckRel:
        """Return per-partition row counts for *self* and *other*."""

        partition_spec = self._normalize_partition_spec(partition)
        resolved = self._resolve_join_spec(other, partition_spec)
        if not resolved.pairs:
            raise ValueError(
                "Partition inspection requires at least one shared or aliased column; "
                f"partition spec={partition_spec.equal_keys!r}."
            )

        left_keys = [left for left, _ in resolved.pairs]

        left_key_exprs = [_quote_identifier(column) for column in left_keys]
        left_group_clause = ", ".join(left_key_exprs)
        left_projection = ", ".join([*left_key_exprs, "COUNT(*) AS left_count"])
        left_counts_relation = self._relation.set_alias("l").query(
            "l", f"SELECT {left_projection} FROM l GROUP BY {left_group_clause}"
        )
        left_counts = type(self)(left_counts_relation)

        right_select_parts: list[str] = []
        right_group_parts: list[str] = []
        for left_column, right_column in resolved.pairs:
            expression = _quote_identifier(right_column)
            right_group_parts.append(expression)
            right_select_parts.append(f"{expression} AS {_quote_identifier(left_column)}")
        right_projection = ", ".join([*right_select_parts, "COUNT(*) AS right_count"])
        right_group_clause = ", ".join(right_group_parts)
        right_counts_relation = other._relation.set_alias("r").query(
            "r", f"SELECT {right_projection} FROM r GROUP BY {right_group_clause}"
        )
        right_counts = type(self)(right_counts_relation)

        key_union_relation = left_counts.project_columns(*left_keys)._relation.union(
            right_counts.project_columns(*left_keys)._relation
        ).distinct()
        keys = type(self)(key_union_relation)

        summary = keys.natural_left(left_counts, allow_collisions=True)
        summary = summary.natural_left(right_counts, allow_collisions=True)

        def _coalesce(column: str) -> str:
            identifier = _quote_identifier(column)
            return f"COALESCE({identifier}, 0)"

        left_expr = _coalesce("left_count")
        right_expr = _coalesce("right_count")

        projections: dict[str, str] = {key: _quote_identifier(key) for key in left_keys}
        projections["left_count"] = left_expr
        projections["right_count"] = right_expr
        projections["pair_count"] = f"({left_expr}) * ({right_expr})"
        projections["shared_partition"] = (
            f"CASE WHEN ({left_expr}) > 0 AND ({right_expr}) > 0 THEN TRUE ELSE FALSE END"
        )

        return summary.project(projections)

    def partitioned_join(
        self,
        other: DuckRel,
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
        spec: JoinSpec,
        /,
        *,
        how: Literal["inner", "left", "right", "outer"],
        project: JoinProjection | None = None,
    ) -> DuckRel:
        """Join *other* using *spec* while constraining matches to partition columns."""

        partition_spec = self._normalize_partition_spec(partition)
        partition_resolved = self._resolve_join_spec(other, partition_spec)
        join_resolved = self._resolve_join_spec(other, spec)
        combined = self._combine_partition_and_join(partition_resolved, join_resolved)
        return self._execute_join(other, how=how, resolved=combined, projection=project)

    def partitioned_inner(
        self,
        other: DuckRel,
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> DuckRel:
        """Perform an inner join constrained by *partition* columns."""

        return self.partitioned_join(other, partition, spec, how="inner", project=project)

    def partitioned_left(
        self,
        other: DuckRel,
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> DuckRel:
        """Perform a left outer join constrained by *partition* columns."""

        return self.partitioned_join(other, partition, spec, how="left", project=project)

    def partitioned_right(
        self,
        other: DuckRel,
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> DuckRel:
        """Perform a right outer join constrained by *partition* columns."""

        return self.partitioned_join(other, partition, spec, how="right", project=project)

    def partitioned_full(
        self,
        other: DuckRel,
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> DuckRel:
        """Perform a full outer join constrained by *partition* columns."""

        return self.partitioned_join(other, partition, spec, how="outer", project=project)

    def left_inner(
        self,
        other: DuckRel,
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> DuckRel:
        """Perform an inner join against *other* using an explicit :class:`JoinSpec`."""

        resolved = self._resolve_join_spec(other, spec)
        return self._execute_join(other, how="inner", resolved=resolved, projection=project)

    def left_outer(
        self,
        other: DuckRel,
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> DuckRel:
        """Perform a left outer join against *other* using an explicit :class:`JoinSpec`."""

        resolved = self._resolve_join_spec(other, spec)
        return self._execute_join(other, how="left", resolved=resolved, projection=project)

    def left_right(
        self,
        other: DuckRel,
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> DuckRel:
        """Perform a right join against *other* using an explicit :class:`JoinSpec`."""

        resolved = self._resolve_join_spec(other, spec)
        return self._execute_join(other, how="right", resolved=resolved, projection=project)

    def inner_join(
        self,
        other: DuckRel,
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> DuckRel:
        """Perform a symmetric inner join using *spec*."""

        resolved = self._resolve_join_spec(other, spec)
        return self._execute_join(other, how="inner", resolved=resolved, projection=project)

    def outer_join(
        self,
        other: DuckRel,
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> DuckRel:
        """Perform a full outer join using *spec*."""

        resolved = self._resolve_join_spec(other, spec)
        return self._execute_join(other, how="outer", resolved=resolved, projection=project)

    def asof_join(
        self,
        other: DuckRel,
        spec: AsofSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> DuckRel:
        """Perform an ASOF join using the provided :class:`AsofSpec`."""

        resolved = self._resolve_asof_spec(other, spec)
        return self._execute_asof_join(other, resolved=resolved, projection=project)

    def semi_join(
        self,
        other: DuckRel,
        /,
        *,
        strict: bool = True,
        **key_aliases: str,
    ) -> DuckRel:
        """Perform a semi join preserving left rows that match *other*."""

        resolved = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        return self._execute_join(other, how="semi", resolved=resolved, projection=None)

    def anti_join(
        self,
        other: DuckRel,
        /,
        *,
        strict: bool = True,
        **key_aliases: str,
    ) -> DuckRel:
        """Perform an anti join preserving left rows that do not match *other*."""

        resolved = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        return self._execute_join(other, how="anti", resolved=resolved, projection=None)

    def order_by(self, **orders: Literal["asc", "desc", "ASC", "DESC"]) -> DuckRel:
        """Return a relation ordered by the specified *orders* mapping."""

        if not orders:
            raise ValueError("order_by() requires at least one column/direction pair.")

        order_clauses: list[str] = []
        for column, direction in orders.items():
            resolved = util.resolve_columns([column], self._columns)[0]
            if not isinstance(direction, str):
                raise TypeError(
                    "Ordering direction must be a string literal 'asc' or 'desc'; "
                    f"received {type(direction).__name__} for column {column!r}."
                )
            normalized = direction.lower()
            if normalized not in {"asc", "desc"}:
                raise ValueError(
                    "Ordering direction must be 'asc' or 'desc'; "
                    f"received {direction!r} for column {column!r}."
                )
            clause = f"{_quote_identifier(resolved)} {normalized.upper()}"
            order_clauses.append(clause)
        relation = self._relation.order(", ".join(order_clauses))
        return type(self)(relation, columns=self._columns, types=self._types)

    def limit(self, count: int) -> DuckRel:
        """Limit the relation to *count* rows."""

        if not isinstance(count, int):
            raise TypeError(
                "limit() expects an integer count; "
                f"received {type(count).__name__}."
            )
        if count < 0:
            raise ValueError(f"limit() requires a non-negative count; received {count}.")
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
        supplied connection and wrapped in a new :class:`duckplus.DuckRel` instance.
        The default strategy materializes via Arrow tables and retains the
        in-memory table.
        """

        runner = strategy or ArrowMaterializeStrategy()
        result = runner.materialize(self._relation, self._columns, into=into)

        if into is not None and result.relation is None:
            raise ValueError(
                "Materialization strategy did not yield a relation for the target connection; "
                f"strategy={type(runner).__name__}."
            )

        if into is None and result.table is None and result.path is None:
            raise ValueError(
                "Materialization strategy did not produce any artefact (table, relation, or path); "
                f"strategy={type(runner).__name__}."
            )

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
            raise ValueError("cast_columns()/try_cast_columns() require at least one column mapping.")

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

    def _apply_star_projection(
        self,
        *,
        rename_entries: Sequence[tuple[str, str]] | None = None,
        transform_entries: Sequence[tuple[str, str]] | None = None,
        add_entries: Sequence[tuple[str, str]] | None = None,
    ) -> DuckRel:
        rename_entries = list(rename_entries or [])
        transform_entries = list(transform_entries or [])
        add_entries = list(add_entries or [])

        if not (rename_entries or transform_entries or add_entries):
            raise ValueError("Star projection requires at least one modification.")

        final_columns = list(self._columns)
        for original, new in rename_entries:
            index = self._lookup[original.casefold()]
            final_columns[index] = new

        util.ensure_unique_names(final_columns)

        final_aliases = {
            column.casefold(): final_columns[index]
            for index, column in enumerate(self._columns)
        }

        rename_sql: list[str] = []
        if rename_entries:
            rename_sql = [
                f"{_quote_identifier(original)} AS {_quote_identifier(new)}"
                for original, new in rename_entries
            ]

        replace_sql: list[str] = []
        if transform_entries:
            replace_sql = [
                f"({expression}) AS {_quote_identifier(final_aliases[original.casefold()])}"
                for original, expression in transform_entries
            ]

        seen = {name.casefold() for name in final_columns}
        add_sql: list[str] = []
        for name, expression in add_entries:
            key = name.casefold()
            if key in seen:
                raise ValueError(
                    "add_columns() would create duplicate column name; "
                    f"column {name!r} already exists."
                )
            seen.add(key)
            add_sql.append(f"({expression}) AS {_quote_identifier(name)}")

        star_parts = ["*"]
        if rename_sql:
            star_parts.append(f"RENAME ({', '.join(rename_sql)})")
        if replace_sql:
            star_parts.append(f"REPLACE ({', '.join(replace_sql)})")
        star_expr = " ".join(star_parts)

        projection_parts = [star_expr, *add_sql]
        projection_sql = ", ".join(part for part in projection_parts if part)
        relation = self._relation.project(projection_sql)
        return type(self)(relation)

    def _build_projection(
        self,
        *,
        allow_collisions: bool,
        suffixes: tuple[str, str] | None,
    ) -> JoinProjection:
        """Return a :class:`JoinProjection` honoring user configuration."""

        allow = allow_collisions or suffixes is not None
        return JoinProjection(allow_collisions=allow, suffixes=suffixes)

    def _compile_projection(
        self,
        other: DuckRel,
        *,
        resolved: _ResolvedJoinSpec,
        projection: JoinProjection | None,
        include_right_keys: bool,
    ) -> tuple[list[str], list[str], list[str]]:
        """Compile projection expressions for join outputs."""

        config = projection or JoinProjection()

        regular_collisions: set[str] = set()
        join_key_collisions: set[str] = set()
        for column in other._columns:
            key = column.casefold()
            if key not in self._lookup:
                continue
            if key in resolved.right_keys:
                if include_right_keys:
                    join_key_collisions.add(key)
            else:
                regular_collisions.add(key)

        collisions_for_validation = regular_collisions
        if collisions_for_validation and not (config.allow_collisions or config.suffixes is not None):
            duplicates = ", ".join(
                sorted(
                    {
                        column
                        for column in other._columns
                        if column.casefold() in collisions_for_validation
                    }
                )
            )
            raise ValueError(f"Join would produce duplicate columns: {duplicates}")

        collisions = regular_collisions | join_key_collisions

        suffix_left = ""
        suffix_right = ""
        if collisions:
            suffix_left, suffix_right = config.suffixes or ("_1", "_2")

        expressions: list[str] = []
        columns: list[str] = []
        types: list[str] = []
        seen: set[str] = set()

        for column, type_name in zip(self._columns, self._types, strict=True):
            key = column.casefold()
            output = column
            if key in regular_collisions:
                output = f"{column}{suffix_left}"
            elif key in join_key_collisions and include_right_keys and config.suffixes is not None:
                output = f"{column}{suffix_left}"
            lower = output.casefold()
            if lower in seen:
                raise ValueError(
                    "Join projection produced duplicate column name "
                    f"{output!r} while processing left relation columns."
                )
            seen.add(lower)
            expressions.append(_alias(_qualify("l", column), output))
            columns.append(output)
            types.append(type_name)

        for column, type_name in zip(other._columns, other._types, strict=True):
            key = column.casefold()
            is_join_key = key in resolved.right_keys
            if is_join_key and not include_right_keys:
                continue
            output = column
            if key in collisions:
                output = f"{column}{suffix_right}"
            lower = output.casefold()
            if lower in seen:
                raise ValueError(
                    "Join projection produced duplicate column name "
                    f"{output!r} while processing right relation columns."
                )
            seen.add(lower)
            expressions.append(_alias(_qualify("r", column), output))
            columns.append(output)
            types.append(type_name)

        return expressions, columns, types

    def _build_natural_join_spec(
        self,
        other: DuckRel,
        *,
        strict: bool,
        key_aliases: Mapping[str, str],
    ) -> _ResolvedJoinSpec:
        """Resolve shared and aliased keys for natural joins."""

        pairs: list[tuple[str, str]] = []
        left_positions: dict[str, int] = {}
        for column in self._columns:
            other_index = other._lookup.get(column.casefold())
            if other_index is None:
                continue
            pairs.append((column, other._columns[other_index]))
            left_positions[column.casefold()] = len(pairs) - 1

        for requested_left, requested_right in key_aliases.items():
            if not isinstance(requested_left, str) or not isinstance(requested_right, str):
                raise TypeError(
                    "Join key aliases must map string column names; "
                    f"received {requested_left!r} -> {requested_right!r}."
                )

            left_candidates = util.resolve_columns(
                [requested_left], self._columns, missing_ok=not strict
            )
            if not left_candidates:
                continue
            right_candidates = util.resolve_columns(
                [requested_right], other._columns, missing_ok=not strict
            )
            if not right_candidates:
                continue

            left_column = left_candidates[0]
            right_column = right_candidates[0]
            position = left_positions.get(left_column.casefold())
            if position is not None:
                pairs[position] = (left_column, right_column)
            else:
                pairs.append((left_column, right_column))
                left_positions[left_column.casefold()] = len(pairs) - 1

        if not pairs:
            raise ValueError(
                "Natural join could not find shared columns between relations; "
                f"left columns={self.columns}, right columns={other.columns}."
            )

        left_keys = frozenset(name.casefold() for name, _ in pairs)
        right_keys = frozenset(name.casefold() for _, name in pairs)
        return _ResolvedJoinSpec(pairs=pairs, left_keys=left_keys, right_keys=right_keys, predicates=[])

    def _normalize_partition_spec(
        self,
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
    ) -> PartitionSpec:
        """Normalize user-provided partition descriptions into a :class:`PartitionSpec`."""

        if isinstance(partition, PartitionSpec):
            return partition
        if isinstance(partition, Mapping):
            return PartitionSpec(equal_keys=tuple((left, right) for left, right in partition.items()))
        if isinstance(partition, Sequence):
            pairs: list[tuple[str, str]] = []
            for entry in partition:
                if isinstance(entry, Sequence) and not isinstance(entry, (str, bytes)):
                    pair = tuple(entry)
                    if len(pair) != 2:
                        raise ValueError(
                            "Partition sequences must contain pairs of column names; "
                            f"received {len(pair)} values in {entry!r}."
                        )
                    left, right = pair
                    if not isinstance(left, str) or not isinstance(right, str):
                        raise TypeError(
                            "Partition sequences must contain string column names; "
                            f"received {left!r} -> {right!r}."
                        )
                    pairs.append((left, right))
                else:
                    raise TypeError(
                        "Partition sequences must contain pairs of column names; "
                        f"received unsupported entry {entry!r}."
                    )
            if not pairs:
                raise ValueError("Partition specification requires at least one column pair.")
            return PartitionSpec(equal_keys=tuple(pairs))
        raise TypeError(
            "Partition specification must be a PartitionSpec, mapping, or sequence of column pairs; "
            f"received {type(partition).__name__}."
        )

    def _combine_partition_and_join(
        self,
        partition: _ResolvedJoinSpec,
        join: _ResolvedJoinSpec,
    ) -> _ResolvedJoinSpec:
        """Merge partition equality pairs with the main join specification."""

        pairs: list[tuple[str, str]] = list(partition.pairs)
        left_lookup: dict[str, tuple[str, int]] = {}
        right_lookup: dict[str, tuple[str, int]] = {}
        for index, (left, right) in enumerate(pairs):
            left_lookup[left.casefold()] = (right.casefold(), index)
            right_lookup[right.casefold()] = (left.casefold(), index)

        for left, right in join.pairs:
            left_key = left.casefold()
            right_key = right.casefold()
            left_entry = left_lookup.get(left_key)
            if left_entry is not None:
                existing_right, existing_index = left_entry
                if existing_right != right_key:
                    raise ValueError(
                        "Partition specification conflicts with join specification: "
                        f"left column {left!r} pairs with both {pairs[existing_index][1]!r} and {right!r}."
                    )
                continue
            right_entry = right_lookup.get(right_key)
            if right_entry is not None:
                existing_left, existing_index = right_entry
                if existing_left != left_key:
                    raise ValueError(
                        "Partition specification conflicts with join specification: "
                        f"right column {right!r} pairs with both {pairs[existing_index][0]!r} and {left!r}."
                    )
                continue
            pairs.append((left, right))
            index = len(pairs) - 1
            left_lookup[left_key] = (right_key, index)
            right_lookup[right_key] = (left_key, index)

        left_keys = frozenset(name.casefold() for name, _ in pairs)
        right_keys = frozenset(name.casefold() for _, name in pairs)
        return _ResolvedJoinSpec(pairs=pairs, left_keys=left_keys, right_keys=right_keys, predicates=list(join.predicates))

    def _resolve_join_spec(self, other: DuckRel, spec: JoinSpec) -> _ResolvedJoinSpec:
        """Resolve a :class:`JoinSpec` against the current relation metadata."""

        pairs: list[tuple[str, str]] = []
        left_positions: dict[str, int] = {}
        for left_name, right_name in spec.equal_keys:
            if not isinstance(left_name, str) or not isinstance(right_name, str):
                raise TypeError(
                    "JoinSpec.equal_keys must contain string column names; "
                    f"received {left_name!r} -> {right_name!r}."
                )
            left_column = util.resolve_columns([left_name], self._columns)[0]
            right_column = util.resolve_columns([right_name], other._columns)[0]
            position = left_positions.get(left_column.casefold())
            if position is not None:
                pairs[position] = (left_column, right_column)
            else:
                pairs.append((left_column, right_column))
                left_positions[left_column.casefold()] = len(pairs) - 1

        predicates: list[str] = []
        for predicate in spec.predicates:
            if isinstance(predicate, ColumnPredicate):
                left_column = util.resolve_columns([predicate.left], self._columns)[0]
                right_column = util.resolve_columns([predicate.right], other._columns)[0]
                predicates.append(
                    f"{_qualify('l', left_column)} {predicate.operator} {_qualify('r', right_column)}"
                )
            else:
                predicates.append(predicate.expression)

        if not pairs and not predicates:
            raise ValueError(
                "Join specification produced no columns or predicates after resolution; "
                f"equal_keys={spec.equal_keys!r}, predicates={spec.predicates!r}."
            )

        left_keys = frozenset(name.casefold() for name, _ in pairs)
        right_keys = frozenset(name.casefold() for _, name in pairs)
        return _ResolvedJoinSpec(pairs=pairs, left_keys=left_keys, right_keys=right_keys, predicates=predicates)

    def _execute_join(
        self,
        other: DuckRel,
        *,
        how: str,
        resolved: _ResolvedJoinSpec,
        projection: JoinProjection | None,
    ) -> DuckRel:
        clauses: list[str] = []
        if resolved.pairs:
            clauses.append(_format_join_condition(resolved.pairs, left_alias="l", right_alias="r"))
        clauses.extend(resolved.predicates)
        if not clauses:
            raise ValueError(
                "Join requires at least one equality key or predicate; "
                f"resolved specification was empty for how={how!r}."
            )

        condition = " AND ".join(clauses)
        left_alias = self._relation.set_alias("l")
        right_alias = other._relation.set_alias("r")
        joined = left_alias.join(right_alias, condition, how=how)

        if how in {"semi", "anti"}:
            projection_exprs = _format_projection(self._columns, alias="l")
            relation = joined.project(", ".join(projection_exprs))
            return type(self)(relation, columns=self._columns, types=self._types)

        expressions, columns, types = self._compile_projection(
            other,
            resolved=resolved,
            projection=projection,
            include_right_keys=how in {"right", "outer"},
        )
        relation = joined.project(", ".join(expressions))
        return type(self)(relation, columns=columns, types=types)

    def _resolve_asof_spec(self, other: DuckRel, spec: AsofSpec) -> _ResolvedAsofSpec:
        """Resolve an :class:`AsofSpec` against relation metadata."""

        base = self._resolve_join_spec(
            other, JoinSpec(equal_keys=spec.equal_keys, predicates=spec.predicates)
        )
        left_column = util.resolve_columns([spec.order.left], self._columns)[0]
        right_column = util.resolve_columns([spec.order.right], other._columns)[0]
        left_type = self._types[self._lookup[left_column.casefold()]]
        right_type = other._types[other._lookup[right_column.casefold()]]

        if _is_temporal_type(left_type) != _is_temporal_type(right_type):
            raise ValueError(
                "ASOF order columns must both be temporal types or both be numeric; "
                f"left column {left_column!r} is {left_type!r}, right column {right_column!r} is {right_type!r}."
            )

        return _ResolvedAsofSpec(
            join=base,
            order_left=left_column,
            order_right=right_column,
            left_type=left_type,
            right_type=right_type,
            direction=spec.direction,
            tolerance=spec.tolerance,
        )

    def _normalized_order_expression(self, *, expression: str, type_name: str) -> str:
        if _is_temporal_type(type_name):
            return f"epoch({expression})"
        return f"CAST({expression} AS DOUBLE)"

    def _absolute_difference_expression(self, spec: _ResolvedAsofSpec) -> str:
        left_expr = _qualify("l", spec.order_left)
        right_expr = _qualify("r", spec.order_right)
        left_normalized = self._normalized_order_expression(
            expression=left_expr, type_name=spec.left_type
        )
        right_normalized = self._normalized_order_expression(
            expression=right_expr, type_name=spec.right_type
        )
        return f"ABS(({left_normalized}) - ({right_normalized}))"

    def _directional_difference_expression(
        self, spec: _ResolvedAsofSpec, *, greater: bool
    ) -> str:
        left_expr = _qualify("l", spec.order_left)
        right_expr = _qualify("r", spec.order_right)
        left_normalized = self._normalized_order_expression(
            expression=left_expr, type_name=spec.left_type
        )
        right_normalized = self._normalized_order_expression(
            expression=right_expr, type_name=spec.right_type
        )
        if greater:
            return f"({left_normalized}) - ({right_normalized})"
        return f"({right_normalized}) - ({left_normalized})"

    def _tolerance_value_expression(self, spec: _ResolvedAsofSpec) -> str:
        if spec.tolerance is None:
            raise ValueError(
                "ASOF join tolerance was requested but no tolerance expression was provided."
            )
        if _is_temporal_type(spec.left_type):
            escaped = spec.tolerance.replace("'", "''")
            return f"epoch(INTERVAL '{escaped}')"
        return spec.tolerance

    def _execute_asof_join(
        self,
        other: DuckRel,
        *,
        resolved: _ResolvedAsofSpec,
        projection: JoinProjection | None,
    ) -> DuckRel:
        expressions, columns, types = self._compile_projection(
            other,
            resolved=resolved.join,
            projection=projection,
            include_right_keys=False,
        )

        clauses: list[str] = []
        if resolved.join.pairs:
            clauses.append(
                _format_join_condition(resolved.join.pairs, left_alias="l", right_alias="r")
            )
        clauses.extend(resolved.join.predicates)

        left_order_expr = _qualify("l", resolved.order_left)
        right_order_expr = _qualify("r", resolved.order_right)

        order_components = [f"CASE WHEN {right_order_expr} IS NULL THEN 1 ELSE 0 END"]
        diff_for_tolerance: str | None = None

        if resolved.direction == "backward":
            clauses.append(f"{left_order_expr} >= {right_order_expr}")
            order_components.append(f"{right_order_expr} DESC")
            diff_for_tolerance = self._directional_difference_expression(resolved, greater=True)
        elif resolved.direction == "forward":
            clauses.append(f"{left_order_expr} <= {right_order_expr}")
            order_components.append(f"{right_order_expr} ASC")
            diff_for_tolerance = self._directional_difference_expression(resolved, greater=False)
        else:
            diff_expr = self._absolute_difference_expression(resolved)
            order_components.append(f"{diff_expr} ASC")
            order_components.append(f"{right_order_expr} ASC")
            diff_for_tolerance = diff_expr

        if resolved.tolerance is not None:
            tolerance_expr = self._tolerance_value_expression(resolved)
            clauses.append(f"{diff_for_tolerance} <= {tolerance_expr}")

        on_clause = " AND ".join(clauses) if clauses else "TRUE"
        order_clause = ", ".join(order_components)

        right_sql = other._relation.sql_query()
        projection_sql = ",\n        ".join(expressions)
        select_sql = ", ".join(_quote_identifier(name) for name in columns)
        query = f"""
WITH left_base AS (
    SELECT *, ROW_NUMBER() OVER () AS __duckplus_row_id
    FROM left_input
),
right_base AS (
    SELECT *
    FROM ({right_sql}) AS right_source
),
ranked AS (
    SELECT
        {projection_sql},
        l.__duckplus_row_id AS __duckplus_row_id,
        ROW_NUMBER() OVER (
            PARTITION BY l.__duckplus_row_id
            ORDER BY {order_clause}
        ) AS __duckplus_rank
    FROM left_base AS l
    LEFT JOIN right_base AS r
      ON {on_clause}
),
filtered AS (
    SELECT *
    FROM ranked
    WHERE __duckplus_rank = 1
)
SELECT {select_sql}
FROM filtered
ORDER BY __duckplus_row_id
"""

        relation = self._relation.query("left_input", query)
        return type(self)(relation, columns=columns, types=types)


