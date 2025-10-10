"""Immutable relational wrapper for Duck+."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Iterable, Literal, NamedTuple, cast

import duckdb

from . import util
from .materialize import (
    ArrowMaterializeStrategy,
    MaterializeStrategy,
    Materialized,
)


def _quote_identifier(identifier: str) -> str:
    """Return *identifier* quoted for SQL usage."""

    escaped = identifier.replace('"', '"' * 2)
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


_TEMPORAL_PREFIXES = ("TIMESTAMP", "DATE", "TIME")


def _is_temporal_type(type_name: str) -> bool:
    """Return ``True`` when *type_name* refers to a temporal DuckDB type."""

    normalized = type_name.upper()
    return any(normalized.startswith(prefix) for prefix in _TEMPORAL_PREFIXES)


@dataclass(frozen=True)
class ColumnPredicate:
    """Join predicate comparing two columns with an operator."""

    left: str
    operator: Literal["=", "!=", "<", "<=", ">", ">="]
    right: str

    def __post_init__(self) -> None:
        if self.operator not in {"=", "!=", "<", "<=", ">", ">="}:
            raise ValueError(f"Unsupported predicate operator: {self.operator!r}")


@dataclass(frozen=True)
class ExpressionPredicate:
    """Arbitrary SQL predicate fragment for joins."""

    expression: str

    def __post_init__(self) -> None:
        if not isinstance(self.expression, str) or not self.expression.strip():
            raise ValueError("Join expression predicates must be non-empty strings")


JoinPredicate = ColumnPredicate | ExpressionPredicate


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


@dataclass(frozen=True)
class JoinSpec:
    """Structured join specification with equality keys and optional predicates."""

    equal_keys: Sequence[tuple[str, str]]
    predicates: Sequence[JoinPredicate] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not isinstance(self.equal_keys, Sequence):
            raise TypeError("JoinSpec.equal_keys must be a sequence of column pairs")
        if not isinstance(self.predicates, Sequence):
            raise TypeError("JoinSpec.predicates must be a sequence of predicates")

        normalized_keys: list[tuple[str, str]] = []
        for pair_obj in cast(Sequence[object], self.equal_keys):
            if isinstance(pair_obj, (str, bytes)):
                raise TypeError("JoinSpec.equal_keys must contain pairs of column names")
            if not isinstance(pair_obj, Sequence):
                raise TypeError("JoinSpec.equal_keys must contain pairs of column names")
            pair = tuple(pair_obj)
            if len(pair) != 2:
                raise ValueError("JoinSpec.equal_keys must contain column name pairs")
            left, right = pair
            if not isinstance(left, str) or not isinstance(right, str):
                raise TypeError("JoinSpec.equal_keys must contain string column names")
            normalized_keys.append((left, right))

        normalized_predicates: list[JoinPredicate] = []
        for predicate_obj in cast(Sequence[object], self.predicates):
            if not isinstance(predicate_obj, (ColumnPredicate, ExpressionPredicate)):
                raise TypeError("JoinSpec.predicates must contain JoinPredicate instances")
            normalized_predicates.append(predicate_obj)

        if not normalized_keys and not normalized_predicates:
            raise ValueError("JoinSpec requires at least one equality key or predicate")

        object.__setattr__(self, "equal_keys", tuple(normalized_keys))
        object.__setattr__(self, "predicates", tuple(normalized_predicates))


@dataclass(frozen=True)
class AsofOrder:
    """Pair of columns describing ASOF ordering."""

    left: str
    right: str


class AsofSpec(JoinSpec):
    """Structured ASOF join specification."""

    __slots__ = ("order", "direction", "tolerance")

    order: AsofOrder
    direction: Literal["backward", "forward", "nearest"]
    tolerance: str | None

    def __init__(
        self,
        *,
        equal_keys: Sequence[tuple[str, str]],
        order: AsofOrder,
        predicates: Sequence[JoinPredicate] = (),
        direction: Literal["backward", "forward", "nearest"] = "backward",
        tolerance: str | None = None,
    ) -> None:
        super().__init__(equal_keys=equal_keys, predicates=predicates)
        object.__setattr__(self, "order", order)
        object.__setattr__(self, "direction", direction)
        object.__setattr__(self, "tolerance", tolerance)
        if direction not in {"backward", "forward", "nearest"}:
            raise ValueError("ASOF direction must be 'backward', 'forward', or 'nearest'")
        if direction == "nearest" and tolerance is None:
            raise ValueError("ASOF nearest direction requires a tolerance")


@dataclass(frozen=True)
class JoinProjection:
    """Projection controls for join column collision handling."""

    allow_collisions: bool = False
    suffixes: tuple[str, str] | None = None

    def __post_init__(self) -> None:
        if self.suffixes is not None:
            if len(self.suffixes) != 2:
                raise ValueError("JoinProjection.suffixes must contain exactly two values")


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
    ) -> tuple[list[str], list[str], list[str]]:
        """Compile projection expressions for join outputs."""

        config = projection or JoinProjection()
        collisions = {
            column.casefold()
            for column in other._columns
            if column.casefold() not in resolved.right_keys
            and column.casefold() in self._lookup
        }

        if collisions and not (config.allow_collisions or config.suffixes is not None):
            duplicates = ", ".join(
                sorted({column for column in other._columns if column.casefold() in collisions})
            )
            raise ValueError(f"Join would produce duplicate columns: {duplicates}")

        suffix_left = ""
        suffix_right = ""
        if collisions:
            suffix_left, suffix_right = config.suffixes or ("_1", "_2")

        expressions: list[str] = []
        columns: list[str] = []
        types: list[str] = []
        seen: set[str] = set()

        for column, type_name in zip(self._columns, self._types, strict=True):
            output = column
            if column.casefold() in collisions:
                output = f"{column}{suffix_left}"
            lower = output.casefold()
            if lower in seen:
                raise ValueError("Join projection produced duplicate column names")
            seen.add(lower)
            expressions.append(_alias(_qualify("l", column), output))
            columns.append(output)
            types.append(type_name)

        for column, type_name in zip(other._columns, other._types, strict=True):
            if column.casefold() in resolved.right_keys:
                continue
            output = column
            if column.casefold() in collisions:
                output = f"{column}{suffix_right}"
            lower = output.casefold()
            if lower in seen:
                raise ValueError("Join projection produced duplicate column names")
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
                raise TypeError("Join key aliases must map string column names")

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
            raise ValueError("No shared columns to join on")

        left_keys = frozenset(name.casefold() for name, _ in pairs)
        right_keys = frozenset(name.casefold() for _, name in pairs)
        return _ResolvedJoinSpec(pairs=pairs, left_keys=left_keys, right_keys=right_keys, predicates=[])

    def _resolve_join_spec(self, other: DuckRel, spec: JoinSpec) -> _ResolvedJoinSpec:
        """Resolve a :class:`JoinSpec` against the current relation metadata."""

        pairs: list[tuple[str, str]] = []
        left_positions: dict[str, int] = {}
        for left_name, right_name in spec.equal_keys:
            if not isinstance(left_name, str) or not isinstance(right_name, str):
                raise TypeError("JoinSpec.equal_keys must contain string column names")
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
            raise ValueError("Join specification produced no columns or predicates")

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
            raise ValueError("Join requires at least one condition")

        condition = " AND ".join(clauses)
        left_alias = self._relation.set_alias("l")
        right_alias = other._relation.set_alias("r")
        joined = left_alias.join(right_alias, condition, how=how)

        if how in {"semi", "anti"}:
            projection_exprs = _format_projection(self._columns, alias="l")
            relation = joined.project(", ".join(projection_exprs))
            return type(self)(relation, columns=self._columns, types=self._types)

        expressions, columns, types = self._compile_projection(
            other, resolved=resolved, projection=projection
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
            raise ValueError("ASOF order columns must be both temporal or both numeric")

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
            raise ValueError("Tolerance not provided")
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
            other, resolved=resolved.join, projection=projection
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


