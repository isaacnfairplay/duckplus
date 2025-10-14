"""Immutable relational wrapper for Duck+."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Generic, Literal, NamedTuple, TypeVar, cast

import duckdb

from . import util
from .aggregates import AggregateExpression
from .filters import AnyColumnExpression, ColumnExpression, FilterExpression
from .ducktypes import DuckType, Unknown, lookup
from ._core_specs import (
    AsofOrder,
    AsofSpec,
    JoinProjection,
    JoinSpec,
    PartitionSpec,
)
from .materialize import (
    ArrowMaterializeStrategy,
    MaterializeStrategy,
    Materialized,
)
from .schema import AnyRow, ColumnDefinition, DuckSchema

if TYPE_CHECKING:
    from pandas import DataFrame as PandasDataFrame
    from polars import DataFrame as PolarsDataFrame

    from .connect import DuckConnection
    from .relation.core import Relation
else:  # pragma: no cover - runtime aliases
    PandasDataFrame = object
    PolarsDataFrame = object


RowType_co = TypeVar("RowType_co", bound=tuple[Any, ...], covariant=True)
RowType = TypeVar("RowType", bound=tuple[Any, ...])
MarkerT = TypeVar("MarkerT", bound=DuckType, covariant=True)
PythonT = TypeVar("PythonT")


def _quote_identifier(identifier: str) -> str:
    """Return *identifier* quoted for SQL usage."""

    return util.quote_identifier(identifier)


def _qualify(alias: str, column: str) -> str:
    """Return a qualified column reference."""

    return f"{alias}.{_quote_identifier(column)}"


def _alias(expression: str, alias: str) -> str:
    """Return a SQL alias expression."""

    return f"{expression} AS {_quote_identifier(alias)}"


def _ensure_orderable_column(reference: AnyColumnExpression) -> None:
    """Ensure *reference* refers to a comparable column when typed."""

    duck_type = reference.duck_type
    if duck_type is Unknown:
        return
    if duck_type.supports("comparable"):
        return
    raise TypeError(
        "order_by() requires comparable column types; "
        f"column {reference.name!r} is declared as {duck_type.describe()}."
    )


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


def _render_join_filter_expression(
    expression: FilterExpression,
    *,
    left_columns: Sequence[str],
    right_columns: Sequence[str],
) -> str:
    """Render *expression* as a join predicate against the provided columns."""

    assignments: dict[int, str] = {}
    for reference in expression._columns():
        reference_id = id(reference)
        if reference_id in assignments:
            continue

        left_matches = util.resolve_columns([reference.name], left_columns, missing_ok=True)
        right_matches = util.resolve_columns([reference.name], right_columns, missing_ok=True)

        left_match = left_matches[0] if left_matches else None
        right_match = right_matches[0] if right_matches else None

        if left_match and right_match:
            raise ValueError(
                "Join predicate column {name!r} was found in both relations; "
                "rename one side or include it in equal_keys to disambiguate."
                .format(name=reference.name)
            )
        if left_match:
            assignments[reference_id] = _qualify("l", left_match)
            continue
        if right_match:
            assignments[reference_id] = _qualify("r", right_match)
            continue

        raise KeyError(
            "Join predicate column {name!r} was not found in either relation.".format(
                name=reference.name
            )
        )

    def resolver(reference: AnyColumnExpression) -> str:
        qualified = assignments.get(id(reference))
        if qualified is None:
            raise KeyError(
                "Join predicate column {name!r} could not be resolved.".format(name=reference.name)
            )
        return qualified

    return expression._render_with_resolver(resolver)


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


def _resolve_duckdb_connection(
    connection: duckdb.DuckDBPyConnection | "DuckConnection" | None,
) -> duckdb.DuckDBPyConnection | None:
    """Return a raw DuckDB connection from optional wrappers."""

    if connection is None:
        return None
    if isinstance(connection, duckdb.DuckDBPyConnection):
        return connection

    from .connect import DuckConnection

    if isinstance(connection, DuckConnection):
        return connection.raw
    raise TypeError(
        "connection must be a duckdb.DuckDBPyConnection, DuckConnection, or None",
    )


class DuckRel(Generic[RowType_co]):
    """Immutable wrapper around :class:`duckdb.DuckDBPyRelation` with typed helpers.

    Besides exposing the underlying relation via :attr:`relation`, DuckRel provides
    convenience utilities for common metadata queries such as
    :meth:`row_count` to efficiently compute the number of rows.
    """

    __slots__ = ("_relation", "_schema")
    _relation: duckdb.DuckDBPyRelation
    _schema: DuckSchema[AnyRow]

    def __setattr__(self, name: str, value: Any) -> None:  # pragma: no cover - defensive
        if name in self.__slots__ and hasattr(self, name):
            raise AttributeError("DuckRel is immutable")
        super().__setattr__(name, value)

    def __init__(
        self,
        relation: duckdb.DuckDBPyRelation,
        *,
        duck_types: Sequence[type[DuckType]] | None = None,
        schema: DuckSchema[AnyRow] | None = None,
    ) -> None:
        super().__setattr__("_relation", relation)
        if schema is not None:
            if duck_types is not None:
                raise ValueError(
                    "DuckRel() received both a schema and explicit type markers; "
                    "pass only one or the other."
                )
            super().__setattr__("_schema", schema)
            return

        raw_columns = list(relation.columns)
        raw_types = list(_relation_types(relation))
        schema_obj = DuckSchema.from_components(
            raw_columns,
            raw_types,
            duck_types=duck_types,
        )
        super().__setattr__("_schema", schema_obj)

    @property
    def schema(self) -> DuckSchema[RowType_co]:
        """Return the structured schema metadata for the relation."""

        return cast(DuckSchema[RowType_co], self._schema)

    @property
    def row_type(self) -> type[RowType_co]:
        """Return the stored tuple annotation for typed row materialization."""

        return self.schema.row_type

    def typed(
        self: "DuckRel[AnyRow]",
        row_type: type[RowType],
    ) -> "DuckRel[RowType]":
        """Return a wrapper that advertises the provided row tuple annotation."""

        typed_schema = self._schema.typed(row_type)
        return cast("DuckRel[RowType]", type(self)(self._relation, schema=typed_schema))

    @property
    def _columns(self) -> tuple[str, ...]:
        return self._schema.column_names

    @property
    def _types(self) -> tuple[str, ...]:
        return self._schema.duckdb_types

    @property
    def _duck_types(self) -> tuple[type[DuckType], ...]:
        return self._schema.duck_types

    @property
    def _lookup(self) -> Mapping[str, int]:
        return self._schema.lookup


    @property
    def relation(self) -> duckdb.DuckDBPyRelation:
        """Return the underlying relation."""

        return self._relation

    @property
    def columns(self) -> Sequence[str]:
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

    @property
    def column_type_markers(self) -> list[type[DuckType]]:
        """Return the declared DuckDB logical type markers per column."""

        return list(self._duck_types)

    @property
    def column_python_annotations(self) -> list[Any]:
        """Return the stored Python annotations for each projected column."""

        return list(self._schema.python_types)

    def fetch_typed(self) -> list[RowType_co]:
        """Fetch every row as a typed tuple based on stored column metadata.

        The returned tuples always include the relation's full projection. To
        narrow the result set, project or select columns before calling
        :meth:`fetch_typed`.
        """

        rows: list[tuple[Any, ...]] = self._relation.fetchall()
        row_annotation = self._schema.row_type
        try:
            typed_rows = [
                cast(RowType_co, row_annotation(*row)) for row in rows
            ]
        except TypeError:  # pragma: no cover - defensive
            return cast(list[RowType_co], rows)
        return typed_rows

    def _column_index(self, name: str) -> int:
        """Return the index for *name* in the projected column list."""

        return self._lookup[name.casefold()]

    def _marker_for_column(self, name: str) -> type[DuckType]:
        """Return the stored :class:`DuckType` marker for *name*."""

        return self._duck_types[self._column_index(name)]

    def _type_for_column(self, name: str) -> str:
        """Return the DuckDB type string for *name*."""

        return self._types[self._column_index(name)]

    def _annotation_for_column(self, name: str) -> Any:
        """Return the stored Python annotation for *name*."""

        return self._schema.python_type(name)

    def _markers_for_columns(self, columns: Sequence[str]) -> list[type[DuckType]]:
        """Return markers aligned with *columns* in projection order."""

        return [self._marker_for_column(column) for column in columns]

    def _annotations_for_columns(self, columns: Sequence[str]) -> list[Any]:
        """Return Python annotations aligned with *columns* in projection order."""

        return [self._schema.python_type(column) for column in columns]

    def _metadata_from_expression(
        self, expression: ColumnExpression[MarkerT, PythonT], *, context: str
    ) -> tuple[type[MarkerT], PythonT]:
        """Return ``(marker, annotation)`` for *expression* validating declared types."""

        resolved = expression.resolve(self._columns)
        declared: type[MarkerT] = expression.duck_type
        marker_any: type[DuckType] = self._schema.ensure_declared_marker(
            column=resolved,
            declared=declared,
            context=context,
        )
        if marker_any is Unknown:
            marker_any = self._marker_for_column(resolved)

        typed_marker = cast(type[MarkerT], marker_any)
        return typed_marker, cast(PythonT, typed_marker.python_annotation)

    def _wrap_same_schema(
        self, relation: duckdb.DuckDBPyRelation
    ) -> "DuckRel[RowType_co]":
        """Return a :class:`DuckRel` for *relation* preserving schema metadata."""

        return type(self)(relation, schema=self._schema)

    def row_count(self) -> int:
        """Return the total number of rows in the relation as an :class:`int`."""

        result = self.aggregate(__row_count=AggregateExpression.count()).relation.fetchone()
        return int(result[0]) if result and result[0] is not None else 0

    def df(self) -> PandasDataFrame:
        """Return the relation as a pandas DataFrame."""

        util.require_optional_dependency(
            "pandas",
            feature="DuckRel.df()",
            extra="pandas",
        )
        return self._relation.df()

    def pl(self) -> PolarsDataFrame:
        """Return the relation as a Polars DataFrame."""

        util.require_optional_dependency(
            "polars",
            feature="DuckRel.pl()",
            extra="polars",
        )
        return self._relation.pl()

    def show(self) -> "DuckRel[RowType_co]":
        """Render the relation using DuckDB's pretty printer and return ``self``."""

        self._relation.show()
        return self

    def project_columns(
        self, *columns: str, missing_ok: bool = False
    ) -> "DuckRel[AnyRow]":
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
        schema: DuckSchema[AnyRow] = self._schema.select(resolved)
        return type(self)(relation, schema=schema)

    def drop(self, *columns: str, missing_ok: bool = False) -> "DuckRel[AnyRow]":
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
        schema: DuckSchema[AnyRow] = self._schema.select(remaining)
        return type(self)(relation, schema=schema)

    def project(
        self, expressions: Mapping[str, str | AnyColumnExpression]
    ) -> "DuckRel[AnyRow]":
        """Project explicit *expressions* keyed by output column name."""

        if not expressions:
            raise ValueError("project() requires at least one expression mapping.")

        alias_candidates = list(expressions.keys())
        aliases, _ = util.normalize_columns(alias_candidates)
        compiled: list[str] = []
        alias_markers: list[type[DuckType]] = []
        for alias in aliases:
            expression_obj = expressions[alias]
            if isinstance(expression_obj, ColumnExpression):
                expression_sql = expression_obj.render(self._columns)
                marker, _ = self._metadata_from_expression(
                    expression_obj, context="Projection"
                )
            elif isinstance(expression_obj, str):
                if not expression_obj.strip():
                    raise ValueError(
                        "Projection expressions must not be empty; "
                        f"alias {alias!r} received an empty expression."
                )
                expression_sql = expression_obj
                marker = Unknown
            else:
                raise TypeError(
                    "Projection expressions must be strings or ColumnExpression instances; "
                    f"alias {alias!r} mapped to {type(expression_obj).__name__}."
                )
            compiled.append(_alias(expression_sql, alias))
            alias_markers.append(marker)
        relation = self._relation.project(", ".join(compiled))
        types = _relation_types(relation)
        schema: DuckSchema[AnyRow] = DuckSchema.from_components(
            aliases, types, duck_types=alias_markers
        )
        return type(self)(relation, schema=schema)

    def rename_columns(self, **mappings: str) -> "DuckRel[AnyRow]":
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

    def transform_columns(
        self, **expressions: str | AnyColumnExpression
    ) -> "DuckRel[AnyRow]":
        """Replace columns using DuckDB's ``REPLACE`` star modifier."""

        if not expressions:
            raise ValueError("transform_columns() requires at least one column expression.")

        resolved: list[tuple[str, str, type[DuckType]]] = []
        seen: set[str] = set()
        for target, expression in expressions.items():

            resolved_name = util.resolve_columns([target], self._columns)[0]
            key = resolved_name.casefold()
            if key in seen:
                raise ValueError(
                    "transform_columns() received duplicate column names; "
                    f"column {resolved_name!r} mapped multiple times."
                )
            seen.add(key)

            if isinstance(expression, ColumnExpression):
                templated = expression.render(self._columns)
                marker, _ = self._metadata_from_expression(expression, context="transform_columns()")
            elif isinstance(expression, str):
                if not expression.strip():
                    raise ValueError(
                        "transform_columns() expressions must not be empty; "
                        f"column {target!r} received an empty expression."
                    )
                quoted = _quote_identifier(resolved_name)
                templated = expression.replace("{column}", quoted).replace("{col}", quoted)
                marker = Unknown
            else:
                raise TypeError(
                    "transform_columns() expects SQL expressions as strings or ColumnExpression instances; "
                    f"received {type(expression).__name__} for {target!r}."
                )
            resolved.append((resolved_name, templated, marker))

        return self._apply_star_projection(transform_entries=resolved)

    def add_columns(
        self, **expressions: str | AnyColumnExpression
    ) -> "DuckRel[AnyRow]":
        """Add computed columns to the relation using ``*`` projection syntax."""

        if not expressions:
            raise ValueError("add_columns() requires at least one expression mapping.")

        resolved: list[tuple[str, str, type[DuckType]]] = []
        seen: set[str] = set()
        for name, expression in expressions.items():
            key = name.casefold()
            if key in seen:
                raise ValueError(
                    "add_columns() received duplicate column names; "
                    f"column {name!r} mapped multiple times."
                )
            seen.add(key)
            if isinstance(expression, ColumnExpression):
                rendered = expression.render(self._columns)
                marker, _ = self._metadata_from_expression(expression, context="add_columns()")
            elif isinstance(expression, str):
                if not expression.strip():
                    raise ValueError(
                        "add_columns() expressions must not be empty; "
                        f"column {name!r} received an empty expression."
                    )
                rendered = expression
                marker = Unknown
            else:
                raise TypeError(
                    "add_columns() expects SQL expressions as strings or ColumnExpression instances; "
                    f"received {type(expression).__name__} for {name!r}."
                )
            resolved.append((name, rendered, marker))

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

    def filter(
        self, expression: str | FilterExpression, /, *args: Any
    ) -> "DuckRel[AnyRow]":
        """Filter the relation using a SQL *expression* with optional parameters."""

        rendered = self._render_filter_expression(expression, args)
        relation = self._relation.filter(rendered)
        return self._wrap_same_schema(relation)

    def aggregate(
        self,
        *group_columns: str | AnyColumnExpression,
        aggregates: Mapping[str, str | AnyColumnExpression | AggregateExpression] | None = None,
        having_expressions: Sequence[str | FilterExpression] | None = None,
        **aggregate_columns: str | AnyColumnExpression | AggregateExpression,
    ) -> "DuckRel[AnyRow]":
        """Return a grouped aggregate relation.

        Parameters
        ----------
        group_columns:
            Column names or :class:`~duckplus.filters.ColumnExpression` instances used for
            ``GROUP BY`` processing. Names are resolved in a case-insensitive manner
            while preserving their stored casing.
        aggregates, aggregate_columns:
            Mapping of output column names to aggregate SQL fragments,
            :class:`~duckplus.filters.ColumnExpression` instances, or
            :class:`AggregateExpression` helpers. Both the ``aggregates`` mapping
            and keyword arguments contribute to the final projection order.
        having_expressions:
            Optional SQL predicates evaluated as ``HAVING`` filters after
            aggregation. Expressions are combined using ``AND`` semantics.
        """

        if aggregates is not None and not isinstance(aggregates, Mapping):
            raise TypeError(
                "aggregate() expects a mapping for the 'aggregates' argument; "
                f"received {type(aggregates).__name__}."
            )

        combined: list[tuple[str, str | AnyColumnExpression | AggregateExpression]] = []
        if aggregates:
            combined.extend(aggregates.items())
        if aggregate_columns:
            combined.extend(aggregate_columns.items())

        if not combined:
            raise ValueError("aggregate() requires at least one aggregate expression.")

        resolved_groups: list[str] = []
        group_expressions: list[str] = []
        final_markers: list[type[DuckType]] = []
        if group_columns:
            seen_groups: set[str] = set()
            for group in group_columns:
                if isinstance(group, ColumnExpression):
                    resolved_name = group.resolve(self._columns)
                    expression_sql = group.render(self._columns)
                    marker, _ = self._metadata_from_expression(
                        group, context="aggregate() GROUP BY"
                    )
                elif isinstance(group, str):
                    resolved_name = util.resolve_columns([group], self._columns)[0]
                    expression_sql = _quote_identifier(resolved_name)
                    marker = self._marker_for_column(resolved_name)
                else:
                    raise TypeError(
                        "aggregate() group columns must be strings or ColumnExpression instances; "
                        f"received {type(group).__name__}."
                    )

                key = resolved_name.casefold()
                if key in seen_groups:
                    raise ValueError(
                        "aggregate() received duplicate group-by column names; "
                        f"column {resolved_name!r} specified multiple times."
                    )
                seen_groups.add(key)
                resolved_groups.append(resolved_name)
                group_expressions.append(expression_sql)
                final_markers.append(marker)

        final_columns = list(resolved_groups)
        final_sources: list[str | None] = []
        final_sources.extend(resolved_groups)
        selection_parts = list(group_expressions)
        seen_aliases = {name.casefold() for name in resolved_groups}

        for alias, expression in combined:
            if not isinstance(alias, str):
                raise TypeError(
                    "aggregate() alias names must be strings; "
                    f"received {type(alias).__name__}."
                )
            if not alias:
                raise ValueError("aggregate() alias names must not be empty.")

            key = alias.casefold()
            if key in seen_aliases:
                raise ValueError(
                    "aggregate() would produce duplicate column names; "
                    f"alias {alias!r} collides with another column."
                )

            if isinstance(expression, AggregateExpression):
                rendered = expression.render(self._columns)
                marker = expression.duck_type
            elif isinstance(expression, ColumnExpression):
                rendered = expression.render_for_aggregate(self._columns)
                marker, _ = self._metadata_from_expression(
                    expression, context="aggregate() projection"
                )
            elif isinstance(expression, str):
                if not expression.strip():
                    raise ValueError(
                        "aggregate() expressions must not be empty; "
                        f"alias {alias!r} received an empty expression."
                    )
                rendered = expression
                marker = Unknown
            else:
                raise TypeError(
                    "aggregate() expressions must be strings, ColumnExpression instances, or AggregateExpression; "
                    f"alias {alias!r} mapped to {type(expression).__name__}."
                )

            selection_parts.append(_alias(rendered, alias))
            final_columns.append(alias)
            seen_aliases.add(key)
            final_markers.append(marker)
            final_sources.append(None)

        util.ensure_unique_names(final_columns)

        having_clauses: list[str] = []
        if having_expressions is not None:
            if not isinstance(having_expressions, Sequence):
                raise TypeError(
                    "having_expressions must be a sequence of strings or FilterExpression instances; "
                    f"received {type(having_expressions).__name__}."
                )
            for clause in having_expressions:
                if isinstance(clause, FilterExpression):
                    rendered_clause = clause.render(self._columns)
                elif isinstance(clause, str):
                    if not clause.strip():
                        raise ValueError("having_expressions entries must not be empty.")
                    rendered_clause = clause
                else:
                    raise TypeError(
                        "having_expressions must contain strings or FilterExpression instances; "
                        f"received {type(clause).__name__}."
                    )
                having_clauses.append(rendered_clause)

        select_sql = ", ".join(selection_parts)
        source_alias = "__this__"
        query_sql = f"SELECT {select_sql} FROM {source_alias}"

        if group_expressions:
            group_clause = ", ".join(group_expressions)
            query_sql = f"{query_sql} GROUP BY {group_clause}"

        if having_clauses:
            having_sql = " AND ".join(having_clauses)
            query_sql = f"{query_sql} HAVING {having_sql}"

        relation = self._relation.query(source_alias, query_sql)
        types = _relation_types(relation)
        definitions: list[ColumnDefinition] = []
        for name, type_name, marker, source in zip(
            final_columns,
            types,
            final_markers,
            final_sources,
            strict=True,
        ):
            if source is not None:
                base = self._schema.column(source)
                if base.name != name:
                    base = base.renamed(name)
                if base.duck_type is not marker:
                    base = base.with_duck_type(marker)
                if base.duckdb_type != type_name:
                    base = base.with_duckdb_type(type_name)
                definitions.append(base)
            else:
                definitions.append(
                    ColumnDefinition(
                        name=name,
                        duck_type=marker,
                        duckdb_type=type_name,
                        python_type=marker.python_annotation,
                    )
                )

        schema: DuckSchema[AnyRow] = DuckSchema(definitions)
        return type(self)(relation, schema=schema)

    def split(
        self, expression: str | FilterExpression, /, *args: Any
    ) -> tuple[DuckRel[AnyRow], DuckRel[AnyRow]]:
        """Split the relation into matching and non-matching partitions.

        Returns a ``(matching, remainder)`` tuple where the first relation
        contains rows satisfying the provided *expression* and the second holds
        rows where the expression evaluates to ``FALSE`` or ``NULL``.
        """

        rendered = self._render_filter_expression(expression, args)
        matches = self._wrap_same_schema(self._relation.filter(rendered))
        remainder_expression = f"NOT (COALESCE(({rendered}), FALSE))"
        remainder = self._wrap_same_schema(self._relation.filter(remainder_expression))
        return matches, remainder

    def natural_inner(
        self,
        other: DuckRel[AnyRow],
        /,
        *,
        strict: bool = True,
        allow_collisions: bool = False,
        suffixes: tuple[str, str] | None = None,
        **key_aliases: str,
    ) -> "DuckRel[AnyRow]":
        """Perform a natural inner join using shared columns and optional aliases."""

        projection = self._build_projection(allow_collisions=allow_collisions, suffixes=suffixes)
        resolved = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        return self._execute_join(other, how="inner", resolved=resolved, projection=projection)

    def natural_left(
        self,
        other: DuckRel[AnyRow],
        /,
        *,
        strict: bool = True,
        allow_collisions: bool = False,
        suffixes: tuple[str, str] | None = None,
        **key_aliases: str,
    ) -> "DuckRel[AnyRow]":
        """Perform a natural left join using shared columns and optional aliases."""

        projection = self._build_projection(allow_collisions=allow_collisions, suffixes=suffixes)
        resolved = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        return self._execute_join(other, how="left", resolved=resolved, projection=projection)

    def natural_right(
        self,
        other: DuckRel[AnyRow],
        /,
        *,
        strict: bool = True,
        allow_collisions: bool = False,
        suffixes: tuple[str, str] | None = None,
        **key_aliases: str,
    ) -> "DuckRel[AnyRow]":
        """Perform a natural right join using shared columns and optional aliases."""

        projection = self._build_projection(allow_collisions=allow_collisions, suffixes=suffixes)
        resolved = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        return self._execute_join(other, how="right", resolved=resolved, projection=projection)

    def natural_full(
        self,
        other: DuckRel[AnyRow],
        /,
        *,
        strict: bool = True,
        allow_collisions: bool = False,
        suffixes: tuple[str, str] | None = None,
        **key_aliases: str,
    ) -> "DuckRel[AnyRow]":
        """Perform a natural full join using shared columns and optional aliases."""

        projection = self._build_projection(allow_collisions=allow_collisions, suffixes=suffixes)
        resolved = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        return self._execute_join(other, how="outer", resolved=resolved, projection=projection)

    def natural_asof(
        self,
        other: DuckRel[AnyRow],
        /,
        *,
        order: AsofOrder,
        direction: Literal["backward", "forward", "nearest"] = "backward",
        tolerance: str | None = None,
        strict: bool = True,
        allow_collisions: bool = False,
        suffixes: tuple[str, str] | None = None,
        **key_aliases: str,
    ) -> "DuckRel[AnyRow]":
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
        other: DuckRel[AnyRow],
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
    ) -> "DuckRel[AnyRow]":
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
        other: DuckRel[AnyRow],
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
        spec: JoinSpec,
        /,
        *,
        how: Literal["inner", "left", "right", "outer"],
        project: JoinProjection | None = None,
    ) -> "DuckRel[AnyRow]":
        """Join *other* using *spec* while constraining matches to partition columns."""

        partition_spec = self._normalize_partition_spec(partition)
        partition_resolved = self._resolve_join_spec(other, partition_spec)
        join_resolved = self._resolve_join_spec(other, spec)
        combined = self._combine_partition_and_join(partition_resolved, join_resolved)
        return self._execute_join(other, how=how, resolved=combined, projection=project)

    def partitioned_inner(
        self,
        other: DuckRel[AnyRow],
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> "DuckRel[AnyRow]":
        """Perform an inner join constrained by *partition* columns."""

        return self.partitioned_join(other, partition, spec, how="inner", project=project)

    def partitioned_left(
        self,
        other: DuckRel[AnyRow],
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> "DuckRel[AnyRow]":
        """Perform a left outer join constrained by *partition* columns."""

        return self.partitioned_join(other, partition, spec, how="left", project=project)

    def partitioned_right(
        self,
        other: DuckRel[AnyRow],
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> "DuckRel[AnyRow]":
        """Perform a right outer join constrained by *partition* columns."""

        return self.partitioned_join(other, partition, spec, how="right", project=project)

    def partitioned_full(
        self,
        other: DuckRel[AnyRow],
        partition: PartitionSpec | Mapping[str, str] | Sequence[tuple[str, str]],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> "DuckRel[AnyRow]":
        """Perform a full outer join constrained by *partition* columns."""

        return self.partitioned_join(other, partition, spec, how="outer", project=project)

    def left_inner(
        self,
        other: DuckRel[AnyRow],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> "DuckRel[AnyRow]":
        """Perform an inner join against *other* using an explicit :class:`JoinSpec`."""

        resolved = self._resolve_join_spec(other, spec)
        return self._execute_join(other, how="inner", resolved=resolved, projection=project)

    def left_outer(
        self,
        other: DuckRel[AnyRow],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> "DuckRel[AnyRow]":
        """Perform a left outer join against *other* using an explicit :class:`JoinSpec`."""

        resolved = self._resolve_join_spec(other, spec)
        return self._execute_join(other, how="left", resolved=resolved, projection=project)

    def left_right(
        self,
        other: DuckRel[AnyRow],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> "DuckRel[AnyRow]":
        """Perform a right join against *other* using an explicit :class:`JoinSpec`."""

        resolved = self._resolve_join_spec(other, spec)
        return self._execute_join(other, how="right", resolved=resolved, projection=project)

    def inner_join(
        self,
        other: DuckRel[AnyRow],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> "DuckRel[AnyRow]":
        """Perform a symmetric inner join using *spec*."""

        resolved = self._resolve_join_spec(other, spec)
        return self._execute_join(other, how="inner", resolved=resolved, projection=project)

    def outer_join(
        self,
        other: DuckRel[AnyRow],
        spec: JoinSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> "DuckRel[AnyRow]":
        """Perform a full outer join using *spec*."""

        resolved = self._resolve_join_spec(other, spec)
        return self._execute_join(other, how="outer", resolved=resolved, projection=project)

    def asof_join(
        self,
        other: DuckRel[AnyRow],
        spec: AsofSpec,
        /,
        *,
        project: JoinProjection | None = None,
    ) -> "DuckRel[AnyRow]":
        """Perform an ASOF join using the provided :class:`AsofSpec`."""

        resolved = self._resolve_asof_spec(other, spec)
        return self._execute_asof_join(other, resolved=resolved, projection=project)

    def semi_join(
        self,
        other: DuckRel[AnyRow],
        /,
        *,
        strict: bool = True,
        **key_aliases: str,
    ) -> "DuckRel[AnyRow]":
        """Perform a semi join preserving left rows that match *other*."""

        resolved = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        return self._execute_join(other, how="semi", resolved=resolved, projection=None)

    def anti_join(
        self,
        other: DuckRel[AnyRow],
        /,
        *,
        strict: bool = True,
        **key_aliases: str,
    ) -> "DuckRel[AnyRow]":
        """Perform an anti join preserving left rows that do not match *other*."""

        resolved = self._build_natural_join_spec(other, strict=strict, key_aliases=key_aliases)
        return self._execute_join(other, how="anti", resolved=resolved, projection=None)

    def order_by(
        self,
        *orderings: Mapping[str | AnyColumnExpression, str]
        | tuple[str | AnyColumnExpression, str],
        **orders: str,
    ) -> "DuckRel[AnyRow]":
        """Return a relation ordered by the specified *orders* mapping.

        Accepts keyword pairs, ``(column, direction)`` tuples, or mappings where
        columns may be strings or :class:`~duckplus.filters.ColumnExpression`
        instances.
        """

        if not orderings and not orders:
            raise ValueError("order_by() requires at least one column/direction pair.")

        normalized_inputs: list[tuple[str | AnyColumnExpression, str]] = []
        for ordering in orderings:
            if isinstance(ordering, Mapping):
                for column, direction in ordering.items():
                    if not isinstance(column, (str, ColumnExpression)):
                        raise TypeError(
                            "order_by() mapping keys must be strings or ColumnExpression instances; "
                            f"received {type(column).__name__}."
                        )
                    if not isinstance(direction, str):
                        raise TypeError(
                            "Ordering direction must be a string literal 'asc' or 'desc'; "
                            f"received {type(direction).__name__} for column {column!r}."
                        )
                    normalized_inputs.append((column, direction))
                continue
            if not isinstance(ordering, tuple) or len(ordering) != 2:
                raise TypeError(
                    "order_by() positional arguments must be (column, direction) tuples or mappings."
                )
            column, direction = ordering
            if not isinstance(column, (str, ColumnExpression)):
                raise TypeError(
                    "order_by() tuple columns must be strings or ColumnExpression instances; "
                    f"received {type(column).__name__}."
                )
            if not isinstance(direction, str):
                raise TypeError(
                    "Ordering direction must be a string literal 'asc' or 'desc'; "
                    f"received {type(direction).__name__} for column {column!r}."
                )
            normalized_inputs.append((column, direction))

        for column, direction in orders.items():
            normalized_inputs.append((column, direction))

        order_clauses: list[str] = []
        for column, direction in normalized_inputs:
            if isinstance(column, ColumnExpression):
                _ensure_orderable_column(column)
                rendered_column = column.render(self._columns)
            elif isinstance(column, str):
                resolved = util.resolve_columns([column], self._columns)[0]
                rendered_column = _quote_identifier(resolved)
            else:
                raise TypeError(
                    "order_by() keys must be column names or ColumnExpression instances; "
                    f"received {type(column).__name__}."
                )

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
            clause = f"{rendered_column} {normalized.upper()}"
            order_clauses.append(clause)
        relation = self._relation.order(", ".join(order_clauses))
        return self._wrap_same_schema(relation)

    def limit(self, count: int) -> "DuckRel[AnyRow]":
        """Limit the relation to *count* rows."""

        if not isinstance(count, int):
            raise TypeError(
                "limit() expects an integer count; "
                f"received {type(count).__name__}."
            )
        if count < 0:
            raise ValueError(f"limit() requires a non-negative count; received {count}.")
        relation = self._relation.limit(count)
        return self._wrap_same_schema(relation)

    def cast_columns(
        self,
        mapping: Mapping[str, util.DuckDBType] | None = None,
        /,
        **casts: util.DuckDBType,
    ) -> "DuckRel[AnyRow]":
        """Return a relation with specified columns ``CAST`` to DuckDB types."""

        return self._cast_columns("CAST", mapping, casts)

    def try_cast_columns(
        self,
        mapping: Mapping[str, util.DuckDBType] | None = None,
        /,
        **casts: util.DuckDBType,
    ) -> "DuckRel[AnyRow]":
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

        wrapped: DuckRel[AnyRow] | None = None
        if result.relation is not None:
            resolved_columns = (
                tuple(result.columns)
                if result.columns is not None
                else tuple(result.relation.columns)
            )
            types = _relation_types(result.relation)
            subset = self._schema.select(resolved_columns)
            definitions: list[ColumnDefinition] = []
            for definition, type_name in zip(subset.definitions, types, strict=True):
                updated = definition
                if updated.duckdb_type != type_name:
                    updated = updated.with_duckdb_type(type_name)
                definitions.append(updated)
            schema: DuckSchema[AnyRow] = DuckSchema(definitions)
            wrapped = type(self)(result.relation, schema=schema)

        return Materialized(
            table=result.table,
            relation=cast("Relation[AnyRow] | None", wrapped),
            path=result.path,
        )

    # Internal helpers -------------------------------------------------

    def _cast_columns(
        self,
        function: Literal["CAST", "TRY_CAST"],
        mapping: Mapping[str, util.DuckDBType] | None,
        casts: Mapping[str, util.DuckDBType],
    ) -> "DuckRel[AnyRow]":
        provided: dict[str, util.DuckDBType] = {}
        if mapping:
            provided.update(mapping)
        provided.update(casts)

        if not provided:
            raise ValueError("cast_columns()/try_cast_columns() require at least one column mapping.")

        resolved: dict[str, str] = {}
        for requested, provided_type in provided.items():
            if provided_type not in util.DUCKDB_TYPE_SET:
                raise ValueError(f"Unsupported DuckDB type: {provided_type!r}")
            resolved_name = util.resolve_columns([requested], self._columns)[0]
            resolved[resolved_name] = str(provided_type)

        expressions: list[str] = []
        updated_markers: list[type[DuckType]] = []
        for column in self._columns:
            if column not in resolved:
                expressions.append(_alias(_quote_identifier(column), column))
                updated_markers.append(self._marker_for_column(column))
                continue

            cast_type = resolved[column]
            expression = f"{function}({_quote_identifier(column)} AS {cast_type})"
            expressions.append(_alias(expression, column))
            marker = lookup(cast_type)
            updated_markers.append(marker)

        relation = self._relation.project(", ".join(expressions))
        types = _relation_types(relation)
        definitions: list[ColumnDefinition] = []
        for definition, type_name, marker in zip(
            self._schema.definitions,
            types,
            updated_markers,
            strict=True,
        ):
            updated = definition
            if updated.duck_type is not marker:
                updated = updated.with_duck_type(marker)
            if updated.duckdb_type != type_name:
                updated = updated.with_duckdb_type(type_name)
            definitions.append(updated)
        schema: DuckSchema[AnyRow] = DuckSchema(definitions)
        return type(self)(relation, schema=schema)

    def _apply_star_projection(
        self,
        *,
        rename_entries: Sequence[tuple[str, str]] | None = None,
        transform_entries: Sequence[tuple[str, str, type[DuckType]]] | None = None,
        add_entries: Sequence[tuple[str, str, type[DuckType]]] | None = None,
    ) -> "DuckRel[AnyRow]":
        rename_entries = list(rename_entries or [])
        transform_entries = list(transform_entries or [])
        add_entries = list(add_entries or [])

        if not (rename_entries or transform_entries or add_entries):
            raise ValueError("Star projection requires at least one modification.")

        final_columns = list(self._columns)
        final_sources: list[str | None] = []
        final_sources.extend(self._columns)
        final_markers = list(self._duck_types)
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
                for original, expression, _ in transform_entries
            ]

        seen = {name.casefold() for name in final_columns}
        add_sql: list[str] = []
        for name, expression, marker in add_entries:
            key = name.casefold()
            if key in seen:
                raise ValueError(
                    "add_columns() would create duplicate column name; "
                    f"column {name!r} already exists."
                )
            seen.add(key)
            add_sql.append(f"({expression}) AS {_quote_identifier(name)}")
            final_columns.append(name)
            final_sources.append(None)
            final_markers.append(marker)

        star_parts = ["*"]
        if rename_sql:
            star_parts.append(f"RENAME ({', '.join(rename_sql)})")
        if replace_sql:
            star_parts.append(f"REPLACE ({', '.join(replace_sql)})")
        star_expr = " ".join(star_parts)

        projection_parts = [star_expr, *add_sql]
        projection_sql = ", ".join(part for part in projection_parts if part)
        relation = self._relation.project(projection_sql)
        for original, _, marker in transform_entries:
            index = self._lookup[original.casefold()]
            final_markers[index] = marker

        types = _relation_types(relation)
        definitions: list[ColumnDefinition] = []
        for name, type_name, marker, source in zip(
            final_columns,
            types,
            final_markers,
            final_sources,
            strict=True,
        ):
            if source is not None:
                base = self._schema.column(source)
                if base.name != name:
                    base = base.renamed(name)
                if base.duck_type is not marker:
                    base = base.with_duck_type(marker)
                if base.duckdb_type != type_name:
                    base = base.with_duckdb_type(type_name)
                definitions.append(base)
            else:
                definitions.append(
                    ColumnDefinition(
                        name=name,
                        duck_type=marker,
                        duckdb_type=type_name,
                        python_type=marker.python_annotation,
                    )
                )

        schema: DuckSchema[AnyRow] = DuckSchema(definitions)
        return type(self)(relation, schema=schema)

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
        other: DuckRel[AnyRow],
        *,
        resolved: _ResolvedJoinSpec,
        projection: JoinProjection | None,
        include_right_keys: bool,
    ) -> tuple[
        list[str],
        list[str],
        list[str],
        list[type[DuckType]],
        list[tuple[Literal["left", "right"], str]],
    ]:
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
        duck_types: list[type[DuckType]] = []
        sources: list[tuple[Literal["left", "right"], str]] = []
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
            duck_types.append(self._duck_types[self._lookup[key]])
            sources.append(("left", column))

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
            duck_types.append(other._duck_types[other._lookup[key]])
            sources.append(("right", column))

        return expressions, columns, types, duck_types, sources

    def _build_natural_join_spec(
        self,
        other: DuckRel[AnyRow],
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

    def _resolve_join_spec(
        self, other: DuckRel[AnyRow], spec: JoinSpec
    ) -> _ResolvedJoinSpec:
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
            if isinstance(predicate, FilterExpression):
                predicates.append(
                    _render_join_filter_expression(
                        predicate,
                        left_columns=self._columns,
                        right_columns=other._columns,
                    )
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
        other: DuckRel[AnyRow],
        *,
        how: str,
        resolved: _ResolvedJoinSpec,
        projection: JoinProjection | None,
    ) -> "DuckRel[AnyRow]":
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
            return self._wrap_same_schema(relation)

        (
            expressions,
            columns,
            types,
            duck_types,
            sources,
        ) = self._compile_projection(
            other,
            resolved=resolved,
            projection=projection,
            include_right_keys=how in {"right", "outer"},
        )
        relation = joined.project(", ".join(expressions))
        actual_types = _relation_types(relation)
        definitions: list[ColumnDefinition] = []
        for name, type_name, marker, (side, source_name) in zip(
            columns,
            actual_types,
            duck_types,
            sources,
            strict=True,
        ):
            base_schema = self._schema if side == "left" else other._schema
            base = base_schema.column(source_name)
            if base.name != name:
                base = base.renamed(name)
            if base.duck_type is not marker:
                base = base.with_duck_type(marker)
            if base.duckdb_type != type_name:
                base = base.with_duckdb_type(type_name)
            definitions.append(base)

        schema: DuckSchema[AnyRow] = DuckSchema(definitions)
        return type(self)(relation, schema=schema)

    def _resolve_asof_spec(
        self, other: DuckRel[AnyRow], spec: AsofSpec
    ) -> _ResolvedAsofSpec:
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
        other: DuckRel[AnyRow],
        *,
        resolved: _ResolvedAsofSpec,
        projection: JoinProjection | None,
    ) -> "DuckRel[AnyRow]":
        (
            expressions,
            columns,
            types,
            duck_types,
            sources,
        ) = self._compile_projection(
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
        actual_types = _relation_types(relation)
        definitions: list[ColumnDefinition] = []
        for name, type_name, marker, (side, source_name) in zip(
            columns,
            actual_types,
            duck_types,
            sources,
            strict=True,
        ):
            base_schema = self._schema if side == "left" else other._schema
            base = base_schema.column(source_name)
            if base.name != name:
                base = base.renamed(name)
            if base.duck_type is not marker:
                base = base.with_duck_type(marker)
            if base.duckdb_type != type_name:
                base = base.with_duckdb_type(type_name)
            definitions.append(base)

        schema: DuckSchema[AnyRow] = DuckSchema(definitions)
        return type(self)(relation, schema=schema)

    @classmethod
    def from_pandas(
        cls,
        frame: PandasDataFrame,
        *,
        connection: duckdb.DuckDBPyConnection | "DuckConnection" | None = None,
    ) -> "DuckRel[AnyRow]":
        """Create a :class:`DuckRel` from a pandas DataFrame."""

        util.require_optional_dependency(
            "pandas",
            feature="DuckRel.from_pandas()",
            extra="pandas",
        )
        raw = _resolve_duckdb_connection(connection)
        relation = (
            duckdb.from_df(frame)
            if raw is None
            else raw.from_df(frame)
        )
        return cls(relation)

    @classmethod
    def from_polars(
        cls,
        frame: PolarsDataFrame,
        *,
        connection: duckdb.DuckDBPyConnection | "DuckConnection" | None = None,
    ) -> "DuckRel[AnyRow]":
        """Create a :class:`DuckRel` from a Polars DataFrame."""

        util.require_optional_dependency(
            "polars",
            feature="DuckRel.from_polars()",
            extra="polars",
        )
        raw = _resolve_duckdb_connection(connection)
        arrow_table = frame.to_arrow()
        relation = (
            duckdb.from_arrow(arrow_table)
            if raw is None
            else raw.from_arrow(arrow_table)
        )
        return cls(relation)
