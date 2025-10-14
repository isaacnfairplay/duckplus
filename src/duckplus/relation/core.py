"""High level relational wrapper offering fluent expression builders."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Generic, LiteralString, TypeVar, cast, overload

from .. import util
from ..aggregates import AggregateArgument, AggregateExpression
from ..duckrel import DuckRel
from ..ducktypes import Boolean, DuckType, Unknown, lookup
from ..filters import ColumnExpression, FilterExpression
from ..schema import AnyRow, ColumnDefinition, DuckSchema

__all__ = ["Expression", "Relation", "RelationColumnSet"]


RowSetT = TypeVar("RowSetT", bound=AnyRow, covariant=True)


def _quote(column: ColumnDefinition) -> str:
    return util.quote_identifier(column.name)


def _format_literal(value: Any) -> str:
    return util.format_sql_literal(util.coerce_scalar(value))


class _OrderSpec:
    __slots__ = ("expression", "direction")

    def __init__(self, expression: "Expression[Any]", direction: str) -> None:
        self.expression = expression
        self.direction = direction

    def to_tuple(self) -> tuple[str | ColumnExpression[Any, Any], str]:
        return self.expression._to_projection(), self.direction


PythonT_co = TypeVar("PythonT_co", covariant=True)


class Expression(Generic[PythonT_co]):
    __slots__ = ("_sql", "_column", "_marker", "_annotation", "_schema")

    def __init__(
        self,
        sql: str,
        *,
        schema: DuckSchema[Any] | None,
        column: ColumnExpression[Any, Any] | None = None,
        marker: type[DuckType] = Unknown,
        annotation: Any = Any,
    ) -> None:
        self._sql = sql
        self._column = column
        self._marker = marker
        self._annotation = annotation
        self._schema = schema

    @property
    def sql(self) -> str:
        return self._sql

    @property
    def marker(self) -> type[DuckType]:
        return self._marker

    @property
    def python_annotation(self) -> Any:
        return self._annotation

    def _require_schema(self) -> DuckSchema[Any]:
        if self._schema is None:
            raise TypeError("Expression operations require schema context.")
        return self._schema

    def _binary(self, other: Any, operator: str, *, reverse: bool = False) -> "Expression[Any]":
        schema = self._require_schema()
        left, right = (self, self._coerce(other))
        if reverse:
            left, right = right, left
        sql = f"({left.sql}) {operator} ({right.sql})"
        return Expression(sql, schema=schema)

    def _compare(self, other: Any, operator: str, *, reverse: bool = False) -> FilterExpression:
        schema = self._require_schema()
        left, right = (self, self._coerce(other))
        if reverse:
            left, right = right, left
        sql = f"({left.sql}) {operator} ({right.sql})"
        # Comparisons always yield booleans; validation already occurred during coercion.
        return FilterExpression.raw(sql)

    def _coerce(self, value: Any) -> "Expression[Any]":
        schema = self._require_schema()
        if isinstance(value, Expression):
            if value._schema is None:
                return Expression(
                    value.sql,
                    schema=schema,
                    column=value._column,
                    marker=value.marker,
                    annotation=value.python_annotation,
                )
            if value._schema is not schema:
                raise TypeError("Expressions from different schemas cannot be combined.")
            return value
        if isinstance(value, ColumnExpression):
            return Expression.from_column_expression(value, schema)
        if isinstance(value, FilterExpression):
            return Expression.from_filter(value, schema)
        return Expression.literal(value, schema=schema)

    def __add__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "+")

    def __radd__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "+", reverse=True)

    def __sub__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "-")

    def __rsub__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "-", reverse=True)

    def __mul__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "*")

    def __rmul__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "*", reverse=True)

    def __truediv__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "/")

    def __rtruediv__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "/", reverse=True)

    def __floordiv__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "//")

    def __rfloordiv__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "//", reverse=True)

    def __mod__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "%")

    def __rmod__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "%", reverse=True)

    def __pow__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "^")

    def __rpow__(self, other: Any) -> "Expression[Any]":
        return self._binary(other, "^", reverse=True)

    def __neg__(self) -> "Expression[Any]":
        schema = self._schema
        return Expression(f"-({self._sql})", schema=schema, marker=self._marker, annotation=self._annotation)

    def __pos__(self) -> "Expression[Any]":
        schema = self._schema
        return Expression(f"+({self._sql})", schema=schema, marker=self._marker, annotation=self._annotation)

    def __eq__(self, other: Any) -> FilterExpression:  # type: ignore[override]
        return self._compare(other, "=")

    def __ne__(self, other: Any) -> FilterExpression:  # type: ignore[override]
        return self._compare(other, "!=")

    def __lt__(self, other: Any) -> FilterExpression:
        return self._compare(other, "<")

    def __le__(self, other: Any) -> FilterExpression:
        return self._compare(other, "<=")

    def __gt__(self, other: Any) -> FilterExpression:
        return self._compare(other, ">")

    def __ge__(self, other: Any) -> FilterExpression:
        return self._compare(other, ">=")

    def asc(self) -> _OrderSpec:
        return _OrderSpec(self, "asc")

    def desc(self) -> _OrderSpec:
        return _OrderSpec(self, "desc")

    @property
    def is_column(self) -> bool:
        return self._column is not None

    def _to_projection(self) -> str | ColumnExpression[Any, Any]:
        if self._column is not None:
            return self._column
        return self._sql

    def as_aggregate_argument(self) -> AggregateArgument:
        projection = self._to_projection()
        if isinstance(projection, ColumnExpression):
            return AggregateArgument.column(projection)
        return AggregateArgument.raw(self._sql)

    @classmethod
    def from_column(
        cls, definition: ColumnDefinition, schema: DuckSchema[Any]
    ) -> "Expression[Any]":
        column: ColumnExpression[Any, Any] = ColumnExpression(
            definition.name,
            duck_type=definition.duck_type,
        )
        marker, annotation = Relation._metadata_from_column_expression(
            schema,
            column,
            context="expression",
        )
        return cls(
            _quote(definition),
            schema=schema,
            column=column,
            marker=marker,
            annotation=annotation,
        )

    @classmethod
    def from_column_expression(
        cls, expression: ColumnExpression[Any, Any], schema: DuckSchema[Any]
    ) -> "Expression[Any]":
        rendered = expression.render(schema.column_names)
        marker, annotation = Relation._metadata_from_column_expression(
            schema,
            expression,
            context="expression",
        )
        return cls(
            rendered,
            schema=schema,
            column=expression,
            marker=marker,
            annotation=annotation,
        )

    @classmethod
    def from_filter(
        cls, expression: FilterExpression, schema: DuckSchema[Any]
    ) -> "Expression[bool]":
        rendered = expression.render(schema.column_names)
        return Expression(
            rendered,
            schema=schema,
            marker=Boolean,
            annotation=bool,
        )

    @classmethod
    def literal(cls, value: Any, *, schema: DuckSchema[Any]) -> "Expression[Any]":
        return cls(
            _format_literal(value),
            schema=schema,
            marker=Unknown,
            annotation=type(value) if value is not None else type(None),
        )

    @classmethod
    def raw(
        cls, expression: str, *, schema: DuckSchema[Any] | None = None
    ) -> "Expression[Any]":
        if not isinstance(expression, str):
            raise TypeError(
                "Raw expressions must be provided as strings; "
                f"received {type(expression).__name__}."
            )
        if not expression.strip():
            raise ValueError("Raw expression must not be empty.")
        return cls(expression, schema=schema)

    @classmethod
    def boolean(
        cls, expression: str, *, schema: DuckSchema[Any]
    ) -> "Expression[bool]":
        return Expression(
            expression,
            schema=schema,
            marker=Boolean,
            annotation=bool,
        )


class RelationColumnSet(Sequence[str], Generic[RowSetT]):
    """Sequence-like descriptor exposing schema backed column expressions."""

    __slots__ = ("_schema",)

    def __init__(self, schema: DuckSchema[RowSetT]) -> None:
        self._schema = schema

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._schema.column_names)

    def __iter__(self) -> Iterator[str]:  # pragma: no cover - trivial
        return iter(self._schema.column_names)

    @overload
    def __getitem__(self, key: int) -> str:
        ...

    @overload
    def __getitem__(self, key: slice) -> list[str]:
        ...

    @overload
    def __getitem__(self, key: LiteralString) -> Expression[Any]:
        ...

    def __getitem__(self, key: int | slice | str) -> str | list[str] | Expression[Any]:
        if isinstance(key, slice):
            return list(self._schema.column_names[key])
        if isinstance(key, int):
            return self._schema.column_names[key]
        if isinstance(key, str):
            definition = self._schema.column(key)
            return Expression.from_column(definition, self._schema)
        raise TypeError("Column lookups must use indexes or column names.")

    def __contains__(self, name: object) -> bool:  # pragma: no cover - trivial
        if not isinstance(name, str):
            return False
        return name.casefold() in self._schema.lookup

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"RelationColumnSet({list(self)!r})"

    def __eq__(self, other: object) -> bool:  # pragma: no cover - trivial
        if isinstance(other, RelationColumnSet):
            return tuple(self) == tuple(other)
        if isinstance(other, Sequence):
            return list(self) == list(other)
        return NotImplemented

    def __getattr__(self, name: str) -> Expression[Any]:
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            return self[name]
        except KeyError as exc:  # pragma: no cover - defensive path
            raise AttributeError(name) from exc

    def literal(self, value: Any) -> Expression[Any]:
        return Expression.literal(value, schema=self._schema)

    def raw(self, expression: str) -> Expression[Any]:
        return Expression.raw(expression, schema=self._schema)

    @property
    def schema(self) -> DuckSchema[RowSetT]:  # pragma: no cover - trivial
        return self._schema


class AggregateContext:
    __slots__ = ("_relation", "columns", "c")

    def __init__(self, relation: "Relation[AnyRow]") -> None:
        self._relation = relation
        column_set = relation._column_context()
        self.columns = column_set
        self.c = column_set

    def _resolve(
        self,
        value: Expression[Any]
        | ColumnExpression[Any, Any]
        | AggregateArgument
        | FilterExpression
        | str,
    ) -> AggregateArgument:
        if isinstance(value, AggregateArgument):
            return value
        expr = self._relation._resolve_expression(value)
        return expr.as_aggregate_argument()

    def count(
        self,
        value: Expression[Any]
        | ColumnExpression[Any, Any]
        | AggregateArgument
        | FilterExpression
        | str
        | None = None,
        *,
        distinct: bool = False,
    ) -> AggregateExpression:
        if value is None:
            return AggregateExpression.count(distinct=distinct)
        return AggregateExpression.count(self._resolve(value), distinct=distinct)

    def sum(
        self,
        value: Expression[Any]
        | ColumnExpression[Any, Any]
        | AggregateArgument
        | FilterExpression
        | str,
    ) -> AggregateExpression:
        return AggregateExpression.sum(self._resolve(value))

    def avg(
        self,
        value: Expression[Any]
        | ColumnExpression[Any, Any]
        | AggregateArgument
        | FilterExpression
        | str,
    ) -> AggregateExpression:
        return AggregateExpression.avg(self._resolve(value))

    def min(
        self,
        value: Expression[Any]
        | ColumnExpression[Any, Any]
        | AggregateArgument
        | FilterExpression
        | str,
    ) -> AggregateExpression:
        return AggregateExpression.min(self._resolve(value))

    def max(
        self,
        value: Expression[Any]
        | ColumnExpression[Any, Any]
        | AggregateArgument
        | FilterExpression
        | str,
    ) -> AggregateExpression:
        return AggregateExpression.max(self._resolve(value))

    def function(
        self,
        name: str,
        *arguments: Expression[Any]
        | ColumnExpression[Any, Any]
        | AggregateArgument
        | FilterExpression
        | str,
    ) -> AggregateExpression:
        resolved = [self._resolve(argument) for argument in arguments]
        return AggregateExpression.function(name, *resolved)


RowT = TypeVar("RowT", bound=AnyRow, covariant=True)


class Relation(DuckRel[RowT], Generic[RowT]):
    """Immutable relational wrapper with fluent expression helpers."""

    def _column_context(self) -> RelationColumnSet[RowT]:
        return RelationColumnSet(self.schema)

    @property
    def columns(self) -> RelationColumnSet[RowT]:
        return self._column_context()

    @property
    def c(self) -> RelationColumnSet[RowT]:
        return self._column_context()

    @staticmethod
    def _metadata_from_column_expression(
        schema: DuckSchema[Any],
        expression: ColumnExpression[Any, Any],
        *,
        context: str,
    ) -> tuple[type[DuckType], Any]:
        resolved = expression.resolve(schema.column_names)
        declared: type[DuckType] = expression.duck_type
        marker: type[DuckType] = schema.ensure_declared_marker(
            column=resolved,
            declared=declared,
            context=context,
        )
        if marker is Unknown:
            marker = lookup(schema.duckdb_type(resolved))
        return marker, marker.python_annotation

    def _resolve_expression(self, value: Any) -> Expression[Any]:
        if callable(value):
            return self._resolve_expression(value(self._column_context()))
        if isinstance(value, Expression):
            if value._schema is None:
                return Expression(
                    value.sql,
                    schema=self.schema,
                    column=value._column,
                    marker=value.marker,
                    annotation=value.python_annotation,
                )
            if value._schema is not self.schema:
                raise TypeError("Expressions can only be reused with the originating relation schema.")
            return value
        if isinstance(value, ColumnExpression):
            return Expression.from_column_expression(value, self.schema)
        if isinstance(value, FilterExpression):
            return Expression.from_filter(value, self.schema)
        if isinstance(value, str):
            if value.casefold() in self.schema.lookup:
                definition = self.schema.column(value)
                return Expression.from_column(definition, self.schema)
            raise TypeError(
                "String expressions must reference projected columns; use Expression.raw() for SQL fragments."
            )
        return Expression.literal(value, schema=self.schema)

    def _prepare_projection(self, value: Any) -> str | ColumnExpression[Any, Any]:
        expression = self._resolve_expression(value)
        return expression._to_projection()

    def _coerce_mapping(self, mapping: Mapping[str, Any]) -> OrderedDict[str, Any]:
        ordered: OrderedDict[str, Any] = OrderedDict()
        for key, value in mapping.items():
            ordered[key] = self._prepare_projection(value)
        return ordered

    def _coerce_aggregates(self, mapping: Mapping[str, Any]) -> OrderedDict[str, Any]:
        context = AggregateContext(self)
        ordered: OrderedDict[str, Any] = OrderedDict()
        for key, value in mapping.items():
            resolved = value(context) if callable(value) else value
            if isinstance(resolved, AggregateExpression):
                ordered[key] = resolved
            else:
                ordered[key] = self._prepare_projection(resolved)
        return ordered

    def _coerce_having(self, value: Any) -> str | FilterExpression:
        expression = value(self._column_context()) if callable(value) else value
        if isinstance(expression, FilterExpression):
            return expression
        resolved = self._resolve_expression(expression)
        return resolved.sql

    def select(self, *columns: Any, **derived: Any) -> "Relation[AnyRow]":
        mapping: OrderedDict[str, Any] = OrderedDict()
        for column in columns:
            if callable(column):
                normalized = column(self._column_context())
                mapping.update(self._normalize_select_entry(normalized))
                continue
            expression = self._prepare_projection(column)
            if isinstance(column, str):
                resolved = self.schema.resolve([column])[0]
                mapping[resolved] = expression
            elif isinstance(expression, ColumnExpression):
                mapping[expression.name] = expression
            else:
                raise TypeError(
                    "Positional select expressions must be column names or mappings."  # noqa: TRY003
                )
        if derived:
            mapping.update(self._coerce_mapping(derived))
        if not mapping:
            raise ValueError("select() requires at least one column or expression.")
        return cast(Relation[AnyRow], DuckRel.project(self, mapping))

    def _normalize_select_entry(self, value: Any) -> OrderedDict[str, Any]:
        if isinstance(value, Mapping):
            return self._coerce_mapping(value)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            result: OrderedDict[str, Any] = OrderedDict()
            for entry in value:
                if isinstance(entry, tuple) and len(entry) == 2:
                    alias, expression = entry
                    result[str(alias)] = self._prepare_projection(expression)
                else:
                    prepared = self._prepare_projection(entry)
                    if isinstance(entry, str):
                        resolved = self.schema.resolve([entry])[0]
                        result[resolved] = prepared
                    elif isinstance(prepared, ColumnExpression):
                        result[prepared.name] = prepared
                    else:
                        raise TypeError(
                            "Sequence select entries must be names or (alias, expression) tuples."
                        )
            return result
        raise TypeError("Callable select entries must return a mapping or sequence.")

    def rename(self, **mappings: str) -> "Relation[AnyRow]":
        return cast(Relation[AnyRow], DuckRel.rename_columns(self, **mappings))

    def mutate(self, **expressions: Any) -> "Relation[AnyRow]":
        if not expressions:
            raise ValueError("mutate() requires at least one expression.")
        lookup = self.schema.lookup
        transforms: dict[str, Any] = {}
        additions: dict[str, Any] = {}
        for name, expression in expressions.items():
            target = self._prepare_projection(expression)
            if name.casefold() in lookup:
                transforms[name] = target
            else:
                additions[name] = target
        result: Relation[AnyRow] = self
        if transforms:
            result = cast(Relation[AnyRow], DuckRel.transform_columns(result, **transforms))
        if additions:
            result = cast(Relation[AnyRow], DuckRel.add_columns(result, **additions))
        return result

    def aggregate(
        self,
        *groups: Any,
        aggregates: Mapping[str, Any] | None = None,
        having_expressions: Sequence[Any] | None = None,
        **named: Any,
    ) -> "Relation[AnyRow]":
        combined: OrderedDict[str, Any] = OrderedDict()
        if aggregates:
            combined.update(self._coerce_aggregates(aggregates))
        if named:
            combined.update(self._coerce_aggregates(named))
        if not combined:
            raise ValueError("aggregate() requires at least one aggregation expression.")

        group_columns: list[str | ColumnExpression[Any, Any]] = []
        for group in groups:
            prepared = self._prepare_projection(group)
            if isinstance(group, str):
                group_columns.append(self.schema.resolve([group])[0])
            else:
                group_columns.append(prepared)

        having: list[str | FilterExpression] | None = None
        if having_expressions:
            having = [self._coerce_having(entry) for entry in having_expressions]

        return cast(
            Relation[AnyRow],
            DuckRel.aggregate(
                self,
                *group_columns,
                aggregates=combined,
                having_expressions=having,
            ),
        )

    def order_by(self, *orderings: Any, **named: str) -> "Relation[RowT]":
        if not orderings and not named:
            raise ValueError("order_by() requires at least one ordering expression.")

        context = self._column_context()
        normalized: list[tuple[str | ColumnExpression[Any, Any], str]] = []

        for ordering in orderings:
            candidate = ordering(context) if callable(ordering) else ordering
            normalized.extend(self._normalize_order_candidate(candidate))

        if named:
            for key, direction in named.items():
                expression = self._prepare_projection(key)
                normalized.append((expression, direction))

        return cast(Relation[RowT], DuckRel.order_by(self, *normalized))

    def _normalize_order_candidate(
        self,
        candidate: Any,
    ) -> list[tuple[str | ColumnExpression[Any, Any], str]]:
        if isinstance(candidate, _OrderSpec):
            return [candidate.to_tuple()]
        if isinstance(candidate, Mapping):
            entries: list[tuple[str | ColumnExpression[Any, Any], str]] = []
            for key, direction in candidate.items():
                expression = self._prepare_projection(key)
                entries.append((expression, direction))
            return entries
        if isinstance(candidate, (list, tuple)):
            if len(candidate) != 2:
                raise ValueError("order_by() tuples must contain (expression, direction).")
            expression, direction = candidate
            prepared = self._prepare_projection(expression)
            return [(prepared, direction)]
        if isinstance(candidate, str):
            resolved = self.schema.resolve([candidate])[0]
            return [(resolved, "asc")]
        expression = self._prepare_projection(candidate)
        return [(expression, "asc")]

    def where(self, predicate: Any) -> "Relation[RowT]":
        expression = predicate(self._column_context()) if callable(predicate) else predicate
        if isinstance(expression, FilterExpression):
            return cast(Relation[RowT], DuckRel.filter(self, expression))
        resolved = self._resolve_expression(expression)
        return cast(Relation[RowT], DuckRel.filter(self, resolved.sql))

