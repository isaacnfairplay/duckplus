"""Schema metadata utilities for DuckRel."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import count
from types import MappingProxyType
from typing import (
    Any,
    Iterable,
    Iterator,
    Mapping,
    NamedTuple,
    Sequence,
    TypeVar,
    cast,
    Generic,
    overload,
)

from . import util
from .ducktypes import DuckType, Unknown, lookup

__all__ = ["AnyRow", "ColumnDefinition", "DuckSchema"]

AnyRow = tuple[Any, ...]
RowType_co = TypeVar("RowType_co", bound=tuple[Any, ...], covariant=True)
RowType = TypeVar("RowType", bound=tuple[Any, ...])
MarkerT = TypeVar("MarkerT", bound=DuckType)

_ROW_ALIAS_COUNTER = count()


def _build_row_alias(
    names: Sequence[str], python_types: Sequence[Any]
) -> type[AnyRow]:
    """Return a unique ``NamedTuple`` describing the tuple row annotation."""

    alias_name = f"DuckSchemaRow_{next(_ROW_ALIAS_COUNTER)}"
    fields: list[tuple[str, Any]] = []
    for index, (name, annotation) in enumerate(
        zip(names, python_types, strict=True)
    ):
        if name.isidentifier() and not name.startswith("_"):
            field_name = name
        else:
            field_name = f"column_{index}"
        fields.append((field_name, annotation))
    return cast(type[AnyRow], NamedTuple(alias_name, fields))


@dataclass(frozen=True, slots=True)
class ColumnDefinition:
    """Describe a single column within a :class:`DuckSchema`."""

    name: str
    duck_type: type[DuckType]
    duckdb_type: str
    python_type: Any
    origin: Any | None = None

    def renamed(self, name: str) -> ColumnDefinition:
        """Return a copy of the definition with ``name`` updated."""

        return ColumnDefinition(
            name=name,
            duck_type=self.duck_type,
            duckdb_type=self.duckdb_type,
            python_type=self.python_type,
            origin=self.origin,
        )

    def with_duck_type(self, duck_type: type[DuckType]) -> ColumnDefinition:
        """Return a copy of the definition with a new :class:`DuckType`."""

        return ColumnDefinition(
            name=self.name,
            duck_type=duck_type,
            duckdb_type=self.duckdb_type,
            python_type=duck_type.python_annotation,
            origin=self.origin,
        )

    def with_duckdb_type(
        self, duckdb_type: str, *, duck_type: type[DuckType] | None = None
    ) -> ColumnDefinition:
        """Return a copy with updated DuckDB type and optional logical marker."""

        marker = duck_type or self.duck_type
        return ColumnDefinition(
            name=self.name,
            duck_type=marker,
            duckdb_type=duckdb_type,
            python_type=marker.python_annotation,
            origin=self.origin,
        )


class DuckSchema(Generic[RowType_co]):
    """Ordered, case-insensitive schema representation."""

    __slots__ = (
        "_definitions",
        "_names",
        "_lookup",
        "_duckdb_types",
        "_duck_types",
        "_python_types",
        "_row_type",
    )

    _definitions: tuple[ColumnDefinition, ...]
    _names: tuple[str, ...]
    _lookup: Mapping[str, int]
    _duckdb_types: tuple[str, ...]
    _duck_types: tuple[type[DuckType], ...]
    _python_types: tuple[Any, ...]
    _row_type: type[AnyRow]

    def __init__(
        self,
        definitions: Sequence[ColumnDefinition],
        *,
        row_type: type[RowType_co] | None = None,
    ) -> None:
        normalized_names, lookup = util.normalize_columns(
            [definition.name for definition in definitions]
        )
        canonical: list[ColumnDefinition] = []
        for index, definition in enumerate(definitions):
            normalized_name = normalized_names[index]
            if definition.name != normalized_name:
                canonical.append(definition.renamed(normalized_name))
            else:
                canonical.append(definition)

        self._definitions = tuple(canonical)
        self._names = tuple(normalized_names)
        self._lookup = MappingProxyType(dict(lookup))
        self._duckdb_types = tuple(definition.duckdb_type for definition in canonical)
        self._duck_types = tuple(definition.duck_type for definition in canonical)
        self._python_types = tuple(definition.python_type for definition in canonical)
        if row_type is None:
            inferred = _build_row_alias(self._names, self._python_types)
            self._row_type = inferred
        else:
            self._row_type = cast(type[AnyRow], row_type)

    @overload
    @classmethod
    def from_components(
        cls,
        columns: Sequence[str],
        types: Sequence[str],
        *,
        duck_types: Sequence[type[DuckType]] | None = None,
        origins: Sequence[Any | None] | None = None,
        row_type: type[RowType],
    ) -> DuckSchema[RowType]:
        ...

    @overload
    @classmethod
    def from_components(
        cls,
        columns: Sequence[str],
        types: Sequence[str],
        *,
        duck_types: Sequence[type[DuckType]] | None = None,
        origins: Sequence[Any | None] | None = None,
        row_type: None = None,
    ) -> DuckSchema[AnyRow]:
        ...

    @classmethod
    def from_components(
        cls,
        columns: Sequence[str],
        types: Sequence[str],
        *,
        duck_types: Sequence[type[DuckType]] | None = None,
        origins: Sequence[Any | None] | None = None,
        row_type: type[RowType] | None = None,
    ) -> DuckSchema[AnyRow]:
        """Return a schema constructed from column metadata components."""

        normalized_columns, _ = util.normalize_columns(columns)
        if len(types) != len(normalized_columns):
            raise ValueError(
                "Number of column types does not match the projected columns; "
                f"expected {len(normalized_columns)} types but received {len(types)}."
            )

        if duck_types is None:
            resolved_markers: list[type[DuckType]] = [Unknown for _ in normalized_columns]
        else:
            resolved_markers = list(duck_types)
            if len(resolved_markers) != len(normalized_columns):
                raise ValueError(
                    "Number of column type markers does not match the projected columns; "
                    f"expected {len(normalized_columns)} markers but received {len(resolved_markers)}."
                )

        if origins is None:
            resolved_origins: Iterable[Any | None] = (None for _ in normalized_columns)
        else:
            resolved_origins = list(origins)
            if len(resolved_origins) != len(normalized_columns):
                raise ValueError(
                    "Number of column origins does not match the projected columns; "
                    f"expected {len(normalized_columns)} origins but received {len(resolved_origins)}."
                )

        definitions = [
            ColumnDefinition(
                name=name,
                duck_type=marker,
                duckdb_type=type_name,
                python_type=marker.python_annotation,
                origin=origin,
            )
            for name, marker, type_name, origin in zip(
                normalized_columns,
                resolved_markers,
                types,
                resolved_origins,
                strict=True,
            )
        ]
        schema = cls(definitions)
        if row_type is not None:
            return schema.typed(row_type)
        return cast(DuckSchema[AnyRow], schema)

    @property
    def column_names(self) -> tuple[str, ...]:
        """Return column names preserving relation order."""

        return self._names

    @property
    def duckdb_types(self) -> tuple[str, ...]:
        """Return DuckDB type names per column."""

        return self._duckdb_types

    @property
    def duck_types(self) -> tuple[type[DuckType], ...]:
        """Return declared Duck type markers per column."""

        return self._duck_types

    @property
    def python_types(self) -> tuple[Any, ...]:
        """Return cached Python annotations per column."""

        return self._python_types

    @property
    def row_type(self) -> type[RowType_co]:
        """Return the tuple annotation representing rows for this schema."""

        return cast(type[RowType_co], self._row_type)

    @property
    def definitions(self) -> tuple[ColumnDefinition, ...]:
        """Return ``ColumnDefinition`` entries in projection order."""

        return self._definitions

    @property
    def lookup(self) -> Mapping[str, int]:
        """Return a case-insensitive mapping of column names to indexes."""

        return self._lookup

    def index(self, name: str) -> int:
        """Return the column index for ``name``."""

        key = name.casefold()
        try:
            return self._lookup[key]
        except KeyError as exc:  # pragma: no cover - defensive path
            raise KeyError(f"Column not found: {name!r}") from exc

    def column(self, name: str) -> ColumnDefinition:
        """Return the :class:`ColumnDefinition` for ``name``."""

        return self._definitions[self.index(name)]

    def marker(self, name: str) -> type[DuckType]:
        """Return the declared :class:`DuckType` marker for ``name``."""

        return self._duck_types[self.index(name)]

    def duckdb_type(self, name: str) -> str:
        """Return the DuckDB type string for ``name``."""

        return self._duckdb_types[self.index(name)]

    def python_type(self, name: str) -> Any:
        """Return the stored Python annotation for ``name``."""

        return self._python_types[self.index(name)]

    def resolve(self, names: Sequence[str], *, missing_ok: bool = False) -> list[str]:
        """Resolve names against the schema returning canonical casing."""

        return util.resolve_columns(names, self._names, missing_ok=missing_ok)

    def select(
        self, names: Sequence[str], *, missing_ok: bool = False
    ) -> DuckSchema[AnyRow]:
        """Return a new schema containing only the provided ``names``."""

        resolved = self.resolve(names, missing_ok=missing_ok)
        return DuckSchema([self.column(name) for name in resolved])

    def typed(self, row_type: type[RowType]) -> DuckSchema[RowType]:
        """Return a schema copy annotated with an explicit row tuple type."""

        clone = self._copy()
        object.__setattr__(clone, "_row_type", row_type)
        return cast(DuckSchema[RowType], clone)

    def _copy(self) -> DuckSchema[AnyRow]:
        """Return a shallow copy preserving cached schema metadata."""

        clone = cast("DuckSchema[AnyRow]", object.__new__(type(self)))
        object.__setattr__(clone, "_definitions", self._definitions)
        object.__setattr__(clone, "_names", self._names)
        object.__setattr__(clone, "_lookup", self._lookup)
        object.__setattr__(clone, "_duckdb_types", self._duckdb_types)
        object.__setattr__(clone, "_duck_types", self._duck_types)
        object.__setattr__(clone, "_python_types", self._python_types)
        object.__setattr__(clone, "_row_type", self._row_type)
        return clone

    @overload
    def ensure_declared_marker(
        self,
        *,
        column: str,
        declared: type[Unknown],
        context: str,
    ) -> type[DuckType]:
        ...

    @overload
    def ensure_declared_marker(
        self,
        *,
        column: str,
        declared: type[MarkerT],
        context: str,
    ) -> type[MarkerT]:
        ...

    def ensure_declared_marker(
        self,
        *,
        column: str,
        declared: type[DuckType],
        context: str,
    ) -> type[DuckType]:
        """Validate that ``declared`` is compatible with the actual schema."""

        definition = self.column(column)
        actual_marker: type[DuckType] = definition.duck_type
        if actual_marker is Unknown:
            actual_marker = lookup(definition.duckdb_type)

        if declared is Unknown:
            return actual_marker

        if actual_marker is Unknown:
            return declared

        if issubclass(actual_marker, declared):
            return declared

        raise TypeError(
            "{context} column {column!r} is typed as {actual_type} but was declared as {declared_type}.".format(
                context=context,
                column=column,
                actual_type=actual_marker.describe(),
                declared_type=declared.describe(),
            )
        )

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._definitions)

    def __iter__(self) -> Iterator[ColumnDefinition]:  # pragma: no cover - trivial
        return iter(self._definitions)

