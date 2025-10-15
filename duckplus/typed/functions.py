"""DuckDB function catalog to power typed expression helpers."""

# pylint: disable=missing-function-docstring,too-few-public-methods,invalid-name,import-outside-toplevel,method-cache-max-size-none

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from decimal import Decimal
from typing import Callable, Iterable, Mapping, Sequence

from .expression import (
    BlobExpression,
    BooleanExpression,
    GenericExpression,
    NumericExpression,
    TypedExpression,
    VarcharExpression,
)
from .expression import _quote_identifier  # pylint: disable=protected-access


class DuckDBCatalogUnavailableError(RuntimeError):
    """Raised when DuckDB function metadata cannot be loaded."""


@dataclass(frozen=True)
class DuckDBFunctionDefinition:
    """Captured metadata describing a DuckDB function overload."""

    schema_name: str
    function_name: str
    function_type: str
    return_type: str | None
    parameter_types: tuple[str, ...]
    varargs: str | None

    def matches_arity(self, argument_count: int) -> bool:
        required = len(self.parameter_types)
        if self.varargs is None:
            return argument_count == required
        return argument_count >= required


class DuckDBFunctionCatalog:
    """Index of DuckDB functions grouped by type and return category."""

    _SCHEMA_PRIORITY = {"main": 0, "duckdb": 1, "pg_catalog": 2}

    def __init__(self, definitions: Sequence[DuckDBFunctionDefinition]):
        index: dict[str, dict[str, dict[str, list[DuckDBFunctionDefinition]]]] = {}
        for definition in definitions:
            category = _categorise_return_type(definition.return_type)
            type_bucket = index.setdefault(definition.function_type, {})
            category_bucket = type_bucket.setdefault(category, {})
            overloads = category_bucket.setdefault(definition.function_name, [])
            overloads.append(definition)

        for type_bucket in index.values():
            for category_bucket in type_bucket.values():
                for variants in category_bucket.values():
                    variants.sort(key=self._definition_sort_key)

        self._index = index

    @staticmethod
    def _definition_sort_key(definition: DuckDBFunctionDefinition) -> tuple[int, int]:
        schema_priority = DuckDBFunctionCatalog._SCHEMA_PRIORITY.get(
            definition.schema_name, 10
        )
        arity = len(definition.parameter_types)
        return (schema_priority, arity)

    @classmethod
    def load(cls) -> DuckDBFunctionCatalog:
        try:
            import duckdb  # type: ignore
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on env
            raise DuckDBCatalogUnavailableError(
                "The 'duckdb' package is required to enumerate DuckDB functions"
            ) from exc

        connection = duckdb.connect()
        try:
            query = connection.execute(
                """
                SELECT schema_name, function_name, function_type, return_type,
                       parameter_types, varargs
                  FROM duckdb_functions()
                 WHERE function_type IN ('scalar', 'aggregate', 'window')
                """
            )
            rows = query.fetchall()
        finally:
            connection.close()

        definitions = [
            DuckDBFunctionDefinition(
                schema_name=row[0],
                function_name=row[1],
                function_type=row[2],
                return_type=row[3],
                parameter_types=tuple(row[4] or ()),
                varargs=row[5],
            )
            for row in rows
        ]
        return cls(definitions)

    def functions_for(
        self, function_type: str, category: str
    ) -> Mapping[str, Sequence[DuckDBFunctionDefinition]]:
        type_bucket = self._index.get(function_type, {})
        return type_bucket.get(category, {})


class _DuckDBFunctionCall:
    """Callable wrapper that renders a function invocation."""

    def __init__(
        self,
        overloads: Sequence[DuckDBFunctionDefinition],
        *,
        return_category: str,
    ) -> None:
        self._overloads = overloads
        self._return_category = return_category

    def __call__(self, *operands: object) -> TypedExpression:
        arguments = [_coerce_function_operand(operand) for operand in operands]
        dependencies = _merge_dependencies(arguments)
        overload = self._select_overload(len(arguments))
        sql_name = _render_function_name(overload)
        rendered_args = ", ".join(argument.render() for argument in arguments)
        sql = f"{sql_name}({rendered_args})" if arguments else f"{sql_name}()"
        return _construct_expression(
            sql,
            return_type=overload.return_type,
            dependencies=dependencies,
            category=self._return_category,
        )

    def _select_overload(self, argument_count: int) -> DuckDBFunctionDefinition:
        for overload in self._overloads:
            if overload.matches_arity(argument_count):
                return overload
        return self._overloads[0]


class _FunctionAccessor:
    """Surface area for functions in a given category."""

    def __init__(
        self,
        functions: Mapping[str, Sequence[DuckDBFunctionDefinition]],
        *,
        return_category: str,
    ) -> None:
        self._functions = functions
        self._return_category = return_category

    def __getattr__(self, name: str) -> _DuckDBFunctionCall:
        if name not in self._functions:
            msg = f"Function '{name}' is not available in DuckDB catalog"
            raise AttributeError(msg) from None
        return _DuckDBFunctionCall(
            self._functions[name], return_category=self._return_category
        )

    def __getitem__(self, name: str) -> _DuckDBFunctionCall:
        if name not in self._functions:
            raise KeyError(name)
        return _DuckDBFunctionCall(
            self._functions[name], return_category=self._return_category
        )

    def __dir__(self) -> Iterable[str]:
        return sorted(name for name in self._functions if name.isidentifier())


class _FunctionCategoryNamespace:
    """Expose function categories grouped by return semantics."""

    def __init__(self, catalog: DuckDBFunctionCatalog, function_type: str) -> None:
        self._catalog = catalog
        self._function_type = function_type

    @property
    @lru_cache(maxsize=None)
    def Numeric(self) -> _FunctionAccessor:  # noqa: N802 - namespace attribute
        return _FunctionAccessor(
            self._catalog.functions_for(self._function_type, "numeric"),
            return_category="numeric",
        )

    @property
    @lru_cache(maxsize=None)
    def Boolean(self) -> _FunctionAccessor:  # noqa: N802
        return _FunctionAccessor(
            self._catalog.functions_for(self._function_type, "boolean"),
            return_category="boolean",
        )

    @property
    @lru_cache(maxsize=None)
    def Varchar(self) -> _FunctionAccessor:  # noqa: N802
        return _FunctionAccessor(
            self._catalog.functions_for(self._function_type, "varchar"),
            return_category="varchar",
        )

    @property
    @lru_cache(maxsize=None)
    def Blob(self) -> _FunctionAccessor:  # noqa: N802
        return _FunctionAccessor(
            self._catalog.functions_for(self._function_type, "blob"),
            return_category="blob",
        )

    @property
    @lru_cache(maxsize=None)
    def Generic(self) -> _FunctionAccessor:  # noqa: N802
        return _FunctionAccessor(
            self._catalog.functions_for(self._function_type, "generic"),
            return_category="generic",
        )


class DuckDBFunctionNamespace:
    """Lazy loader exposing DuckDB's scalar, aggregate, and window functions."""

    def __init__(
        self,
        *,
        loader: Callable[[], DuckDBFunctionCatalog] | None = None,
    ) -> None:
        self._loader = loader or DuckDBFunctionCatalog.load
        self._catalog: DuckDBFunctionCatalog | None = None

    def _catalogue(self) -> DuckDBFunctionCatalog:
        if self._catalog is None:
            self._catalog = self._loader()
        return self._catalog

    @property
    def Scalar(self) -> _FunctionCategoryNamespace:  # noqa: N802
        return _FunctionCategoryNamespace(self._catalogue(), "scalar")

    @property
    def Aggregate(self) -> _FunctionCategoryNamespace:  # noqa: N802
        return _FunctionCategoryNamespace(self._catalogue(), "aggregate")

    @property
    def Window(self) -> _FunctionCategoryNamespace:  # noqa: N802
        return _FunctionCategoryNamespace(self._catalogue(), "window")


def _coerce_function_operand(value: object) -> TypedExpression:
    if isinstance(value, TypedExpression):
        return value
    if isinstance(value, bool):
        return BooleanExpression("TRUE" if value else "FALSE")
    if isinstance(value, (int, float, Decimal)):
        return NumericExpression.literal(value)
    if isinstance(value, bytes):
        return BlobExpression.literal(value)
    if isinstance(value, str):
        return GenericExpression(
            _quote_identifier(value),
            type_annotation="IDENTIFIER",
            dependencies=(value,),
        )
    if value is None:
        return GenericExpression("NULL")
    msg = "DuckDB function arguments must be typed expressions or supported literals"
    raise TypeError(msg)


def _merge_dependencies(expressions: Iterable[TypedExpression]) -> frozenset[str]:
    dependencies: set[str] = set()
    for expression in expressions:
        dependencies.update(expression.dependencies)
    return frozenset(dependencies)


def _render_function_name(definition: DuckDBFunctionDefinition) -> str:
    schema = definition.schema_name
    name = definition.function_name
    if schema in ("main", "pg_catalog"):
        return _quote_identifier(name) if name != name.lower() else name
    return f"{_quote_identifier(schema)}.{_quote_identifier(name)}"


def _construct_expression(
    sql: str,
    *,
    return_type: str | None,
    dependencies: frozenset[str],
    category: str,
) -> TypedExpression:
    annotation = (return_type or "UNKNOWN").upper()
    if category == "numeric":
        return NumericExpression(sql, dependencies=dependencies, type_annotation=annotation)
    if category == "boolean":
        return BooleanExpression(sql, dependencies=dependencies, type_annotation=annotation)
    if category == "varchar":
        return VarcharExpression(sql, dependencies=dependencies, type_annotation=annotation)
    if category == "blob":
        return BlobExpression(sql, dependencies=dependencies, type_annotation=annotation)
    return GenericExpression(sql, dependencies=dependencies, type_annotation=annotation)


def _categorise_return_type(return_type: str | None) -> str:
    if return_type is None:
        return "generic"
    normalized = return_type.upper()
    if any(
        normalized.startswith(prefix)
        for prefix in (
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
            "REAL",
            "INTERVAL",
        )
    ):
        return "numeric"
    if normalized.startswith("BOOLEAN"):
        return "boolean"
    if any(
        normalized.startswith(prefix)
        for prefix in ("VARCHAR", "STRING", "TEXT", "JSON", "UUID")
    ):
        return "varchar"
    if normalized.startswith("BLOB"):
        return "blob"
    return "generic"
