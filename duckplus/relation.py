"""Utilities for working with DuckDB relations."""

# pylint: disable=import-error,too-many-lines

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from os import PathLike
from pathlib import Path
from collections.abc import Sequence, Iterator
from uuid import uuid4
import warnings
from importlib import import_module
from typing import Any, Iterable, Mapping, TypeVar, cast, overload
from typing import Literal

import duckdb  # type: ignore[import-not-found]

from ._table_utils import require_connection
from .duckcon import DuckCon
from .typed.select import SelectStatementBuilder
from .typed.dependencies import ExpressionDependency
from .typed.expressions.base import AliasedExpression, BooleanExpression, TypedExpression
from .typed.types import BooleanType


T = TypeVar("T")

JoinCondition = Mapping[str, str] | Iterable[tuple[str, str]] | Iterable[str] | str | None


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

    def row_count(self) -> int:
        """Return the number of rows produced by the relation."""

        require_connection(self.duckcon, "Relation.row_count")
        count_relation = self._relation.aggregate("COUNT(*) AS duckplus_row_count")
        row = count_relation.fetchone()
        if row is None:
            return 0
        return int(row[0])

    def null_ratios(self) -> dict[str, float]:
        """Return the ratio of ``NULL`` values for each column in the relation."""

        require_connection(self.duckcon, "Relation.null_ratios")
        if not self.columns:
            return {}

        expressions: list[str] = []
        for column in self.columns:
            quoted = self._quote_identifier(column)
            expression = (
                f"COALESCE(AVG(CASE WHEN {quoted} IS NULL "
                "THEN 1.0 ELSE 0.0 END), 0.0)"
            )
            expressions.append(expression)

        ratio_relation = self._relation.aggregate(", ".join(expressions))
        row = ratio_relation.fetchone()
        if row is None:
            return {column: 0.0 for column in self.columns}

        return {column: float(value) for column, value in zip(self.columns, row, strict=False)}

    def sample_pandas(self, limit: int | None = 50) -> Any:
        """Return a Pandas DataFrame containing a sample of the relation."""

        helper = "Relation.sample_pandas"
        require_connection(self.duckcon, helper)
        relation = self._prepare_sample_relation(limit, helper=helper)
        self._require_module("pandas", helper, "pip install pandas")
        return relation.df()

    def iter_pandas_batches(
        self, batch_size: int, *, limit: int | None = None
    ) -> Iterator[Any]:
        """Yield Pandas DataFrame batches from the relation."""

        helper = "Relation.iter_pandas_batches"
        require_connection(self.duckcon, helper)
        relation = self._prepare_sample_relation(limit, helper=helper)
        self._require_module("pandas", helper, "pip install pandas")
        normalised = self._normalise_batch_size(batch_size, helper)
        return self._iterate_pandas_batches(relation, normalised)

    def sample_arrow(self, limit: int | None = 50) -> Any:
        """Return a PyArrow Table containing a sample of the relation."""

        helper = "Relation.sample_arrow"
        require_connection(self.duckcon, helper)
        relation = self._prepare_sample_relation(limit, helper=helper)
        self._require_module("pyarrow", helper, "pip install pyarrow")
        return relation.fetch_arrow_table()

    def iter_arrow_batches(
        self, batch_size: int, *, limit: int | None = None
    ) -> Iterator[Any]:
        """Yield PyArrow tables from the relation in batches."""

        helper = "Relation.iter_arrow_batches"
        require_connection(self.duckcon, helper)
        relation = self._prepare_sample_relation(limit, helper=helper)
        pyarrow = self._require_module("pyarrow", helper, "pip install pyarrow")
        normalised = self._normalise_batch_size(batch_size, helper)
        reader = relation.fetch_arrow_reader(normalised)
        return self._iterate_arrow_batches(reader, pyarrow)

    def sample_polars(self, limit: int | None = 50) -> Any:
        """Return a Polars DataFrame containing a sample of the relation."""

        helper = "Relation.sample_polars"
        require_connection(self.duckcon, helper)
        relation = self._prepare_sample_relation(limit, helper=helper)
        polars = self._require_module("polars", helper, "pip install polars")
        rows = relation.fetchall()
        return polars.DataFrame(rows, schema=list(self.columns), orient="row")

    def iter_polars_batches(
        self, batch_size: int, *, limit: int | None = None
    ) -> Iterator[Any]:
        """Yield Polars DataFrame batches from the relation."""

        helper = "Relation.iter_polars_batches"
        require_connection(self.duckcon, helper)
        relation = self._prepare_sample_relation(limit, helper=helper)
        polars = self._require_module("polars", helper, "pip install polars")
        normalised = self._normalise_batch_size(batch_size, helper)
        return self._iterate_polars_batches(
            relation, normalised, polars, list(self.columns)
        )

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

    @classmethod
    def from_odbc_query(
        cls,
        duckcon: DuckCon,
        connection_string: str,
        query: str,
        *,
        parameters: Iterable[Any] | None = None,
    ) -> "Relation":
        """Create a relation from an ODBC query executed via nano-ODBC."""

        if not duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call from_odbc_query. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        connection = duckcon.connection
        sql = cls._build_odbc_query_sql(connection_string, query, parameters)

        try:
            relation = connection.sql(sql)
        except duckdb.CatalogException as exc:
            message = str(exc)
            if "odbc_query" in message or "nano_odbc" in message:
                msg = (
                    "nano-ODBC extension is not loaded. Create DuckCon with "
                    "extra_extensions=(\"nanodbc\",) before querying ODBC sources."
                )
                raise RuntimeError(msg) from exc
            raise

        return cls.from_relation(duckcon, relation)

    @classmethod
    def from_odbc_table(
        cls,
        duckcon: DuckCon,
        connection_string: str,
        table: str,
    ) -> "Relation":
        """Create a relation by scanning an ODBC table via nano-ODBC."""

        if not duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call from_odbc_table. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        connection = duckcon.connection

        sql = cls._build_odbc_scan_sql(connection_string, table)

        try:
            relation = connection.sql(sql)
        except duckdb.CatalogException as exc:
            message = str(exc)
            if "odbc_scan" in message or "nano_odbc" in message:
                msg = (
                    "nano-ODBC extension is not loaded. Create DuckCon with "
                    "extra_extensions=(\"nanodbc\",) before scanning ODBC tables."
                )
                raise RuntimeError(msg) from exc
            raise

        return cls.from_relation(duckcon, relation)

    @classmethod
    def from_excel(  # pylint: disable=too-many-arguments, too-many-locals
        cls,
        duckcon: DuckCon,
        source: str | PathLike[str],
        *,
        sheet: str | int | None = None,
        header: bool | None = None,
        skip: int | None = None,
        skiprows: int | None = None,
        limit: int | None = None,
        names: Sequence[str] | None = None,
        dtype: Mapping[str, str] | Sequence[str] | None = None,
        all_varchar: bool | None = None,
    ) -> "Relation":
        """Create a relation from an Excel workbook via DuckDB's excel extension."""

        if not duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call from_excel. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        if skip is not None and skiprows is not None and skip != skiprows:
            msg = "Both 'skip' and alias 'skiprows' were provided"
            raise ValueError(msg)
        if skip is None:
            skip = skiprows

        duckcon._load_excel()  # type: ignore[attr-defined]  # pylint: disable=protected-access

        connection = duckcon.connection

        arguments: list[str] = [cls._quote_sql_string(str(source))]

        options = {
            "sheet": sheet,
            "header": header,
            "skip": skip,
            "limit": limit,
            "names": list(names) if names is not None else None,
            "dtype": cls._normalise_excel_dtype(dtype),
            "all_varchar": all_varchar,
        }

        for key, value in options.items():
            if value is None:
                continue
            rendered = cls._serialise_excel_parameter(value)
            arguments.append(f"{key}={rendered}")

        sql = f"SELECT * FROM read_excel({', '.join(arguments)})"

        try:
            relation = connection.sql(sql)
        except duckdb.BinderException as exc:
            msg = "Invalid parameters supplied to read_excel"
            raise ValueError(msg) from exc

        return cls.from_relation(duckcon, relation)

    @staticmethod
    def _build_odbc_query_sql(
        connection_string: str,
        query: str,
        parameters: Iterable[Any] | None,
    ) -> str:
        connection_literal = Relation._quote_sql_string(connection_string)
        query_literal = Relation._quote_sql_string(query)

        if parameters is None:
            return f"SELECT * FROM odbc_query({connection_literal}, {query_literal})"

        rendered = ", ".join(Relation._serialise_odbc_parameter(value) for value in parameters)
        return (
            "SELECT * FROM odbc_query("
            f"{connection_literal}, {query_literal}, [{rendered}]"
            ")"
        )

    @staticmethod
    def _build_odbc_scan_sql(connection_string: str, table: str) -> str:
        connection_literal = Relation._quote_sql_string(connection_string)
        table_literal = Relation._quote_sql_string(table)
        return f"SELECT * FROM odbc_scan({connection_literal}, {table_literal})"

    @staticmethod
    def _quote_sql_string(value: str) -> str:
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _serialise_odbc_parameter(value: Any) -> str:
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if value is None:
            return "NULL"
        if isinstance(value, (int, float)):
            return repr(value)
        if isinstance(value, str):
            return Relation._quote_sql_string(value)
        msg = (
            "Unsupported parameter type for ODBC query bindings: "
            f"{type(value)!r}"
        )
        raise TypeError(msg)

    @staticmethod
    def _normalise_excel_dtype(
        dtype: Mapping[str, str] | Sequence[str] | None,
    ) -> Mapping[str, str] | Sequence[str] | None:
        if dtype is None:
            return None
        if isinstance(dtype, Mapping):
            return dict(dtype)
        return list(dtype)

    @staticmethod
    def _serialise_excel_parameter(value: object) -> str:
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return repr(value)
        if isinstance(value, str):
            return Relation._quote_sql_string(value)
        if isinstance(value, Mapping):
            parts = []
            for key, subvalue in value.items():
                if not isinstance(key, str):
                    msg = "Excel option maps require string keys"
                    raise TypeError(msg)
                rendered_value = Relation._serialise_excel_parameter(subvalue)
                parts.append(
                    f"{Relation._quote_sql_string(key)}: {rendered_value}"
                )
            return "{" + ", ".join(parts) + "}"
        if isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray)
        ):
            rendered_items = [Relation._serialise_excel_parameter(item) for item in value]
            return "[" + ", ".join(rendered_items) + "]"
        msg = (
            "Unsupported parameter type for Excel reader option: "
            f"{type(value)!r}"
        )
        raise TypeError(msg)

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

    def add(self, **expressions: TypedExpression) -> "Relation":  # pylint: disable=too-many-locals
        """Return a new relation with additional computed columns.

        Expressions must be provided through :mod:`duckplus.typed`. Typed
        expressions carry dependency metadata, allowing the helper to validate
        that references only target columns present on the original relation.
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

        original_casefolded = dict(self._casefolded_columns)
        original_column_sql = [
            self._quote_identifier(column) for column in self.columns
        ]

        existing_matches = {
            column
            for alias in expressions
            for column in original_casefolded.get(alias.casefold(), ())
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
                self._assert_add_dependencies(
                    alias,
                    dependencies,
                    casefolded_columns=original_casefolded,
                )

            validation_builder = SelectStatementBuilder()
            for column_sql in original_column_sql:
                validation_builder.column(column_sql)
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

        builder = SelectStatementBuilder()
        for column_sql in original_column_sql:
            builder.column(column_sql)
        for alias, expression_sql in prepared:
            builder.column(expression_sql, alias=alias)

        select_list = builder.build_select_list()

        try:
            relation = self._relation.project(select_list)
        except duckdb.BinderException as error:
            msg = "add expressions reference unknown columns"
            raise ValueError(msg) from error

        return type(self).from_relation(self.duckcon, relation)

    def join(
        self,
        other: "Relation",
        *,
        on: JoinCondition = None,
    ) -> "Relation":
        """Return an inner join with conflict validation."""

        return self._join(other, join_type="inner", on=on)

    def left_join(
        self,
        other: "Relation",
        *,
        on: JoinCondition = None,
    ) -> "Relation":
        """Return a left join with conflict validation."""

        return self._join(other, join_type="left", on=on)

    def right_join(
        self,
        other: "Relation",
        *,
        on: JoinCondition = None,
    ) -> "Relation":
        """Return a right join with conflict validation."""

        return self._join(other, join_type="right", on=on)

    def outer_join(
        self,
        other: "Relation",
        *,
        on: JoinCondition = None,
    ) -> "Relation":
        """Return a full outer join with conflict validation."""

        return self._join(other, join_type="outer", on=on)

    def semi_join(
        self,
        other: "Relation",
        *,
        on: JoinCondition = None,
    ) -> "Relation":
        """Return a semi join keeping only left relation columns."""

        return self._join(other, join_type="semi", on=on)

    def materialize(
        self,
        name: str | None = None,
        *,
        temporary: bool = True,
        replace: bool = True,
    ) -> "Relation":
        """Materialise the relation into a DuckDB table and return it.

        By default the relation is materialised into a temporary table that is
        discarded when the underlying :class:`~duckplus.duckcon.DuckCon`
        closes. Use ``name`` to control the table identifier, set
        ``temporary=False`` to persist the result, and ``replace=False`` to
        require that the table does not already exist.
        """

        helper = "Relation.materialize"
        connection = require_connection(self.duckcon, helper)

        if name is None:
            table_name = f"duckplus_materialized_{uuid4().hex}"
        else:
            if not isinstance(name, str):
                msg = "materialize table name must be a string"
                raise TypeError(msg)
            table_name = name.strip()
            if not table_name:
                msg = "materialize table name cannot be empty"
                raise ValueError(msg)

        quoted_table = self._quote_qualified_identifier(table_name)
        view_name = f"duckplus_materialize_view_{uuid4().hex}"
        quoted_view = self._quote_identifier(view_name)

        self._relation.to_view(view_name, replace=True)

        try:
            statements: list[str] = ["CREATE"]
            if replace:
                statements.extend(["OR", "REPLACE"])
            if temporary:
                statements.append("TEMP")
            statements.extend(["TABLE", quoted_table, "AS SELECT * FROM", quoted_view])
            connection.execute(" ".join(statements))
        finally:
            connection.execute(f"DROP VIEW {quoted_view}")

        materialized = connection.table(table_name)
        return type(self).from_relation(self.duckcon, materialized)

    def asof_join(
        self,
        other: "Relation",
        *,
        order: tuple[object, object],
        on: JoinCondition = None,
        tolerance: object | None = None,
        direction: Literal["backward", "forward"] = "backward",
    ) -> "Relation":
        """Return an as-of join aligning rows by an ordering column."""

        if not isinstance(other, Relation):
            msg = "asof_join requires another Relation instance"
            raise TypeError(msg)

        if self.duckcon is not other.duckcon:
            msg = "Joined relations must originate from the same DuckCon"
            raise ValueError(msg)

        if not self.duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call asof_join. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        if not isinstance(order, tuple) or len(order) != 2:
            msg = "asof_join order must be a two-item tuple"
            raise TypeError(msg)

        direction_key = direction.casefold()
        if direction_key not in {"backward", "forward"}:
            msg = "asof_join direction must be 'backward' or 'forward'"
            raise ValueError(msg)

        join_pairs = self._prepare_join_pairs(other, on)

        left_casefold_map = {
            key: list(values) for key, values in self._casefolded_columns.items()
        }
        right_casefold_map = self._build_casefold_map(other.columns)

        left_alias = "left"
        right_alias = "right"

        left_order_sql, _ = self._normalise_asof_operand(
            order[0],
            alias=left_alias,
            relation_label="left asof order operand",
            casefold_map=left_casefold_map,
        )
        right_order_sql, _ = self._normalise_asof_operand(
            order[1],
            alias=right_alias,
            relation_label="right asof order operand",
            casefold_map=right_casefold_map,
        )

        tolerance_sql: str | None
        if tolerance is None:
            tolerance_sql = None
        else:
            tolerance_sql = self._normalise_asof_tolerance(
                tolerance,
                left_alias=left_alias,
                right_alias=right_alias,
                left_casefold_map=left_casefold_map,
                right_casefold_map=right_casefold_map,
            )

        left_identifier = self._quote_identifier(left_alias)
        right_identifier = self._quote_identifier(right_alias)

        condition_clauses = [
            (
                f"{left_identifier}.{self._quote_identifier(left_column)} = "
                f"{right_identifier}.{self._quote_identifier(right_column)}"
            )
            for left_column, right_column in join_pairs
        ]

        if direction_key == "backward":
            inequality_clause = f"{left_order_sql} >= {right_order_sql}"
            if tolerance_sql is not None:
                tolerance_clause = (
                    f"({left_order_sql} - {right_order_sql}) <= {tolerance_sql}"
                )
            else:
                tolerance_clause = None
        else:
            inequality_clause = f"{left_order_sql} <= {right_order_sql}"
            if tolerance_sql is not None:
                tolerance_clause = (
                    f"({right_order_sql} - {left_order_sql}) <= {tolerance_sql}"
                )
            else:
                tolerance_clause = None

        condition_clauses.append(inequality_clause)
        if tolerance_clause is not None:
            condition_clauses.append(tolerance_clause)

        condition_sql = " AND ".join(condition_clauses)

        projection_entries = self._build_join_projection_entries(
            other,
            left_alias=left_alias,
            right_alias=right_alias,
            include_right_columns=True,
        )
        if not projection_entries:
            msg = "asof_join requires at least one projected column"
            raise ValueError(msg)
        select_list = ", ".join(projection_entries)

        left_subquery = self._relation.sql_query()
        right_subquery = other._relation.sql_query()

        query = (
            f"SELECT {select_list}\n"
            f"FROM ({left_subquery}) AS {left_identifier}\n"
            f"ASOF JOIN ({right_subquery}) AS {right_identifier}\n"
            f"ON {condition_sql}"
        )

        try:
            joined_relation = self.duckcon.connection.sql(query)
        except duckdb.BinderException as error:
            msg = "asof join expressions reference unknown columns"
            raise ValueError(msg) from error

        return type(self).from_relation(self.duckcon, joined_relation)

    def _join(
        self,
        other: "Relation",
        *,
        join_type: Literal["inner", "left", "right", "outer", "semi"],
        on: JoinCondition,
    ) -> "Relation":
        if not isinstance(other, Relation):
            msg = "join helpers require another Relation instance"
            raise TypeError(msg)

        if self.duckcon is not other.duckcon:
            msg = "Joined relations must originate from the same DuckCon"
            raise ValueError(msg)

        if not self.duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call join helpers. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        join_pairs = self._prepare_join_pairs(other, on)
        if not join_pairs:
            msg = (
                "join requires at least one shared column or explicit join condition"
            )
            raise ValueError(msg)

        left_alias = self._relation.alias
        right_alias = other._relation.alias

        condition_sql = self._render_join_condition(join_pairs, left_alias, right_alias)

        try:
            joined_relation = self._relation.join(
                other._relation,
                condition_sql,
                how=join_type,
            )
        except duckdb.BinderException as error:
            msg = "Join condition references unknown columns"
            raise ValueError(msg) from error

        projection_entries = self._build_join_projection_entries(
            other,
            left_alias,
            right_alias,
            include_right_columns=join_type != "semi",
        )

        if not projection_entries:
            msg = "join helpers require at least one projected column"
            raise ValueError(msg)

        select_list = ", ".join(projection_entries)
        projected = joined_relation.project(select_list)
        return type(self).from_relation(self.duckcon, projected)

    def aggregate(  # pylint: disable=too-many-locals,keyword-arg-before-vararg
        self,
        group_by: Iterable[str] | str | None = None,
        *filters: object,
        **aggregations: TypedExpression,
    ) -> "Relation":
        """Return a grouped relation with computed aggregate columns.

        Aggregations must be provided through :mod:`duckplus.typed` so DuckPlus
        can validate column dependencies before delegating to DuckDB.
        """

        if not aggregations:
            msg = "aggregate requires at least one aggregation expression"
            raise ValueError(msg)

        resolved_group_by = self._normalise_group_by(group_by)
        filter_clauses = self._normalise_filter_clauses(
            filters,
            label_prefix="aggregate filter",
            error_context="Aggregate filters",
        )

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

    def append_csv(
        self,
        target: Path | PathLike[str] | str,
        *,
        unique_id_column: Sequence[str] | str | None = None,
        match_all_columns: bool = False,
        mutate: bool = True,
        header: bool = True,
        delimiter: str = ",",
        quotechar: str = '"',
        encoding: str = "utf-8",
    ) -> "Relation":
        """Append rows from the relation into a CSV file."""

        connection = require_connection(self.duckcon, "Relation.append_csv")
        target_path = Path(target)
        if target_path.exists() and target_path.is_dir():
            msg = "Relation.append_csv requires a file path, not a directory"
            raise ValueError(msg)

        unique_columns = self._normalise_identifier_sequence(
            unique_id_column,
            helper="Relation.append_csv",
            parameter="unique_id_column",
        )

        existing_relation: duckdb.DuckDBPyRelation | None = None
        if (
            target_path.exists()
            and (unique_columns is not None or match_all_columns)
            and target_path.stat().st_size > 0
        ):
            existing_relation = connection.from_csv_auto(
                str(target_path),
                header=header,
                delimiter=delimiter,
                quotechar=quotechar,
            )

        append_subset = self._deduplicate_against_existing(
            helper="Relation.append_csv",
            existing_relation=existing_relation,
            unique_id_columns=unique_columns,
            match_all_columns=match_all_columns,
        )

        view_name = f"duckplus_append_view_{uuid4().hex}"
        table_name = f"duckplus_append_{uuid4().hex}"
        quoted_view = self._quote_identifier(view_name)
        quoted_table = self._quote_identifier(table_name)
        append_subset.create_view(view_name)
        try:
            connection.execute(
                f"CREATE TEMP TABLE {quoted_table} AS SELECT * FROM {quoted_view}"
            )
        finally:
            connection.execute(f"DROP VIEW {quoted_view}")

        materialised = connection.sql(f"SELECT * FROM {quoted_table}")
        result = type(self).from_relation(self.duckcon, materialised)

        if not mutate:
            materialised.execute()
            return result

        file_exists = target_path.exists()
        file_size = target_path.stat().st_size if file_exists else 0
        should_write_header = header and file_size == 0
        has_new_rows = materialised.limit(1).fetchone() is not None

        if not has_new_rows and not should_write_header:
            materialised.execute()
            return result

        target_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if file_size > 0 else "w"

        try:
            with target_path.open(mode, encoding=encoding, newline="") as handle:
                if quotechar:
                    writer = csv.writer(
                        handle,
                        delimiter=delimiter,
                        quotechar=quotechar,
                    )
                else:
                    writer = csv.writer(handle, delimiter=delimiter)
                if should_write_header:
                    writer.writerow(result.columns)
                if has_new_rows:
                    while True:
                        chunk = materialised.fetchmany(1024)
                        if not chunk:
                            break
                        writer.writerows(chunk)
        finally:
            materialised.execute()

        return result

    def append_parquet(
        self,
        target: Path | PathLike[str] | str,
        *,
        unique_id_column: Sequence[str] | str | None = None,
        match_all_columns: bool = False,
        mutate: bool = False,
        temp_directory: Path | PathLike[str] | str | None = None,
        compression: str | None = None,
    ) -> "Relation":
        """Append rows from the relation into a Parquet file."""

        connection = require_connection(self.duckcon, "Relation.append_parquet")
        target_path = Path(target)
        if target_path.exists() and target_path.is_dir():
            msg = "Relation.append_parquet requires a Parquet file path"
            raise ValueError(msg)

        unique_columns = self._normalise_identifier_sequence(
            unique_id_column,
            helper="Relation.append_parquet",
            parameter="unique_id_column",
        )

        existing_relation: duckdb.DuckDBPyRelation | None = None
        if target_path.exists() and target_path.stat().st_size > 0:
            existing_relation = connection.from_parquet(str(target_path))

        append_subset = self._deduplicate_against_existing(
            helper="Relation.append_parquet",
            existing_relation=existing_relation,
            unique_id_columns=unique_columns,
            match_all_columns=match_all_columns,
        )

        result = type(self).from_relation(self.duckcon, append_subset)

        if not mutate:
            return result

        has_new_rows = append_subset.limit(1).fetchone() is not None
        if not target_path.exists() and not has_new_rows:
            return result
        if target_path.exists() and not has_new_rows:
            return result

        combined_relation = append_subset
        if existing_relation is not None and has_new_rows:
            combined_relation = existing_relation.union(append_subset)

        output_directory = (
            Path(temp_directory)
            if temp_directory is not None
            else target_path.parent
        )
        output_directory.mkdir(parents=True, exist_ok=True)
        temp_name = target_path.stem + f"_{uuid4().hex}.parquet"
        temp_path = output_directory / temp_name

        try:
            combined_relation.write_parquet(
                str(temp_path),
                compression=compression,
                overwrite=True,
            )
            temp_path.replace(target_path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise

        return result

    def write_parquet_dataset(
        self,
        directory: Path | PathLike[str] | str,
        *,
        partition_column: str,
        filename_template: str = "{partition}.parquet",
        partition_actions: Mapping[object, Literal["append", "overwrite"]]
        | None = None,
        default_action: Literal["append", "overwrite"] = "overwrite",
        immutable: bool = False,
    ) -> None:
        """Persist rows into a partitioned Parquet dataset."""

        _connection = require_connection(
            self.duckcon, "Relation.write_parquet_dataset"
        )

        resolved_column = self._resolve_column(partition_column)
        if resolved_column is None:
            msg = f"Partition column '{partition_column}' does not exist on the relation"
            raise ValueError(msg)

        if default_action not in {"append", "overwrite"}:
            msg = "default_action must be either 'append' or 'overwrite'"
            raise ValueError(msg)

        if immutable and partition_actions is not None:
            msg = "partition_actions cannot be provided when immutable=True"
            raise ValueError(msg)

        actions: dict[object, Literal["append", "overwrite"]]
        actions = {}
        if partition_actions is not None:
            for key, action in partition_actions.items():
                if action not in {"append", "overwrite"}:
                    msg = "partition_actions values must be 'append' or 'overwrite'"
                    raise ValueError(msg)
                actions[key] = action

        target_directory = Path(directory)
        target_directory.mkdir(parents=True, exist_ok=True)

        column_identifier = self._quote_identifier(resolved_column)
        partitions = [
            row[0]
            for row in self._relation.project(column_identifier).distinct().fetchall()
        ]

        def quote_literal(value: object) -> str:
            if value is None:
                return "NULL"
            if isinstance(value, bool):
                return "TRUE" if value else "FALSE"
            if isinstance(value, (int, float)):
                return repr(value)
            text = str(value)
            escaped = text.replace("'", "''")
            return f"'{escaped}'"

        for partition_value in partitions:
            try:
                file_name = filename_template.format(partition=partition_value)
            except Exception as error:  # pragma: no cover - defensive conversion guard
                msg = (
                    "Failed to render filename_template for partition value "
                    f"{partition_value!r}"
                )
                raise ValueError(msg) from error

            if not file_name:
                msg = "filename_template must produce a non-empty file name"
                raise ValueError(msg)

            file_path = target_directory / file_name

            if immutable and file_path.exists():
                msg = (
                    "Partition '{partition}' already exists; immutable datasets "
                    "only support inserting new partitions"
                ).format(partition=partition_value)
                raise ValueError(msg)

            action = actions.get(partition_value, default_action)
            if immutable:
                action = "overwrite"

            if partition_value is None:
                predicate = f"{column_identifier} IS NULL"
            else:
                literal = quote_literal(partition_value)
                predicate = f"{column_identifier} = {literal}"

            partition_relation = self._relation.filter(predicate)

            file_name = str(file_path)

            if action == "append":
                if file_path.exists():
                    existing = _connection.read_parquet(file_name)
                    combined = existing.union(partition_relation)
                    combined.write_parquet(file_name, overwrite=True)
                else:
                    partition_relation.write_parquet(file_name)
            else:
                partition_relation.write_parquet(file_name, overwrite=True)

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

    def _prepare_join_pairs(
        self,
        other: "Relation",
        on: JoinCondition,
    ) -> list[tuple[str, str]]:
        left_casefold_map = self._build_casefold_map(self.columns)
        right_casefold_map = self._build_casefold_map(other.columns)

        pairs, seen_pairs = self._prepare_explicit_join_pairs(
            on,
            left_casefold_map,
            right_casefold_map,
        )

        for column in self.columns:
            key = column.casefold()
            right_matches = right_casefold_map.get(key)
            if not right_matches:
                continue

            left_matches = left_casefold_map.get(key, [])
            if len(left_matches) > 1:
                formatted = self._format_column_list(left_matches)
                msg = (
                    "Join on shared columns is ambiguous on left relation; "
                    f"multiple columns match ignoring case: {formatted}"
                )
                raise ValueError(msg)

            if len(right_matches) > 1:
                formatted = self._format_column_list(right_matches)
                msg = (
                    "Join on shared columns is ambiguous on right relation; "
                    f"multiple columns match ignoring case: {formatted}"
                )
                raise ValueError(msg)

            right_column = right_matches[0]
            pair_key = (column.casefold(), right_column.casefold())
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            pairs.append((column, right_column))

        return pairs

    def _prepare_explicit_join_pairs(
        self,
        on: JoinCondition,
        left_casefold_map: Mapping[str, list[str]],
        right_casefold_map: Mapping[str, list[str]],
    ) -> tuple[list[tuple[str, str]], set[tuple[str, str]]]:
        pairs: list[tuple[str, str]] = []
        seen_pairs: set[tuple[str, str]] = set()

        for left_name, right_name in self._normalise_join_on_entries(on):
            left_resolved = self._resolve_casefolded_column(
                left_casefold_map,
                left_name,
                relation_label="Left",
            )
            right_resolved = self._resolve_casefolded_column(
                right_casefold_map,
                right_name,
                relation_label="Right",
            )
            pair_key = (left_resolved.casefold(), right_resolved.casefold())
            if pair_key in seen_pairs:
                msg = (
                    "join condition for columns "
                    f"'{left_resolved}' and '{right_resolved}' specified multiple times"
                )
                raise ValueError(msg)
            seen_pairs.add(pair_key)
            pairs.append((left_resolved, right_resolved))

        return pairs, seen_pairs

    @staticmethod
    def _normalise_join_on_entries(
        on: JoinCondition,
    ) -> list[tuple[str, str]]:
        if on is None:
            return []

        if isinstance(on, Mapping):
            entries: Iterable[object] = on.items()
        elif isinstance(on, str):
            entries = [(on, on)]
        else:
            entries = cast(Iterable[object], on)

        normalised: list[tuple[str, str]] = []
        for entry in entries:
            if isinstance(entry, str):
                left_name = right_name = entry
            elif isinstance(entry, tuple) and len(entry) == 2:
                left_name, right_name = entry
            else:
                msg = (
                    "join on entries must be string column names or (left, right) column pairs"
                )
                raise TypeError(msg)

            if not isinstance(left_name, str) or not isinstance(right_name, str):
                msg = "join on column names must be strings"
                raise TypeError(msg)

            left_trimmed = left_name.strip()
            right_trimmed = right_name.strip()
            if not left_trimmed or not right_trimmed:
                msg = "join on column names cannot be empty"
                raise ValueError(msg)

            normalised.append((left_trimmed, right_trimmed))

        return normalised

    def _render_join_condition(
        self,
        join_pairs: Iterable[tuple[str, str]],
        left_alias: str,
        right_alias: str,
    ) -> str:
        left_identifier = self._quote_identifier(left_alias)
        right_identifier = self._quote_identifier(right_alias)

        clauses = []
        for left_column, right_column in join_pairs:
            left_reference = f"{left_identifier}.{self._quote_identifier(left_column)}"
            right_reference = f"{right_identifier}.{self._quote_identifier(right_column)}"
            clauses.append(f"{left_reference} = {right_reference}")

        return " AND ".join(clauses)

    def _normalise_asof_operand(
        self,
        operand: object,
        *,
        alias: str,
        relation_label: str,
        casefold_map: Mapping[str, list[str]],
    ) -> tuple[str, frozenset[ExpressionDependency]]:
        if isinstance(operand, str):
            column = operand.strip()
            if not column:
                msg = f"{relation_label} cannot be empty"
                raise ValueError(msg)
            resolved = self._resolve_casefolded_column(
                casefold_map,
                column,
                relation_label=relation_label,
            )
            alias_identifier = self._quote_identifier(alias)
            column_identifier = self._quote_identifier(resolved)
            return f"{alias_identifier}.{column_identifier}", frozenset()

        if isinstance(operand, TypedExpression):
            dependencies = operand.dependencies
            allowed_aliases = {
                alias.casefold(): (alias, casefold_map),
            }
            self._validate_asof_expression_dependencies(
                dependencies,
                allowed_aliases=allowed_aliases,
                context=relation_label,
            )
            return operand.render(), dependencies

        msg = (
            f"{relation_label} must be a column name or typed expression "
            f"(got {type(operand)!r})"
        )
        raise TypeError(msg)

    def _normalise_asof_tolerance(
        self,
        tolerance: object,
        *,
        left_alias: str,
        right_alias: str,
        left_casefold_map: Mapping[str, list[str]],
        right_casefold_map: Mapping[str, list[str]],
    ) -> str:
        if isinstance(tolerance, bool):
            msg = "asof tolerance cannot be a boolean"
            raise TypeError(msg)

        if isinstance(tolerance, (int, float)):
            return repr(tolerance)

        if isinstance(tolerance, str):
            sql = tolerance.strip()
            if not sql:
                msg = "asof tolerance cannot be empty"
                raise ValueError(msg)
            return sql

        if isinstance(tolerance, TypedExpression):
            dependencies = tolerance.dependencies
            allowed_aliases = {
                left_alias.casefold(): (left_alias, left_casefold_map),
                right_alias.casefold(): (right_alias, right_casefold_map),
            }
            self._validate_asof_expression_dependencies(
                dependencies,
                allowed_aliases=allowed_aliases,
                context="asof tolerance expression",
            )
            return tolerance.render()

        msg = (
            "asof tolerance must be a SQL string, numeric literal, or typed expression "
            f"(got {type(tolerance)!r})"
        )
        raise TypeError(msg)

    def _validate_asof_expression_dependencies(
        self,
        dependencies: frozenset[ExpressionDependency],
        *,
        allowed_aliases: Mapping[str, tuple[str, Mapping[str, list[str]]]],
        context: str,
    ) -> None:
        if not dependencies:
            return

        alias_items = {
            key.casefold(): (name, mapping)
            for key, (name, mapping) in allowed_aliases.items()
        }

        default_alias: tuple[str, Mapping[str, list[str]]] | None
        if len(alias_items) == 1:
            default_alias = next(iter(alias_items.values()))
        else:
            default_alias = None

        allowed_names = ", ".join(sorted(name for name, _ in alias_items.values()))

        for dependency in dependencies:
            column = dependency.column_name
            table = dependency.table_name

            if column is None:
                msg = f"{context} cannot reference entire tables"
                raise ValueError(msg)

            if table is None:
                if default_alias is None:
                    msg = (
                        f"{context} must qualify column '{column}' with one of: {allowed_names}"
                    )
                    raise ValueError(msg)
                alias_name, casefold_map = default_alias
            else:
                alias_key = table.casefold()
                try:
                    alias_name, casefold_map = alias_items[alias_key]
                except KeyError as error:
                    msg = (
                        f"{context} references unknown table alias '{table}'. "
                        f"Expected one of: {allowed_names}"
                    )
                    raise ValueError(msg) from error

            self._resolve_casefolded_column(
                casefold_map,
                column,
                relation_label=f"{context} alias '{alias_name}'",
            )

    def _build_join_projection_entries(
        self,
        other: "Relation",
        left_alias: str,
        right_alias: str,
        *,
        include_right_columns: bool,
    ) -> list[str]:
        left_identifier = self._quote_identifier(left_alias)
        right_identifier = self._quote_identifier(right_alias)

        entries: list[str] = []
        for column in self.columns:
            reference = f"{left_identifier}.{self._quote_identifier(column)}"
            entries.append(f"{reference} AS {self._quote_identifier(column)}")

        if include_right_columns:
            left_casefolds = {column.casefold() for column in self.columns}
            for column in other.columns:
                if column.casefold() in left_casefolds:
                    continue
                reference = f"{right_identifier}.{self._quote_identifier(column)}"
                entries.append(f"{reference} AS {self._quote_identifier(column)}")

        return entries

    @staticmethod
    def _build_casefold_map(columns: Iterable[str]) -> dict[str, list[str]]:
        mapping: dict[str, list[str]] = {}
        for column in columns:
            mapping.setdefault(column.casefold(), []).append(column)
        return mapping

    @classmethod
    def _resolve_casefolded_column(
        cls,
        casefold_map: Mapping[str, list[str]],
        column: str,
        *,
        relation_label: str,
    ) -> str:
        matches = casefold_map.get(column.casefold())
        if not matches:
            msg = f"{relation_label} join column '{column}' does not exist"
            raise KeyError(msg)

        if len(matches) > 1:
            formatted = cls._format_column_list(matches)
            msg = (
                f"{relation_label} join column '{column}' is ambiguous; "
                f"matches: {formatted}. Rename columns to disambiguate"
            )
            raise ValueError(msg)

        return matches[0]

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

    def filter(self, *conditions: object) -> "Relation":
        """Return a new relation filtered by the provided conditions."""

        if not conditions:
            msg = "filter requires at least one condition"
            raise ValueError(msg)

        if not self.duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call filter. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        filter_clauses = self._normalise_filter_clauses(
            conditions,
            label_prefix="filter",
            error_context="Filters",
        )

        working_relation = self._relation
        for clause_sql, dependencies, label in filter_clauses:
            if dependencies is not None:
                self._assert_dependencies_exist(
                    dependencies,
                    error_prefix=label,
                )
            try:
                working_relation = working_relation.filter(clause_sql)
            except duckdb.BinderException as error:
                msg = f"{label} references unknown columns"
                raise ValueError(msg) from error

        return type(self).from_relation(self.duckcon, working_relation)

    def _normalise_filter_clauses(
        self,
        filters: tuple[object, ...],
        *,
        label_prefix: str,
        error_context: str,
    ) -> list[tuple[str, frozenset[ExpressionDependency] | None, str]]:
        clauses: list[tuple[str, frozenset[ExpressionDependency] | None, str]] = []
        for index, condition in enumerate(filters, start=1):
            label = f"{label_prefix} condition {index}"
            if isinstance(condition, str):
                sql = condition.strip()
                if not sql:
                    msg = f"{label} cannot be empty"
                    raise ValueError(msg)
                clauses.append((sql, None, label))
                continue

            if isinstance(condition, TypedExpression):
                boolean_expression = self._unwrap_boolean_expression(
                    condition,
                    error_context=error_context,
                )
                clauses.append(
                    (
                        boolean_expression.render(),
                        boolean_expression.dependencies,
                        label,
                    )
                )
                continue

            msg = (
                f"{error_context} must be SQL strings or boolean typed expressions"
            )
            raise TypeError(msg)

        return clauses

    @staticmethod
    def _resolve_column_from_casefolded(
        column: str,
        casefolded_columns: Mapping[str, tuple[str, ...]],
    ) -> str | None:
        matches = casefolded_columns.get(column.casefold())
        if matches is None:
            return None
        if len(matches) > 1:
            formatted = Relation._format_column_list(matches)
            msg = (
                f"Column reference '{column}' is ambiguous; multiple columns match ignoring "
                f"case: {formatted}"
            )
            raise ValueError(msg)
        return matches[0]

    def _resolve_column(self, column: str) -> str | None:
        return self._resolve_column_from_casefolded(column, self._casefolded_columns)

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

    def _prepare_sample_relation(
        self, limit: int | None, *, helper: str
    ) -> duckdb.DuckDBPyRelation:
        if limit is None:
            return self._relation.project("*")
        if not isinstance(limit, int):
            msg = f"{helper} limit must be an integer or None"
            raise TypeError(msg)
        if limit <= 0:
            msg = f"{helper} limit must be greater than zero"
            raise ValueError(msg)
        return self._relation.limit(limit)

    @staticmethod
    def _normalise_batch_size(batch_size: int, helper: str) -> int:
        if not isinstance(batch_size, int):
            msg = f"{helper} batch_size must be an integer"
            raise TypeError(msg)
        if batch_size <= 0:
            msg = f"{helper} batch_size must be greater than zero"
            raise ValueError(msg)
        return batch_size

    @staticmethod
    def _iterate_pandas_batches(
        relation: duckdb.DuckDBPyRelation, batch_size: int
    ) -> Iterator[Any]:
        while True:
            chunk = relation.fetch_df_chunk(batch_size)
            if chunk is None:
                break
            if len(chunk) == 0:  # pragma: no cover - depends on pandas behaviour
                break
            yield chunk

    @staticmethod
    def _iterate_arrow_batches(reader: Any, pyarrow_module: Any) -> Iterator[Any]:
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break
            if batch is None:  # pragma: no cover - defensive
                break
            yield pyarrow_module.Table.from_batches([batch])

    @staticmethod
    def _iterate_polars_batches(
        relation: duckdb.DuckDBPyRelation,
        batch_size: int,
        polars_module: Any,
        columns: Sequence[str],
    ) -> Iterator[Any]:
        while True:
            rows = relation.fetchmany(batch_size)
            if not rows:
                break
            yield polars_module.DataFrame(rows, schema=list(columns), orient="row")

    @staticmethod
    def _require_module(module: str, helper: str, install_hint: str) -> Any:
        try:
            return import_module(module)
        except ModuleNotFoundError as exc:
            msg = f"{helper} requires {module}. Install it with {install_hint}."
            raise ModuleNotFoundError(msg) from exc

    @staticmethod
    def _normalise_identifier_sequence(
        columns: Sequence[str] | str | None,
        *,
        helper: str,
        parameter: str,
    ) -> tuple[str, ...] | None:
        """Normalise identifier inputs used by file append helpers."""

        if columns is None:
            return None

        if isinstance(columns, str):
            values = [columns]
        else:
            if not isinstance(columns, Sequence):
                msg = f"{helper} {parameter} must be a string or sequence of strings"
                raise TypeError(msg)
            values = list(columns)

        normalised: list[str] = []
        seen: set[str] = set()
        for column in values:
            if not isinstance(column, str):
                msg = f"{helper} {parameter} must contain only strings"
                raise TypeError(msg)
            trimmed = column.strip()
            if not trimmed:
                msg = f"{helper} {parameter} entries cannot be empty"
                raise ValueError(msg)
            key = trimmed.casefold()
            if key in seen:
                msg = f"{helper} {parameter} '{column}' specified multiple times"
                raise ValueError(msg)
            seen.add(key)
            normalised.append(trimmed)

        if not normalised:
            msg = f"{helper} {parameter} must contain at least one column"
            raise ValueError(msg)

        return tuple(normalised)

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        escaped = identifier.replace("\"", "\"\"")
        return f'"{escaped}"'

    @staticmethod
    def _quote_qualified_identifier(identifier: str) -> str:
        parts = identifier.split(".")
        quoted_parts: list[str] = []
        for part in parts:
            trimmed = part.strip()
            if not trimmed:
                msg = "Qualified identifier segments cannot be empty"
                raise ValueError(msg)
            quoted_parts.append(Relation._quote_identifier(trimmed))
        return ".".join(quoted_parts)

    def _deduplicate_against_existing(
        self,
        *,
        helper: str,
        existing_relation: duckdb.DuckDBPyRelation | None,
        unique_id_columns: tuple[str, ...] | None,
        match_all_columns: bool,
    ) -> duckdb.DuckDBPyRelation:
        if unique_id_columns is not None and match_all_columns:
            msg = (
                f"{helper} cannot specify both unique_id_column and match_all_columns"
            )
            raise ValueError(msg)

        if existing_relation is None:
            return self._relation

        join_columns: list[str]
        if unique_id_columns is not None:
            join_columns = [
                self._require_column(column, helper, parameter="unique_id_column")
                for column in unique_id_columns
            ]
        elif match_all_columns:
            join_columns = list(self.columns)
        else:
            return self._relation

        if not join_columns:
            return self._relation

        existing_columns = tuple(existing_relation.columns)
        existing_map = {column.casefold(): column for column in existing_columns}

        missing = [
            column for column in join_columns if column.casefold() not in existing_map
        ]
        if missing:
            formatted = self._format_column_list(missing)
            msg = f"{helper} columns missing from target file: {formatted}"
            raise ValueError(msg)

        if match_all_columns:
            source_map = {column.casefold(): column for column in self.columns}
            extra = [
                column
                for column in existing_columns
                if column.casefold() not in source_map
            ]
            if extra:
                formatted = self._format_column_list(extra)
                msg = (
                    f"{helper} target file contains columns not present on the relation:"
                    f" {formatted}"
                )
                raise ValueError(msg)

        try:
            condition = cast(Any, join_columns)
            return self._relation.join(existing_relation, condition, how="anti")
        except duckdb.BinderException as error:
            formatted = self._format_column_list(join_columns)
            msg = f"{helper} could not compare rows using columns: {formatted}"
            raise ValueError(msg) from error

    def _require_column(
        self,
        column: str,
        helper: str,
        *,
        parameter: str,
    ) -> str:
        resolved = self._resolve_column(column)
        if resolved is None:
            msg = f"{helper} {parameter} '{column}' does not exist on the relation"
            raise ValueError(msg)
        return resolved

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
        self, alias: str, expression: TypedExpression
    ) -> tuple[str, frozenset[ExpressionDependency] | None]:
        if not isinstance(expression, TypedExpression):
            msg = (
                "add expressions must be typed expressions representing the new "
                f"column definition (got {type(expression)!r})"
            )
            raise TypeError(msg)

        typed_expression = self._unwrap_expression_for_alias(
            alias,
            expression,
            context=(
                "Aliased expressions passed to add must use the same alias as "
                "the target column"
            ),
        )
        return typed_expression.render(), typed_expression.dependencies

    def _normalise_aggregate_expression(
        self, alias: str, expression: TypedExpression
    ) -> tuple[str, frozenset[ExpressionDependency] | None]:
        if not isinstance(expression, TypedExpression):
            msg = (
                "aggregate expressions must be typed expressions representing the "
                f"aggregation (got {type(expression)!r})"
            )
            raise TypeError(msg)

        typed_expression = self._unwrap_expression_for_alias(
            alias,
            expression,
            context=(
                "Aliased expressions passed to aggregate must use the same alias as "
                "the target column"
            ),
        )
        return typed_expression.render(), typed_expression.dependencies

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
        self,
        expression: TypedExpression,
        *,
        error_context: str,
    ) -> BooleanExpression:
        current = expression
        while isinstance(current, AliasedExpression):
            current = current.base
        if not isinstance(current, BooleanExpression) and not isinstance(
            current.duck_type, BooleanType
        ):
            msg = f"{error_context} must be boolean expressions"
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
        *,
        casefolded_columns: Mapping[str, tuple[str, ...]] | None = None,
    ) -> None:
        self._assert_dependencies_exist(
            dependencies,
            error_prefix=f"add expression for column '{alias}'",
            casefolded_columns=casefolded_columns,
        )

    def _assert_dependencies_exist(
        self,
        dependencies: frozenset[ExpressionDependency],
        *,
        error_prefix: str,
        casefolded_columns: Mapping[str, tuple[str, ...]] | None = None,
    ) -> None:
        lookup = self._casefolded_columns if casefolded_columns is None else casefolded_columns
        for dependency in dependencies:
            column = dependency.column_name
            if column is None:
                continue
            try:
                resolved = self._resolve_column_from_casefolded(column, lookup)
            except ValueError as error:  # pragma: no cover - defensive
                msg = (
                    f"{error_prefix} references ambiguous column '{column}'"
                )
                raise ValueError(msg) from error
            if resolved is None:
                msg = f"{error_prefix} references unknown columns"
                raise ValueError(msg)
