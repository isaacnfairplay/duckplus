"""Shared type-checker contract scenarios for static typed integrations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from textwrap import dedent, indent
from typing import Iterable, Mapping

from duckplus.static_typed import ducktype


def _public_attributes(
    obj: object,
    *,
    skip_prefixes: Iterable[str] = ("_",),
) -> list[str]:
    """Return sorted public attribute names for ``obj`` respecting ``skip_prefixes``."""

    prefixes = tuple(skip_prefixes)
    return sorted(
        {
            name
            for name in dir(obj)
            if not any(name.startswith(prefix) for prefix in prefixes)
        }
    )


def _render_attribute_block(
    variable: str,
    initializer: str,
    attributes: Iterable[str],
) -> str:
    names = list(attributes)
    if not names:
        return ""
    lines = [f"{variable} = {initializer}"]
    for name in names:
        lines.append(f"_ = {variable}.{name}")
    return "\n".join(lines)


def _attribute_contract_source() -> str:
    """Dynamically build a ty contract covering all public expression attributes."""

    numeric_expr = ducktype.Numeric.literal(Decimal("1"))
    boolean_expr = ducktype.Boolean.literal(True)
    varchar_expr = ducktype.Varchar.literal("value")
    blob_expr = ducktype.Blob.literal(b"payload")
    generic_expr = ducktype.Generic("payload")
    date_expr = ducktype.Date.literal(date(2024, 1, 1))
    timestamp_expr = ducktype.Timestamp.literal(datetime(2024, 1, 1))
    row_number_expr = ducktype.row_number()

    numeric_factory = ducktype.Numeric
    boolean_factory = ducktype.Boolean
    varchar_factory = ducktype.Varchar
    blob_factory = ducktype.Blob
    generic_factory = ducktype.Generic
    date_factory = ducktype.Date
    timestamp_factory = ducktype.Timestamp

    numeric_aggregate = ducktype.Numeric.Aggregate
    generic_aggregate = ducktype.Generic.Aggregate
    temporal_aggregate = ducktype.Timestamp.Aggregate

    numeric_case = ducktype.Numeric.case()
    boolean_case = ducktype.Boolean.case()
    varchar_case = ducktype.Varchar.case()
    temporal_case = ducktype.Timestamp.case()

    select_builder = ducktype.select()

    blocks = [
        _render_attribute_block(
            "numeric_expr",
            "ducktype.Numeric.literal(Decimal('1'))",
            _public_attributes(numeric_expr),
        ),
        _render_attribute_block(
            "boolean_expr",
            "ducktype.Boolean.literal(True)",
            _public_attributes(boolean_expr),
        ),
        _render_attribute_block(
            "varchar_expr",
            "ducktype.Varchar.literal('value')",
            _public_attributes(varchar_expr),
        ),
        _render_attribute_block(
            "blob_expr",
            "ducktype.Blob.literal(b'payload')",
            _public_attributes(blob_expr),
        ),
        _render_attribute_block(
            "generic_expr",
            "ducktype.Generic('payload')",
            _public_attributes(generic_expr),
        ),
        _render_attribute_block(
            "date_expr",
            "ducktype.Date.literal(date(2024, 1, 1))",
            _public_attributes(date_expr),
        ),
        _render_attribute_block(
            "timestamp_expr",
            "ducktype.Timestamp.literal(datetime(2024, 1, 1))",
            _public_attributes(timestamp_expr),
        ),
        _render_attribute_block(
            "row_number_expr",
            "ducktype.row_number()",
            _public_attributes(row_number_expr),
        ),
        _render_attribute_block(
            "numeric_factory",
            "ducktype.Numeric",
            _public_attributes(numeric_factory),
        ),
        _render_attribute_block(
            "boolean_factory",
            "ducktype.Boolean",
            _public_attributes(boolean_factory),
        ),
        _render_attribute_block(
            "varchar_factory",
            "ducktype.Varchar",
            _public_attributes(varchar_factory),
        ),
        _render_attribute_block(
            "blob_factory",
            "ducktype.Blob",
            _public_attributes(blob_factory),
        ),
        _render_attribute_block(
            "generic_factory",
            "ducktype.Generic",
            _public_attributes(generic_factory),
        ),
        _render_attribute_block(
            "date_factory",
            "ducktype.Date",
            _public_attributes(date_factory),
        ),
        _render_attribute_block(
            "timestamp_factory",
            "ducktype.Timestamp",
            _public_attributes(timestamp_factory),
        ),
        _render_attribute_block(
            "numeric_aggregate",
            "ducktype.Numeric.Aggregate",
            _public_attributes(numeric_aggregate),
        ),
        _render_attribute_block(
            "generic_aggregate",
            "ducktype.Generic.Aggregate",
            _public_attributes(generic_aggregate),
        ),
        _render_attribute_block(
            "temporal_aggregate",
            "ducktype.Timestamp.Aggregate",
            _public_attributes(temporal_aggregate),
        ),
        _render_attribute_block(
            "numeric_case",
            "ducktype.Numeric.case()",
            _public_attributes(numeric_case),
        ),
        _render_attribute_block(
            "boolean_case",
            "ducktype.Boolean.case()",
            _public_attributes(boolean_case),
        ),
        _render_attribute_block(
            "varchar_case",
            "ducktype.Varchar.case()",
            _public_attributes(varchar_case),
        ),
        _render_attribute_block(
            "temporal_case",
            "ducktype.Timestamp.case()",
            _public_attributes(temporal_case),
        ),
        _render_attribute_block(
            "select_builder",
            "ducktype.select()",
            _public_attributes(select_builder),
        ),
    ]

    contract_body = "\n\n".join(block for block in blocks if block)

    header = (
        "from __future__ import annotations\n\n"
        "from datetime import date, datetime\n"
        "from decimal import Decimal\n\n"
        "from duckplus.static_typed import ducktype\n\n"
        "def check_attributes() -> None:\n"
    )

    if contract_body:
        indented_body = indent(contract_body, "    ")
    else:  # pragma: no cover - defensive guard
        indented_body = "    pass"

    footer = "\n\ncheck_attributes()"
    return f"{header}{indented_body}{footer}"


@dataclass(frozen=True)
class CheckerExpectation:
    """Expectation for a static type checker run."""

    ok: bool
    diagnostic: str | None = None

    def normalised_diagnostic(self) -> str | None:
        if self.diagnostic is None:
            return None
        return self.diagnostic.lower()


@dataclass(frozen=True)
class TypeCheckCase:
    """Reusable scenario executed against one or more type checkers."""

    name: str
    filename: str
    source: str
    expectations: Mapping[str, CheckerExpectation]

    def rendered_source(self) -> str:
        return dedent(self.source).strip() + "\n"

    def expectation_for(self, tool: str) -> CheckerExpectation | None:
        return self.expectations.get(tool)


TYPECHECK_CASES: tuple[TypeCheckCase, ...] = (
    TypeCheckCase(
        name="invalid-generic-sum",
        filename="invalid_generic_sum.py",
        source="""
        from duckplus.static_typed import ducktype


        def demo() -> None:
            expr = ducktype.Generic("customer")
            expr.sum()
        """,
        expectations={
            "mypy": CheckerExpectation(
                ok=False,
                diagnostic='has no attribute "sum"',
            ),
            "ty": CheckerExpectation(
                ok=False,
                diagnostic="possibly-missing-attribute",
            ),
        },
    ),
    TypeCheckCase(
        name="numeric-missing-attribute",
        filename="numeric_missing_attribute.py",
        source="""
        from duckplus.static_typed.ducktype import Numeric


        def demo() -> None:
            Numeric.literal(1).contains("value")
        """,
        expectations={
            "ty": CheckerExpectation(
                ok=False,
                diagnostic="unresolved-attribute",
            )
        },
    ),
    TypeCheckCase(
        name="numeric-table-argument",
        filename="numeric_table_argument.py",
        source="""
        from duckplus.static_typed.ducktype import Numeric


        def make_column() -> None:
            Numeric("amount", table=1)
        """,
        expectations={
            "ty": CheckerExpectation(
                ok=False,
                diagnostic="invalid-argument-type",
            )
        },
    ),
    TypeCheckCase(
        name="select-alias-nonstring",
        filename="select_alias_nonstring.py",
        source="""
        from duckplus.static_typed import ducktype


        def build() -> None:
            builder = ducktype.select()
            builder.column(ducktype.Numeric("amount"), alias=123)
        """,
        expectations={
            "ty": CheckerExpectation(
                ok=False,
                diagnostic="invalid-argument-type",
            )
        },
    ),
    TypeCheckCase(
        name="select-from-nonstring",
        filename="select_from_nonstring.py",
        source="""
        from duckplus.static_typed import ducktype


        def build() -> None:
            ducktype.select().from_(123)
        """,
        expectations={
            "ty": CheckerExpectation(
                ok=False,
                diagnostic="invalid-argument-type",
            )
        },
    ),
    TypeCheckCase(
        name="varchar-literal-nonstring",
        filename="varchar_literal_nonstring.py",
        source="""
        from duckplus.static_typed.ducktype import Varchar


        def make_literal() -> None:
            Varchar.literal(123)
        """,
        expectations={
            "ty": CheckerExpectation(
                ok=False,
                diagnostic="invalid-argument-type",
            )
        },
    ),
    TypeCheckCase(
        name="boolean-literal-nonbool",
        filename="boolean_literal_nonbool.py",
        source="""
        from duckplus.static_typed.ducktype import Boolean


        def make_boolean() -> None:
            Boolean.literal(1)
        """,
        expectations={
            "ty": CheckerExpectation(
                ok=False,
                diagnostic="invalid-argument-type",
            )
        },
    ),
    TypeCheckCase(
        name="expression-type-mismatch",
        filename="expression_type_mismatch.py",
        source="""
        from duckplus.static_typed.ducktype import Numeric
        from duckplus.static_typed.expression import BooleanExpression


        def expects_bool(expr: BooleanExpression) -> None:
            pass


        def demo() -> None:
            expects_bool(Numeric.literal(1))
        """,
        expectations={
            "ty": CheckerExpectation(
                ok=False,
                diagnostic="invalid-argument-type",
            )
        },
    ),
    TypeCheckCase(
        name="assignment-mismatch",
        filename="assignment_mismatch.py",
        source="""
        from duckplus.static_typed.ducktype import Varchar
        from duckplus.static_typed.expression import NumericExpression


        expr: NumericExpression = Varchar.literal("value")
        """,
        expectations={
            "ty": CheckerExpectation(
                ok=False,
                diagnostic="invalid-assignment",
            )
        },
    ),
    TypeCheckCase(
        name="numeric-factory-usage",
        filename="typed_ducktype_usage.py",
        source="""
        from duckplus.static_typed.ducktype import Numeric, select


        def demo() -> None:
            builder = select()
            expr = Numeric.literal(1)
            builder = builder.column(expr, alias="value")
            builder.build()
        """,
        expectations={
            "mypy": CheckerExpectation(ok=True),
            "ty": CheckerExpectation(ok=True),
        },
    ),
    TypeCheckCase(
        name="composite-builder-usage",
        filename="composite_builder_usage.py",
        source="""
        from duckplus.static_typed import ducktype


        def build_statement() -> None:
            amount = ducktype.Numeric("amount", table="orders")
            threshold = ducktype.Numeric.literal(100)
            over_threshold = amount > threshold
            status = (
                ducktype.Varchar.literal("high")
                .coalesce("unknown")
                .trim()
            )
            categorised = (
                ducktype.Numeric.case()
                .when(over_threshold, amount)
                .else_(threshold)
                .end()
            )
            total = amount.sum()
            builder = (
                ducktype.select()
                .column(categorised, alias="categorised")
                .column(total, alias="total")
                .column(status, alias="status")
                .from_("orders")
            )
            builder.build()
        """,
        expectations={
            "ty": CheckerExpectation(ok=True),
        },
    ),
    TypeCheckCase(
        name="static-typed-api-surface",
        filename="static_typed_api_surface.py",
        source="""
        from __future__ import annotations

        from datetime import date, datetime
        from decimal import Decimal

        from duckplus.static_typed import (
            AGGREGATE_FUNCTIONS,
            SCALAR_FUNCTIONS,
            WINDOW_FUNCTIONS,
            Blob,
            Decimal_18_2,
            Boolean,
            Date,
            Double,
            ExpressionDependency,
            Float,
            Generic,
            Integer,
            Numeric,
            Smallint,
            Tinyint,
            Timestamp,
            Timestamp_ms,
            Timestamp_ns,
            Timestamp_s,
            Timestamp_tz,
            Timestamp_us,
            Utinyint,
            Usmallint,
            Uinteger,
            Varchar,
            ducktype,
            select,
        )
        from duckplus.static_typed.expression import (
            AliasedExpression,
            BlobExpression,
            BlobFactory,
            BooleanExpression,
            BooleanFactory,
            CaseExpressionBuilder,
            DateExpression,
            DuckTypeNamespace,
            GenericExpression,
            GenericFactory,
            NumericAggregateFactory,
            NumericExpression,
            NumericFactory,
            SelectStatementBuilder,
            TemporalAggregateFactory,
            TypedExpression,
            VarcharExpression,
            VarcharFactory,
            _format_numeric,
            _quote_identifier,
            _quote_string,
        )
        from duckplus.static_typed import DuckDBFunctionNamespace
        from duckplus.static_typed.types import (
            BooleanType,
            DecimalType,
            DuckDBType,
            FloatingType,
            GenericType,
            IntegerType,
            NumericType,
            TemporalType,
            UnknownType,
            VarcharType,
            infer_numeric_literal_type,
            parse_type,
        )


        def exercise_api(namespace: DuckTypeNamespace = ducktype) -> None:
            number_column: NumericExpression = namespace.Numeric("amount", table="orders")
            number_literal = namespace.Numeric.literal(Decimal("10.50"))
            arithmetic = (number_column + number_literal).coalesce(Decimal("0"), 5).abs()
            powered = arithmetic.pow(2)
            null_safe = powered.nullif(0)
            summed = namespace.Numeric.Aggregate.sum(number_column)
            averaged = namespace.Numeric.Aggregate.avg(number_column)
            counted = namespace.Numeric.Aggregate.count()
            counted_if = namespace.Numeric.Aggregate.count_if(namespace.Boolean.literal(True))
            filtered = namespace.Numeric.Aggregate.sum_filter(namespace.Boolean("include"), ("orders", "amount"))
            maxed_by = namespace.Numeric.Aggregate.max_by(number_column, ("orders", "amount"))
            mined_by = namespace.Numeric.Aggregate.min_by(number_column, 1)

            boolean_column: BooleanExpression = namespace.Boolean("is_active", table="orders")
            combined_boolean = (~boolean_column) & namespace.Boolean.literal(True) | boolean_column
            boolean_case = namespace.Boolean.case().when(True, boolean_column).otherwise(False).end()

            varchar_column: VarcharExpression = namespace.Varchar("name", table="orders")
            varchar_chain = (
                varchar_column.coalesce(namespace.Varchar.literal("unknown"))
                .trim()
                .slice(1, 5)
            )
            contains = varchar_chain.contains("A")
            starts = varchar_chain.starts_with("A")
            varchar_length = varchar_chain.length()
            concatenated = varchar_chain + "!"
            mirrored = "Hello, " + concatenated
            varchar_case = (
                namespace.Varchar.case()
                .when(True, "value")
                .else_(namespace.Varchar.literal("fallback"))
                .end()
            )

            blob_column: BlobExpression = namespace.Blob("payload")
            blob_literal = namespace.Blob.literal(b"data")
            blob_alias: AliasedExpression = blob_literal.alias("payload_alias")

            generic_column: GenericExpression = namespace.Generic("payload")
            generic_null = namespace.Generic.null()
            generic_case = namespace.Generic.case().when(True, generic_column).else_(generic_null).end()
            generic_max = namespace.Generic.Aggregate.max(generic_column)
            generic_min_by = namespace.Generic.Aggregate.min_by(generic_column, number_column)

            row_number_expr: NumericExpression = namespace.row_number()
            aliased_row = row_number_expr.alias("row_number")
            windowed = aliased_row.over(
                partition_by=[varchar_column],
                order_by=[number_column],
                frame="ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            )

            decimal_literal = Decimal_18_2.literal(Decimal("1.23"))
            date_literal = namespace.Date.literal(date.today())
            date_expression = DateExpression.column("created")
            timestamp_literal = namespace.Timestamp.literal(datetime.now())
            timestamp_ns_literal = namespace.Timestamp_ns.literal(datetime.now())
            timestamp_ms_literal = namespace.Timestamp_ms.literal(datetime.now())
            timestamp_us_literal = namespace.Timestamp_us.literal(datetime.now())
            timestamp_s_literal = namespace.Timestamp_s.literal(datetime.now())
            timestamp_tz_literal = namespace.Timestamp_tz.literal(datetime.now())
            temporal_case = (
                namespace.Timestamp.case()
                .when(True, timestamp_literal)
                .else_(timestamp_tz_literal)
                .end()
            )
            temporal_coalesce = timestamp_literal.coalesce(timestamp_tz_literal)
            temporal_max = namespace.Timestamp.Aggregate.max(timestamp_literal)
            temporal_min_by = namespace.Timestamp.Aggregate.min_by(timestamp_literal, number_column)

            numeric_case_builder: CaseExpressionBuilder[NumericExpression] = namespace.Numeric.case()
            numeric_case = (
                numeric_case_builder.when(boolean_column, number_column)
                .when(namespace.Boolean.literal(False), namespace.Numeric.literal(0))
                .else_(Decimal("10.00"))
                .end()
            )

            builder: SelectStatementBuilder = select()
            select_builder = (
                builder.column(numeric_case, alias="computed")
                .column(blob_alias, if_exists=True)
                .star(
                    replace={"subtotal": number_column},
                    exclude=["legacy_column"],
                    replace_if_exists=[("bonus", namespace.Numeric("bonus"))],
                    exclude_if_exists=["maybe_missing"],
                )
                .from_("orders")
            )
            select_list = select_builder.build_select_list(
                available_columns=["legacy_column", "maybe_missing"]
            )
            select_sql = select_builder.build(
                available_columns=["legacy_column", "maybe_missing"]
            )

            cast_target = parse_type("INTEGER")
            assert cast_target is not None
            cast_expression = number_column.cast(cast_target)
            try_cast_expression = number_column.try_cast("INTEGER")
            window_cast = cast_expression.over(
                partition_by=number_column,
                order_by=number_column,
            )

            column_dependency = ExpressionDependency.column("amount", table="orders")
            table_dependency = ExpressionDependency.table("orders")

            numeric_factory: NumericFactory = namespace.Numeric
            varchar_factory: VarcharFactory = namespace.Varchar
            boolean_factory: BooleanFactory = namespace.Boolean
            blob_factory: BlobFactory = namespace.Blob
            generic_factory: GenericFactory = namespace.Generic
            timestamp_factory = namespace.Timestamp

            numeric_raw = numeric_factory._raw("RAW_SQL", dependencies=(column_dependency,))
            varchar_raw = varchar_factory._raw("SQL")
            boolean_raw = boolean_factory._raw("SQL")
            blob_raw = blob_factory._raw("SQL")
            generic_raw = generic_factory._raw("SQL")
            timestamp_raw = timestamp_factory._raw("CURRENT_TIMESTAMP")

            numeric_coerced = numeric_factory.coerce(("orders", "amount"))
            varchar_coerced = varchar_factory.coerce("label")
            boolean_coerced = boolean_factory.coerce(True)
            blob_coerced = blob_factory.coerce(b"data")
            temporal_coerced = timestamp_factory.coerce(("orders", "created_at"))

            numeric_aggregate_factory: NumericAggregateFactory = namespace.Numeric.Aggregate
            temporal_aggregate_factory: TemporalAggregateFactory = namespace.Timestamp.Aggregate

            inferred_type = infer_numeric_literal_type(Decimal("2.5"))
            numeric_type = NumericType("NUMERIC")
            integer_type = IntegerType("INTEGER")
            floating_type = FloatingType("DOUBLE")
            boolean_type = BooleanType("BOOLEAN")
            varchar_type = VarcharType("VARCHAR")
            generic_type = GenericType("UNKNOWN")
            temporal_type = TemporalType("TIMESTAMP")
            decimal_type = DecimalType(10, 2)
            unknown_type = UnknownType()

            formatted = _format_numeric(Decimal("1.0"))
            quoted_identifier = _quote_identifier("alias")
            quoted_string = _quote_string("value")

            function_namespace = DuckDBFunctionNamespace()
            scalar_namespace = function_namespace.Scalar
            aggregate_namespace = function_namespace.Aggregate
            window_namespace = function_namespace.Window

            scalar_call = scalar_namespace.Numeric.abs(number_column)
            aggregate_call = aggregate_namespace.Numeric.sum("amount")
            filtered_call = aggregate_namespace.Numeric.sum_filter(boolean_column, "amount")
            window_members = window_namespace.__dir__()
            symbol_call = scalar_namespace.Boolean.symbols["~~"](boolean_column, boolean_coerced)
            scalar_via_global = SCALAR_FUNCTIONS.Varchar.lower(varchar_column)

            decimal_names: tuple[str, ...] = namespace.decimal_factory_names

            alias_numeric = numeric_case.alias("value")
            alias_window = alias_numeric.over(order_by=[number_column])

            _ = (
                number_literal,
                arithmetic,
                powered,
                null_safe,
                summed,
                averaged,
                counted,
                counted_if,
                filtered,
                maxed_by,
                mined_by,
                combined_boolean,
                boolean_case,
                contains,
                starts,
                varchar_length,
                concatenated,
                mirrored,
                varchar_case,
                blob_alias,
                generic_case,
                generic_max,
                generic_min_by,
                row_number_expr,
                aliased_row,
                windowed,
                decimal_literal,
                date_literal,
                date_expression,
                temporal_case,
                temporal_coalesce,
                temporal_max,
                temporal_min_by,
                timestamp_literal,
                timestamp_ns_literal,
                timestamp_ms_literal,
                timestamp_us_literal,
                timestamp_s_literal,
                timestamp_tz_literal,
                numeric_case,
                cast_expression,
                try_cast_expression,
                window_cast,
                numeric_raw,
                varchar_raw,
                boolean_raw,
                blob_raw,
                generic_raw,
                timestamp_raw,
                numeric_coerced,
                varchar_coerced,
                boolean_coerced,
                blob_coerced,
                temporal_coerced,
                numeric_aggregate_factory,
                temporal_aggregate_factory,
                inferred_type,
                numeric_type,
                integer_type,
                floating_type,
                boolean_type,
                varchar_type,
                generic_type,
                temporal_type,
                decimal_type,
                unknown_type,
                formatted,
                quoted_identifier,
                quoted_string,
                function_namespace,
                scalar_namespace,
                aggregate_namespace,
                window_namespace,
                scalar_call,
                aggregate_call,
                filtered_call,
                window_members,
                symbol_call,
                scalar_via_global,
                decimal_names,
                alias_window,
            )
            _ = (
                select_builder,
                select_list,
                select_sql,
                column_dependency,
                table_dependency,
                cast_target,
            )


        def ensure_alias_exports() -> None:
            exported = (
                Numeric("amount"),
                Varchar("name"),
                Boolean("flag"),
                Blob("payload"),
                Generic("payload"),
                Tinyint("tiny"),
                Smallint("small"),
                Integer("identifier"),
                Utinyint("utiny"),
                Usmallint("usmall"),
                Uinteger("uvalue"),
                Float("float_col"),
                Double("double_col"),
                Date("created"),
                Timestamp("created_at"),
                Timestamp_s("created_s"),
                Timestamp_ms("created_ms"),
                Timestamp_us("created_us"),
                Timestamp_ns("created_ns"),
                Timestamp_tz("created_tz"),
            )
            builder = ducktype.select().column(Numeric.literal(1), alias="one").from_("dual")
            _ = exported, builder


        exercise_api()
        ensure_alias_exports()
        """,
        expectations={
            "ty": CheckerExpectation(ok=True),
            "mypy": CheckerExpectation(ok=True),
        },
    ),
    TypeCheckCase(
        name="static-typed-expression-attribute-coverage",
        filename="static_typed_expression_attribute_coverage.py",
        source=_attribute_contract_source(),
        expectations={
            "ty": CheckerExpectation(ok=True),
            "mypy": CheckerExpectation(ok=True),
        },
    ),
)


def cases_for(tool: str) -> tuple[TypeCheckCase, ...]:
    return tuple(case for case in TYPECHECK_CASES if case.expectation_for(tool) is not None)
