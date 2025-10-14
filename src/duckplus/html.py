"""HTML rendering helpers for Duck+."""
from __future__ import annotations

from collections.abc import Sequence
from html import escape as html_escape
from typing import Any

from .core import DuckRel
from .schema import AnyRow


def _quote_identifier(identifier: str) -> str:
    """Return *identifier* quoted for SQL usage."""

    return f'"{identifier.replace("\"", "\"\"")}"'


def _sql_literal(value: str) -> str:
    """Return a SQL string literal for *value*."""

    return "'" + value.replace("'", "''") + "'"


def _validate_attributes(style: dict[str, Any]) -> str:
    """Return serialized table attributes from *style*."""

    allowed_attrs = {"class", "id"}
    fragments: list[str] = []
    for key, raw_value in style.items():
        attr = key[:-1] if key.endswith("_") else key
        if attr not in allowed_attrs:
            raise ValueError(
                "Unsupported HTML attribute provided; only 'class' and 'id' attributes are permitted."
            )
        value = html_escape(str(raw_value), quote=True)
        fragments.append(f" {attr}=\"{value}\"")
    return "".join(fragments)


def _build_row_expression(columns: Sequence[str], null_literal: str) -> str:
    """Return the SQL expression that renders a table row."""

    if not columns:
        return "'<tr></tr>'"

    cells: list[str] = []
    replacements = [
        ("&", "&amp;"),
        ("<", "&lt;"),
        (">", "&gt;"),
        ('"', "&quot;"),
        ("'", "&#x27;"),
    ]
    for column in columns:
        identifier = _quote_identifier(column)
        value_ref = f"numbered.{identifier}"
        escaped = f"CAST({value_ref} AS VARCHAR)"
        for source, target in replacements:
            escaped = f"REPLACE({escaped}, {_sql_literal(source)}, {_sql_literal(target)})"
        value_expression = (
            f"CASE WHEN {value_ref} IS NULL THEN {null_literal} ELSE {escaped} END"
        )
        cells.append(f"'<td>' || {value_expression} || '</td>'")

    inner = " || ".join(cells)
    return f"'<tr>' || {inner} || '</tr>'"


def to_html(
    rel: DuckRel[AnyRow],
    *,
    max_rows: int = 100,
    null_display: str = "",
    **style: Any,
) -> str:
    """Return an HTML table preview of *rel* limited to *max_rows* rows.

    Values are escaped inside DuckDB before rendering to avoid materializing Python objects unnecessarily. When the
    relation contains more than ``max_rows`` rows a ``<tfoot>`` entry summarizes how many records were not rendered.
    """

    if not isinstance(max_rows, int):
        raise TypeError("max_rows must be provided as an integer.")
    if max_rows < 0:
        raise ValueError("max_rows must be non-negative.")

    table_attributes = _validate_attributes(dict(style))
    columns = rel.columns
    escaped_headers = "".join(f"<th>{html_escape(name)}</th>" for name in columns)
    header_html = f"<thead><tr>{escaped_headers}</tr></thead>"

    relation = rel.relation
    total_count = rel.row_count()

    body_rows_html = ""
    if max_rows > 0 and total_count > 0:
        null_literal = _sql_literal(html_escape(str(null_display)))
        row_expression = _build_row_expression(columns, null_literal)
        query = f"""
            WITH limited AS (
                SELECT *
                FROM __duckplus_rel
                LIMIT {max_rows}
            ),
            numbered AS (
                SELECT *, ROW_NUMBER() OVER () AS __rownum
                FROM limited
            )
            SELECT
                COALESCE(
                    STRING_AGG({row_expression}, '' ORDER BY __rownum),
                    ''
                )
            FROM numbered
        """
        result = relation.query("__duckplus_rel", query).fetchone()
        body_rows_html = str(result[0]) if result and result[0] is not None else ""

    tbody_html = f"<tbody>{body_rows_html}</tbody>"

    shown = min(max_rows, total_count)
    footer_html = ""
    if total_count > max_rows:
        remaining = total_count - max_rows
        row_word = "row" if remaining == 1 else "rows"
        colspan = max(len(columns), 1)
        summary = (
            f"Showing first {shown} of {total_count} rows "
            f"({remaining} more {row_word} not shown)."
        )
        footer_html = f"<tfoot><tr><td colspan=\"{colspan}\">{summary}</td></tr></tfoot>"

    return f"<table{table_attributes}>{header_html}{tbody_html}{footer_html}</table>"
