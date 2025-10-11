"""HTML rendering helpers for Duck+."""

from __future__ import annotations

from html import escape

from .core import DuckRel


def to_html(
    rel: DuckRel,
    *,
    max_rows: int = 100,
    table_class: str | None = None,
    table_id: str | None = None,
) -> str:
    """Render *rel* as an HTML table."""

    if max_rows <= 0:
        raise ValueError("max_rows must be a positive integer.")

    fetch = max_rows + 1
    limited = rel.limit(fetch)
    table = limited.materialize().require_table()
    column_names = rel.columns
    rows = table.to_pylist()

    truncated = len(rows) > max_rows
    visible = rows[:max_rows]

    attributes = _format_attributes(table_class=table_class, table_id=table_id)
    html_parts: list[str] = [f"<table{attributes}>"]
    html_parts.append("<thead><tr>")
    for column in column_names:
        html_parts.append(f"<th scope=\"col\">{escape(column)}</th>")
    html_parts.append("</tr></thead>")

    html_parts.append("<tbody>")
    for row in visible:
        html_parts.append("<tr>")
        for column in column_names:
            value = row.get(column)
            cell = "" if value is None else escape(str(value))
            html_parts.append(f"<td>{cell}</td>")
        html_parts.append("</tr>")
    if not visible:
        html_parts.append("<tr><td colspan=\"{}\">(no rows)</td></tr>".format(len(column_names)))
    html_parts.append("</tbody>")

    if truncated:
        html_parts.append("<tfoot><tr><td colspan=\"{}\">".format(len(column_names)))
        html_parts.append(
            escape(
                "Additional rows not shown; render limited to "
                f"the first {max_rows} rows."
            )
        )
        html_parts.append("</td></tr></tfoot>")

    html_parts.append("</table>")
    return "".join(html_parts)


def _format_attributes(*, table_class: str | None, table_id: str | None) -> str:
    attributes: list[str] = []
    if table_class:
        attributes.append(f' class="{escape(table_class, quote=True)}"')
    if table_id:
        attributes.append(f' id="{escape(table_id, quote=True)}"')
    return "".join(attributes)


__all__ = ["to_html"]

