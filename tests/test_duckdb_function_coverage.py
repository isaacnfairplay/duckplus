"""Completeness checks for DuckDB function coverage within DuckPlus."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from duckplus import DuckCon
from duckplus.typed.functions import (
    AGGREGATE_FUNCTIONS,
    SCALAR_FUNCTIONS,
    WINDOW_FUNCTIONS,
    DuckDBFunctionSignature,
    _StaticFunctionNamespace,
)
from duckplus.typed.types import parse_type

from function_catalog_expectations import EXPECTED_DOCSTRING_GAPS

CATALOG_DOC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "versions"
    / "1.1"
    / "api"
    / "typed"
    / "function_catalog.md"
)


def _fetch_duckdb_overloads(function_type: str) -> set[tuple]:
    manager = DuckCon()
    with manager as connection:
        rows = connection.execute(
            """
            SELECT schema_name,
                   function_name,
                   function_type,
                   return_type,
                   parameters,
                   parameter_types,
                   varargs
              FROM duckdb_functions()
             WHERE function_type = ?
            """,
            [function_type],
        ).fetchall()
    overloads: set[tuple] = set()
    for (
        schema_name,
        function_name,
        duck_type,
        return_type,
        parameters,
        parameter_types,
        varargs,
    ) in rows:
        parameter_types_tuple = tuple(
            parse_type(parameter).render() if parameter is not None else "UNKNOWN"
            for parameter in (parameter_types or ())
        )
        parameter_names_tuple = tuple(parameters or ())
        varargs_rendered = parse_type(varargs).render() if varargs is not None else None
        return_rendered = parse_type(return_type).render() if return_type is not None else "UNKNOWN"
        overloads.add(
            (
                schema_name,
                function_name,
                duck_type,
                parameter_types_tuple,
                parameter_names_tuple,
                varargs_rendered,
                return_rendered,
            )
        )
    return overloads


def _collect_namespace_overloads(namespace: object) -> set[tuple]:
    overloads: set[tuple] = set()
    seen: set[str] = set()
    for cls in namespace.__class__.__mro__:
        for name, value in cls.__dict__.items():
            if name.startswith("_") or name in seen:
                continue
            category = getattr(namespace, name, None)
            if isinstance(category, _StaticFunctionNamespace):
                seen.add(name)
                overloads.update(_collect_call_overloads(category._IDENTIFIER_FUNCTIONS.values()))
                overloads.update(_collect_call_overloads(category._SYMBOLIC_FUNCTIONS.values()))
    return overloads


def _collect_call_overloads(calls: Iterable[object]) -> set[tuple]:
    overloads: set[tuple] = set()
    for call in calls:
        if getattr(call, "_is_filter_variant", False):
            continue
        signatures: Sequence[DuckDBFunctionSignature] = call.signatures
        for signature in signatures:
            parameter_types = tuple(param.render() for param in signature.parameter_types)
            parameter_names = tuple(signature.parameters)
            varargs = signature.varargs.render() if signature.varargs else None
            overloads.add(
                (
                    signature.schema_name,
                    signature.function_name,
                    signature.function_type,
                    parameter_types,
                    parameter_names,
                    varargs,
                    signature.return_annotation(),
                )
            )
    return overloads


def _collect_namespace_calls(namespace: object) -> list[object]:
    calls: list[object] = []
    seen: set[str] = set()
    for cls in namespace.__class__.__mro__:
        for name in cls.__dict__:
            if name.startswith("_") or name in seen:
                continue
            category = getattr(namespace, name, None)
            if isinstance(category, _StaticFunctionNamespace):
                seen.add(name)
                calls.extend(category._IDENTIFIER_FUNCTIONS.values())
                calls.extend(category._SYMBOLIC_FUNCTIONS.values())
    return calls


def _collect_namespace_names(namespace: object) -> set[str]:
    names: set[str] = set()
    seen: set[str] = set()
    for cls in namespace.__class__.__mro__:
        for name in cls.__dict__:
            if name.startswith("_") or name in seen:
                continue
            category = getattr(namespace, name, None)
            if isinstance(category, _StaticFunctionNamespace):
                seen.add(name)
                names.update(category._IDENTIFIER_FUNCTIONS.keys())
                names.update(category._SYMBOLIC_FUNCTIONS.keys())
    return names


def _load_documented_functions() -> dict[str, set[str]]:
    documented: dict[str, set[str]] = {"scalar": set(), "aggregate": set(), "window": set()}
    current_type: str | None = None
    text = CATALOG_DOC_PATH.read_text(encoding="utf-8")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.startswith("## "):
            heading = line[3:].strip().lower()
            if heading.startswith("scalar"):
                current_type = "scalar"
            elif heading.startswith("aggregate"):
                current_type = "aggregate"
            elif heading.startswith("window"):
                current_type = "window"
            else:
                current_type = None
            continue
        if not current_type or line.startswith("###") or not line:
            continue
        if line.startswith("Symbolic operators"):
            continue
        match = re.match(r"- ``(?P<name>.+?)``", line)
        if match:
            documented[current_type].add(match.group("name"))
    return documented


def test_typed_function_metadata_matches_duckdb_catalog() -> None:
    duckdb_overloads = {
        function_type: _fetch_duckdb_overloads(function_type)
        for function_type in ("scalar", "aggregate", "window")
    }

    typed_namespaces: Mapping[str, object] = {
        "scalar": SCALAR_FUNCTIONS,
        "aggregate": AGGREGATE_FUNCTIONS,
        "window": WINDOW_FUNCTIONS,
    }
    typed_overloads = {
        function_type: _collect_namespace_overloads(namespace)
        for function_type, namespace in typed_namespaces.items()
    }

    for function_type in ("scalar", "aggregate", "window"):
        missing = duckdb_overloads[function_type] - typed_overloads[function_type]
        extra = typed_overloads[function_type] - duckdb_overloads[function_type]
        assert not missing, (
            f"Missing typed wrappers for {function_type} overloads: {sorted(missing)}"
        )
        assert not extra, (
            f"Unexpected extra {function_type} overloads defined in typed namespace: {sorted(extra)}"
        )


def test_typed_function_docstrings_cover_catalog_metadata() -> None:
    typed_namespaces = (
        SCALAR_FUNCTIONS,
        AGGREGATE_FUNCTIONS,
        WINDOW_FUNCTIONS,
    )

    missing: set[tuple[str, str, str]] = set()
    for namespace in typed_namespaces:
        for call in _collect_namespace_calls(namespace):
            signatures: Sequence[DuckDBFunctionSignature] = call.signatures
            if not signatures:
                continue
            primary = signatures[0]
            doc = call.__doc__ or ""
            description = (primary.description or "").strip()
            comment = (primary.comment or "").strip()
            macro = (primary.macro_definition or "").strip()

            if description and description not in doc:
                missing.add((primary.function_type, primary.function_name, "description"))
            if comment and comment not in doc:
                missing.add((primary.function_type, primary.function_name, "comment"))
            if macro and macro not in doc:
                missing.add((primary.function_type, primary.function_name, "macro_definition"))

    assert missing == EXPECTED_DOCSTRING_GAPS, (
        "Docstring coverage mismatches DuckDB catalog metadata. Update the typed "
        "function generator or the expectations fixture to resolve: "
        f"{sorted(missing ^ EXPECTED_DOCSTRING_GAPS)}"
    )


def test_typed_function_documentation_lists_catalog() -> None:
    doc_functions = _load_documented_functions()
    doc_text = CATALOG_DOC_PATH.read_text(encoding="utf-8")
    assert "FILTER" in doc_text and "(WHERE" in doc_text

    typed_namespaces: Mapping[str, object] = {
        "scalar": SCALAR_FUNCTIONS,
        "aggregate": AGGREGATE_FUNCTIONS,
        "window": WINDOW_FUNCTIONS,
    }

    for function_type, namespace in typed_namespaces.items():
        names = _collect_namespace_names(namespace)
        if function_type == "aggregate":
            names = {name for name in names if not name.endswith("_filter")}
        assert names == doc_functions[function_type], (
            "Documentation must list all typed functions. Run the namespace "
            "generator script to refresh the catalog. "
            f"Missing: {sorted(names - doc_functions[function_type])}; "
            f"Extra: {sorted(doc_functions[function_type] - names)}"
        )
