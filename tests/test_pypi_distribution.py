"""Integration checks for the published duckplus==1.4.3 wheel via subprocess probes."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap

import pytest


def _run_probe(script: str) -> dict[str, object]:
    """Execute ``script`` in a subprocess and return the parsed JSON payload."""

    command = [sys.executable, "-c", textwrap.dedent(script)]
    try:
        result = subprocess.run(
            command, check=True, capture_output=True, text=True
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - diagnostic path
        stderr = exc.stderr or ""
        if "site-packages/duckplus/__init__.py" in stderr and "No such file" in stderr:
            pytest.skip("duckplus==1.4.3 is not available in the probe environment")
        raise
    return json.loads(result.stdout)


def test_pypi_distribution_report(tmp_path):
    """Collect behavioural signals from the pip-distributed package."""

    script = """
    import json
    import importlib
    import importlib.util
    import sys
    import sysconfig
    import tempfile
    import warnings
    from pathlib import Path

    def load_duckplus():
        purelib = Path(sysconfig.get_paths()["purelib"])
        package_dir = purelib / "duckplus"
        spec = importlib.util.spec_from_file_location(
            "duckplus_pypi",
            package_dir / "__init__.py",
            submodule_search_locations=[str(package_dir)],
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        loader = spec.loader
        assert loader is not None
        loader.exec_module(module)
        return module

    module = load_duckplus()

    has_coalesce = hasattr(module.ducktype.Boolean("label"), "coalesce")

    expected_duckcon_helpers = [
        "read_csv",
        "read_parquet",
        "read_json",
        "read_excel",
        "read_odbc_query",
        "read_odbc_table",
    ]
    duckcon_helper_presence = {
        name: hasattr(module.DuckCon, name) for name in expected_duckcon_helpers
    }
    duckcon_helper_callables = {
        name: callable(getattr(module.DuckCon, name, None))
        for name in expected_duckcon_helpers
    }

    scalar_macro_expectations = {
        "scalar": [
            "split_part",
            "array_to_string",
            "array_to_string_comma_default",
            "array_append",
            "array_intersect",
            "array_pop_back",
            "array_pop_front",
            "array_prepend",
            "array_push_back",
            "array_push_front",
            "array_reverse",
            "current_catalog",
            "current_database",
            "current_query",
            "current_role",
            "current_schema",
            "current_schemas",
            "current_user",
            "session_user",
            "pg_get_constraintdef",
            "pg_get_viewdef",
            "pg_size_pretty",
            "pg_typeof",
            "has_any_column_privilege",
            "has_column_privilege",
            "has_database_privilege",
            "has_foreign_data_wrapper_privilege",
            "has_function_privilege",
            "has_language_privilege",
            "has_schema_privilege",
            "has_sequence_privilege",
            "has_server_privilege",
            "has_table_privilege",
            "has_tablespace_privilege",
            "pg_collation_is_visible",
            "pg_conversion_is_visible",
            "pg_function_is_visible",
            "pg_has_role",
            "pg_opclass_is_visible",
            "pg_operator_is_visible",
            "pg_opfamily_is_visible",
            "pg_table_is_visible",
            "pg_ts_config_is_visible",
            "pg_ts_dict_is_visible",
            "pg_ts_parser_is_visible",
            "pg_ts_template_is_visible",
            "pg_type_is_visible",
        ]
    }
    scalar_macro_callables = {
        name: callable(getattr(module.functions.scalar, name, None))
        for name in scalar_macro_expectations["scalar"]
        if hasattr(module.functions.scalar, name)
    }
    scalar_macro_missing = [
        name
        for name in scalar_macro_expectations["scalar"]
        if name not in scalar_macro_callables
    ]
    scalar_macro_noncallable = [
        name for name, is_callable in scalar_macro_callables.items() if not is_callable
    ]

    typed_macro_expectations = {
        "Varchar": [
            "split_part",
            "array_to_string",
            "array_to_string_comma_default",
            "current_catalog",
            "current_database",
            "current_query",
            "current_role",
            "current_schema",
            "current_schemas",
            "current_user",
            "session_user",
            "pg_get_constraintdef",
            "pg_get_viewdef",
            "pg_size_pretty",
            "pg_typeof",
        ],
        "Generic": [
            "array_append",
            "array_intersect",
            "array_pop_back",
            "array_pop_front",
            "array_prepend",
            "array_push_back",
            "array_push_front",
            "array_reverse",
        ],
        "Boolean": [
            "has_any_column_privilege",
            "has_column_privilege",
            "has_database_privilege",
            "has_foreign_data_wrapper_privilege",
            "has_function_privilege",
            "has_language_privilege",
            "has_schema_privilege",
            "has_sequence_privilege",
            "has_server_privilege",
            "has_table_privilege",
            "has_tablespace_privilege",
            "pg_collation_is_visible",
            "pg_conversion_is_visible",
            "pg_function_is_visible",
            "pg_has_role",
            "pg_opclass_is_visible",
            "pg_operator_is_visible",
            "pg_opfamily_is_visible",
            "pg_table_is_visible",
            "pg_ts_config_is_visible",
            "pg_ts_dict_is_visible",
            "pg_ts_parser_is_visible",
            "pg_ts_template_is_visible",
            "pg_type_is_visible",
        ],
    }
    scalar_namespaces = module.static_typed.SCALAR_FUNCTIONS
    typed_macro_missing: dict[str, list[str]] = {}
    for namespace_name, names in typed_macro_expectations.items():
        namespace = getattr(scalar_namespaces, namespace_name)
        missing = [name for name in names if not hasattr(namespace, name)]
        if missing:
            typed_macro_missing[namespace_name] = missing

    workdir = Path(tempfile.mkdtemp())
    csv_path = workdir / "example.csv"
    csv_path.write_text("value,label\\n1,true\\n2,false\\n3,\\n", encoding="utf-8")

    duckcon = module.DuckCon()
    aggregate_total = None
    with duckcon:
        relation = duckcon.read_csv(csv_path, header=True)
        label_expr = module.ducktype.Boolean("label")
        if has_coalesce:
            filled_label = label_expr.coalesce(False)
        else:
            filled_label = (
                module.ducktype.Boolean.case()
                .when(label_expr.is_null(), False)
                .else_(label_expr)
                .end()
            )

        enriched = relation.add(
            (module.ducktype.Numeric("value") * 2).alias("double_value"),
            filled_label=filled_label,
        )

        aggregate = (
            enriched.aggregate()
            .agg(
                module.ducktype.Numeric.Aggregate.sum("double_value"),
                alias="total_double",
            )
            .all()
        )
        row = aggregate.relation.fetchone()
        if row is not None:
            aggregate_total = row[0]

    approx_namespace = module.ducktype.Numeric.Aggregate
    approx_available = hasattr(approx_namespace, "approx_quantile")
    approx_result = None
    approx_error = None
    approx_signature_error = None
    with module.DuckCon() as connection:
        base = connection.sql("SELECT * FROM (VALUES (1), (2), (3), (4)) AS t(value)")
        relation = module.Relation.from_relation(duckcon=connection, relation=base)
        if approx_available:
            try:
                expr = approx_namespace.approx_quantile("value", 0.5).alias("median")
                approx_relation = relation.aggregate().agg(expr).all()
                approx_row = approx_relation.relation.fetchone()
                if approx_row is not None:
                    approx_result = approx_row[0]
            except Exception as exc:  # pragma: no cover - diagnostic capture
                approx_error = repr(exc)
        else:
            try:
                module.functions.approx_quantile_numeric("value", 0.5)
            except Exception as exc:  # pragma: no cover - expected failure path
                approx_signature_error = repr(exc)

    typed_warning_emitted = False
    typed_alias_matches = False
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        typed_module = importlib.import_module("duckplus_pypi.typed")
        typed_warning_emitted = any(
            item.category is DeprecationWarning for item in caught
        )
        typed_alias_matches = typed_module.Numeric is module.static_typed.Numeric

    schema_diff = None
    with module.DuckCon() as connection:
        baseline = module.Relation.from_relation(
            connection,
            connection.sql("SELECT 1::INTEGER AS id, 'label'::VARCHAR AS payload"),
        )
        candidate = module.Relation.from_relation(
            connection,
            connection.sql("SELECT 1::INTEGER AS id, 99::INTEGER AS payload"),
        )
        diff = module.schema.diff_relations(
            baseline,
            candidate,
            baseline_label="baseline",
            candidate_label="candidate",
            warn=False,
        )
        if diff.type_drift:
            first = diff.type_drift[0]
            schema_diff = {
                "column": first.column,
                "expected_type": first.expected_type,
                "observed_type": first.observed_type,
            }
        else:
            schema_diff = {}

    payload = {
        "boolean_has_coalesce": has_coalesce,
        "aggregate_total": aggregate_total,
        "duckcon_closed_after": not duckcon.is_open,
        "approx_namespace_available": approx_available,
        "approx_result": approx_result,
        "approx_error": approx_error,
        "approx_signature_error": approx_signature_error,
        "typed_warning_emitted": typed_warning_emitted,
        "typed_alias_matches": typed_alias_matches,
        "schema_diff": schema_diff,
        "duckcon_helper_presence": duckcon_helper_presence,
        "duckcon_helper_callables": duckcon_helper_callables,
        "scalar_macro_missing": scalar_macro_missing,
        "scalar_macro_noncallable": scalar_macro_noncallable,
        "typed_macro_missing": typed_macro_missing,
    }
    print(json.dumps(payload))
    """

    result = _run_probe(script)

    assert result["aggregate_total"] == 12
    assert result["duckcon_closed_after"]
    assert not result["boolean_has_coalesce"]
    assert not result["approx_namespace_available"]
    assert result["approx_result"] is None
    assert result["approx_error"] is None
    assert result["approx_signature_error"] is not None
    assert result["typed_warning_emitted"]
    assert not result["typed_alias_matches"]
    assert result["schema_diff"] == {
        "column": "payload",
        "expected_type": "VARCHAR",
        "observed_type": "INTEGER",
    }
    assert all(result["duckcon_helper_presence"].values())
    assert all(result["duckcon_helper_callables"].values())
    assert result["scalar_macro_missing"] == []
    assert result["scalar_macro_noncallable"] == []
    assert result["typed_macro_missing"] == {}
