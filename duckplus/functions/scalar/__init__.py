"""Scalar DuckDB function helpers organised by domain."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

_SIDE_EFFECT_MODULES: tuple[str, ...] = (
    "duckplus.functions.scalar.string",
    "duckplus.functions.scalar.list",
    "duckplus.functions.scalar.system",
    "duckplus.functions.scalar.postgres_privilege",
    "duckplus.functions.scalar.postgres_visibility",
)

# Import modules with registration side effects explicitly so tests can assert
# the dependency surface while keeping the helpers introspectable.
string_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[0])
list_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[1])
system_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[2])
postgres_privilege_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[3])
postgres_visibility_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[4])

from .string import (  # noqa: E402  # Imported after side-effect module load.
    array_to_string,
    array_to_string_comma_default,
    split_part,
)
from .list import (  # noqa: E402  # Imported after side-effect module load.
    array_append,
    array_intersect,
    array_pop_back,
    array_pop_front,
    array_prepend,
    array_push_back,
    array_push_front,
    array_reverse,
)
from .system import (  # noqa: E402  # Imported after side-effect module load.
    current_catalog,
    current_database,
    current_query,
    current_role,
    current_schema,
    current_schemas,
    current_user,
    session_user,
    pg_get_constraintdef,
    pg_get_viewdef,
    pg_size_pretty,
    pg_typeof,
)
from .postgres_privilege import (  # noqa: E402  # Imported after side-effect load.
    has_any_column_privilege,
    has_column_privilege,
    has_database_privilege,
    has_foreign_data_wrapper_privilege,
    has_function_privilege,
    has_language_privilege,
    has_schema_privilege,
    has_sequence_privilege,
    has_server_privilege,
    has_table_privilege,
    has_tablespace_privilege,
)
from .postgres_visibility import (  # noqa: E402  # Imported after side-effect load.
    pg_collation_is_visible,
    pg_conversion_is_visible,
    pg_function_is_visible,
    pg_has_role,
    pg_opclass_is_visible,
    pg_operator_is_visible,
    pg_opfamily_is_visible,
    pg_table_is_visible,
    pg_ts_config_is_visible,
    pg_ts_dict_is_visible,
    pg_ts_parser_is_visible,
    pg_ts_template_is_visible,
    pg_type_is_visible,
)

# Re-export the modules for callers that prefer attribute access through the
# scalar package.
string = string_module
list = list_module  # pylint: disable=redefined-builtin
system = system_module
postgres_privilege = postgres_privilege_module
postgres_visibility = postgres_visibility_module

SIDE_EFFECT_MODULES: tuple[str, ...] = _SIDE_EFFECT_MODULES

__all__ = [
    "string_module",
    "list_module",
    "system_module",
    "postgres_privilege_module",
    "postgres_visibility_module",
    "string",
    "list",
    "system",
    "postgres_privilege",
    "postgres_visibility",
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
    "SIDE_EFFECT_MODULES",
]
