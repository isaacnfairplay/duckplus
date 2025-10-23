# Typed expressions and Protocols

```{tip}
Looking for a catalogue of helpers? See :doc:`typed_reference` for a quick
reference that lists every Protocol and factory.
```

DuckPlus 2.0 exposes typed expression classes whose methods are injected by a
metaclass. Static analysers cannot evaluate that metaclass, so the package ships
inline ``typing.Protocol`` definitions describing the full method surface. Code
should cast values to those Protocols to receive completions and type checking.

```python
from duckplus.typed import (
    as_boolean_proto,
    as_string_proto,
    as_temporal_proto,
    predicate,
    timestamp,
    varchar,
)
from duckplus.typed.protocols import (
    BooleanExprProto,
    StringExprProto,
    TemporalExprProto,
)

col: StringExprProto = as_string_proto(varchar("name"))
flag: BooleanExprProto = as_boolean_proto(predicate("active"))
created: TemporalExprProto = as_temporal_proto(timestamp("created_at"))
print(col.strip().split_part(" ", 1).length())
print(flag.and_(col.like("Jo%")))
print(created.date_trunc("day").strftime("%Y-%m-%d"))
```

At runtime the ``ExprMeta`` metaclass installs methods listed in
``spec/string_spec.py``, ``spec/numeric_spec.py``, ``spec/boolean_spec.py``, and
``spec/temporal_spec.py``. The SPEC modules are simple Python dicts so imports
remain fast and static analysers can compare the runtime behaviour against the
Protocol definitions.

Why no ``.pyi`` files? Pyright and Astral ty already understand Protocols without
extra stubs, and PEP 561 typing markers let them read the inline annotations
immediately.
