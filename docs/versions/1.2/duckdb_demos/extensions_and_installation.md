# Installing extensions the DuckPlus way

Several DuckDB demos showcase optional extensions such as nano-ODBC, Excel, and
HTTPFS. DuckPlus layers ergonomic helpers on top of those workflows so
production code can keep dependencies explicit.

```python
from duckplus import DuckCon, Relation

manager = DuckCon(extra_extensions=("nanodbc", "excel"))
with manager as connection:
    # Extensions requested at construction are installed and loaded automatically.
    extension_rows = manager.extensions()
    for info in extension_rows:
        print(info.name, info.loaded)

    workbook = Relation.from_excel(
        manager,
        "reports/Q1.xlsx",
        sheet="Summary",
        header=True,
    )
    print(workbook.relation.limit(5).fetchall())
```

Use :meth:`DuckCon.extensions` to audit installed modules the same way the
DuckDB demo inspects ``duckdb_extensions()``. For one-off installations—such as
``httpfs`` when previewing the remote dataset demo—call ``connection.execute``
with ``INSTALL``/``LOAD`` commands before invoking the DuckPlus relation or IO
helpers.
