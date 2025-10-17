# ``duckplus.duckcon``

The :mod:`duckplus.duckcon` module provides the :class:`DuckCon` context manager
and the accompanying :class:`ExtensionInfo` dataclass. Together they centralise
connection lifecycle management and expose extension metadata in a structured
form.

## ``ExtensionInfo``

``ExtensionInfo`` captures the state of a DuckDB extension returned by
:meth:`DuckCon.extensions`:

- ``name`` – canonical extension name as reported by DuckDB.
- ``loaded`` – ``True`` when the extension is currently loaded into the
  connection.
- ``installed`` – ``True`` when DuckDB recorded the extension as installed on the
  machine.
- ``install_path`` – filesystem path used during installation if available.
- ``description`` – human readable summary provided by DuckDB, if any.
- ``aliases`` – tuple of alternate names registered for the extension.
- ``version`` – version string when the extension reports one.
- ``install_mode`` – DuckDB installation mode (e.g. ``GIT"``, ``WEB"``).
- ``installed_from`` – installation source such as a URL or package name.

The dataclass is frozen so audit code can safely use instances as dictionary keys
or cache entries.

## ``DuckCon``

### Construction and lifecycle

``DuckCon(database=":memory:", *, extra_extensions=None, **connect_kwargs)``
wraps :func:`duckdb.connect` and enforces single-owner semantics. Use it as a
context manager to open and close the underlying
``duckdb.DuckDBPyConnection`` automatically. Passing ``extra_extensions``
installs and loads the named community extensions (``"nanodbc"`` or
``"excel"``). Additional keyword arguments are forwarded directly to
:func:`duckdb.connect`.

``__enter__`` opens the connection, installs requested extensions, and returns
the ``DuckDBPyConnection``. ``__exit__`` always closes the connection.
Re-entering an already open manager raises ``RuntimeError`` to guard against
incorrect reuse.

### Properties

- ``is_open`` – ``True`` while the context manager owns an open connection.
- ``connection`` – returns the active ``DuckDBPyConnection``. Accessing the
  property while closed raises ``RuntimeError`` with a descriptive message.

### Methods

- ``register_helper(name, helper, *, overwrite=False)`` – register a callable
  that receives the open connection as its first argument. Set ``overwrite`` to
  ``True`` to replace an existing helper; otherwise duplicates raise
  ``ValueError``.
- ``apply_helper(name, *args, **kwargs)`` – invoke a registered helper while the
  connection is open. Raises ``KeyError`` when the helper name is unknown and
  ``RuntimeError`` if the manager is closed.
- ``load_nano_odbc(*, install=True)`` – deprecated compatibility wrapper that
  forwards to the internal nano-ODBC loader. Emits ``DeprecationWarning`` advising
  callers to pass ``extra_extensions=("nanodbc",)`` when constructing ``DuckCon``.
- ``extensions()`` – return a tuple of :class:`ExtensionInfo` instances describing
  installed DuckDB extensions. Requires an open connection.
- ``table(name)`` – convenience factory that returns a :class:`duckplus.table.Table`
  bound to this manager.

DuckPlus also exposes ``load_excel`` and nano-ODBC installers internally. These
helpers remain private to keep the surface area compact.
