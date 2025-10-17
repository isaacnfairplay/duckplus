# ``duckplus.extensions``

``duckplus.extensions`` audits DuckDB extensions and highlights which relation
helpers cover bundled features.

## Constants

- ``COMMUNITY_EXTENSION_NAMES`` – frozenset of community extensions distributed via
  ``duckdb-extensions`` (e.g. ``"excel"``, ``"postgres_scanner"``). The audit skips
  these because they are not bundled with the core engine.
- ``DEFAULT_BUNDLED_HELPER_COVERAGE`` – mapping from extension name to helper names
  that already support it. Used as a baseline when computing audits.

## Dataclasses

- ``BundledExtensionAuditEntry`` – pairs an :class:`duckplus.duckcon.ExtensionInfo`
  with helper coverage. The ``has_helper`` property returns ``True`` when at least
  one helper covers the extension.

## Functions

- ``audit_bundled_extensions(infos, *, helper_coverage=None)`` – filter a sequence
  of :class:`ExtensionInfo` records, returning tuples of
  :class:`BundledExtensionAuditEntry` for bundled extensions. ``helper_coverage``
  allows callers to override or extend the default coverage mapping.
- ``collect_bundled_extension_audit(duckcon, *, helper_coverage=None)`` – convenience
  wrapper that calls :meth:`duckplus.duckcon.DuckCon.extensions` before delegating
  to :func:`audit_bundled_extensions`.

Private helpers such as ``_is_bundled_extension`` keep the surface area focused on
high-level auditing.
