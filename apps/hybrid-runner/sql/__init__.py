"""Versioned SQL executed by the pipeline (resource package, no code).

* ``extract/`` - queries against the sources and the local warehouse; values are bind
  parameters (``:name`` for SQLAlchemy, ``$name`` for DuckDB).
* ``load/`` - statements of the consumer publication (locks, schemas, Gold export).

Identifiers that SQL cannot bind are written as ``{name}`` and rendered only after validation
against an allowlist (``utils.sql_files.render_identifiers``). dbt models live in ``dbt/``.
"""
