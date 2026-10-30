"""Publish a validated package into the consumer database (SQL Server "controle e relatórios").

Guarantees:

* **Contract first** - package checksums, quality gate, column names/types, nullability, key
  uniqueness and reference date are verified before any write.
* **Idempotent** - the content hash of the package is compared with the ACTIVE publication for
  the same reference date; identical content is recorded as SKIPPED_IDENTICAL and nothing is
  rewritten.
* **Atomic replacement** - staging insert, in-database validation, delete of the date's rows,
  insert from staging and publication log update happen in ONE transaction. Any failure rolls
  back, so the previous valid version stays in place; the failure is then logged separately.
* **Traceable** - every attempt (ACTIVE, SUPERSEDED, SKIPPED_IDENTICAL, FAILED, REFUSED) is a
  row in ``rpt.publication_log`` and every published row carries its ``publication_id``.
"""

from __future__ import annotations

import logging
import re
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import (
    Column,
    Date,
    Engine,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    delete,
    func,
    insert,
    select,
    text,
    update,
)

from load.package import Package
from utils import structured_log
from utils.contracts import consumer_contract
from utils.errors import PublicationError
from utils.sql_files import read_sql, render_identifiers
from utils.sqltypes import Dec, Timestamp

logger = logging.getLogger(__name__)

ACTIVE, SUPERSEDED, SKIPPED, FAILED, REFUSED = (
    "ACTIVE",
    "SUPERSEDED",
    "SKIPPED_IDENTICAL",
    "FAILED",
    "REFUSED",
)

# --------------------------------------------------------------------------- schema

metadata = MetaData()


def _sql_type(logical: str):
    if logical == "date":
        return Date
    if logical == "integer":
        return Integer
    if m := re.fullmatch(r"string\((\d+)\)", logical):
        return String(int(m.group(1)))
    if m := re.fullmatch(r"decimal\((\d+),(\d+)\)", logical):
        return Dec(int(m.group(1)), int(m.group(2)))
    raise ValueError(f"unknown logical type {logical}")


def _product_tables() -> dict[str, tuple[Table, Table]]:
    tables = {}
    for name, spec in consumer_contract()["products"].items():
        cols = [
            Column(
                c["name"], _sql_type(c["type"]), nullable=c["nullable"], primary_key=c["name"] in spec["key"]
            )
            for c in spec["columns"]
        ]
        target = Table(
            name, metadata, *cols, Column("publication_id", String(80), nullable=False), schema="rpt"
        )
        staging = Table(
            f"stg_{name}",
            metadata,
            *[Column(c.name, c.type, nullable=True) for c in cols],
            Column("publication_id", String(80), nullable=False),
            schema="stg",
        )
        tables[name] = (target, staging)
    return tables


PRODUCT_TABLES = _product_tables()

publication_log = Table(
    "publication_log",
    metadata,
    Column("publication_id", String(80), primary_key=True),
    Column("reference_date", Date, nullable=False),
    Column("pipeline_run_id", String(40), nullable=False),
    Column("status", String(20), nullable=False),
    Column("content_sha256", String(64), nullable=True),
    Column("package_uri", String(400), nullable=False),
    Column("liquidity_rows", Integer, nullable=True),
    Column("exposure_rows", Integer, nullable=True),
    Column("published_at", Timestamp, nullable=False),
    Column("superseded_at", Timestamp, nullable=True),
    Column("superseded_by", String(80), nullable=True),
    Column("message", String(2000), nullable=True),
    schema="rpt",
)
# At most one ACTIVE publication per reference date, enforced by the database itself
# (filtered unique index) in addition to the publication lock taken inside the transaction.
Index(
    "ux_publication_log_active_date",
    publication_log.c.reference_date,
    unique=True,
    mssql_where=publication_log.c.status == ACTIVE,
    sqlite_where=publication_log.c.status == ACTIVE,
    postgresql_where=publication_log.c.status == ACTIVE,
)


def _connect(engine: Engine):
    conn = engine.connect()
    if engine.dialect.name == "sqlite":  # no schemas in SQLite (dev fallback only)
        conn = conn.execution_options(schema_translate_map={"rpt": None, "stg": None})
    return conn


SCHEMAS = ("rpt", "stg")


def ensure_schema(engine: Engine) -> None:
    """Create the publication schemas and tables if missing (idempotent)."""
    with _connect(engine) as conn:
        if engine.dialect.name == "mssql":
            for schema in SCHEMAS:
                conn.execute(text(read_sql("load/create_schema_sqlserver.sql")), {"schema": schema})
        elif engine.dialect.name == "postgresql":
            template = read_sql("load/create_schema_postgresql.sql")
            for schema in SCHEMAS:
                conn.execute(text(render_identifiers(template, SCHEMAS, schema=schema)))
        metadata.create_all(conn)
        conn.commit()


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)  # stored as UTC in DATETIME columns


def publish(engine: Engine, package: Package, *, _fault: Callable[[str], None] | None = None) -> dict:
    """Publish ``package``. ``_fault`` is a test hook called at named points inside the transaction."""
    ensure_schema(engine)
    ref = package.reference_date
    publication_id = f"{ref:%Y%m%d}-{package.pipeline_run_id}-{uuid.uuid4().hex}"
    rows = {name: t.to_pylist() for name, t in package.products.items()}
    base_log = {
        "publication_id": publication_id,
        "reference_date": ref,
        "pipeline_run_id": package.pipeline_run_id,
        "content_sha256": package.content_sha256,
        "package_uri": package.uri,
        "liquidity_rows": len(rows["liquidity_summary"]),
        "exposure_rows": len(rows["exposure_compliance"]),
    }
    try:
        with _connect(engine) as conn, conn.begin():
            _acquire_publication_lock(conn)
            current = (
                conn.execute(
                    select(publication_log)
                    .where(publication_log.c.reference_date == ref, publication_log.c.status == ACTIVE)
                    .with_for_update()
                )
                .mappings()
                .first()
            )
            if current is not None and current["content_sha256"] == package.content_sha256:
                conn.execute(
                    insert(publication_log).values(
                        **base_log,
                        status=SKIPPED,
                        published_at=_now(),
                        message=f"identical to active publication {current['publication_id']}",
                    )
                )
                result = {
                    "publication_status": SKIPPED,
                    "publication_id": publication_id,
                    "active_publication_id": current["publication_id"],
                }
            else:
                for name, (target, staging) in PRODUCT_TABLES.items():
                    payload = [r | {"publication_id": publication_id} for r in rows[name]]
                    if payload:
                        conn.execute(insert(staging), payload)
                    _validate_staging(conn, name, staging, publication_id, len(payload))
                    if _fault:
                        _fault(f"before_replace:{name}")
                    conn.execute(delete(target).where(target.c.reference_date == ref))
                    cols = [c.name for c in target.columns]
                    conn.execute(
                        insert(target).from_select(
                            cols,
                            select(*[staging.c[c] for c in cols]).where(
                                staging.c.publication_id == publication_id
                            ),
                        )
                    )
                    if _fault:
                        _fault(f"after_replace:{name}")
                    conn.execute(delete(staging).where(staging.c.publication_id == publication_id))
                now = _now()
                if current is not None:
                    conn.execute(
                        update(publication_log)
                        .where(publication_log.c.publication_id == current["publication_id"])
                        .values(status=SUPERSEDED, superseded_at=now, superseded_by=publication_id)
                    )
                conn.execute(insert(publication_log).values(**base_log, status=ACTIVE, published_at=now))
                result = {
                    "publication_status": ACTIVE,
                    "publication_id": publication_id,
                    "superseded": current["publication_id"] if current else None,
                }
    except PublicationError as exc:
        _record_failure(engine, base_log, REFUSED, str(exc))
        raise
    except Exception as exc:
        _record_failure(engine, base_log, FAILED, f"{type(exc).__name__}: {exc}")
        raise PublicationError(f"publication {publication_id} rolled back: {exc}") from exc

    result |= {
        "reference_date": ref.isoformat(),
        "content_sha256": package.content_sha256,
        "rows": {k: len(v) for k, v in rows.items()},
    }
    structured_log.event(logger, "publication_finished", run_id=package.pipeline_run_id, **result)
    return result


PUBLICATION_LOCK_TIMEOUT_S = 120
_PG_LOCK_KEY = 0x45524C5055  # arbitrary constant identifying "ERL publication"
_MSSQL_LOCK_RESOURCE = "erl_consumer_publication"


def _acquire_publication_lock(conn) -> None:
    """Serialise publications with a lock released at commit/rollback.

    A row lock on the ACTIVE log row is not enough: the first publication of a date has no
    row to lock, and SQL Server ignores ``SELECT ... FOR UPDATE`` (concurrent publishers
    deadlocked in the SQL Server integration test). Publications are daily and short, so one
    lock for the whole consumer database keeps staging and target writes free of deadlocks.
    SQLite (dev stand-in) already serialises writers at database level.
    """
    dialect = conn.dialect.name
    if dialect == "mssql":
        rc = conn.execute(
            text(read_sql("load/lock_publication_sqlserver.sql")),
            {"resource": _MSSQL_LOCK_RESOURCE, "timeout_ms": PUBLICATION_LOCK_TIMEOUT_S * 1000},
        ).scalar_one()
        if rc < 0:
            raise TimeoutError(f"publication lock not acquired (sp_getapplock returned {rc})")
    elif dialect == "postgresql":
        conn.execute(
            text(read_sql("load/set_lock_timeout_postgresql.sql")),
            {"timeout": f"{PUBLICATION_LOCK_TIMEOUT_S}s"},
        )
        conn.execute(text(read_sql("load/lock_publication_postgresql.sql")), {"lock_key": _PG_LOCK_KEY})


def _validate_staging(conn, name: str, staging: Table, publication_id: str, expected: int) -> None:
    spec = consumer_contract()["products"][name]
    mine = staging.c.publication_id == publication_id
    count = conn.execute(select(func.count()).select_from(staging).where(mine)).scalar_one()
    if count != expected:
        raise PublicationError(f"{name}: staging holds {count} rows, expected {expected}")
    key_cols = [staging.c[k] for k in spec["key"]]
    dupes = conn.execute(
        select(*key_cols).where(mine).group_by(*key_cols).having(func.count() > 1).limit(1)
    ).first()
    if dupes is not None:
        raise PublicationError(f"{name}: duplicate key in staging {tuple(dupes)}")


def _record_failure(engine: Engine, base_log: dict, status: str, message: str) -> None:
    try:
        with _connect(engine) as conn, conn.begin():
            conn.execute(
                insert(publication_log).values(
                    **base_log, status=status, published_at=_now(), message=message[:2000]
                )
            )
    except Exception:  # never mask the original error
        logger.exception("could not record publication failure")
    structured_log.event(
        logger,
        "publication_failed",
        logging.ERROR,
        run_id=base_log["pipeline_run_id"],
        publication_id=base_log["publication_id"],
        status=status,
        error=message[:500],
    )


def active_publications(engine: Engine) -> list[dict]:
    ensure_schema(engine)
    with _connect(engine) as conn:
        return [
            dict(r)
            for r in conn.execute(
                select(publication_log).order_by(
                    publication_log.c.reference_date, publication_log.c.published_at
                )
            ).mappings()
        ]
