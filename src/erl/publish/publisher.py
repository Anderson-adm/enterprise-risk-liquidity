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

import hashlib
import io
import json
import logging
import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
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

from erl import log
from erl.contracts import consumer_contract
from erl.errors import PublicationError
from erl.ingestion import bronze
from erl.sqltypes import Dec, Timestamp
from erl.storage import ObjectStore

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


def ensure_schema(engine: Engine) -> None:
    with _connect(engine) as conn:
        if engine.dialect.name == "mssql":
            for schema in ("rpt", "stg"):
                conn.execute(text(f"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA {schema}')"))
        elif engine.dialect.name == "postgresql":
            for schema in ("rpt", "stg"):
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        metadata.create_all(conn)
        conn.commit()


# --------------------------------------------------------------------------- package


@dataclass(frozen=True)
class Package:
    uri: str
    reference_date: date
    pipeline_run_id: str
    products: dict[str, pa.Table]
    content_sha256: str


def load_package(store: ObjectStore, manifest_key: str) -> Package:
    try:
        manifest = json.loads(store.get(manifest_key))
    except Exception as exc:  # missing or unreadable manifest = no committed package
        raise PublicationError(f"cannot read package manifest {manifest_key}: {exc}") from exc
    gate = manifest.get("quality_gate", {})
    if gate.get("status") != "PASSED":
        raise PublicationError(f"package quality gate is {gate.get('status')!r}, not PASSED")
    ref = date.fromisoformat(manifest["reference_date"])
    contract = consumer_contract()["products"]
    if set(manifest["products"]) != set(contract):
        raise PublicationError(f"package products {sorted(manifest['products'])} != {sorted(contract)}")

    tables: dict[str, pa.Table] = {}
    for name, entry in manifest["products"].items():
        data = store.get(entry["object"])
        if bronze.sha256_hex(data) != entry["sha256"]:
            raise PublicationError(f"checksum mismatch for {entry['object']}")
        table = pq.read_table(io.BytesIO(data))
        if table.num_rows != entry["rows"]:
            raise PublicationError(f"{name}: {table.num_rows} rows, manifest says {entry['rows']}")
        validate_product(name, table, ref)
        tables[name] = table
    return Package(store.uri(manifest_key), ref, manifest["pipeline_run_id"], tables, content_hash(tables))


def validate_product(name: str, table: pa.Table, ref: date) -> None:
    if not table.num_rows:
        raise PublicationError(f"{name}: empty product cannot replace the last valid publication")
    spec = consumer_contract()["products"][name]
    expected = [c["name"] for c in spec["columns"]]
    if table.column_names != expected:
        raise PublicationError(f"{name}: columns {table.column_names} != contract {expected}")
    for col in spec["columns"]:
        arrow_type = table.schema.field(col["name"]).type
        if not _type_matches(col["type"], arrow_type):
            raise PublicationError(f"{name}.{col['name']}: type {arrow_type} violates {col['type']}")
        if not col["nullable"] and table.column(col["name"]).null_count:
            raise PublicationError(f"{name}.{col['name']}: NULL in non-nullable column")
    rows = table.to_pylist()
    if any(r["reference_date"] != ref for r in rows):
        raise PublicationError(f"{name}: rows outside reference date {ref}")
    keys = [tuple(r[k] for k in spec["key"]) for r in rows]
    if len(keys) != len(set(keys)):
        raise PublicationError(f"{name}: duplicate keys {spec['key']}")


def _type_matches(logical: str, arrow_type: pa.DataType) -> bool:
    if logical == "date":
        return pa.types.is_date32(arrow_type)
    if logical == "integer":
        return pa.types.is_integer(arrow_type)
    if m := re.fullmatch(r"string\((\d+)\)", logical):
        return pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type)
    if m := re.fullmatch(r"decimal\((\d+),(\d+)\)", logical):
        return (
            pa.types.is_decimal(arrow_type)
            and arrow_type.scale == int(m.group(2))
            and arrow_type.precision <= int(m.group(1))
        )
    return False


def content_hash(products: dict[str, pa.Table]) -> str:
    """Hash of the business content, independent of file encoding and row order."""
    h = hashlib.sha256()
    for name in sorted(products):
        rows = sorted(json.dumps(r, default=_canonical, sort_keys=True) for r in products[name].to_pylist())
        h.update(name.encode())
        for r in rows:
            h.update(r.encode())
    return h.hexdigest()


def _canonical(v):
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, date):
        return v.isoformat()
    raise TypeError(type(v))


# --------------------------------------------------------------------------- publish


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
    log.event(logger, "publication_finished", run_id=package.pipeline_run_id, **result)
    return result


PUBLICATION_LOCK_TIMEOUT_S = 120
_PG_LOCK_KEY = 0x45524C5055  # arbitrary constant identifying "ERL publication"


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
            text(
                "DECLARE @rc int; EXEC @rc = sp_getapplock @Resource = 'erl_consumer_publication', "
                "@LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = :ms; SELECT @rc"
            ),
            {"ms": PUBLICATION_LOCK_TIMEOUT_S * 1000},
        ).scalar_one()
        if rc < 0:
            raise TimeoutError(f"publication lock not acquired (sp_getapplock returned {rc})")
    elif dialect == "postgresql":
        conn.execute(text(f"SET LOCAL lock_timeout = '{PUBLICATION_LOCK_TIMEOUT_S}s'"))
        conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": _PG_LOCK_KEY})


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
    log.event(
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
