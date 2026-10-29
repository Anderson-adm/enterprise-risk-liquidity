"""SQL extraction from the four database sources into landing snapshots.

Each (source, entity) is read with one parameterised SELECT. Identifiers in the SQL come
only from the static ``EXTRACTS`` table and the Bronze contract (never from user input);
the reference date is always a bound parameter.

Output per entity (landing zone, consumed by the Bronze ingestion — local Python or the
Glue ``db_ingest`` job):

    landing/db/<source>/<entity>/reference_date=<D>/<run_id>.parquet   business columns as text
    landing/db/<source>/<entity>/reference_date=<D>/<run_id>.json      extraction manifest

Rows are streamed in chunks (``fetchmany``) and written as Parquet row groups so memory stays
bounded for large legacy extractions.
"""

from __future__ import annotations

import hashlib
import json
import logging
import tempfile
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import Date, Engine, bindparam, text
from sqlalchemy.exc import DBAPIError, OperationalError

from erl import log
from erl.contracts import bronze_entity
from erl.errors import SourceUnavailableError
from erl.ingestion import bronze
from erl.storage import ObjectStore

logger = logging.getLogger(__name__)

CHUNK_ROWS = 5_000


@dataclass(frozen=True)
class ExtractSpec:
    from_object: str
    where: str | None  # uses :d (reference date)
    order_by: str


EXTRACTS: dict[tuple[str, str], ExtractSpec] = {
    ("fund_ops", "fund"): ExtractSpec("fund", None, "fund_id"),
    ("fund_ops", "portfolio"): ExtractSpec("portfolio", None, "portfolio_id"),
    ("fund_ops", "source_identity_map"): ExtractSpec(
        "source_identity_map", None, "source_system, external_identifier"
    ),
    ("portfolio_risk", "processing_batch"): ExtractSpec(
        "processing_batch", "reference_date = :d", "batch_id"
    ),
    ("portfolio_risk", "processed_position"): ExtractSpec(
        "processed_position", "reference_date = :d", "position_id"
    ),
    ("legacy_finance", "cash_balance"): ExtractSpec("vw_saldo_caixa", "dt_ref = :d", "cod_cart"),
    # obligations open on D: requested up to D and settling after D
    ("legacy_finance", "pending_redemption"): ExtractSpec(
        "vw_resgate_pendente", "dt_pedido <= :d AND dt_cotizacao > :d", "id_resgate"
    ),
    ("exposure_compliance", "instrument_registry"): ExtractSpec(
        "instrument_registry", None, "instrument_code"
    ),
    ("exposure_compliance", "stock_position"): ExtractSpec(
        "stock_position", "position_date = :d", "stock_id"
    ),
    ("exposure_compliance", "concentration_limit"): ExtractSpec("concentration_limit", None, "rule_id"),
}


def landing_prefix(source: str, entity: str, ref: date) -> str:
    return f"landing/db/{source}/{entity}/reference_date={ref.isoformat()}/"


def check_readiness(engine: Engine, source: str, ref: date) -> None:
    """The risk system publishes a processing batch per date; extracting before it completes
    would read a partial portfolio. Other sources have no readiness signal."""
    if source != "portfolio_risk":
        return
    with engine.connect() as conn:
        status = conn.execute(
            text(
                "SELECT status FROM processing_batch WHERE reference_date = :d ORDER BY batch_id DESC"
            ).bindparams(bindparam("d", type_=Date)),
            {"d": ref},
        ).scalar()
    if status != "COMPLETED":
        raise SourceUnavailableError(
            f"portfolio_risk batch for {ref} is {status or 'missing'}, not COMPLETED"
        )


def extract_entity(
    engine: Engine, store: ObjectStore, source: str, entity: str, ref: date, run_id: str
) -> dict:
    spec = EXTRACTS[(source, entity)]
    contract = bronze_entity(source, entity)
    cols = ", ".join(contract.columns)
    sql = f"SELECT {cols} FROM {spec.from_object}"
    if spec.where:
        sql += f" WHERE {spec.where}"
    sql += f" ORDER BY {spec.order_by}"

    schema = pa.schema(
        [pa.field(c, pa.string()) for c in contract.columns]
        + [pa.field("meta_row_number", pa.int64()), pa.field("meta_row_hash", pa.string())]
    )
    content = hashlib.sha256()
    rows_total = 0
    started = time.monotonic()
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "extract.parquet"
        with pq.ParquetWriter(path, schema, compression="zstd") as writer, engine.connect() as conn:
            stmt = text(sql).bindparams(bindparam("d", type_=Date)) if ":d" in sql else text(sql)
            result = conn.execution_options(stream_results=True).execute(
                stmt, {"d": ref} if ":d" in sql else {}
            )
            while chunk := result.fetchmany(CHUNK_ROWS):
                values = [tuple(bronze.stringify(v) for v in row) for row in chunk]
                hashes = [bronze.row_hash(v) for v in values]
                for h in hashes:
                    content.update(h.encode())
                data = {c: [v[i] for v in values] for i, c in enumerate(contract.columns)}
                data["meta_row_number"] = list(range(rows_total + 1, rows_total + len(values) + 1))
                data["meta_row_hash"] = hashes
                writer.write_table(pa.Table.from_pydict(data, schema=schema))
                rows_total += len(values)
        if rows_total == 0:
            pq.write_table(schema.empty_table(), path)
        payload = path.read_bytes()

    prefix = landing_prefix(source, entity, ref)
    data_key = store.put(prefix + f"{run_id}.parquet", payload)
    manifest = {
        "run_id": run_id,
        "source_system": source,
        "entity": entity,
        "reference_date": ref.isoformat(),
        "rows": rows_total,
        "object": prefix + f"{run_id}.parquet",
        "object_sha256": bronze.sha256_hex(payload),
        "content_sha256": content.hexdigest(),  # order-sensitive hash of row hashes
        "query": sql,
        "extracted_at": bronze.utc_now_iso(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    store.put(prefix + f"{run_id}.json", json.dumps(manifest, indent=2).encode())
    log.event(
        logger,
        "entity_extracted",
        run_id=run_id,
        source_system=source,
        entity=entity,
        reference_date=ref.isoformat(),
        records_read=rows_total,
        landing_object=data_key,
        elapsed_time=manifest["elapsed_seconds"],
    )
    return manifest


def extract_source(engine: Engine, store: ObjectStore, source: str, ref: date, run_id: str) -> list[dict]:
    try:
        check_readiness(engine, source, ref)
        return [
            extract_entity(engine, store, source, entity, ref, run_id)
            for (src, entity) in EXTRACTS
            if src == source
        ]
    except (OperationalError, DBAPIError) as exc:
        raise SourceUnavailableError(f"{source} unreachable: {type(exc.orig).__name__}") from exc
