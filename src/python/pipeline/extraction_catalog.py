"""Which SQL file extracts each Bronze database entity, and how its result is typed.

Changing a source view or filter means editing ``src/sql/extract/*.sql`` only. Result columns
are typed from the source table definitions so every engine yields the same canonical text
(SQLite would otherwise return its stored text for timestamps); views are left untyped, as
their columns are already returned identically by all engines.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import Engine
from sqlalchemy.exc import DBAPIError, OperationalError

from extract.landing_snapshot import extract_to_landing
from extract.sql_query import fetch_scalar
from ingestion.layout import landing_db_prefix
from sources.schemas import SOURCE_METADATA
from storage.objects import ObjectStore
from utils.contracts import bronze_entity
from utils.errors import SourceUnavailableError
from utils.sql_files import read_sql


@dataclass(frozen=True)
class ExtractQuery:
    sql_file: str  # relative to src/sql
    typed_by_table: str | None  # source table whose column types type the result


EXTRACT_QUERIES: dict[tuple[str, str], ExtractQuery] = {
    ("fund_ops", "fund"): ExtractQuery("extract/extract_query_table_fund.sql", "fund"),
    ("fund_ops", "portfolio"): ExtractQuery("extract/extract_query_table_portfolio.sql", "portfolio"),
    ("fund_ops", "source_identity_map"): ExtractQuery(
        "extract/extract_query_table_source_identity_map.sql", "source_identity_map"
    ),
    ("portfolio_risk", "processing_batch"): ExtractQuery(
        "extract/extract_query_table_processing_batch.sql", "processing_batch"
    ),
    ("portfolio_risk", "processed_position"): ExtractQuery(
        "extract/extract_query_table_processed_position.sql", "processed_position"
    ),
    ("legacy_finance", "cash_balance"): ExtractQuery("extract/extract_query_view_saldo_caixa.sql", None),
    ("legacy_finance", "pending_redemption"): ExtractQuery(
        "extract/extract_query_view_resgate_pendente.sql", None
    ),
    ("exposure_compliance", "instrument_registry"): ExtractQuery(
        "extract/extract_query_table_instrument_registry.sql", "instrument_registry"
    ),
    ("exposure_compliance", "stock_position"): ExtractQuery(
        "extract/extract_query_table_stock_position.sql", "stock_position"
    ),
    ("exposure_compliance", "concentration_limit"): ExtractQuery(
        "extract/extract_query_table_concentration_limit.sql", "concentration_limit"
    ),
}

# The risk system publishes a processing batch per date; extracting before it completes would
# read a partial portfolio. The other sources expose no readiness signal.
READINESS_QUERIES = {"portfolio_risk": "extract/readiness_query_table_processing_batch.sql"}


def check_readiness(engine: Engine, source: str, ref: date) -> None:
    if source not in READINESS_QUERIES:
        return
    status = fetch_scalar(engine, read_sql(READINESS_QUERIES[source]), {"reference_date": ref})
    if status != "COMPLETED":
        raise SourceUnavailableError(f"{source} batch for {ref} is {status or 'missing'}, not COMPLETED")


def extract_entity(
    engine: Engine,
    store: ObjectStore,
    source: str,
    entity: str,
    ref: date,
    run_id: str,
    *,
    compression: str,
    batch_rows: int,
) -> dict:
    query = EXTRACT_QUERIES[(source, entity)]
    columns = bronze_entity(source, entity).columns
    types = None
    if query.typed_by_table:
        table = SOURCE_METADATA[source][0].tables[query.typed_by_table]
        types = {c: table.c[c].type for c in columns}
    return extract_to_landing(
        engine,
        store,
        prefix=landing_db_prefix(source, entity, ref),
        source=source,
        entity=entity,
        reference_date=ref,
        run_id=run_id,
        sql=read_sql(query.sql_file),
        params={"reference_date": ref},
        columns=columns,
        compression=compression,
        batch_size=batch_rows,
        result_types=types,
    )


def extract_source(
    engine: Engine,
    store: ObjectStore,
    source: str,
    ref: date,
    run_id: str,
    *,
    compression: str,
    batch_rows: int,
) -> list[dict]:
    """Readiness check + every entity of ``source``; a connection failure means the source is
    unavailable (no partial run continues)."""
    try:
        check_readiness(engine, source, ref)
        return [
            extract_entity(
                engine, store, src, entity, ref, run_id, compression=compression, batch_rows=batch_rows
            )
            for (src, entity) in EXTRACT_QUERIES
            if src == source
        ]
    except (OperationalError, DBAPIError) as exc:
        raise SourceUnavailableError(f"{source} unreachable: {type(exc.orig).__name__}") from exc
