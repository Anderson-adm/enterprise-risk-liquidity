"""Bronze ingestion of database extraction snapshots (landing -> Bronze).

Local counterpart of the Glue job ``glue/jobs/db_ingest.py``; both follow the same contract:
verify the landing object against its extraction manifest, skip snapshots whose content was
already accepted, append metadata columns and write one Bronze object per run.
"""

from __future__ import annotations

import io
import json
import logging
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

from erl import log
from erl.contracts import bronze_entity
from erl.errors import ContractError
from erl.ingestion import bronze
from erl.storage import ObjectStore

logger = logging.getLogger(__name__)


def ingest_snapshot(
    store: ObjectStore, landing_manifest_key: str, component: str = "local-python"
) -> bronze.IngestionManifest:
    lm = json.loads(store.get(landing_manifest_key))
    source, entity_name = lm["source_system"], lm["entity"]
    ref = date.fromisoformat(lm["reference_date"])
    run_id = lm["run_id"]
    entity = bronze_entity(source, entity_name)
    if completed := bronze.completed_delivery(
        store, source, entity_name, run_id, lm["object"], lm["content_sha256"]
    ):
        return completed
    payload = store.get(lm["object"])

    base = dict(
        run_id=run_id,
        source_system=source,
        entity=entity_name,
        reference_date=ref.isoformat(),
        component=component,
        input_object=lm["object"],
        input_sha256=lm["content_sha256"],
        snapshot_key=f"{source}/{entity_name}/{ref.isoformat()}",
    )
    if bronze.sha256_hex(payload) != lm["object_sha256"]:
        raise ContractError(f"landing object {lm['object']} does not match its manifest checksum")

    table = pq.read_table(io.BytesIO(payload))
    expected = [*entity.columns, "meta_row_number", "meta_row_hash"]
    if table.column_names != expected:
        raise ContractError(f"{entity.table}: landing columns {table.column_names} != {expected}")

    dup = bronze.find_accepted_duplicate(store, source, entity_name, ref, lm["content_sha256"])
    if dup is not None:
        m = bronze.IngestionManifest(
            **base, status=bronze.DUPLICATE, records_read=table.num_rows, duplicate_of_run=dup["run_id"]
        )
        bronze.write_manifest(store, m)
        log.event(
            logger,
            "snapshot_duplicate_skipped",
            run_id=run_id,
            source_system=source,
            entity=entity_name,
            reference_date=ref.isoformat(),
            duplicate_of_run=dup["run_id"],
        )
        return m

    records = table.num_rows
    if not records:
        # A zero-row Parquet carries no delivery metadata. Keep a control row so Silver
        # can supersede the previous nonempty snapshot before filtering the marker out.
        table = pa.Table.from_pylist(
            [dict.fromkeys(entity.columns) | {"meta_row_number": 0, "meta_row_hash": None}],
            schema=table.schema,
        )
    n = table.num_rows
    snapshot_key = f"{source}/{entity_name}/{ref.isoformat()}"
    meta = {
        "meta_source_system": [source] * n,
        "meta_entity": [entity_name] * n,
        "meta_run_id": [run_id] * n,
        "meta_ingested_at": [bronze.utc_now_iso()] * n,
        "meta_component": [component] * n,
        "meta_source_object": [lm["object"]] * n,
        "meta_source_sha256": [lm["object_sha256"]] * n,
        "meta_snapshot_key": [snapshot_key] * n,
    }
    columns = {c: table.column(c) for c in entity.columns}
    columns |= {k: pa.array(v, pa.string()) for k, v in meta.items()}
    columns["meta_row_number"] = table.column("meta_row_number")
    columns["meta_row_hash"] = table.column("meta_row_hash")
    out = pa.table(columns).select(bronze.bronze_schema(entity).names).cast(bronze.bronze_schema(entity))

    key = bronze.bronze_key(entity, ref, run_id)
    store.put(key, bronze.parquet_bytes(out))
    m = bronze.IngestionManifest(
        **base,
        status=bronze.ACCEPTED,
        records_read=records,
        records_written=records,
        bronze_object=key,
        raw_object=lm["object"],
    )
    bronze.write_manifest(store, m)
    log.event(
        logger,
        "snapshot_ingested",
        run_id=run_id,
        source_system=source,
        entity=entity_name,
        reference_date=ref.isoformat(),
        records_read=records,
        records_written=records,
        bronze_object=key,
    )
    return m
