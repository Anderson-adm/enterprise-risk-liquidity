"""Local (PyArrow) Bronze ingestion of database extraction snapshots.

The Glue job ``glue/db-gb/job.py`` composes the same steps of
``ingestion.snapshot_manifests`` with a Spark writer.
"""

from __future__ import annotations

from erl_protocol import layout
from erl_protocol.clock import utc_now_iso
from erl_protocol.contracts import bronze_entity
from erl_protocol.errors import ContractError
from erl_protocol.manifests import IngestionManifest
from erl_protocol.objects import ObjectStore
from erl_protocol.parquet import parquet_bytes_to_table, require_codec, table_to_parquet_bytes
from erl_protocol.records import ROW_HASH, ROW_NUMBER, empty_snapshot_marker, snapshot_to_bronze
from erl_protocol.snapshot_manifests import (
    LandingSnapshot,
    bronze_metadata,
    completed_snapshot,
    landing_object_sha256,
    record_accepted,
    record_duplicate,
    verify_landing_checksum,
)


def ingest_snapshot(
    store: ObjectStore, landing_manifest_key: str, *, compression: str, component: str = "local-python"
) -> IngestionManifest:
    """Append one landing snapshot to Bronze (or record it as DUPLICATE); returns its manifest.

    ``compression`` is the contract codec; it is proven before anything is written."""
    snapshot = LandingSnapshot.read(store, landing_manifest_key)
    entity = bronze_entity(snapshot.source_system, snapshot.entity)
    if completed := completed_snapshot(store, snapshot):
        return completed
    require_codec(compression)
    payload = store.get(snapshot.object)
    verify_landing_checksum(snapshot, landing_object_sha256(store, snapshot, payload))

    table = parquet_bytes_to_table(payload)
    expected = [*entity.columns, ROW_NUMBER, ROW_HASH]
    if table.column_names != expected:
        raise ContractError(f"{entity.table}: landing columns {table.column_names} != {expected}")
    if duplicate := record_duplicate(store, snapshot, component, table.num_rows):
        return duplicate

    records = table.num_rows
    if not records:
        table = empty_snapshot_marker(entity, table.schema)
    out = snapshot_to_bronze(entity, table, bronze_metadata(snapshot, component, utc_now_iso()))
    key = layout.bronze_key(entity, snapshot.reference_date, snapshot.run_id)
    store.put(key, table_to_parquet_bytes(out, compression))
    return record_accepted(store, snapshot, component, [key], records)
