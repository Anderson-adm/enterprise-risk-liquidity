"""Write one query result as a landing snapshot (Parquet + extraction manifest).

    <prefix><run_id>.parquet   business columns as canonical text + row number + row hash
    <prefix><run_id>.json      extraction manifest (commit marker of the snapshot)

Rows are streamed in batches and written as Parquet row groups, so memory stays bounded for
large legacy extractions. A retry of the same run returns the committed manifest after
re-verifying the object checksum. The landing snapshot is consumed by the Bronze ingestion
(local ``ingestion.snapshots`` or the Glue ``db_ingest`` job).
"""

from __future__ import annotations

import hashlib
import json
import logging
import tempfile
import time
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import Engine
from sqlalchemy.types import TypeEngine

from erl_protocol import structured_log
from erl_protocol.checksums import sha256_hex
from erl_protocol.clock import utc_now_iso
from erl_protocol.errors import ContractError
from erl_protocol.objects import ObjectStore
from erl_protocol.records import ROW_HASH, ROW_HASH_VERSION, ROW_NUMBER, landing_schema, row_hash, stringify
from extract.sql_query import execute_query_in_batches

logger = logging.getLogger(__name__)


def extract_to_landing(
    engine: Engine,
    store: ObjectStore,
    *,
    prefix: str,
    source: str,
    entity: str,
    reference_date: date,
    run_id: str,
    sql: str,
    params: Mapping[str, Any],
    columns: Sequence[str],
    compression: str,
    batch_size: int,
    result_types: Mapping[str, TypeEngine] | None = None,
) -> dict:
    """Run ``sql`` and commit its rows under ``prefix``; returns the extraction manifest."""
    manifest_key = prefix + f"{run_id}.json"
    if store.exists(manifest_key):
        committed = json.loads(store.get(manifest_key))
        if sha256_hex(store.get(committed["object"])) != committed["object_sha256"]:
            raise ContractError(f"committed landing snapshot checksum mismatch: {manifest_key}")
        return committed

    schema = landing_schema(columns)
    content = hashlib.sha256()
    written = 0
    started = time.monotonic()
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "extract.parquet"
        with pq.ParquetWriter(path, schema, compression=compression) as writer:

            def write_batch(chunk) -> None:
                nonlocal written
                values = [tuple(stringify(v) for v in row) for row in chunk]
                hashes = [row_hash(v) for v in values]
                for h in hashes:
                    content.update(h.encode())
                data = {c: [v[i] for v in values] for i, c in enumerate(columns)}
                data[ROW_NUMBER] = list(range(written + 1, written + len(values) + 1))
                data[ROW_HASH] = hashes
                writer.write_table(pa.Table.from_pydict(data, schema=schema))
                written += len(values)

            result = execute_query_in_batches(
                engine,
                sql,
                params,
                expected_columns=columns,
                batch_size=batch_size,
                consume=write_batch,
                result_types=result_types,
            )
        if result.rows == 0:
            pq.write_table(schema.empty_table(), path, compression=compression)
        payload = path.read_bytes()

    data_key = store.put(prefix + f"{run_id}.parquet", payload)
    manifest = {
        "run_id": run_id,
        "source_system": source,
        "entity": entity,
        "reference_date": reference_date.isoformat(),
        "rows": result.rows,
        "object": prefix + f"{run_id}.parquet",
        "object_sha256": sha256_hex(payload),
        "content_sha256": content.hexdigest(),  # order-sensitive hash of row hashes
        "content_hash_version": ROW_HASH_VERSION,
        "query": sql,
        "extracted_at": utc_now_iso(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    store.put(manifest_key, json.dumps(manifest, indent=2).encode())
    structured_log.event(
        logger,
        "entity_extracted",
        run_id=run_id,
        source_system=source,
        entity=entity,
        reference_date=reference_date.isoformat(),
        records_read=result.rows,
        batches=result.batches,
        landing_object=data_key,
        elapsed_time=manifest["elapsed_seconds"],
    )
    return manifest
