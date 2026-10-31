"""Versioned file acceptance/commit protocol, with a caller-supplied parser.

No format-specific implementation lives here. The unit supplies parse and entity_name;
local composition and Lambda handlers use this same pinned integrity implementation.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import date

from erl_protocol import layout, manifests, structured_log
from erl_protocol.checksums import sha256_hex
from erl_protocol.clock import utc_now_iso
from erl_protocol.file_contract import ParsedDelivery, RejectedDelivery
from erl_protocol.manifests import IngestionManifest
from erl_protocol.objects import ObjectStore
from erl_protocol.parquet import require_codec, table_to_parquet_bytes
from erl_protocol.records import build_bronze_table

logger = logging.getLogger(__name__)


def write_raw(store: ObjectStore, source_system: str, ref: date, run_id: str, name: str, data: bytes) -> str:
    """Keep the delivered bytes as received (run-scoped key)."""
    key = layout.raw_key(source_system, ref, run_id, name)
    store.put(key, data)
    return key


def quarantine(
    store: ObjectStore,
    *,
    source_system: str,
    ref: date,
    run_id: str,
    name: str,
    data: bytes,
    reason: str,
    detail: str,
) -> str:
    """Store a rejected delivery and its reason next to it (``<file>.rejection.json``)."""
    key = layout.quarantine_key(source_system, ref, run_id, name)
    store.put(key, data)
    store.put(
        key + ".rejection.json",
        json.dumps(
            {
                "run_id": run_id,
                "source_system": source_system,
                "reference_date": ref.isoformat(),
                "object": name,
                "sha256": sha256_hex(data),
                "reason": reason,
                "detail": detail,
                "quarantined_at": utc_now_iso(),
            },
            indent=2,
        ).encode("utf-8"),
    )
    return key


def ingest_file(
    store: ObjectStore,
    *,
    source_system: str,
    object_name: str,
    data: bytes,
    run_id: str,
    run_reference_date: date,
    component: str,
    compression: str,
    parse: Callable[[bytes], ParsedDelivery],
    entity_name: str,
) -> IngestionManifest:
    """Validate one delivered file and append it to Bronze. Never raises for bad *data*:
    contract violations end in quarantine with a REJECTED manifest. Infrastructure errors
    (storage unavailable) and a runtime unable to write the contract's Parquet codec
    (``ConfigurationError``, raised before any write) do propagate."""
    digest = sha256_hex(data)
    if completed := manifests.completed_delivery(
        store, source_system, entity_name, run_id, object_name, digest
    ):
        return completed
    require_codec(compression)  # fail before the raw copy: no orphan objects on a bad runtime
    raw_key = write_raw(store, source_system, run_reference_date, run_id, object_name, data)

    def manifest(**kw) -> IngestionManifest:
        base = dict(
            run_id=run_id,
            source_system=source_system,
            entity=entity_name,
            component=component,
            input_object=object_name,
            input_sha256=digest,
            raw_object=raw_key,
        )
        return IngestionManifest(**(base | kw))

    try:
        parsed = parse(data)
        _check_reference_date(source_system, parsed.reference_date, run_reference_date)
    except RejectedDelivery as rej:
        ref = rej.reference_date or run_reference_date
        quarantine(
            store,
            source_system=source_system,
            ref=ref,
            run_id=run_id,
            name=object_name,
            data=data,
            reason=rej.reason,
            detail=rej.detail,
        )
        m = manifest(
            reference_date=ref.isoformat(),
            status=manifests.REJECTED,
            rejection_reason=rej.reason,
            rejection_detail=rej.detail[:2000],
        )
        manifests.write_manifest(store, m)
        structured_log.event(logger, "file_rejected", logging.WARNING, **_fields(m))
        return m

    ref = parsed.reference_date
    late = ref < run_reference_date
    dup = manifests.find_accepted_duplicate(
        store, source_system, entity_name, ref, digest, snapshot_key=parsed.snapshot_key
    )
    if dup is not None:
        m = manifest(
            reference_date=ref.isoformat(),
            status=manifests.DUPLICATE,
            records_read=len(parsed.rows),
            duplicate_of_run=dup["run_id"],
            late_arrival=late,
        )
        manifests.write_manifest(store, m)
        structured_log.event(logger, "file_duplicate_skipped", **_fields(m))
        return m

    ingested_at = utc_now_iso()
    table = build_bronze_table(
        parsed.entity,
        parsed.rows,
        run_id=run_id,
        component=component,
        source_object=object_name,
        source_sha256=digest,
        snapshot_keys=[parsed.snapshot_key] * len(parsed.rows),
        ingested_at=ingested_at,
    )
    # one Bronze object per delivered file: suffix keeps several files of one run apart
    suffix = "__" + layout.safe_name(object_name.rsplit("/", 1)[-1]).rsplit(".", 1)[0]
    key = layout.bronze_key(parsed.entity, ref, run_id, suffix)
    store.put(key, table_to_parquet_bytes(table, compression))
    m = manifest(
        reference_date=ref.isoformat(),
        status=manifests.ACCEPTED,
        records_read=len(parsed.rows),
        records_written=table.num_rows,
        bronze_object=key,
        late_arrival=late,
        snapshot_key=parsed.snapshot_key,
    )
    manifests.write_manifest(store, m)
    structured_log.event(logger, "file_ingested", **_fields(m))
    return m


def _check_reference_date(source_system: str, delivered: date, requested: date) -> None:
    if delivered > requested:
        raise RejectedDelivery(
            "FUTURE_REFERENCE_DATE", f"delivery for {delivered} in run for {requested}", requested
        )
    if source_system == "market_data" and delivered != requested:
        # Market data is a daily snapshot; a stale file must not be used as today's prices.
        raise RejectedDelivery(
            "STALE_REFERENCE_DATE", f"market data for {delivered} delivered as {requested}", requested
        )


def _fields(m: IngestionManifest) -> dict:
    return {
        "run_id": m.run_id,
        "source_system": m.source_system,
        "entity": m.entity,
        "reference_date": m.reference_date,
        "status": m.status,
        "records_read": m.records_read,
        "records_written": m.records_written,
        "input_object": m.input_object,
        "late_arrival": m.late_arrival,
        "rejection_reason": m.rejection_reason,
    }
