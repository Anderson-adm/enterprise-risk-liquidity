"""Acceptance steps of a database extraction snapshot (landing -> Bronze), shared by the local
writer (``ingestion.snapshots``, PyArrow) and the Glue job (``glue/bronze/db_ingest.py``, Spark).

Each entry point composes the same steps; only the Bronze write differs:

1. ``completed_snapshot``  - a retry of this run returns the manifest already committed;
2. ``verify_landing_checksum`` - the landing object matches its extraction manifest;
3. ``record_duplicate``    - identical to the current accepted snapshot -> DUPLICATE manifest;
4. write Bronze under a run-scoped key (never overwrite);
5. ``record_accepted``     - ACCEPTED manifest = commit marker.

No PyArrow or Spark here.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date

from ingestion import manifests
from ingestion.manifests import IngestionManifest
from storage.objects import ObjectStore
from utils import structured_log
from utils.checksums import sha256_hex
from utils.errors import ContractError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LandingSnapshot:
    """Extraction manifest of one landing snapshot (written by ``extract.landing_snapshot``)."""

    manifest_key: str
    source_system: str
    entity: str
    reference_date: date
    run_id: str
    object: str
    object_sha256: str
    content_sha256: str
    rows: int
    content_hash_version: int | None  # None: landing written before identities were versioned

    @classmethod
    def read(cls, store: ObjectStore, manifest_key: str) -> LandingSnapshot:
        lm = json.loads(store.get(manifest_key))
        return cls(
            manifest_key=manifest_key,
            source_system=lm["source_system"],
            entity=lm["entity"],
            reference_date=date.fromisoformat(lm["reference_date"]),
            run_id=lm["run_id"],
            object=lm["object"],
            object_sha256=lm["object_sha256"],
            content_sha256=lm["content_sha256"],
            rows=lm["rows"],
            content_hash_version=lm.get("content_hash_version"),
        )

    @property
    def snapshot_key(self) -> str:
        return f"{self.source_system}/{self.entity}/{self.reference_date.isoformat()}"


def bronze_metadata(snapshot: LandingSnapshot, component: str, ingested_at: str) -> dict[str, str]:
    """Constant ``meta_*`` text columns of this delivery, in contract order."""
    return {
        "meta_source_system": snapshot.source_system,
        "meta_entity": snapshot.entity,
        "meta_run_id": snapshot.run_id,
        "meta_ingested_at": ingested_at,
        "meta_component": component,
        "meta_source_object": snapshot.object,
        "meta_source_sha256": snapshot.object_sha256,
        "meta_snapshot_key": snapshot.snapshot_key,
    }


def _manifest(snapshot: LandingSnapshot, component: str, **fields) -> IngestionManifest:
    return IngestionManifest(
        run_id=snapshot.run_id,
        source_system=snapshot.source_system,
        entity=snapshot.entity,
        reference_date=snapshot.reference_date.isoformat(),
        component=component,
        input_object=snapshot.object,
        input_sha256=snapshot.content_sha256,
        snapshot_key=snapshot.snapshot_key,
        input_hash_version=snapshot.content_hash_version,
        **fields,
    )


def completed_snapshot(store: ObjectStore, snapshot: LandingSnapshot) -> IngestionManifest | None:
    return manifests.completed_delivery(
        store,
        snapshot.source_system,
        snapshot.entity,
        snapshot.run_id,
        snapshot.object,
        snapshot.content_sha256,
    )


def verify_landing_checksum(snapshot: LandingSnapshot, actual_sha256: str) -> None:
    if actual_sha256 != snapshot.object_sha256:
        raise ContractError(f"landing object {snapshot.object} does not match its manifest checksum")


def landing_object_sha256(store: ObjectStore, snapshot: LandingSnapshot, payload: bytes | None = None) -> str:
    """Checksum of the landing object: from bytes already read, else from the store."""
    return sha256_hex(payload) if payload is not None else store.sha256_hex(snapshot.object)


def record_duplicate(
    store: ObjectStore, snapshot: LandingSnapshot, component: str, records_read: int
) -> IngestionManifest | None:
    dup = manifests.find_accepted_duplicate(
        store,
        snapshot.source_system,
        snapshot.entity,
        snapshot.reference_date,
        snapshot.content_sha256,
        hash_version=snapshot.content_hash_version,
    )
    if dup is None:
        return None
    m = _manifest(
        snapshot,
        component,
        status=manifests.DUPLICATE,
        records_read=records_read,
        duplicate_of_run=dup["run_id"],
    )
    manifests.write_manifest(store, m)
    structured_log.event(
        logger,
        "snapshot_duplicate_skipped",
        run_id=snapshot.run_id,
        source_system=snapshot.source_system,
        entity=snapshot.entity,
        reference_date=snapshot.reference_date.isoformat(),
        duplicate_of_run=dup["run_id"],
    )
    return m


def record_accepted(
    store: ObjectStore, snapshot: LandingSnapshot, component: str, bronze_objects: list[str], records: int
) -> IngestionManifest:
    m = _manifest(
        snapshot,
        component,
        status=manifests.ACCEPTED,
        records_read=records,
        records_written=records,
        bronze_object=",".join(bronze_objects),
        raw_object=snapshot.object,
    )
    manifests.write_manifest(store, m)
    structured_log.event(
        logger,
        "snapshot_ingested",
        run_id=snapshot.run_id,
        source_system=snapshot.source_system,
        entity=snapshot.entity,
        reference_date=snapshot.reference_date.isoformat(),
        records_read=records,
        records_written=records,
        bronze_object=m.bronze_object,
        parts=len(bronze_objects),
    )
    return m
