"""Ingestion manifests: the commit marker of every Bronze delivery.

A delivery is visible to Silver only through an ACCEPTED manifest. Deliveries identical to the
current accepted snapshot are recorded as DUPLICATE and not written again; reverting to an
older historical version is a new accepted delivery. Pure JSON + object store: no PyArrow,
so the Glue library can import this module.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date

from erl_protocol import layout
from erl_protocol.clock import utc_now_iso
from erl_protocol.contracts import bronze_contract
from erl_protocol.errors import ContractError
from erl_protocol.objects import ObjectStore

ACCEPTED = "ACCEPTED"
DUPLICATE = "DUPLICATE"
REJECTED = "REJECTED"


@dataclass
class IngestionManifest:
    run_id: str
    source_system: str
    entity: str
    reference_date: str
    status: str
    component: str
    input_object: str
    input_sha256: str
    records_read: int = 0
    records_written: int = 0
    records_rejected: int = 0
    bronze_object: str | None = None
    raw_object: str | None = None
    late_arrival: bool = False
    duplicate_of_run: str | None = None
    rejection_reason: str | None = None
    rejection_detail: str | None = None
    ingested_at: str = field(default_factory=utc_now_iso)
    snapshot_key: str | None = None
    input_hash_version: int | None = None  # DB-snapshot row-identity version; None: file bytes/legacy

    def key(self) -> str:
        return layout.manifest_key(
            self.source_system,
            self.entity,
            date.fromisoformat(self.reference_date),
            self.run_id,
            self.input_object,
        )


def write_manifest(store: ObjectStore, manifest: IngestionManifest) -> str:
    return store.put(manifest.key(), json.dumps(asdict(manifest), indent=2).encode("utf-8"))


def list_manifests(store: ObjectStore, source_system: str, entity: str, ref: date) -> list[dict]:
    prefix = f"manifests/ingestion/{source_system}/{entity}/{layout.partition(ref)}/"
    return [json.loads(store.get(k)) for k in store.list(prefix) if k.endswith(".json")]


def completed_delivery(
    store: ObjectStore, source: str, entity: str, run_id: str, input_object: str, input_sha256: str
) -> IngestionManifest | None:
    """The manifest this run already wrote for ``input_object`` (a retry), if any."""
    suffix = f"/{run_id}__{layout.safe_name(input_object.rsplit('/', 1)[-1])}.json"
    for key in store.list(f"manifests/ingestion/{source}/{entity}/"):
        if key.endswith(suffix):
            manifest = json.loads(store.get(key))
            if manifest["input_sha256"] != input_sha256 or manifest["input_object"] != input_object:
                raise ContractError(f"delivery changed within immutable run {run_id}; use a new run id")
            return IngestionManifest(**manifest)
    return None


def find_accepted_duplicate(
    store: ObjectStore,
    source_system: str,
    entity: str,
    ref: date,
    input_sha256: str,
    *,
    snapshot_key: str | None = None,
    hash_version: int | None = None,
) -> dict | None:
    """The latest ACCEPTED delivery of the same snapshot, if its content equals ``input_sha256``
    computed with the same identity version (``hash_version``; legacy manifests have none).

    Reference registers (``snapshot_scope: latest`` in the contract) compare across dates."""
    spec = bronze_contract()["entities"][f"{source_system}__{entity}"]
    if spec.get("snapshot_scope") == "latest":
        prefix = f"manifests/ingestion/{source_system}/{entity}/"
        manifests = [json.loads(store.get(k)) for k in store.list(prefix) if k.endswith(".json")]
    else:
        manifests = list_manifests(store, source_system, entity, ref)
    accepted = [
        m
        for m in manifests
        if m["status"] == ACCEPTED and (snapshot_key is None or m.get("snapshot_key") == snapshot_key)
    ]
    latest = max(accepted, key=lambda m: (m["ingested_at"], m["run_id"]), default=None)
    if not latest or latest["input_sha256"] != input_sha256:
        return None
    return latest if latest.get("input_hash_version") == hash_version else None
