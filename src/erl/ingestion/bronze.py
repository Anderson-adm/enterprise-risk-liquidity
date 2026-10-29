"""Bronze zone: raw copies, structured Parquet snapshots, ingestion manifests and quarantine.

Key layout (identical for the local directory store and S3):

    bronze/raw/<source>/bronze_reference_date=<D>/<run_id>/<original file name>
    bronze/structured/<source>/<entity>/bronze_reference_date=<D>/<run_id>.parquet
    manifests/ingestion/<source>/<entity>/bronze_reference_date=<D>/<run_id>__<object>.json
    quarantine/<source>/bronze_reference_date=<D>/<run_id>/<file>  (+ <file>.rejection.json)

Every key contains the run id, so a re-run writes new objects instead of replacing old ones.
Deliveries identical to the current accepted snapshot are recorded as DUPLICATE and not
written again; reverting to an older historical version is a new accepted delivery.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import secrets
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from erl.contracts import PARTITION_COLUMN, BronzeEntity, bronze_contract, metadata_columns
from erl.errors import ContractError
from erl.storage import ObjectStore

ACCEPTED = "ACCEPTED"
DUPLICATE = "DUPLICATE"
REJECTED = "REJECTED"

_RUN_ID = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{6}$")


def new_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    return f"{now:%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


def validate_run_id(run_id: str) -> str:
    if not _RUN_ID.match(run_id):
        raise ValueError(f"invalid run_id {run_id!r}; expected YYYYMMDDTHHMMSSZ-xxxxxx")
    return run_id


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def row_hash(values: Iterable[str | None]) -> str:
    """Stable hash of a record's business values (NULL distinct from empty string)."""
    joined = "\x1f".join("\x00" if v is None else v for v in values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def _partition(d: date) -> str:
    return f"{PARTITION_COLUMN}={d.isoformat()}"


def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)[:150]


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

    def key(self) -> str:
        obj = safe_name(self.input_object.rsplit("/", 1)[-1])
        return (
            f"manifests/ingestion/{self.source_system}/{self.entity}/"
            f"{_partition(date.fromisoformat(self.reference_date))}/{self.run_id}__{obj}.json"
        )


def write_manifest(store: ObjectStore, manifest: IngestionManifest) -> str:
    return store.put(manifest.key(), json.dumps(asdict(manifest), indent=2).encode("utf-8"))


def list_manifests(store: ObjectStore, source_system: str, entity: str, ref: date) -> list[dict]:
    prefix = f"manifests/ingestion/{source_system}/{entity}/{_partition(ref)}/"
    return [json.loads(store.get(k)) for k in store.list(prefix) if k.endswith(".json")]


def completed_delivery(
    store: ObjectStore, source: str, entity: str, run_id: str, input_object: str, input_sha256: str
) -> IngestionManifest | None:
    suffix = f"/{run_id}__{safe_name(input_object.rsplit('/', 1)[-1])}.json"
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
) -> dict | None:
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
    return latest if latest and latest["input_sha256"] == input_sha256 else None


def write_raw(store: ObjectStore, source_system: str, ref: date, run_id: str, name: str, data: bytes) -> str:
    key = f"bronze/raw/{source_system}/{_partition(ref)}/{run_id}/{safe_name(name)}"
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
    key = f"quarantine/{source_system}/{_partition(ref)}/{run_id}/{safe_name(name)}"
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


def bronze_key(entity: BronzeEntity, ref: date, run_id: str, suffix: str = "") -> str:
    return (
        f"bronze/structured/{entity.source_system}/{entity.entity}/{_partition(ref)}/{run_id}{suffix}.parquet"
    )


def bronze_schema(entity: BronzeEntity) -> pa.Schema:
    fields = [pa.field(c, pa.string()) for c in entity.columns]
    for name in metadata_columns():
        fields.append(pa.field(name, pa.int64() if name == "meta_row_number" else pa.string()))
    return pa.schema(fields)


def build_bronze_table(
    entity: BronzeEntity,
    rows: Sequence[Sequence[str | None]],
    *,
    run_id: str,
    component: str,
    source_object: str,
    source_sha256: str,
    snapshot_keys: Sequence[str],
    ingested_at: str,
) -> pa.Table:
    """Rows are business values in contract column order, already stringified."""
    if len(snapshot_keys) != len(rows):
        raise ValueError("one snapshot key per row is required")
    width = len(entity.columns)
    for i, r in enumerate(rows, start=1):
        if len(r) != width:
            raise ValueError(f"row {i} has {len(r)} values, contract {entity.table} expects {width}")
    cols: dict[str, list] = {c: [r[i] for r in rows] for i, c in enumerate(entity.columns)}
    n = len(rows)
    cols |= {
        "meta_source_system": [entity.source_system] * n,
        "meta_entity": [entity.entity] * n,
        "meta_run_id": [run_id] * n,
        "meta_ingested_at": [ingested_at] * n,
        "meta_component": [component] * n,
        "meta_source_object": [source_object] * n,
        "meta_source_sha256": [source_sha256] * n,
        "meta_snapshot_key": list(snapshot_keys),
        "meta_row_number": list(range(1, n + 1)),
        "meta_row_hash": [row_hash(r) for r in rows],
    }
    return pa.Table.from_pydict(cols, schema=bronze_schema(entity))


def parquet_bytes(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


def stringify(value: object) -> str | None:
    """Canonical text for DB values: exact decimals, ISO dates, no float noise."""
    from decimal import Decimal

    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        raise TypeError("binary float values are not accepted in financial extracts")
    return str(value)
