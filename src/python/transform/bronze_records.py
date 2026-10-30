"""Shape source records into the Bronze contract: canonical text, row hashes, metadata columns.

Pure functions (Arrow tables in, Arrow tables out). Business columns are canonical text exactly
as received (exact decimals, ISO dates); typing happens in Silver (dbt).
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa

from utils.contracts import BronzeEntity, metadata_columns

ROW_NUMBER = "meta_row_number"
ROW_HASH = "meta_row_hash"


def stringify(value: object) -> str | None:
    """Canonical text for DB values: exact decimals, ISO dates, no float noise."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, float):
        raise TypeError("binary float values are not accepted in financial extracts")
    return str(value)


def row_hash(values: Iterable[str | None]) -> str:
    """Stable hash of a record's business values (NULL distinct from empty string)."""
    joined = "\x1f".join("\x00" if v is None else v for v in values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def landing_schema(columns: Sequence[str]) -> pa.Schema:
    """Landing snapshot: business columns as text + row number + row hash."""
    return pa.schema(
        [pa.field(c, pa.string()) for c in columns]
        + [pa.field(ROW_NUMBER, pa.int64()), pa.field(ROW_HASH, pa.string())]
    )


def bronze_schema(entity: BronzeEntity) -> pa.Schema:
    fields = [pa.field(c, pa.string()) for c in entity.columns]
    for name in metadata_columns():
        fields.append(pa.field(name, pa.int64() if name == ROW_NUMBER else pa.string()))
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
        ROW_NUMBER: list(range(1, n + 1)),
        ROW_HASH: [row_hash(r) for r in rows],
    }
    return pa.Table.from_pydict(cols, schema=bronze_schema(entity))


def empty_snapshot_marker(entity: BronzeEntity, schema: pa.Schema) -> pa.Table:
    """A zero-row Parquet carries no delivery metadata. One control row (business columns NULL,
    row number 0) lets Silver supersede the previous nonempty snapshot before filtering it out."""
    return pa.Table.from_pylist(
        [dict.fromkeys(entity.columns) | {ROW_NUMBER: 0, ROW_HASH: None}], schema=schema
    )


def snapshot_to_bronze(entity: BronzeEntity, landing: pa.Table, meta: Mapping[str, str]) -> pa.Table:
    """Landing snapshot (business columns + row number/hash) -> Bronze table with ``meta``
    (the constant ``meta_*`` text columns of this delivery) in contract column order."""
    n = landing.num_rows
    columns = {c: landing.column(c) for c in entity.columns}
    columns |= {k: pa.array([v] * n, pa.string()) for k, v in meta.items()}
    columns[ROW_NUMBER] = landing.column(ROW_NUMBER)
    columns[ROW_HASH] = landing.column(ROW_HASH)
    schema = bronze_schema(entity)
    return pa.table(columns).select(schema.names).cast(schema)
