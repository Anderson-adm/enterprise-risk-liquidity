"""Publication package: the hand-off between the lakehouse (Gold) and the consumer database.

Layout and manifest: ``load.package_manifest``. Locally the package is built from DuckDB
(``build_from_duckdb``). On AWS the Glue job ``glue/load/gold_load.py`` builds the same package
from the Iceberg Gold tables. In both cases only rows written by the current pipeline run are
exported, and only after the quality gate passed. ``load_package`` re-verifies everything the
publisher reads: checksums, gate, products, columns, types, nullability, keys and date.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa

from load import package_manifest
from storage.objects import ObjectStore
from storage.parquet import parquet_bytes_to_table, table_to_parquet_bytes
from utils import structured_log
from utils.checksums import sha256_hex
from utils.contracts import consumer_contract
from utils.errors import PublicationError
from utils.sql_files import read_sql, render_identifiers

logger = logging.getLogger(__name__)

EXPORT_QUERY = "load/export_query_gold_product.sql"


def build_from_duckdb(
    store: ObjectStore, warehouse: Path, ref: date, run_id: str, quality_gate: dict, *, compression: str
) -> str:
    """Export this run's Gold rows of every consumer product; returns the manifest key."""
    package_manifest.require_passed_gate(quality_gate)
    products = {}
    template = read_sql(EXPORT_QUERY)
    with duckdb.connect(str(warehouse), read_only=True) as con:
        for name, spec in consumer_contract()["products"].items():
            columns = [c["name"] for c in spec["columns"]]
            sql = render_identifiers(
                template, {*columns, spec["gold_model"]}, columns=columns, gold_model=spec["gold_model"]
            )
            table = con.sql(sql, params={"reference_date": ref, "pipeline_run_id": run_id}).to_arrow_table()
            data = table_to_parquet_bytes(table, compression)
            key = package_manifest.product_key(ref, run_id, name)
            store.put(key, data)
            products[name] = {"object": key, "sha256": sha256_hex(data), "rows": table.num_rows}
    manifest = package_manifest.build_manifest(ref, run_id, "local-python", quality_gate, products)
    key = package_manifest.manifest_key(ref, run_id)
    store.put(key, json.dumps(manifest, indent=2).encode())
    structured_log.event(
        logger,
        "publication_package_built",
        run_id=run_id,
        reference_date=ref.isoformat(),
        manifest=key,
        rows={k: v["rows"] for k, v in products.items()},
    )
    return key


@dataclass(frozen=True)
class Package:
    uri: str
    reference_date: date
    pipeline_run_id: str
    products: dict[str, pa.Table]
    content_sha256: str


def load_package(store: ObjectStore, manifest_key: str) -> Package:
    try:
        manifest = json.loads(store.get(manifest_key))
    except Exception as exc:  # missing or unreadable manifest = no committed package
        raise PublicationError(f"cannot read package manifest {manifest_key}: {exc}") from exc
    gate = manifest.get("quality_gate", {})
    if gate.get("status") != "PASSED":
        raise PublicationError(f"package quality gate is {gate.get('status')!r}, not PASSED")
    ref = date.fromisoformat(manifest["reference_date"])
    contract = consumer_contract()["products"]
    if set(manifest["products"]) != set(contract):
        raise PublicationError(f"package products {sorted(manifest['products'])} != {sorted(contract)}")

    tables: dict[str, pa.Table] = {}
    for name, entry in manifest["products"].items():
        data = store.get(entry["object"])
        if sha256_hex(data) != entry["sha256"]:
            raise PublicationError(f"checksum mismatch for {entry['object']}")
        table = parquet_bytes_to_table(data)
        if table.num_rows != entry["rows"]:
            raise PublicationError(f"{name}: {table.num_rows} rows, manifest says {entry['rows']}")
        validate_product(name, table, ref)
        tables[name] = table
    return Package(store.uri(manifest_key), ref, manifest["pipeline_run_id"], tables, content_hash(tables))


def validate_product(name: str, table: pa.Table, ref: date) -> None:
    if not table.num_rows:
        raise PublicationError(f"{name}: empty product cannot replace the last valid publication")
    spec = consumer_contract()["products"][name]
    expected = [c["name"] for c in spec["columns"]]
    if table.column_names != expected:
        raise PublicationError(f"{name}: columns {table.column_names} != contract {expected}")
    for col in spec["columns"]:
        arrow_type = table.schema.field(col["name"]).type
        if not _type_matches(col["type"], arrow_type):
            raise PublicationError(f"{name}.{col['name']}: type {arrow_type} violates {col['type']}")
        if not col["nullable"] and table.column(col["name"]).null_count:
            raise PublicationError(f"{name}.{col['name']}: NULL in non-nullable column")
    rows = table.to_pylist()
    if any(r["reference_date"] != ref for r in rows):
        raise PublicationError(f"{name}: rows outside reference date {ref}")
    keys = [tuple(r[k] for k in spec["key"]) for r in rows]
    if len(keys) != len(set(keys)):
        raise PublicationError(f"{name}: duplicate keys {spec['key']}")


def _type_matches(logical: str, arrow_type: pa.DataType) -> bool:
    if logical == "date":
        return pa.types.is_date32(arrow_type)
    if logical == "integer":
        return pa.types.is_integer(arrow_type)
    if m := re.fullmatch(r"string\((\d+)\)", logical):
        return pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type)
    if m := re.fullmatch(r"decimal\((\d+),(\d+)\)", logical):
        return (
            pa.types.is_decimal(arrow_type)
            and arrow_type.scale == int(m.group(2))
            and arrow_type.precision <= int(m.group(1))
        )
    return False


def content_hash(products: dict[str, pa.Table]) -> str:
    """Hash of the business content, independent of file encoding and row order."""
    h = hashlib.sha256()
    for name in sorted(products):
        rows = sorted(json.dumps(r, default=_canonical, sort_keys=True) for r in products[name].to_pylist())
        h.update(name.encode())
        for r in rows:
            h.update(r.encode())
    return h.hexdigest()


def _canonical(v):
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, date):
        return v.isoformat()
    raise TypeError(type(v))
