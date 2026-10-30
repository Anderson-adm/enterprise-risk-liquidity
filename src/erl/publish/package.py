"""Publication package: the hand-off between the lakehouse (Gold) and the consumer database.

    publish/reference_date=<D>/<run_id>/<product>.parquet
    publish/reference_date=<D>/<run_id>/manifest.json    (written last = commit marker)

Locally the package is built from DuckDB (this module). On AWS the Glue job
``glue/jobs/gold_load.py`` builds the same package from the Iceberg Gold tables. In both cases
only rows written by the current pipeline run are exported, and only after the quality gate
passed. The publisher re-verifies everything it reads.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa

from erl import log
from erl.contracts import consumer_contract
from erl.errors import QualityGateError
from erl.ingestion import bronze
from erl.storage import ObjectStore

logger = logging.getLogger(__name__)


def package_prefix(ref: date, run_id: str) -> str:
    return f"publish/reference_date={ref.isoformat()}/{run_id}/"


def build_from_duckdb(store: ObjectStore, warehouse: Path, ref: date, run_id: str, quality_gate: dict) -> str:
    if quality_gate.get("status") != "PASSED":
        raise QualityGateError(
            f"refusing to build a publication package: gate is {quality_gate.get('status')}"
        )
    products = {}
    prefix = package_prefix(ref, run_id)
    with duckdb.connect(str(warehouse), read_only=True) as con:
        for name, spec in consumer_contract()["products"].items():
            cols = ", ".join(c["name"] for c in spec["columns"])
            table = con.sql(
                f"SELECT {cols} FROM gold.{spec['gold_model']} "
                "WHERE reference_date = $ref AND pipeline_run_id = $run ORDER BY portfolio_id",
                params={"ref": ref, "run": run_id},
            ).to_arrow_table()
            products[name] = _put_product(store, prefix, name, table)
    manifest = {
        "pipeline_run_id": run_id,
        "reference_date": ref.isoformat(),
        "producer": "local-python",
        "quality_gate": quality_gate,
        "products": products,
        "created_at": bronze.utc_now_iso(),
    }
    key = prefix + "manifest.json"
    store.put(key, json.dumps(manifest, indent=2).encode())
    log.event(
        logger,
        "publication_package_built",
        run_id=run_id,
        reference_date=ref.isoformat(),
        manifest=key,
        rows={k: v["rows"] for k, v in products.items()},
    )
    return key


def _put_product(store: ObjectStore, prefix: str, name: str, table: pa.Table) -> dict:
    data = bronze.parquet_bytes(table)
    key = prefix + f"{name}.parquet"
    store.put(key, data)
    return {"object": key, "sha256": bronze.sha256_hex(data), "rows": table.num_rows}
