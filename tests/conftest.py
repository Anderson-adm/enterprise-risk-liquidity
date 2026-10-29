from __future__ import annotations

import io
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from erl import config
from erl.contracts import consumer_contract
from erl.ingestion import bronze
from erl.storage import LocalStore

D1, D2, D3 = date(2026, 10, 26), date(2026, 10, 27), date(2026, 10, 28)


@pytest.fixture
def settings(tmp_path, monkeypatch):
    monkeypatch.setenv("ERL_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("ERL_MODE", "local")
    monkeypatch.setenv("ERL_SOURCE_BACKEND", "sqlite")
    for var in ("ERL_CONSUMER_URL", "ERL_FUND_OPS_URL", "ERL_PORTFOLIO_RISK_URL"):
        monkeypatch.delenv(var, raising=False)
    return config.load()


@pytest.fixture
def store(tmp_path) -> LocalStore:
    return LocalStore(tmp_path / "lake")


def run_id() -> str:
    return bronze.new_run_id()


def _arrow_type(logical: str) -> pa.DataType:
    if logical == "date":
        return pa.date32()
    if logical == "integer":
        return pa.int32()
    if logical.startswith("string"):
        return pa.string()
    p, s = logical.removeprefix("decimal(").rstrip(")").split(",")
    return pa.decimal128(int(p), int(s))


def product_rows(product: str, ref: date, portfolios: list[str], value: str = "100.00") -> pa.Table:
    """Build a contract-conformant product table for publisher tests."""
    spec = consumer_contract()["products"][product]
    rows = []
    for p in portfolios:
        row = {}
        for col in spec["columns"]:
            name, logical = col["name"], col["type"]
            if name == "reference_date":
                row[name] = ref
            elif name == "portfolio_id":
                row[name] = p
            elif logical == "date":
                row[name] = ref
            elif logical == "integer":
                row[name] = 1
            elif logical.startswith("string"):
                row[name] = "PASS" if name == "concentration_check" else "X"
            else:
                scale = int(logical.rstrip(")").split(",")[1])
                row[name] = Decimal(value).quantize(Decimal(1).scaleb(-scale)) if scale else Decimal(value)
        rows.append(row)
    schema = pa.schema([pa.field(c["name"], _arrow_type(c["type"])) for c in spec["columns"]])
    return pa.Table.from_pylist(rows, schema=schema)


def write_package(store, ref: date, run: str, tables: dict[str, pa.Table], gate: str = "PASSED") -> str:
    import json

    prefix = f"publish/reference_date={ref.isoformat()}/{run}/"
    products = {}
    for name, table in tables.items():
        buf = io.BytesIO()
        pq.write_table(table, buf)
        data = buf.getvalue()
        store.put(prefix + f"{name}.parquet", data)
        products[name] = {
            "object": prefix + f"{name}.parquet",
            "sha256": bronze.sha256_hex(data),
            "rows": table.num_rows,
        }
    manifest = {
        "pipeline_run_id": run,
        "reference_date": ref.isoformat(),
        "producer": "test",
        "quality_gate": {"status": gate},
        "products": products,
    }
    store.put(prefix + "manifest.json", json.dumps(manifest).encode())
    return prefix + "manifest.json"
