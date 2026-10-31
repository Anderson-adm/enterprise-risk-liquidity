from __future__ import annotations

import importlib.util
import io
import os
from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import make_url

from erl_protocol.checksums import sha256_hex
from erl_protocol.contracts import consumer_contract, parquet_compression
from erl_protocol.objects import LocalStore
from erl_protocol.run_ids import new_run_id
from pipeline import config

ROOT = Path(__file__).resolve().parents[1]
D1, D2, D3 = date(2026, 10, 26), date(2026, 10, 27), date(2026, 10, 28)
CODEC = parquet_compression()  # the contract codec, passed explicitly like the pipeline does
EXTRACT = {"compression": CODEC, "batch_rows": 5_000}  # extraction_catalog keyword arguments


def load_entrypoint(relative_path: str):
    """Import a runtime entry point (``lambda/...``, ``glue/...``) from its file. Entry points are
    not packages; the built artifacts are tested separately by scripts/build/smoke_artifacts.py."""
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(f"entrypoint_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONTAINERS_UP = os.environ.get("ERL_IT_CONTAINERS") == "1"
REQUIRES_CONTAINERS = [
    pytest.mark.integration,
    pytest.mark.skipif(not CONTAINERS_UP, reason="set ERL_IT_CONTAINERS=1 with containers up"),
]
# Tests taking ``db_settings`` run once per backend: the SQLite stand-in always, the real
# PostgreSQL/SQL Server containers only with ``-m integration`` and ERL_IT_CONTAINERS=1.
BACKENDS = ["sqlite", pytest.param("containers", marks=REQUIRES_CONTAINERS)]
_URL_VARS = [f"ERL_{s.upper()}_URL" for s in config.DB_SOURCES] + ["ERL_CONSUMER_URL"]


def isolated_url(url: str) -> str:
    """Same server, separate ``it_`` database, so tests never touch the development data."""
    u = make_url(url)
    return u.set(database=f"it_{u.database}").render_as_string(hide_password=False)


def use_backend(monkeypatch, data_dir, backend: str) -> config.Settings:
    monkeypatch.setenv("ERL_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ERL_MODE", "local")
    monkeypatch.setenv("ERL_SOURCE_BACKEND", backend)
    for var in _URL_VARS:
        monkeypatch.delenv(var, raising=False)
    if backend == "containers":
        base = config.load()
        for source in config.DB_SOURCES:
            monkeypatch.setenv(f"ERL_{source.upper()}_URL", isolated_url(base.source_url(source)))
        monkeypatch.setenv("ERL_CONSUMER_URL", isolated_url(base.consumer_url()))
    return config.load()


@pytest.fixture
def settings(tmp_path, monkeypatch):
    return use_backend(monkeypatch, tmp_path / "data", "sqlite")


@pytest.fixture(params=BACKENDS)
def db_settings(request, tmp_path, monkeypatch):
    return use_backend(monkeypatch, tmp_path / "data", request.param)


def reset_consumer(engine) -> None:
    """Drop the publisher's tables (test databases only); ``publish`` recreates them."""
    from load import publisher

    with publisher._connect(engine) as conn:
        publisher.metadata.drop_all(conn)
        conn.commit()


@pytest.fixture
def store(tmp_path) -> LocalStore:
    return LocalStore(tmp_path / "lake")


def run_id() -> str:
    return new_run_id()


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
            "sha256": sha256_hex(data),
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
