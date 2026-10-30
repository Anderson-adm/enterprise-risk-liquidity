"""Regression for the AWS ingestion failure of 2026-10-30: the managed pandas layer's PyArrow
raised ``ArrowNotImplementedError: Support for codec 'zstd' not built`` *after* the raw XML
had been written, leaving a Bronze object without a commit manifest.

The exact managed layer is not available offline (fetching it would be an AWS call), so the
failure mode is reproduced by a PyArrow whose writer lacks the codec. The fix is verified on
the built package by scripts/build/smoke_artifacts.py."""

from __future__ import annotations

import io
from datetime import datetime
from decimal import Decimal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from connect.databases import engine_for
from ingestion import files, snapshots
from pipeline import extraction_catalog
from sources import seed, synthetic
from storage.parquet import require_codec, table_to_parquet_bytes
from tests.conftest import CODEC, D1, EXTRACT, run_id
from utils.contracts import parquet_compression
from utils.errors import ConfigurationError


def test_contract_codec_round_trips_the_contract_value_types(tmp_path):
    codec = parquet_compression()
    schema = pa.schema(
        [
            pa.field("quantity", pa.decimal128(24, 6)),
            pa.field("market_value", pa.decimal128(20, 2)),
            pa.field("processed_at", pa.timestamp("us")),
            pa.field("reference_date", pa.date32()),
            pa.field("code", pa.string()),
        ]
    )
    rows = [
        {
            "quantity": Decimal("999999999999999999.999999"),
            "market_value": Decimal("-999999999999999999.99"),
            "processed_at": datetime(2026, 10, 27, 19, 0, 0, 123456),
            "reference_date": D1,
            "code": "SYN-EQ-005",
        },
        dict.fromkeys(schema.names),
    ]
    table = pa.Table.from_pylist(rows, schema=schema)
    path = tmp_path / "roundtrip.parquet"
    path.write_bytes(table_to_parquet_bytes(table, parquet_compression()))
    assert pq.read_table(path).equals(table)
    assert pq.ParquetFile(path).metadata.row_group(0).column(0).compression == codec.upper()
    with duckdb.connect() as con:  # the local Silver/Gold reader decodes the same file exactly
        assert con.execute("SELECT quantity, market_value FROM read_parquet(?)", [str(path)]).fetchone() == (
            Decimal("999999999999999999.999999"),
            Decimal("-999999999999999999.99"),
        )


@pytest.fixture
def pyarrow_without_codec(monkeypatch):
    """A PyArrow build whose Parquet writer lacks the contract codec (like the managed layer)."""
    real = pq.write_table
    codec = parquet_compression()

    def write_table(table, where, *args, compression="snappy", **kwargs):
        if compression == codec:
            raise pa.ArrowNotImplementedError(f"Support for codec '{codec}' not built")
        return real(table, where, *args, compression=compression, **kwargs)

    monkeypatch.setattr(pq, "write_table", write_table)


def test_codec_check_is_a_real_round_trip(pyarrow_without_codec):
    with pytest.raises(ConfigurationError, match="cannot use Parquet codec"):
        require_codec(parquet_compression())


def test_file_delivery_fails_before_any_write_without_the_codec(store, pyarrow_without_codec):
    name, body = next((n, b) for s, d, n, b in synthetic.generate().files if s == "external_xml" and d == D1)
    with pytest.raises(ConfigurationError):
        files.ingest_file(
            store,
            source_system="external_xml",
            object_name=name,
            data=body,
            run_id=run_id(),
            run_reference_date=D1,
            component="test",
            compression=CODEC,
        )
    assert store.list("") == [], "no raw copy, Bronze object or manifest may be left behind"


def test_snapshot_ingestion_fails_before_any_write_without_the_codec(settings, store, monkeypatch):
    seed.generate_all(settings, (D1,))
    engine = engine_for(settings.source_url("fund_ops"))
    try:
        landing = extraction_catalog.extract_entity(
            engine, store, "fund_ops", "fund", D1, run_id(), **EXTRACT
        )
    finally:
        engine.dispose()
    before = store.list("")
    real = pq.write_table

    def write_table(table, where, *args, compression="snappy", **kwargs):
        if compression == parquet_compression():
            raise pa.ArrowNotImplementedError("Support for codec not built")
        return real(table, where, *args, compression=compression, **kwargs)

    monkeypatch.setattr(pq, "write_table", write_table)
    manifest_key = landing["object"].removesuffix(".parquet") + ".json"
    with pytest.raises(ConfigurationError):
        snapshots.ingest_snapshot(store, manifest_key, compression=CODEC)
    assert store.list("") == before


def test_bronze_objects_are_written_with_the_contract_codec(store):
    name, body = next((n, b) for s, d, n, b in synthetic.generate().files if s == "market_data" and d == D1)
    m = files.ingest_file(
        store,
        source_system="market_data",
        object_name=name,
        data=body,
        run_id=run_id(),
        run_reference_date=D1,
        component="test",
        compression=CODEC,
    )
    meta = pq.ParquetFile(io.BytesIO(store.get(m.bronze_object))).metadata
    assert {meta.row_group(0).column(i).compression for i in range(meta.num_columns)} == {
        parquet_compression().upper()
    }
