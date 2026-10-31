"""Database extraction and Bronze snapshot ingestion. Runs on the SQLite stand-in and, with
``-m integration`` and ERL_IT_CONTAINERS=1, on the real PostgreSQL/SQL Server containers."""

from __future__ import annotations

import io
import json

import pyarrow.parquet as pq
import pytest
from sqlalchemy import Date, bindparam, text

from connect.databases import engine_for
from erl_protocol import manifests
from erl_protocol.checksums import sha256_hex
from erl_protocol.errors import ContractError, SourceUnavailableError
from erl_protocol.records import stringify
from ingestion import snapshots
from pipeline import extraction_catalog
from sources import seed, synthetic
from tests.conftest import CODEC, D1, D2, EXTRACT, run_id


@pytest.fixture
def seeded(db_settings):
    seed.generate_all(db_settings)
    return db_settings


def _extract(settings, store, source, ref, run=None, **overrides):
    engine = engine_for(settings.source_url(source))
    try:
        return extraction_catalog.extract_source(
            engine, store, source, ref, run or run_id(), **(EXTRACT | overrides)
        )
    finally:
        engine.dispose()


def test_every_database_entity_in_the_contract_has_an_extract(seeded):
    from erl_protocol.contracts import bronze_contract

    db_entities = {
        (s["source_system"], s["entity"])
        for s in bronze_contract()["entities"].values()
        if s["delivery"] == "database"
    }
    assert db_entities == set(extraction_catalog.EXTRACT_QUERIES)


def test_extraction_keeps_exact_decimals_and_counts(seeded, store):
    manifests = {m["entity"]: m for m in _extract(seeded, store, "portfolio_risk", D2)}
    pos = manifests["processed_position"]
    expected = [
        r
        for r in synthetic.generate().tables["portfolio_risk"]["processed_position"]
        if r["reference_date"] == D2
    ]
    assert pos["rows"] == len(expected)
    table = pq.read_table(io.BytesIO(store.get(pos["object"])))
    assert table.column("market_value").to_pylist() == [format(r["market_value"], "f") for r in expected]
    assert pos["object_sha256"] == sha256_hex(store.get(pos["object"]))


def test_chunked_extraction_produces_the_same_content(seeded, store):
    full = {m["entity"]: m for m in _extract(seeded, store, "exposure_compliance", D1)}
    chunked = {m["entity"]: m for m in _extract(seeded, store, "exposure_compliance", D1, batch_rows=3)}
    for entity in full:
        assert full[entity]["content_sha256"] == chunked[entity]["content_sha256"]


def test_float_values_are_refused():
    with pytest.raises(TypeError):
        stringify(0.1)


def test_risk_extraction_waits_for_completed_batch(seeded, store):
    engine = engine_for(seeded.source_url("portfolio_risk"))
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE processing_batch SET status = 'RUNNING' WHERE reference_date = :d").bindparams(
                bindparam("d", type_=Date)
            ),
            {"d": D1},
        )
    engine.dispose()
    with pytest.raises(SourceUnavailableError, match="RUNNING"):
        _extract(seeded, store, "portfolio_risk", D1)


def test_unreachable_database_is_source_unavailable(seeded, store, monkeypatch):
    monkeypatch.setenv("ERL_LEGACY_FINANCE_URL", "postgresql+psycopg://nobody:x@127.0.0.1:1/none")
    with pytest.raises(SourceUnavailableError, match="legacy_finance unreachable"):
        _extract(seeded, store, "legacy_finance", D1)


def test_snapshot_ingestion_and_duplicate_detection(seeded, store):
    first = _extract(seeded, store, "legacy_finance", D1)
    keys = [m["object"].replace(".parquet", ".json") for m in first]
    accepted = [snapshots.ingest_snapshot(store, k, compression=CODEC) for k in keys]
    assert {m.status for m in accepted} == {manifests.ACCEPTED}
    table = pq.read_table(io.BytesIO(store.get(accepted[0].bronze_object)))
    assert table.column("meta_snapshot_key")[0].as_py() == f"legacy_finance/{accepted[0].entity}/{D1}"

    second = _extract(seeded, store, "legacy_finance", D1)  # same data, new run
    dups = [
        snapshots.ingest_snapshot(store, m["object"].replace(".parquet", ".json"), compression=CODEC)
        for m in second
    ]
    assert {m.status for m in dups} == {manifests.DUPLICATE}


def test_tampered_landing_object_is_refused(seeded, store):
    m = _extract(seeded, store, "fund_ops", D1)[0]
    key = m["object"].replace(".parquet", ".json")
    manifest = json.loads(store.get(key))
    manifest["object_sha256"] = "0" * 64
    tampered = key.replace(".json", ".tampered.json")
    store.put(tampered, json.dumps(manifest).encode())
    with pytest.raises(ContractError, match="checksum"):
        snapshots.ingest_snapshot(store, tampered, compression=CODEC)
