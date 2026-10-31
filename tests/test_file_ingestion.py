"""Market data CSV and XML ingestion: contract checks, quarantine, duplicates, late arrivals."""

from __future__ import annotations

import io
import json

import pyarrow.parquet as pq
import pytest

from erl_protocol import manifests
from erl_protocol.checksums import sha256_hex
from ingestion import files
from sources import synthetic
from tests.conftest import CODEC, D1, D2, D3, run_id


def _ingest(store, source, name, data, ref, run=None):
    return files.ingest_file(
        store,
        source_system=source,
        object_name=name,
        data=data,
        run_id=run or run_id(),
        run_reference_date=ref,
        component="test",
        compression=CODEC,
    )


@pytest.fixture(scope="module")
def dataset():
    return synthetic.generate()


def _file(ds, source, delivery, contains=""):
    return next((n, b) for s, d, n, b in ds.files if s == source and d == delivery and contains in n)


def test_market_data_is_accepted_with_metadata(store, dataset):
    name, body = _file(dataset, "market_data", D1)
    m = _ingest(store, "market_data", name, body, D1)
    assert m.status == manifests.ACCEPTED
    assert m.records_written == len(synthetic.INSTRUMENTS)
    table = pq.read_table(io.BytesIO(store.get(m.bronze_object)))
    assert set(table.column("meta_source_sha256").to_pylist()) == {sha256_hex(body)}
    assert table.column("meta_row_number").to_pylist() == list(range(1, table.num_rows + 1))
    # values are kept exactly as delivered (decimal comma); typing happens in Silver
    assert "," in table.column("close_price")[0].as_py()
    assert store.exists(m.raw_object)


def test_redelivery_of_identical_file_is_skipped_as_duplicate(store, dataset):
    name, body = _file(dataset, "market_data", D1)
    first = _ingest(store, "market_data", name, body, D1)
    second = _ingest(store, "market_data", name, body, D1)
    assert second.status == manifests.DUPLICATE
    assert second.duplicate_of_run == first.run_id
    assert second.bronze_object is None
    assert len(store.list("bronze/structured/market_data/")) == 1


def test_stale_market_data_is_rejected_and_quarantined(store, dataset):
    name, body = _file(dataset, "market_data", D1)
    m = _ingest(store, "market_data", name, body, D2)  # D1 prices delivered as D2
    assert m.status == manifests.REJECTED
    assert m.rejection_reason == "STALE_REFERENCE_DATE"
    quarantined = store.list("quarantine/market_data/")
    assert any(k.endswith(".rejection.json") for k in quarantined)
    reason = json.loads(store.get(next(k for k in quarantined if k.endswith(".rejection.json"))))
    assert reason["reason"] == "STALE_REFERENCE_DATE" and reason["sha256"] == sha256_hex(body)


def test_csv_header_mismatch_is_a_contract_rejection(store):
    m = _ingest(store, "market_data", "bad.csv", b"date;code;price\n2026-10-26;X;1,0\n", D1)
    assert (m.status, m.rejection_reason) == (manifests.REJECTED, "SCHEMA_MISMATCH")


def test_truncated_internal_xml_is_quarantined(store, dataset):
    name, body = _file(dataset, "internal_xml", D2, "_resend")
    m = _ingest(store, "internal_xml", name, body, D2)
    assert (m.status, m.rejection_reason) == (manifests.REJECTED, "MALFORMED_XML")
    assert store.list("bronze/structured/internal_xml/") == []


def test_external_xml_failing_xsd_is_quarantined(store, dataset):
    name, body = _file(dataset, "external_xml", D2, "EXT77-0002_20261027.xml")
    m = _ingest(store, "external_xml", name, body, D2)
    assert (m.status, m.rejection_reason) == (manifests.REJECTED, "XSD_VALIDATION_FAILED")
    assert "qty" in m.rejection_detail


def test_late_external_xml_lands_in_its_own_reference_date(store, dataset):
    name, body = _file(dataset, "external_xml", D3, "_retransmission")
    m = _ingest(store, "external_xml", name, body, D3)
    assert m.status == manifests.ACCEPTED and m.late_arrival
    assert m.reference_date == D2.isoformat()
    assert "bronze_reference_date=2026-10-27" in m.bronze_object


def test_future_reference_date_is_rejected(store, dataset):
    name, body = _file(dataset, "external_xml", D3, "EXT77-0001_20261028")
    m = _ingest(store, "external_xml", name, body, D2)
    assert (m.status, m.rejection_reason) == (manifests.REJECTED, "FUTURE_REFERENCE_DATE")


def test_xml_external_entities_are_not_resolved(store, tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP-SECRET")
    xxe = (
        f'<?xml version="1.0"?><!DOCTYPE r [<!ENTITY x SYSTEM "file://{secret}">]>'
        "<PortfolioStatement version='1'><Header><PortfolioCode>&x;</PortfolioCode>"
        "<ReferenceDate>2026-10-26</ReferenceDate><GeneratedAt>2026-10-26T00:00:00</GeneratedAt></Header>"
        "<Positions><Position><InstrumentCode>A</InstrumentCode><Quantity>1</Quantity>"
        "<MarketValue>1.00</MarketValue><Currency>BRL</Currency></Position></Positions></PortfolioStatement>"
    ).encode()
    m = _ingest(store, "internal_xml", "xxe.xml", xxe, D1)
    assert (m.status, m.rejection_reason) == (manifests.REJECTED, "DTD_NOT_ALLOWED")
    assert "TOP-SECRET" not in json.dumps(m.__dict__)


def test_bronze_is_append_only(store, dataset):
    name, body = _file(dataset, "market_data", D1)
    run = run_id()
    m = _ingest(store, "market_data", name, body, D1, run)
    from erl_protocol.errors import ObjectExistsError

    with pytest.raises(ObjectExistsError):
        store.put(m.bronze_object, b"overwrite attempt")
