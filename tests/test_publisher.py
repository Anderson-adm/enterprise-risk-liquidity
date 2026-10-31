"""Consumer publication: contract checks, idempotency, atomic replacement, preservation of the
last valid version. Runs on the SQLite stand-in and, with ``-m integration`` and
ERL_IT_CONTAINERS=1, on the real SQL Server consumer database (isolated ``it_`` database)."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from connect.databases import engine_for
from erl_protocol.errors import PublicationError
from load import package, publisher
from sources import seed
from tests.conftest import D1, D2, product_rows, reset_consumer, run_id, write_package


@pytest.fixture
def engine(db_settings):
    seed.ensure_databases(db_settings)
    eng = engine_for(db_settings.consumer_url())
    reset_consumer(eng)
    yield eng
    eng.dispose()


def _package(store, ref, portfolios, value="100.00", gate="PASSED"):
    run = run_id()
    tables = {
        "liquidity_summary": product_rows("liquidity_summary", ref, portfolios, value),
        "exposure_compliance": product_rows("exposure_compliance", ref, portfolios, value),
    }
    return package.load_package(store, write_package(store, ref, run, tables, gate))


def _rows(engine, product="liquidity_summary"):
    target, _ = publisher.PRODUCT_TABLES[product]
    with publisher._connect(engine) as conn:
        return [
            dict(r)
            for r in conn.execute(
                select(target).order_by(target.c.reference_date, target.c.portfolio_id)
            ).mappings()
        ]


def _log(engine):
    return publisher.active_publications(engine)


def test_first_publication_is_active(store, engine):
    pkg = _package(store, D1, ["PRT-0001", "PRT-0002"])
    result = publisher.publish(engine, pkg)
    assert result["publication_status"] == publisher.ACTIVE
    rows = _rows(engine)
    assert [r["portfolio_id"] for r in rows] == ["PRT-0001", "PRT-0002"]
    assert {r["publication_id"] for r in rows} == {result["publication_id"]}
    assert str(rows[0]["total_value"]) == "100.00"  # exact decimal round trip


def test_identical_content_is_not_republished(store, engine):
    first = publisher.publish(engine, _package(store, D1, ["PRT-0001"]))
    second = publisher.publish(engine, _package(store, D1, ["PRT-0001"]))  # new run, same content
    assert second["publication_status"] == publisher.SKIPPED
    assert second["active_publication_id"] == first["publication_id"]
    assert {r["publication_id"] for r in _rows(engine)} == {first["publication_id"]}


def test_new_content_supersedes_previous_version_for_the_same_date_only(store, engine):
    d1 = publisher.publish(engine, _package(store, D1, ["PRT-0001"]))
    old = publisher.publish(engine, _package(store, D2, ["PRT-0001", "PRT-0002"]))
    new = publisher.publish(engine, _package(store, D2, ["PRT-0001"], value="250.00"))
    assert new["superseded"] == old["publication_id"]
    rows = _rows(engine)
    assert [(r["reference_date"], r["portfolio_id"], str(r["total_value"])) for r in rows] == [
        (D1, "PRT-0001", "100.00"),
        (D2, "PRT-0001", "250.00"),
    ]
    statuses = {r["publication_id"]: r["status"] for r in _log(engine)}
    assert statuses == {
        d1["publication_id"]: "ACTIVE",
        old["publication_id"]: "SUPERSEDED",
        new["publication_id"]: "ACTIVE",
    }


@pytest.mark.parametrize("point", ["before_replace:liquidity_summary", "after_replace:exposure_compliance"])
def test_failure_mid_publication_preserves_the_last_valid_version(store, engine, point):
    good = publisher.publish(engine, _package(store, D1, ["PRT-0001", "PRT-0002"]))
    before = (_rows(engine), _rows(engine, "exposure_compliance"))

    def fault(where):
        if where == point:
            raise RuntimeError("simulated connection loss")

    with pytest.raises(PublicationError, match="rolled back"):
        publisher.publish(engine, _package(store, D1, ["PRT-0003"], value="999.00"), _fault=fault)
    assert (_rows(engine), _rows(engine, "exposure_compliance")) == before
    log = {r["publication_id"]: r for r in _log(engine)}
    assert log[good["publication_id"]]["status"] == "ACTIVE"
    failed = [r for r in log.values() if r["status"] == "FAILED"]
    assert len(failed) == 1 and "simulated connection loss" in failed[0]["message"]
    # staging rows of the failed attempt were rolled back as well
    _, staging = publisher.PRODUCT_TABLES["liquidity_summary"]
    with publisher._connect(engine) as conn:
        assert conn.execute(select(staging)).first() is None


def test_package_without_passed_gate_is_refused(store):
    with pytest.raises(PublicationError, match="quality gate"):
        _package(store, D1, ["PRT-0001"], gate="FAILED")


def test_tampered_package_is_refused(store):
    run = run_id()
    tables = {p: product_rows(p, D1, ["PRT-0001"]) for p in ("liquidity_summary", "exposure_compliance")}
    key = write_package(store, D1, run, tables)
    obj = key.replace("manifest.json", "liquidity_summary.parquet")
    (store.root / obj).write_bytes(b"corrupted")
    with pytest.raises(PublicationError, match="checksum"):
        package.load_package(store, key)


def test_contract_violations_are_refused(store):
    import pyarrow as pa

    dup = product_rows("liquidity_summary", D1, ["PRT-0001", "PRT-0001"])
    with pytest.raises(PublicationError, match="duplicate keys"):
        package.validate_product("liquidity_summary", dup, D1)
    wrong_date = product_rows("liquidity_summary", D2, ["PRT-0001"])
    with pytest.raises(PublicationError, match="outside reference date"):
        package.validate_product("liquidity_summary", wrong_date, D1)
    t = product_rows("liquidity_summary", D1, ["PRT-0001"])
    as_float = t.set_column(
        t.schema.get_field_index("total_value"), "total_value", pa.array([100.0], pa.float64())
    )
    with pytest.raises(PublicationError, match="violates decimal"):
        package.validate_product("liquidity_summary", as_float, D1)


def test_missing_manifest_means_no_package(store):
    with pytest.raises(PublicationError, match="cannot read package manifest"):
        package.load_package(store, "publish/reference_date=2026-10-26/none/manifest.json")


def test_content_hash_ignores_row_order(store):
    a = product_rows("liquidity_summary", D1, ["PRT-0001", "PRT-0002"])
    b = a.take([1, 0])
    assert package.content_hash({"x": a}) == package.content_hash({"x": b})


def test_database_refuses_two_active_publications_for_one_date(store, engine):
    from sqlalchemy import insert
    from sqlalchemy.exc import IntegrityError

    first = publisher.publish(engine, _package(store, D1, ["PRT-0001"]))
    row = {k: v for k, v in _log(engine)[0].items()} | {"publication_id": "concurrent-writer"}
    with pytest.raises(IntegrityError), publisher._connect(engine) as conn, conn.begin():
        conn.execute(insert(publisher.publication_log).values(**row))
    assert [r["publication_id"] for r in _log(engine) if r["status"] == "ACTIVE"] == [first["publication_id"]]
