"""Behaviour only the real engines can show: catalog types, exact DECIMAL limits, views,
equivalence with the SQLite stand-in, true concurrency and the end-to-end run on SQL Server.

Not run by default. Start the containers (docs/LOCAL_RUNTIME.md), then:
    set -a; . ./.env; set +a
    ERL_IT_CONTAINERS=1 uv run pytest -m integration
Tests use separate ``it_*`` databases on the same servers; development data is not touched.
"""

from __future__ import annotations

import io
import threading
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import CHAR, Date, DateTime, String, delete, insert, make_url, select, text

from connect.databases import engine_for
from load import package, publisher
from pipeline import config, extraction_catalog, steps
from sources import seed
from sources.schemas import SOURCE_METADATA
from storage.objects import LocalStore
from tests.conftest import (
    D1,
    D2,
    D3,
    EXTRACT,
    REQUIRES_CONTAINERS,
    product_rows,
    reset_consumer,
    run_id,
    use_backend,
    write_package,
)
from utils.contracts import consumer_contract
from utils.errors import PublicationError
from utils.sqltypes import Dec

pytestmark = [pytest.mark.dbt, *REQUIRES_CONTAINERS]

DIALECTS = {
    "fund_ops": "postgresql",
    "portfolio_risk": "postgresql",
    "legacy_finance": "mssql",
    "exposure_compliance": "mssql",
}


@pytest.fixture(scope="module")
def it(tmp_path_factory):
    with pytest.MonkeyPatch.context() as mp:
        s = use_backend(mp, tmp_path_factory.mktemp("it") / "data", "containers")
        seed.generate_all(s)
        yield s


@pytest.fixture
def consumer(it):
    eng = engine_for(it.consumer_url())
    reset_consumer(eng)
    yield eng
    eng.dispose()


def _engine(it, source):
    return engine_for(it.source_url(source))


def _max_decimal(p: int, s: int) -> Decimal:
    return Decimal(("9" * (p - s)) + ("." + "9" * s if s else ""))


# --------------------------------------------------------------------------- engines and catalog


def test_sources_are_real_postgres_and_sqlserver(it):
    for source, dialect in DIALECTS.items():
        engine = _engine(it, source)
        try:
            assert engine.dialect.name == dialect
            assert engine.url.database == f"it_{source}"
        finally:
            engine.dispose()


def _catalog(conn, table: str) -> dict[str, tuple]:
    rows = conn.execute(
        text(
            "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH "
            "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :t"
        ),
        {"t": table},
    ).all()
    return {r[0]: tuple(r[1:]) for r in rows}


def _expected(col, dialect: str):
    t = col.type
    if isinstance(t, Dec):
        return ("numeric", t.impl.precision, t.impl.scale)
    if isinstance(t, DateTime):
        return ("datetime2",) if dialect == "mssql" else ("timestamp without time zone",)
    if isinstance(t, Date):
        return ("date",)
    if isinstance(t, CHAR):
        return ("char" if dialect == "mssql" else "character", t.length)
    if isinstance(t, String):
        return ("varchar" if dialect == "mssql" else "character varying", t.length)
    return None


def _check_types(conn, dialect: str, tables) -> list[str]:
    problems = []
    for table in tables:
        catalog = _catalog(conn, table.name)
        for col in table.columns:
            exp = _expected(col, dialect)
            if exp is None:
                continue
            data_type, precision, scale, length = catalog[col.name]
            got = {
                "numeric": (data_type, precision, scale),
                "varchar": (data_type, length),
                "character varying": (data_type, length),
                "char": (data_type, length),
                "character": (data_type, length),
            }.get(data_type, (data_type,))
            if got != exp:
                problems.append(f"{table.name}.{col.name}: {got} != {exp}")
    return problems


def test_source_column_types_are_exact_in_the_catalog(it):
    problems = []
    for source, dialect in DIALECTS.items():
        engine = _engine(it, source)
        try:
            with engine.connect() as conn:
                problems += _check_types(conn, dialect, SOURCE_METADATA[source][0].sorted_tables)
        finally:
            engine.dispose()
    assert problems == []


def test_consumer_column_types_follow_the_contract(it, consumer):
    publisher.ensure_schema(consumer)
    with consumer.connect() as conn:
        tables = [t for pair in publisher.PRODUCT_TABLES.values() for t in pair]
        assert _check_types(conn, "mssql", [*tables, publisher.publication_log]) == []


def test_legacy_sql_server_exposes_views_and_extraction_reads_them(it):
    engine = _engine(it, "legacy_finance")
    try:
        with engine.connect() as conn:
            views = set(conn.execute(text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS")).scalars())
            via_view = conn.execute(
                text("SELECT count(*) FROM vw_saldo_caixa WHERE dt_ref = :d"), {"d": D1}
            ).scalar_one()
        manifests = extraction_catalog.extract_source(
            engine, LocalStore(it.data_dir / "lake-views"), "legacy_finance", D1, run_id(), **EXTRACT
        )
    finally:
        engine.dispose()
    assert {"vw_saldo_caixa", "vw_resgate_pendente"} <= views
    cash = next(m for m in manifests if m["entity"] == "cash_balance")
    assert "FROM vw_saldo_caixa" in cash["query"]
    assert cash["rows"] == via_view > 0


# --------------------------------------------------------------------------- equivalence with SQLite


def test_extraction_is_identical_to_the_sqlite_stand_in(it, tmp_path, monkeypatch):
    """Same synthetic data, same extraction code: every entity must produce the same row
    count and the same order-sensitive content hash on SQLite and on PostgreSQL/SQL Server."""
    real = {}
    for source in config.DB_SOURCES:
        engine = _engine(it, source)
        try:
            for ref in (D1, D2, D3):
                for m in extraction_catalog.extract_source(
                    engine, LocalStore(tmp_path / "real"), source, ref, run_id(), **EXTRACT
                ):
                    real[(source, m["entity"], ref)] = (m["rows"], m["content_sha256"])
        finally:
            engine.dispose()

    lite_settings = use_backend(monkeypatch, tmp_path / "lite", "sqlite")
    seed.generate_all(lite_settings)
    lite = {}
    for source in config.DB_SOURCES:
        engine = engine_for(lite_settings.source_url(source))
        try:
            for ref in (D1, D2, D3):
                for m in extraction_catalog.extract_source(
                    engine, LocalStore(tmp_path / "lite"), source, ref, run_id(), **EXTRACT
                ):
                    lite[(source, m["entity"], ref)] = (m["rows"], m["content_sha256"])
        finally:
            engine.dispose()

    assert real.keys() == lite.keys()
    assert {k: v for k, v in real.items() if lite[k] != v} == {}


def test_source_decimals_keep_full_precision_through_extraction(it, tmp_path):
    """Largest and smallest values of NUMERIC(24,6)/NUMERIC(20,2) survive extraction exactly."""
    meta, _ = SOURCE_METADATA["portfolio_risk"]
    pos = meta.tables["processed_position"]
    probe = date(2026, 10, 30)
    big_qty, big_mv = _max_decimal(24, 6), _max_decimal(20, 2)
    rows = [
        {"quantity": big_qty, "market_value": big_mv},
        {"quantity": Decimal("0.000001"), "market_value": Decimal("0.01")},
        {"quantity": -big_qty, "market_value": -big_mv},
    ]
    engine = _engine(it, "portfolio_risk")
    try:
        with engine.begin() as conn:
            template = dict(conn.execute(select(pos).limit(1)).mappings().one())
            next_id = conn.execute(text("SELECT max(position_id) FROM processed_position")).scalar_one()
            conn.execute(
                insert(pos),
                [
                    template | r | {"position_id": next_id + i + 1, "reference_date": probe}
                    for i, r in enumerate(rows)
                ],
            )
        try:
            m = extraction_catalog.extract_entity(
                engine,
                LocalStore(tmp_path),
                "portfolio_risk",
                "processed_position",
                probe,
                run_id(),
                **EXTRACT,
            )
            table = pq.read_table(io.BytesIO(LocalStore(tmp_path).get(m["object"])))
        finally:
            with engine.begin() as conn:
                conn.execute(delete(pos).where(pos.c.reference_date == probe))
    finally:
        engine.dispose()
    assert table.column("quantity").to_pylist() == [format(r["quantity"], "f") for r in rows]
    assert table.column("market_value").to_pylist() == [format(r["market_value"], "f") for r in rows]


def test_consumer_decimals_keep_full_precision(it, consumer):
    store = LocalStore(it.data_dir / "lake-dec")
    tables = {}
    for product, spec in consumer_contract()["products"].items():
        t = product_rows(product, D1, ["PRT-0001"])
        for col in spec["columns"]:
            if col["type"].startswith("decimal"):
                p, s = (int(x) for x in col["type"][8:-1].split(","))
                idx = t.schema.get_field_index(col["name"])
                t = t.set_column(
                    idx, t.schema.field(idx), pa.array([_max_decimal(p, s)], t.schema.field(idx).type)
                )
        tables[product] = t
    pkg = package.load_package(store, write_package(store, D1, run_id(), tables))
    publisher.publish(consumer, pkg)
    for product, (target, _) in publisher.PRODUCT_TABLES.items():
        with consumer.connect() as conn:
            row = dict(conn.execute(select(target)).mappings().one())
        expected = tables[product].to_pylist()[0]
        assert {k: row[k] for k in expected} == expected


# --------------------------------------------------------------------------- concurrency


def _package(store, ref, value):
    tables = {
        p: product_rows(p, ref, ["PRT-0001", "PRT-0002"], value)
        for p in ("liquidity_summary", "exposure_compliance")
    }
    return package.load_package(store, write_package(store, ref, run_id(), tables))


def _race(engine, packages) -> list:
    barrier = threading.Barrier(len(packages))
    outcomes: list = [None] * len(packages)

    def worker(i, pkg):
        barrier.wait()
        try:
            outcomes[i] = publisher.publish(engine, pkg)["publication_status"]
        except PublicationError as exc:
            outcomes[i] = f"error: {exc}"

    threads = [threading.Thread(target=worker, args=(i, p)) for i, p in enumerate(packages)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    return outcomes


def _assert_single_consistent_version(engine, ref):
    log = [r for r in publisher.active_publications(engine) if r["reference_date"] == ref]
    active = [r for r in log if r["status"] == publisher.ACTIVE]
    assert len(active) == 1, log
    with publisher._connect(engine) as conn:
        for target, staging in publisher.PRODUCT_TABLES.values():
            ids = set(
                conn.execute(select(target.c.publication_id).where(target.c.reference_date == ref)).scalars()
            )
            assert ids == {active[0]["publication_id"]}
            assert conn.execute(select(staging)).first() is None
    return log


def test_concurrent_publications_are_serialised_and_leave_one_consistent_version(it, consumer):
    store = LocalStore(it.data_dir / "lake-race")
    publisher.ensure_schema(consumer)  # DDL outside the race

    # 1) four first publications of the same date at the same instant (no row to lock yet)
    first = _race(consumer, [_package(store, D2, f"{100 + i}.00") for i in range(4)])
    assert first == [publisher.ACTIVE] * 4, first
    log = _assert_single_consistent_version(consumer, D2)
    assert sorted(r["status"] for r in log) == [publisher.ACTIVE] + [publisher.SUPERSEDED] * 3

    # 2) replacements of two dates racing against existing ACTIVE versions
    second = _race(consumer, [_package(store, d, f"{200 + i}.00") for i, d in enumerate([D1, D2, D1, D2])])
    assert second == [publisher.ACTIVE] * 4, second
    _assert_single_consistent_version(consumer, D1)
    log = _assert_single_consistent_version(consumer, D2)
    assert not [r for r in log if r["status"] == publisher.FAILED]


def test_publication_lock_also_serialises_a_postgresql_consumer(it, monkeypatch):
    """The consumer is SQL Server; the PostgreSQL branch of the lock is exercised here so
    that no publisher code path is left untested."""
    pg_url = make_url(it.source_url("fund_ops")).set(database="it_consumer_pg")
    monkeypatch.setenv("ERL_CONSUMER_URL", pg_url.render_as_string(hide_password=False))
    seed.ensure_databases(config.load())
    engine = engine_for(pg_url.render_as_string(hide_password=False))
    try:
        reset_consumer(engine)
        publisher.ensure_schema(engine)
        store = LocalStore(it.data_dir / "lake-race-pg")
        outcomes = _race(engine, [_package(store, D3, f"{300 + i}.00") for i in range(4)])
        assert outcomes == [publisher.ACTIVE] * 4, outcomes
        log = _assert_single_consistent_version(engine, D3)
        assert sorted(r["status"] for r in log) == [publisher.ACTIVE] + [publisher.SUPERSEDED] * 3
    finally:
        engine.dispose()


def test_concurrent_retries_of_the_same_package_have_unique_attempt_ids(it, consumer):
    from sqlalchemy import event

    publisher.ensure_schema(consumer)
    pkg = _package(LocalStore(it.data_dir / "lake-same-package"), D1, "100.00")
    barrier = threading.Barrier(4)

    def synchronise_old_attempt_count(conn, cursor, statement, parameters, context, executemany):
        if "count(*)" in statement.lower() and "pipeline_run_id" in statement.lower():
            barrier.wait(timeout=30)

    event.listen(consumer, "after_cursor_execute", synchronise_old_attempt_count)
    try:
        outcomes = _race(consumer, [pkg] * 4)
    finally:
        event.remove(consumer, "after_cursor_execute", synchronise_old_attempt_count)
    assert sorted(outcomes) == [publisher.ACTIVE] + [publisher.SKIPPED] * 3
    log = _assert_single_consistent_version(consumer, D1)
    assert len(log) == 4 and len({r["publication_id"] for r in log}) == 4


# --------------------------------------------------------------------------- end to end


def test_pipeline_publishes_to_sql_server_and_is_idempotent(it, consumer):
    first = steps.run_all(D1, settings=it)
    assert first["status"] == "SUCCEEDED", first
    second = steps.run_all(D1, settings=it)
    assert second["steps"]["publish"]["publication_status"] == "SKIPPED_IDENTICAL"
    with consumer.connect() as conn:
        n = conn.execute(
            text("SELECT count(*) FROM rpt.liquidity_summary WHERE reference_date = :d"), {"d": D1}
        ).scalar()
    assert n == 8
