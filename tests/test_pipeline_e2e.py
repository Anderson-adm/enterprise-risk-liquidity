"""End-to-end regression over the seven sources: generate -> extract -> Bronze -> dbt (DuckDB)
-> quality gate -> package -> publish, plus reprocessing and failure scenarios.

Marked ``dbt`` because each run invokes ``dbt build`` (about 30 s per run)."""

from __future__ import annotations

import json
from decimal import Decimal

import duckdb
import pytest

from connect.databases import engine_for
from load import publisher
from pipeline import steps
from sources import seed, synthetic
from tests.conftest import D1, D2, D3

pytestmark = pytest.mark.dbt


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """One environment shared by the ordered scenario tests below."""
    import os

    from pipeline import config

    data = tmp_path_factory.mktemp("e2e") / "data"
    env = {"ERL_DATA_DIR": str(data), "ERL_MODE": "local", "ERL_SOURCE_BACKEND": "sqlite"}
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    settings = config.load()
    seed.generate_all(settings)
    yield {"settings": settings, "runs": {}}
    for k, v in old.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def _run(world, ref, label):
    state = steps.run_all(ref, settings=world["settings"])
    world["runs"][label] = state
    return state


def _gold(world, sql, params=None):
    with duckdb.connect(str(world["settings"].warehouse_path), read_only=True) as con:
        return con.execute(sql, params or []).fetchall()


def _log(world):
    engine = engine_for(world["settings"].consumer_url())
    try:
        return publisher.active_publications(engine)
    finally:
        engine.dispose()


def test_01_first_three_days_publish(world):
    for ref in (D1, D2, D3):
        state = _run(world, ref, f"first_{ref}")
        assert state["status"] == "SUCCEEDED", json.dumps(state, indent=1, default=str)
        assert state["steps"]["publish"]["publication_status"] == "ACTIVE"
    assert [(r["reference_date"], r["status"]) for r in _log(world)] == [
        (D1, "ACTIVE"),
        (D2, "ACTIVE"),
        (D3, "ACTIVE"),
    ]


def test_02_all_seven_sources_contribute(world):
    sources = _gold(
        world,
        "SELECT DISTINCT unnest(string_split(lineage_sources, ',')) FROM gold.gold_liquidity_summary "
        "UNION SELECT DISTINCT unnest(string_split(lineage_sources, ',')) "
        "FROM gold.gold_exposure_compliance",
    )
    assert {s for (s,) in sources} == {
        "fund_ops",
        "portfolio_risk",
        "legacy_finance",
        "exposure_compliance",
        "market_data",
        "internal_xml",
        "external_xml",
    }


def test_03_d2_scenarios_are_handled(world):
    state = world["runs"][f"first_{D2}"]
    rejected_files = {f["reason"] for f in state["steps"]["ingest"]["files_rejected"]}
    assert rejected_files == {"MALFORMED_XML", "XSD_VALIDATION_FAILED"}
    assert state["steps"]["gate"]["quality_gate"]["warnings"] == [
        "test.erl.assert_active_portfolios_have_positions"
    ]
    reasons = dict(
        _gold(
            world,
            "SELECT reject_reason, count(*) FROM silver.silver_rejected_records "
            "WHERE reference_date = ? GROUP BY 1",
            [D2],
        )
    )
    assert reasons == {"DUPLICATE_RECORD": 2, "UNMAPPED_PORTFOLIO": 2}
    # correction inside the risk system: version 2 wins over version 1
    assert _gold(
        world,
        "SELECT quantity FROM silver.silver_position WHERE reference_date = ? "
        "AND portfolio_id = 'PRT-0002' AND instrument_code = 'SYN-EQ-005'",
        [D2],
    ) == [(Decimal(synthetic.CORRECTED_QTY_D2),)]
    # PRT-0008's statement was quarantined, so D2 was first published without it
    first_d2 = next(r for r in _log(world) if r["reference_date"] == D2)
    assert first_d2["liquidity_rows"] == 7


def test_04_business_results(world):
    breach = _gold(
        world,
        "SELECT portfolio_id, top_issuer_code FROM gold.gold_exposure_compliance "
        "WHERE reference_date = ? AND concentration_check = 'BREACH'",
        [D1],
    )
    assert breach == [("PRT-0004", "ISS-B")]
    mismatch = _gold(
        world,
        "SELECT portfolio_id FROM gold.gold_exposure_compliance "
        "WHERE reference_date = ? AND reconciliation_status = 'MISMATCH'",
        [D3],
    )
    assert mismatch == [("PRT-0003",)]
    ratios = _gold(
        world, "SELECT min(liquidity_ratio), max(liquidity_ratio) FROM gold.gold_liquidity_summary"
    )
    assert Decimal(0) <= ratios[0][0] <= ratios[0][1] <= Decimal(1)


def test_05_late_arrival_reprocessing_supersedes_d2(world):
    state = _run(world, D2, "reprocess_d2")
    assert state["steps"]["publish"]["publication_status"] == "ACTIVE"
    assert state["steps"]["publish"]["rows"]["liquidity_summary"] == 8
    d2 = [r for r in _log(world) if r["reference_date"] == D2]
    assert [r["status"] for r in d2] == ["SUPERSEDED", "ACTIVE"]


def test_06_rerun_is_idempotent(world):
    before = _gold(world, "SELECT count(*) FROM gold.gold_liquidity_summary")
    state = _run(world, D2, "rerun_d2")
    assert state["steps"]["publish"]["publication_status"] == "SKIPPED_IDENTICAL"
    assert state["steps"]["ingest"]["by_status"].get("ACCEPTED", 0) == 0  # every snapshot was a duplicate
    assert _gold(world, "SELECT count(*) FROM gold.gold_liquidity_summary") == before


def test_07_correction_of_published_position(world):
    seed.correct_position(world["settings"], D3)
    state = _run(world, D3, "correct_d3")
    assert state["steps"]["publish"]["publication_status"] == "ACTIVE"
    assert state["steps"]["publish"]["superseded"] is not None


def test_08_critical_quality_failure_blocks_publication_and_keeps_last_version(world):
    snapshot = (
        "SELECT 'liq', portfolio_id, pipeline_run_id FROM gold.gold_liquidity_summary "
        "WHERE reference_date = ? "
        "UNION ALL SELECT 'exp', portfolio_id, pipeline_run_id FROM gold.gold_exposure_compliance "
        "WHERE reference_date = ? ORDER BY 1, 2"
    )
    before = _gold(world, snapshot, [D3, D3])
    active_before = [r["publication_id"] for r in _log(world) if r["status"] == "ACTIVE"]
    seed.degrade_market_data(world["settings"], D3)
    state = _run(world, D3, "degraded_d3")
    assert state["status"] == "FAILED" and state["error_type"] == "quality_gate"
    assert state["steps"]["gate"]["status"] == "FAILED"
    assert "load" not in state["steps"] and "publish" not in state["steps"]
    # final Gold untouched (write-audit-publish) and the consumer still serves the last version
    assert _gold(world, snapshot, [D3, D3]) == before
    assert [r["publication_id"] for r in _log(world) if r["status"] == "ACTIVE"] == active_before


def test_09_unavailable_source_fails_before_transformation(world):
    seed.remove_market_data(world["settings"], D1)
    state = _run(world, D1, "unavailable_d1")
    assert state["status"] == "FAILED" and state["error_type"] == "source_unavailable"
    assert state["steps"]["collect"]["status"] == "FAILED"
    assert "transform" not in state["steps"]
    assert state["steps"]["report"]["status"] == "SUCCEEDED"


def test_10_removed_portfolio_disappears_from_gold_and_consumer(world):
    from sqlalchemy import text

    for source, sql in (
        (
            "portfolio_risk",
            "DELETE FROM processed_position WHERE reference_date = '2026-10-27' "
            "AND portfolio_code = 'prt0001'",
        ),
        (
            "exposure_compliance",
            "DELETE FROM stock_position WHERE position_date = '2026-10-27' AND fund_ref = 'FND001/01'",
        ),
    ):
        engine = engine_for(world["settings"].source_url(source))
        try:
            with engine.begin() as conn:
                conn.execute(text(sql))
        finally:
            engine.dispose()
    state = _run(world, D2, "removed_d2")
    assert state["status"] == "SUCCEEDED", state
    for product in ("liquidity_summary", "exposure_compliance"):
        rows = _gold(world, f"SELECT portfolio_id FROM gold.gold_{product} WHERE reference_date = ?", [D2])
        assert ("PRT-0001",) not in rows
        assert len(rows) == state["steps"]["publish"]["rows"][product] == 7
