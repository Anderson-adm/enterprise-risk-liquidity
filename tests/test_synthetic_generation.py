"""Regression for the single-date generation defect: ``erl generate --dates <D1>`` used to
alias the D2 fixtures (correction, duplicates, broken/truncated XML, a D2 redemption) onto D1.

Scenario roles are positional and generation is prefix-stable: the rows and files of a date
never depend on the dates requested after it."""

from __future__ import annotations

import json
from datetime import date

import duckdb
import pytest

from ingestion import parsers
from pipeline import cli, steps
from sources import seed, synthetic
from tests.conftest import D1, D2, D3


def _slice(ds: synthetic.SyntheticDataset, d: date) -> dict:
    """Everything the generator attributes to day ``d`` (rows by their own date columns)."""
    risk, legacy, comp = (ds.tables[s] for s in ("portfolio_risk", "legacy_finance", "exposure_compliance"))
    return {
        "batches": [r for r in risk["processing_batch"] if r["reference_date"] == d],
        "positions": [r for r in risk["processed_position"] if r["reference_date"] == d],
        "cash": [r for r in legacy["tb_saldo_caixa"] if r["dt_ref"] == d],
        "redemptions_requested": [r for r in legacy["tb_resgate"] if r["dt_pedido"] == d],
        "stock": [r for r in comp["stock_position"] if r["position_date"] == d],
        "files": sorted((s, n, b) for s, delivered, n, b in ds.files if delivered == d),
    }


def test_single_date_receives_no_event_of_another_date():
    single = synthetic.generate((D1,))
    day = _slice(single, D1)
    assert [r for r in day["positions"] if r["processing_version"] > 1] == []
    keys = [(r["portfolio_code"], r["instrument_code"], r["processing_version"]) for r in day["positions"]]
    assert len(keys) == len(set(keys)), "no duplicate position on a single clean date"
    assert {r["id_resgate"] for r in single.tables["legacy_finance"]["tb_resgate"]} == {1, 2, 4}
    assert all(r["dt_pedido"] <= D1 for r in single.tables["legacy_finance"]["tb_resgate"])
    names = [n for _, n, _ in day["files"]]
    assert not any("resend" in n or "retransmission" in n for n in names)
    for source, name, body in day["files"]:
        parsed = parsers.PARSERS[source](body)  # every delivery is valid (no broken XML)
        assert parsed.reference_date == D1, name
        if source == "market_data":
            assert len(parsed.rows) == len(set(parsed.rows)) == len(synthetic.INSTRUMENTS)
    assert set(single.tables) == {"fund_ops", "portfolio_risk", "legacy_finance", "exposure_compliance"}


@pytest.mark.parametrize("k", [1, 2, 3])
def test_generation_is_prefix_stable(k):
    full = synthetic.generate(synthetic.DEFAULT_DATES)
    prefix = synthetic.generate(synthetic.DEFAULT_DATES[:k])
    for d in synthetic.DEFAULT_DATES[:k]:
        assert _slice(prefix, d) == _slice(full, d), d


def test_scenario_roles_follow_the_requested_dates():
    later_alone = _slice(synthetic.generate((D2,)), D2)
    assert [r for r in later_alone["positions"] if r["processing_version"] > 1] == []
    two = _slice(synthetic.generate((D1, D2)), D2)
    assert [r["processing_version"] for r in two["positions"] if r["processing_version"] > 1] == [2]
    assert any("resend" in n for _, n, _ in two["files"])
    assert not any("retransmission" in n for _, n, _ in two["files"])


@pytest.mark.parametrize("dates", [(), (D2, D1), (D1, D1), (D1, D3, D2)])
def test_dates_must_be_strictly_increasing(dates):
    with pytest.raises(ValueError):
        synthetic.generate(dates)


def test_cli_refuses_unordered_dates(settings, capsys):
    assert cli.main(["generate", "--dates", D2.isoformat(), D1.isoformat()]) == 2
    assert "strictly increasing" in capsys.readouterr().err
    assert not settings.inbox_dir.exists(), "nothing generated before validation"


@pytest.mark.dbt
def test_single_date_world_publishes_the_same_d1_as_the_full_world(tmp_path, monkeypatch):
    """The canonical D1 slice used for the AWS attempt can now be produced by the CLI itself."""
    from tests.conftest import use_backend

    def publish_d1(name: str, dates: tuple[date, ...]) -> tuple[dict, list]:
        settings = use_backend(monkeypatch, tmp_path / name, "sqlite")
        seed.generate_all(settings, dates)
        state = steps.run_all(D1, settings=settings)
        assert state["status"] == "SUCCEEDED", state
        with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
            gold = con.execute(
                "SELECT * EXCLUDE (pipeline_run_id, lineage_bronze_runs) FROM gold.gold_liquidity_summary "
                "WHERE reference_date = ? ORDER BY portfolio_id",
                [D1],
            ).fetchall()
        report = json.loads((settings.runs_dir / state["run_id"] / "run_report.json").read_text())
        return report, gold

    single_report, single_gold = publish_d1("single", (D1,))
    full_report, full_gold = publish_d1("full", synthetic.DEFAULT_DATES)
    assert single_gold == full_gold and len(single_gold) == 8
    assert single_report["ingestion"]["files_rejected"] == []
    assert single_report["quality_gate"]["status"] == "PASSED"
    assert single_report["records_read"] == full_report["records_read"]
