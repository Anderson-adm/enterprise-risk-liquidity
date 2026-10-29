"""Contract consistency, quality gate evaluation, configuration and synthetic data determinism."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from erl import config, pipeline
from erl.contracts import bronze_contract, consumer_contract
from erl.errors import ConfigurationError
from erl.ingestion import files
from erl.sources import synthetic

ROOT = Path(__file__).resolve().parents[1]


def test_consumer_columns_exist_in_gold_models():
    for spec in consumer_contract()["products"].values():
        sql = (ROOT / "dbt" / "models" / "gold" / f"{spec['gold_model']}_candidate.sql").read_text()
        aliases = set(re.findall(r"\bas\s+([a-z_]+)\s*,?\s*$", sql, flags=re.M | re.I))
        missing = (
            {c["name"] for c in spec["columns"]}
            - aliases
            - {"portfolio_id", "fund_id", "lineage_sources", "lineage_bronze_runs", "position_source_system"}
        )
        assert not missing, f"{spec['gold_model']} lacks {missing}"


def test_bronze_contract_is_consistent():
    contract = bronze_contract()
    for table, spec in contract["entities"].items():
        assert table == f"{spec['source_system']}__{spec['entity']}"
        assert len(spec["columns"]) == len(set(spec["columns"]))
    assert {s["source_system"] for s in contract["entities"].values()} == set(config.ALL_SOURCES)
    for source, (entity, _) in files.FILE_SOURCES.items():
        assert f"{source}__{entity}" in contract["entities"]


def test_dbt_sources_cover_the_bronze_contract():
    sources = (ROOT / "dbt" / "models" / "staging" / "_sources.yml").read_text()
    used = set(re.findall(r"- name: (\w+__\w+)", sources))
    assert used <= set(bronze_contract()["entities"])
    unused = set(bronze_contract()["entities"]) - used
    assert unused == {"portfolio_risk__processing_batch"}  # readiness signal, consumed at extraction


def _transform(**kw):
    base = {"dbt_invocation_id": "x", "failures": [], "warnings": [], "skipped": []}
    return base | kw


def test_gate_passes_with_warnings_only():
    gate = pipeline.evaluate_gate(_transform(warnings=["test.erl.completeness"]))
    assert gate["status"] == "PASSED" and gate["warnings"] == ["test.erl.completeness"]


@pytest.mark.parametrize("kw", [{"failures": [{"node": "test.erl.x"}]}, {"skipped": ["model.erl.gold"]}])
def test_gate_fails_on_test_failure_or_skipped_nodes(kw):
    assert pipeline.evaluate_gate(_transform(**kw))["status"] == "FAILED"


def test_run_results_summary():
    results = {
        "metadata": {"invocation_id": "abc"},
        "results": [
            {"unique_id": "model.erl.a", "status": "success"},
            {"unique_id": "test.erl.b", "status": "fail", "failures": 3, "message": "Got 3 results"},
            {"unique_id": "test.erl.c", "status": "warn"},
            {"unique_id": "model.erl.d", "status": "skipped"},
        ],
    }
    s = pipeline.summarize_run_results(results)
    assert s["by_status"] == {"success": 1, "fail": 1, "warn": 1, "skipped": 1}
    assert [f["node"] for f in s["failures"]] == ["test.erl.b"]


@pytest.mark.parametrize("text", ["2026-13-01", "26/10/2026", "2999-01-01"])
def test_invalid_reference_dates_are_rejected(text):
    with pytest.raises(ConfigurationError):
        pipeline.parse_reference_date(text)


def test_container_backend_requires_password(monkeypatch):
    monkeypatch.setenv("ERL_SOURCE_BACKEND", "containers")
    monkeypatch.delenv("ERL_PG_PASSWORD", raising=False)
    monkeypatch.delenv("ERL_FUND_OPS_URL", raising=False)
    with pytest.raises(ConfigurationError, match="ERL_PG_PASSWORD"):
        config.load().source_url("fund_ops")


def test_synthetic_data_is_deterministic():
    a, b = synthetic.generate(), synthetic.generate()
    assert [(s, d, n, body) for s, d, n, body in a.files] == [(s, d, n, body) for s, d, n, body in b.files]
    assert a.tables == b.tables


def test_synthetic_codes_are_clearly_fictitious():
    assert all(code.startswith("SYN-") for code, *_ in synthetic.INSTRUMENTS)
