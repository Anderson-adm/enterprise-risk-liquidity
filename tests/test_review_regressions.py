"""Independent review regressions; SQL runs locally, AWS is never contacted."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from jinja2 import Environment
from sqlalchemy import text

from erl import pipeline
from erl.errors import ErlError, PublicationError
from erl.ingestion import bronze, database, files
from erl.publish import publisher
from erl.sources import extract, seed, synthetic
from tests.conftest import D1, D2, product_rows, run_id, write_package

ROOT = Path(__file__).resolve().parents[1]


def latest_rows(store, source, entity, scope="snapshot"):
    env = Environment(extensions=["jinja2.ext.do"])
    module = env.from_string((ROOT / "dbt/macros/platform.sql").read_text()).make_module(
        {
            "source": lambda *args: (
                f"read_json_auto('{store.root}/manifests/ingestion/**/*.json', union_by_name=true)"
            )
        }
    )
    relation = f"read_parquet('{store.root}/bronze/structured/{source}/{entity}/*/*.parquet')"
    with duckdb.connect() as con:
        return con.sql(str(module.latest_delivery(relation, scope))).to_arrow_table()


@pytest.mark.parametrize("empty", [False, True])
def test_database_correction_and_empty_snapshot_replace_previous_delivery(settings, store, empty):
    seed.generate_all(settings)
    engine = seed.engine_for(settings.source_url("legacy_finance"))

    def ingest():
        m = extract.extract_entity(engine, store, "legacy_finance", "cash_balance", D1, run_id())
        return database.ingest_snapshot(store, m["object"].replace(".parquet", ".json"))

    try:
        ingest()
        with engine.begin() as conn:
            if empty:
                conn.execute(text("DELETE FROM tb_saldo_caixa WHERE dt_ref = '2026-10-26'"))
            else:
                conn.execute(text("UPDATE tb_saldo_caixa SET vl_saldo = vl_saldo + 1"))
        ingest()
        if empty:
            assert latest_rows(store, "legacy_finance", "cash_balance").num_rows == 0
        else:
            with engine.begin() as conn:
                conn.execute(text("UPDATE tb_saldo_caixa SET vl_saldo = vl_saldo - 1"))
            assert ingest().status == bronze.ACCEPTED  # A -> B -> A is a correction
    finally:
        engine.dispose()


def test_reference_register_dedup_compares_current_delivery_across_dates(settings, store):
    seed.generate_all(settings)
    engine = seed.engine_for(settings.source_url("fund_ops"))

    def ingest(ref):
        m = extract.extract_entity(engine, store, "fund_ops", "fund", ref, run_id())
        return database.ingest_snapshot(store, m["object"].replace(".parquet", ".json"))

    try:
        with engine.connect() as conn:
            originals = dict(conn.execute(text("SELECT fund_id, fund_name FROM fund")).all())
        ingest(D1)
        with engine.begin() as conn:
            conn.execute(text("UPDATE fund SET fund_name = 'CORRECTED'"))
        ingest(D2)
        with engine.begin() as conn:
            for fund, name in originals.items():
                conn.execute(
                    text("UPDATE fund SET fund_name = :n WHERE fund_id = :f"), {"n": name, "f": fund}
                )
        assert ingest(D1).status == bronze.ACCEPTED
        assert set(latest_rows(store, "fund_ops", "fund", "latest")["fund_name"].to_pylist()) == set(
            originals.values()
        )
    finally:
        engine.dispose()


def test_file_reversion_is_not_an_historical_duplicate(store):
    name, body = next((n, b) for s, d, n, b in synthetic.generate().files if s == "market_data" and d == D1)

    def ingest(data):
        return files.ingest_file(
            store,
            source_system="market_data",
            object_name=name,
            data=data,
            run_id=run_id(),
            run_reference_date=D1,
            component="review",
        )

    ingest(body)
    ingest(body.replace(b";BRL", b";USD"))
    assert ingest(body).status == bronze.ACCEPTED


def test_nonzero_dbt_exit_cannot_pass_the_gate():
    transform = {"dbt_invocation_id": "x", "dbt_exit_code": 2, "failures": [], "skipped": [], "warnings": []}
    assert pipeline.evaluate_gate(transform)["status"] == "FAILED"


def test_transform_retry_cannot_reuse_previous_run_results(settings, monkeypatch):
    run = run_id()
    target = settings.runs_dir / run / "dbt/target"
    target.mkdir(parents=True)
    (target / "run_results.json").write_text(
        json.dumps({"metadata": {"invocation_id": "previous"}, "results": []})
    )
    monkeypatch.setattr(
        pipeline.subprocess,
        "run",
        lambda *a, **kw: SimpleNamespace(stdout="parse failed", stderr="", returncode=2),
    )
    with pytest.raises(ErlError, match="run_results"):
        pipeline.step_transform(settings, D1, run)


def test_empty_publication_package_is_refused(store):
    tables = {p: product_rows(p, D1, []) for p in ("liquidity_summary", "exposure_compliance")}
    with pytest.raises(PublicationError, match="empty"):
        publisher.load_package(store, write_package(store, D1, run_id(), tables))


def test_bronze_without_commit_manifest_is_invisible_to_silver(settings, store, monkeypatch):
    seed.generate_all(settings)
    engine = seed.engine_for(settings.source_url("fund_ops"))

    def ingest():
        m = extract.extract_entity(engine, store, "fund_ops", "fund", D1, run_id())
        return database.ingest_snapshot(store, m["object"].replace(".parquet", ".json"))

    try:
        ingest()
        before = latest_rows(store, "fund_ops", "fund", "latest")["fund_name"].to_pylist()
        with engine.begin() as conn:
            conn.execute(text("UPDATE fund SET fund_name = 'UNCOMMITTED'"))

        def fail_manifest(*args):
            raise OSError("manifest write failed after Bronze")

        monkeypatch.setattr(bronze, "write_manifest", fail_manifest)
        with pytest.raises(OSError, match="manifest write failed"):
            ingest()
        assert latest_rows(store, "fund_ops", "fund", "latest")["fund_name"].to_pylist() == before
    finally:
        engine.dispose()


def test_teardown_rejects_the_state_bucket_even_with_matching_project_tags():
    from scripts import teardown_aws

    s3 = SimpleNamespace(
        get_bucket_tagging=lambda **kw: {
            "TagSet": [
                {"Key": "Project", "Value": "enterprise-risk-liquidity"},
                {"Key": "ManagedBy", "Value": "terraform"},
                {"Key": "Purpose", "Value": "terraform-state"},
                {"Key": "Environment", "Value": "shared"},
            ]
        }
    )
    with pytest.raises(SystemExit, match="refusing"):
        teardown_aws.bucket_in_scope(s3, "erl-tfstate-111111111111", "111111111111", "erl")


def test_teardown_does_not_claim_success_when_s3_rejects_deletions():
    from scripts import teardown_aws

    s3 = SimpleNamespace(
        get_paginator=lambda *a: SimpleNamespace(
            paginate=lambda **kw: [{"Versions": [{"Key": "a", "VersionId": "1"}]}]
        ),
        delete_objects=lambda **kw: {"Errors": [{"Key": "a", "Code": "AccessDenied"}]},
    )
    with pytest.raises(RuntimeError, match="AccessDenied"):
        teardown_aws.empty_bucket(s3, "erl-dev-data-111111111111")


@pytest.mark.parametrize("failed_check", ["fmt", "tflint", "checkov"])
def test_terraform_checks_propagates_failures(tmp_path, failed_check):
    (tmp_path / "scripts").mkdir()
    (tmp_path / "evidence/terraform").mkdir(parents=True)
    (tmp_path / "infra").mkdir()
    script = tmp_path / "scripts/terraform_checks.sh"
    shutil.copyfile(ROOT / "scripts/terraform_checks.sh", script)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    commands = {
        "terraform": 'if [ "$1" = fmt ] && [ "$REVIEW_FAIL" = fmt ]; then exit 7; fi; echo Success',
        "tflint": 'if [ "$1" != --version ] && [ "$REVIEW_FAIL" = tflint ]; then exit 7; fi; '
        'echo "TFLint version 0.65"',
        "uvx": 'if [ "$REVIEW_FAIL" = checkov ]; then exit 7; fi; echo "Passed checks: 1"',
    }
    for name, body in commands.items():
        path = bin_dir / name
        path.write_text("#!/bin/bash\n" + body + "\n")
        path.chmod(0o700)
    result = subprocess.run(
        ["bash", str(script)],
        capture_output=True,
        text=True,
        env=os.environ | {"PATH": f"{bin_dir}:{os.environ['PATH']}", "REVIEW_FAIL": failed_check},
    )
    assert result.returncode != 0, result.stdout


def test_rejected_required_market_delivery_cannot_fall_back_to_old_prices(settings):
    from erl.errors import ContractError

    seed.generate_all(settings)
    first = run_id()
    pipeline.step_extract(settings, D1, first)
    pipeline.step_collect(settings, D1, first)
    pipeline.step_ingest(settings, D1, first)
    market = next((settings.inbox_dir / "market_data" / D1.isoformat()).iterdir())
    market.write_bytes(b"invalid;header\n")
    retry = run_id()
    pipeline.step_extract(settings, D1, retry)
    pipeline.step_collect(settings, D1, retry)
    with pytest.raises(ContractError, match="market_data"):
        pipeline.step_ingest(settings, D1, retry)


def test_successful_step_retry_reuses_the_committed_outcome(settings):
    seed.generate_all(settings)
    run = run_id()
    first = pipeline.run_step("collect", pipeline.step_collect, settings, D1, run)
    assert pipeline.run_step("collect", pipeline.step_collect, settings, D1, run) == first


def test_incomplete_warehouse_does_not_mask_the_original_pipeline_failure(settings):
    settings.warehouse_path.parent.mkdir(parents=True)
    duckdb.connect(str(settings.warehouse_path)).close()
    state = pipeline.run_all(D1, settings=settings)
    assert state["status"] == "FAILED" and state["error_type"] == "source_unavailable"
    assert state["steps"]["report"]["status"] == "SUCCEEDED"
    report = json.loads(Path(state["steps"]["report"]["report"]).read_text())
    assert "rejected_records_error" in report


def test_partial_ingest_retry_reuses_each_completed_file_delivery(store):
    name, body = next((n, b) for s, d, n, b in synthetic.generate().files if s == "market_data" and d == D1)
    args = dict(
        source_system="market_data",
        object_name=name,
        data=body,
        run_id=run_id(),
        run_reference_date=D1,
        component="review",
    )
    first = files.ingest_file(store, **args)
    assert files.ingest_file(store, **args) == first


def test_partial_extraction_retry_reuses_each_completed_snapshot(settings, store):
    seed.generate_all(settings)
    engine = seed.engine_for(settings.source_url("fund_ops"))
    try:
        run = run_id()
        first = extract.extract_entity(engine, store, "fund_ops", "fund", D1, run)
        assert extract.extract_entity(engine, store, "fund_ops", "fund", D1, run) == first
    finally:
        engine.dispose()


def test_local_financial_ratios_do_not_round_through_binary_float():
    env = Environment(extensions=["jinja2.ext.do"])
    module = env.from_string((ROOT / "dbt/macros/platform.sql").read_text()).make_module(
        {"target": SimpleNamespace(type="duckdb")}
    )
    sql = module.ratio("cast('999999999999.99' as decimal(20,2))", "cast('3.00' as decimal(20,2))")
    with duckdb.connect() as con:
        value, kind = con.execute(f"SELECT cast({sql} as decimal(18,6)), typeof({sql})").fetchone()
    assert value == Decimal("333333333333.330000")
    assert kind.startswith("DECIMAL"), kind
