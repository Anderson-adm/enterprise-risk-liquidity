"""Independent review regressions; SQL runs locally, AWS is never contacted."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from jinja2 import Environment
from sqlalchemy import text

from connect.databases import engine_for
from erl_protocol import manifests
from erl_protocol.errors import ConfigurationError, ErlError, PublicationError
from ingestion import files, snapshots
from load import package
from pipeline import extraction_catalog, steps
from sources import seed, synthetic
from tests.conftest import CODEC, D1, D2, EXTRACT, product_rows, run_id, write_package

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
    engine = engine_for(settings.source_url("legacy_finance"))

    def ingest():
        m = extraction_catalog.extract_entity(
            engine, store, "legacy_finance", "cash_balance", D1, run_id(), **EXTRACT
        )
        return snapshots.ingest_snapshot(store, m["object"].replace(".parquet", ".json"), compression=CODEC)

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
            assert ingest().status == manifests.ACCEPTED  # A -> B -> A is a correction
    finally:
        engine.dispose()


def test_reference_register_dedup_compares_current_delivery_across_dates(settings, store):
    seed.generate_all(settings)
    engine = engine_for(settings.source_url("fund_ops"))

    def ingest(ref):
        m = extraction_catalog.extract_entity(engine, store, "fund_ops", "fund", ref, run_id(), **EXTRACT)
        return snapshots.ingest_snapshot(store, m["object"].replace(".parquet", ".json"), compression=CODEC)

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
        assert ingest(D1).status == manifests.ACCEPTED
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
            compression=CODEC,
        )

    ingest(body)
    ingest(body.replace(b";BRL", b";USD"))
    assert ingest(body).status == manifests.ACCEPTED


def test_nonzero_dbt_exit_cannot_pass_the_gate():
    transform = {"dbt_invocation_id": "x", "dbt_exit_code": 2, "failures": [], "skipped": [], "warnings": []}
    assert steps.evaluate_gate(transform)["status"] == "FAILED"


def test_transform_retry_cannot_reuse_previous_run_results(settings, monkeypatch):
    run = run_id()
    target = settings.runs_dir / run / "dbt/target"
    target.mkdir(parents=True)
    (target / "run_results.json").write_text(
        json.dumps({"metadata": {"invocation_id": "previous"}, "results": []})
    )
    monkeypatch.setattr(
        steps.subprocess,
        "run",
        lambda *a, **kw: SimpleNamespace(stdout="parse failed", stderr="", returncode=2),
    )
    with pytest.raises(ErlError, match="run_results"):
        steps.step_transform(settings, D1, run)


def test_empty_publication_package_is_refused(store):
    tables = {p: product_rows(p, D1, []) for p in ("liquidity_summary", "exposure_compliance")}
    with pytest.raises(PublicationError, match="empty"):
        package.load_package(store, write_package(store, D1, run_id(), tables))


def test_bronze_without_commit_manifest_is_invisible_to_silver(settings, store, monkeypatch):
    seed.generate_all(settings)
    engine = engine_for(settings.source_url("fund_ops"))

    def ingest():
        m = extraction_catalog.extract_entity(engine, store, "fund_ops", "fund", D1, run_id(), **EXTRACT)
        return snapshots.ingest_snapshot(store, m["object"].replace(".parquet", ".json"), compression=CODEC)

    try:
        ingest()
        before = latest_rows(store, "fund_ops", "fund", "latest")["fund_name"].to_pylist()
        with engine.begin() as conn:
            conn.execute(text("UPDATE fund SET fund_name = 'UNCOMMITTED'"))

        def fail_manifest(*args):
            raise OSError("manifest write failed after Bronze")

        monkeypatch.setattr(manifests, "write_manifest", fail_manifest)
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
    from erl_protocol.errors import ContractError

    seed.generate_all(settings)
    first = run_id()
    steps.step_extract(settings, D1, first)
    steps.step_collect(settings, D1, first)
    steps.step_ingest(settings, D1, first)
    market = next((settings.inbox_dir / "market_data" / D1.isoformat()).iterdir())
    market.write_bytes(b"invalid;header\n")
    retry = run_id()
    steps.step_extract(settings, D1, retry)
    steps.step_collect(settings, D1, retry)
    with pytest.raises(ContractError, match="market_data"):
        steps.step_ingest(settings, D1, retry)


def test_successful_step_retry_reuses_the_committed_outcome(settings):
    seed.generate_all(settings)
    run = run_id()
    first = steps.run_step("collect", steps.step_collect, settings, D1, run)
    assert steps.run_step("collect", steps.step_collect, settings, D1, run) == first


def test_incomplete_warehouse_does_not_mask_the_original_pipeline_failure(settings):
    settings.warehouse_path.parent.mkdir(parents=True)
    duckdb.connect(str(settings.warehouse_path)).close()
    state = steps.run_all(D1, settings=settings)
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
        compression=CODEC,
    )
    first = files.ingest_file(store, **args)
    assert files.ingest_file(store, **args) == first


def test_partial_extraction_retry_reuses_each_completed_snapshot(settings, store):
    seed.generate_all(settings)
    engine = engine_for(settings.source_url("fund_ops"))
    try:
        run = run_id()
        first = extraction_catalog.extract_entity(engine, store, "fund_ops", "fund", D1, run, **EXTRACT)
        assert (
            extraction_catalog.extract_entity(engine, store, "fund_ops", "fund", D1, run, **EXTRACT) == first
        )
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


def test_terraform_checks_never_see_credentials_or_a_local_backend(tmp_path):
    """A .terraform/ left by a real deployment holds S3 backend metadata; checks run on a copy."""
    (tmp_path / "scripts").mkdir()
    (tmp_path / "evidence/terraform").mkdir(parents=True)
    dev = tmp_path / "infra/environments/dev"
    (dev / ".terraform").mkdir(parents=True)
    (dev / ".terraform/terraform.tfstate").write_text('{"backend": {"type": "s3"}}')
    (dev / "dev.auto.tfvars").write_text('allowed_account_id = "000000000000"')
    (dev / "main.tf").write_text("")
    script = tmp_path / "scripts/terraform_checks.sh"
    shutil.copyfile(ROOT / "scripts/terraform_checks.sh", script)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    seen = tmp_path / "seen.txt"
    (bin_dir / "terraform").write_text(
        "#!/bin/bash\n"
        f'echo "$AWS_PROFILE|$AWS_CONFIG_FILE|$AWS_SHARED_CREDENTIALS_FILE|$(find . -name .terraform -o '
        f'-name "*.tfvars" | wc -l)" >> {seen}\necho Success\n'
    )
    (bin_dir / "tflint").write_text('#!/bin/bash\necho "TFLint version 0.65"\n')
    (bin_dir / "uvx").write_text('#!/bin/bash\necho "Passed checks: 1"\n')
    for f in bin_dir.iterdir():
        f.chmod(0o700)
    result = subprocess.run(
        ["bash", str(script)],
        capture_output=True,
        text=True,
        env=os.environ | {"PATH": f"{bin_dir}:{os.environ['PATH']}", "AWS_PROFILE": "data-dev"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert set(seen.read_text().split()) == {"|/dev/null|/dev/null|0"}


def test_sql_publisher_imports_without_duckdb():
    """Only the local package builder needs DuckDB; the SQL publisher must not pull it in."""
    code = (
        "import sys\n"
        "sys.modules['duckdb'] = None  # any 'import duckdb' now raises ImportError\n"
        "import load.publisher, load.package\n"
        "assert load.publisher.Package is load.package.Package\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_dbt_subprocess_gets_the_settings_paths_over_inherited_ones(settings, monkeypatch):
    stale = settings.data_dir.parent / "stale"
    monkeypatch.setenv("ERL_LAKE_DIR", str(stale / "lake"))
    monkeypatch.setenv("ERL_DUCKDB_PATH", str(stale / "erl.duckdb"))
    monkeypatch.setenv("ERL_UNRELATED_OPTION", "kept")
    seen = {}

    def fake_run(cmd, *, env, **kwargs):
        seen.update(env)
        return subprocess.CompletedProcess(cmd, 2, "", "")

    monkeypatch.setattr(steps.subprocess, "run", fake_run)
    with pytest.raises(ErlError, match="run_results"):
        steps.step_transform(settings, D1, run_id())
    assert seen["ERL_LAKE_DIR"] == str(settings.lake_dir)
    assert seen["ERL_DUCKDB_PATH"] == str(settings.warehouse_path)
    assert seen["ERL_UNRELATED_OPTION"] == "kept"
    assert os.environ["ERL_LAKE_DIR"] == str(stale / "lake"), "the parent environment is not modified"


# ------------------------------------------------- adversarial review H1-H3 (2026-10-30)


@pytest.mark.parametrize(
    ("a", "b"),
    [
        (["a\x1fb", "c"], ["a", "b\x1fc"]),  # the separator inside a value moved a field boundary
        ([None], ["\x00"]),  # NULL vs the old NULL marker as text
        ([None], [""]),  # NULL vs empty string
        ([], [""]),  # no field vs one empty field
        (["a"], ["a", None]),  # arity
        (["1", "2"], ["2", "1"]),  # order is part of the identity
    ],
)
def test_row_identity_distinguishes_different_field_sequences(a, b):
    from erl_protocol.records import row_hash

    assert row_hash(a) != row_hash(b)


def test_row_identity_is_stable_and_accepts_only_canonical_text():
    from erl_protocol.records import row_hash

    assert row_hash(["PRT-0001", None, "1250.25", "é"]) == row_hash(("PRT-0001", None, "1250.25", "é"))
    with pytest.raises(TypeError):
        row_hash([Decimal("1.5")])  # values are hashed only after canonical stringification


def test_snapshots_differing_only_in_field_boundaries_are_both_accepted(store):
    from sqlalchemy import create_engine

    from erl_protocol.layout import landing_db_prefix
    from extract.landing_snapshot import extract_to_landing

    def landed(a: str, b: str) -> dict:
        engine = create_engine("sqlite://")
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE t (a TEXT, b TEXT)"))
            conn.execute(text("INSERT INTO t VALUES (:a, :b)"), {"a": a, "b": b})
        return extract_to_landing(
            engine,
            store,
            prefix=landing_db_prefix("fund_ops", "fund", D1),
            source="fund_ops",
            entity="fund",
            reference_date=D1,
            run_id=run_id(),
            sql="SELECT a, b FROM t",
            params={},
            columns=("a", "b"),
            compression=CODEC,
            batch_size=10,
        )

    first, second = landed("a\x1fb", "c"), landed("a", "b\x1fc")
    assert first["content_sha256"] != second["content_sha256"]
    assert first["content_hash_version"] == second["content_hash_version"]


def test_content_identities_of_different_versions_are_never_duplicates(store):
    from erl_protocol.records import ROW_HASH_VERSION

    prefix = "manifests/ingestion/fund_ops/fund/bronze_reference_date=2026-10-26/"
    accepted = {"status": "ACCEPTED", "input_sha256": "X", "ingested_at": "1", "run_id": "legacy"}
    store.put(prefix + "legacy__x.json", json.dumps(accepted).encode())  # written without a version
    args = (store, "fund_ops", "fund", D1, "X")
    assert manifests.find_accepted_duplicate(*args, hash_version=ROW_HASH_VERSION) is None
    store.put(
        prefix + "new__x.json",
        json.dumps(
            accepted | {"run_id": "new", "ingested_at": "2", "input_hash_version": ROW_HASH_VERSION}
        ).encode(),
    )
    assert manifests.find_accepted_duplicate(*args, hash_version=ROW_HASH_VERSION)["run_id"] == "new"


def _with_value(product: str, column: str, value):
    import pyarrow as pa

    table = product_rows(product, D1, ["PRT-0001"])
    index = table.column_names.index(column)
    return table.set_column(index, table.field(column), pa.array([value], table.field(column).type))


@pytest.mark.parametrize(
    ("value", "ok"),
    [
        ("P" * 20, True),  # exactly string(20)
        ("P" * 21, False),  # one character too long
        ("é" * 20, False),  # 20 characters, but not representable in the SQL Server VARCHAR consumer
        ("PRT-0001\U0001f600", False),  # non-BMP character
    ],
)
def test_string_columns_respect_the_declared_length_and_charset(value, ok):
    table = _with_value("exposure_compliance", "portfolio_id", value)
    if ok:
        package.validate_product("exposure_compliance", table, D1)
    else:
        with pytest.raises(PublicationError, match=r"portfolio_id.*string\(20\)"):
            package.validate_product("exposure_compliance", table, D1)


def test_null_in_a_nullable_string_column_is_still_accepted():
    package.validate_product(
        "exposure_compliance", _with_value("exposure_compliance", "top_issuer_code", None), D1
    )


def test_a_permissive_consumer_cannot_mask_a_string_contract_violation(store, tmp_path):
    from load import publisher

    engine = engine_for(f"sqlite:///{tmp_path / 'consumer.sqlite'}")
    publisher.ensure_schema(engine)
    target, _ = publisher.PRODUCT_TABLES["exposure_compliance"]
    row = _with_value("exposure_compliance", "portfolio_id", "P" * 21).to_pylist()[0]
    with publisher._connect(engine) as conn, conn.begin():  # SQLite stores 21 characters in "VARCHAR(20)"
        conn.execute(target.insert().values(**row, publication_id="probe"))
    tables = {
        "liquidity_summary": product_rows("liquidity_summary", D1, ["PRT-0001"]),
        "exposure_compliance": _with_value("exposure_compliance", "portfolio_id", "P" * 21),
    }
    with pytest.raises(PublicationError, match=r"string\(20\)"):
        package.load_package(store, write_package(store, D1, run_id(), tables))
    engine.dispose()


def test_checkpoints_are_not_reused_in_an_incompatible_context(settings):
    import dataclasses

    from pipeline.state import state_path

    seed.generate_all(settings, (D1,))
    run = run_id()
    steps.run_step("validate", steps.step_validate, settings, D1, run)
    steps.run_step("collect", steps.step_collect, settings, D1, run)
    before = state_path(settings, run).read_bytes()
    aws = dataclasses.replace(settings, mode="aws", s3_bucket="erl-dev-data-111111111111")

    def must_not_run(*_):
        raise AssertionError("an incompatible checkpoint must not lead to execution either")

    with pytest.raises(ConfigurationError, match="mode"):
        steps.run_step("collect", must_not_run, aws, D1, run)
    with pytest.raises(ConfigurationError, match="mode"):
        steps.run_step("ingest", must_not_run, aws, D1, run)  # not yet run: still the same run
    assert state_path(settings, run).read_bytes() == before, "earlier records stay untouched"


def test_compatible_resume_reuses_checkpoints_despite_operational_changes(settings):
    import dataclasses

    seed.generate_all(settings, (D1,))
    run = run_id()
    first = steps.run_step("collect", steps.step_collect, settings, D1, run)
    tuned = dataclasses.replace(settings, extract_batch_rows=7)  # does not change any result
    assert steps.run_step("collect", lambda *_: {"rerun": True}, tuned, D1, run) == first


def test_legacy_checkpoints_without_context_are_refused_explicitly(settings):
    from pipeline.state import state_path

    run = run_id()
    path = state_path(settings, run)
    path.parent.mkdir(parents=True)
    legacy = {
        "run_id": run,
        "reference_date": D1.isoformat(),
        "mode": "local",
        "steps": {"collect": {"status": "SUCCEEDED"}},
    }
    path.write_text(json.dumps(legacy))
    with pytest.raises(ConfigurationError, match="no recorded context"):
        steps.run_step("collect", lambda *_: {}, settings, D1, run)
    assert json.loads(path.read_text()) == legacy


def test_checkpoint_context_holds_no_secret(monkeypatch, tmp_path):
    from pipeline import config
    from pipeline.state import run_context

    monkeypatch.setenv("ERL_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("ERL_SOURCE_BACKEND", "containers")
    monkeypatch.setenv("ERL_PG_PASSWORD", "pg-s3cr3t-value")
    monkeypatch.setenv("ERL_MSSQL_SA_PASSWORD", "ms-s3cr3t-value")
    text_ctx = json.dumps(run_context(config.load()))
    assert "s3cr3t" not in text_ctx and "fund_ops" in text_ctx
