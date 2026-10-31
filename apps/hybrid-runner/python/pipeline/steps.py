"""Pipeline steps: they compose the generic packages; Airflow only calls them through the CLI.

Each step is a plain function ``step(settings, ref, run_id) -> dict`` that reads its inputs
from the object store / run state and records its outcome in ``runs/<run_id>/state.json``.
``erl run`` executes them in order; the Airflow DAG calls the same steps one task each.

Steps: validate -> extract -> collect -> ingest -> transform -> gate -> load -> publish -> report
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

from erl_protocol import layout, manifests, structured_log
from erl_protocol import publication as package_manifest
from erl_protocol.contracts import parquet_compression
from erl_protocol.errors import (
    ConfigurationError,
    ContractError,
    ErlError,
    QualityGateError,
    SourceUnavailableError,
)
from erl_protocol.run_ids import new_run_id, validate_run_id
from ingestion import files, snapshots
from load import package, publisher
from pipeline import config, extraction_catalog
from pipeline.config import DB_SOURCES, FILE_SOURCES, Settings
from pipeline.state import check_context, load_state, run_context, save_step
from utils.sql_files import read_sql

logger = logging.getLogger(__name__)

REJECTED_SUMMARY_QUERY = "extract/extract_query_silver_rejected_records_summary.sql"
REQUIRED_FILE_SOURCES = {"market_data"}  # without prices neither product can be computed


def run_step(
    name: str, fn: Callable[[Settings, date, str], dict], settings: Settings, ref: date, run_id: str
) -> dict:
    validate_run_id(run_id)
    state = load_state(settings, run_id)
    if state.get("reference_date", ref.isoformat()) != ref.isoformat():
        raise ConfigurationError(f"run {run_id} already belongs to {state['reference_date']}")
    check_context(state, run_context(settings), run_id)
    previous = state["steps"].get(name, {})
    if name != "report" and previous.get("status") == "SUCCEEDED":
        # A successful step is a committed checkpoint. Retrying it must not append to
        # the same immutable keys again; corrected data belongs to a NEW pipeline run.
        return previous
    started = time.monotonic()
    structured_log.event(logger, "step_started", run_id=run_id, step=name, reference_date=ref.isoformat())
    try:
        if settings.mode == "aws" and name not in {"validate", "report"}:
            from connect.aws import assert_expected_account

            assert_expected_account(settings.aws_session(), settings.aws_account_id)
        result = fn(settings, ref, run_id)
    except Exception as exc:
        category = exc.category if isinstance(exc, ErlError) else "unexpected"
        outcome = {
            **(exc.details if isinstance(exc, ErlError) else {}),
            "status": "FAILED",
            "error_type": category,
            "error": f"{type(exc).__name__}: {exc}"[:2000],
            "elapsed_time": round(time.monotonic() - started, 3),
        }
        save_step(settings, run_id, ref, name, outcome)
        structured_log.event(
            logger,
            "step_failed",
            logging.ERROR,
            run_id=run_id,
            step=name,
            reference_date=ref.isoformat(),
            **{k: v for k, v in outcome.items() if k != "status"},
        )
        raise
    outcome = {"status": "SUCCEEDED", "elapsed_time": round(time.monotonic() - started, 3), **result}
    save_step(settings, run_id, ref, name, outcome)
    structured_log.event(
        logger,
        "step_succeeded",
        run_id=run_id,
        step=name,
        reference_date=ref.isoformat(),
        elapsed_time=outcome["elapsed_time"],
    )
    return outcome


# ------------------------------------------------------------------------- steps


def parse_reference_date(text: str) -> date:
    try:
        ref = date.fromisoformat(text)
    except ValueError as exc:
        raise ConfigurationError(f"reference date must be YYYY-MM-DD, got {text!r}") from exc
    if ref > datetime.now(UTC).date():
        raise ConfigurationError(f"reference date {ref} is in the future")
    return ref


def step_validate(settings: Settings, ref: date, run_id: str) -> dict:
    validate_run_id(run_id)
    if settings.mode == "aws":
        from connect.aws import assert_expected_account

        identity = assert_expected_account(settings.aws_session(), settings.aws_account_id)
        return {"aws_account": identity["Account"], "aws_region": settings.aws_region}
    return {"source_backend": settings.source_backend}


def step_extract(settings: Settings, ref: date, run_id: str) -> dict:
    store = settings.store()
    out = {}
    for source in DB_SOURCES:
        engine = settings.source_engine(source)
        try:
            landed = extraction_catalog.extract_source(
                engine,
                store,
                source,
                ref,
                run_id,
                compression=parquet_compression(),
                batch_rows=settings.extract_batch_rows,
            )
        finally:
            engine.dispose()
        out[source] = {m["entity"]: m["rows"] for m in landed}
    return {"records_read": out}


def step_collect(settings: Settings, ref: date, run_id: str) -> dict:
    """Move the day's file deliveries from the inbox into the landing zone (run-scoped keys)."""
    store = settings.store()
    collected: dict[str, list[str]] = {}
    for source in FILE_SOURCES:
        folder = settings.inbox_dir / source / ref.isoformat()
        names = sorted(p.name for p in folder.glob("*") if p.is_file()) if folder.exists() else []
        if not names and source in REQUIRED_FILE_SOURCES:
            raise SourceUnavailableError(f"{source}: no delivery for {ref} in {folder}")
        for name in names:
            key = layout.landing_file_key(source, ref, run_id, name)
            data = (folder / name).read_bytes()
            if store.exists(key):
                if store.get(key) != data:
                    raise ContractError(f"delivery changed within immutable run {run_id}; use a new run id")
            else:
                store.put(key, data)
        collected[source] = names
    return {"files": collected}


def step_ingest(settings: Settings, ref: date, run_id: str) -> dict:
    store = settings.store()
    landing_db = [
        k
        for source in DB_SOURCES
        for k in store.list(f"landing/db/{source}/")
        if k.endswith(f"/reference_date={ref.isoformat()}/{run_id}.json")
    ]
    landing_files = store.list("landing/files/")
    landing_files = [k for k in landing_files if f"/delivery_date={ref.isoformat()}/{run_id}/" in k]
    if settings.mode == "aws":
        from pipeline import aws_services

        results = aws_services.ingest_on_aws(settings, ref, run_id, landing_db, landing_files)
    else:
        codec = parquet_compression()
        results = [snapshots.ingest_snapshot(store, k, compression=codec).__dict__ for k in landing_db]
        for key in landing_files:
            source = key.split("/")[2]
            m = files.ingest_file(
                store,
                source_system=source,
                object_name=key.rsplit("/", 1)[-1],
                data=store.get(key),
                run_id=run_id,
                run_reference_date=ref,
                component="local-python",
                compression=codec,
            )
            results.append(m.__dict__)
    statuses = Counter(m["status"] for m in results)
    rejected = [
        {"source_system": m["source_system"], "object": m["input_object"], "reason": m["rejection_reason"]}
        for m in results
        if m["status"] == manifests.REJECTED
    ]
    late = [m["input_object"] for m in results if m.get("late_arrival")]
    db_failed = [m for m in results if m["source_system"] in DB_SOURCES and m["status"] == manifests.REJECTED]
    if db_failed:
        raise SourceUnavailableError(f"database snapshots rejected: {db_failed}")
    for source in REQUIRED_FILE_SOURCES:
        if not any(
            m["source_system"] == source and m["status"] in {manifests.ACCEPTED, manifests.DUPLICATE}
            for m in results
        ):
            raise ContractError(
                f"{source}: no accepted delivery for this run; refusing to reuse historical data",
                details={"files_rejected": rejected},
            )
    return {
        "manifests": len(results),
        "by_status": dict(statuses),
        "records_written": sum(m["records_written"] for m in results),
        "files_rejected": rejected,
        "late_arrivals": late,
    }


def _dbt_env(settings: Settings) -> dict[str, str]:
    """Environment of the dbt subprocess: the inherited one (credentials, PATH, DBT_* options)
    with the paths resolved in ``settings`` set explicitly. Settings is the source of truth for
    the run; stale ERL_LAKE_DIR / ERL_DUCKDB_PATH values cannot redirect dbt elsewhere."""
    env = dict(os.environ)
    env["DBT_PROFILES_DIR"] = str(settings.dbt_project_dir)
    env["ERL_LAKE_DIR"] = str(settings.lake_dir)
    env["ERL_DUCKDB_PATH"] = str(settings.warehouse_path)
    return env


def step_transform(settings: Settings, ref: date, run_id: str) -> dict:
    target = "athena" if settings.mode == "aws" else "local"
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    out_dir = settings.runs_dir / run_id / "dbt"
    results_file = out_dir / "target" / "run_results.json"
    results_file.unlink(missing_ok=True)  # a parse/crash on retry must not reuse old success
    cmd = [
        str(Path(sys.executable).with_name("dbt")),
        "build",
        "--project-dir",
        str(settings.dbt_project_dir),
        "--target",
        target,
        "--target-path",
        str(out_dir / "target"),
        "--log-path",
        str(out_dir / "logs"),
        "--vars",
        json.dumps({"reference_date": ref.isoformat(), "pipeline_run_id": run_id}),
    ]
    proc = subprocess.run(  # noqa: S603 - fixed argv, no shell; vars are validated date + run id
        cmd, cwd=settings.dbt_project_dir, env=_dbt_env(settings), capture_output=True, text=True, check=False
    )
    (out_dir / "dbt_stdout.log").parent.mkdir(parents=True, exist_ok=True)
    (out_dir / "dbt_stdout.log").write_text(proc.stdout + proc.stderr)
    if not results_file.exists():
        raise ErlError(f"dbt did not produce run_results.json (exit {proc.returncode}); see {out_dir}")
    summary = summarize_run_results(json.loads(results_file.read_text()))
    summary["dbt_exit_code"] = proc.returncode
    return summary


def summarize_run_results(results: dict) -> dict:
    by_status = Counter(r["status"] for r in results["results"])
    failures = [
        {
            "node": r["unique_id"],
            "status": r["status"],
            "failures": r.get("failures"),
            "message": (r.get("message") or "")[:300],
        }
        for r in results["results"]
        if r["status"] in {"fail", "error"}
    ]
    warnings = [r["unique_id"] for r in results["results"] if r["status"] == "warn"]
    skipped = [r["unique_id"] for r in results["results"] if r["status"] == "skipped"]
    return {
        "dbt_invocation_id": results["metadata"]["invocation_id"],
        "by_status": dict(by_status),
        "failures": failures,
        "warnings": warnings,
        "skipped": skipped,
    }


def step_gate(settings: Settings, ref: date, run_id: str) -> dict:
    transform = load_state(settings, run_id)["steps"].get("transform", {})
    if transform.get("status") != "SUCCEEDED":
        raise QualityGateError("transform step did not complete; nothing to evaluate")
    gate = evaluate_gate(transform)
    if gate["status"] != "PASSED":
        raise QualityGateError(
            f"critical quality checks failed: {gate['tests_failed']}", details={"quality_gate": gate}
        )
    return {"quality_gate": gate}


def evaluate_gate(transform: dict) -> dict:
    failed = (
        bool(transform["failures"]) or bool(transform["skipped"]) or bool(transform.get("dbt_exit_code", 0))
    )
    return {
        "status": "FAILED" if failed else "PASSED",
        "dbt_invocation_id": transform["dbt_invocation_id"],
        "tests_failed": [f["node"] for f in transform["failures"]],
        "warnings": transform["warnings"],
    }


def step_load(settings: Settings, ref: date, run_id: str) -> dict:
    state = load_state(settings, run_id)
    gate = state["steps"].get("gate", {}).get("quality_gate", {"status": "MISSING"})
    if gate.get("status") != "PASSED":
        raise QualityGateError(f"quality gate is {gate.get('status')}; Gold will not be loaded")
    committed_key = package_manifest.manifest_key(ref, run_id)
    if settings.store().exists(committed_key):
        package.load_package(settings.store(), committed_key)
        return {"package_manifest": committed_key}
    if settings.mode == "aws":
        from pipeline import aws_services

        key = aws_services.run_gold_load(settings, ref, run_id, gate)
    else:
        key = package.build_from_duckdb(
            settings.store(), settings.warehouse_path, ref, run_id, gate, compression=parquet_compression()
        )
    return {"package_manifest": key}


def step_publish(settings: Settings, ref: date, run_id: str) -> dict:
    key = load_state(settings, run_id)["steps"].get("load", {}).get("package_manifest")
    if not key:
        raise QualityGateError("no publication package for this run")
    pkg = package.load_package(settings.store(), key)
    engine = settings.consumer_engine()
    try:
        return publisher.publish(engine, pkg)
    finally:
        engine.dispose()


def step_report(settings: Settings, ref: date, run_id: str) -> dict:
    """Run report: everything an operator needs to answer what happened to this run."""
    state = load_state(settings, run_id)
    report = {
        "run_id": run_id,
        "reference_date": ref.isoformat(),
        "mode": settings.mode,
        "steps": {k: v.get("status") for k, v in state["steps"].items()},
        "records_read": state["steps"].get("extract", {}).get("records_read"),
        "ingestion": {
            k: state["steps"].get("ingest", {}).get(k)
            for k in ("by_status", "records_written", "files_rejected", "late_arrivals")
        },
        "quality_gate": state["steps"].get("gate", {}).get("quality_gate"),
        "publication": {
            k: state["steps"].get("publish", {}).get(k)
            for k in ("publication_status", "publication_id", "superseded", "rows", "content_sha256")
        },
    }
    if settings.mode == "local" and settings.warehouse_path.exists():
        import duckdb

        try:
            report["rejected_records"] = rejected_summary(settings, ref)
        except duckdb.Error as exc:
            # A failed/partial transform may never have created Silver. Supplementary
            # diagnostics must not hide the original failure or prevent its report.
            report["rejected_records_error"] = str(exc)[:500]
    path = settings.runs_dir / run_id / "run_report.json"
    path.write_text(json.dumps(report, indent=2, default=str))
    return {"report": str(path)}


def rejected_summary(settings: Settings, ref: date) -> list[dict]:
    import duckdb

    with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
        rows = con.execute(read_sql(REJECTED_SUMMARY_QUERY), {"reference_date": ref}).fetchall()
    return [{"entity": e, "source_system": s, "reason": r, "records": n} for e, s, r, n in rows]


STEPS: dict[str, Callable[[Settings, date, str], dict]] = {
    "validate": step_validate,
    "extract": step_extract,
    "collect": step_collect,
    "ingest": step_ingest,
    "transform": step_transform,
    "gate": step_gate,
    "load": step_load,
    "publish": step_publish,
    "report": step_report,
}


def run_all(ref: date, run_id: str | None = None, settings: Settings | None = None) -> dict:
    settings = settings or config.load()
    run_id = run_id or new_run_id()
    failed: Exception | None = None
    for name, fn in STEPS.items():
        if name == "report":
            break
        try:
            run_step(name, fn, settings, ref, run_id)
        except Exception as exc:  # recorded in state by run_step; stop and report
            failed = exc
            break
    run_step("report", step_report, settings, ref, run_id)  # always produce evidence
    state = load_state(settings, run_id)
    state["status"] = "FAILED" if failed else "SUCCEEDED"
    if failed:
        state["error_type"] = failed.category if isinstance(failed, ErlError) else "unexpected"
    return state
