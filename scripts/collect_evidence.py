#!/usr/bin/env python3
"""Run every documented scenario from a clean data directory and store the evidence.

    uv run python scripts/collect_evidence.py      # writes evidence/local/<ERL_SOURCE_BACKEND>/

ERL_SOURCE_BACKEND=sqlite (default) uses the SQLite stand-ins; =containers uses the PostgreSQL
and SQL Server containers, in separate ``ev_*`` databases (development data is not touched).
Everything written here is produced by this run (measured timings, real outputs). The
backend label in the summary states which engines were used.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = os.environ.setdefault("ERL_SOURCE_BACKEND", "sqlite")
# ERL_EVIDENCE_OUT keeps earlier evidence intact when a new comparison run is recorded.
OUT = Path(os.environ.get("ERL_EVIDENCE_OUT", ROOT / "evidence" / "local" / BACKEND)).resolve()
DATA = ROOT / "data" / f"evidence-run-{BACKEND}"

os.environ["ERL_DATA_DIR"] = str(DATA)
os.environ.setdefault("ERL_MODE", "local")

from sqlalchemy import make_url, text  # noqa: E402

from load import publisher  # noqa: E402
from pipeline import config, steps  # noqa: E402
from sources import seed  # noqa: E402
from utils import structured_log  # noqa: E402

D1, D2, D3 = date(2026, 10, 26), date(2026, 10, 27), date(2026, 10, 28)

SCENARIOS = [
    ("01_first_run_d1", D1, None),
    ("02_first_run_d2_quarantine_and_rejections", D2, None),
    ("03_first_run_d3_late_arrival_for_d2", D3, None),
    ("04_reprocess_d2_after_late_arrival", D2, None),
    ("05_rerun_d2_idempotent", D2, None),
    ("06_correction_of_published_position_d3", D3, "correct"),
    ("07_critical_quality_failure_d3", D3, "degrade"),
    ("08_source_unavailable_d1", D1, "remove"),
]

CONSUMER_QUERIES = {
    "liquidity_summary_d2": (
        "SELECT reference_date, portfolio_id, total_value, liquid_value, liquidity_ratio, "
        "pending_redemptions, redemption_coverage, lineage_sources FROM {rpt}liquidity_summary "
        "WHERE reference_date = '2026-10-27' ORDER BY portfolio_id"
    ),
    "exposure_compliance_d3": (
        "SELECT reference_date, portfolio_id, total_exposure, top_issuer_code, top_issuer_pct, "
        "concentration_limit_pct, concentration_check, reconciliation_status, quality_status "
        "FROM {rpt}exposure_compliance WHERE reference_date = '2026-10-28' ORDER BY portfolio_id"
    ),
    "publication_log": (
        "SELECT reference_date, publication_id, status, liquidity_rows, exposure_rows, "
        "superseded_by, message FROM {rpt}publication_log ORDER BY reference_date, published_at"
    ),
    "active_version_per_date": (
        "SELECT reference_date, publication_id, pipeline_run_id, content_sha256 "
        "FROM {rpt}publication_log WHERE status = 'ACTIVE' ORDER BY reference_date"
    ),
}


def _isolate_databases() -> None:
    """Point every database at an ``ev_`` copy on the same servers."""
    base = config.load()
    urls = {f"ERL_{s.upper()}_URL": base.source_url(s) for s in config.DB_SOURCES}
    urls["ERL_CONSUMER_URL"] = base.consumer_url()
    for var, url in urls.items():
        u = make_url(url)
        os.environ[var] = u.set(database=f"ev_{u.database}").render_as_string(hide_password=False)


def main() -> int:
    shutil.rmtree(DATA, ignore_errors=True)
    shutil.rmtree(OUT, ignore_errors=True)
    (OUT / "runs").mkdir(parents=True)
    if BACKEND == "containers":
        _isolate_databases()
    settings = config.load()
    structured_log.configure("WARNING", settings.data_dir / "logs" / "erl.jsonl")
    seed.generate_all(settings)
    consumer = settings.consumer_engine()
    with publisher._connect(consumer) as conn:  # clean consumer (sqlite: new file; containers: ev_ db)
        publisher.metadata.drop_all(conn)
        conn.commit()

    summary = []
    for label, ref, scenario in SCENARIOS:
        if scenario == "correct":
            seed.correct_position(settings, ref)
        elif scenario == "degrade":
            seed.degrade_market_data(settings, ref)
        elif scenario == "remove":
            seed.remove_market_data(settings, ref)
        started = time.monotonic()
        state = steps.run_all(ref, settings=settings)
        elapsed = round(time.monotonic() - started, 2)
        report = json.loads((settings.runs_dir / state["run_id"] / "run_report.json").read_text())
        report["step_elapsed_seconds"] = {k: v.get("elapsed_time") for k, v in state["steps"].items()}
        report["total_elapsed_seconds"] = elapsed
        (OUT / "runs" / f"{label}.json").write_text(json.dumps(report, indent=2, default=str))
        summary.append(
            {
                "scenario": label,
                "reference_date": ref.isoformat(),
                "run_status": state["status"],
                "error_type": state.get("error_type"),
                "publication": state["steps"].get("publish", {}).get("publication_status"),
                "elapsed_seconds": elapsed,
            }
        )
        print(json.dumps(summary[-1]))

    rpt = "" if consumer.dialect.name == "sqlite" else "rpt."
    lines = [f"-- consumer engine: {consumer.dialect.name}", ""]
    with consumer.connect() as con:
        for name, template in CONSUMER_QUERIES.items():
            sql = template.format(rpt=rpt)
            result = con.execute(text(sql))
            lines += [f"-- {name}", sql, " | ".join(result.keys())]
            lines += [" | ".join("" if v is None else str(v) for v in row) for row in result]
            lines.append("")
    consumer.dispose()
    (OUT / "consumer_queries.txt").write_text("\n".join(lines))

    meta = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "source_backend": settings.source_backend,
        "engines": {
            "sources": "SQLite stand-ins (same SQLAlchemy code as PostgreSQL/SQL Server)"
            if settings.source_backend == "sqlite"
            else "PostgreSQL + SQL Server containers",
            "transformation": "dbt-duckdb (local); Athena/Iceberg NOT exercised",
            "consumer": "SQLite stand-in" if settings.source_backend == "sqlite" else "SQL Server container",
        },
        "scenarios": summary,
    }
    (OUT / "summary.json").write_text(json.dumps(meta, indent=2))
    for f in OUT.rglob("*"):  # no machine-specific absolute paths in versioned evidence
        if f.is_file():
            f.write_text(f.read_text().replace(str(ROOT), "<repo>"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
