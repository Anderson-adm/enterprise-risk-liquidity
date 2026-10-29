#!/usr/bin/env python3
"""Run every documented scenario from a clean data directory and store the evidence.

    uv run python scripts/collect_evidence.py            # writes evidence/local/

Everything written here is produced by this run (measured timings, real outputs). The
backend label in the summary states which engines were used (sqlite stand-ins vs containers).
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "evidence" / "local"
DATA = ROOT / "data" / "evidence-run"

os.environ["ERL_DATA_DIR"] = str(DATA)
os.environ.setdefault("ERL_MODE", "local")
os.environ.setdefault("ERL_SOURCE_BACKEND", "sqlite")
sys.path.insert(0, str(ROOT / "src"))

from erl import config, log, pipeline  # noqa: E402
from erl.sources import seed  # noqa: E402

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
        "pending_redemptions, redemption_coverage, lineage_sources FROM liquidity_summary "
        "WHERE reference_date = '2026-10-27' ORDER BY portfolio_id"
    ),
    "exposure_compliance_d3": (
        "SELECT reference_date, portfolio_id, total_exposure, top_issuer_code, top_issuer_pct, "
        "concentration_limit_pct, concentration_check, reconciliation_status, quality_status "
        "FROM exposure_compliance WHERE reference_date = '2026-10-28' ORDER BY portfolio_id"
    ),
    "publication_log": (
        "SELECT reference_date, publication_id, status, liquidity_rows, exposure_rows, "
        "superseded_by, message FROM publication_log ORDER BY reference_date, published_at"
    ),
    "active_version_per_date": (
        "SELECT reference_date, publication_id, pipeline_run_id, content_sha256 "
        "FROM publication_log WHERE status = 'ACTIVE' ORDER BY reference_date"
    ),
}


def main() -> int:
    shutil.rmtree(DATA, ignore_errors=True)
    shutil.rmtree(OUT, ignore_errors=True)
    (OUT / "runs").mkdir(parents=True)
    settings = config.load()
    log.configure("WARNING", settings.data_dir / "logs" / "erl.jsonl")
    seed.generate_all(settings)

    summary = []
    for label, ref, scenario in SCENARIOS:
        if scenario == "correct":
            seed.correct_position(settings, ref)
        elif scenario == "degrade":
            seed.degrade_market_data(settings, ref)
        elif scenario == "remove":
            seed.remove_market_data(settings, ref)
        started = time.monotonic()
        state = pipeline.run_all(ref, settings=settings)
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

    db = settings.consumer_url().removeprefix("sqlite:///")
    with sqlite3.connect(db) as con:
        lines = []
        for name, sql in CONSUMER_QUERIES.items():
            cur = con.execute(sql)
            cols = [c[0] for c in cur.description]
            lines += [f"-- {name}", sql, " | ".join(cols)]
            lines += [" | ".join("" if v is None else str(v) for v in row) for row in cur.fetchall()]
            lines.append("")
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
