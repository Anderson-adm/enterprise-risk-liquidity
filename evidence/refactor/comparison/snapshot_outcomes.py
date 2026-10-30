"""Normalise one collect_evidence.py run into run-id/timestamp-free JSON for exact comparison.

usage: python snapshot_outcomes.py <evidence_dir> <consumer_url> <out.json>
"""

import json
import re
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

RUN = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{6}")
DROP = {"run_id", "report", "elapsed_time", "step_elapsed_seconds", "total_elapsed_seconds",
        "dbt_invocation_id", "publication_id", "content_sha256", "generated_at", "elapsed_seconds"}


def scrub(obj):
    if isinstance(obj, dict):
        return {k: scrub(v) for k, v in sorted(obj.items()) if k not in DROP}
    if isinstance(obj, list):
        return [scrub(v) for v in obj]
    if isinstance(obj, str):
        return re.sub(r"-[0-9a-f]{32}\b", "-<uuid>", RUN.sub("<run>", obj))
    return obj


def main(evidence: str, url: str, out: str) -> None:
    ev = Path(evidence)
    result = {"summary": scrub(json.loads((ev / "summary.json").read_text())["scenarios"]), "runs": {}}
    for f in sorted((ev / "runs").glob("*.json")):
        result["runs"][f.stem] = scrub(json.loads(f.read_text()))
    engine = create_engine(url)
    rpt = "" if engine.dialect.name == "sqlite" else "rpt."
    with engine.connect() as con:
        for table in ("liquidity_summary", "exposure_compliance"):
            rows = con.execute(text(f"SELECT * FROM {rpt}{table} ORDER BY reference_date, portfolio_id"))
            keys = [k for k in rows.keys() if k not in {"publication_id", "lineage_bronze_runs"}]
            result[table] = [
                {k: (None if r[k] is None else str(r[k])) for k in keys} for r in rows.mappings()
            ]
        log = con.execute(text(
            f"SELECT reference_date, status, liquidity_rows, exposure_rows, superseded_by, message "
            f"FROM {rpt}publication_log ORDER BY reference_date, published_at")).mappings().all()
        result["publication_log"] = [
            {"reference_date": str(r["reference_date"]), "status": r["status"],
             "rows": [r["liquidity_rows"], r["exposure_rows"]],
             "superseded": r["superseded_by"] is not None,
             "message": RUN.sub("<run>", re.sub(r"-[0-9a-f]{32}", "-<uuid>", r["message"] or ""))}
            for r in log
        ]
    engine.dispose()
    Path(out).write_text(json.dumps(result, indent=1, sort_keys=True, default=str))
    print(out, "liquidity rows", len(result["liquidity_summary"]), "exposure rows",
          len(result["exposure_compliance"]), "log", len(result["publication_log"]))


if __name__ == "__main__":
    main(*sys.argv[1:])
