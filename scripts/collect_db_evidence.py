#!/usr/bin/env python3
"""Per-engine evidence: server version, catalog column types, views and row counts of the
databases left by ``scripts/collect_evidence.py``.

    uv run python scripts/collect_db_evidence.py      # writes evidence/databases/<engine>.json

SQLite is read from data/evidence-run-sqlite/; PostgreSQL and SQL Server from the ``ev_*``
databases of the containers run (only when ERL_PG_PASSWORD / ERL_MSSQL_SA_PASSWORD are set).
Passwords are never written; URLs are recorded without credentials.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "evidence" / "databases"

from sqlalchemy import create_engine, func, inspect, select, table, text  # noqa: E402

SQLITE_DIR = ROOT / "data" / "evidence-run-sqlite"
PG = ["ev_fund_ops", "ev_portfolio_risk"]
MSSQL = ["ev_legacy_finance", "ev_exposure_compliance", "ev_consumer_reporting"]


def _describe(url: str, schemas: list[str | None]) -> dict:
    engine = create_engine(url)
    try:
        insp = inspect(engine)
        dialect = engine.dialect
        out: dict = {"url": engine.url.render_as_string(hide_password=True).replace(str(ROOT), "<repo>")}
        with engine.connect() as conn:
            for schema in schemas:
                for name in sorted(insp.get_table_names(schema=schema)):
                    cols = {
                        c["name"]: c["type"].compile(dialect=dialect) for c in insp.get_columns(name, schema)
                    }
                    rows = conn.execute(select(func.count()).select_from(table(name, schema=schema))).scalar()
                    out.setdefault("tables", {})[f"{schema + '.' if schema else ''}{name}"] = {
                        "rows": rows,
                        "columns": cols,
                    }
                for name in sorted(insp.get_view_names(schema=schema)):
                    rows = conn.execute(select(func.count()).select_from(table(name, schema=schema))).scalar()
                    out.setdefault("views", {})[name] = {"rows": rows}
        return out
    finally:
        engine.dispose()


def _write(engine_name: str, payload: dict) -> None:
    payload = {
        "engine": engine_name,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
    } | payload
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{engine_name}.json").write_text(json.dumps(payload, indent=2, default=str) + "\n")
    print(f"wrote {OUT / f'{engine_name}.json'}")


def main() -> int:
    if SQLITE_DIR.exists():
        dbs = sorted((SQLITE_DIR / "sources").glob("*.sqlite")) + sorted(
            (SQLITE_DIR / "consumer").glob("*.sqlite")
        )
        _write(
            "sqlite",
            {
                "server_version": f"SQLite {sqlite3.sqlite_version} (embedded; development stand-in)",
                "note": "SQLite has no exact DECIMAL: utils.sqltypes.Dec stores canonical text here.",
                "databases": {p.stem: _describe(f"sqlite:///{p}", [None]) for p in dbs},
            },
        )
    pg_pwd, ms_pwd = os.environ.get("ERL_PG_PASSWORD"), os.environ.get("ERL_MSSQL_SA_PASSWORD")
    if pg_pwd:
        url = f"postgresql+psycopg://erl:{pg_pwd}@localhost:5432/"
        with create_engine(url + "postgres").connect() as conn:
            version = conn.execute(text("SELECT version()")).scalar()
        _write(
            "postgresql",
            {
                "server_version": version,
                "container_image": "postgres:17-alpine",
                "databases": {db: _describe(url + db, ["public"]) for db in PG},
            },
        )
    if ms_pwd:
        url = f"mssql+pymssql://sa:{ms_pwd}@localhost:1433/"
        with create_engine(url + "master").connect() as conn:
            version = " ".join(conn.execute(text("SELECT @@VERSION")).scalar().split())
        _write(
            "sqlserver",
            {
                "server_version": version,
                "container_image": "mcr.microsoft.com/mssql/server:2022-latest",
                "databases": {
                    db: _describe(url + db, ["rpt", "stg"] if db.endswith("consumer_reporting") else ["dbo"])
                    for db in MSSQL
                },
            },
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
