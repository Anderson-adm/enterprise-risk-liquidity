"""Load the synthetic dataset into the source databases and the file inbox, and apply
post-publication scenarios (corrections, missing or degraded deliveries)."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import Engine, create_engine, func, insert, select, text

from erl import log
from erl.config import DB_SOURCES, Settings
from erl.sources import synthetic
from erl.sources.schemas import SOURCE_METADATA

logger = logging.getLogger(__name__)


def engine_for(url: str) -> Engine:
    if url.startswith("sqlite:///"):
        Path(url.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
    return create_engine(url, future=True)


def reset_source(engine: Engine, source: str, tables: dict[str, list[dict]]) -> dict[str, int]:
    """Drop and recreate one source database's objects, then load rows (dev only)."""
    metadata, views = SOURCE_METADATA[source]
    with engine.begin() as conn:
        for view in views:
            conn.execute(text(f"DROP VIEW IF EXISTS {view}"))
        metadata.drop_all(conn)
        metadata.create_all(conn)
        for view, body in views.items():
            conn.execute(text(f"CREATE VIEW {view} AS {body}"))
        counts = {}
        for table in metadata.sorted_tables:
            rows = tables.get(table.name, [])
            if rows:
                conn.execute(insert(table), rows)
            counts[table.name] = len(rows)
    return counts


def write_inbox(settings: Settings, ds: synthetic.SyntheticDataset) -> int:
    for source, delivery, name, body in ds.files:
        path = settings.inbox_dir / source / delivery.isoformat() / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    return len(ds.files)


def generate_all(settings: Settings, dates: tuple[date, ...] = synthetic.DEFAULT_DATES) -> dict:
    ds = synthetic.generate(dates)
    summary: dict[str, object] = {}
    for source in DB_SOURCES:
        engine = engine_for(settings.source_url(source))
        try:
            summary[source] = reset_source(engine, source, ds.tables[source])
        finally:
            engine.dispose()
    summary["inbox_files"] = write_inbox(settings, ds)
    log.event(logger, "synthetic_data_generated", dates=[d.isoformat() for d in dates], summary=summary)
    return summary


# ------------------------------------------------------------------------- scenarios


def correct_position(
    settings: Settings,
    ref: date,
    portfolio_id: str = "PRT-0001",
    instrument: str = "SYN-EQ-003",
    new_quantity: Decimal = Decimal("12500"),
) -> dict:
    """Risk system reprocesses an already published position (new processing_version)."""
    _meta, _ = SOURCE_METADATA["portfolio_risk"]
    pos = _meta.tables["processed_position"]
    code = synthetic.risk_code(portfolio_id)
    engine = engine_for(settings.source_url("portfolio_risk"))
    try:
        with engine.begin() as conn:
            current = (
                conn.execute(
                    select(pos)
                    .where(
                        pos.c.portfolio_code == code,
                        pos.c.instrument_code == instrument,
                        pos.c.reference_date == ref,
                    )
                    .order_by(pos.c.processing_version.desc())
                )
                .mappings()
                .first()
            )
            if current is None:
                raise ValueError(f"no position {portfolio_id}/{instrument} on {ref}")
            unit = current["market_value"] / current["quantity"]
            next_id = conn.execute(select(func.max(pos.c.position_id))).scalar_one() + 1
            row = dict(current) | {
                "position_id": next_id,
                "quantity": synthetic.q6(new_quantity),
                "market_value": synthetic.q2(new_quantity * unit),
                "processing_version": current["processing_version"] + 1,
                "processing_status": "REPROCESSED",
                "processed_at": datetime.combine(ref, datetime.min.time()) + timedelta(days=1, hours=8),
            }
            conn.execute(insert(pos), [row])
    finally:
        engine.dispose()
    log.event(
        logger,
        "scenario_position_corrected",
        reference_date=ref.isoformat(),
        portfolio_id=portfolio_id,
        instrument=instrument,
        version=row["processing_version"],
    )
    return {k: str(v) for k, v in row.items()}


def remove_market_data(settings: Settings, ref: date) -> list[str]:
    """Simulate the market data provider not delivering the day's file."""
    folder = settings.inbox_dir / "market_data" / ref.isoformat()
    removed = []
    for f in sorted(folder.glob("*.csv")):
        f.unlink()
        removed.append(f.name)
    return removed


def degrade_market_data(settings: Settings, ref: date, keep: int = 3) -> str:
    """Re-deliver the day's market data with most instruments missing (critical quality case)."""
    folder = settings.inbox_dir / "market_data" / ref.isoformat()
    source = next(iter(sorted(folder.glob("market_data_*.csv"))), None)
    if source is None:
        raise FileNotFoundError(f"no market data delivery in {folder}")
    lines = source.read_text(encoding="utf-8").splitlines()
    target = folder / f"market_data_{ref:%Y%m%d}_v2.csv"
    target.write_text("\n".join(lines[: keep + 1]) + "\n", encoding="utf-8")
    source.unlink()
    return target.name
