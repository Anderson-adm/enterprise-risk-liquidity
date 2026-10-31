"""Bytes -> parsed delivery. Format-specific implementation owned by this unit."""

from __future__ import annotations

import csv
import io

from erl_protocol.contracts import bronze_entity
from erl_protocol.file_contract import ParsedDelivery, RejectedDelivery, parse_iso_date


def parse(data: bytes) -> ParsedDelivery:
    entity = bronze_entity("market_data", "daily_price")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise RejectedDelivery("INVALID_ENCODING", str(exc)) from exc
    reader = csv.reader(io.StringIO(text), delimiter=";")
    try:
        header = next(reader)
    except StopIteration as exc:
        raise RejectedDelivery("EMPTY_FILE", "no header line") from exc
    if tuple(h.strip() for h in header) != entity.columns:
        raise RejectedDelivery("SCHEMA_MISMATCH", f"header {header} != contract {list(entity.columns)}")
    rows: list[tuple[str | None, ...]] = []
    for line_no, raw in enumerate(reader, start=2):
        if not raw or all(not v.strip() for v in raw):
            continue
        if len(raw) != len(entity.columns):
            raise RejectedDelivery("SCHEMA_MISMATCH", f"line {line_no}: {len(raw)} fields")
        rows.append(tuple(v.strip() or None for v in raw))
    if not rows:
        raise RejectedDelivery("EMPTY_FILE", "header only")
    dates = {r[0] for r in rows}
    if len(dates) != 1:
        raise RejectedDelivery("MIXED_REFERENCE_DATES", f"{sorted(d or '' for d in dates)}")
    ref = parse_iso_date(dates.pop())
    return ParsedDelivery(entity, ref, rows, f"market_data/daily_price/{ref.isoformat()}")
