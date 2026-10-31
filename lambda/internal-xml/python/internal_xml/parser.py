"""Bytes -> parsed delivery. Format-specific implementation owned by this unit."""

from __future__ import annotations

from erl_protocol.contracts import bronze_entity
from erl_protocol.file_contract import ParsedDelivery, parse_iso_date
from erl_protocol.xml_validation import parse_xml


def parse(data: bytes) -> ParsedDelivery:
    entity = bronze_entity("internal_xml", "portfolio_position")
    root = parse_xml(data, "internal_portfolio_statement.xsd")
    portfolio = root.findtext("Header/PortfolioCode").strip()
    ref_text = root.findtext("Header/ReferenceDate").strip()
    generated = root.findtext("Header/GeneratedAt").strip()
    ref = parse_iso_date(ref_text)
    rows = [
        (
            portfolio,
            ref_text,
            generated,
            p.findtext("InstrumentCode").strip(),
            p.findtext("Quantity").strip(),
            p.findtext("MarketValue").strip(),
            p.findtext("Currency").strip(),
        )
        for p in root.iterfind("Positions/Position")
    ]
    return ParsedDelivery(entity, ref, rows, f"internal_xml/portfolio_position/{ref.isoformat()}/{portfolio}")
