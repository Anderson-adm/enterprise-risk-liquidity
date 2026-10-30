"""Parsing and structural validation of file deliveries (market data CSV, internal XML,
external transfer-in XML). Pure functions: bytes in, ``ParsedDelivery`` or ``RejectedDelivery``.

Ingestion validates *structure* (header, field count, XSD, reference date). Value-level
quality (numbers, identifiers, duplicates) is handled in Silver, where rejected records are
kept with a reason code.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date, datetime
from functools import cache

from lxml import etree

from utils.contracts import BronzeEntity, bronze_entity, contract_bytes
from utils.errors import ContractError

FILE_SOURCES = {
    "market_data": ("daily_price", "csv"),
    "internal_xml": ("portfolio_position", "xml"),
    "external_xml": ("transfer_in_position", "xml"),
}

EXT_NS = "urn:erl:synthetic:transfer-in:v1"


@dataclass(frozen=True)
class ParsedDelivery:
    entity: BronzeEntity
    reference_date: date
    rows: list[tuple[str | None, ...]]
    snapshot_key: str


class RejectedDelivery(ContractError):
    def __init__(self, reason: str, detail: str, reference_date: date | None = None) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail
        self.reference_date = reference_date


# --------------------------------------------------------------------------- CSV


def parse_market_data_csv(data: bytes) -> ParsedDelivery:
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
    ref = _parse_iso_date(dates.pop())
    return ParsedDelivery(entity, ref, rows, f"market_data/daily_price/{ref.isoformat()}")


# --------------------------------------------------------------------------- XML


def _secure_parser(schema: etree.XMLSchema | None = None) -> etree.XMLParser:
    # No DTD/entity resolution and no network access: XML from third parties is untrusted.
    return etree.XMLParser(
        resolve_entities=False, no_network=True, load_dtd=False, huge_tree=False, schema=schema
    )


@cache
def _schema(name: str) -> etree.XMLSchema:
    xsd = contract_bytes(name)
    return etree.XMLSchema(etree.fromstring(xsd, parser=_secure_parser()))


def _parse_xml(data: bytes, xsd_name: str) -> etree._Element:
    try:
        root = etree.fromstring(data, parser=_secure_parser())
    except etree.XMLSyntaxError as exc:
        raise RejectedDelivery("MALFORMED_XML", str(exc)) from exc
    # The contracts define no DTD; a DOCTYPE in third-party XML can only be an entity trick.
    if root.getroottree().docinfo.doctype:
        raise RejectedDelivery("DTD_NOT_ALLOWED", "documents must not declare a DOCTYPE")
    schema = _schema(xsd_name)
    try:
        valid = schema.validate(root)
    except etree.XMLSchemaValidateError as exc:
        raise RejectedDelivery("XSD_VALIDATION_FAILED", str(exc)) from exc
    if not valid:
        errors = "; ".join(f"line {e.line}: {e.message}" for e in list(schema.error_log)[:5])
        raise RejectedDelivery("XSD_VALIDATION_FAILED", errors)
    return root


def parse_internal_xml(data: bytes) -> ParsedDelivery:
    entity = bronze_entity("internal_xml", "portfolio_position")
    root = _parse_xml(data, "internal_portfolio_statement.xsd")
    portfolio = root.findtext("Header/PortfolioCode").strip()
    ref_text = root.findtext("Header/ReferenceDate").strip()
    generated = root.findtext("Header/GeneratedAt").strip()
    ref = _parse_iso_date(ref_text)
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


def parse_external_xml(data: bytes) -> ParsedDelivery:
    entity = bronze_entity("external_xml", "transfer_in_position")
    root = _parse_xml(data, "transfer_in_portfolio.xsd")
    ns = {"ext": EXT_NS}
    admin = root.get("administrator")
    fund_id = root.find("ext:Fund", ns).get("externalId")
    as_of = root.findtext("ext:AsOf", namespaces=ns).strip()
    try:
        ref = datetime.strptime(as_of, "%d/%m/%Y").date()  # noqa: DTZ007 - a calendar date, no time zone
    except ValueError as exc:
        raise RejectedDelivery("INVALID_REFERENCE_DATE", as_of) from exc
    rows = [
        (admin, fund_id, as_of, h.get("asset"), h.get("qty"), h.get("price"), h.get("ccy"))
        for h in root.iterfind("ext:Holdings/ext:Holding", ns)
    ]
    snapshot = f"external_xml/transfer_in_position/{ref.isoformat()}/{admin}:{fund_id}"
    return ParsedDelivery(entity, ref, rows, snapshot)


PARSERS = {
    "market_data": parse_market_data_csv,
    "internal_xml": parse_internal_xml,
    "external_xml": parse_external_xml,
}


def _parse_iso_date(text: str | None) -> date:
    try:
        return date.fromisoformat((text or "").strip())
    except ValueError as exc:
        raise RejectedDelivery("INVALID_REFERENCE_DATE", repr(text)) from exc
