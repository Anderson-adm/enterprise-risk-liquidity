"""File-delivered sources (market data CSV, internal XML, external transfer-in XML).

The same functions run locally (``erl ingest-files``) and inside the AWS Lambda handlers
(``erl.aws.lambda_handlers``); only the ``ObjectStore`` differs.

Ingestion validates *structure* (header, field count, XSD, reference date). Value-level
quality (numbers, identifiers, duplicates) is handled in Silver, where rejected records are
kept with a reason code.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass
from datetime import date, datetime
from functools import cache
from importlib import resources

from lxml import etree

from erl import log
from erl.contracts import BronzeEntity, bronze_entity
from erl.errors import ContractError
from erl.ingestion import bronze
from erl.storage import ObjectStore

logger = logging.getLogger(__name__)

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
    xsd = resources.files("erl.contracts").joinpath(name).read_bytes()
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


# --------------------------------------------------------------------------- orchestration


def ingest_file(
    store: ObjectStore,
    *,
    source_system: str,
    object_name: str,
    data: bytes,
    run_id: str,
    run_reference_date: date,
    component: str,
) -> bronze.IngestionManifest:
    """Validate one delivered file and append it to Bronze. Never raises for bad *data*:
    contract violations end in quarantine with a REJECTED manifest. Infrastructure errors
    (storage unavailable) do propagate."""
    entity_name, _ = FILE_SOURCES[source_system]
    digest = bronze.sha256_hex(data)
    if completed := bronze.completed_delivery(store, source_system, entity_name, run_id, object_name, digest):
        return completed
    raw_key = bronze.write_raw(store, source_system, run_reference_date, run_id, object_name, data)

    def manifest(**kw) -> bronze.IngestionManifest:
        base = dict(
            run_id=run_id,
            source_system=source_system,
            entity=entity_name,
            component=component,
            input_object=object_name,
            input_sha256=digest,
            raw_object=raw_key,
        )
        return bronze.IngestionManifest(**(base | kw))

    try:
        parsed = PARSERS[source_system](data)
        _check_reference_date(source_system, parsed.reference_date, run_reference_date)
    except RejectedDelivery as rej:
        ref = rej.reference_date or run_reference_date
        bronze.quarantine(
            store,
            source_system=source_system,
            ref=ref,
            run_id=run_id,
            name=object_name,
            data=data,
            reason=rej.reason,
            detail=rej.detail,
        )
        m = manifest(
            reference_date=ref.isoformat(),
            status=bronze.REJECTED,
            rejection_reason=rej.reason,
            rejection_detail=rej.detail[:2000],
        )
        bronze.write_manifest(store, m)
        log.event(logger, "file_rejected", logging.WARNING, **_fields(m))
        return m

    ref = parsed.reference_date
    late = ref < run_reference_date
    dup = bronze.find_accepted_duplicate(
        store, source_system, entity_name, ref, digest, snapshot_key=parsed.snapshot_key
    )
    if dup is not None:
        m = manifest(
            reference_date=ref.isoformat(),
            status=bronze.DUPLICATE,
            records_read=len(parsed.rows),
            duplicate_of_run=dup["run_id"],
            late_arrival=late,
        )
        bronze.write_manifest(store, m)
        log.event(logger, "file_duplicate_skipped", **_fields(m))
        return m

    ingested_at = bronze.utc_now_iso()
    table = bronze.build_bronze_table(
        parsed.entity,
        parsed.rows,
        run_id=run_id,
        component=component,
        source_object=object_name,
        source_sha256=digest,
        snapshot_keys=[parsed.snapshot_key] * len(parsed.rows),
        ingested_at=ingested_at,
    )
    # one Bronze object per delivered file: suffix keeps several files of one run apart
    suffix = "__" + bronze.safe_name(object_name.rsplit("/", 1)[-1]).rsplit(".", 1)[0]
    key = bronze.bronze_key(parsed.entity, ref, run_id, suffix)
    store.put(key, bronze.parquet_bytes(table))
    m = manifest(
        reference_date=ref.isoformat(),
        status=bronze.ACCEPTED,
        records_read=len(parsed.rows),
        records_written=table.num_rows,
        bronze_object=key,
        late_arrival=late,
        snapshot_key=parsed.snapshot_key,
    )
    bronze.write_manifest(store, m)
    log.event(logger, "file_ingested", **_fields(m))
    return m


def _check_reference_date(source_system: str, delivered: date, requested: date) -> None:
    if delivered > requested:
        raise RejectedDelivery(
            "FUTURE_REFERENCE_DATE", f"delivery for {delivered} in run for {requested}", requested
        )
    if source_system == "market_data" and delivered != requested:
        # Market data is a daily snapshot; a stale file must not be used as today's prices.
        raise RejectedDelivery(
            "STALE_REFERENCE_DATE", f"market data for {delivered} delivered as {requested}", requested
        )


def _fields(m: bronze.IngestionManifest) -> dict:
    return {
        "run_id": m.run_id,
        "source_system": m.source_system,
        "entity": m.entity,
        "reference_date": m.reference_date,
        "status": m.status,
        "records_read": m.records_read,
        "records_written": m.records_written,
        "input_object": m.input_object,
        "late_arrival": m.late_arrival,
        "rejection_reason": m.rejection_reason,
    }
