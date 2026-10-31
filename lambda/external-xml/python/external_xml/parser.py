"""Bytes -> parsed delivery. Format-specific implementation owned by this unit."""

from __future__ import annotations

from datetime import datetime

from erl_protocol.contracts import bronze_entity
from erl_protocol.file_contract import ParsedDelivery, RejectedDelivery
from erl_protocol.xml_validation import parse_xml

EXT_NS = "urn:erl:synthetic:transfer-in:v1"


def parse(data: bytes) -> ParsedDelivery:
    entity = bronze_entity("external_xml", "transfer_in_position")
    root = parse_xml(data, "transfer_in_portfolio.xsd")
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
