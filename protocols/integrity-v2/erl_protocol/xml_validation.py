"""DTD-free XSD validation used by XML producers only."""

from functools import cache

from lxml import etree

from erl_protocol.contracts import contract_bytes
from erl_protocol.file_contract import RejectedDelivery


def _secure_parser(schema: etree.XMLSchema | None = None) -> etree.XMLParser:
    # No DTD/entity resolution and no network access: XML from third parties is untrusted.
    return etree.XMLParser(
        resolve_entities=False, no_network=True, load_dtd=False, huge_tree=False, schema=schema
    )


@cache
def _schema(name: str) -> etree.XMLSchema:
    xsd = contract_bytes(name)
    return etree.XMLSchema(etree.fromstring(xsd, parser=_secure_parser()))


def parse_xml(data: bytes, xsd_name: str) -> etree._Element:
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
