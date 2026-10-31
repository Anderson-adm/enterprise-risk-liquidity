"""Local-mode integration with the three Lambdas through their published parse API."""

from external_xml import parse as parse_external_xml
from internal_xml import parse as parse_internal_xml
from market_data import parse as parse_market_data_csv

from erl_protocol.file_contract import ParsedDelivery, RejectedDelivery

FILE_SOURCES = {
    "market_data": ("daily_price", "csv"),
    "internal_xml": ("portfolio_position", "xml"),
    "external_xml": ("transfer_in_position", "xml"),
}

PARSERS = {
    "market_data": parse_market_data_csv,
    "internal_xml": parse_internal_xml,
    "external_xml": parse_external_xml,
}

__all__ = [
    "FILE_SOURCES",
    "PARSERS",
    "ParsedDelivery",
    "RejectedDelivery",
    "parse_external_xml",
    "parse_internal_xml",
    "parse_market_data_csv",
]
