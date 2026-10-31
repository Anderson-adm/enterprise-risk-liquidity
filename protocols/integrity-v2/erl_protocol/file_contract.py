"""Published Python result/exception for file parsers; no parser implementation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from erl_protocol.contracts import BronzeEntity
from erl_protocol.errors import ContractError


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


def parse_iso_date(text: str | None) -> date:
    try:
        return date.fromisoformat((text or "").strip())
    except ValueError as exc:
        raise RejectedDelivery("INVALID_REFERENCE_DATE", repr(text)) from exc
