"""Typed access to the data contracts in ``src/contracts`` (JSON/XSD resource package).

The contract files are data, versioned and packaged with the code (wheel, Lambda ZIP and Glue
library), so the code and the contract it was tested with always travel together.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cache
from importlib import resources
from typing import Any

PARTITION_COLUMN = "bronze_reference_date"


@dataclass(frozen=True)
class BronzeEntity:
    table: str
    source_system: str
    entity: str
    delivery: str
    ingestion_pattern: str
    columns: tuple[str, ...]


def contract_bytes(name: str) -> bytes:
    """Raw content of a contract file (JSON or XSD)."""
    return resources.files("contracts").joinpath(name).read_bytes()


@cache
def bronze_contract() -> dict[str, Any]:
    return json.loads(contract_bytes("bronze_contract.json"))


@cache
def consumer_contract() -> dict[str, Any]:
    return json.loads(contract_bytes("consumer_contract.json"))


def bronze_entity(source_system: str, entity: str) -> BronzeEntity:
    table = f"{source_system}__{entity}"
    spec = bronze_contract()["entities"].get(table)
    if spec is None:
        raise KeyError(f"no Bronze contract for {table}")
    return BronzeEntity(
        table=table,
        source_system=spec["source_system"],
        entity=spec["entity"],
        delivery=spec["delivery"],
        ingestion_pattern=spec["ingestion_pattern"],
        columns=tuple(spec["columns"]),
    )


def entities_for(source_system: str) -> list[BronzeEntity]:
    return [
        bronze_entity(spec["source_system"], spec["entity"])
        for spec in bronze_contract()["entities"].values()
        if spec["source_system"] == source_system
    ]


def metadata_columns() -> list[str]:
    return [c["name"] for c in bronze_contract()["metadata_columns"]]


def parquet_compression() -> str:
    """Parquet codec required by the Bronze contract for every object written with PyArrow."""
    return bronze_contract()["parquet_compression"]
