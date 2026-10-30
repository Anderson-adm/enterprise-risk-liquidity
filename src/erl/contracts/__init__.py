"""Data and consumption contracts, loaded from the JSON files in this package."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cache
from importlib import resources
from typing import Any


@dataclass(frozen=True)
class BronzeEntity:
    table: str
    source_system: str
    entity: str
    delivery: str
    ingestion_pattern: str
    columns: tuple[str, ...]


def _load(name: str) -> dict[str, Any]:
    return json.loads(resources.files(__package__).joinpath(name).read_text(encoding="utf-8"))


@cache
def bronze_contract() -> dict[str, Any]:
    return _load("bronze_contract.json")


@cache
def consumer_contract() -> dict[str, Any]:
    return _load("consumer_contract.json")


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


def parquet_compression() -> str:
    """Parquet codec required by the Bronze contract for every object written with PyArrow."""
    return bronze_contract()["parquet_compression"]


def metadata_columns() -> list[str]:
    return [c["name"] for c in bronze_contract()["metadata_columns"]]


PARTITION_COLUMN = "bronze_reference_date"
