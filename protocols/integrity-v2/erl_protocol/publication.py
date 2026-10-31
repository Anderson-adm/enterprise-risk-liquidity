"""Publication package layout and manifest, shared by the local builder (``load.package``) and
the Glue Gold Load job (``glue/export-risk-analytics/job.py``). Pure: no PyArrow, Spark or I/O.

    publish/reference_date=<D>/<run_id>/<product>.parquet
    publish/reference_date=<D>/<run_id>/manifest.json    (written last = commit marker)
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date

from erl_protocol.clock import utc_now_iso
from erl_protocol.errors import QualityGateError


def package_prefix(ref: date, run_id: str) -> str:
    return f"publish/reference_date={ref.isoformat()}/{run_id}/"


def product_key(ref: date, run_id: str, product: str) -> str:
    return package_prefix(ref, run_id) + f"{product}.parquet"


def manifest_key(ref: date, run_id: str) -> str:
    return package_prefix(ref, run_id) + "manifest.json"


def require_passed_gate(quality_gate: Mapping) -> None:
    if quality_gate.get("status") != "PASSED":
        raise QualityGateError(
            f"refusing to build a publication package: gate is {quality_gate.get('status')}"
        )


def build_manifest(
    ref: date, run_id: str, producer: str, quality_gate: Mapping, products: Mapping[str, dict]
) -> dict:
    """``products``: name -> {"object", "sha256", "rows"} of each written product."""
    return {
        "pipeline_run_id": run_id,
        "reference_date": ref.isoformat(),
        "producer": producer,
        "quality_gate": dict(quality_gate),
        "products": dict(products),
        "created_at": utc_now_iso(),
    }
