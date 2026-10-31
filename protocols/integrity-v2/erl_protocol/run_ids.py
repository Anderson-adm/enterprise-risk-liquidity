"""Pipeline run identifiers: ``YYYYMMDDTHHMMSSZ-xxxxxx`` (UTC start time + random suffix).

Every object a run writes carries its run id in the key, which makes the platform's writes
append-only and lets a retry re-enter the same run.
"""

from __future__ import annotations

import re
import secrets
from datetime import UTC, datetime

_RUN_ID = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{6}$")


def new_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    return f"{now:%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


def validate_run_id(run_id: str) -> str:
    if not _RUN_ID.match(run_id):
        raise ValueError(f"invalid run_id {run_id!r}; expected YYYYMMDDTHHMMSSZ-xxxxxx")
    return run_id
