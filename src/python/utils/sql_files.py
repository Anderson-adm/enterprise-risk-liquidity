"""Load versioned SQL from the ``sql`` resource package (``src/sql``).

Values are always bind parameters. Identifiers cannot be bound, so a statement may contain
``{name}`` placeholders that ``render_identifiers`` fills only with names from an explicit
allowlist (for example the columns of a data contract).
"""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping
from importlib import resources

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")


def read_sql(relative_path: str) -> str:
    """Text of ``src/sql/<relative_path>``, e.g. ``extract/extract_query_table_fund.sql``."""
    if not relative_path.endswith(".sql") or ".." in relative_path.split("/"):
        raise ValueError(f"not a SQL resource path: {relative_path!r}")
    return resources.files("sql").joinpath(relative_path).read_text(encoding="utf-8")


def render_identifiers(sql: str, allowed: Collection[str], **identifiers: str | list[str]) -> str:
    """Fill ``{name}`` placeholders with validated identifiers (a list renders comma-separated)."""
    rendered: Mapping[str, str] = {}
    for key, value in identifiers.items():
        names = value if isinstance(value, list) else [value]
        for name in names:
            if not _IDENTIFIER.fullmatch(name) or name not in allowed:
                raise ValueError(f"identifier {name!r} is not allowed in {key}")
        rendered = {**rendered, key: ", ".join(names)}
    return sql.format_map(rendered)
