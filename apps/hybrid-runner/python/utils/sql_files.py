"""Bind SQL resource access to the local runner; validation is versioned with integrity."""

from erl_protocol.sql_files import read_sql as read_resource
from erl_protocol.sql_files import render_identifiers

__all__ = ["read_sql", "render_identifiers"]


def read_sql(relative_path: str) -> str:
    return read_resource(relative_path, package="sql")
