"""Execute SQL text received from the caller (normally a file of ``src/sql``) with binds.

These functions know no source, table, date or business rule: the caller supplies the SQL,
the parameters, the expected output columns and the batch size.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy import Date, DateTime, Engine, TextClause, bindparam, column, text
from sqlalchemy.types import TypeEngine

from utils.errors import ContractError


@dataclass(frozen=True)
class QueryResult:
    rows: int
    batches: int


def typed_statement(
    sql: str, params: Mapping[str, Any], result_types: Mapping[str, TypeEngine] | None = None
) -> tuple[TextClause, dict[str, Any]]:
    """``text(sql)`` with typed date binds (portable across drivers) and typed result columns
    (so every engine returns the same Python values, e.g. ``datetime`` on SQLite too).

    Returns the statement and the parameters it uses: a register query without ``:reference_date``
    may receive the same parameter set as the dated queries."""
    stmt = text(sql)
    used = {name: params[name] for name in stmt.compile().params if name in params}
    binds = [
        bindparam(name, type_=DateTime() if isinstance(v, datetime) else Date())
        for name, v in used.items()
        if isinstance(v, date)
    ]
    if binds:
        stmt = stmt.bindparams(*binds)
    if result_types:
        stmt = stmt.columns(*[column(name, type_) for name, type_ in result_types.items()])
    return stmt, used


def execute_query_in_batches(
    engine: Engine,
    sql: str,
    params: Mapping[str, Any],
    *,
    expected_columns: Sequence[str],
    batch_size: int,
    consume: Callable[[Sequence[Sequence[Any]]], None],
    result_types: Mapping[str, TypeEngine] | None = None,
) -> QueryResult:
    """Stream the result in ``batch_size`` chunks to ``consume`` (rows in column order).

    Opens one connection for the whole read and closes it, with the cursor, on exit or error.
    The result columns must equal ``expected_columns`` (the data contract) exactly."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    rows = batches = 0
    stmt, used = typed_statement(sql, params, result_types)
    with engine.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(stmt, used)
        try:
            if list(result.keys()) != list(expected_columns):
                raise ContractError(
                    f"query columns {list(result.keys())} != contract {list(expected_columns)}"
                )
            while chunk := result.fetchmany(batch_size):
                consume(chunk)
                rows += len(chunk)
                batches += 1
        finally:
            result.close()
    return QueryResult(rows=rows, batches=batches)


def fetch_scalar(engine: Engine, sql: str, params: Mapping[str, Any]) -> Any:
    """First column of the first row (``None`` when there is no row)."""
    stmt, used = typed_statement(sql, params)
    with engine.connect() as conn:
        return conn.execute(stmt, used).scalar()
