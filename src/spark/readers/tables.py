"""Read rows of a catalog table (e.g. Iceberg through ``glue_catalog``) with equality filters.

Filters and the column projection are pushed down by the table format; the caller decides
which table, columns and values (no business rule here).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_table_rows(
    spark: SparkSession, table: str, equals: Mapping[str, object], columns: Sequence[str]
) -> DataFrame:
    if not equals:
        raise ValueError("at least one filter is required (no full-table reads)")
    condition = reduce(lambda a, b: a & b, [F.col(c) == F.lit(v) for c, v in equals.items()])
    return spark.table(table).where(condition).select(*columns)
