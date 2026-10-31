"""Native PySpark operations owned and deployed by this job only."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def write_parquet(df: DataFrame, uri: str, *, files: int, shuffle: bool) -> None:
    """``shuffle=True`` repartitions into ``files`` balanced files (large inputs);
    ``shuffle=False`` coalesces without a shuffle - only for outputs whose size the caller has
    bounded (e.g. one file per small consumer product). ``errorifexists`` refuses to write into
    an existing prefix."""
    if files < 1:
        raise ValueError("files must be >= 1")
    shaped = df.repartition(files) if shuffle else df.coalesce(files)
    shaped.write.mode("errorifexists").parquet(uri)


def read_table_rows(
    spark: SparkSession, table: str, equals: Mapping[str, object], columns: Sequence[str]
) -> DataFrame:
    if not equals:
        raise ValueError("at least one filter is required (no full-table reads)")
    condition = reduce(lambda a, b: a & b, [F.col(c) == F.lit(v) for c, v in equals.items()])
    return spark.table(table).where(condition).select(*columns)
