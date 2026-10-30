"""Write a DataFrame as Parquet files under a prefix that must not exist yet."""

from __future__ import annotations

from pyspark.sql import DataFrame


def write_parquet(df: DataFrame, uri: str, *, files: int, shuffle: bool) -> None:
    """``shuffle=True`` repartitions into ``files`` balanced files (large inputs);
    ``shuffle=False`` coalesces without a shuffle - only for outputs whose size the caller has
    bounded (e.g. one file per small consumer product). ``errorifexists`` refuses to write into
    an existing prefix."""
    if files < 1:
        raise ValueError("files must be >= 1")
    shaped = df.repartition(files) if shuffle else df.coalesce(files)
    shaped.write.mode("errorifexists").parquet(uri)
