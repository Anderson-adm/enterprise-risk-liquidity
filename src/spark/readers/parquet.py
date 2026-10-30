"""Read Parquet objects into a DataFrame (lazy: nothing is read until an action)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def read_parquet(spark: SparkSession, uri: str) -> DataFrame:
    return spark.read.parquet(uri)
