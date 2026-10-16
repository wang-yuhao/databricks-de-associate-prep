"""Read Delta UniForm tables through their Iceberg metadata with PyIceberg, a non-Spark engine.

This is what "Iceberg reads" means: Snowflake, Trino, BigQuery, DuckDB, or PyIceberg read the same
Parquet files through Iceberg metadata, while Spark keeps writing Delta.
"""
from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import SparkSession

from .classroom import _local_path


def table_location(spark: SparkSession, table: str) -> Path:
    return _local_path(spark.sql(f"DESCRIBE DETAIL {table}").first()["location"])


def metadata_files(spark: SparkSession, table: str) -> list[Path]:
    """Iceberg metadata JSON files written by UniForm, oldest first."""
    meta = table_location(spark, table) / "metadata"
    files = list(meta.glob("*.metadata.json")) if meta.exists() else []
    return sorted(files, key=lambda p: int(re.match(r"(\d+)", p.name).group(1)))


def load_table(spark: SparkSession, table: str):
    """A PyIceberg `StaticTable` for the latest Iceberg snapshot of a UniForm table."""
    from pyiceberg.table import StaticTable

    files = metadata_files(spark, table)
    if not files:
        raise FileNotFoundError(f"{table} has no Iceberg metadata. Is UniForm enabled "
                                "(delta.universalFormat.enabledFormats = 'iceberg')?")
    # FsspecFileIO handles Windows file URIs that PyArrow's local filesystem rejects.
    return StaticTable.from_metadata(files[-1].as_posix(),
                                     properties={"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})


def read_as_pandas(spark: SparkSession, table: str, row_filter: str = "True", limit: int | None = None):
    return load_table(spark, table).scan(row_filter=row_filter, limit=limit).to_pandas()
