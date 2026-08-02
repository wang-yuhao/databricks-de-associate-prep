"""Postgres access layer.

Uses psycopg2 + pandas rather than Spark's JDBC reader on purpose: running this
project locally (no Databricks) means there's no guarantee you have the Postgres
JDBC driver jar on the Spark classpath, and pulling it from Maven Central requires
network access you may not have in a locked-down environment. psycopg2 is pure
Python, pip-installable, and gets the same data -- we just hand it to Spark via
`spark.createDataFrame(pandas_df)` afterwards.

If you *do* have the JDBC driver available and prefer spark.read.jdbc(...), the
ingestion module (src/ingestion/postgres_to_bronze.py) isolates the read behind
`fetch_table` so you can swap the implementation without touching the rest of
the pipeline.
"""
import os

import pandas as pd
import psycopg2


def get_pg_connection(cfg: dict):
    pg = cfg["postgres"]
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", pg["host"]),
        port=os.environ.get("POSTGRES_PORT", pg["port"]),
        dbname=os.environ.get("POSTGRES_DB", pg["database"]),
        user=os.environ.get("POSTGRES_USER", pg["user"]),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


def fetch_table(
    cfg: dict,
    table_key: str,
    symbols: list = None,
    since_ts=None,
    chunksize: int = 250_000,
):
    """Yields pandas DataFrame chunks from a configured Postgres table.

    table_key: one of 'bars', 'quotes', 'trades' -- looked up in config.yaml under
               postgres.tables so column/table names stay out of the code.
    symbols:   optional list of ticker symbols to filter to.
    since_ts:  optional watermark (str or datetime) -- only rows strictly after this
               timestamp are returned, which is what makes incremental/streaming-style
               ingestion possible (see src/ingestion/postgres_to_bronze.py --incremental).
    """
    table_cfg = cfg["postgres"]["tables"][table_key]
    table_name = table_cfg["name"]
    symbol_col = table_cfg["symbol_col"]
    ts_col = table_cfg["timestamp_col"]

    where_clauses = []
    params = []
    if symbols:
        where_clauses.append(f"{symbol_col} = ANY(%s)")
        params.append(symbols)
    if since_ts is not None:
        where_clauses.append(f"{ts_col} > %s")
        params.append(since_ts)

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query = f"SELECT * FROM {table_name} {where_sql} ORDER BY {ts_col}"

    conn = get_pg_connection(cfg)
    try:
        for chunk in pd.read_sql(query, conn, params=params or None, chunksize=chunksize):
            yield chunk
    finally:
        conn.close()


def get_max_timestamp(cfg: dict, table_key: str):
    """Used by incremental ingestion to find the watermark already in Delta bronze."""
    table_cfg = cfg["postgres"]["tables"][table_key]
    conn = get_pg_connection(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT max({table_cfg['timestamp_col']}) FROM {table_cfg['name']}")
            return cur.fetchone()[0]
    finally:
        conn.close()
