"""Databricks notebook ergonomics for JupyterLab: display(), %sql / %%sql magics."""
from __future__ import annotations

import html

from pyspark.sql import DataFrame, SparkSession

_spark: SparkSession | None = None


def display(obj=None, limit: int = 1000) -> None:
    """Databricks-style display(): DataFrames render as a scrollable HTML table."""
    from IPython.display import HTML
    from IPython.display import display as ipy_display

    if isinstance(obj, DataFrame):
        if obj.isStreaming:
            raise ValueError("display() of a streaming DataFrame is not supported locally; "
                             "write it to a table with trigger(availableNow=True) and display the table.")
        pdf = obj.limit(limit + 1).toPandas()
        truncated = len(pdf) > limit
        pdf = pdf.head(limit)
        table = pdf.to_html(index=False, na_rep="null", escape=True, max_cols=200)
        note = f"{len(pdf)} row(s){' (truncated to ' + str(limit) + ')' if truncated else ''}"
        ipy_display(HTML(
            "<div style='max-height:440px;overflow:auto;border:1px solid #ddd'>"
            f"{table}</div><small style='color:#666'>{html.escape(note)}</small>"))
    else:
        ipy_display(obj)


def split_sql(text: str) -> list[str]:
    """Split a SQL script on semicolons that are outside quotes and comments."""
    stmts, buf, i, n = [], [], 0, len(text)
    quote = None
    while i < n:
        c = text[i]
        if quote:
            buf.append(c)
            if c == quote:
                quote = None
        elif c in ("'", '"', "`"):
            quote = c
            buf.append(c)
        elif text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j == -1 else j
            continue
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            i = n if j == -1 else j + 2
            continue
        elif c == ";":
            stmts.append("".join(buf))
            buf = []
        else:
            buf.append(c)
        i += 1
    stmts.append("".join(buf))
    return [s.strip() for s in stmts if s.strip()]


def run_sql(text: str, show: bool = True):
    result = None
    for stmt in split_sql(text):
        result = _spark.sql(stmt)
    if show and result is not None and result.columns:
        display(result)
    return result


def register(spark: SparkSession) -> None:
    global _spark
    _spark = spark
    try:
        from IPython import get_ipython
    except ImportError:
        return
    ip = get_ipython()
    if ip is None:
        return

    def sql_magic(line, cell=None):
        """%%sql cell (or %sql line) magic: runs each statement, displays the last result."""
        run_sql(cell if cell is not None else line)

    ip.register_magic_function(sql_magic, magic_kind="line_cell", magic_name="sql")
