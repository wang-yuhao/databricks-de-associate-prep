"""AUTO CDC (APPLY CHANGES) for the local engine: SCD type 1 and SCD type 2 on Delta tables.

Semantics follow Databricks:
  * events are ordered by `sequence_by`; late (out-of-order) events are handled correctly
  * SCD1 keeps one row per key; deletes remove the row (a tombstone is kept so that late updates
    older than the delete are ignored)
  * SCD2 keeps history with `__START_AT` / `__END_AT` (same type as the sequence column); a new
    version is only created when a tracked column changes; a delete closes the current version
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..spark import file_uri

SEQ, DEL, TRUNC = "__adelab_seq", "__adelab_is_delete", "__adelab_is_truncate"


def _cond(expr) -> F.Column:
    if expr is None:
        return F.lit(False)
    return F.coalesce(F.expr(expr) if isinstance(expr, str) else expr, F.lit(False))


def _seq_col(seq) -> F.Column:
    return F.expr(seq) if isinstance(seq, str) else seq


def output_columns(source_cols: list[str], cdc: dict) -> list[str]:
    if cdc["column_list"]:
        return list(cdc["column_list"])
    excluded = set(cdc["except_column_list"] or [])
    return [c for c in source_cols if c not in excluded]


def target_schema(source: DataFrame, cdc: dict) -> T.StructType:
    cols = output_columns(source.columns, cdc)
    fields = [source.schema[c] for c in cols]
    if cdc["scd_type"] == 2:
        seq_type = source.select(_seq_col(cdc["sequence_by"]).alias("s")).schema["s"].dataType
        fields += [T.StructField("__START_AT", seq_type, True), T.StructField("__END_AT", seq_type, True)]
    return T.StructType(fields)


def _prepare(batch: DataFrame, cdc: dict) -> DataFrame:
    return (batch.withColumn(SEQ, _seq_col(cdc["sequence_by"]))
                 .withColumn(DEL, _cond(cdc["apply_as_deletes"]))
                 .withColumn(TRUNC, _cond(cdc["apply_as_truncates"])))


# --------------------------------------------------------------------------- SCD 1


def apply_scd1(spark: SparkSession, batch: DataFrame, table: str, state_path: Path, cdc: dict) -> int:
    keys = cdc["keys"]
    cols = output_columns(batch.columns, cdc)
    ev = _prepare(batch, cdc)

    if cdc["apply_as_truncates"] is not None and ev.filter(F.col(TRUNC)).limit(1).count():
        spark.sql(f"TRUNCATE TABLE {table}")
        ev = ev.filter(~F.col(TRUNC))

    latest = (ev.withColumn("__rn", F.row_number().over(
        Window.partitionBy(*keys).orderBy(F.col(SEQ).desc()))).filter("__rn = 1").drop("__rn"))

    state_uri = file_uri(state_path)
    if DeltaTable.isDeltaTable(spark, state_uri):
        state = spark.read.format("delta").load(state_uri)
        latest = (latest.alias("e").join(state.alias("s"), keys, "left")
                  .filter(F.col("s.__seq").isNull() | (F.col(f"e.{SEQ}") > F.col("s.__seq")))
                  .select("e.*"))
    # Materialize before the MERGEs below change the tables this plan reads from.
    latest = latest.localCheckpoint(eager=True)
    n = latest.count()
    if n:
        on = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
        upd = {f"`{c}`": (f"coalesce(s.`{c}`, t.`{c}`)" if cdc["ignore_null_updates"] else f"s.`{c}`")
               for c in cols}
        (DeltaTable.forName(spark, table).alias("t").merge(latest.alias("s"), on)
            .whenMatchedDelete(condition=f"s.{DEL}")
            .whenMatchedUpdate(condition=f"NOT s.{DEL}", set=upd)
            .whenNotMatchedInsert(condition=f"NOT s.{DEL}", values={f"`{c}`": f"s.`{c}`" for c in cols})
            .execute())
        new_state = latest.select(*keys, F.col(SEQ).alias("__seq"), F.col(DEL).alias("__deleted"))
        if DeltaTable.isDeltaTable(spark, state_uri):
            (DeltaTable.forPath(spark, state_uri).alias("t").merge(new_state.alias("s"), on)
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        else:
            new_state.write.format("delta").save(state_uri)
    return n


# --------------------------------------------------------------------------- SCD 2


def _scd2_rebuild(pdf: pd.DataFrame, data_cols: list[str], tracked: list[str], ignore_nulls: bool
                  ) -> pd.DataFrame:
    pdf = pdf.sort_values([SEQ, DEL], kind="stable")
    out, cur = [], None
    for rec in pdf.to_dict("records"):
        if rec[DEL]:
            if cur is not None:
                cur["__END_AT"] = rec[SEQ]
                out.append(cur)
                cur = None
            continue
        vals = {c: rec[c] for c in data_cols}
        if ignore_nulls and cur is not None:
            vals = {c: (cur[c] if pd.isna(v) else v) for c, v in vals.items()}
        if cur is None:
            cur = {**vals, "__START_AT": rec[SEQ], "__END_AT": None}
        elif any(not (pd.isna(cur[c]) and pd.isna(vals[c])) and cur[c] != vals[c] for c in tracked):
            cur["__END_AT"] = rec[SEQ]
            out.append(cur)
            cur = {**vals, "__START_AT": rec[SEQ], "__END_AT": None}
        else:
            cur.update({c: vals[c] for c in data_cols if c not in tracked})
    if cur is not None:
        out.append(cur)
    return pd.DataFrame(out, columns=data_cols + ["__START_AT", "__END_AT"])


def apply_scd2(spark: SparkSession, batch: DataFrame, table: str, cdc: dict) -> int:
    keys = cdc["keys"]
    tgt = spark.table(table)
    data_cols = [c for c in tgt.columns if c not in ("__START_AT", "__END_AT")]
    seq_type = tgt.schema["__START_AT"].dataType
    seq_src_cols = {cdc["sequence_by"]} if isinstance(cdc["sequence_by"], str) else set()
    if cdc["track_history_column_list"]:
        tracked = list(cdc["track_history_column_list"])
    else:
        skip = set(keys) | seq_src_cols | set(cdc["track_history_except_column_list"] or [])
        tracked = [c for c in data_cols if c not in skip]

    ev = _prepare(batch, cdc).withColumn(SEQ, F.col(SEQ).cast(seq_type))
    new_events = ev.select(*[F.col(c) for c in data_cols], SEQ, DEL).dropDuplicates()
    affected = new_events.select(*keys).distinct()

    existing = tgt.join(affected, keys, "left_semi").persist()
    # Turn existing history back into events: each version is an upsert at __START_AT, and a version
    # closed without a successor starting at __END_AT means the key was deleted at __END_AT.
    upserts = existing.select(*data_cols, F.col("__START_AT").alias(SEQ), F.lit(False).alias(DEL))
    starts = existing.select(*keys, F.col("__START_AT").alias("__s"))
    deletes = (existing.filter(F.col("__END_AT").isNotNull()).alias("v")
               .join(starts.alias("n"), [F.col(f"v.{k}").eqNullSafe(F.col(f"n.{k}")) for k in keys]
                     + [F.col("v.__END_AT") == F.col("n.__s")], "left_anti")
               .select(*[F.col(f"v.{k}").alias(k) for k in keys],
                       *[F.lit(None).cast(tgt.schema[c].dataType).alias(c) for c in data_cols if c not in keys],
                       F.col("v.__END_AT").alias(SEQ), F.lit(True).alias(DEL))
               .select(*data_cols, SEQ, DEL))
    events = upserts.unionByName(deletes).unionByName(new_events)

    out_schema = T.StructType([tgt.schema[c] for c in data_cols]
                              + [T.StructField("__START_AT", seq_type), T.StructField("__END_AT", seq_type)])
    ignore = bool(cdc["ignore_null_updates"])
    rebuilt = events.groupBy(*keys).applyInPandas(
        lambda pdf: _scd2_rebuild(pdf, data_cols, tracked, ignore), schema=out_schema)
    # Materialize before deleting the affected keys from the table the plan reads from.
    rebuilt = rebuilt.localCheckpoint(eager=True)
    affected = affected.localCheckpoint(eager=True)
    n = new_events.count()

    on = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    DeltaTable.forName(spark, table).alias("t").merge(affected.alias("s"), on).whenMatchedDelete().execute()
    rebuilt.write.format("delta").mode("append").saveAsTable(table)
    existing.unpersist()
    return n
