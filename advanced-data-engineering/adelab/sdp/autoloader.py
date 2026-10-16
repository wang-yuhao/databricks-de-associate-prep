"""Auto Loader (`format("cloudFiles")`) emulation on top of Spark's file streaming source.

Supported options (others are passed through to the underlying reader):
    cloudFiles.format              json | csv | parquet | text
    cloudFiles.schemaLocation      where the inferred schema is persisted between runs
    cloudFiles.inferColumnTypes    default false: JSON/CSV columns are inferred as STRING, as in Auto Loader
    cloudFiles.schemaHints         "col TYPE, col2 TYPE" overrides
    cloudFiles.schemaEvolutionMode addNewColumns (default) | rescue | none
    cloudFiles.maxFilesPerTrigger  mapped to maxFilesPerTrigger
    rescuedDataColumn              name of the rescued data column (default `_rescued_data`)

JSON sources get a real rescued data column: fields that are not in the schema, and values that fail
to cast to a hinted type, are collected into `_rescued_data` as JSON, together with `_file_path`.
"""
from __future__ import annotations

import json
import zlib
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming.readwriter import DataStreamReader

_orig_format = DataStreamReader.format
_orig_option = DataStreamReader.option
_orig_options = DataStreamReader.options
_orig_schema = DataStreamReader.schema
_orig_load = DataStreamReader.load
_installed = False


def _state(reader) -> dict:
    if not hasattr(reader, "_adelab"):
        reader._adelab = {"cloudfiles": False, "cf": {}, "other": {}, "schema": None}
    return reader._adelab


def _format(self, source):
    st = _state(self)
    if source.lower() == "cloudfiles":
        st["cloudfiles"] = True
        return self
    return _orig_format(self, source)


def _option(self, key, value):
    st = _state(self)
    if st["cloudfiles"] or key.startswith("cloudFiles."):
        if key.startswith("cloudFiles."):
            st["cf"][key[len("cloudFiles."):]] = str(value)
        else:
            st["other"][key] = str(value)
        return self
    return _orig_option(self, key, value)


def _options(self, **opts):
    for k, v in opts.items():
        _option(self, k, v)
    return self


def _schema(self, schema):
    st = _state(self)
    st["schema"] = schema
    return _orig_schema(self, schema)


def _parse_hints(hints: str) -> dict[str, str]:
    out = {}
    for part in [h.strip() for h in hints.split(",") if h.strip()]:
        col, typ = part.split(None, 1)
        out[col.strip("`")] = typ.strip()
    return out


def _local(path: str) -> Path:
    p = path[5:] if path.startswith("file:") else path
    if len(p) > 2 and p[0] == "/" and p[2] == ":":
        p = p[1:]
    return Path(p)


def _infer(spark: SparkSession, fmt: str, path: str, other: dict, infer_types: bool) -> T.StructType:
    reader = spark.read.format(fmt).options(**other)
    if fmt == "csv":
        reader = reader.option("inferSchema", str(infer_types).lower())
    if fmt == "json" and not infer_types:
        reader = reader.option("primitivesAsString", "true")
    schema = reader.load(path).schema
    if not infer_types and fmt in ("json", "csv"):
        schema = T.StructType([T.StructField(f.name, T.StringType(), True)
                               if isinstance(f.dataType, T.AtomicType) else f for f in schema.fields])
    return T.StructType([f for f in schema.fields if f.name != "_corrupt_record"])


def _load(self, path=None, format=None, schema=None, **options):  # noqa: A002
    st = _state(self)
    if not st["cloudfiles"]:
        return _orig_load(self, path, format, schema, **options)

    spark = SparkSession.getActiveSession()
    cf, other = dict(st["cf"]), {**st["other"], **{k: str(v) for k, v in options.items()}}
    fmt = cf.get("format")
    if not fmt:
        raise ValueError("Auto Loader requires the option cloudFiles.format")
    rescued_col = other.pop("rescuedDataColumn", "_rescued_data")
    if "maxFilesPerTrigger" in cf:
        other["maxFilesPerTrigger"] = cf["maxFilesPerTrigger"]
    user_schema = schema or st["schema"]
    hints = _parse_hints(cf.get("schemaHints", ""))
    infer_types = cf.get("inferColumnTypes", "false").lower() == "true"
    mode = cf.get("schemaEvolutionMode", "addNewColumns" if user_schema is None else "none")

    # ---- schema: user supplied, or inferred and persisted in schemaLocation
    if user_schema is not None:
        base = user_schema if isinstance(user_schema, T.StructType) else T._parse_datatype_string(user_schema)
    else:
        loc = cf.get("schemaLocation")
        managed_root = spark.conf.get("adelab.autoloader.schemaRoot", None)
        if not loc and managed_root:  # inside a pipeline the schema location is managed for you
            loc = str(Path(managed_root) / f"{zlib.crc32(str(path).encode()):08x}")
        if not loc:
            raise ValueError("cloudFiles.schemaLocation is required when no schema is provided")
        schema_file = _local(loc) / "_schemas" / "schema.json"
        inferred = _infer(spark, fmt, path, other, infer_types)
        if schema_file.exists():
            base = T.StructType.fromJson(json.loads(schema_file.read_text()))
            new_cols = [f for f in inferred.fields if f.name not in base.fieldNames()]
            if new_cols and mode == "addNewColumns":
                base = T.StructType(base.fields + new_cols)
                print(f"[auto loader] schema evolved, added columns: {[f.name for f in new_cols]}")
        else:
            base = inferred
        schema_file.parent.mkdir(parents=True, exist_ok=True)
        schema_file.write_text(json.dumps(base.jsonValue()))

    typed = T.StructType([T.StructField(f.name, T._parse_datatype_string(hints[f.name]), True)
                          if f.name in hints else f for f in base.fields])
    for col, typ in hints.items():
        if col not in typed.fieldNames():
            typed = typed.add(col, T._parse_datatype_string(typ), True)

    if fmt != "json" or (mode == "none" and not hints):
        r = spark.readStream.format(fmt).options(**other).schema(typed)
        df = _orig_load(r, path)
        return df.withColumn(rescued_col, F.lit(None).cast("string")) if mode == "rescue" else df

    # ---- JSON with a rescued data column: parse each line as a map so nothing is lost
    passthrough = {k: v for k, v in other.items()
                   if k in ("maxFilesPerTrigger", "pathGlobFilter", "recursiveFileLookup")}
    raw = _orig_load(spark.readStream.format("text").options(**passthrough), path)
    # withColumn/drop (not select) keeps the hidden `_metadata` column available downstream.
    df = raw.withColumn("__m", F.from_json("value", "map<string,string>"))
    empty = F.create_map().cast("map<string,string>")
    known = typed.fieldNames()
    problems = []
    for f in typed.fields:
        val = F.col("__m")[f.name]
        if isinstance(f.dataType, T.StringType):
            df = df.withColumn(f.name, val)
        else:
            casted = F.expr(f"try_cast(__m['{f.name}'] AS {f.dataType.simpleString()})")
            df = df.withColumn(f.name, casted)
            problems.append(F.when(val.isNotNull() & casted.isNull(),
                                   F.create_map(F.lit(f.name), val)).otherwise(empty))
    extra = F.map_filter("__m", lambda k, v: ~k.isin(known)) if mode != "none" else empty
    rescued = F.map_concat(extra, *problems) if problems else extra
    df = df.withColumn("__r", rescued).withColumn(rescued_col, F.when(F.size("__r") > 0, F.to_json(
        F.map_concat("__r", F.create_map(F.lit("_file_path"), F.col("_metadata.file_path"))))))
    return df.drop("value", "__m", "__r")


def install() -> None:
    global _installed
    if _installed:
        return
    DataStreamReader.format = _format
    DataStreamReader.option = _option
    DataStreamReader.options = _options
    DataStreamReader.schema = _schema
    DataStreamReader.load = _load
    _installed = True


def uninstall() -> None:
    global _installed
    DataStreamReader.format = _orig_format
    DataStreamReader.option = _orig_option
    DataStreamReader.options = _orig_options
    DataStreamReader.schema = _orig_schema
    DataStreamReader.load = _orig_load
    _installed = False


def read_cloudfiles(spark: SparkSession, path: str, fmt: str, **options) -> DataFrame:
    """Convenience wrapper: same as spark.readStream.format('cloudFiles')...load(path)."""
    install()
    r = spark.readStream.format("cloudFiles").option("cloudFiles.format", fmt)
    for k, v in options.items():
        r = r.option(k, v)
    return r.load(path)
