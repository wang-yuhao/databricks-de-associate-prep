# Day 6 — Practice Tasks: Python Project Structure, UDFs & Testing

---

## Task 1 — UDF Type Selection

For each scenario, choose the best implementation:

| Scenario | Best choice |
|----------|-------------|
| Capitalize a string column | ? |
| Custom ML scoring using NumPy | ? |
| Normalize values using pandas mean/std | ? |
| Apply per-group statistics to a DataFrame | ? |

Choices: A) Spark built-in (`F.upper`), B) Python UDF, C) Pandas Scalar UDF, D) Pandas Grouped Map UDF

---

## Task 2 — UDF Registration

You have a Python UDF `score_risk` decorated with `@udf(returnType=DoubleType())`. How do you make it callable from a SQL cell?

```python
___
```

---

## Task 3 — transform() Chain

Write a pipeline that:
1. Adds a `vat_total` column = `amount * 1.25`
2. Filters rows where `amount > 0` and `customer IS NOT NULL`
3. Adds a `region_upper` column = uppercased `region`

Using `.transform()` calls:

```python
final_df = (
    raw_df
    .transform(___)
    .transform(___)
    .transform(___)
)
```

---

## Task 4 — assertDataFrameEqual

You want to test that `result_df` matches `expected_df` where:
- Row order may differ
- Float columns may differ by up to 0.001 relative tolerance

Fill in:

```python
from pyspark.testing import assertDataFrameEqual

assertDataFrameEqual(result_df, expected_df, checkRowOrder=___, rtol=___)
```

---

## Task 5 — assertSchemaEqual vs assertDataFrameEqual

Choose the right assertion for each check:

| Check | Assertion |
|-------|-----------|
| Verify transformation adds correct column with correct type | ? |
| Verify filter doesn't change schema | ? |
| Verify exact row values | ? |

---

## Task 6 — Library Scope

Match each installation method to its persistence scope:

| Method | Scope |
|--------|-------|
| `%pip install` in a notebook | ? |
| Libraries UI on cluster | ? |
| Init script | ? |
| `libraries` in job config | ? |

---

## Task 7 — Pandas UDF Serialization

Why is a Pandas UDF faster than a row-by-row Python UDF in PySpark?

(Free text — write 1-2 sentences)

---

## Answers

<details>
<summary>Click to reveal</summary>

**Task 1:**
| Scenario | Best choice |
|----------|-------------|
| Capitalize string | A (Spark built-in) |
| Custom ML scoring | C (Pandas Scalar UDF) |
| Normalize with pandas | C (Pandas Scalar UDF) |
| Per-group statistics | D (Pandas Grouped Map UDF) |

**Task 2:** `spark.udf.register("score_risk", score_risk)`

**Task 3:**
```python
def add_vat(df): return df.withColumn("vat_total", F.col("amount") * 1.25)
def filter_valid(df): return df.filter((F.col("amount") > 0) & F.col("customer").isNotNull())
def upper_region(df): return df.withColumn("region_upper", F.upper("region"))

final_df = raw_df.transform(add_vat).transform(filter_valid).transform(upper_region)
```

**Task 4:** `checkRowOrder=False, rtol=1e-3`

**Task 5:**
| Check | Assertion |
|-------|-----------|
| Correct column + type | assertDataFrameEqual |
| Schema unchanged | assertSchemaEqual |
| Exact row values | assertDataFrameEqual |

**Task 6:**
| Method | Scope |
|--------|-------|
| `%pip install` | Notebook session only |
| Libraries UI | Cluster lifetime |
| Init script | Cluster lifetime |
| Job `libraries` | Job run only |

**Task 7:** Pandas UDFs use Apache Arrow for columnar (batch) data transfer between JVM and Python, avoiding row-by-row serialization/deserialization overhead that makes standard Python UDFs slow.

</details>
