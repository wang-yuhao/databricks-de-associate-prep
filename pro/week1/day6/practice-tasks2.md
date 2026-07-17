# Day 6 Practice Tasks — Python Project Structure, UDFs & Testing

> **Exam section:** Python Project Structure, UDFs & Testing (~22% of exam)
> **Prerequisite:** Read `study-notes.md` completely before starting these tasks.
> **Estimated time:** 2-3 hours
> **Difficulty:** 🔥🔥🔥 Professional Level

---

## How to Use These Tasks

Work through each task **in order** — each one builds on the last. Every task has:

- 📖 **Context** — why this matters for the exam
- 🛠️ **Instructions** — what you must do, step by step
- ✅ **Expected outcome** — how to verify your answer
- ⚠️ **Exam trap** — a common wrong-answer pitfall

---

## Task 1 — Scaffold a DABs-Ready Python Project

📖 **Context**: The exam tests whether you know where business logic, tests, and notebooks each belong in a well-structured Databricks Asset Bundle project. Logic that lives only inside a notebook cannot be unit tested with pytest.

🛠️ **Instructions**:

### Step 1 — Create the directory skeleton:

```bash
mkdir -p my_project/src/my_project
mkdir -p my_project/tests
mkdir -p my_project/notebooks
mkdir -p my_project/resources

cd my_project
touch databricks.yml pyproject.toml requirements.txt
touch src/my_project/__init__.py
touch src/my_project/transformations.py
touch src/my_project/utils.py
touch src/my_project/config.py
touch tests/__init__.py
touch tests/test_transformations.py
touch tests/test_utils.py
```

### Step 2 — Define packaging in `pyproject.toml`:

```toml
[project]
name = "my_project"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "pyspark>=3.5.0",
]

[project.optional-dependencies]
dev = ["pytest>=7.0.0"]

[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"
```

### Step 3 — Keep notebooks thin — import from the package instead of duplicating logic:

```python
# notebooks/02_transform.py
# %pip install -e ..   (editable install of the src/ package on the cluster)

from my_project.transformations import add_total_with_tax, filter_valid_orders

df = spark.table("training.prep.orders")
result = df.transform(filter_valid_orders).transform(add_total_with_tax, tax_rate=0.19)
result.write.mode("overwrite").saveAsTable("training.prep.orders_clean")
```

✅ **Expected outcome**:
- `src/my_project/` holds all reusable business logic as plain functions
- `tests/` mirrors `src/my_project/` file-for-file
- `notebooks/` only orchestrates: import, call, write — no inline transformation logic
- `databricks.yml` + `resources/` are ready for DABs deployment

⚠️ **Exam trap**: Writing transformation logic directly inside a notebook cell. Wrong! Notebook-only code **cannot be imported by pytest**, so it can't be unit tested. Business logic belongs in the `src/` package; notebooks only call it.

---

## Task 2 — Regular Python UDFs

📖 **Context**: The exam tests whether you understand how a standard Python UDF is registered, used from both the DataFrame API and SQL, and why it is the slowest UDF option.

🛠️ **Instructions**:

### Step 1 — Define and register a UDF:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def classify_amount(amount):
    if amount is None:
        return "unknown"
    elif amount < 50:
        return "small"
    elif amount < 200:
        return "medium"
    else:
        return "large"

spark.udf.register("classify_amount", classify_amount)
```

### Step 2 — Use it from both APIs:

```python
df = spark.table("training.prep.orders")

# DataFrame API
df.withColumn("size_class", classify_amount("amount")).display()

# SQL (only works after spark.udf.register)
spark.sql("SELECT order_id, classify_amount(amount) AS size_class FROM training.prep.orders").display()
```

### Step 3 — Break it on purpose to see the type-mismatch trap:

```python
@udf(StringType())
def bad_udf(amount):
    return amount * 2   # returns a number, but declared type is StringType -> silently miscast
```

✅ **Expected outcome**:
- UDF callable from `.withColumn()` and from `spark.sql(...)` after registration
- You can explain why `classify_amount` must handle `None` explicitly (nulls are not auto-skipped)

⚠️ **Exam trap**: Assuming a UDF decorated with `@udf` is automatically usable in SQL, or vice versa. `@udf` enables DataFrame API use; `spark.udf.register()` is a **separate, additional** step required for SQL use.

---

## Task 3 — Scalar Pandas UDFs for Vectorized Performance

📖 **Context**: The exam tests whether you know Pandas UDFs use Apache Arrow to operate on whole columns (batches) instead of row-by-row, making them dramatically faster than regular UDFs.

🛠️ **Instructions**:

### Step 1 — Write a scalar Pandas UDF:

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import FloatType
import pandas as pd

@pandas_udf(FloatType())
def normalize_amount(amounts: pd.Series) -> pd.Series:
    return (amounts - amounts.mean()) / amounts.std()

df.withColumn("normalized_amount", normalize_amount("amount")).display()
```

### Step 2 — Confirm Arrow is enabled (required for Pandas UDFs):

```python
print(spark.conf.get("spark.sql.execution.arrow.pyspark.enabled"))
# Should be "true" on modern Databricks Runtimes by default
```

### Step 3 — Compare mentally against Task 2:

| | Regular UDF | Scalar Pandas UDF |
|---|---|---|
| Input unit | One row at a time | A `pandas.Series` (batch) |
| Serialization | Pickle, row-by-row | Apache Arrow, columnar |
| Type hints | Not required | Required (`pd.Series -> pd.Series`) |

✅ **Expected outcome**:
- `normalize_amount` accepts and returns a `pandas.Series`, not a scalar
- You can explain that the function signature (`pd.Series -> pd.Series`) is what makes it a *scalar* Pandas UDF

⚠️ **Exam trap**: Believing Pandas UDFs beat built-in Spark functions in performance. Wrong! Built-in Spark functions (native JVM) are still faster than Pandas UDFs — always prefer built-ins when the operation already exists natively.

---

## Task 4 — Grouped Map Pandas UDFs (`applyInPandas`)

📖 **Context**: The exam tests per-group custom logic — computing something that depends on all rows within a group at once, such as a rank or a group-level normalization.

🛠️ **Instructions**:

### Step 1 — Define the function and output schema:

```python
schema = "order_id BIGINT, customer STRING, amount DOUBLE, rank INT"

def add_rank(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["rank"] = pdf["amount"].rank(ascending=False).astype(int)
    return pdf
```

### Step 2 — Apply it per group using the modern API:

```python
# Preferred modern syntax (PandasUDFType.GROUPED_MAP is deprecated)
ranked = df.groupBy("region").applyInPandas(add_rank, schema=schema)
ranked.display()
```

### Step 3 — Contrast with `mapInPandas` (no grouping, whole-partition transform):

```python
def add_row_id(iterator):
    for pdf in iterator:
        pdf["row_id"] = range(len(pdf))
        yield pdf

df.mapInPandas(add_row_id, schema=df.schema.add("row_id", "long")).display()
```

✅ **Expected outcome**:
- `applyInPandas` receives and returns a full `pandas.DataFrame` **per group**
- The output schema must be declared up front and match exactly what the function returns
- `mapInPandas` operates per **partition**, not per group — no `groupBy()` needed

⚠️ **Exam trap**: Using the legacy `@pandas_udf(schema, PandasUDFType.GROUPED_MAP)` syntax on the exam and assuming it's still the recommended API. `df.groupBy(...).applyInPandas(func, schema)` is the current, preferred syntax.

---

## Task 5 — Unit Testing Transformations with pytest

📖 **Context**: The exam tests your ability to write and structure pytest tests for `DataFrame.transform()`-style functions using a local SparkSession fixture.

🛠️ **Instructions**:

### Step 1 — Write the transformations under test (`src/my_project/transformations.py`):

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def add_total_with_tax(df: DataFrame, tax_rate: float = 0.2) -> DataFrame:
    return df.withColumn("total_with_tax", F.col("amount") * (1 + tax_rate))

def filter_valid_orders(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("amount") > 0) &
        (F.col("customer").isNotNull()) &
        (F.col("order_id").isNotNull())
    )
```

### Step 2 — Write the test file with a session-scoped fixture:

```python
# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from my_project.transformations import add_total_with_tax, filter_valid_orders

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()

def test_add_total_with_tax(spark):
    input_df = spark.createDataFrame(
        [(1, "Alice", 100.0), (2, "Bob", 200.0)],
        "order_id BIGINT, customer STRING, amount DOUBLE"
    )
    result_df = add_total_with_tax(input_df, tax_rate=0.2)
    expected_df = spark.createDataFrame(
        [(1, "Alice", 100.0, 120.0), (2, "Bob", 200.0, 240.0)],
        "order_id BIGINT, customer STRING, amount DOUBLE, total_with_tax DOUBLE"
    )
    assertDataFrameEqual(result_df, expected_df)

def test_filter_valid_orders_removes_nulls(spark):
    input_df = spark.createDataFrame(
        [(1, "Alice", 100.0), (None, "Bob", 50.0), (3, None, 75.0), (4, "Dave", -10.0)],
        "order_id BIGINT, customer STRING, amount DOUBLE"
    )
    result_df = filter_valid_orders(input_df)
    assert result_df.count() == 1
    assert result_df.collect()[0]["order_id"] == 1
```

### Step 3 — Run the suite:

```bash
pip install pytest --break-system-packages
pytest -v tests/
pytest tests/test_transformations.py::test_add_total_with_tax
```

✅ **Expected outcome**:
- All tests pass locally without any Databricks cluster
- The `spark` fixture is created **once per test session**, not once per test

⚠️ **Exam trap**: Using `scope="function"` (the default) for the `spark` fixture. This recreates a SparkSession for every single test, which is slow and can exhaust resources. Always scope it to `"session"` for a test suite.

---

## Task 6 — `assertDataFrameEqual` and `assertSchemaEqual` Deep Dive

📖 **Context**: The exam tests the specific parameters of these two assertion helpers — especially row ordering and floating-point tolerance.

🛠️ **Instructions**:

### Step 1 — Compare two DataFrames with different row order:

```python
from pyspark.testing import assertDataFrameEqual

actual_df = spark.createDataFrame([(2, "b"), (1, "a")], "id INT, val STRING")
expected_df = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, val STRING")

# Passes: row order does not matter by default
assertDataFrameEqual(actual_df, expected_df)

# Fails: forcing exact row order
assertDataFrameEqual(actual_df, expected_df, checkRowOrder=True)
```

### Step 2 — Use tolerance for floating-point comparisons:

```python
actual_df = spark.createDataFrame([(1, 10.0000001)], "id INT, amount DOUBLE")
expected_df = spark.createDataFrame([(1, 10.0)], "id INT, amount DOUBLE")

# Fails without tolerance
# assertDataFrameEqual(actual_df, expected_df)

# Passes with a relative tolerance
assertDataFrameEqual(actual_df, expected_df, rtol=1e-5)
```

### Step 3 — Schema-only comparison:

```python
from pyspark.testing import assertSchemaEqual

df1 = spark.createDataFrame([(1, "a")], "id INT, val STRING")
df2 = spark.createDataFrame([(2, "b")], "id INT, val STRING")

assertSchemaEqual(df1.schema, df2.schema)  # passes — data differs, schema matches
```

✅ **Expected outcome**:
- You can explain that `checkRowOrder=False` is the **default**
- You can explain when `rtol` is necessary (float arithmetic drift)
- You know `assertSchemaEqual` ignores row data entirely

⚠️ **Exam trap**: Assuming `assertDataFrameEqual` ignores column nullability differences. Wrong! A schema mismatch on nullability can still raise an `AssertionError` even when the data values are identical.

---

## Task 7 — Library Management Across Scopes

📖 **Context**: The exam tests where a library installation is visible — notebook session, cluster, init script, or job — and the side effect of `%pip install`.

🛠️ **Instructions**:

### Step 1 — Notebook-scoped install:

```python
%pip install great-expectations==0.18.0 pandas==2.0.0
# NOTE: this restarts the Python interpreter — all variables defined
# above this cell are cleared and must be re-run
```

### Step 2 — Install from a private wheel stored in a Unity Catalog Volume:

```python
%pip install /Volumes/training/prep/landing/libs/my_package-1.0.0-py3-none-any.whl
```

### Step 3 — Declare dependencies for a DABs job (`requirements.txt` referenced from the bundle):

```yaml
# resources/jobs.yml
tasks:
  - task_key: transform
    notebook_task:
      notebook_path: ../notebooks/02_transform.py
    libraries:
      - requirements: /Volumes/training/prep/landing/requirements.txt
```

### Step 4 — Verify what's installed:

```python
import pkg_resources
print([p.project_name for p in pkg_resources.working_set])
```

✅ **Expected outcome**:
- You can map each scope (Notebook / Cluster / Init script / Job) to its persistence lifetime
- You can explain the interpreter-restart side effect of `%pip install`

⚠️ **Exam trap**: Running `%pip install` in the middle of a notebook and then referencing a variable defined in an earlier cell without re-running it. The interpreter restart clears all Python state — cells above the `%pip install` must be re-executed.

---

## Task 8 — Concept Quiz

Answer these rapid-fire questions:

1. Where should reusable transformation logic live in a DABs project — `notebooks/` or `src/<package>/`?
2. What is the difference between `@udf` and `spark.udf.register()`?
3. Why are Pandas UDFs faster than regular Python UDFs?
4. What serialization format do Pandas UDFs rely on?
5. What is the modern replacement for `@pandas_udf(schema, PandasUDFType.GROUPED_MAP)`?
6. What's the difference between `applyInPandas` and `mapInPandas`?
7. Why should the `spark` pytest fixture be `scope="session"` rather than the default?
8. Does `assertDataFrameEqual` check row order by default?
9. What does the `rtol` parameter of `assertDataFrameEqual` control?
10. What happens to your notebook's Python variables when you run `%pip install`?

---

## Key Takeaways for the Exam

✅ **Project Structure**
- Business logic → `src/<package>/`; tests → `tests/`; notebooks stay thin (import + orchestrate)
- `pyproject.toml` defines packaging; `databricks.yml` + `resources/` define DABs deployment

✅ **UDFs**
- Regular UDF: row-by-row, pickle-serialized, slowest — needs `spark.udf.register()` separately for SQL use
- Scalar Pandas UDF: `pd.Series -> pd.Series`, Arrow-serialized, fast, vectorizable operations
- Grouped Map: `df.groupBy(...).applyInPandas(func, schema)` — current API, receives/returns a full `pandas.DataFrame` per group
- `mapInPandas`: per-partition, no grouping required
- Always prefer built-in Spark functions over any UDF type when available

✅ **Testing**
- Use a `scope="session"` SparkSession fixture for speed
- `assertDataFrameEqual`: `checkRowOrder=False` by default, use `rtol` for float tolerance
- `assertSchemaEqual`: schema only, still fails on nullability mismatches
- Unit tests run locally (`master("local[1]")`) — no cluster required

✅ **Library Management**
- `%pip install` = notebook-scoped, restarts the Python interpreter (clears variables)
- Cluster libraries / init scripts persist for the cluster's lifetime
- Job-level `libraries:` in a DABs job config persist only for that job run

---

## Next Steps

You've completed Day 6! You now understand Python project structure, UDFs, and testing at a professional level. Tomorrow (Day 7), you'll cover Databricks Asset Bundles (DABs) and CI/CD.
