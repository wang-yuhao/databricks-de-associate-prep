# Day 6 — Python Project Structure, UDFs & Testing (~22% of exam)

## Schedule
- **Morning (2h):** Read all sections below
- **Mid-morning (2h):** Work through `notebooks/pro_day6_testing_udfs.py`
- **Afternoon (2h):** Complete `practice-tasks.md`
- **Evening (1h):** Review testing & UDF questions

---

## 6.1 Python Project Structure for Databricks

A well-structured Python project for DABs (Databricks Asset Bundles):

```
my_project/
├── databricks.yml              # DABs config
├── pyproject.toml             # Python packaging
├── requirements.txt
├── src/
│   └── my_project/
│       ├── __init__.py
│       ├── transformations.py    # Business logic
│       ├── utils.py              # Helper functions
│       └── config.py             # Configuration
├── tests/
│   ├── __init__.py
│   ├── test_transformations.py
│   └── test_utils.py
├── notebooks/
│   ├── 01_ingest.py
│   └── 02_transform.py
└── resources/
    └── jobs.yml               # Job definitions
```

---

## 6.2 Python UDFs

### Regular Python UDF
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define UDF
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

# Register for SQL use
spark.udf.register("classify_amount", classify_amount)

# Use in DataFrame API
df.withColumn("size_class", classify_amount("amount"))

# Use in SQL
spark.sql("SELECT classify_amount(amount) FROM training.prep.orders")
```

### Pandas UDF (Vectorized) — Preferred for Performance
```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import FloatType
import pandas as pd

# Scalar Pandas UDF: column-in, column-out
@pandas_udf(FloatType())
def normalize_amount(amounts: pd.Series) -> pd.Series:
    return (amounts - amounts.mean()) / amounts.std()

# Apply
df.withColumn("normalized_amount", normalize_amount("amount"))

# Grouped Map Pandas UDF: apply function per group
from pyspark.sql.functions import PandasUDFType

schema = "order_id BIGINT, customer STRING, amount DOUBLE, rank INT"

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def add_rank(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["rank"] = pdf["amount"].rank(ascending=False).astype(int)
    return pdf

df.groupBy("region").apply(add_rank)
```

### UDF Performance Comparison
| Type | Serialization | Performance | Use Case |
|------|--------------|-------------|----------|
| Python UDF | Row-by-row (pickle) | Slowest | Simple logic, rare use |
| Pandas UDF (Scalar) | Columnar (Arrow) | Fast | Vectorizable operations |
| Pandas UDF (Grouped Map) | Group-level (Arrow) | Fast | Per-group transformations |
| Spark built-in | Native JVM | Fastest | Always prefer when available |

---

## 6.3 Unit Testing with pytest

### Testing Transformations (DataFrame.transform)
```python
# src/my_project/transformations.py
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

# Chainable with .transform()
result = (
    raw_df
    .transform(add_total_with_tax, tax_rate=0.19)  # Germany VAT
    .transform(filter_valid_orders)
)
```

### Test File
```python
# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

from my_project.transformations import add_total_with_tax, filter_valid_orders

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()

def test_add_total_with_tax(spark):
    # Arrange
    input_data = [(1, "Alice", 100.0), (2, "Bob", 200.0)]
    schema = "order_id BIGINT, customer STRING, amount DOUBLE"
    input_df = spark.createDataFrame(input_data, schema)

    # Act
    result_df = add_total_with_tax(input_df, tax_rate=0.2)

    # Assert
    expected_data = [(1, "Alice", 100.0, 120.0), (2, "Bob", 200.0, 240.0)]
    expected_schema = "order_id BIGINT, customer STRING, amount DOUBLE, total_with_tax DOUBLE"
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assertDataFrameEqual(result_df, expected_df)

def test_filter_valid_orders_removes_nulls(spark):
    input_data = [
        (1, "Alice", 100.0),
        (None, "Bob", 50.0),   # null order_id
        (3, None, 75.0),       # null customer
        (4, "Dave", -10.0),    # negative amount
    ]
    schema = "order_id BIGINT, customer STRING, amount DOUBLE"
    input_df = spark.createDataFrame(input_data, schema)

    result_df = filter_valid_orders(input_df)

    assert result_df.count() == 1
    assert result_df.collect()[0]["order_id"] == 1

def test_schema_preserved(spark):
    input_df = spark.createDataFrame([(1, "Alice", 100.0)], "order_id BIGINT, customer STRING, amount DOUBLE")
    result_df = filter_valid_orders(input_df)
    assertSchemaEqual(result_df.schema, input_df.schema)
```

---

## 6.4 assertDataFrameEqual & assertSchemaEqual

```python
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

# assertDataFrameEqual: checks rows + schema
# Parameters:
# - actual: result DataFrame
# - expected: expected DataFrame
# - checkRowOrder: bool (default False) - order doesn't matter
# - rtol: float - relative tolerance for floats
assertDataFrameEqual(actual_df, expected_df, checkRowOrder=False, rtol=1e-5)

# assertSchemaEqual: checks schema only
assertSchemaEqual(actual_df.schema, expected_df.schema)
# Raises AssertionError with diff if schemas differ
```

---

## 6.5 Library Management

```python
# Install libraries on cluster (init script or cluster policy)
# %pip install in notebook (restarts Python interpreter)
%pip install great-expectations==0.18.0 pandas==2.0.0

# Install from private wheel
%pip install /Volumes/training/prep/landing/libs/my_package-1.0.0-py3-none-any.whl

# requirements.txt approach (for DABs)
# pip install -r /Volumes/training/prep/landing/requirements.txt

# Check installed packages
import pkg_resources
print([p.project_name for p in pkg_resources.working_set])
```

### Library Scopes
| Scope | Method | Persistence |
|-------|--------|-------------|
| Notebook | `%pip install` | Notebook session only |
| Cluster | Libraries UI / cluster policy | Cluster lifetime |
| Init script | Bash script on cluster start | Cluster lifetime |
| Job | `libraries` in job config | Job run only |

---

## Key Exam Points ✔️

- **Pandas UDFs** (vectorized) use Apache Arrow for serialization — much faster than row-by-row Python UDFs
- Always prefer **built-in Spark functions** over UDFs when possible
- `DataFrame.transform(func)` enables clean function chaining
- `assertDataFrameEqual` checks both data and schema; `checkRowOrder=False` ignores row order
- `assertSchemaEqual` only checks schema structure
- Register UDFs with `spark.udf.register(name, func)` to use in SQL
- `%pip install` in a notebook restarts the Python interpreter
- Pandas UDF requires Apache Arrow to be enabled (default in modern Databricks runtimes)
