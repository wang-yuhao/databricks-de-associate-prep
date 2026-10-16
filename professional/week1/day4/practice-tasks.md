# Day 4 Practice Tasks — Delta Live Tables CDC (Change Data Capture)

> **Exam section:** Data Pipelines (40%), Data Transformation, Cleansing, and Quality (10%)
> **Prerequisite:** Read `study-notes.md` and complete Day 3 practice tasks.
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

## Task 1 — CDC with APPLY CHANGES INTO (SCD Type 1)

📖 **Context**: The Professional exam heavily tests CDC patterns using `dlt.apply_changes()`. You must understand how SCD Type 1 maintains only current state and how `sequence_by` handles out-of-order events.

🛠️ **Instructions**:

### Step 1 — Create a bronze CDC source table:

```python
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="bronze_customer_cdc",
    comment="Raw CDC events from upstream database",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_customer_cdc():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/mnt/cdc/customer_schema")
            .load("/mnt/cdc/customers/")
            .select(
                col("customer_id").cast("int"),
                col("name").cast("string"),
                col("email").cast("string"),
                col("phone").cast("string"),
                col("address").cast("string"),
                col("city").cast("string"),
                col("state").cast("string"),
                col("updated_timestamp").cast("timestamp"),
                col("_change_type").cast("string")  # insert, update, delete
            )
    )
```

### Step 2 — Apply CDC with SCD Type 1 (current state only):

```python
# Create the target table first
dlt.create_streaming_table(
    name="silver_customers_current",
    comment="Current customer records (SCD Type 1)",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)

# Apply changes to target table
dlt.apply_changes(
    target="silver_customers_current",
    source="bronze_customer_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_timestamp"),
    except_column_list=["_rescued_data", "_change_type"],
    stored_as_scd_type="1"
)
```

### Step 3 — Verify SCD Type 1 behavior:

```python
# Query the silver table
@dlt.table(
    name="customer_count_verification",
    comment="Verification query for SCD Type 1"
)
def customer_count_verification():
    return (
        dlt.read("silver_customers_current")
            .groupBy("state")
            .agg(
                count("customer_id").alias("customer_count"),
                max("updated_timestamp").alias("latest_update")
            )
    )
```

✅ **Expected outcome**: 
- Only current (latest) version of each customer is stored
- Updates overwrite previous records based on `customer_id`
- `sequence_by` ensures late-arriving events don't overwrite newer data
- Deletes are handled based on `_change_type` field

⚠️ **Exam trap**: 
- SCD Type 1 does NOT keep history — only current state
- Without `sequence_by`, late-arriving older updates could overwrite newer data
- `keys` parameter is REQUIRED — specifies the unique identifier
- `except_column_list` removes metadata columns from target

---

## Task 2 — CDC with APPLY CHANGES INTO (SCD Type 2)

📖 **Context**: SCD Type 2 maintains full history with temporal columns. The exam tests your understanding of `__START_AT`, `__END_AT`, and `__CURRENT` columns that DLT automatically creates.

🛠️ **Instructions**:

### Step 1 — Apply CDC with SCD Type 2 (full history):

```python
# Create target table for historical tracking
dlt.create_streaming_table(
    name="silver_customers_history",
    comment="Complete customer history (SCD Type 2)",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)

# Apply changes with SCD Type 2
dlt.apply_changes(
    target="silver_customers_history",
    source="bronze_customer_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_timestamp"),
    stored_as_scd_type="2",
    track_history_column_list=["email", "phone", "address", "city", "state"],
    track_history_except_column_list=[]
)
```

### Step 2 — Query historical data with temporal columns:

```python
@dlt.table(
    name="customer_history_analysis",
    comment="Analysis of customer changes over time"
)
def customer_history_analysis():
    return (
        dlt.read("silver_customers_history")
            .filter(col("__CURRENT") == True)
            .select(
                col("customer_id"),
                col("name"),
                col("email"),
                col("__START_AT").alias("valid_from"),
                col("__END_AT").alias("valid_to"),
                col("__CURRENT").alias("is_current")
            )
    )
```

### Step 3 — Analyze change frequency:

```python
@dlt.table(
    name="customer_change_frequency",
    comment="How often customers update their information"
)
def customer_change_frequency():
    return (
        dlt.read("silver_customers_history")
            .groupBy("customer_id")
            .agg(
                count("*").alias("total_versions"),
                min("__START_AT").alias("first_seen"),
                max("__START_AT").alias("last_updated"),
                sum(when(col("__CURRENT") == True, 1).otherwise(0)).alias("current_count")
            )
            .filter(col("total_versions") > 1)
    )
```

✅ **Expected outcome**: 
- Each change creates a new row with temporal tracking
- `__START_AT`: When this version became active
- `__END_AT`: When this version was superseded (NULL for current)
- `__CURRENT`: Boolean flag for latest version
- Full audit trail of all changes

⚠️ **Exam trap**: 
- SCD Type 2 creates MULTIPLE rows per customer (one per change)
- `__CURRENT = True` identifies the latest version
- `track_history_column_list` specifies which columns trigger new versions
- `track_history_except_column_list` excludes columns from versioning
- Temporal columns are added AUTOMATICALLY by DLT

---

## Task 3 — CDC with Delete Operations

📖 **Context**: Handling deletes in CDC is tricky. The exam tests whether you understand how `apply_changes()` processes delete events differently in Type 1 vs Type 2.

🛠️ **Instructions**:

### Step 1 — Configure CDC source with delete handling:

```python
@dlt.table(
    name="bronze_product_cdc",
    comment="Product CDC with INSERT, UPDATE, DELETE events"
)
def bronze_product_cdc():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/mnt/cdc/product_schema")
            .load("/mnt/cdc/products/")
            .select(
                col("product_id").cast("int"),
                col("product_name").cast("string"),
                col("category").cast("string"),
                col("price").cast("decimal(10,2)"),
                col("updated_timestamp").cast("timestamp"),
                col("_change_type").cast("string")  # insert, update, delete
            )
    )
```

### Step 2 — Apply changes with delete handling (SCD Type 1):

```python
dlt.create_streaming_table("silver_products_current")

dlt.apply_changes(
    target="silver_products_current",
    source="bronze_product_cdc",
    keys=["product_id"],
    sequence_by=col("updated_timestamp"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type"],
    stored_as_scd_type="1"
)
```

### Step 3 — Apply changes with delete handling (SCD Type 2):

```python
dlt.create_streaming_table("silver_products_history")

dlt.apply_changes(
    target="silver_products_history",
    source="bronze_product_cdc",
    keys=["product_id"],
    sequence_by=col("updated_timestamp"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type"],
    stored_as_scd_type="2"
)
```

### Step 4 — Query deleted products:

```python
@dlt.table(
    name="deleted_products_audit",
    comment="Audit trail of deleted products"
)
def deleted_products_audit():
    return (
        dlt.read("silver_products_history")
            .filter(col("__END_AT").isNotNull() & (col("__CURRENT") == False))
            .select(
                col("product_id"),
                col("product_name"),
                col("category"),
                col("__START_AT").alias("active_from"),
                col("__END_AT").alias("deleted_at")
            )
    )
```

✅ **Expected outcome**: 
- Type 1 deletes: Row is physically removed from table
- Type 2 deletes: `__END_AT` is set, `__CURRENT = False`
- Deleted records remain in Type 2 for audit purposes
- `apply_as_deletes` condition determines which rows are deletes

⚠️ **Exam trap**: 
- SCD Type 1: Deletes REMOVE rows (no history)
- SCD Type 2: Deletes PRESERVE rows with `__CURRENT = False`
- `apply_as_deletes` must be an expression, not a column name
- Without `apply_as_deletes`, delete events are treated as updates
- For audit compliance, always use SCD Type 2!

---

## Task 4 — Advanced CDC: Multiple Keys and Ignore Null Updates

📖 **Context**: Real-world CDC often involves composite keys and handling NULL values. The exam may test edge cases like multi-column keys and `ignore_null_updates`.

🛠️ **Instructions**:

### Step 1 — CDC with composite keys:

```python
@dlt.table(
    name="bronze_order_line_cdc",
    comment="Order line items with composite key"
)
def bronze_order_line_cdc():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/mnt/cdc/order_line_schema")
            .load("/mnt/cdc/order_lines/")
            .select(
                col("order_id").cast("int"),
                col("line_number").cast("int"),
                col("product_id").cast("int"),
                col("quantity").cast("int"),
                col("unit_price").cast("decimal(10,2)"),
                col("discount").cast("decimal(5,2)"),
                col("updated_timestamp").cast("timestamp")
            )
    )
```

### Step 2 — Apply changes with composite key:

```python
dlt.create_streaming_table("silver_order_lines")

dlt.apply_changes(
    target="silver_order_lines",
    source="bronze_order_line_cdc",
    keys=["order_id", "line_number"],  # Composite key
    sequence_by=col("updated_timestamp"),
    ignore_null_updates=True,
    stored_as_scd_type="1"
)
```

### Step 3 — Test null handling:

```python
# Example: If CDC sends an update with NULL discount,
# ignore_null_updates=True will keep the existing discount value

@dlt.table(
    name="order_line_validation",
    comment="Validate no NULL values in critical fields"
)
def order_line_validation():
    return (
        dlt.read("silver_order_lines")
            .select(
                col("order_id"),
                col("line_number"),
                col("product_id"),
                col("quantity"),
                when(col("discount").isNull(), lit("NULL_DISCOUNT"))
                    .otherwise(col("discount").cast("string"))
                    .alias("discount_status")
            )
    )
```

✅ **Expected outcome**: 
- Composite keys work with list: `keys=["col1", "col2"]`
- `ignore_null_updates=True` prevents NULLs from overwriting existing values
- Useful when CDC system sends partial updates
- NULL in key columns is never allowed

⚠️ **Exam trap**: 
- `keys` can have multiple columns for composite keys
- NULL in key columns causes pipeline failure
- `ignore_null_updates` applies to ALL columns, not selective
- Without `ignore_null_updates`, NULLs overwrite existing values
- Default behavior: `ignore_null_updates=False`

---

## Task 5 — Concept Quiz

Answer these rapid-fire questions:

1. What is the key difference between SCD Type 1 and Type 2 in DLT?
   - A) Type 1 is faster
   - B) Type 1 keeps current state only, Type 2 keeps full history ✓
   - C) Type 1 uses MERGE, Type 2 uses INSERT
   - D) Type 1 cannot handle deletes

2. What does `sequence_by` do in `apply_changes()`?
   - A) Orders the output table
   - B) Prevents out-of-order events from overwriting newer data ✓
   - C) Creates a sequence number column
   - D) Partitions the target table

3. In SCD Type 2, what does `__CURRENT = True` indicate?
   - A) The row is being updated
   - B) The row is the latest version of the record ✓
   - C) The row has been validated
   - D) The row is a current year record

4. How are deletes handled in SCD Type 1 vs Type 2?
   - A) Same in both types
   - B) Type 1 removes rows, Type 2 sets __END_AT and __CURRENT=False ✓
   - C) Type 2 removes rows, Type 1 keeps them
   - D) Neither type supports deletes

5. What happens if `sequence_by` column has NULL values?
   - A) Row is ignored
   - B) Pipeline fails ✓
   - C) NULL is treated as oldest timestamp
   - D) NULL is treated as current timestamp

---

## Key Takeaways for the Exam

✅ **SCD Type 1 vs Type 2:**
- **Type 1** = Current state only (updates overwrite)
- **Type 2** = Full history with temporal columns
- Choose based on audit requirements

✅ **sequence_by is Critical:**
- Required to handle out-of-order events
- Must be a timestamp or monotonically increasing column
- NULL values cause pipeline failure
- Without it, late updates can corrupt data

✅ **SCD Type 2 Temporal Columns:**
- `__START_AT`: When version became active
- `__END_AT`: When version was superseded (NULL = current)
- `__CURRENT`: Boolean for latest version
- Created AUTOMATICALLY by DLT

✅ **Delete Handling:**
- `apply_as_deletes`: Expression to identify delete events
- Type 1: Physical deletion
- Type 2: Logical deletion (`__CURRENT = False`)
- Always use Type 2 for compliance/audit

✅ **Advanced Features:**
- Composite keys: `keys=["col1", "col2"]`
- `ignore_null_updates`: Prevents NULLs from overwriting
- `except_column_list`: Removes metadata columns
- `track_history_column_list`: Specifies version-triggering columns (Type 2)

---

## Next Steps

You've completed Day 4! You now understand CDC patterns with Delta Live Tables at a professional level. Tomorrow (Day 5), you'll cover Advanced Structured Streaming including windowing, watermarks, stateful transformations, and stream-stream joins.
