# Databricks Certified Data Engineer Professional
# Mock Exam 2 — Monitoring, Performance, Security & Governance

> **Exam reference:** July 2026 Exam Guide (30 Nov 2025 version)  
> **Topics covered:** Section 5 (10%), Section 6 (13%), Section 7 (10%), Section 8 (7%)  
> **Questions:** 20 multiple-choice | **Time:** ~40 min

---

## Section 5 — Monitoring and Alerting — 10%

### Key Knowledge Points
- **DLT Event Log:** Source of truth for pipeline health. Queryable via `event_log()` table-valued function. Events include `flow_progress`, `planning_information`, `maintenance_progress`, `user_action`.
- **Lakeflow Jobs monitoring:** Job run history, task-level metrics, repair runs, email/webhook alerts on failure/success.
- **Databricks SQL Alerts:** SQL query-based alerts with configurable conditions and notification destinations (email, Slack, PagerDuty via webhooks).
- **Ganglia / Spark UI:** Cluster-level metrics (CPU, memory, shuffle, GC). Access via cluster detail > Spark UI.
- **Structured Streaming metrics:** `StreamingQueryListener`, `streamingQuery.lastProgress`, `streamingQuery.status`.
- **Audit Logs:** System table `system.access.audit` for tracking all workspace actions.
- **System Tables:** `system.billing.usage`, `system.compute.clusters`, `system.access.audit` in Unity Catalog.

---

### Q1
A data engineer needs to monitor a Structured Streaming query and receive alerts when the **processing rate falls below 1000 records/second**. Which approach is BEST?

**A.** Set up a Databricks SQL Alert on the `system.billing.usage` table  
**B.** Use `streamingQuery.lastProgress` to read `processedRowsPerSecond` in a companion job, then trigger an alert via Databricks SQL Alert  
**C.** Check the Ganglia UI manually every hour  
**D.** Configure a `trigger(processingTime='1 second')` and check the notebook output  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
`streamingQuery.lastProgress` returns a dictionary of recent metrics including `processedRowsPerSecond`, `inputRowsPerSecond`, `durationMs`, and batch details. By writing these metrics to a Delta monitoring table and creating a Databricks SQL Alert, you get automated notifications.

```python
import time
from pyspark.sql import Row

while query.isActive:
    progress = query.lastProgress
    if progress:
        rate = progress.get("processedRowsPerSecond", 0)
        # Write to monitoring Delta table
        monitoring_df = spark.createDataFrame([Row(
            query_name=progress["name"],
            batch_id=progress["batchId"],
            rows_per_second=rate,
            ts=progress["timestamp"]
        )])
        monitoring_df.write.mode("append").saveAsTable("monitoring.stream_metrics")
    time.sleep(60)
```

Alternatively, implement a `StreamingQueryListener` for event-driven monitoring without polling.
</details>

---

### Q2
A data engineer wants to query the DLT pipeline event log to find records dropped by expectations in the last 24 hours. Which system function returns the event log?

**A.** `SELECT * FROM system.dlt.events WHERE ...`  
**B.** `SELECT * FROM event_log(TABLE(catalog.schema.pipeline_table)) WHERE ...`  
**C.** `SELECT * FROM delta.`/pipelines/<id>/events` WHERE ...`  
**D.** `DESCRIBE EXTENDED pipeline_table`  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
DLT exposes its event log via the `event_log()` **table-valued function** (TVF), passing any table from the pipeline:

```sql
SELECT
  timestamp,
  details:flow_progress:data_quality:dropped_records AS dropped,
  details:flow_progress:name AS flow_name
FROM event_log(TABLE(main.silver.orders))
WHERE event_type = 'flow_progress'
  AND timestamp > current_timestamp() - INTERVAL 24 HOURS
  AND details:flow_progress:data_quality:dropped_records > 0
ORDER BY timestamp DESC;
```

Event types to know:
| Event Type | Description |
|---|---|
| `flow_progress` | Per-batch metrics including data quality |
| `planning_information` | Pipeline graph / dependency resolution |
| `maintenance_progress` | OPTIMIZE / VACUUM runs on DLT tables |
| `user_action` | Manual pipeline start/stop events |
</details>

---

### Q3
An organization needs to audit **who deleted rows** from a Unity Catalog Delta table. Where is this information available?

**A.** `DESCRIBE HISTORY table_name`  
**B.** `system.access.audit` system table  
**C.** `information_schema.table_changes`  
**D.** Delta Lake transaction log (`_delta_log`)  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B** (also partially A)

**Explanation:**  
- **`DESCRIBE HISTORY`** shows the Delta transaction log history including the operation type (`DELETE`), `operationParameters`, and `userMetadata` — but not the authenticated user's identity in all cases.
- **`system.access.audit`** is the Unity Catalog system table that captures **authenticated user identity, action type, resource, timestamp** for all workspace operations including data mutations. This is the authoritative audit source for security and compliance.

For the exam: `system.access.audit` is the answer for **user identity + action auditing**. `DESCRIBE HISTORY` is for **data change auditing** (what changed, when, with what operation).

```sql
SELECT userIdentity, actionName, requestParams, response
FROM system.access.audit
WHERE actionName = 'deltaSharingSendFile'
  AND date > current_date() - 7
ORDER BY eventTime DESC;
```
</details>

---

### Q4
A Databricks SQL Alert is configured to fire when a query returns **any rows**. The underlying query counts late-arriving records:
```sql
SELECT COUNT(*) as late_count FROM orders WHERE arrival_delay_minutes > 30 AND order_date = current_date()
```
The alert never fires even though late records exist. What is the most likely cause?

**A.** Databricks SQL Alerts cannot use `COUNT(*)` queries  
**B.** The alert condition should be `"Value > 0"` on the `late_count` column, not `"Has any rows"`  
**C.** The query only runs once and needs a `REFRESH` command  
**D.** SQL Alerts are only supported for streaming queries  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
A `COUNT(*)` query **always returns one row** (the count value). So the alert condition `"Has any rows"` is always true — even when the count is 0. The correct configuration is:
- Column: `late_count`
- Condition: `Value > 0`

Databricks SQL Alerts evaluate against a **specific column and threshold value**. Use `"Value >"`, `"Value ="`, `"Value <"`, or `"Is not null"` conditions against numeric/string columns. The alert fires when the condition becomes true and can be configured to notify once, always, or on state change.
</details>

---

### Q5
Which system table tracks **Databricks compute costs per workspace** and is useful for FinOps monitoring?

**A.** `system.compute.clusters`  
**B.** `system.billing.usage`  
**C.** `system.access.audit`  
**D.** `system.lakeflow.jobs`  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

`system.billing.usage` contains DBU consumption records per workspace, cluster, SKU, and job. Use this with `system.billing.list_prices` to calculate cost. Available to account admins and workspace admins. Query patterns:

```sql
SELECT 
  workspace_id,
  cluster_id,
  sku_name,
  SUM(dbus) AS total_dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - 30
GROUP BY 1, 2, 3
ORDER BY total_dbus DESC;
```
</details>

---

## Section 6 — Cost & Performance Optimisation — 13%

### Key Knowledge Points
- **Z-Ordering:** Co-locates related data in the same set of files. Best for high-cardinality columns used in filters. `OPTIMIZE table ZORDER BY (col1, col2)`.
- **Liquid Clustering:** Replaces Z-Order + partitioning. Incremental, no full-table rewrite. `CLUSTER BY (col1, col2)`. Use `OPTIMIZE` to apply clustering.
- **Auto Optimize:** Optimized Writes (merge small files during write) + Auto Compaction (async background compaction).
- **Caching:** Delta Cache (SSD-based, automatic), Spark Cache (`df.cache()`, memory-based, explicit).
- **Photon:** Vectorized query engine for Delta Lake. Replaces Spark's default JVM row-based execution with C++ vectorized execution.
- **Serverless compute:** No cluster management, instant startup, automatic scaling. Priced per DBU consumed (not idle time).
- **Partitioning best practices:** Low-cardinality columns only (date, region). High-cardinality partitioning causes small files and partition pruning overhead.
- **Broadcast joins:** `spark.sql.autoBroadcastJoinThreshold` (default 10MB). Force with `broadcast()` hint.
- **Adaptive Query Execution (AQE):** Dynamically optimizes join strategies, skew handling, and partition coalescing. Enabled by default in Spark 3.x.

---

### Q6
A Delta table is queried frequently with filters on `user_id` (50 million unique values) and `event_date`. Currently no optimization is applied. A data engineer wants to optimize query performance. What is the RECOMMENDED approach in 2026?

**A.** Partition by `user_id` and Z-Order by `event_date`  
**B.** Partition by `event_date` and Z-Order by `user_id`  
**C.** Use Liquid Clustering on `(user_id, event_date)` with `CLUSTER BY`  
**D.** Partition by both `user_id` and `event_date`  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: C**

**Explanation:**  
**Liquid Clustering** is Databricks' recommended approach as of 2024+ and replaces both traditional partitioning and Z-Ordering. Reasons:

- **Option A** — Partitioning by `user_id` with 50M values creates 50M partitions = catastrophic small-file problem.
- **Option B** — Partitioning by `event_date` works but Z-Order by `user_id` still causes full-table rewrites.
- **Option D** — Multi-column high-cardinality partitioning creates exponentially more small files.
- **Option C** — Liquid Clustering efficiently clusters data by both columns **incrementally**, handles mixed access patterns, and avoids partition management overhead.

```sql
-- Create table with Liquid Clustering
CREATE TABLE events
CLUSTER BY (user_id, event_date)
AS SELECT * FROM raw_events;

-- Apply clustering incrementally (only unclustered files)
OPTIMIZE events;
```

**When to use what:**
| Method | Use when |
|---|---|
| Partitioning | Low-cardinality + often filter on a single column (e.g., `date`) |
| Z-Order | Medium cardinality, classic approach (being superseded) |
| Liquid Clustering | Default choice for new tables in 2026 |
</details>

---

### Q7
A Spark job joins a 500GB fact table with a 5MB dimension table. The job runs slowly due to shuffle. What optimization would MOST improve performance?

**A.** Increase the number of shuffle partitions with `spark.sql.shuffle.partitions = 2000`  
**B.** Use a broadcast join by adding `/*+ BROADCAST(dim) */` hint or rely on AQE auto-broadcast  
**C.** Repartition the fact table before the join with `fact.repartition(500)`  
**D.** Use `sortMergeJoin` explicitly  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
When one side of a join is small (< `autoBroadcastJoinThreshold`, default 10MB), Spark should automatically **broadcast** it to all executors, eliminating the shuffle entirely. For a 5MB dimension table, broadcasting is ideal.

If AQE doesn't auto-broadcast (e.g., statistics not available), use the hint:
```sql
SELECT /*+ BROADCAST(d) */ f.*, d.name
FROM fact f
JOIN dim d ON f.dim_id = d.id;
```
or in Python:
```python
from pyspark.sql.functions import broadcast
result = fact.join(broadcast(dim), "dim_id")
```

**AQE (Adaptive Query Execution)** — enabled by default in Databricks Runtime 8+:
- `spark.sql.adaptive.enabled = true`
- `spark.sql.adaptive.autoBroadcastJoinThreshold` — runtime broadcast threshold
- `spark.sql.adaptive.skewJoin.enabled` — handles data skew automatically
</details>

---

### Q8
A Databricks SQL query on a Delta table consistently takes 10 minutes despite Z-Ordering. Spark UI shows most time is spent on file listing. What is the most likely root cause?

**A.** Z-Order index is corrupted and needs to be rebuilt  
**B.** The table has too many small files due to frequent streaming appends without compaction  
**C.** Delta Cache is disabled on the SQL Warehouse  
**D.** The cluster is under-provisioned with too few cores  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
Frequent streaming writes create many small files. **File listing overhead** is a classic symptom of the **small files problem** — the query planner spends excessive time discovering and reading file metadata even before processing data.

**Solutions:**
1. Enable **Auto Optimize** on the table:
```sql
ALTER TABLE events
SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                   'delta.autoOptimize.autoCompact' = 'true');
```
2. Schedule periodic `OPTIMIZE` runs:
```sql
OPTIMIZE events;
```
3. Switch to **Liquid Clustering** which manages compaction more efficiently.
4. Check `DESCRIBE DETAIL table` — the `numFiles` field shows current file count; target < 1GB per file.
</details>

---

### Q9
A data engineer wants to understand why a query using `MERGE INTO` is slow. Which tool provides the MOST detail about the physical execution plan?

**A.** `EXPLAIN FORMATTED` in SQL  
**B.** Databricks Query Profile in SQL Editor (graphical plan with per-node metrics)  
**C.** `DESCRIBE HISTORY` on the target table  
**D.** `spark.sparkContext.statusTracker.getJobIdsForGroup()`  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
The **Databricks SQL Query Profile** (available in the SQL Editor history) provides a **graphical representation** of the query physical plan with actual row counts, time per operator, and memory usage — far more actionable than `EXPLAIN` output.

Key things to look for in the Query Profile for `MERGE INTO`:
- Which scan reads more rows than expected (missing file pruning)
- Shuffle operations and their sizes
- `BroadcastHashJoin` vs `SortMergeJoin`
- AQE optimizations applied

`EXPLAIN FORMATTED` shows the logical and physical plan as text but lacks runtime metrics.
</details>

---

### Q10
What is the PRIMARY advantage of **serverless compute** over classic interactive clusters for SQL workloads?

**A.** Serverless computes runs faster due to larger memory allocation  
**B.** Serverless eliminates cluster startup time and charges only for query execution time, not idle time  
**C.** Serverless supports ML workloads better than standard clusters  
**D.** Serverless is required for Unity Catalog access  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
Serverless compute (SQL Warehouses) key characteristics:
- **Instant start** — no cluster initialization delay (< 5 seconds vs 2-5 minutes)
- **Per-query billing** — billed for DBUs consumed during query execution only, not idle time
- **Fully managed** — no cluster configuration, auto-scaling is transparent
- **Photon-enabled by default**

For cost optimization: serverless is typically more cost-effective for **bursty, intermittent** workloads. Classic clusters are more cost-effective for **sustained, high-throughput** workloads due to lower per-DBU pricing.

**Serverless SQL Warehouse tiers:** 2X-Small → 4X-Large. Each size doubles the number of concurrent queries and processing capacity.
</details>

---

## Section 7 — Ensuring Data Security and Compliance — 10%

### Key Knowledge Points
- **Unity Catalog privilege hierarchy:** Metastore > Catalog > Schema > Table/View/Function/Volume
- **Privilege types:** `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`, `CREATE TABLE`, `ALL PRIVILEGES`
- **Column masking:** `ALTER TABLE ... ALTER COLUMN ... SET MASK function_name`
- **Row filters:** `ALTER TABLE ... SET ROW FILTER function_name ON (columns)`
- **Dynamic Views:** Legacy approach for row/column security (pre-Unity Catalog column masking)
- **Network security:** Private Link, IP access lists, VNet injection
- **Encryption:** Customer-Managed Keys (CMK) for workspace encryption
- **Data isolation:** Unity Catalog metastore-level isolation, workspace isolation

---

### Q11
A Unity Catalog table `main.finance.transactions` contains a `credit_card_number` column. Only the last 4 digits should be visible to analysts; full values visible only to auditors. Which Unity Catalog feature implements this?

**A.** Create a view that masks the column and grant SELECT on the view only  
**B.** Use `ALTER TABLE ... ALTER COLUMN credit_card_number SET MASK mask_credit_card USING COLUMNS (current_user())`  
**C.** Apply a row-level security policy filtering on `credit_card_number`  
**D.** Revoke `SELECT` on the table and grant only `SELECT` on specific columns  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
**Column Masking** (introduced in Unity Catalog) applies a masking function at the column level. The mask function can use `current_user()` or `is_member()` to return different values based on the accessor's identity.

```sql
-- Create the masking function
CREATE FUNCTION main.security.mask_credit_card(raw_value STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('auditors') THEN raw_value
  ELSE CONCAT('****-****-****-', RIGHT(raw_value, 4))
END;

-- Apply to column
ALTER TABLE main.finance.transactions
ALTER COLUMN credit_card_number
SET MASK main.security.mask_credit_card;
```

Analysts see: `****-****-****-1234`  
Auditors see: `4532-1234-5678-1234`

**Option A** (dynamic views) is the legacy approach. Column masking is the modern Unity Catalog native solution and is applied transparently on the base table — simpler to manage.
</details>

---

### Q12
A data engineer needs to ensure that each department can only see rows from the `sales` table where `department = current_user_department`. Which Unity Catalog feature is correct?

**A.** Column masking function  
**B.** Row filter function applied with `ALTER TABLE ... SET ROW FILTER`  
**C.** `GRANT SELECT WHERE department = current_group()` syntax  
**D.** Partition-based access control (grant access to specific partitions)  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
**Row Filters** in Unity Catalog allow table-level predicates that are automatically applied to every query on the table based on caller identity.

```sql
-- Create filter function
CREATE FUNCTION main.security.dept_row_filter(dept STRING)
RETURNS BOOLEAN
RETURN is_member('admins') OR dept = session_user();
-- Note: in practice, use a user-attribute mapping table

-- Apply to table
ALTER TABLE main.sales.transactions
SET ROW FILTER main.security.dept_row_filter ON (department);
```

Row filters are **transparent** to end users and applied at the catalog layer — users cannot bypass them even with direct Delta reads (when Unity Catalog governance is enforced).

Difference: **Row Filters** = which rows to show. **Column Masking** = how to display column values.
</details>

---

### Q13
A Databricks workspace must comply with **GDPR right to erasure**. A user requests deletion of all their data from a Delta table. Delta Lake's immutable log means direct file deletion is not possible. What is the correct approach?

**A.** Run `DELETE FROM users WHERE user_id = 'target_id'` and `VACUUM` immediately  
**B.** Run `DELETE FROM users WHERE user_id = 'target_id'`, wait for `delta.deletedFileRetentionDuration` to expire, then run `VACUUM`  
**C.** Delta Lake does not support GDPR compliance  
**D.** Use `RESTORE TABLE` to roll back to a version before the user's data was ingested  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
Delta Lake supports GDPR erasure through a **two-step process**:
1. `DELETE FROM table WHERE user_id = 'target_id'` — marks the data as deleted in the transaction log and writes new data files without the deleted rows
2. `VACUUM table RETAIN 0 HOURS` — physically deletes old files **after** the retention period expires

The retention period (`delta.deletedFileRetentionDuration`, default 7 days) exists to support time travel. For GDPR compliance, you must:
- Reduce retention or wait for it to expire
- Run `VACUUM` (requires `spark.databricks.delta.retentionDurationCheck.enabled = false` to bypass the safety check when using RETAIN 0 HOURS)

**Important:** After `VACUUM`, time travel to pre-deletion versions is impossible. This is the desired outcome for erasure compliance.

```sql
DELETE FROM main.users.profiles WHERE user_id = 'GDPR-REQUEST-123';
-- After retention period:
VACUUM main.users.profiles RETAIN 0 HOURS;
```
</details>

---

### Q14
Which privilege is needed to **read data** from a Unity Catalog table AND traverse the catalog hierarchy?

**A.** Only `SELECT` on the table  
**B.** `SELECT` on the table + `USE SCHEMA` on the parent schema + `USE CATALOG` on the parent catalog  
**C.** `ALL PRIVILEGES` on the catalog  
**D.** `SELECT` on the table + `USAGE` on all parent objects  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
Unity Catalog uses **hierarchical privilege inheritance**. To read from `main.finance.transactions`, a user needs:
1. `USE CATALOG` on `main` — to traverse into the catalog
2. `USE SCHEMA` on `main.finance` — to traverse into the schema
3. `SELECT` on `main.finance.transactions` — to read the table

Without `USE CATALOG` and `USE SCHEMA`, `SELECT` alone is insufficient. Note: `USAGE` was the legacy privilege name; in current Unity Catalog, `USE CATALOG` and `USE SCHEMA` are the correct privilege names.

```sql
GRANT USE CATALOG ON CATALOG main TO `analyst@company.com`;
GRANT USE SCHEMA ON SCHEMA main.finance TO `analyst@company.com`;
GRANT SELECT ON TABLE main.finance.transactions TO `analyst@company.com`;
```
</details>

---

### Q15
A data engineer needs to prevent Databricks cluster nodes from being exposed to the public internet. Which network security feature achieves this?

**A.** IP Access Lists on the workspace  
**B.** VNet Injection (customer-managed VNet) with no public IP  
**C.** Private Link endpoint for the workspace UI  
**D.** Network Security Group (NSG) on the workspace subnet  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
**VNet Injection** (also called customer-managed VNet) deploys Databricks cluster nodes into a **customer-owned VNet** rather than a Databricks-managed VNet. Combined with `No Public IP` (NPIP) configuration, cluster nodes have no public IP addresses and all traffic stays within the private network.

- **IP Access Lists** — restrict **who can connect to the workspace UI/API**, not cluster node exposure
- **Private Link** — ensures the workspace control plane is accessed via private endpoints, not public internet
- **NSG** — a complementary control but doesn't itself remove public IPs from nodes

**Defense-in-depth architecture:**  
VNet Injection + NPIP + Private Link + IP Access Lists + CMK encryption
</details>

---

## Section 8 — Data Governance — 7%

### Key Knowledge Points
- **Unity Catalog hierarchy:** Account > Metastore > Catalog > Schema > Table/View/Volume/Function
- **Data lineage:** Automatic capture for SQL operations and DLT pipelines. View in Catalog Explorer.
- **Tags:** Apply to catalogs, schemas, tables, columns for classification and discoverability.
- **Table ownership:** Owner has implicit ALL PRIVILEGES. Transfer with `ALTER TABLE ... SET OWNER TO`.
- **Volumes:** Unity Catalog-governed unstructured/semi-structured storage (replaces raw ADLS mounts).
- **Managed vs. External tables:** Managed = UC controls lifecycle (DROP TABLE deletes data). External = UC governs metadata only (DROP TABLE keeps data).
- **Metastore:** One per region per account. Assigned to workspaces.

---

### Q16
A data engineer drops a **managed** Unity Catalog table with `DROP TABLE main.finance.transactions`. What happens to the underlying data files in the storage?

**A.** The metadata is removed but data files remain in cloud storage  
**B.** Both metadata and data files are deleted  
**C.** Data files are archived to a recycle bin for 30 days  
**D.** The operation fails because managed tables cannot be dropped  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
**Managed tables** in Unity Catalog are fully controlled by Unity Catalog — when dropped, **both** the table metadata AND the underlying data files in cloud storage are deleted. This is a **destructive, irreversible** operation (unless backups exist).

Contrast with **External tables**: `DROP TABLE` only removes the metadata; data files remain in the external storage location.

| Table Type | `DROP TABLE` behavior |
|---|---|
| Managed | Deletes metadata + data files |
| External | Deletes metadata only; data files remain |

**Best practice:** For important production tables, use external tables or ensure backup procedures are in place before dropping managed tables.
</details>

---

### Q17
A company wants to tag all PII columns across their Unity Catalog with `"pii" = "true"`. Which SQL command applies a tag to a column?

**A.** `ALTER TABLE table_name SET COLUMN TAG (column_name = 'pii:true')`  
**B.** `ALTER TABLE table_name ALTER COLUMN col_name SET TAGS ('pii' = 'true')`  
**C.** `COMMENT ON COLUMN table_name.col_name IS 'pii:true'`  
**D.** `CREATE TAG pii ON TABLE table_name.col_name`  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
Unity Catalog **Tags** can be applied at catalog, schema, table, and column level using SQL:

```sql
-- Column tag
ALTER TABLE main.finance.transactions
ALTER COLUMN credit_card_number
SET TAGS ('pii' = 'true', 'sensitivity' = 'high');

-- Table tag
ALTER TABLE main.finance.transactions
SET TAGS ('domain' = 'finance', 'sla_tier' = 'gold');

-- View tags
SHOW TAGS ON TABLE main.finance.transactions;
```

Tags enable **data discovery** (search by tag in Catalog Explorer), **automated governance policies** (e.g., mask all columns tagged `pii=true`), and **data classification at scale**.
</details>

---

### Q18
A data lineage query shows that table `gold.sales_summary` was produced by `silver.orders` and `silver.customers`. Which DLT/Databricks feature automatically captures this lineage?

**A.** Users must manually document lineage in table comments  
**B.** Unity Catalog automatically captures column-level lineage for SQL operations and DLT pipelines  
**C.** Data lineage is only captured for DLT pipelines, not ad-hoc SQL  
**D.** Lineage requires configuring a separate metadata service  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
Unity Catalog provides **automatic data lineage** capture at both table and column level for:
- All SQL operations (INSERT, MERGE, CREATE TABLE AS SELECT, etc.) run in Databricks
- Declarative Pipelines (DLT) tables
- Python DataFrame operations (table-level lineage)

Lineage is viewable in **Catalog Explorer** — visual graph showing upstream and downstream dependencies. Use the lineage graph to understand impact of schema changes and for regulatory compliance (e.g., GDPR data mapping).

API access:
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
lineage = w.lineage_tracking.table_lineages(table_name="main.gold.sales_summary")
```
</details>

---

### Q19
A team uses Unity Catalog **Volumes** to store raw CSV files. What is a Volume in Unity Catalog?

**A.** A governed storage location for unstructured/semi-structured files managed by Unity Catalog  
**B.** A special table type for storing large binary objects  
**C.** A cluster configuration for high-memory workloads  
**D.** A replicated Delta table across multiple catalogs  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: A**

**Explanation:**  
**Volumes** are Unity Catalog objects that provide governed access to cloud storage for files (not tables). They replace the use of raw ADLS/S3 mounts.

Two types:
- **Managed Volume** — UC manages storage location within the metastore root storage
- **External Volume** — points to a customer-managed external location

```sql
-- Create external volume
CREATE EXTERNAL VOLUME main.raw.landing_zone
LOCATION 'abfss://landing@storageaccount.dfs.core.windows.net/'

-- Read files from volume (use /Volumes/ path)
df = spark.read.csv('/Volumes/main/raw/landing_zone/orders.csv')
```

**Governance on Volumes:** Grant `READ VOLUME` / `WRITE VOLUME` privileges. All access is audited via `system.access.audit`.
</details>

---

### Q20
A new data engineer joins the team. They need to be able to create tables and views in the schema `main.analytics` but NOT modify or drop existing objects. Which grants are needed?

**A.** `GRANT ALL PRIVILEGES ON SCHEMA main.analytics TO engineer`  
**B.** `GRANT USE CATALOG ON CATALOG main TO engineer; GRANT USE SCHEMA, CREATE TABLE, CREATE VIEW ON SCHEMA main.analytics TO engineer`  
**C.** `GRANT MODIFY ON SCHEMA main.analytics TO engineer`  
**D.** `GRANT CREATE ON CATALOG main TO engineer`  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
Least-privilege principle: grant only what is needed.
- `USE CATALOG` — required to traverse into the `main` catalog
- `USE SCHEMA` — required to traverse into `main.analytics`
- `CREATE TABLE` — allows creating new tables in the schema
- `CREATE VIEW` — allows creating new views

**Not needed / wrong:**
- `ALL PRIVILEGES` (Option A) also grants `MODIFY` and `DROP` on all objects — too permissive
- `MODIFY` (Option C) allows editing **existing** table data, not creating new tables
- `CREATE ON CATALOG` (Option D) would allow creating new schemas in the catalog, not tables within a specific schema

This follows the **Unity Catalog least-privilege model** recommended for production environments.
</details>

---

## Score Guide

| Correct | Score | Assessment |
|---|---|---|
| 18–20 | 90–100% | Excellent — Exam ready |
| 15–17 | 75–85% | Good — Review weak areas |
| 12–14 | 60–70% | Borderline — More practice needed |
| < 12 | < 60% | Needs significant study |

**Exam passing score: 70% (≥ 42/59 on the real exam)**

---

*Source: Databricks Certified Data Engineer Professional Exam Guide, July 2026 edition*
