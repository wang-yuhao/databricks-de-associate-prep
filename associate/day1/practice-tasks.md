# Day 1 — Practice Tasks (Azure Databricks)

## Setup: Access Azure Databricks Workspace

> ✅ You already have an Azure account — use it to create a real Azure Databricks workspace (no Community Edition limitations!).

### Steps to Create Your Azure Databricks Workspace

1. Go to [portal.azure.com](https://portal.azure.com) and sign in
2. In the top search bar, type **Azure Databricks** and select it
3. Click **+ Create**
4. Fill in the **Basics** tab:
   - **Subscription**: Your Azure subscription
   - **Resource Group**: Click **Create new** → name it `databricks-prep-rg`
   - **Workspace name**: `databricks-de-prep`
   - **Region**: `West Europe` (closest to Munich)
   - **Pricing Tier**: `Trial (Premium - 14 Days Free DBUs)` ← avoids DBU charges
5. (Optional) **Networking** tab: Leave defaults (public access) — fine for learning
6. Click **Review + Create** → **Create**
   - Deployment takes ~2–3 minutes
7. When deployment is complete, click **Go to resource** → **Launch Workspace**
   - This opens the Databricks workspace in a new browser tab

> 💡 **Azure Advantage over Community Edition:**
> - Full multi-node clusters available
> - Unity Catalog is enabled by default on new workspaces (2023+)
> - Job clusters, SQL Warehouses, and Delta Live Tables / Lakeflow Declarative Pipelines all available
> - Azure Data Lake Storage Gen2 (ADLS Gen2) integration for a real data lake

---

## Task 1: Explore the Azure Databricks Workspace UI (30 min)

When you first launch the workspace you land on the **Home** screen. Take 30 minutes to click through every left-sidebar item.

### Left-sidebar navigation (as of 2025/2026 UI)

| Icon/Label | What you'll find |
|---|---|
| **Home** | Your personal workspace shortcuts and recent items |
| **Workspace** | File browser — notebooks, folders, repos (`/Users/<email>`) |
| **Catalog** | Unity Catalog explorer: Catalog → Schema → Table / Volume / View |
| **SQL Editor** | SQL Warehouses + query editor (replaces old "Data" tab) |
| **Workflows** | Job scheduler — create and monitor pipeline jobs |
| **Compute** | All-Purpose clusters, SQL Warehouses, cluster policies |
| **Delta Live Tables / Lakeflow** | Declarative pipeline definitions (new name from 2025) |
| **Marketplace** | Data & AI solutions from Databricks partners |
| **Partner Connect** | One-click integration with BI tools (Power BI, Tableau, etc.) |
| **Settings** (gear icon) | Workspace admin settings, Unity Catalog config, user management |

### Step-by-step exploration

1. **Compute** → Click **Create compute**
   - Note the three compute types in the UI: **All-Purpose**, **Job**, **SQL Warehouse**
   - Under Databricks Runtime, scroll through available versions — note the naming convention:
     - `16.4 LTS` = Long-Term Support, latest as of May 2026
     - `15.4 LTS ML` = Machine Learning runtime (includes MLflow, TensorFlow, PyTorch)
     - `16.x` non-LTS = shorter support lifecycle, cutting-edge features
   - Under **Worker type**, note Azure VM sizes (e.g., `Standard_DS3_v2`, `Standard_D8s_v3`)
   - **Cancel** — do not create yet
2. **Catalog** (Unity Catalog Explorer)
   - Expand the `main` catalog → you'll see `default` and `information_schema` schemas
   - Note the object types under a schema: Tables, Views, Volumes, Functions, Models
   - Click the ℹ️ icon on any object to see lineage, permissions, and tags
3. **SQL Editor**
   - Note the **SQL Warehouse** selector (top right) — you need a running warehouse to run SQL
   - SQL Warehouses are separate compute from clusters; they're optimized for SQL-only workloads
4. **Workflows** → Click **Create Job** (then cancel) — note you can schedule Python, notebook, SQL, or DLT tasks in a DAG
5. **Settings → Workspace admin** → Under **Identity and Access**, see Unity Catalog is enabled

**Questions to answer after exploring:**

- What is the difference between an **All-Purpose cluster** and a **SQL Warehouse**?
- Where do you go to grant a user access to a catalog?
- What is the default catalog in a new Azure Databricks workspace?
- What is the difference between the **Workspace** file browser and the **Catalog** explorer?

---

## Task 2: Create Your First All-Purpose Cluster (20 min)

All-purpose clusters are for interactive development in notebooks. They are more expensive than job clusters — always terminate them when done.

### Step-by-step instructions

1. In the left sidebar, click **Compute**
2. Make sure the **All-purpose compute** tab is selected (default)
3. Click **Create compute** (top-right blue button)
4. Configure the cluster:

   | Setting | Value | Notes |
   |---|---|---|
   | **Compute name** | `learning-cluster` | Descriptive name |
   | **Policy** | Unrestricted | Gives full control; policies restrict settings in production |
   | **Single node** toggle | ✅ Enable **Single node** | Cost-efficient for learning; no workers, driver does everything |
   | **Access mode** | **Single User** | Required for Unity Catalog (Python UDFs, Row/Column filters) |
   | **Databricks Runtime** | `16.4 LTS (Spark 3.5, Scala 2.12)` | Latest LTS as of May 2026 — always pick LTS for exams |
   | **Node type** | `Standard_DS3_v2` | 4 cores, 14 GB RAM — good balance for learning |
   | **Auto termination** | `30 minutes` | ⚠️ CRITICAL — prevents surprise Azure charges |

5. Expand **Advanced options** → notice the **Spark config**, **Environment variables**, **Init scripts** tabs — leave defaults
6. Click **Create compute** → wait 3–5 minutes for the cluster to start

> ⚠️ **Cost Tip:** A `Standard_DS3_v2` single-node cluster costs approximately **$0.20–0.35/hr** in DBU charges + Azure VM cost. The 14-day trial covers DBU cost only. Always verify auto-termination is set!

### What to observe while the cluster starts

- **Status transitions**: `Pending` → `Running`
- Click the cluster name to open its details page
- In the **Event log** tab: watch provisioning events
- In the **Spark UI** tab: available once running — shows Spark jobs, stages, tasks
- In the **Metrics** tab: CPU/memory usage graphs
- In the **Configuration** tab: see the Spark version, Python version, and cloud region

### Questions to answer after:

- What does **Single User** access mode mean for Unity Catalog?
- What is the difference between the **Driver** node and **Worker** nodes?
- In Single Node mode, is there a worker node? What runs on the driver?
- Can you change the runtime version of a running cluster? (No — you must restart or create a new one)

---

## Task 3: Create a Notebook and Run Basic Commands (40 min)

### Create the notebook

1. In the left sidebar, click **Workspace**
2. Navigate to **Home** (your personal folder, shown as `/Users/<your-email>`)
3. Click **+ Add** (or right-click in the folder panel) → **Notebook**
4. Set:
   - **Name**: `day1-exploration`
   - **Default language**: Python
5. The notebook opens in the editor. At the top, click **Connect** → select `learning-cluster`
   - Wait for the green dot — cluster is attached and ready

> 💡 **Notebook UI tips:**
> - Press **Shift+Enter** to run a cell and move to the next
> - Press **Ctrl+Enter** (Win) / **Cmd+Enter** (Mac) to run a cell and stay on it
> - Click **+** between cells to add a new cell
> - Use the language dropdown on each cell to override the default language

---

### Cell 1 — Spark and cluster info

```python
# Check Spark, Python, and cluster details
print(f"Spark version:  {spark.version}")
print(f"Python version: {spark.sparkContext.pythonVer}")
print(f"App name:       {spark.sparkContext.appName}")
print(f"Master:         {spark.sparkContext.master}")
print(f"DBR version:    {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion')}")
```

**Expected output:** Spark 3.5.x, Python 3.11 (DBR 16.x ships with Python 3.11), master = `local` (Single Node)

---

### Cell 2 — List DBFS root (legacy storage layer)

```python
# DBFS = Databricks File System — a virtual layer on top of Azure Blob Storage
# Legacy system; Unity Catalog Volumes is the modern replacement
display(dbutils.fs.ls('dbfs:/'))
```

**Expected output:** Default DBFS folders including `/databricks-datasets`, `/user`, `/tmp`, `/FileStore`

---

### Cell 3 — Explore dbutils — the Databricks utility library

```python
# dbutils is a Databricks-specific helper — NOT available outside Databricks
# Key modules:
# - dbutils.fs       → file system operations (ls, cp, mv, rm, mount)
# - dbutils.secrets  → read secrets from Azure Key Vault or Databricks Secret Scope
# - dbutils.notebook → chain notebooks together (run, exit)
# - dbutils.widgets  → parameterize notebooks (text, dropdown, combobox, multiselect)

help(dbutils)
```

---

### Cell 4 — Check DBFS mounts (Azure Blob-backed)

```python
# List existing storage mounts
# On a fresh workspace you may see /databricks-datasets already mounted
display(dbutils.fs.mounts())
```

> 🔵 **Azure context:** In Azure Databricks, DBFS root (`dbfs:/`) is backed by an **Azure Blob Storage** container that was automatically created in the managed resource group when you provisioned the workspace. You can see it in the Azure Portal under your `databricks-prep-rg` resource group — look for a storage account starting with `dbstorage...`.

---

### Cell 5 — Use magic commands

```python
# Magic commands switch the language of a single cell
# They override the notebook's default language for that cell only
```

```
%fs ls /databricks-datasets/
```

```sql
-- %sql magic: run Spark SQL in a Python notebook
%sql
SHOW CATALOGS;
```

```sql
%sql
USE CATALOG main;
SHOW SCHEMAS;
```

```python
%sh
# %sh runs shell commands on the DRIVER node only (not workers)
echo "Driver hostname: $(hostname)"
python3 --version
```

```md
%md
## Magic Commands Reference

| Magic | Language / Action |
|---|---|
| `%python` | Python (PySpark) |
| `%sql` | Spark SQL |
| `%scala` | Scala |
| `%r` | R |
| `%sh` | Bash — runs on driver node only |
| `%fs` | Shorthand for `dbutils.fs` commands |
| `%md` | Markdown — renders as formatted text |
| `%run ./other-notebook` | Execute another notebook inline |
| `%pip install <pkg>` | Install Python package on cluster |
| `%conda install <pkg>` | Install via conda (select runtimes) |
```

---

### Cell 6 — Unity Catalog namespace exploration

```sql
%sql
-- The 3-level Unity Catalog namespace: catalog.schema.table
-- Navigate the hierarchy
SHOW CATALOGS;
```

```sql
%sql
USE CATALOG main;
SHOW SCHEMAS;
```

```sql
%sql
-- Create your own schema (namespace) for practice
CREATE SCHEMA IF NOT EXISTS main.day1_practice
COMMENT 'Day 1 practice schema for Databricks DE Associate prep';

SHOW SCHEMAS IN main;
```

```sql
%sql
-- Create a simple managed Delta table in your schema
CREATE OR REPLACE TABLE main.day1_practice.sample_flights (
  origin     STRING,
  destination STRING,
  delay_min  INT,
  flight_date DATE
)
USING DELTA
COMMENT 'Sample flight data for practice';

DESCRIBE TABLE EXTENDED main.day1_practice.sample_flights;
```

---

### Cell 7 — Read sample data and write Delta

```python
# Databricks ships with built-in sample datasets — no external storage needed
df = spark.read.csv(
    '/databricks-datasets/flights/departuredelays.csv',
    header=True,
    inferSchema=True
)

print(f"Row count: {df.count()}")
print(f"Columns:   {df.columns}")
display(df.limit(5))
```

```python
# Write to your Unity Catalog Delta table (managed table — Databricks manages location)
(df
 .withColumnRenamed("origin", "origin")
 .limit(1000)
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("main.day1_practice.sample_flights")
)

print("Write complete ✅")
```

```sql
%sql
-- Read back and verify
SELECT origin, destination, COUNT(*) AS flight_count
FROM main.day1_practice.sample_flights
GROUP BY origin, destination
ORDER BY flight_count DESC
LIMIT 10;
```

---

### Cell 8 — Inspect the Delta transaction log

```python
# Delta tables consist of: Parquet data files + _delta_log/ transaction log
# The transaction log is what makes Delta ACID-compliant

# Check Delta history (Unity Catalog managed table)
display(spark.sql("DESCRIBE HISTORY main.day1_practice.sample_flights"))
```

```sql
%sql
-- OPTIMIZE compacts small Parquet files → better query performance
OPTIMIZE main.day1_practice.sample_flights;

-- VACUUM removes old files no longer needed (default: 7-day retention)
-- VACUUM main.day1_practice.sample_flights RETAIN 168 HOURS;
-- (skip VACUUM for now to preserve time travel)
```

```sql
%sql
-- Time Travel: query older versions of a Delta table
SELECT * FROM main.day1_practice.sample_flights VERSION AS OF 0
LIMIT 5;
```

---

### Cell 9 — Notebook widgets (parameterization)

```python
# Widgets let you parameterize a notebook — key for Databricks Jobs
dbutils.widgets.text("origin_filter", "ORD", "Filter by Origin Airport")

# Read the widget value
origin = dbutils.widgets.get("origin_filter")
print(f"Filtering for origin: {origin}")
```

```python
# Use the widget value in a query
filtered_df = spark.sql(f"""
    SELECT * FROM main.day1_practice.sample_flights
    WHERE origin = '{origin}'
""")
display(filtered_df)
```

---

### Cell 10 — dbutils.notebook: chaining notebooks

```python
# In production pipelines, you chain notebooks together
# dbutils.notebook.run("./other-notebook", timeout_seconds=60, arguments={"key": "value"})

# dbutils.notebook.exit() passes a return value to the parent notebook
# dbutils.notebook.exit("success")

# For now, just understand the API:
help(dbutils.notebook)
```

---

## Task 4: Install a Python Package and Use %pip (10 min)

```python
# Install packages directly in a notebook cell (installs on all cluster nodes)
%pip install faker

# After %pip, the Python kernel restarts automatically — cells above this re-run
```

```python
from faker import Faker
import pandas as pd

fake = Faker()

# Generate fake data using pandas, convert to Spark DataFrame
data = [{"name": fake.name(), "email": fake.email(), "city": fake.city()} for _ in range(100)]
pdf = pd.DataFrame(data)

# Convert pandas → Spark
sdf = spark.createDataFrame(pdf)
display(sdf)
```

> 💡 `%pip install` installs the package on all nodes of the cluster and automatically restarts the Python interpreter. Always put `%pip` cells at the **top** of a notebook to avoid losing variables.

---

## Task 5: Connect to Azure Data Lake Storage Gen2 (ADLS Gen2) — Optional Bonus (30 min)

> 🔵 Azure-specific skill important for both the DE Associate exam and real-world interviews.

### Option A: Unity Catalog External Location (modern, recommended approach)

In production, ADLS Gen2 is connected via **Unity Catalog External Locations** — no mounting required. This is the preferred pattern in 2025+:

1. Go to **Catalog** → **External Data** → **External Locations** → **Create external location**
2. Fill in:
   - **Storage credential**: Create or select a managed identity / service principal
   - **URL**: `abfss://<container>@<storage_account>.dfs.core.windows.net/`
3. Once created, files at that path are accessible via `spark.read` without mounting

```python
# After creating an External Location via Unity Catalog:
storage_url = "abfss://<container>@<storage_account>.dfs.core.windows.net/"

df = spark.read.parquet(storage_url + "my-data/")
display(df.limit(5))
```

### Option B: Legacy DBFS Mount (for exam awareness — avoid in production)

```python
# Classic mount pattern — understand for the exam, use External Locations in practice
storage_account = "<your_storage_account_name>"
container        = "<your_container_name>"
client_id        = "<service_principal_client_id>"    # from Azure AD App Registration
client_secret    = dbutils.secrets.get(scope="azure-kv", key="sp-secret")
tenant_id        = "<your_aad_tenant_id>"

configs = {
    "fs.azure.account.auth.type":                   "OAuth",
    "fs.azure.account.oauth.provider.type":         "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id":            client_id,
    "fs.azure.account.oauth2.client.secret":        client_secret,
    "fs.azure.account.oauth2.client.endpoint":      f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

dbutils.fs.mount(
    source      = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
    mount_point = "/mnt/datalake",
    extra_configs = configs
)

display(dbutils.fs.ls("/mnt/datalake"))
```

### Option C: Built-in Databricks sample datasets (no setup required)

```python
# Always available — use these for all Day 1–3 practice
display(dbutils.fs.ls('/databricks-datasets/'))
```

```python
# Read a sample Parquet dataset (NYC taxi data)
df_taxi = spark.read.format("delta").load("/databricks-datasets/nyctaxi/tripdata/green/")
print(f"Row count: {df_taxi.count()}")
display(df_taxi.limit(5))
```

---

## Task 6: Create and Run a Simple Job (Workflow) (20 min)

Jobs are how you productionize notebooks. Understanding the notebook → job path is a core exam topic.

1. **Save your notebook** (`Ctrl+S` / `Cmd+S`)
2. In the left sidebar → **Workflows** → **Create job**
3. Configure the job:
   - **Job name**: `day1-practice-job`
   - **Task name**: `run-exploration-notebook`
   - **Type**: Notebook
   - **Source**: Workspace → navigate to your `day1-exploration` notebook
   - **Cluster**: Click **Add new cluster** → configure a **Job cluster** (not all-purpose):
     - Runtime: `16.4 LTS`
     - Node type: `Standard_DS3_v2`
     - Workers: 0 (Single Node) or 1
     - **Auto termination** is automatic for Job clusters — they terminate when the job finishes
4. Click **Create task** → then **Run now** (top-right)
5. Watch the run in the **Runs** tab — click into it to see logs

**Key differences between All-Purpose and Job clusters:**

| | All-Purpose | Job cluster |
|---|---|---|
| **Lifecycle** | Manual start/stop | Created per run, auto-terminated |
| **Cost** | Higher (idle time billed) | Lower (only runs during job) |
| **Use case** | Interactive development | Production pipelines |
| **Multi-task sharing** | Shared by any notebook | Dedicated to one job run |

---

## Task 7: Architecture Quiz (Self-Check)

Answer these questions (check your answers in `study-notes.md`):

1. What is the difference between an **All-Purpose Cluster** and a **Job Cluster**?
2. What are the 3 levels of the **Unity Catalog** namespace? Give an example path.
3. What two components make up a **Delta table** on storage?
4. What is **DBFS** and why should you prefer **Unity Catalog Volumes** in production?
5. What cluster access mode is required for Unity Catalog Python UDFs and row/column filters?
6. What is the difference between `%sql` magic and `spark.sql()`?
7. How do you pass parameters to a notebook when it is run as a Databricks Job?
8. 🔵 **Azure-specific**: What Azure service backs DBFS storage in an Azure Databricks workspace?
9. 🔵 **Azure-specific**: What is the difference between **Azure Blob Storage** and **ADLS Gen2**, and which should you use for a data lake?
10. 🔵 **Azure-specific**: What is the modern (Unity Catalog) way to connect to ADLS Gen2 vs the legacy mount approach?

**Answers:**

1. All-purpose: interactive, long-running, manually managed, more expensive. Job cluster: created per-run, auto-terminated, cheaper — use for production pipelines.
2. `catalog.schema.table` — e.g., `main.sales.orders`
3. Parquet data files + `_delta_log/` transaction log directory
4. DBFS is a virtual file system overlay on Azure Blob Storage. Unity Catalog Volumes are preferred because they integrate with governance, fine-grained access control, auditing, and lineage tracking.
5. **Single User** access mode (also called Dedicated access mode in some UI versions)
6. `%sql` is a notebook magic that switches cell language to SQL for one cell. `spark.sql()` is a Python API call that returns a DataFrame — useful when you need to chain with Python transformations.
7. Via **Widgets** (`dbutils.widgets`) — set default values in the notebook; override them in the job task parameters when creating the job.
8. 🔵 DBFS in Azure Databricks is backed by **Azure Blob Storage** (in the workspace's managed resource group)
9. 🔵 ADLS Gen2 adds a **hierarchical namespace**, POSIX-style ACLs, and better throughput for big data workloads — always prefer ADLS Gen2 for a data lake. Blob Storage is flat (no real folders) and has weaker access controls.
10. 🔵 Modern: create a **Unity Catalog External Location** with a storage credential (managed identity or service principal) — no mount required. Legacy: `dbutils.fs.mount()` with OAuth config — avoid in new projects.

---

## Task 8: Evening Practice Questions

Go to [CertSafari — Databricks DE Associate](https://www.certsafari.com/databricks/data-engineer-associate) and do **20 questions** from the Databricks Intelligence Platform / workspace domain.

Track your score here:

- **Score:** ___ / 20
- **Topics I struggled with:** ___________________________
- **Notes for review:** ___________________________

---

## Databricks Runtime 16.4 LTS — Key Facts for the Exam

> DBR 16.4 LTS was released in May 2025 and is the current LTS as of May 2026.

- **Spark version**: Apache Spark 3.5
- **Python version**: Python 3.11
- **Scala version**: 2.12 (also a 2.13 variant)
- **Notable features in DBR 16.x**:
  - Fine-grained access control on dedicated compute is **GA**
  - Auto Loader can now clean processed files in source directory
  - Auto Loader supports automatic type widening (Public Preview)
  - Liquid clustering auto-compaction improvement
  - Dashboards, alerts, and queries supported as workspace files
  - `IDENTIFIER` support in DBSQL for catalog operations

---

## Azure Cost Checklist (Do Before Ending Each Study Session)

- [ ] Terminate your `learning-cluster` (or confirm auto-termination fired in the last 30 min)
- [ ] Stop any running SQL Warehouses (Compute → SQL Warehouses → Stop)
- [ ] Check Azure Cost Management: [portal.azure.com](https://portal.azure.com) → **Cost Management + Billing**
- [ ] Set a **Budget Alert** in Azure Cost Management: notify at **€10 spent** to avoid surprises
- [ ] Optionally: check the managed resource group (`databricks-prep-rg`) in the Azure Portal to see underlying VMs and storage accounts spawned by the cluster
