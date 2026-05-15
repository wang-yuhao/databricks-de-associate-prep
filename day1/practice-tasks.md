# Day 1 — Practice Tasks (Azure Databricks)

## Setup: Access Azure Databricks Workspace

> ✅ You already have an Azure account — use it to create a real Azure Databricks workspace (no Community Edition limitations!).

### Steps to Create Your Azure Databricks Workspace

1. Go to [portal.azure.com](https://portal.azure.com) and sign in
2. Click **Create a resource** → Search for **Azure Databricks**
3. Click **Create** and fill in:
   - **Subscription**: Your Azure subscription
   - **Resource Group**: Create new → `databricks-prep-rg`
   - **Workspace name**: `databricks-de-prep`
   - **Region**: West Europe (or nearest to Munich)
   - **Pricing Tier**: `Trial (Premium - 14 Days Free DBUs)` ← use this to avoid charges
4. Click **Review + Create** → **Create** (takes ~2 min)
5. Once deployed, click **Launch Workspace**

> 💡 **Azure Advantage over Community Edition:**
> - Full multi-node clusters available
> - Unity Catalog is supported
> - Job clusters, SQL Warehouses, and Delta Live Tables all available
> - You can use Azure Data Lake Storage Gen2 (ADLS Gen2) as your data lake

---

## Task 1: Explore the Azure Databricks Workspace UI (30 min)

**Steps:**

1. Navigate to **Compute** → Click **Create compute**
   - Note cluster types: **All-Purpose** vs later you'll see **Job** clusters
   - Note **Databricks Runtime** versions (Standard, ML, GPU variants)
   - Note **Worker type**: choose from Azure VM sizes (e.g., `Standard_DS3_v2`)
   - **Don't create it yet** — just observe, then cancel
2. Navigate to **Workspace** → Explore the folder structure (your home folder is `/Users/<your-email>`)
3. Navigate to **Data** (Catalog Explorer) → Note the **Unity Catalog** hierarchy: Catalog → Schema → Table
4. Navigate to **SQL Editor** → Note the SQL Warehouses section
5. Navigate to **Workflows** → This is where you'll schedule Jobs later

**What to note:**

- Where are cluster settings vs SQL Warehouse settings?
- What Databricks Runtime versions are available? What's the difference between Standard and ML runtimes?
- What's the difference between the **Unity Catalog** (Data tab) and legacy **DBFS**?
- In Unity Catalog, what is the default catalog called in your workspace?

---

## Task 2: Create Your First All-Purpose Cluster (20 min)

1. Go to **Compute** → **Create compute**
2. Settings:
   - **Name**: `learning-cluster`
   - **Policy**: Unrestricted
   - **Cluster mode**: Single node (cost-efficient for learning)
   - **Runtime**: Latest LTS (e.g. 15.x LTS)
   - **Node type**: `Standard_DS3_v2` (4 cores, 14 GB RAM — good for learning)
   - **Auto termination**: 30 minutes (important — prevents surprise costs!)
3. Click **Create compute** and wait for it to start (~3–5 min)

> ⚠️ **Cost Tip:** Always set Auto Termination. Azure DBUs are billed per hour of cluster runtime. A single `Standard_DS3_v2` node costs ~$0.20–0.30/hr DBU + Azure VM cost. The 14-day trial covers DBU costs.

**Questions to answer after:**

- What state does the cluster show while starting? After it's running? (`Pending` → `Running`)
- Where can you see cluster event logs and Spark UI?
- What does the **Driver** vs **Worker** distinction mean in Single Node mode?
- Where is the cluster's cloud provider (Azure) region shown?

---

## Task 3: Create a Notebook and Run Basic Commands (30 min)

1. Go to **Workspace** → **Create** → **Notebook**
2. Name: `day1-exploration`, Language: **Python**
3. Attach to your `learning-cluster`

Run the following cells:

```python
# Cell 1: Check Spark and cluster info
print(f"Spark version: {spark.version}")
print(f"Python version: {spark.sparkContext.pythonVer}")
print(f"App name: {spark.sparkContext.appName}")
print(f"Master: {spark.sparkContext.master}")
```

```python
# Cell 2: List DBFS root (legacy storage)
display(dbutils.fs.ls('dbfs:/'))
```

```python
# Cell 3: Check Azure-specific DBFS mounts
# On Azure Databricks, DBFS is backed by Azure Blob Storage
display(dbutils.fs.mounts())
```

```python
# Cell 4: Explore dbutils secrets (useful for connecting to Azure services)
help(dbutils.secrets)
```

```md
%md
## This is a Markdown Cell

Azure Databricks notebooks support **multiple languages** per cell using magic commands:
- `%python` — Python (PySpark)
- `%sql` — SQL (Spark SQL)
- `%md` — Markdown
- `%sh` — Shell commands on driver node
- `%fs` — DBFS file system commands (shorthand for dbutils.fs)
- `%scala` — Scala
- `%r` — R
```

```
# Cell 5: Use %fs magic to browse DBFS
%fs ls /
```

```sql
-- Cell 6: SQL magic — explore Unity Catalog
%sql
SHOW CATALOGS;
```

```sql
-- Cell 7: Check schemas in main catalog
%sql
USE CATALOG main;
SHOW SCHEMAS;
```

**Expected outcomes:**

- Spark version output shown (e.g., 3.5.x)
- DBFS listing shows default folders (`/databricks-datasets`, `/user`, etc.)
- `SHOW CATALOGS` shows at least `main` and `hive_metastore`
- `SHOW SCHEMAS` shows `default` and `information_schema`

---

## Task 4: Connect to Azure Data Lake Storage Gen2 (ADLS Gen2) — Optional Bonus (30 min)

> 🔵 This is an Azure-specific skill that gives you a real-world edge for the exam and interviews.

### Option A: Direct access using Azure AD credential passthrough (simplest)

```python
# Access ADLS Gen2 using your Azure AD identity
# Replace with your actual storage account and container
storage_account = "<your_storage_account_name>"
container = "<your_container_name>"

# Mount path
dbutils.fs.mount(
  source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
  mount_point = "/mnt/datalake",
  extra_configs = {"fs.azure.account.auth.type": "AzureActiveDirectory"}
)
```

### Option B: Use pre-loaded Databricks sample datasets (no setup required)

```python
# Databricks ships with sample datasets in DBFS
display(dbutils.fs.ls('/databricks-datasets/'))

# Read a sample CSV
df = spark.read.csv('/databricks-datasets/flights/departuredelays.csv', 
                    header=True, inferSchema=True)
display(df.limit(10))
print(f"Row count: {df.count()}")
print(f"Schema: {df.schema}")
```

---

## Task 5: Architecture Quiz (Self-Check)

Answer these questions (check your answers in `study-notes.md`):

1. What is the difference between an **All-Purpose Cluster** and a **Job Cluster**?
2. What are the 3 levels of the **Unity Catalog** namespace? Give an example path.
3. What two components make up a **Delta table** on storage?
4. What is **DBFS** and why should you prefer **Unity Catalog Volumes** in production?
5. What cluster access mode is required for Python UDFs with Unity Catalog?
6. 🔵 **Azure-specific**: What Azure service backs DBFS storage in an Azure Databricks workspace?
7. 🔵 **Azure-specific**: What is the difference between **Azure Blob Storage** and **ADLS Gen2**, and which should you use for a data lake?

**Answers:**

1. All-purpose: interactive, long-running, manually managed, more expensive. Job cluster: created per-run, auto-terminated, cheaper — use for production pipelines.
2. `catalog.schema.table` — e.g., `main.sales.orders`
3. Parquet data files + `_delta_log/` transaction log directory
4. DBFS is a virtual file system overlay on cloud storage (Azure Blob in Azure). Unity Catalog Volumes are preferred because they integrate with governance, access control, and auditing.
5. `Single User` access mode
6. 🔵 DBFS in Azure Databricks is backed by **Azure Blob Storage** (in the workspace's managed resource group)
7. 🔵 ADLS Gen2 adds a **hierarchical namespace**, fine-grained ACLs (POSIX-style), and better performance for big data workloads — always prefer ADLS Gen2 for a data lake.

---

## Task 6: Evening Practice Questions

Go to [CertSafari — Databricks DE Associate](https://www.certsafari.com/databricks/data-engineer-associate) and do **20 questions** from the Databricks Intelligence Platform / workspace domain.

Track your score here:

- **Score:** ___ / 20
- **Topics I struggled with:** ___________________________
- **Notes for review:** ___________________________

---

## Azure Cost Checklist (Do Before Ending Each Study Session)

- [ ] Terminate your `learning-cluster` (or confirm auto-termination fired)
- [ ] Stop any running SQL Warehouses
- [ ] Check Azure Cost Management: [portal.azure.com](https://portal.azure.com) → Cost Management + Billing
- [ ] Set a **Budget Alert** in Azure: notify at $10 spent to avoid surprises
