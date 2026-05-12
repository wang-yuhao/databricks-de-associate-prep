# Day 1 — Practice Tasks

## Setup: Create Your Free Databricks Community Edition

1. Go to [community.cloud.databricks.com](https://community.cloud.databricks.com)
2. Sign up with your email (free, no credit card)
3. After login, you'll see the Databricks workspace

> ⚠️ Community Edition has a single-node cluster and no Unity Catalog. You'll explore the UI concepts today and do hands-on Spark work from Day 2 onwards.

---

## Task 1: Explore the Workspace UI (30 min)

**Steps:**
1. Navigate to **Compute** → Click **Create Cluster**
   - Note the cluster type options (Standard vs Single Node)
   - Note the Databricks Runtime version options
   - **Don't create it yet** — just observe options, then cancel
2. Navigate to **Workspace** → Explore the folder structure
3. Navigate to **Data** → Note the Catalog/Schema/Tables structure
4. Navigate to **SQL** → Note the SQL Editor and Warehouses

**What to note:**
- Where are cluster settings?
- What Databricks Runtime versions are available?
- What's the difference between the Catalog Explorer and legacy DBFS?

---

## Task 2: Create Your First Cluster (20 min)

1. Go to **Compute** → **Create Cluster**
2. Settings:
   - Name: `learning-cluster`
   - Policy: Unrestricted
   - Single node (Community Edition only supports this)
   - Runtime: Latest LTS (e.g. 14.x LTS)
   - Node type: default
3. Click **Create Cluster** and wait for it to start (2–3 min)

**Questions to answer after:**
- What state does the cluster show while starting? After it's running?
- Where can you see cluster logs?
- What does "Auto Termination" mean in the settings?

---

## Task 3: Create a Notebook and Run Basic Commands (30 min)

1. Go to **Workspace** → **Create** → **Notebook**
2. Name: `day1-exploration`, Language: Python
3. Attach to your `learning-cluster`

Run the following cells:

```python
# Cell 1: Check Spark version
print(f"Spark version: {spark.version}")
print(f"Python version: {spark.sparkContext.pythonVer}")
```

```python
# Cell 2: List DBFS root
display(dbutils.fs.ls('dbfs:/'))
```

```python
# Cell 3: Check available magic commands
# Switch cell to %md and type:
```
```markdown
%md
## This is a Markdown Cell
Databricks notebooks support **multiple languages** per cell using magic commands:
- `%python` — Python
- `%sql` — SQL
- `%md` — Markdown
- `%sh` — Shell
- `%fs` — DBFS file system commands
```

```python
# Cell 4: Use %fs magic
%fs ls /
```

```sql
-- Cell 5: Switch to SQL (use %sql magic or change cell type)
%sql
SHOW DATABASES;
```

**Expected outcomes:**
- You should see Spark version output
- DBFS listing shows default folders
- SQL shows at least `default` database

---

## Task 4: Architecture Quiz (Self-Check)

Answer these questions (check your answers in study-notes.md):

1. What is the difference between an All-Purpose Cluster and a Job Cluster?
2. What are the 3 levels of the Unity Catalog namespace? Give an example path.
3. What two components make up a Delta table on storage?
4. What is DBFS and why should you prefer Volumes in production?
5. What cluster access mode is required for Python UDFs with Unity Catalog?

**Answers:**
1. All-purpose: interactive, manually managed, more expensive. Job cluster: created per-run, auto-terminated, cheaper.
2. catalog.schema.table (e.g., `main.sales.orders`)
3. Parquet data files + `_delta_log/` transaction log directory
4. DBFS is a virtual file system overlay on cloud storage. Volumes are preferred because they integrate with Unity Catalog governance.
5. `Single User` access mode

---

## Task 5: Evening Practice Questions

Go to [CertSafari — Databricks DE Associate](https://www.certsafari.com/databricks/data-engineer-associate) and do **20 questions** from the Databricks Intelligence Platform / workspace domain.

Track your score here:
- **Score:** ___ / 20
- **Topics I struggled with:** ___________________________
- **Notes for review:** ___________________________
