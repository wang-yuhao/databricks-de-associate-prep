# Day 1 — Databricks Intelligence Platform (~20% of exam)

## ⏰ Schedule
- **Morning (2h):** Read all sections below
- **Mid-morning (2h):** Watch videos + explore Databricks UI
- **Afternoon (2h):** Complete practice-tasks.md
- **Evening (1h):** CertSafari — filter to Platform topic, do 20 questions

---

## 1.1 Lakehouse Architecture

### What is a Lakehouse?
A **Lakehouse** combines the best of a Data Warehouse (structured, ACID, BI-ready) and a Data Lake (cheap object storage, schema-on-read, unstructured data). Databricks implements this via **Delta Lake** on top of cloud object storage (S3, ADLS, GCS).

```
┌─────────────────────────────────────────────────────────┐
│                  DATABRICKS LAKEHOUSE                    │
│                                                          │
│  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
│  │  Data   │  │   ML /   │  │ Business │  │  Data    │ │
│  │ Engin.  │  │  AI/ML   │  │ Intell.  │  │ Science  │ │
│  └────┬────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘ │
│       └────────────┴─────────────┴──────────────┘       │
│                    DELTA LAKE (storage)                  │
│              Parquet files + transaction log             │
│  ┌──────────────────────────────────────────────────┐   │
│  │           Cloud Object Storage (S3/ADLS/GCS)    │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

**Key benefits over traditional data warehouse:**
- No data duplication (data lives once in object storage)
- Supports all data types (structured, semi-structured, unstructured)
- Open format (Parquet + Delta transaction log)
- Unified governance via Unity Catalog

---

## 1.2 Databricks Workspace Components

### Clusters
| Type | Use Case | Notes |
|---|---|---|
| **All-Purpose Cluster** | Interactive notebooks, ad-hoc analysis | Manually started/stopped; persists |
| **Job Cluster** | Automated jobs/pipelines | Created per-job run, terminated after; cheaper |
| **SQL Warehouse** | Databricks SQL queries only | Serverless option available |

**Cluster modes:**
- **Single Node:** Driver only, no workers. Good for small data, ML model development.
- **Standard (Multi-Node):** Driver + worker nodes. Required for distributed Spark jobs.
- **High Concurrency:** Multiple users share; fine-grained access control. (Legacy — now managed by serverless.)

**Auto-scaling:** Clusters can scale worker count between min/max based on workload.

**Cluster access modes (Unity Catalog):**
- `Single User` — Dedicated to one user; required for Python UDFs with Unity Catalog
- `Shared` — Multiple users; Unity Catalog enforced; no Python UDFs
- `No Isolation Shared` — Legacy; no Unity Catalog enforcement

### Notebooks
- Support Python, SQL, R, Scala
- Magic commands: `%python`, `%sql`, `%md`, `%sh`, `%fs` (DBFS), `%run` (run another notebook)
- Results are displayed inline; notebooks can be scheduled as jobs

### Databricks Repos
- Git-backed folders in workspace
- Supports GitHub, GitLab, Azure DevOps, Bitbucket
- Enables version control, branching, PR workflows

---

## 1.3 Storage: DBFS, External Locations, Volumes

### DBFS (Databricks File System)
- Virtual file system overlay over cloud object storage
- `dbfs:/` prefix in code; `/dbfs/` for file system access in drivers
- **Deprecated pattern:** Storing production data in DBFS root (`dbfs:/user/hive/warehouse`)
- **Best practice:** Use Unity Catalog external locations and volumes instead

### External Locations (Unity Catalog)
- Define cloud storage paths accessible within Unity Catalog
- Created by storage admins; grants given to users/groups
```sql
CREATE EXTERNAL LOCATION my_location
  URL 's3://my-bucket/data/'
  WITH (STORAGE CREDENTIAL my_credential);
```

### Volumes (Unity Catalog)
- Non-tabular file storage managed under Unity Catalog
- `MANAGED`: files stored in Unity Catalog-managed location
- `EXTERNAL`: files at a specified external location
```sql
CREATE VOLUME catalog.schema.my_volume;  -- managed
CREATE EXTERNAL VOLUME catalog.schema.ext_vol LOCATION 's3://bucket/path';  -- external
```
Access: `/Volumes/catalog/schema/volume_name/file.csv`

---

## 1.4 Delta Lake Fundamentals (Overview)

> Deep dive in Day 3. For today: understand what Delta Lake IS.

Delta Lake is an **open-source storage layer** that brings ACID transactions to Apache Spark and cloud storage. Key features:

| Feature | What It Means |
|---|---|---|
| **ACID Transactions** | Safe concurrent reads/writes |
| **Schema Enforcement** | Rejects data that doesn't match table schema |
| **Schema Evolution** | Can add new columns safely |
| **Time Travel** | Query historical versions of data |
| **MERGE** | Upsert (INSERT + UPDATE) in a single statement |
| **OPTIMIZE/ZORDER** | Compaction + data skipping |

Every Delta table = **Parquet data files** + `_delta_log/` transaction log.

---

## 1.5 Unity Catalog (Overview)

> Deep dive in Day 6. For today: understand the hierarchy.

```
Metastore  (one per region/account)
  └── Catalog
        └── Schema (Database)
              ├── Table
              ├── View
              └── Volume
```

**Three-part naming:** `catalog.schema.table`

**Key objects:**
- **Metastore:** Top-level metadata store. One per Databricks account region.
- **Catalog:** Namespace for organizing schemas. Replaces legacy hive_metastore.
- **Schema/Database:** Container for tables, views, volumes.
- **Table:** Can be MANAGED (Unity controls lifecycle) or EXTERNAL (you control storage).

---

## 1.6 Key Exam-Focus Points for This Domain

1. ✅ Know the difference between **all-purpose** vs **job clusters** (cost, lifecycle)
2. ✅ Know **cluster access modes** and when to use Single User vs Shared
3. ✅ Know the **3-level Unity Catalog namespace**: catalog → schema → table
4. ✅ Know what **DBFS** is and why you should prefer Volumes/External Locations
5. ✅ Know that Delta Lake = Parquet files + transaction log (`_delta_log/`)
6. ✅ Know **Databricks Repos** = Git-backed workspace folders
7. ✅ Know what a **SQL Warehouse** is (serverless compute for SQL)

---

## 📺 Videos to Watch Today

1. [Databricks Architecture Overview](https://www.youtube.com/watch?v=FeMREjmRQN8) (10 min) — What is the Databricks Intelligence Platform?
2. [Delta Lake in 5 Minutes](https://www.youtube.com/watch?v=BMO90DI82Zc) (5 min) — Delta Lake fundamentals
3. [Unity Catalog Overview](https://www.youtube.com/watch?v=qBBBVJc7ISo) (15 min) — Governance hierarchy

---

## 🔗 Reading Links
- [Databricks Architecture](https://docs.databricks.com/en/getting-started/overview.html)
- [Cluster Types](https://docs.databricks.com/en/compute/index.html)
- [Unity Catalog Overview](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)


---

## 1.7 Cluster Configuration Deep Dive

### Instance Pools
Instance pools maintain a set of **idle, ready-to-use VM instances** to reduce cluster startup time.

- **Without pool:** Cluster start = 3–8 minutes (cloud VM provisioning)
- **With pool:** Cluster start = 30–60 seconds (instances already running)
- Instances in the pool are billed at a reduced DBU rate when idle
- Configure min/max idle instances and instance type

```
Workspace → Compute → Instance Pools → Create Pool
  → Min Idle Instances: 2
  → Max Capacity: 10
  → Instance Type: Standard_DS3_v2 (Azure) / m5.xlarge (AWS)
  → Idle Instance Auto Termination: 60 min
```

### Cluster Policies
Cluster policies **restrict what cluster configurations users can create**, enforcing cost controls and best practices.

**Use cases:**
- Prevent users from creating expensive large clusters
- Enforce auto-termination settings
- Mandate specific DBR versions
- Restrict instance types

```json
// Example cluster policy JSON
{
  "autotermination_minutes": {
    "type": "fixed",
    "value": 30,
    "hidden": false
  },
  "node_type_id": {
    "type": "allowlist",
    "values": ["Standard_DS3_v2", "Standard_DS4_v2"]
  },
  "num_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 10
  },
  "spark_version": {
    "type": "regex",
    "pattern": ".*lts.*",
    "defaultValue": "15.4.x-scala2.12"
  }
}
```

> 🔑 **Exam key point:** Cluster policies are defined by admins and assigned to users/groups. Users can only create clusters that comply with the policy. Policies enforce governance at the compute layer.

### Init Scripts
Init scripts are shell scripts that run on **every node of every cluster** at startup, before the Spark context is created.

**Use cases:**
- Install OS-level packages (`apt-get install ...`)
- Install Python packages not in DBR
- Set environment variables
- Configure network settings

```bash
#!/bin/bash
# Example init script: install a Python package
pip install great-expectations==0.18.0

# Install OS package
apt-get install -y libgomp1

# Set environment variable
export MY_COMPANY_ENV=production
```

```python
# Attach init script via cluster config (UI or API)
# Scripts can be stored in:
# - DBFS: dbfs:/databricks/scripts/my_init.sh
# - Volumes: /Volumes/catalog/schema/volume/scripts/my_init.sh  (preferred)
# - Workspace: /Users/user@company.com/init.sh
```

| Init Script Type | When It Runs | Scope |
|---|---|---|
| **Cluster-scoped** | On that cluster only | Attached in cluster config |
| **Global** | On ALL clusters in workspace | Set by admin in workspace settings |

> ⚠️ **Exam trap:** Init scripts run at cluster **startup**, not at notebook execution. If the script fails, the cluster fails to start. Test scripts on a dev cluster before using in production.

### Cluster Tags
Tags are key-value metadata attached to clusters for **cost allocation and chargeback**.

```python
# Cluster tags appear in cloud billing reports
# Example tags:
{
  "team": "data-engineering",
  "project": "etl-pipeline",
  "environment": "production",
  "cost_center": "1234"
}
```

---

## 1.8 DBU (Databricks Unit) Pricing Model

Databricks charges in **DBUs (Databricks Units)** — a unit of processing capability per hour.

### DBU Rates by Workload

| Workload | DBU Rate |
|---|---|
| All-Purpose compute (interactive) | Highest DBU rate |
| Jobs compute (automated) | Lower DBU rate |
| SQL Warehouses (Serverless) | Per query execution time |
| Instance Pool (idle) | Reduced DBU rate |
| DLT (Delta Live Tables) | Pipeline DBU rate |

> 🔑 **Key point:** **Job clusters are always cheaper than all-purpose clusters** for the same workload because they use the Jobs compute DBU rate. This is why you should use job clusters for production pipelines.

### Cost Optimization Best Practices
1. **Always set auto-termination** on all-purpose clusters (30–60 min)
2. **Use job clusters** (not all-purpose) for scheduled pipelines
3. **Use instance pools** to reduce startup time while minimizing idle cost
4. **Use cluster policies** to cap max workers and enforce auto-termination
5. **Use Serverless SQL Warehouses** for ad-hoc SQL — zero idle cost
6. **Use Single Node** clusters for development/prototyping (no worker cost)
7. **Tag all clusters** for cost tracking

---

## 1.9 Databricks Repos vs Workspace Files

### Repos (Git-backed)
- Connected to a Git provider (GitHub, GitLab, Bitbucket, Azure DevOps)
- Supports: clone, pull, push, create branch, merge, create PR
- Files in Repos are versioned in Git
- **Best for:** collaborative development, CI/CD, production code
- Supports **sparse checkout** — only check out specific folders

### Workspace Files
- Native Databricks workspace, no Git connection
- Supports notebooks, folders, dashboards
- **Best for:** quick experimentation, shared dashboards
- Cannot push/pull to Git directly

| Feature | Repos | Workspace Files |
|---|---|---|
| Git integration | ✅ Yes | ❌ No |
| Branching | ✅ Yes | ❌ No |
| PR workflow | ✅ Yes | ❌ No |
| Collaboration | Branching-based | Sharing-based |
| Recommended for production | ✅ Yes | ❌ No |

---

## 1.10 Updated Key Exam-Focus Points

8. ✅ Know **instance pools** — reduce cluster startup time; idle VMs billed at lower DBU rate
9. ✅ Know **cluster policies** — admin-defined JSON constraints on cluster config; enforces cost controls
10. ✅ Know **init scripts** — shell scripts on all nodes at startup; stored in Volumes or DBFS
11. ✅ Know **DBU** — job clusters have lower DBU rate than all-purpose; always use job clusters for production
12. ✅ Know **cluster tags** — key-value metadata for cost allocation and chargeback
13. ✅ Know **Repos vs Workspace Files** — Repos = Git-backed; use for production code
14. ✅ Know **auto-termination** — always set on all-purpose clusters to avoid idle costs
15. ✅ Know **Single Node cluster** — driver only, no workers; good for small data and ML dev
