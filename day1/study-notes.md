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
