# Databricks Data Engineer Associate — 7-Day Prep Guide
### ☁️ Azure Databricks Edition

A structured 7-day study plan for the **Databricks Certified Data Engineer Associate** exam, fully updated for **Azure Databricks** with Unity Catalog.

---

## ☁️ Platform: Azure Databricks

This guide uses **Azure Databricks** (not Community Edition).  
All paths, storage references, and setup steps below are Azure-specific.

### Prerequisites
- Azure subscription (free trial works: https://azure.microsoft.com/free/)
- Azure Databricks workspace (Premium tier — required for Unity Catalog)
- Basic Python and SQL knowledge

---

## 🚀 One-Time Setup (Do This First)

### Step 1 — Provision Azure Databricks Workspace

1. Go to [portal.azure.com](https://portal.azure.com)
2. Search **Azure Databricks** → **Create**
3. Fill in:
   - **Resource Group**: create new → `databricks-prep-rg`
   - **Workspace name**: `databricks-prep`
   - **Region**: West Europe (or nearest to you)
   - **Pricing tier**: **Premium** (required for Unity Catalog)
4. Click **Review + Create** → **Create**
5. Wait ~3 min → **Launch Workspace**

### Step 2 — Create a Learning Cluster

In your Databricks workspace:

1. Go to **Compute** → **Create compute**
2. Settings:
   - **Policy**: Unrestricted
   - **Access mode**: Single User (your email)
   - **Databricks Runtime**: `15.4 LTS (Scala 2.12, Spark 3.5.0)` or latest LTS
   - **Node type**: `Standard_DS3_v2` (14 GB RAM, 4 cores — cheapest that works)
   - **Auto termination**: 30 minutes (important for cost control)
   - **Single node**: ✅ checked (sufficient for all practice tasks)
3. Click **Create compute**

> 💡 **Cost tip**: Always check auto-termination is enabled. Stop the cluster manually when done studying.

### Step 3 — Set Up Unity Catalog Namespace

Open a notebook attached to your cluster and run:

```sql
-- Create the catalog used throughout all practice tasks
CREATE CATALOG IF NOT EXISTS training
  COMMENT 'Databricks DE Associate exam prep catalog';

CREATE SCHEMA IF NOT EXISTS training.prep
  COMMENT '7-day study plan practice schema';

USE CATALOG training;
USE SCHEMA prep;

SELECT current_catalog(), current_schema(), current_user();
```

### Step 4 — Create a Unity Catalog Volume (landing zone)

```sql
-- Create a managed Volume for file-based exercises
CREATE VOLUME IF NOT EXISTS training.prep.landing
  COMMENT 'Landing zone for CSV, JSON, and streaming files';
```

Verify the volume path in Python:
```python
display(dbutils.fs.ls("/Volumes/training/prep/landing/"))
```

### Step 5 — Install Databricks CLI (optional, for DABs/CI-CD on Day 5)

```bash
# On your local machine
pip install databricks-cli
databricks configure --token
# Enter: https://adb-<your-workspace-id>.azuredatabricks.net
# Enter: your Personal Access Token (Settings → Developer → Access tokens)
```

---

## 📅 7-Day Study Plan

| Day | Topic | Key Skills |
|-----|-------|------------|
| [Day 1](./day1/) | Databricks Platform & Unity Catalog | Workspace, clusters, notebooks, UC namespaces |
| [Day 2](./day2/) | Apache Spark Fundamentals | DataFrames, transformations, actions, SQL |
| [Day 3](./day3/) | Delta Lake Deep Dive | ACID, time travel, MERGE, OPTIMIZE, VACUUM |
| [Day 4](./day4/) | Structured Streaming | readStream, writeStream, triggers, Auto Loader |
| [Day 5](./day5/) | DLT, Lakeflow Jobs & CI/CD | Delta Live Tables, Workflows, DABs, secrets |
| [Day 6](./day6/) | Unity Catalog & Data Governance | Lineage, row/column security, audit logs |
| [Day 7](./day7/) | Mock Exam & Review | Full practice test, weak-spot review |

---

## 📁 Repo Structure

```
databricks-de-associate-prep/
├── README.md              ← This file (start here)
├── cheatsheet.md          ← Quick reference for the exam
├── resources.md           ← Links, docs, practice tests
├── day1/
│   ├── study-notes.md     ← Concept notes
│   └── practice-tasks.md ← Hands-on exercises
├── day2/ … day7/          ← Same structure per day
└── notebooks/
    ├── day2_spark_basics.py
    ├── day3_delta_lake.py
    ├── day4_streaming.py
    └── day5_dlt_pipeline.py
```

---

## 🗂️ Unity Catalog Namespace Used Throughout

| Layer | Value |
|-------|-------|
| **Catalog** | `training` |
| **Schema** | `prep` |
| **Full prefix** | `training.prep.<table>` |
| **Volume path** | `/Volumes/training/prep/landing/` |

All practice tasks use `training.prep` as the default namespace. Run `USE CATALOG training; USE SCHEMA prep;` at the start of every notebook session.

---

## 📚 Key Resources

- [Azure Databricks Docs](https://learn.microsoft.com/en-us/azure/databricks/)
- [Exam Guide — Data Engineer Associate](https://www.databricks.com/learn/certification/data-engineer-associate)
- [Delta Lake Docs](https://docs.delta.io/latest/index.html)
- [Unity Catalog Docs](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Full cheatsheet →](./cheatsheet.md)
- [All resources →](./resources.md)

---

## ✅ Exam Quick Facts

| Item | Detail |
|------|--------|
| **Duration** | 90 minutes |
| **Questions** | 45 multiple choice |
| **Passing score** | ~70% |
| **Format** | Online proctored |
| **Cost** | $200 USD |
| **Validity** | 2 years |

**Exam topic weights (approximate):**
- Databricks Lakehouse Platform — 24%
- ELT with Apache Spark — 29%
- Incremental Data Processing — 22%
- Production Pipelines — 16%
- Data Governance — 9%
