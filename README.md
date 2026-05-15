# 🎯 Databricks Data Engineer Associate — 7-Day Intensive Prep

> **Exam:** Databricks Certified Data Engineer Associate (updated syllabus July 2025)  
> **Format:** 45 multiple-choice questions · 90 minutes · $200  
> **Goal:** Pass in 7 days with focused, hands-on study  
> **Environment:** ☁️ **Azure Databricks** (full-featured, Unity Catalog enabled)

---

## ☁️ Azure Databricks Setup (Do This First)

### 1. Create Your Workspace

1. Go to [portal.azure.com](https://portal.azure.com) → **Create a resource** → Search **Azure Databricks**
2. Fill in:
   - **Subscription**: Your Azure subscription
   - **Resource Group**: `databricks-prep-rg` (create new)
   - **Workspace name**: `databricks-de-prep`
   - **Region**: West Europe (Munich is closest)
   - **Pricing Tier**: `Trial (Premium – 14 Days Free DBUs)` ← avoids DBU charges
3. **Review + Create** → **Create** (~2 min). Then click **Launch Workspace**.

### 2. Create Your Learning Cluster (once)

| Setting | Value |
|---|---|
| **Name** | `learning-cluster` |
| **Runtime** | Latest LTS (e.g. 16.x LTS) |
| **Mode** | Single Node |
| **Node type** | `Standard_DS3_v2` (4 vCPU, 14 GB) |
| **Auto-termination** | **30 minutes** ← always set this |
| **Access mode** | Single User |

### 3. Create Practice Catalog, Schema & Volume (run once in a notebook)

```sql
CREATE CATALOG  IF NOT EXISTS training  COMMENT 'DE Associate exam prep';
CREATE SCHEMA   IF NOT EXISTS training.prep COMMENT 'Shared practice schema';
CREATE VOLUME   IF NOT EXISTS training.prep.landing COMMENT 'File landing zone';
```

All practice tasks use:
- **Catalog:** `training`
- **Schema:** `training.prep`
- **Volume path:** `/Volumes/training/prep/landing/`
- **Checkpoints:** `/Volumes/training/prep/landing/checkpoints/<query-name>/`

### 4. Key Azure Databricks URLs

| Resource | URL |
|---|---|
| Azure Portal | https://portal.azure.com |
| Workspace | `https://adb-<id>.<n>.azuredatabricks.net` |
| Unity Catalog docs | https://docs.databricks.com/en/data-governance/unity-catalog/azure.html |
| ADLS Gen2 docs | https://docs.databricks.com/en/connect/storage/azure-storage.html |
| Azure Databricks pricing | https://azure.microsoft.com/en-us/pricing/details/databricks/ |

> 💡 **Cost guardrail:** Always set **Auto Termination = 30 min**. A `Standard_DS3_v2` single-node costs ~$0.20–0.30/hr in DBUs + Azure VM cost. The 14-day Premium trial covers DBUs.

---

## 📋 Exam Topics (Current Syllabus — July 2025)

| Domain | Weight | Day |
|---|---|---|
| 1. Databricks Intelligence Platform | ~20% | Day 1 |
| 2. Development & Ingestion (ELT, PySpark, SQL) | ~25% | Day 2–3 |
| 3. Data Processing & Transformations (Delta, Streaming) | ~20% | Day 3–4 |
| 4. Productionizing Data Pipelines (DLT, Workflows, CI/CD) | ~20% | Day 5 |
| 5. Data Governance & Quality (Unity Catalog) | ~15% | Day 6 |
| **Review + Mock Exams** | — | Day 7 |

---

## 🗓️ 7-Day Study Plan

### Day 1 — Databricks Intelligence Platform
📂 [`day1/`](./day1/)
- Architecture: Lakehouse, Delta Lake, Unity Catalog overview
- Databricks workspace: clusters, notebooks, repos
- Storage: DBFS, external locations, **Unity Catalog Volumes**, ADLS Gen2
- **Practice:** Set up Azure Databricks workspace, create cluster & UC structure

### Day 2 — Ingestion, PySpark & SQL
📂 [`day2/`](./day2/) · 📓 [`notebooks/day2_spark_basics.py`](./notebooks/day2_spark_basics.py)
- PySpark DataFrames: transformations, joins, window functions
- Auto Loader (`cloudFiles`), write modes
- SQL: CTEs, window functions, PIVOT
- **Practice:** Ingest data via Auto Loader into UC managed tables

### Day 3 — Delta Lake Deep Dive
📂 [`day3/`](./day3/) · 📓 [`notebooks/day3_delta_lake.py`](./notebooks/day3_delta_lake.py)
- CRUD, MERGE, time travel, RESTORE
- OPTIMIZE, ZORDER, Liquid Clustering, VACUUM
- Schema evolution, Change Data Feed (CDF)
- Medallion architecture (Bronze/Silver/Gold)
- **Practice:** Full Delta Lake lifecycle with UC managed tables

### Day 4 — Structured Streaming
📂 [`day4/`](./day4/) · 📓 [`notebooks/day4_streaming.py`](./notebooks/day4_streaming.py)
- readStream / writeStream, trigger types
- Windowed aggregations + watermarking
- Checkpoints stored in **Unity Catalog Volumes** (not `/tmp/`)
- Auto Loader as streaming source
- **Practice:** Delta→Delta streams, Auto Loader streaming, window agg

### Day 5 — Delta Live Tables, Lakeflow Jobs & CI/CD
📂 [`day5/`](./day5/) · 📓 [`notebooks/day5_dlt_pipeline.py`](./notebooks/day5_dlt_pipeline.py)
- DLT: `@dlt.table`, `@dlt.expect`, streaming tables vs materialized views
- Lakeflow Jobs: multi-task workflows, `taskValues`, job clusters
- Databricks Asset Bundles (DABs), CI/CD with GitHub Actions
- Azure Key Vault secret scopes
- **Practice:** Real DLT pipeline in Azure Databricks (not simulated)

### Day 6 — Unity Catalog & Data Governance
📂 [`day6/`](./day6/)
- 3-level namespace, data objects, external locations
- Grants, Row/Column-Level Security
- Data lineage, audit logging
- **Practice:** Full UC governance exercises

### Day 7 — Review & Mock Exams
📂 [`day7/`](./day7/)
- 3 full mock exams with answer keys
- Domain-by-domain gap analysis
- Exam day checklist

---

## 📁 Repository Structure

```
databricks-de-associate-prep/
├── README.md                    ← This file (start here)
├── cheatsheet.md                ← Complete exam cheat sheet
├── resources.md                 ← All study links and resources
├── day1/
│   ├── study-notes.md
│   └── practice-tasks.md
├── day2/
│   ├── study-notes.md
│   └── practice-tasks.md
├── day3/
│   ├── study-notes.md
│   └── practice-tasks.md
├── day4/
│   ├── study-notes.md
│   └── practice-tasks.md
├── day5/
│   ├── study-notes.md
│   └── practice-tasks.md
├── day6/
│   ├── study-notes.md
│   └── practice-tasks.md
├── day7/
│   ├── study-notes.md
│   ├── mock-exam-1.md
│   ├── mock-exam-2.md
│   ├── mock-exam-3.md
│   └── exam-day-checklist.md
└── notebooks/
    ├── day2_spark_basics.py
    ├── day3_delta_lake.py
    ├── day4_streaming.py
    └── day5_dlt_pipeline.py
```

---

## ⚡ Quick Start (After Workspace Setup)

```sql
-- Run this once in any notebook to initialize your practice environment
CREATE CATALOG  IF NOT EXISTS training  COMMENT 'DE Associate exam prep';
CREATE SCHEMA   IF NOT EXISTS training.prep COMMENT 'Practice schema (all days use this)';
CREATE VOLUME   IF NOT EXISTS training.prep.landing COMMENT 'File landing zone for all exercises';

USE CATALOG training;
USE SCHEMA prep;
SELECT current_catalog(), current_schema(), current_user();
```

Then import notebooks from the `notebooks/` folder via **Workspace → Import** and attach them to `learning-cluster`.

---

## 📌 Azure vs Community Edition — Key Differences

| Feature | Community Edition ❌ | Azure Databricks ✅ |
|---|---|---|
| Unity Catalog | Not available | Fully supported |
| Delta Live Tables | Not available | Fully supported |
| Lakeflow Jobs | Not available | Fully supported |
| Multi-node clusters | Not available | Fully supported |
| ADLS Gen2 storage | Not available | Native integration |
| Azure Key Vault secrets | Not available | Native integration |
| SQL Warehouses | Not available | Supported |
| Volumes (`/Volumes/`) | Not available | Fully supported |

> 🎯 **This repo is written exclusively for Azure Databricks.** All file paths, storage patterns, and features assume a Unity Catalog-enabled Azure workspace.

---

## 🔗 Exam Registration

- **Register:** [Databricks Certification Portal](https://www.webassessor.com/databricks)
- **Exam guide:** [DE Associate Exam Guide (PDF)](https://www.databricks.com/sites/default/files/2024-04/Exam-Guide-Databricks-Certified-Data-Engineer-Associate.pdf)
- **Cost:** $200 (vouchers sometimes available via Databricks Academy)
