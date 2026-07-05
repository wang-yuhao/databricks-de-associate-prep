# Databricks Data Engineer Professional — 14-Day Prep Guide

> ☁️ Azure Databricks Edition

A structured **14-day study plan** for the **Databricks Certified Data Engineer Professional** exam, building directly on top of the Associate prep in this repo.

---

## Exam Overview

| Item | Details |
|------|---------|
| Questions | 59 (+ 4 unscored) |
| Duration | 120 minutes |
| Passing Score | ~70% |
| Cost | $200 USD |
| Format | Multiple choice / multi-select |
| Recommended XP | 1+ year hands-on Databricks |

---

## Exam Sections & Weights

| # | Section | Weight |
|---|---------|--------|
| 1 | Developing Code for Data Processing (Python + SQL) | **22%** |
| 2 | Data Ingestion & Acquisition | 7% |
| 3 | Data Transformation, Cleansing, and Quality | 10% |
| 4 | Data Sharing and Federation | 5% |
| 5 | Monitoring and Alerting | 10% |
| 6 | Cost & Performance Optimisation | **13%** |
| 7 | Ensuring Data Security and Compliance | 10% |
| 8 | Data Governance | 7% |
| 9 | Debugging and Deploying | 10% |
| 10 | Data Modelling | 6% |

> Sections 1 and 6 together = **35%** of the exam. Prioritise them.

---

## 14-Day Study Plan

### Week 1 — Core Engineering & Advanced Delta

| Day | Topic | Section(s) | Folder |
|-----|-------|------------|--------|
| 1 | Advanced Delta Lake & Optimistic Concurrency | 1, 6 | `week1/day1/` |
| 2 | Auto Loader & Advanced Ingestion Patterns | 2 | `week1/day2/` |
| 3 | Lakeflow Declarative Pipelines (DLT advanced) | 1, 3 | `week1/day3/` |
| 4 | CDC with APPLY CHANGES INTO | 3 | `week1/day4/` |
| 5 | Structured Streaming — advanced patterns | 1, 5 | `week1/day5/` |
| 6 | Python project structure, UDFs, Testing | 1 | `week1/day6/` |
| 7 | Databricks Asset Bundles (DABs) & CI/CD | 9 | `week1/day7/` |

### Week 2 — Governance, Performance & Deployment

| Day | Topic | Section(s) | Folder |
|-----|-------|------------|--------|
| 8 | Cost & Performance Optimisation | 6 | `week2/day8/` |
| 9 | Data Quality — Expectations, Great Expectations | 3, 5 | `week2/day9/` |
| 10 | Data Security & Compliance | 7 | `week2/day10/` |
| 11 | Data Governance & Unity Catalog Advanced | 8 | `week2/day11/` |
| 12 | Delta Sharing & Data Federation | 4 | `week2/day12/` |
| 13 | Data Modelling — Medallion, Star, SCD | 10 | `week2/day13/` |
| 14 | Full Mock Exam + Weakness Review | All | `mock-exam.md` |

---

## Repo Structure (Professional Section)

```text
pro/
├── README.md                  ← This file
├── cheatsheet-pro.md          ← Quick reference card
├── mock-exam.md               ← 60-question timed mock exam
├── week1/
│   ├── day1/  (Advanced Delta Lake)
│   ├── day2/  (Auto Loader & Ingestion)
│   ├── day3/  (Lakeflow Pipelines)
│   ├── day4/  (CDC & APPLY CHANGES)
│   ├── day5/  (Advanced Streaming)
│   ├── day6/  (Python, UDFs, Testing)
│   └── day7/  (DABs & CI/CD)
├── week2/
│   ├── day8/  (Cost & Performance)
│   ├── day9/  (Data Quality)
│   ├── day10/ (Security & Compliance)
│   ├── day11/ (Governance & UC Advanced)
│   ├── day12/ (Delta Sharing & Federation)
│   └── day13/ (Data Modelling)
└── notebooks/
    ├── pro_day1_advanced_delta.py
    ├── pro_day2_autoloader.py
    ├── pro_day3_lakeflow_pipelines.py
    ├── pro_day4_cdc_apply_changes.py
    ├── pro_day5_advanced_streaming.py
    ├── pro_day6_testing_udfs.py
    └── pro_day7_dabs_cicd.py
```

---

## Prerequisites

- Completed (or equivalent knowledge of) the **7-day Associate prep** in this repo
- Azure Databricks workspace (Premium tier with Unity Catalog enabled)
- Familiarity with Python, SQL, PySpark, and basic Spark concepts
- Databricks CLI installed (`pip install databricks-cli` or `pip install databricks-sdk`)

---

## Key Differences from Associate Exam

| Topic | Associate | Professional |
|-------|-----------|--------------|
| Delta Lake | CRUD, time travel, MERGE | Optimistic concurrency, clones, Bloom filters |
| Pipelines | Basic DLT | APPLY CHANGES, expectations, CDC |
| Streaming | readStream/writeStream basics | Stateful ops, watermarks, output modes |
| CI/CD | Workflows overview | DABs, REST API, Databricks CLI, unit testing |
| Governance | Unity Catalog basics | Row/col filters, audit logs, Delta Sharing |
| Performance | OPTIMIZE, ZORDER | Adaptive Query Execution, caching, photon |
| Testing | Manual run | `assertDataFrameEqual`, pytest, integration tests |

---

## Official Resources

- [Exam Guide (Databricks)](https://www.databricks.com/learn/certification/data-engineer-professional)
- [Databricks Documentation](https://docs.databricks.com)
- [Lakeflow Declarative Pipelines](https://docs.databricks.com/en/dlt/index.html)
- [Delta Lake OSS](https://delta.io)
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Unity Catalog Best Practices](https://docs.databricks.com/en/data-governance/unity-catalog/best-practices.html)
