# 🎯 Databricks Data Engineer Associate — 7-Day Intensive Prep

> **Exam:** Databricks Certified Data Engineer Associate (updated syllabus July 2025)  
> **Format:** 45 multiple-choice questions · 90 minutes · $200  
> **Goal:** Pass in 7 days with focused, hands-on study

---

## 📋 Exam Topics (Current Syllabus — May 2026)

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
- Storage: DBFS, external locations, volumes
- **Practice:** Set up free Community Edition, explore UI

### Day 2 — Development & Ingestion Part 1
📂 [`day2/`](./day2/)
- SQL & PySpark fundamentals on Databricks
- Reading/writing data: CSV, JSON, Parquet, Delta
- DataFrame API: filter, select, join, groupBy, agg
- Auto Loader (cloud file ingestion)

### Day 3 — Development & Ingestion Part 2 + Delta Lake Deep Dive
📂 [`day3/`](./day3/)
- Delta Lake: ACID transactions, time travel, MERGE, OPTIMIZE/ZORDER
- Schema evolution and enforcement
- Change Data Feed (CDF)
- Medallion architecture (Bronze/Silver/Gold)

### Day 4 — Data Processing & Streaming
📂 [`day4/`](./day4/)
- Structured Streaming fundamentals
- Trigger types: Once, AvailableNow, Continuous, ProcessingTime
- Watermarking, checkpointing, streaming aggregations
- Streaming with Delta Lake

### Day 5 — Productionizing Pipelines (DLT + Workflows + CI/CD)
📂 [`day5/`](./day5/)
- Delta Live Tables (DLT): pipelines, expectations, modes
- Lakeflow Jobs / Databricks Workflows: tasks, dependencies, triggers
- CI/CD: Databricks Repos, Asset Bundles (DABs), GitHub integration
- Monitoring and alerting

### Day 6 — Data Governance & Quality (Unity Catalog)
📂 [`day6/`](./day6/)
- Unity Catalog: metastore, catalog, schema, tables, volumes
- Access control: grants, roles, row/column filters
- Data lineage, auditing
- Delta Sharing: sharing data externally
- Lakehouse Federation

### Day 7 — Full Review + Mock Exams
📂 [`day7/`](./day7/)
- Cheatsheet review
- 3× full practice exams (45 questions each)
- Weak-area drilling
- Exam-day checklist

---

## 📚 Key Resources

| Resource | Link | Use For |
|---|---|---|
| Official Exam Guide (May 2026) | [PDF](https://www.databricks.com/sites/default/files/2026-05/databricks-certified-data-engineer-associate-exam-guide-may-2026.pdf) | Exam blueprint |
| Databricks Free Training | [academy.databricks.com](https://www.databricks.com/learn/training/home) | Official courses |
| Databricks Community Edition | [community.cloud.databricks.com](https://community.cloud.databricks.com) | Free hands-on env |
| Official Documentation | [docs.databricks.com](https://docs.databricks.com) | Deep dives |
| Practice Questions | [CertSafari](https://www.certsafari.com/databricks/data-engineer-associate) | 592 free questions |
| ExamTopics Forum | [examtopics.com/databricks](https://www.examtopics.com/discussions/databricks/) | Community discussion |
| O'Reilly Study Guide | [oreilly.com](https://www.oreilly.com/library/view/databricks-certified-data/9781098166823/) | Book |

---

## 📁 Repository Structure

```
databricks-de-associate-prep/
├── README.md                  ← You are here (master guide + schedule)
├── cheatsheet.md              ← Quick-reference for exam day
├── resources.md               ← All learning links organized by topic
├── day1/
│   ├── study-notes.md         ← Concept explanations
│   └── practice-tasks.md      ← Hands-on exercises with setup instructions
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
│   ├── mock-exam-1.md         ← 45-question practice test
│   ├── mock-exam-2.md         ← 45-question practice test
│   └── exam-day-checklist.md
└── notebooks/                 ← Databricks .py notebooks to import
    ├── day2_spark_basics.py
    ├── day3_delta_lake.py
    ├── day4_streaming.py
    └── day5_dlt_pipeline.py
```

---

## ⏰ Daily Time Budget (8–10 hours/day)

| Block | Duration | Activity |
|---|---|---|
| Morning | 2h | Read study-notes.md, watch recommended videos |
| Mid-morning | 2h | Hands-on practice in Community Edition |
| Afternoon | 2h | Complete practice-tasks.md exercises |
| Late afternoon | 1h | Do topic quiz (CertSafari/ExamTopics) |
| Evening | 1h | Review, make personal notes, cheatsheet additions |

---

## ✅ Exam Registration

1. Go to [webassessor.com/databricks](http://webassessor.com/databricks)
2. Create/login to account
3. Schedule exam: Online proctored (from home) or test center
4. System check: [kryterion.com/systemcheck](https://www.kryterion.com/systemcheck/)
5. Cost: $200 USD

> ⚠️ **Note:** Code in the exam is SQL-first; Python (PySpark) is used when SQL is insufficient. No reference materials allowed.
