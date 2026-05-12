# 🎯 Databricks Certified Data Engineer Associate — 7-Day Intensive Study Plan

> **Goal:** Pass the Databricks DE Associate exam in 7 days  
> **Exam:** 45 questions · 90 minutes · 70% passing score · $200 USD  
> **Last Updated:** May 2026 (aligned with July 2025 exam syllabus revision)

---

## 📋 Exam Blueprint

| Domain | Weight | Questions (≈) |
|---|---|---|
| 1. Databricks Intelligence Platform | 10% | 4–5 |
| 2. Development and Ingestion | 30% | 13–14 |
| 3. Data Processing & Transformations | 31% | 14 |
| 4. Productionizing Data Pipelines | 18% | 8 |
| 5. Data Governance & Quality | 11% | 5 |

**Passing:** 32/45 correct (70%)

---

## 🗓️ 7-Day Learning Path

| Day | Focus | Study Folder | Time |
|---|---|---|---|
| **Day 1** | Platform Architecture + Delta Lake Fundamentals | [`/day1`](./day1/) | 4–5h |
| **Day 2** | Development & Ingestion — Auto Loader, COPY INTO, Streaming | [`/day2`](./day2/) | 5–6h |
| **Day 3** | Data Processing — PySpark, SQL Transformations, Window Functions | [`/day3`](./day3/) | 5–6h |
| **Day 4** | Delta Lake Advanced — Time Travel, MERGE, Optimization | [`/day4`](./day4/) | 5h |
| **Day 5** | Productionizing — Delta Live Tables, Lakeflow Jobs, CI/CD | [`/day5`](./day5/) | 5–6h |
| **Day 6** | Data Governance — Unity Catalog, Security, Data Quality | [`/day6`](./day6/) | 4–5h |
| **Day 7** | Full Mock Exams + Weak Spot Review | [`/day7`](./day7/) | 6–7h |

---

## 🛠️ Setup Before Day 1

### 1. Databricks Community Edition (Free)

```
https://community.cloud.databricks.com/
```

- Sign up for **FREE** Databricks Community Edition
- You get a single-node cluster + DBFS storage
- Sufficient for all hands-on tasks in this plan

### 2. Official Exam Guide (May 2026)

```
https://www.databricks.com/sites/default/files/2026-05/databricks-certified-data-engineer-associate-exam-guide-may-2026.pdf
```

### 3. Free Databricks Academy Training

```
https://www.databricks.com/learn/training/home
```

Relevant free courses:
- **Data Engineering with Databricks** (covers 80% of exam)
- **Introduction to Delta Lake**
- **Just Enough Spark for Databricks Users**

---

## 📂 Repository Structure

```
databricks-de-associate-prep/
├── README.md                  ← You are here
├── cheatsheet.md              ← Quick-reference for all key concepts
├── practice-questions/
│   ├── domain1-platform.md    ← 20 practice Qs: Platform
│   ├── domain2-ingestion.md   ← 30 practice Qs: Ingestion
│   ├── domain3-processing.md  ← 30 practice Qs: Processing
│   ├── domain4-pipelines.md   ← 25 practice Qs: Pipelines
│   ├── domain5-governance.md  ← 20 practice Qs: Governance
│   └── mock-exam-full.md      ← Full 45-question mock exam
├── notebooks/
│   ├── day2_autoloader_demo.py
│   ├── day3_pyspark_transforms.py
│   ├── day4_delta_advanced.py
│   ├── day5_dlt_pipeline.py
│   └── day6_unity_catalog.py
├── day1/ → study notes: Platform + Delta Basics
├── day2/ → study notes: Ingestion
├── day3/ → study notes: Processing
├── day4/ → study notes: Delta Advanced
├── day5/ → study notes: Productionizing
├── day6/ → study notes: Governance
└── day7/ → mock exam guide + final review
```

---

## ⚡ Key Resources

| Resource | Link | Priority |
|---|---|---|
| Official Exam Guide | [PDF Link](https://www.databricks.com/sites/default/files/2026-05/databricks-certified-data-engineer-associate-exam-guide-may-2026.pdf) | 🔴 Must |
| Databricks Academy | [academy.databricks.com](https://www.databricks.com/learn/training/home) | 🔴 Must |
| Delta Lake Docs | [docs.delta.io](https://docs.delta.io/latest/index.html) | 🔴 Must |
| Community Edition | [community.cloud.databricks.com](https://community.cloud.databricks.com/) | 🔴 Must |
| Practice Questions (CertSafari) | [certsafari.com](https://www.certsafari.com/databricks/data-engineer-associate) | 🟡 Recommended |
| ExamTopics Discussions | [examtopics.com](https://www.examtopics.com/exams/databricks/certified-data-engineer-associate/view/) | 🟡 Recommended |
| Unity Catalog Docs | [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html) | 🟡 Recommended |

---

## 🎯 Exam Day Tips

1. **Read carefully** — watch for keywords: *MOST efficient*, *BEST practice*, *NOT correct*
2. **Eliminate obviously wrong answers** first
3. **Time management:** ~2 minutes per question (90 min / 45 Qs)
4. **Flag difficult questions** and return to them
5. **Trust first instinct** — don't overthink
6. All questions are worth the same — don't spend >3 min on any one

---

> 💡 **Pro tip from the community:** Focus 60% of your time on Domains 2 & 3 (Ingestion + Processing = 61% of exam). These topics — Auto Loader, Delta Lake MERGE/OPTIMIZE, PySpark transformations — are where most questions come from.
