# Day 1 Practice Tasks — Databricks Platform & Architecture

## Task 1 — Cluster Configuration
Create a cluster config (JSON or YAML snippet) that:
- Uses Databricks Runtime 15.4 LTS
- Is a Standard cluster with auto-termination after 20 minutes
- Has Spark config: `spark.sql.shuffle.partitions = 8`
- Uses `Standard_DS3_v2` nodes

> Write the JSON/YAML below and explain every field choice.

```json
// Your answer here
```

---

## Task 2 — Cluster Access Mode Quiz
For each scenario, choose the correct Access Mode and justify your answer:

| Scenario | Access Mode | Why? |
|---|---|---|
| Single analyst running exploratory SQL | | |
| Team of 5 engineers sharing a cluster for ETL | | |
| ML job using a third-party library not supported by Unity Catalog | | |
| Pipeline needing row-level security via Unity Catalog | | |

---

## Task 3 — Repos & Git Branching
1. In your Databricks Repos, clone the repo `databricks-de-associate-prep`.
2. Create a feature branch called `feature/day1-practice`.
3. Make a small change to a notebook, commit it, and push.
4. Write the CLI commands you would use to achieve the same steps via the Databricks CLI.

```bash
# Your CLI commands here
```

---

## Task 4 — dbutils Exploration
In a notebook, run the following and record the output:
```python
dbutils.help()
dbutils.fs.help()
dbutils.fs.ls("dbfs:/")
dbutils.secrets.listScopes()
```
> Paste or describe what you observed.

---

## Task 5 — Concept Short-Answer
Answer in 2–3 sentences each:
1. What is the difference between **All-Purpose** and **Job** clusters?
2. Why is the **Driver** node a single point of failure, and how do Databricks Jobs mitigate this?
3. What does Unity Catalog add on top of the Hive metastore?
