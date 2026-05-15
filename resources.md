# 📚 Learning Resources — Databricks DE Associate

## ☁️ Azure Databricks Resources

| Resource | Link |
|---|---|
| Azure Databricks Docs | [learn.microsoft.com/azure/databricks](https://learn.microsoft.com/en-us/azure/databricks/) |
| ADLS Gen2 Integration | [Azure Storage docs](https://docs.databricks.com/en/connect/storage/azure-storage.html) |
| Unity Catalog on Azure | [UC Azure setup](https://docs.databricks.com/en/data-governance/unity-catalog/azure.html) |
| Azure Key Vault Secrets | [Secret scopes](https://docs.databricks.com/en/security/secrets/secret-scopes.html) |
| Databricks Asset Bundles | [DABs docs](https://docs.databricks.com/en/dev-tools/bundles/index.html) |
| Delta Live Tables | [DLT on Azure](https://docs.databricks.com/en/delta-live-tables/index.html) |
| Lakeflow Jobs | [Workflows](https://docs.databricks.com/en/workflows/index.html) |
| Azure Databricks Pricing | [Pricing page](https://azure.microsoft.com/en-us/pricing/details/databricks/) |
| Service Principal Auth | [SP setup](https://docs.databricks.com/en/connect/storage/azure-storage.html#service-principal) |

---

## 🎓 Official Databricks Training (FREE)

| Course | Link | Duration | Relevant Days |
|---|---|---|---|
| Data Engineering with Databricks | [Databricks Academy](https://www.databricks.com/learn/training/data-engineering) | ~8h | All |
| Apache Spark Programming with Databricks | [Academy](https://www.databricks.com/learn/training/apache-spark-programming) | ~6h | Day 2–3 |
| Delta Lake Fundamentals | [Academy](https://www.databricks.com/learn/training/delta-lake-fundamentals) | ~3h | Day 3 |
| Databricks Lakehouse Fundamentals | [Academy](https://www.databricks.com/learn/training/lakehouse-fundamentals) | ~2h | Day 1 |

> 🔑 **Login required:** [Databricks Academy Login](https://www.databricks.com/learn/training/login) — free account

---

## 📖 Documentation Deep Dives

### Day 1 — Platform
- [Databricks Architecture Overview](https://docs.databricks.com/en/getting-started/overview.html)
- [Cluster Types and Configurations](https://docs.databricks.com/en/compute/index.html)
- [Unity Catalog Overview](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [External Locations & Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [Azure Databricks Workspace Setup](https://learn.microsoft.com/en-us/azure/databricks/getting-started/)

### Day 2 — Ingestion & SQL/PySpark
- [Read and Write Data](https://docs.databricks.com/en/ingestion/index.html)
- [Auto Loader Documentation](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
- [DataFrame API (PySpark)](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html)
- [Databricks SQL](https://docs.databricks.com/en/sql/index.html)
- [Unity Catalog Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)

### Day 3 — Delta Lake
- [Delta Lake Guide](https://docs.databricks.com/en/delta/index.html)
- [MERGE INTO](https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html)
- [Delta Time Travel](https://docs.databricks.com/en/delta/history.html)
- [OPTIMIZE and ZORDER](https://docs.databricks.com/en/delta/optimize.html)
- [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)
- [Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html)

### Day 4 — Structured Streaming
- [Structured Streaming Guide](https://docs.databricks.com/en/structured-streaming/index.html)
- [Trigger Types](https://docs.databricks.com/en/structured-streaming/triggers.html)
- [Windowed Aggregations](https://docs.databricks.com/en/structured-streaming/window.html)
- [Streaming Checkpoints](https://docs.databricks.com/en/structured-streaming/checkpoints.html)
- [Auto Loader as Streaming Source](https://docs.databricks.com/en/ingestion/auto-loader/index.html)

### Day 5 — DLT, Jobs & CI/CD
- [Delta Live Tables Overview](https://docs.databricks.com/en/delta-live-tables/index.html)
- [DLT Python API Reference](https://docs.databricks.com/en/delta-live-tables/python-ref.html)
- [DLT SQL Reference](https://docs.databricks.com/en/delta-live-tables/sql-ref.html)
- [DLT Expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)
- [Lakeflow Jobs (Workflows)](https://docs.databricks.com/en/workflows/index.html)
- [Task Values (taskValues API)](https://docs.databricks.com/en/workflows/jobs/share-task-context.html)
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [GitHub Actions CI/CD with DABs](https://docs.databricks.com/en/dev-tools/bundles/ci-cd.html)

### Day 6 — Unity Catalog & Governance
- [Unity Catalog on Azure](https://docs.databricks.com/en/data-governance/unity-catalog/azure.html)
- [Unity Catalog Privileges Reference](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html)
- [Row Filters and Column Masks](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html)
- [Data Lineage](https://docs.databricks.com/en/data-governance/unity-catalog/data-lineage.html)
- [Audit Logging](https://docs.databricks.com/en/administration-guide/account-settings/audit-logs.html)

---

## 📝 Practice Exams & Question Banks

| Resource | Type | Link |
|---|---|---|
| CertSafari | Practice questions | [certsafari.com](https://www.certsafari.com/exams/databricks-certified-data-engineer-associate) |
| Udemy — Derar Alhussein | Full course + exam | [Udemy listing](https://www.udemy.com/course/databricks-certified-data-engineer-associate-practice-exams/) |
| Databricks Sample Questions | Official | [Exam guide PDF](https://www.databricks.com/sites/default/files/2024-04/Exam-Guide-Databricks-Certified-Data-Engineer-Associate.pdf) |

---

## 🛠️ Free Tools for Hands-On Practice

| Tool | Purpose | Link |
|---|---|---|
| Azure Free Account | Host your workspace | [azure.microsoft.com/free](https://azure.microsoft.com/en-us/free/) |
| Databricks Community Edition | SQL/DLT concepts only | [community.cloud.databricks.com](https://community.cloud.databricks.com) |
| Databricks CLI | DABs deploy/run | [CLI docs](https://docs.databricks.com/en/dev-tools/cli/index.html) |
| DBeaver (free) | SQL client for testing | [dbeaver.io](https://dbeaver.io) |

---

## 📅 Recommended Daily Schedule

| Time | Activity |
|---|---|
| Morning (2h) | Read `study-notes.md` for the day |
| Mid-morning (2h) | Watch Databricks Academy videos |
| Afternoon (2h) | Complete `practice-tasks.md` in Azure Databricks |
| Evening (1h) | CertSafari practice questions (filter by today's domain) |

---

## 🔗 Exam Registration

- **Register:** [Databricks Certification Portal](https://www.webassessor.com/databricks)
- **Exam guide PDF:** [DE Associate Exam Guide](https://www.databricks.com/sites/default/files/2024-04/Exam-Guide-Databricks-Certified-Data-Engineer-Associate.pdf)
- **Cost:** $200
- **Duration:** 90 minutes, 45 questions, multiple-choice
- **Passing score:** ~70% (not officially published; ~32/45 is typically safe)
