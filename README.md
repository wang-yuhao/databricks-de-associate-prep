# Databricks Data Engineering Prep

Study material and hands-on labs for Databricks data engineering, from the Associate certification to the
**Advanced Data Engineering with Databricks** course series.

| Track | Folder | What's inside | Runs on |
|---|---|---|---|
| **Associate** certification | [`associate/`](associate/README.md) | 7-day plan: study notes, practice tasks, notebooks, 3 mock exams, cheatsheet | Databricks workspace |
| **Professional** certification | [`professional/`](professional/README.md) | 14-day plan: study notes, practice tasks, notebooks, 4 mock exams, cheatsheet | Databricks workspace |
| **Advanced Data Engineering** (Databricks Academy) | [`advanced-data-engineering/`](advanced-data-engineering/README.md) | The 4 courses below as runnable demos and labs, plus a local runtime that emulates the Databricks features they use | **Your laptop** (free), optional Databricks Free Edition |
| Practice project | [`stock-quant-project/`](stock-quant-project/README.md) | End-to-end medallion pipeline on real market data (PySpark + Delta) | Your laptop |

### Advanced Data Engineering with Databricks

| # | Course | Demos and labs |
|---|---|---|
| 1 | [Advanced Techniques with Spark Declarative Pipelines](advanced-data-engineering/01-advanced-sdp/README.md) | multi-flow + Liquid Clustering + expectations · multiplex + Delta sinks + Iceberg reads · AUTO CDC SCD2 · advanced data quality · e-commerce lab |
| 2 | [Databricks Data Privacy](advanced-data-engineering/02-data-privacy/README.md) | row filters and column masks · PII pseudonymization and anonymization · CDF delete propagation · CDF lab |
| 3 | [Databricks Performance Optimization](advanced-data-engineering/03-performance-optimization/README.md) | Spark UI · file explosion · data skipping and Liquid Clustering lab · shuffle · exploding join lab (skew and spill) · UDFs |
| 4 | [Automated Deployment with Declarative Automation Bundles](advanced-data-engineering/04-automated-deployment-dabs/README.md) | auth · simple DAB · multi-environment DAB · templates · CI/CD with GitHub Actions · ML in workflows |

**Start here:** [`advanced-data-engineering/README.md`](advanced-data-engineering/README.md) → *Setup* (about 10 minutes, one time). On **Databricks Free Edition**: [`advanced-data-engineering/FREE_EDITION_GUIDE.md`](advanced-data-engineering/FREE_EDITION_GUIDE.md).

## Repository layout

```
.
├── associate/                    Data Engineer Associate: 7-day plan
├── professional/                 Data Engineer Professional: 14-day plan
├── advanced-data-engineering/    Databricks Academy "Advanced Data Engineering" series, runnable locally
│   ├── setup/                    setup_env.ps1 · start_lab.ps1 · stop_spark.ps1 · setup_env.sh
│   ├── adelab/                   local runtime: Spark+Delta session, SDP engine, classroom helpers
│   ├── 01-advanced-sdp/          notebooks · pipelines · solutions · databricks (SQL versions)
│   ├── 02-data-privacy/          notebooks · solutions · databricks (Unity Catalog SQL)
│   ├── 03-performance-optimization/  notebooks · solutions
│   ├── 04-automated-deployment-dabs/ notebooks · bundles · labs · solutions
│   └── tests/                    pytest suite for the local runtime
└── stock-quant-project/          independent practice project
```
