# Stock Quant Pipeline — Databricks Exam Practice Project

A local, open-source (PySpark + Delta Lake, no Databricks account needed) medallion
pipeline over your Alpaca bars/quotes/trades data, built specifically to drill the
sections you scored lowest on in the Data Engineer Professional exam: **Developing
Code (38%)**, **Cost & Performance Optimization (37%)**, **Debugging and Deploying
(50%)**, plus governance, security, and data quality along the way.

```
Postgres (your real data)  ─┐
                             ├─▶  bronze (raw)  ─▶  silver (clean, deduped)  ─▶  gold (features)  ─▶  ML  ─▶  backtest
synthetic generator (test)  ─┘
```

---

## 0. Before you start: what this can and can't practice

This runs on **open-source Spark + Delta Lake locally**, not a Databricks workspace.
Most of Section 1 (Developing Code) and Section 2 (Cost & Performance) transfers
directly — Delta MERGE, structured streaming, window functions, Pandas UDFs,
Z-Ordering, query plans, testing patterns. A few things are Databricks-managed and
genuinely can't be replicated locally:

| Databricks-only feature | What we do instead here |
|---|---|
| Unity Catalog (3-level namespace, row filters, column masks, lineage) | Spark's built-in catalog (`bronze.*`/`silver.*`/`gold.*` databases) + a hand-written masked view (`src/governance/catalog_metadata.py`) |
| Lakeflow Declarative Pipelines (`@dlt.table`, `EXPECT ... ON VIOLATION`) | Hand-written quality-flag functions + quarantine tables (`src/transform/quality_checks.py`) that do the same job |
| Liquid Clustering | Classic `OPTIMIZE ... ZORDER BY` (still open source, still tested on the exam) |
| Databricks Asset Bundles / Jobs UI | `src/run_pipeline.py` as a plain orchestrator script |
| System tables (query/billing/audit history) | `monitoring.pipeline_runs` Delta table (`src/monitoring/pipeline_monitor.py`) |

**If you want 1:1 practice on the items in the left column**, Databricks Community
Edition is free and this project's logic maps over directly — bronze/silver/gold
Python becomes a Lakeflow pipeline, `catalog_metadata.py`'s masked view becomes a real
`SET MASK`, etc. Doing it here first, then porting the working logic to Community
Edition, is a good use of Week 2 day 12 in your retake study plan.

---

## 1. Setup

```bash
cd stock-quant-project
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# edit .env with your real Postgres credentials
```

**Java:** you need Java 11 or 17 on PATH (`java -version`). Java 21 works too, but
disables Arrow-accelerated Pandas operations by default (see the note in
`src/utils/spark_session.py`) due to a known Arrow/JDK 17+ reflection issue —
everything still runs correctly, just marginally slower on the feature-engineering
step. If you're on Java 8 or 11 and want the speed-up, flip
`spark.sql.execution.arrow.pyspark.enabled` back to `"true"` there.

**First run needs internet access once:** `get_spark()` uses
`configure_spark_with_delta_pip`, which pulls the Delta Lake JVM jars from Maven
Central via Ivy the first time you start a session (a few dozen MB), then caches
them in `~/.ivy2` for every run after that. If you're behind a restrictive corporate
proxy/firewall that blocks Maven Central, you'll need to either allow it through or
pre-download the jars and point `spark.jars` at them directly.

---

## 2. Point it at your real schema

Open `config/config.yaml` and check `postgres.tables` matches your actual table and
column names (defaults assume `bars`/`quotes`/`trades` tables each with `symbol` and
`timestamp` columns). You don't need to touch any Python code for this — everything
downstream reads table/column names from this file.

---

## 3. Run it

### Step 1 — validate the whole pipeline with fake data first (no DB needed)
```bash
python -m src.run_pipeline --mode synthetic --years 3
```
This generates realistic AAPL/NVDA/TSLA/AMD bars/quotes/trades, runs every stage, trains
models, and backtests — so you know the pipeline itself works before pointing it at
real data. Expect the model's accuracy to sit close to 50% and underperform buy-and-hold
here — the synthetic data is a pure random walk with no real signal on purpose, so a
model finding no exploitable edge is the *correct*, expected result, not a bug.

### Step 2 — run against your real Postgres data
```bash
python -m src.run_pipeline --mode postgres
```
Or run stages individually while you're learning/debugging each one:
```bash
python -m src.ingestion.postgres_to_bronze --mode postgres --table bars
python -m src.transform.bronze_to_silver --all
python -m src.transform.silver_to_gold
python -m src.ml.train_model
python -m src.backtest.backtester
```

### Optional: streaming + Delta CDC practice
```bash
python -m src.streaming.streaming_silver_demo --table bars
```
Reads bronze as a stream and MERGEs into silver via `foreachBatch` — the open-source
analog of `APPLY CHANGES INTO` in Lakeflow Declarative Pipelines. Direct practice for
your weakest exam section.

### Optional: cost/performance optimization exercises
```bash
python -m src.optimization.optimize_tables --table gold/features --zorder-by symbol --vacuum
```
Prints the query plan before and after `OPTIMIZE ... ZORDER BY` so you can see file
pruning improve. Do this exercise on day 3–4 of your retake study plan.

### Optional: incremental ingestion (watermark-based, for a scheduled job)
```bash
python -m src.ingestion.postgres_to_bronze --mode incremental --table bars
```

---

## 4. Run the tests
```bash
pytest tests/ -v
```
Covers the quality-check logic and, importantly, a **lookahead-bias test** on the
feature engineering (`test_no_lookahead_in_feature_columns`) — this is the single
most common bug in home-grown trading model projects: a feature that accidentally
uses future information, which makes backtests look great and live trading fail
immediately. Keep this test passing if you modify `feature_engineering.py`.

---

## 5. What's in `src/`

```
src/
  utils/            SparkSession + Delta setup, config loader, Postgres reader
  ingestion/        Postgres -> bronze (batch, incremental, and synthetic test data)
  transform/        bronze -> silver (quality/quarantine/dedup/merge), silver -> gold (features)
  streaming/         Structured Streaming + Delta MERGE demo (optional, exam-focused)
  ml/               Technical indicators (Pandas-based, Java-version-portable), model training + MLflow tracking
  backtest/         Vectorized backtester with Sharpe/CAGR/drawdown/win-rate
  optimization/     OPTIMIZE / ZORDER / VACUUM / query plan comparison
  governance/       Table/column comments, a column-masking view pattern
  monitoring/       pipeline_runs Delta table + a simple quarantine-rate alert
  run_pipeline.py   Orchestrates every stage end to end, in dependency order
```

---

## 6. On the "make the most profit" part

Being direct about this: nothing in this project, or in quantitative trading
generally, can promise profit. What this project *can* give you is a legitimate
research/backtesting framework and, not incidentally, a lot of the exact hands-on
Spark/Delta reps your exam retake needs. A few things worth knowing as you extend it:

- The included strategy is deliberately simple (long/flat on a probability threshold)
  and is a **starting scaffold**, not a finished trading system.
- `test_no_lookahead_in_feature_columns` exists because lookahead bias is the #1 way
  backtests lie to people — any new feature you add should pass an equivalent check.
- A backtest that looks good after you've tuned it against the same held-out period
  several times is measuring your tuning, not the strategy — see the warning at the
  top of `src/backtest/backtester.py`.
- If you ever move toward real capital: paper-trade extensively first, and treat
  every historical result here as "what this rule would have done on this sample,"
  not a forecast. This isn't financial advice, and I'm not a financial advisor.

---

## 7. Extending it

Some natural next steps once the base pipeline is working on your real data:
- Add more technical indicators to `src/ml/feature_engineering.py` (keep the
  lookahead-bias test passing).
- Try walk-forward validation instead of a single time-based split in `train_model.py`.
- Add a short-selling leg to the backtester (currently long/flat only).
- Wire `maybe_alert()` in `monitoring/pipeline_monitor.py` to an actual email/Slack
  webhook instead of a print statement.
- Try the same `add_table_comment` / `create_masked_gold_view` pattern against a real
  Unity Catalog table on Databricks Community Edition to see the native version.


---

## 8. Trading-bot integration notes (additive only)

This project can share the same underlying Postgres data as the [`trading-bot`](https://github.com/wang-yuhao/trading-bot) repo. All integration work here follows one rule: **additive only** -- no existing trading-bot tables are dropped, renamed, or rewritten, and no historical data is re-fetched unless there's a genuine gap.

What's been added so far, and why:

- **`sql/001_additive_indexes.sql`** -- creates `timestamp` indexes on the existing per-symbol-year `*_quotes`/`*_trades`/`*_bars` tables (`CREATE INDEX IF NOT EXISTS`, safe to re-run) so incremental/watermark reads don't have to scan entire tables (some per-symbol quote tables are ~20GB). Also adds a small new `ingestion_watermark` control table for possible future use -- it is **not** required by the current ingestion code (see below).
- **`scripts/startup_incremental_fetch.sh`** -- an idempotent script for WSL startup (wire it up via `crontab -e` with `@reboot`, or run manually after `docker-compose up -d postgres`). It calls the pipeline's existing `--mode incremental` flag (`postgres_to_bronze.py` already computes its own watermark as `max(timestamp)` currently in bronze per table) and then re-runs the MERGE-based `bronze_to_silver.py --all`.

Why not a single unified view across both repos' data? A naive `UNION`/view-based consolidation was considered and rejected: several per-symbol quote tables are already tens of GB, so a blanket view would force expensive full scans on every read. The additive indexing + per-table incremental reads above get the performance benefit without touching existing structure or risking data loss.

Recall commands after a long break (see `scripts/startup_incremental_fetch.sh` for the automated version):

```bash
# 1) bring the DB back up
docker-compose up -d postgres
docker ps                     # confirm the postgres container is healthy

# 2) one-time additive index migration (safe to re-run)
psql -h localhost -U <user> -d <db> -f sql/001_additive_indexes.sql

# 3) incremental catch-up + promote to silver (what the startup script automates)
python -m src.ingestion.postgres_to_bronze --mode incremental --table bars
python -m src.transform.bronze_to_silver --all
```
