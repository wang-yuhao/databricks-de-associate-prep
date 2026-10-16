# Day 14: Final Review & Exam Preparation

## Schedule
- Morning: Full review of weak areas + flashcard drill
- Afternoon: Mock exam simulation (60 questions, 120 min)
- Evening: Review incorrect answers, consolidate notes

---

## 14.1 Comprehensive Topic Review

### Delta Lake Advanced
- `MERGE INTO`: upsert with matched/not-matched conditions
- `RESTORE TABLE`: point-in-time recovery using version or timestamp
- `OPTIMIZE ZORDER BY`: collocate related data for faster queries
- Change Data Feed: `delta.enableChangeDataFeed = true`, read with `readChangeFeed`
- Liquid Clustering: replaces partitioning, use `CLUSTER BY (col)`
- `VACUUM`: removes old files, default 7-day retention

### Streaming
- `availableNow=True` trigger: batch-mode streaming (replaces `once`)
- Watermarks: enable late data handling + bounded state
- `foreachBatch`: custom sink logic, supports MERGE
- Auto Loader: `cloudFiles` format, directory listing vs file notification mode
- Stream-stream joins: both sides need watermarks

### DLT / Lakeflow Pipelines
- `@dlt.expect` / `@dlt.expect_or_drop` / `@dlt.expect_or_fail`
- `dlt.apply_changes()`: CDC with SCD Type 1 (default) or Type 2
- `dlt.read()` vs `dlt.read_stream()`: batch vs streaming
- Event log at `/pipelines/<id>/system/events`
- Triggered vs Continuous execution modes

### Unity Catalog
- 3-level namespace: `catalog.schema.table`
- GRANT/REVOKE: USE CATALOG → USE SCHEMA → SELECT/MODIFY
- Column masking: `ALTER COLUMN ... SET MASK <function>`
- Row filters: `SET ROW FILTER <function> ON (<col>)`
- Audit: `system.access.audit`
- External locations require storage credentials

### DABs & CI/CD
- `databricks.yml`: root config with `bundle`, `targets`, `variables`, `resources`
- `mode: development`: username-prefixed resources
- `mode: production`: strict settings, no personal clusters
- GitHub Actions: secrets for DATABRICKS_HOST, DATABRICKS_TOKEN
- `databricks bundle validate` → `deploy` → `run`

### Performance & Cost
- AQE: automatic join strategy, skew handling, coalescing
- Broadcast join threshold: `spark.sql.autoBroadcastJoinThreshold` (10MB)
- `repartition()` = full shuffle; `coalesce()` = no shuffle
- Photon: C++ vectorized engine; auto on for SQL/Delta
- Spark Cache: executor memory; Delta Cache: SSD raw files
- Shuffle partitions default 200; tune with AQE or set explicitly

### Security
- Secrets: `dbutils.secrets.get(scope, key)` - redacted in output
- Service principals: for CI/CD and production jobs
- Cluster policies: enforce configurations, prevent overspending
- Single User mode: required for UC with ML; Shared mode: UC enforced

---

## 14.2 Common Exam Traps

| Trap | Correct Answer |
|------|----------------|
| `once` trigger | Use `availableNow=True` instead |
| DLT expect behavior | `expect` = warn, `expect_or_drop` = drop, `expect_or_fail` = fail |
| Delta VACUUM default | 7-day retention, cannot vacuum below `delta.deletedFileRetentionDuration` |
| Broadcast join hint | `broadcast()` forces join regardless of size threshold |
| Unity Catalog metastore | One per region; workspace attaches to metastore |
| APPLY CHANGES INTO | Requires `dlt.create_streaming_table()` not `@dlt.table` |
| Checkpoint reuse | NEVER reuse checkpoint for different streaming queries |
| mode: development | Prepends username, no auto-retry |
| `coalesce` vs `repartition` | coalesce = no shuffle (fewer partitions only), repartition = full shuffle |
| Secret scope types | Databricks-backed or Azure Key Vault-backed |

---

## 14.3 Quick Reference: Key Commands

```sql
-- Delta
DESCRIBE HISTORY orders;
RESTORE TABLE orders TO VERSION AS OF 5;
OPTIMIZE orders ZORDER BY (customer_id);
VACUUM orders RETAIN 168 HOURS;
ALTER TABLE orders SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Unity Catalog
GRANT SELECT ON TABLE catalog.schema.table TO `group`;
ALTER TABLE t ALTER COLUMN email SET MASK mask_fn;
SHOW GRANTS ON TABLE catalog.schema.table;

-- DLT CLI
databricks pipelines start --pipeline-id <id>
databricks pipelines start --pipeline-id <id> --full-refresh

-- DABs
databricks bundle validate
databricks bundle deploy --target prod
databricks bundle run job_name --target prod
```

---

## 14.4 Exam Day Tips

- Read every option carefully - many are close but one key word differs
- `APPLY CHANGES INTO` vs `MERGE INTO`: DLT vs standard SQL
- DLT tables are always Delta format under the hood
- Unity Catalog replaces Hive metastore - they don't coexist easily
- Jobs API v2.1 is current; always use `tasks` not `task`
- AQE is enabled by default in DBR 3.0+
- `spark.conf.set()` changes take effect in current session only
- Photon cannot be disabled per-query; it's cluster-level
- `dbutils.fs` operations are driver-side, not distributed
- CLONE creates a full copy; SHALLOW CLONE shares data files

---

## 14.5 Score Tracking Template

| Domain | Score | Target | Status |
|--------|-------|--------|--------|
| Delta Lake | /10 | 8/10 | |
| Streaming | /10 | 8/10 | |
| DLT Pipelines | /10 | 8/10 | |
| Unity Catalog | /10 | 8/10 | |
| DABs & CI/CD | /10 | 8/10 | |
| Performance | /10 | 8/10 | |
| Security | /10 | 8/10 | |
| **Total** | **/70** | **56/70** | |

---

## Practice Tasks
- [ ] Complete full 60-question mock exam in 120 minutes
- [ ] Review all incorrect answers with explanation
- [ ] Drill flashcards on DLT expectations and Unity Catalog privileges
- [ ] Practice MERGE INTO and APPLY CHANGES INTO syntax from memory
- [ ] Review DABs YAML structure without reference
