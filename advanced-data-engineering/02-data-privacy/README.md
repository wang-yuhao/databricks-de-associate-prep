# Course 2: Databricks Data Privacy

Protect sensitive data in the lakehouse with Unity Catalog controls, pseudonymization and anonymization, and compliant deletes using Change Data Feed.

**Prerequisites:** Delta Lake (MERGE, time travel, VACUUM), Structured Streaming, basic Unity Catalog.

## Lesson map

| Official lesson | Practice here |
|---|---|
| Regulatory Compliance · Data Privacy · Key Concepts and Components | [Key concepts](#key-concepts) |
| Audit Your Data · Data Isolation | Notebook 2.1, sections I-J |
| **Demo:** Securing Data in Unity Catalog | [`notebooks/2.1-demo-securing-data-in-unity-catalog`](notebooks/2.1-demo-securing-data-in-unity-catalog.ipynb) · native SQL: [`databricks/sql/2.1-row-filters-column-masks.sql`](databricks/sql/2.1-row-filters-column-masks.sql) |
| Pseudonymization & Anonymization · Summary & Best Practices | [Key concepts](#pseudonymization-vs-anonymization) |
| **Demo:** PII Data Security | [`notebooks/2.2-demo-pii-data-security`](notebooks/2.2-demo-pii-data-security.ipynb) |
| Capturing Changed Data · Deleting Data in Databricks | [Key concepts](#deleting-data-for-real) |
| **Demo:** Processing Records from CDF and Propagating Changes | [`notebooks/2.3-demo-processing-records-from-cdf-and-propagating-changes`](notebooks/2.3-demo-processing-records-from-cdf-and-propagating-changes.ipynb) |
| **Lab:** Propagating Changes with CDF | [`notebooks/2.4-lab-propagating-changes-with-cdf`](notebooks/2.4-lab-propagating-changes-with-cdf.ipynb) · [`solutions/`](solutions/2.4-lab-propagating-changes-with-cdf-solution.ipynb) |

## Local emulation of Unity Catalog
Unity Catalog is a managed service, so the local runtime emulates only what the demos need:
- `adelab.privacy.use_identity(spark, "eu_analyst")` switches the simulated user. It re-registers **`is_account_group_member()`**, so policy functions written exactly as on Databricks return persona-specific results. (`current_user()` is a Spark built-in that can't be overridden. Use groups, which is also the recommended practice.)
- Row filters and column masks are written as **SQL UDFs**, identical to UC. Open-source Delta has no `SET ROW FILTER` / `SET MASK`, so they are applied through a **dynamic view**. The notebook shows the native `ALTER TABLE` statements next to each step.
- GRANTs, tags, audit and lineage system tables need a workspace. Use the SQL file in `databricks/sql/` on Databricks Free Edition.

## Key concepts

### Regulations and principles
- **GDPR** (EU) and **CCPA/CPRA** (California): lawful basis, purpose limitation, **data minimization**, storage limitation, the rights of access, rectification and **erasure** ("right to be forgotten", typically within one month), breach notification. HIPAA and PCI-DSS add sector rules.
- **PII**: direct identifiers (name, email, SSN) and **quasi-identifiers** (zip, birth date, gender) that identify a person in combination.
- Privacy by design: classify data at ingestion, minimize what you store, restrict access by default, and make deletion an engineered pipeline, not an ad-hoc script.

### Unity Catalog building blocks
- Securables: metastore → catalog → schema → table / view / volume / function / model. Privileges (`USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`, ...) are inherited downward. Grant to **groups**.
- **Row filters** and **column masks**: SQL UDFs attached to a table, evaluated per querying user. **Dynamic views** are the older alternative.
- **Tags** and **Data Classification** (find and label PII), **ABAC** policies driven by tags.
- **Audit**: `system.access.audit`. **Lineage**: `system.access.table_lineage` / `column_lineage`. **History**: `DESCRIBE HISTORY`.

### Data isolation
Catalog per environment, region, or business unit, each with its own **managed storage location** (data residency). **Workspace-catalog binding** limits which workspaces can use a catalog. Use separate service principals per environment.

### Pseudonymization vs. anonymization
| | Pseudonymization | Anonymization |
|---|---|---|
| Techniques | salted / keyed hashing, tokenization (vault) | generalization, suppression, perturbation, aggregation |
| Re-identifiable? | yes, with the key or vault | no (verify k-anonymity, l-diversity) |
| Still personal data under GDPR? | **yes** | no |

### Deleting data for real
- `DELETE` creates a new table version. Old files stay for **time travel** until `VACUUM` (default retention 7 days). **Deletion vectors** only mark rows as deleted until the files are rewritten (`REORG TABLE ... APPLY (PURGE)`, or OPTIMIZE).
- **Change Data Feed** (`delta.enableChangeDataFeed = true`): row-level `insert / update_preimage / update_postimage / delete` with `_commit_version`. Read with `table_changes()` or `readChangeFeed`. Use it to **propagate deletes** downstream incrementally. CDF files also hold deleted data until VACUUM.
- Streaming readers of a table that receives deletes/updates need `skipChangeCommits`, or must read the change feed.
