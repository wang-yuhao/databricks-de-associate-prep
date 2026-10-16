# Databricks Professional Mock Exam

60 questions | 120 minutes | Passing score: ~70%

---

## Instructions
- Complete all 60 questions in 120 minutes
- Multiple choice and multi-select format
- Mark your answers and check against answer key at bottom
- Simulate exam conditions: no notes, no internet

---

## Questions 1-20: Delta Lake & Streaming

1. Which trigger type should you use to process all available data once and then stop?
   - A) `.trigger(once=True)`
   - B) `.trigger(availableNow=True)`
   - C) `.trigger(processingTime="0 seconds")`
   - D) `.trigger(continuous="1 second")`

2. What is the default retention period for VACUUM?
   - A) 24 hours
   - B) 7 days
   - C) 30 days
   - D) 90 days

3. Which output mode should you use for a windowed aggregation with watermark in streaming?
   - A) complete
   - B) update
   - C) append
   - D) replace

4. How do you enable Change Data Feed on a Delta table?
   - A) `ALTER TABLE t SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`
   - B) `CREATE TABLE t (...) WITH CHANGE FEED`
   - C) `SET delta.changeDataFeed = true`
   - D) Change Data Feed is enabled by default

5. What command applies Z-ordering to a Delta table?
   - A) `ZORDER t BY (col)`
   - B) `OPTIMIZE t ZORDER BY (col)`
   - C) `ALTER TABLE t ZORDER (col)`
   - D) `CLUSTER t BY (col)`

---

## Questions 21-40: DLT & Unity Catalog

21. Which DLT expectation will stop the pipeline if violated?
   - A) `@dlt.expect()`
   - B) `@dlt.expect_or_drop()`
   - C) `@dlt.expect_or_fail()`
   - D) `@dlt.expect_halt()`

22. For `dlt.apply_changes()`, what type of table must the target be?
   - A) Created with `@dlt.table`
   - B) Created with `@dlt.view`
   - C) Created with `dlt.create_streaming_table()`
   - D) Any Delta table

23. In Unity Catalog, what is the three-level namespace?
   - A) workspace.database.table
   - B) catalog.schema.table
   - C) metastore.catalog.table
   - D) database.schema.table

24. How do you apply a column mask in Unity Catalog?
   - A) `ALTER TABLE t ALTER COLUMN col SET MASK func`
   - B) `CREATE MASK func ON TABLE t(col)`
   - C) `GRANT MASK func TO col ON t`
   - D) Masks are auto-applied based on user role

25. Where is the DLT event log stored?
   - A) `/pipelines/<id>/events`
   - B) `/pipelines/<id>/system/events`
   - C) `/system/pipelines/events/<id>`
   - D) `system.pipelines.events`

---

## Questions 41-60: DABs, Performance & Security

41. In DABs, what does `mode: development` do?
   - A) Disables auto-retry and prepends username to resources
   - B) Enables verbose logging
   - C) Allows personal clusters in production
   - D) Sets development as the default target

42. What is the default broadcast join threshold in Spark?
   - A) 1MB
   - B) 10MB
   - C) 100MB
   - D) 1GB

43. Which command forces a broadcast join regardless of size?
   - A) `df.hint("broadcast")`
   - B) `broadcast(df)`
   - C) `df.broadcast()`
   - D) All of the above

44. What is the difference between `repartition()` and `coalesce()`?
   - A) `repartition()` = full shuffle; `coalesce()` = no shuffle
   - B) They are identical
   - C) `coalesce()` can increase partitions; `repartition()` cannot
   - D) `repartition()` is deprecated

45. How are secrets displayed when printed in Databricks?
   - A) Encrypted hash
   - B) `[REDACTED]`
   - C) `***`
   - D) Plain text

46. Which cluster mode is required for Unity Catalog with ML features?
   - A) Shared
   - B) Standard
   - C) Single User
   - D) High Concurrency

47. What is the default number of shuffle partitions in Spark?
   - A) 100
   - B) 200
   - C) 400
   - D) Automatic (depends on data size)

48. How do you read Change Data Feed between versions 5 and 10?
   - A) `SELECT * FROM table_changes('t', 5, 10)`
   - B) `SELECT * FROM cdf('t', 5, 10)`
   - C) `DESCRIBE CHANGES t FROM 5 TO 10`
   - D) Change Data Feed doesn't support version ranges

49. In DABs, which file is the root configuration?
   - A) `bundle.yml`
   - B) `config.yml`
   - C) `databricks.yml`
   - D) `settings.json`

50. What does AQE stand for?
   - A) Automatic Query Execution
   - B) Adaptive Query Execution
   - C) Advanced Query Engine
   - D) Accelerated Query Execution

---

## Answer Key

1. B  | 11. - | 21. C | 31. - | 41. A | 51. - |
2. B  | 12. - | 22. C | 32. - | 42. B | 52. - |
3. C  | 13. - | 23. B | 33. - | 43. B | 53. - |
4. A  | 14. - | 24. A | 34. - | 44. A | 54. - |
5. B  | 15. - | 25. B | 35. - | 45. B | 55. - |
6. -  | 16. - | 26. - | 36. - | 46. C | 56. - |
7. -  | 17. - | 27. - | 37. - | 47. B | 57. - |
8. -  | 18. - | 28. - | 38. - | 48. A | 58. - |
9. -  | 19. - | 29. - | 39. - | 49. C | 59. - |
10. - | 20. - | 30. - | 40. - | 50. B | 60. - |

---

## Scoring Guide

- 50-60: Excellent - Ready for exam
- 42-49: Good - Review weak areas
- 35-41: Pass threshold - More study needed
- <35: Not ready - Complete full study plan

---

**Note**: This is a sample mock exam. The full version would contain 60 questions covering all exam domains per the official exam outline.
