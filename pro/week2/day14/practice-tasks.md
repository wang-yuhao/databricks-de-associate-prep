# Day 14 Practice Tasks — Final Review & Mock Exam

> **Exam section:** All sections (comprehensive review)
> **Prerequisite:** Complete days 8-13 before starting.
> **Estimated time:** 4-6 hours
> **Difficulty:** 🔥🔥🔥🔥 Full Exam Simulation

---

## Overview

This is your final preparation day. You will:
1. Review critical concepts via flashcards
2. Take a timed 60-question mock exam
3. Analyze your mistakes
4. Drill your weak areas
5. Prepare mentally for exam day

---

## Task 1 — Morning Flashcard Drill (90 minutes)

📖 **Context:**
Rapid-fire review of high-frequency exam topics.

🛠️ **Instructions:**

Go through these questions out loud. Time yourself: 1 minute per question.

### Delta Lake & Performance (15 questions)

1. What does `OPTIMIZE ZORDER BY` do?
2. Default retention period for `VACUUM`?
3. What is Liquid Clustering?
4. Difference between `RESTORE TABLE` and time travel query?
5. What enables Change Data Feed?
6. When does AQE help performance? When doesn't it?
7. Default broadcast join threshold?
8. What's the difference between Delta Cache and Spark Cache?
9. `repartition()` vs `coalesce()` - which shuffles?
10. Default shuffle partitions?
11. How to calculate optimal shuffle partitions?
12. What is Photon? What workloads benefit most?
13. What does `PushedFilters` in EXPLAIN indicate?
14. When should you use Spot instances?
15. What setting prevents forgotten clusters from running?

### DLT & Streaming (15 questions)

16. `expect` vs `expect_or_drop` vs `expect_or_fail` - behaviors?
17. Does `expect_all_or_drop` drop if ANY or ALL expectations fail?
18. What function implements CDC in DLT?
19. Default SCD type for `apply_changes()`?
20. How to enable SCD Type 2?
21. Target table type required for `apply_changes()`?
22. Where is DLT event log stored?
23. Can you query a `@dlt.view` outside the pipeline?
24. `availableNow` vs `processingTime` trigger?
25. What enables late data handling in streaming?
26. Do stream-stream joins require watermarks?
27. What does `foreachBatch` enable?
28. Difference between Triggered and Continuous pipeline mode?
29. What does full-refresh do?
30. Auto Loader schema evolution modes?

### Unity Catalog & Security (15 questions)

31. Unity Catalog namespace levels?
32. Privilege hierarchy: catalog → schema → ?
33. How to implement column masking?
34. How to implement row-level security?
35. Where are audit logs stored?
36. External location requires what?
37. What's the difference between managed and external tables in Unity Catalog?
38. Do secrets get printed when you `print()` them?
39. Databricks-backed vs Azure Key Vault-backed secret scopes?
40. Why use service principals instead of personal tokens in CI/CD?
41. Which access mode is required for Unity Catalog with Python UDFs?
42. Do IP access lists apply to service principals?
43. Cluster policy type that prevents user changes?
44. What happens to Spark cache when executor dies?
45. What is the `information_schema` used for?

### DABs, CLI & Monitoring (15 questions)

46. What file is the root of a DABs bundle?
47. CLI command to deploy a bundle?
48. `mode: development` vs `mode: production` in DABs?
49. CLI command to trigger immediate job run?
50. CLI command to check run status?
51. SDK v2 main class?
52. How to pass parameters to a job via CLI?
53. What's `run_as` used for in job configuration?
54. Email notification setting for job failures?
55. How to export/import job definitions via CLI?
56. What does `jobs reset` do?
57. How to list all clusters via CLI?
58. How to start a stopped cluster via CLI?
59. What's the difference between `jobs run-now` and `runs get`?
60. How to cancel a running job?

✅ **Expected outcome:**
You can answer all 60 questions in under 60 minutes (1 min each).

---

## Task 2 — Mock Exam Simulation (120 minutes)

📖 **Context:**
Simulate real exam conditions. 60 questions in 120 minutes.

🛠️ **Instructions:**

**Setup:**
1. Find a quiet space
2. Set a 120-minute timer
3. No notes, no Google
4. Take screenshot/photo of each question for later review

**Mock Exam Questions:**

Create a practice exam by combining:
- 13 questions on Performance & Cost (13 × 13% = ~13)
- 10 questions on Data Transformation & Quality
- 10 questions on Monitoring & Alerting
- 10 questions on Security & Compliance
- 7 questions on Data Governance
- 7 questions on Data Ingestion
- 6 questions on Debugging & Deploying
- 5 questions on Data Sharing & Federation
- 4 questions on Data Modelling

**Exam Question Templates:**

Here are example question types you'll see:

**Type 1: Code completion**
```
You need to create a DLT pipeline that drops invalid rows. Which decorator should you use?

A) @dlt.expect("valid_amount", "amount > 0")
B) @dlt.expect_or_drop("valid_amount", "amount > 0")
C) @dlt.expect_or_fail("valid_amount", "amount > 0")
D) @dlt.validate("valid_amount", "amount > 0")

Answer: B
```

**Type 2: Scenario-based**
```
Your streaming job is failing with OOM errors during a broadcast join. 
What should you check first?

A) Increase executor memory
B) Check broadcast join threshold
C) Enable AQE
D) Increase shuffle partitions

Answer: B (likely table > 10MB threshold)
```

**Type 3: Best practice**
```
Which authentication method should you use for production CI/CD pipelines?

A) Personal access token
B) Username and password
C) Service principal
D) API key

Answer: C
```

**Type 4: Command/syntax**
```
What CLI command triggers an immediate job run with parameters?

A) databricks jobs start --job-id 123 --params '{"env": "prod"}'
B) databricks jobs run-now --job-id 123 --notebook-params '{"env": "prod"}'
C) databricks runs create --job-id 123 --params '{"env": "prod"}'
D) databricks jobs execute --job-id 123 --args '{"env": "prod"}'

Answer: B
```

✅ **Expected outcome:**
Complete 60 questions in 120 minutes. Target: 70%+ correct (42/60).

---

## Task 3 — Mistake Analysis (60 minutes)

📖 **Context:**
Learning from mistakes is more valuable than getting things right.

🛠️ **Instructions:**

For EVERY incorrect answer:

1. **Write down the question**
2. **Why did you get it wrong?**
   - Didn't know the concept?
   - Misread the question?
   - Confused two similar concepts?
3. **What's the correct answer and why?**
4. **What similar questions might appear?**

**Example mistake analysis:**

```
Question: What does `expect_all_or_drop` do if ONE expectation fails?

My answer: Keeps the row (Wrong!)
Correct answer: Drops the row

Why I got it wrong:
- Confused with `expect_all` (which warns)
- Thought "all" meant "all must fail" - but it means "evaluates all, drops if ANY fails"

Similar questions to watch for:
- expect vs expect_or_drop vs expect_or_fail
- expect_all vs expect_all_or_drop
- ANY vs ALL logic

Mnemonic: "_or_drop" suffix = DROP the row
```

✅ **Expected outcome:**
You have a personalized "weak spots" document to drill before exam.

---

## Task 4 — Targeted Weak Spot Drill (90 minutes)

📖 **Context:**
Drill your weakest topics until they become strengths.

🛠️ **Instructions:**

Based on your mistake analysis, spend 90 minutes drilling:

**If you struggled with DLT:**
- Re-create the day12 DLT expectations notebook
- Practice CDC with `apply_changes()` from memory
- Query the event log without notes

**If you struggled with Performance:**
- Re-create the day8 AQE comparison
- Practice calculating optimal shuffle partitions
- Explain EXPLAIN output

**If you struggled with Security/CLI:**
- Practice all databricks CLI commands from memory
- Create secrets and use in notebook
- Configure cluster policy from scratch

**If you struggled with Unity Catalog:**
- Write GRANT/REVOKE statements from memory
- Implement column masking
- Query audit logs

✅ **Expected outcome:**
Your weak areas are now strong areas.

---

## Task 5 — Exam Day Preparation Checklist

📖 **Context:**
Mental and logistical preparation is as important as technical knowledge.

🛠️ **Instructions:**

### Night Before Exam

- [ ] Get 7-8 hours of sleep
- [ ] Review flashcards one final time (30 min max)
- [ ] Prepare workspace: quiet room, stable internet
- [ ] Test webcam, microphone, screen sharing (for proctored exam)
- [ ] Have ID ready
- [ ] Close all applications except exam browser

### Morning of Exam

- [ ] Eat a good breakfast
- [ ] Do NOT cram - you know this material
- [ ] Arrive at workspace 30 min early
- [ ] Use bathroom before starting
- [ ] Have water nearby

### During Exam

- [ ] Read EVERY question carefully
- [ ] Watch for keywords: "NOT", "EXCEPT", "MUST", "CAN"
- [ ] If stuck, flag and move on - come back later
- [ ] Don't second-guess yourself too much
- [ ] Manage time: 2 min/question average
- [ ] Review flagged questions at end

### Common Exam Traps to Watch For

⚠️ **Trap 1:** "Which of the following is NOT required?"
- Read carefully - they want the EXCEPTION

⚠️ **Trap 2:** Very similar answer choices
- `expect` vs `expect_or_drop`
- `repartition` vs `coalesce`
- `fixed` vs `range` policy types

⚠️ **Trap 3:** Deprecated features
- `once` trigger (use `availableNow` instead)
- "No Isolation Shared" cluster mode (deprecated)

⚠️ **Trap 4:** Default values
- Broadcast threshold: 10MB
- Shuffle partitions: 200
- VACUUM retention: 7 days

---

## Final Knowledge Checklist

### Core Concepts You MUST Know Cold

**Delta Lake:**
- [ ] OPTIMIZE, VACUUM, RESTORE, ZORDER
- [ ] Change Data Feed: enable and query
- [ ] Liquid Clustering vs partitioning
- [ ] Time travel syntax

**Performance:**
- [ ] AQE: 3 optimizations
- [ ] Broadcast join threshold and OOM risks
- [ ] repartition() vs coalesce()
- [ ] Photon use cases
- [ ] EXPLAIN output reading

**DLT:**
- [ ] expect vs expect_or_drop vs expect_or_fail
- [ ] apply_changes() for CDC
- [ ] SCD Type 1 vs Type 2
- [ ] Event log location and queries
- [ ] Triggered vs Continuous modes

**Streaming:**
- [ ] availableNow vs processingTime triggers
- [ ] Watermarks and late data
- [ ] foreachBatch use cases
- [ ] Auto Loader schema evolution
- [ ] Stream-stream join requirements

**Unity Catalog:**
- [ ] 3-level namespace
- [ ] GRANT/REVOKE syntax
- [ ] Column masking and row filters
- [ ] system.access.audit queries
- [ ] External locations + storage credentials

**Security:**
- [ ] Secrets: dbutils.secrets.get()
- [ ] Service principals for CI/CD
- [ ] Cluster access modes
- [ ] IP access lists
- [ ] Cluster policies

**DABs & CLI:**
- [ ] databricks.yml structure
- [ ] jobs run-now vs runs get
- [ ] Bundle deploy workflow
- [ ] SDK v2 WorkspaceClient

---

## Scoring & Next Steps

### Mock Exam Score Interpretation

| Score | Readiness | Action |
|-------|-----------|--------|
| 80%+ (48/60) | Excellent | Schedule exam this week |
| 70-79% (42-47) | Good | Drill weak areas 1 more day, then schedule |
| 60-69% (36-41) | Okay | Spend 2-3 more days on weak topics |
| <60% (<36) | Not ready | Review entire week 2 content again |

### Post-Exam (After You Pass!)

✅ **Update your LinkedIn:**
- Databricks Certified Data Engineer Professional
- Add certificate to profile

✅ **Update your resume:**
- List certification with exam date
- Highlight: "Databricks Certified Data Engineer Professional"

✅ **Leverage in interviews:**
- Certification proves hands-on Databricks expertise
- Discuss real scenarios from practice tasks
- Reference Unity Catalog, DLT, DABs experience

---

## Final Words of Encouragement

You've spent 14 days mastering:
- Advanced Delta Lake operations
- Auto Loader and complex ingestion patterns
- DLT with expectations and CDC
- Performance tuning and cost optimization  
- Unity Catalog governance
- Security best practices
- DABs and CI/CD
- Databricks CLI and SDK

**You are ready.**

The Professional exam is challenging, but you've done the work. Trust your preparation.

Good luck! 🚀

---

**Next step:** Schedule your exam at [Databricks Academy](https://academy.databricks.com/) or [Kryterion](https://www.kryteriononline.com/).

**Exam format:**
- 60 multiple-choice questions
- 120 minutes
- 70% passing score (42/60)
- Proctored online or at test center
- Results immediately after completion
