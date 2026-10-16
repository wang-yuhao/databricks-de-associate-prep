# 📋 Exam Day Checklist

## 🗓️ The Day Before

- [ ] **Complete all 3 mock exams** — aim for 75%+ on all three
- [ ] **Review cheatsheet.md** — especially sections you keep missing
- [ ] **Review the Quick Hits** in day7/study-notes.md
- [ ] **Prepare your environment** (see technical checklist below)
- [ ] **Get 7–8 hours of sleep** — cognitive performance drops significantly with less
- [ ] **Set two alarms** so you don't oversleep

---

## 🖥️ Technical Setup (Online Proctored)

### System Requirements
- [ ] OS: Windows 10+, macOS 10.13+, or Ubuntu 18.04+
- [ ] Browser: **Google Chrome** (latest version) — recommended by Kryterion
- [ ] RAM: 4 GB minimum, 8 GB recommended
- [ ] Stable internet: wired connection preferred, 1 Mbps+ upload/download
- [ ] Camera: Webcam that can rotate 360° (to show room)
- [ ] Microphone: Built-in or external (must be functional)

### System Check (Do 24h Before!)
1. Go to: https://www.kryterion.com/systemcheck/
2. Run the full system compatibility check
3. Install **Sentinel** (Kryterion's secure browser) if required
4. Test your camera and microphone
5. Ensure no VPN is active during the exam

### Room Preparation
- [ ] Clean, uncluttered desk — only allowed: water (clear bottle), ID
- [ ] No books, notes, or papers visible
- [ ] No dual monitors (disconnect second monitor)
- [ ] Close all applications except the exam browser
- [ ] Good lighting — examiner must see your face clearly
- [ ] Quiet room — no other people, no pets
- [ ] Phone must be face-down or in another room
- [ ] Inform others you should not be disturbed for 2 hours

---

## 📅 Exam Registration

1. **Register at:** https://webassessor.com/databricks
2. Create/login to your Kryterion Webassessor account
3. Select: **Databricks Certified Data Engineer Associate**
4. Choose: **Online Proctored** or **Testing Center**
5. Schedule your time slot (book at least 24h in advance)
6. **Cost:** $200 USD (credit/debit card)
7. **Vouchers:** Check if you have a Databricks training voucher

---

## ⏰ Exam Day Timeline

| Time | Action |
|---|---|
| T-60 min | Restart computer, close all background apps |
| T-30 min | Log in to webassessor.com and confirm your appointment |
| T-15 min | Start check-in process (Kryterion opens 15 min before) |
| T-10 min | Complete ID verification and room scan |
| T-0 | Exam starts — 45 questions, 90 minutes |

---

## 📝 During the Exam

### Time Management
- **Total:** 90 minutes for 45 questions = **2 minutes per question**
- **Round 1:** Answer all questions you're confident about (~30 min for 45)
- **Round 2:** Return to flagged/uncertain questions
- **Round 3:** Final review if time permits

### Question Strategy
- Read the **entire question** before looking at answers
- Watch for qualifier words: **always, never, only, best, primary**
- Questions often describe a scenario — identify **what problem is being solved**
- For "which statement is correct" — eliminate clearly wrong answers first
- When two options seem similar, the more **specific/detailed** one is usually correct

### Common Question Patterns
| Pattern | Strategy |
|---|---|
| "What is the BEST way to..." | Look for Databricks-recommended approach (DLT, UC, Auto Loader) |
| "A data engineer needs to..." | Map the requirement to a specific Databricks feature |
| "Which statement about X is TRUE" | One answer is subtly more precise than others |
| Code snippet with blank | Know the exact method/option name |
| "What CANNOT be done with..." | Think about limitations, not features |

---

## 🚨 If Something Goes Wrong

- **Internet drop:** Stay calm, reconnect quickly. Kryterion saves progress.
- **Proctor ends session unexpectedly:** Note the time, contact Databricks at certification@databricks.com
- **System crash:** Contact Kryterion support: 1-855-KRYTERION
- **Proctor chat:** Use the chat window to communicate issues without speaking

---

## 📊 After the Exam

- Results shown **immediately** after submission
- Score report emailed within 24 hours
- **If you pass:** Digital badge via Credly within 1–2 business days
- **If you fail:** You may retake after 14 days; second retake after 30 days
- Exam is valid for **2 years** from pass date

---

## 🧠 Last-Minute Mental Checklist

Before clicking Submit, confirm you can answer these 10 questions:

1. What is the difference between `OPTIMIZE` and `VACUUM`?
2. What does `Trigger.AvailableNow()` do vs `Trigger.Once()`?
3. What are the 3 DLT expectation decorators and what does each do when violated?
4. What happens when you `DROP TABLE` on a managed vs external Unity Catalog table?
5. What is the `_change_type` column in Change Data Feed and what are its possible values?
6. What does Auto Loader's `cloudFiles.schemaEvolutionMode = "rescue"` do?
7. When is `mergeSchema=True` used vs `overwriteSchema=True`?
8. What is the difference between DLT Triggered and Continuous pipeline modes?
9. What privileges does a user need to query a table in Unity Catalog?
10. What is the difference between Z-Ordering and partitioning?

**If you can answer all 10 clearly → you are ready. Go pass this exam! 🎯**
