# Final AI Usage Summary

Consolidated summary of how **Cursor (AI-assisted IDE)** was used across this assessment.

---

## Scope of AI assistance

| Area | AI role | Human role |
|------|---------|------------|
| Requirements & design docs | Draft from PDF spec | Review, correct, approve |
| Data generator | Scaffold script with defect constants | Validate counts locally |
| Bronze / Silver / Gold code | Module scaffolding, DQ logic, SQL | Run on Databricks, fix integration bugs |
| Debugging | Root-cause analysis from metrics output | Apply fixes, restart cluster, verify |
| Dashboard SQL | Query drafts | Build Lakeview, tune chart settings |
| Assessment documentation | Draft README, reflection, strategy docs | Edit for accuracy and voice |

---

## Tools

- **Cursor** — primary AI coding assistant
- **Chat context** — pasted Databricks errors, SQL results, and config paths
- **No automated code deployment** — all changes reviewed before git push

---

## High-value prompts (by phase)

### Planning

- Explain assessment PDF requirements and produce implementation plan
- Create `requirements-analysis.md`, `design-notes.md`, `data-model.md`

### Bronze

- Implement Bronze ingestion with Delta, metadata columns, `ingestion_log`
- Fix Databricks path and `ModuleNotFoundError` issues

### Silver

- Implement four DQ categories + `quality_check_result` + metrics
- Debug 100% order failure metrics → identified `1640.0` float string casting bug
- Fix `transforms.py`, `metrics.py`, `quality_checks.py`

### Gold

- Implement three Gold tables from quality-pass Silver
- Revenue rules: Completed orders, `total_amount`, segmentation tiers

### Dashboard

- SQL for top 10 products, revenue distribution, segmentation
- `DASHBOARD_GUIDE.md` for Lakeview setup

### Documentation

- Draft assessment deliverables (this docs folder)

---

## Prompt files

| File | Content |
|------|---------|
| `ai-prompts/bronze-layer.md` | Bronze design and implementation prompts |
| `ai-prompts/silver-layer.md` | Silver DQ implementation |
| `ai-prompts/gold-layer.md` | Gold aggregations |
| `ai-prompts/dashboard.md` | Dashboard SQL |
| `ai-prompts/debugging.md` | Silver casting debug session |
| `ai-prompts/documentation.md` | Documentation drafting prompts |

---

## What AI did well

- Fast scaffolding of repetitive PySpark patterns (cast, flag, aggregate, write Delta)
- Generating validation SQL alongside each layer
- Explaining Spark casting behavior for pandas CSV quirks
- Producing structured documentation from conversation history

---

## What required human verification

- **All Databricks execution** — AI cannot run the cluster
- **Expected defect counts** — compared metrics to assessment spec (100, 200, 50, 30, 40, etc.)
- **Git sync + Python restart** — ensured fixes were actually loaded
- **Dashboard visuals** — chart types, axes, and layout in Lakeview UI
- **Candidate-specific info** — workspace paths, dashboard URL, reflection voice

---

## Responsible use statement

AI was used as a **pair-programming and documentation assistant**. I reviewed, tested, and modified all generated code. No secrets or credentials were shared with AI tools. The final pipeline behavior was validated on Databricks with SQL checks at each layer.

---

## Estimated time impact

| Without AI (estimate) | With AI (estimate) |
|-----------------------|---------------------|
| 2–3 days full implementation | ~1 day implementation + documentation |
| Longer debug on casting issue | Resolved in 1–2 iterations with metrics pasted into chat |

Exact times depend on Databricks CE availability and familiarity with the workspace.
