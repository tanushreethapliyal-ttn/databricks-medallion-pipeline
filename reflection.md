# Reflection

Personal reflection on the Databricks Medallion Architecture assessment project.

---

## What went well

**Incremental delivery.** Building Bronze → Silver → Gold → Dashboard in sequence made failures easier to isolate. Each layer had a clear validation query set before moving on.

**Design-first documentation.** Writing `requirements-analysis.md`, `design-notes.md`, and `data-model.md` before heavy coding kept implementation aligned with assessment rules — especially *flag, don't delete* in Silver and *quality-pass only* in Gold.

**Config-driven pipeline.** A single `pipeline_config.yaml` for paths, schemas, and table names avoided hardcoding workspace URLs across notebooks.

**AI-assisted debugging.** Pasting `quality_metrics` output into Cursor quickly surfaced the `1640.0` casting issue — a subtle bug that would have been hard to spot from code review alone.

**Dashboard on Gold only.** Keeping dashboard SQL strictly on `gold.*` tables made the analytics layer easy to explain to reviewers.

---

## What was challenging

**Databricks Community Edition constraints.** Workspace CSV paths, module caching, and SQL warehouse startup added friction not present in local development.

**Silver casting edge case.** Pandas float formatting for nullable integer columns was not obvious from the assessment spec. It required multiple iterations and cluster restarts to confirm fixes.

**Metrics accuracy.** Computing quality metrics from the wrong DataFrame stage (post-finalize without `_raw_*` columns, or using `quality_check_result` before it existed) produced misleading counts until the metrics module was aligned with flag columns.

**Documentation volume.** The assessment expects substantial written artifacts alongside code; balancing implementation speed with thorough docs required dedicated time at the end.

---

## Key technical decisions

| Decision | Rationale |
|----------|-----------|
| STRING columns in Bronze | Maximum raw fidelity; cast in Silver |
| Overwrite Delta tables per run | Simple idempotency for assessment scope |
| Append `quality_metrics` | Preserve run history for debugging |
| `regexp_extract` for integer casts | Handles `1640.0` without fragile `try_cast` chains |
| Gold revenue = Completed orders only | Matches assessment assumption |
| Per-customer segmentation in Gold | Powers pie chart; `segment_type` maps to High-Value / Repeat / One-Time / Inactive |

---

## What I would improve with more time

1. **Automated tests** — `pytest` with local Spark and small fixtures asserting exact DQ failure counts and Gold revenue on a subset.
2. **Incremental processing** — `merge` instead of full overwrite for production-scale data.
3. **Data generator** — emit nullable integers as clean CSV ints (`Int64`) by default to reduce casting surprises.
4. **Pipeline orchestration** — single Databricks Workflow job chaining Bronze → Silver → Gold with email on failure.
5. **Unity Catalog** — catalog + volume paths instead of workspace file paths for enterprise deployment.

---

## AI usage reflection

Cursor accelerated scaffolding (module structure, SQL, documentation) and shortened debug cycles. I treated AI output as a **draft**: every layer was run on Databricks and validated against expected metrics before acceptance.

Most valuable AI interactions were **debugging sessions with real cluster output**, not initial code generation.

Prompt history is preserved in `ai-prompts/` and summarized in [final-ai-usage-summary.md](final-ai-usage-summary.md).

---

## Outcome

The pipeline ingests 10k / 100k / 500 rows, flags intentional defects with measurable Silver metrics, produces Gold aggregates from trustworthy rows, and exposes results through a three-tile Lakeview dashboard. The project demonstrates medallion layering, structured data quality, and operational visibility suitable for a batch e-commerce analytics use case.
