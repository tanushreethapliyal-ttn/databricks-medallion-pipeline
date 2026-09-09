# Tool Workflow — Cursor + Databricks

How this project was built using **Cursor** (AI-assisted IDE) and **Databricks Community Edition**.

## Tools used

| Tool | Role |
|------|------|
| **Cursor** | Code generation, refactoring, debugging, documentation |
| **Git / GitHub** | Version control; Databricks Repos sync |
| **Databricks CE** | PySpark execution, Delta tables, SQL warehouse, Lakeview |
| **Python (local)** | Sample data generation only |
| **PyYAML** | Pipeline configuration |

## Workflow overview

```text
1. Read assessment PDF → requirements & design docs (Cursor)
2. Generate sample CSVs locally (Python script)
3. Push repo → Databricks Repos
4. Implement Bronze → run on cluster → fix path/import issues
5. Implement Silver → run → debug casting / metrics
6. Implement Gold → run → validate aggregates
7. Build Lakeview dashboard from SQL queries
8. Document prompts, debugging, reflection
```

## Cursor usage pattern

### Phase 1 — Planning (no cluster)

- Prompted Cursor to produce `requirements-analysis.md`, `design-notes.md`, `data-model.md`.
- Reviewed output for alignment with PDF spec before writing pipeline code.

### Phase 2 — Incremental implementation

| Layer | Approach |
|-------|----------|
| Data generation | Single script with explicit defect constants; validated counts |
| Bronze | Thin notebooks + shared `ingest_utils.py`; config-driven paths |
| Silver | Modular `transforms`, `quality_checks`, `metrics`, `silver_pipeline` |
| Gold | `aggregations.py` + `gold_pipeline.py` |
| Dashboard | SQL file + Lakeview guide |

**Rule followed:** Implement one layer, run on Databricks, fix errors, then proceed. Avoid generating all layers without execution feedback.

### Phase 3 — Debug with cluster output

When Silver showed 100% order failures, workflow was:

1. Paste Databricks output + `quality_metrics` into Cursor chat.
2. AI hypothesized root cause (pandas `1640.0` float strings vs `try_cast` to INT).
3. Apply fix locally → git push → pull in Repos → **restart Python** → re-run Silver.
4. Confirm metrics match expected counts before Gold.

### Phase 4 — Documentation

- Cursor drafted assessment docs from conversation history and codebase.
- Prompt logs saved under `ai-prompts/` per layer.
- Human review for accuracy and personal reflection.

## Databricks ↔ local sync

| Action | Where |
|--------|-------|
| Edit code | Cursor (local) or Databricks Repos |
| Run pipeline | Databricks notebooks only |
| Commit | Local git → push → Repos pull |
| Config | `config/pipeline_config.yaml` |

**Important:** After pulling code changes, restart the Python interpreter on the cluster to avoid cached old modules (`transforms.py`, `metrics.py`).

## Prompting practices that worked

| Practice | Why |
|----------|-----|
| Paste **full error messages** and SQL results | Faster root-cause analysis |
| Reference **design docs** in prompts | Keeps AI aligned with flag-not-delete, Gold filters |
| Ask for **minimal diffs** | Easier review on assessment code |
| Request **validation SQL** with each layer | Immediate cluster verification |
| Save prompts in `ai-prompts/` | Assessment artifact requirement |

## Prompting practices to avoid

| Anti-pattern | Risk |
|--------------|------|
| Generate entire pipeline without running | Hidden integration bugs |
| Skip `git pull` / module restart on Databricks | Stale code appears “broken” |
| Accept AI metrics without comparing to expected defect counts | False confidence |
| Hardcode workspace paths outside config | Breaks portability |

## File ownership

| Human | AI-assisted |
|-------|-------------|
| Final run validation on Databricks | Scaffold modules and notebooks |
| Dashboard visual layout | SQL query drafts |
| Reflection and candidate info | Documentation drafts |
| Git commit decisions | Debug suggestions |

## Suggested reviewer commands

```bash
# Local — regenerate data
pip install -r requirements.txt
python src/data_generation/generate_sample_data.py
```

Databricks: run `ingest_all.py` → `create_silver_tables.py` → `create_gold_tables.py` → dashboard queries.
