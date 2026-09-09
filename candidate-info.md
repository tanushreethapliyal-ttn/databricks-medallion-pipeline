# Candidate Information

| Field | Value |
|-------|-------|
| **Name** | Tanushree Thapliyal |
| **Email** | tanushreethapliyal3@gmail.com |
| **Assessment** | TO THE NEW — Data Engineering C1 Coding Evaluation (Medallion Architecture) |
| **Submission date** | September 2026 |

## Databricks workspace

| Item | Value |
|------|-------|
| **Platform** | Databricks Community Edition |
| **Repo path** | `/Workspace/Users/tanushreethapliyal3@gmail.com/databricks-medallion-pipeline/databricks-medallion-pipeline` |
| **CSV landing path** | `.../Data/raw/` (see `config/pipeline_config.yaml`) |
| **Schemas** | `bronze`, `silver`, `gold` |

## Deliverables checklist

| Deliverable | Location | Status |
|-------------|----------|--------|
| Data generation | `src/data_generation/`, `data/raw/` | Complete |
| Bronze layer | `src/bronze/` | Complete |
| Silver layer + DQ | `src/silver/` | Complete |
| Gold layer | `src/gold/` | Complete |
| Dashboard SQL + Lakeview | `src/dashboard/` | Complete |
| Requirements analysis | `docs/requirements-analysis.md` | Complete |
| Design notes | `docs/design-notes.md` | Complete |
| Data model | `docs/data-model.md` | Complete |
| Data quality strategy | `docs/data-quality-strategy.md` | Complete |
| Debugging notes | `docs/debugging-notes.md` | Complete |
| Reflection | `docs/reflection.md` | Complete |
| AI prompt history | `ai-prompts/` | Complete |
| AI usage summary | `docs/final-ai-usage-summary.md` | Complete |

## Dashboard link

> **Add your published Lakeview dashboard URL here before submission.**

`https://<your-workspace>/sql/dashboards/<dashboard-id>`

## Git repository

> **Add your GitHub repository URL here before submission.**

`https://github.com/<username>/databricks-medallion-pipeline`

## Notes for reviewers

- Pipeline was developed locally (data generation + docs) and executed on Databricks (Bronze → Gold).
- AI (Cursor) was used for design, implementation scaffolding, and debugging; all code was reviewed and validated on the cluster.
- Intentional data defects match the assessment specification; Silver metrics were verified against expected counts.
