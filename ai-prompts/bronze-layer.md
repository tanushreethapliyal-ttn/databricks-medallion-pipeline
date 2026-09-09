# AI Prompts — Bronze Layer

Prompt history for designing and implementing the Bronze ingestion layer on Databricks.

---

## Prompt 1: Databricks readiness review (before Bronze)

**PROMPT SENT:**

> Review the repository and adapt the implementation for Databricks execution.
> Pipeline must run on Databricks with PySpark/SQL, Delta tables, configurable paths,
> no hardcoded secrets, usable from notebooks/jobs.

**AI RESPONSE SUMMARY:**

- Confirmed planning + data generation complete; pipeline not started.
- Recommended thin notebooks + `src/ecommerce_pipeline` modules.
- Config-driven landing path (Volume/DBFS/workspace).
- Bronze overwrite + `ingestion_log` append.
- Next step: scaffold config + Bronze only.

**YOUR EVALUATION:**

- **Accepted:** Config-driven paths, Delta tables, separate local data generation.
- **Accepted:** Bronze-first implementation order.
- **Note:** Community Edition uses workspace paths instead of explicit DBFS landing zones.

---

## Prompt 2: Confirm CSV path and implement Bronze

**PROMPT SENT:**

> CSV path:
> `/Workspace/Users/tanushreethapliyal3@gmail.com/databricks-medallion-pipeline/databricks-medallion-pipeline/Data/raw`
>
> Using Databricks free edition. Schemas bronze/silver/gold created.
> Implement Bronze: read 3 CSVs, write Delta tables, metadata columns,
> ingestion_log, raw data only.

**AI RESPONSE SUMMARY:**

Created:

- `config/pipeline_config.yaml` — configurable landing path and table names
- `src/bronze/schemas.py` — explicit STRING Bronze schemas
- `src/bronze/config_loader.py` — YAML + widget overrides
- `src/bronze/ingest_utils.py` — shared ingest/audit logic
- `src/bronze/01_ingest_customers.py`, `02_ingest_orders.py`, `03_ingest_products.py`
- `src/bronze/ingest_all.py` — orchestrator notebook
- `database/schema.sql`, `database/setup-notes.md`

**YOUR EVALUATION:**

- **Accepted:** STRING schemas at Bronze (cast in Silver per design).
- **Accepted:** `_ingestion_timestamp`, `_source_file`, `_run_id` metadata.
- **Accepted:** `bronze.ingestion_log` append with SUCCESS/FAILED rows.
- **Accepted:** `landing_path` widget override on `ingest_all.py`.
- **To validate on cluster:** workspace CSV path readability and `pyyaml` availability.

---

## Prompt 3: Design alignment check

**PROMPT SENT (implicit from design docs):**

> Bronze must preserve raw data, apply explicit schemas, log row counts and timestamps.

**DESIGN DECISIONS APPLIED:**

| Decision | Implementation |
|----------|----------------|
| No business transforms | CSV → STRING columns + metadata only |
| Explicit schema | `schemas.py` StructType per entity |
| Idempotent reload | `mode("overwrite")` on entity tables |
| Audit history | `mode("append")` on `bronze.ingestion_log` |
| Fail-fast | Failed ingest writes FAILED log row and raises error |

---

## Validation plan (to run in Databricks)

After running `src/bronze/ingest_all.py`:

```sql
SELECT COUNT(*) FROM bronze.customers;   -- expect 10000
SELECT COUNT(*) FROM bronze.orders;      -- expect 100000
SELECT COUNT(*) FROM bronze.products;    -- expect 500

SELECT target_table, row_count, status, ingestion_timestamp
FROM bronze.ingestion_log
ORDER BY ingestion_timestamp DESC
LIMIT 10;
```

**If ingestion fails:**

1. Check `landing_path` widget matches uploaded CSV folder.
2. Confirm filenames: `customers.csv`, `orders.csv`, `products.csv`.
3. Run `%pip install pyyaml` if config load fails.
4. Inspect latest `bronze.ingestion_log` row with `status = FAILED`.

---

## FINAL DECISION

Use `src/bronze/ingest_all.py` as the primary Bronze entrypoint in Databricks Repos.
Individual entity notebooks (`01_*`, `02_*`, `03_*`) can be run separately for debugging.
