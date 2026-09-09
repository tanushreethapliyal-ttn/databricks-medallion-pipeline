# Debugging Notes

Issues encountered during implementation and how they were resolved on Databricks Community Edition.

---

## 1. Bronze — `ModuleNotFoundError: path_utils`

**Symptom:** `ingest_all.py` failed importing local modules.

**Cause:** Databricks notebook `sys.path` did not include `src/bronze`.

**Fix:** Inline path resolution in `ingest_all.py` and `bootstrap`-style setup; later consolidated in `config_loader.py` with notebook path detection.

**Lesson:** Always bootstrap `sys.path` from the notebook’s repo location on Databricks Repos.

---

## 2. Bronze — CSV path not readable by Spark

**Symptom:** `FileNotFoundError` or empty reads from workspace CSV path.

**Cause:** Spark on Community Edition sometimes cannot read `/Workspace/...` paths directly.

**Fix:** `src/bronze/path_resolver.py` — resolve path, copy to DBFS FileStore fallback when needed.

**Validation:**

```sql
SELECT COUNT(*) FROM bronze.customers;  -- 10000
```

---

## 3. Bronze — `ingest_all` called `module.run()` 

**Symptom:** `AttributeError: module has no attribute 'run'`.

**Cause:** Orchestrator expected per-entity `run()` functions; implementation used `ingest_entity()` directly.

**Fix:** Refactored `ingest_all.py` to call `ingest_entity()` for each entity.

**Lesson:** After git pull, confirm the cluster is running the latest notebook version.

---

## 4. Silver — 100% order quality failures

**Symptom:** After first Silver run:

| Metric | Actual | Expected |
|--------|--------|----------|
| `customer_id_not_null` | 100,000 | 100 |
| `order_types_valid` | 100,000 | 0 |
| `customer_id_exists` | 0 | 50 |
| `order_id_unique` | 40 | 40 ✓ |

**Cause (root):** Pandas writes nullable integer columns in `orders.csv` as float strings (`1640.0`). Spark `try_cast('1640.0' AS INT)` returned NULL for almost every row, so:

- Typed `customer_id` / `product_id` were NULL → completeness and type checks failed everywhere
- FK checks skipped NULL keys → `customer_id_exists` showed 0

`order_id` values were plain integers (`1`, `2`, …) so uniqueness still showed the correct 40 duplicate rows — this was the key clue.

**Fix (layered):**

1. `transforms.py` — integer casting via `regexp_extract` on trimmed raw strings
2. `metrics.py` — use `_type_flags` / `_metric_*` columns instead of `quality_check_result` on pre-finalize DataFrames
3. `quality_checks.py` — explicit `_metric_customer_id_missing` columns for order completeness metrics

**Validation after fix:**

```sql
SELECT check_name, failed_count
FROM silver.quality_metrics
WHERE table_name = 'silver.orders'
  AND measured_at = (SELECT MAX(measured_at) FROM silver.quality_metrics WHERE table_name = 'silver.orders');
```

Expected: 100 / 200 / 50 / 30 / 40 / 0 / 0.

---

## 5. Silver — stale Python modules on cluster

**Symptom:** Metrics unchanged after pushing code fixes.

**Cause:** Databricks cached old `transforms.py` / `metrics.py` in the interpreter session.

**Fix:** **Restart Python** (or detach/reattach cluster) after every git pull of Silver modules.

**Verification:** Pipeline prints `version=regexp-int-v3` and debug line:

```text
DEBUG [regexp-int-v3] orders cast check — non-null customer_id: 99900, non-null product_id: 99800
```

If version string is missing, new code is not loaded.

---

## 6. Silver — Spark column alias in casts (earlier iteration)

**Symptom:** `_raw_customer_id` appeared blank when derived from already-aliased `customer_id`.

**Fix:** `_with_raw_string_columns()` creates `_raw_*` from Bronze first, then casts from `_raw_*` — never from typed aliases.

---

## 7. Gold — empty or unexpected aggregates

**Symptom (preventive):** Gold revenue too low.

**Checks:**

```sql
SELECT COUNT(*) FROM silver.orders WHERE is_quality_pass AND order_status = 'Completed';
SELECT COUNT(*) FROM gold.revenue_by_customer WHERE order_count > 0;
```

**Common causes:**

- Silver not re-run after cast fix
- Filtering on wrong `order_status` value (case-sensitive `Completed`)

---

## 8. Dashboard — `TABLE_OR_VIEW_NOT_FOUND`

**Symptom:** SQL queries fail in Lakeview.

**Fix:** Run `src/gold/create_gold_tables.py` first; confirm `SHOW TABLES IN gold`.

---

## 9. Dashboard — revenue bucket order on chart

**Symptom:** X-axis shows `5,000 - 9,999` after `10,000+` (alphabetical sort).

**Fix:** Sort visualization by `bucket_order` column from the distribution query, or rename buckets with numeric prefixes.

---

## Debug query cheat sheet

```sql
-- Bronze ingest status
SELECT * FROM bronze.ingestion_log ORDER BY ingestion_timestamp DESC LIMIT 10;

-- Silver flagged orders
SELECT order_id, customer_id, product_id, quality_check_result
FROM silver.orders WHERE NOT is_quality_pass LIMIT 20;

-- Cast sanity (Bronze vs Silver)
SELECT b.customer_id AS bronze_customer_id, s.customer_id AS silver_customer_id
FROM bronze.orders b
JOIN silver.orders s ON b.order_id = s.order_id
LIMIT 10;

-- Gold segmentation
SELECT segment_type, COUNT(*), SUM(total_revenue)
FROM gold.customer_segmentation GROUP BY 1;
```

---

## Decision tree (simplified)

```text
Pipeline failed?
├── Bronze → check landing_path, ingestion_log FAILED rows, CSV filenames
├── Silver → check quality_metrics; if 100% failures on orders → casting / module cache
├── Gold → check Silver is_quality_pass counts and Completed filter
└── Dashboard → check gold tables exist; SQL warehouse running
```
