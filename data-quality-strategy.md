# Data Quality Strategy

How data quality is defined, detected, recorded, and consumed across the medallion pipeline.

Aligned with [design-notes.md](design-notes.md) and [data-model.md](data-model.md).

---

## Principles

1. **Preserve raw data in Bronze** — no business corrections at ingest.
2. **Detect and flag in Silver** — bad rows stay in the table with explicit failure codes.
3. **Measure centrally** — `silver.quality_metrics` summarizes each check per run.
4. **Consume safely in Gold** — aggregates use only `is_quality_pass = true` rows.
5. **No silent deletes** — exclusion happens at read time (Gold filter), not by dropping Silver rows.

---

## Failure codes

| Code | Category | When applied |
|------|----------|--------------|
| `COMPLETENESS_FAIL` | Completeness | NULL or empty required field |
| `UNIQUENESS_FAIL` | Uniqueness | Duplicate primary key value |
| `TYPE_VALIDATION_FAIL` | Type validation | Non-empty raw value cannot be cast or enum invalid |
| `REF_INTEGRITY_FAIL` | Referential integrity | FK not found in master table |
| `CONSISTENCY_FAIL` | Business logic | e.g. Completed order without `payment_date` |

Defined in `src/silver/quality_codes.py`.

---

## Rules by entity

### Customers (`silver.customers`)

| Check | Rule | Code |
|-------|------|------|
| Completeness | `_raw_customer_id`, `customer_name`, `email` not blank | `COMPLETENESS_FAIL` |
| Uniqueness | `customer_id` unique | `UNIQUENESS_FAIL` on all rows in duplicate groups |
| Type | `signup_date`, `lifetime_value` castable; `customer_segment` in {Premium, Standard, Basic} | `TYPE_VALIDATION_FAIL` |

**Intentional defects:** 50 empty emails; 10 duplicate `customer_id` pairs (20 rows flagged for uniqueness).

### Products (`silver.products`)

| Check | Rule | Code |
|-------|------|------|
| Completeness | `_raw_product_id`, `product_name`, `category` not blank | `COMPLETENESS_FAIL` |
| Uniqueness | `product_id` unique | `UNIQUENESS_FAIL` |
| Type | Numeric fields castable | `TYPE_VALIDATION_FAIL` |

**Intentional defects:** none (catalog assumed clean).

### Orders (`silver.orders`)

| Check | Rule | Code |
|-------|------|------|
| Completeness | Required order fields including `_raw_customer_id`, `_raw_product_id` | `COMPLETENESS_FAIL` |
| Uniqueness | `order_id` unique | `UNIQUENESS_FAIL` |
| Type | Dates, numerics, `order_status` enum | `TYPE_VALIDATION_FAIL` |
| Referential integrity | `customer_id` ∈ customers; `product_id` ∈ products | `REF_INTEGRITY_FAIL` |
| Consistency | Completed → `payment_date` required; Cancelled → no payment | `CONSISTENCY_FAIL` |

**Intentional defects:** 100 NULL customer_id; 200 NULL product_id; 50 orphan customer_id; 30 orphan product_id; 20 duplicate order_id pairs.

---

## Row-level representation

Each Silver entity row includes:

| Column | Description |
|--------|-------------|
| `quality_check_result` | `ARRAY<STRING>` of failure codes (empty = pass) |
| `is_quality_pass` | `true` when array is empty |
| `silver_processed_at` | Processing timestamp |

Implementation: `src/silver/quality_checks.py` → `finalize_quality_columns()`.

---

## Quality metrics (`silver.quality_metrics`)

Grain: **one row per check per table per pipeline run**.

| Column | Description |
|--------|-------------|
| `run_id` | Pipeline run UUID |
| `table_name` | e.g. `silver.orders` |
| `check_type` | Completeness, Uniqueness, Type validation, Referential integrity, Business logic |
| `check_name` | e.g. `customer_id_not_null` |
| `failed_count` | Rows matching check condition |
| `total_count` | Total rows in Silver table |
| `failure_rate` | `failed_count / total_count` |
| `measured_at` | UTC timestamp |

Metrics are computed from the **pre-finalize** checked DataFrame (with `_type_flags`, `_metric_*` columns) so per-check counts are accurate.

---

## Processing order

```text
products (Silver) ──┐
                    ├──► orders (Silver) — FK checks need customer/product IDs
customers (Silver) ─┘
```

Silver masters must exist before order referential integrity checks.

---

## Gold consumption rules

| Rule | Implementation |
|------|----------------|
| Quality pass only | `is_quality_pass = true` |
| Revenue orders | `order_status = 'Completed'` |
| Revenue field | `total_amount` |
| Duplicate PK rows | Excluded (all dup rows flagged in Silver) |
| Invalid FK rows | Excluded |

Gold does **not** re-run DQ logic; it trusts Silver flags.

---

## Casting strategy (Silver)

- Bronze business columns remain **STRING**.
- Silver creates `_raw_*` columns, then casts to typed columns.
- Integer IDs use `regexp_extract` to handle pandas float strings like `1640.0`.
- Failed casts → NULL typed column + `TYPE_VALIDATION_FAIL` when raw was non-empty.

See `src/silver/transforms.py`.

---

## Monitoring queries

```sql
-- Latest failure summary for orders
SELECT check_name, failed_count, ROUND(failure_rate * 100, 2) AS failure_pct
FROM silver.quality_metrics
WHERE table_name = 'silver.orders'
  AND measured_at = (
    SELECT MAX(measured_at) FROM silver.quality_metrics WHERE table_name = 'silver.orders'
  )
ORDER BY failed_count DESC;

-- Sample flagged rows
SELECT order_id, customer_id, quality_check_result
FROM silver.orders
WHERE NOT is_quality_pass
LIMIT 20;
```

---

## Expected metrics (full dataset)

| Check (orders) | Expected `failed_count` |
|----------------|-------------------------|
| `customer_id_not_null` | 100 |
| `product_id_not_null` | 200 |
| `customer_id_exists` | 50 |
| `product_id_exists` | 30 |
| `order_id_unique` | 40 |
| `order_types_valid` | 0 |
| `payment_status_consistency` | 0 |

| Check (customers) | Expected |
|-------------------|----------|
| `email_not_null` | 50 |
| `customer_id_unique` | 20 rows (10 duplicate pairs) |

---

## Future improvements (out of assessment scope)

- DLT expectations or Great Expectations integration
- Quarantine tables for failed rows
- Alerting when `failure_rate` exceeds thresholds
- Incremental Silver merges instead of full overwrite
