# AI Prompts — Silver Layer

## Prompt 1: Implement Silver after Bronze

**PROMPT SENT:**

> Bronze is done. Move to Silver layer.
> Implement 4 quality checks, quality_check_result, quality metrics, Delta tables.

**AI RESPONSE SUMMARY:**

Created Silver package:

- `transforms.py` — cast Bronze STRING columns to typed Silver columns
- `quality_checks.py` — completeness, uniqueness, type, referential integrity, business logic
- `metrics.py` — `silver.quality_metrics` summary rows
- `silver_pipeline.py` — orchestration (products → customers → orders)
- `create_silver_tables.py` — Databricks entry notebook
- `01`–`05` quality check notebooks per assessment structure

**DESIGN DECISIONS:**

| Decision | Rationale |
|----------|-----------|
| Flag, don't delete | Assessment requires bad rows remain with `quality_check_result` |
| Processing order | products + customers before orders (FK checks) |
| `try_cast` | Invalid values become NULL and trigger `TYPE_VALIDATION_FAIL` |
| Metrics append | Preserve run history in `silver.quality_metrics` |

## Validation SQL (run after Silver)

```sql
SELECT COUNT(*) FROM silver.customers;
SELECT COUNT(*) FROM silver.orders;
SELECT COUNT(*) FROM silver.products;

-- Expected intentional failures (approximate)
SELECT COUNT(*) FROM silver.customers WHERE array_contains(quality_check_result, 'COMPLETENESS_FAIL');
SELECT COUNT(*) FROM silver.customers WHERE array_contains(quality_check_result, 'UNIQUENESS_FAIL');
SELECT COUNT(*) FROM silver.orders WHERE array_contains(quality_check_result, 'REF_INTEGRITY_FAIL');

SELECT * FROM silver.quality_metrics ORDER BY measured_at DESC;
```
