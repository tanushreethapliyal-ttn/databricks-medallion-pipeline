# AI Prompts — Debugging (Silver Orders 100% Failures)

Prompt history for the most significant debug session in this project.

---

## Prompt 1: Silver run — unexpected metrics

**PROMPT SENT:**

> Silver pipeline completed but orders show 100% failures:
> - customer_id_not_null: 100000
> - order_types_valid: 100000
> - customer_id_exists: 0
> - order_id_unique: 40
>
> Customers metrics look correct (~70 failures).
> Paste of quality_metrics and sample UNIQUENESS_FAIL rows.

**AI RESPONSE SUMMARY:**

- Identified likely cast/metrics bugs, not bad source data
- Hypothesis: Spark column alias resolution in `cast_orders`
- Hypothesis: `_is_blank()` on INTEGER columns in metrics
- Proposed `_raw_*` column pattern and metrics on pre-finalize DataFrames

**OUTCOME:** Partial fix; metrics still 100% after first sync.

---

## Prompt 2: Metrics unchanged after fix

**PROMPT SENT:**

> Re-ran Silver, same metrics:
> customer_id_not_null 100000, order_types_valid 100000,
> customer_id_exists 0, order_id_unique 40

**AI RESPONSE SUMMARY:**

- Inspected `orders.csv`: `customer_id` values like `1640.0` (pandas float strings)
- `try_cast('1640.0' AS INT)` → NULL in Spark
- `order_id` without `.0` still casts → explains 40 uniqueness only
- Fix: cast via `regexp_extract` or DOUBLE intermediate
- Add `SILVER_PIPELINE_VERSION` debug print to confirm code sync
- Restart Python on cluster after git pull

**OUTCOME:** Success. Final metrics:

| check_name | failed_count |
|------------|--------------|
| customer_id_not_null | 100 |
| product_id_not_null | 200 |
| customer_id_exists | 50 |
| product_id_exists | 30 |
| order_id_unique | 40 |
| order_types_valid | 0 |

---

## Prompt 3: Verification

**PROMPT SENT:**

> (Pasted corrected metrics table)

**AI RESPONSE SUMMARY:**

- Confirmed Silver correct; proceed to Gold layer

---

## Lessons for future prompts

1. Always paste **full metrics table**, not just failure counts
2. Compare **which checks pass vs fail** — partial success narrows root cause
3. Include **Bronze sample row** (`SELECT customer_id FROM bronze.orders LIMIT 5`)
4. Confirm **debug version string** in notebook output after fixes
5. Mention whether **Python was restarted** on cluster

---

## Files changed in this debug cycle

| File | Change |
|------|--------|
| `src/silver/transforms.py` | `_raw_*` columns; `regexp_extract` int cast |
| `src/silver/metrics.py` | Flag columns; `_metric_*` for completeness |
| `src/silver/quality_checks.py` | Completeness on `_raw_*`; metric helper columns |
| `src/silver/silver_pipeline.py` | Metrics on `checked_df`; debug prints |
| `src/data_generation/generate_sample_data.py` | `Int64` for order FK columns (preventive) |
