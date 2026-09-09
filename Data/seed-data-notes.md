# Seed Data Notes

Summary of how assessment CSVs in `data/raw/` were produced. Full technical detail: `src/data_generation/DATA_GENERATION_NOTES.md`.

## Purpose

Provide reproducible synthetic e-commerce data with **exact intentional data-quality defects** required by the assessment specification.

## Files

| File | Rows | Description |
|------|------|-------------|
| `customers.csv` | 10,000 | Customer master with email and duplicate-id defects |
| `orders.csv` | 100,000 | Order facts with NULL FKs, orphan FKs, duplicate ids |
| `products.csv` | 500 | Clean product catalog |

## Generator

```bash
pip install -r requirements.txt
python src/data_generation/generate_sample_data.py
```

- **Script:** `src/data_generation/generate_sample_data.py`
- **Seed:** `RANDOM_SEED = 42` (reproducible with same Python/pandas/Faker versions)
- **Output:** `data/raw/` by default

## Intentional defects

### Customers

| Issue | Count |
|-------|-------|
| Empty `email` | 50 |
| Duplicate `customer_id` rows (ids 1–10) | 10 extra rows |

### Orders

| Issue | Count |
|-------|-------|
| NULL `customer_id` | 100 |
| NULL `product_id` | 200 |
| `customer_id` not in customers (99,991–100,040) | 50 |
| `product_id` not in products (501–530) | 30 |
| Duplicate `order_id` rows (ids 1–20) | 20 extra rows |

### Products

No intentional defects.

## Data quirks affecting Silver

| Quirk | Impact | Mitigation |
|-------|--------|------------|
| Pandas writes nullable `customer_id` / `product_id` as floats (`1640.0`) | Spark `try_cast` to INT can fail | Silver `regexp_extract` int casting |
| Empty string used for NULL in CSV (`na_rep=""`) | Treated as blank in completeness checks | `_raw_*` string columns |

## Valid foreign-key ranges

- Customers: `customer_id` 1–9,990
- Products: `product_id` 1–500
- Invalid order references: customer ids ≥ 99,991; product ids 501–530

## Upload to Databricks

Copy the three CSVs to the workspace landing path configured in `config/pipeline_config.yaml`:

```text
/Workspace/Users/<user>/databricks-medallion-pipeline/databricks-medallion-pipeline/Data/raw/
```

Filenames must match: `customers.csv`, `orders.csv`, `products.csv`.

## Regeneration policy

- Do **not** change defect counts unless the assessment specification changes.
- Constants are module-level in `generate_sample_data.py` with post-generation validation.
- After regeneration, re-run Bronze → Silver → Gold on Databricks.
