# Data Generation Notes

Documentation for `generate_sample_data.py` — synthetic CSV sources for the e-commerce medallion pipeline assessment.

## How Data Is Generated

The generator builds three datasets in order:

1. **products** — 500 clean master records (`product_id` 1–500).
2. **customers** — 9,990 unique customers (`customer_id` 1–9,990), then intentional defects.
3. **orders** — 99,980 primary orders (`order_id` 1–99,980), intentional defects on disjoint primary rows, then 20 duplicate-id rows.

Output files are written to `data/raw/` by default:

- `customers.csv`
- `orders.csv`
- `products.csv`

After generation, the script validates row counts, intentional issue counts, and clean-row sanity checks. It **fails with a clear error** if any expected count does not match.

## Why Faker and Randomization

| Approach | Purpose |
|----------|---------|
| **Faker** (`Faker.seed_instance(42)`) | Realistic synthetic names, countries, emails, and product words without real PII |
| **`random` module** (`random.seed(42)`) | Reproducible choices for categories, dates, quantities, prices, and defect row selection |

Faker produces human-readable values that look like production data while remaining entirely synthetic. Seeding both Faker and `random` makes runs reproducible on the same machine and Python version.

## How Quality Issues Are Introduced

Issues are applied **explicitly** after base valid rows are created. Primary order defects use **disjoint row index sets** so each defect type hits exactly the required number of rows without overlap.

### Customers (`customers.csv`)

| Issue | Count | How it is introduced |
|-------|-------|----------------------|
| NULL email | 50 | `email` set to empty string for `customer_id` 1001–1050 |
| Duplicate `customer_id` | 10 rows | Second copies of rows with `customer_id` 1–10 appended to the file |

**Row math:** 9,990 unique + 10 duplicate rows = **10,000** total rows.

### Orders (`orders.csv`)

| Issue | Count | How it is introduced |
|-------|-------|----------------------|
| NULL `customer_id` | 100 | `customer_id` set to null on 100 shuffled primary rows |
| NULL `product_id` | 200 | `product_id` set to null on 200 different primary rows |
| Invalid `customer_id` | 50 | Values 99,991–100,040 (not in customers) |
| Invalid `product_id` | 30 | Values 501–530 (not in products) |
| Duplicate `order_id` | 20 rows | Second copies of rows with `order_id` 1–20 |

Defect rows are chosen only from primary orders with `order_id` > 20 so duplicate copies do not double-count NULL or invalid FK issues.

**Row math:** 99,980 primary + 20 duplicate rows = **100,000** total rows.

### Products (`products.csv`)

No intentional defects. All 500 rows are valid master data.

### Valid foreign-key ranges

- **Customers:** `customer_id` 1–9,990 (ids 1–10 also appear on duplicate rows).
- **Products:** `product_id` 1–500.
- **Invalid order references:** customer ids ≥ 99,991; product ids 501–530.

## Expected Record Counts

| File | Rows |
|------|------|
| `customers.csv` | 10,000 |
| `orders.csv` | 100,000 |
| `products.csv` | 500 |

## Expected Issue Counts

| Dataset | Issue | Expected count |
|---------|-------|----------------|
| Customers | NULL / empty `email` | 50 |
| Customers | Duplicate `customer_id` rows (second occurrences) | 10 |
| Orders | NULL `customer_id` | 100 |
| Orders | NULL `product_id` | 200 |
| Orders | `customer_id` not in customers (non-null) | 50 |
| Orders | `product_id` not in products (non-null) | 30 |
| Orders | Duplicate `order_id` rows (second occurrences) | 20 |

These counts match the assessment specification and are **not modified** by the generator.

## Clean Data Guarantees

Beyond the rows above:

- Customer `customer_id` values are unique except for ids 1–10 (exactly two rows each).
- Product `product_id` values are unique (500 rows).
- Order `order_id` values are unique except for ids 1–20 (exactly two rows each).
- Clean order rows keep `total_amount = quantity × unit_price` (rounded to 2 decimals).
- `payment_date` is populated for `Completed` orders; empty for `Pending` and `Cancelled` on generated clean rows.

## How to Reproduce the Data

### Prerequisites

```bash
pip install -r requirements.txt
```

### Generate (default output: `data/raw/`)

```bash
python src/data_generation/generate_sample_data.py
```

### Custom output directory

```bash
python src/data_generation/generate_sample_data.py --output-dir ./data/raw
```

### Expected success output

```
Sample data generated successfully:
  customers: .../data/raw/customers.csv
  orders: .../data/raw/orders.csv
  products: .../data/raw/products.csv
```

If validation fails, the script exits with code 1 and prints which expected count was wrong.

### Reproducibility

- **Seed:** `RANDOM_SEED = 42` in `generate_sample_data.py`
- Re-running without code changes produces identical CSV content (same Python, pandas, and Faker versions).

## Source Constants

All volumes and issue counts are defined as module-level constants at the top of `generate_sample_data.py`. Change only when the assessment specification changes.
