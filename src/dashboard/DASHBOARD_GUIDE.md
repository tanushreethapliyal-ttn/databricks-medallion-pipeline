# Dashboard Guide — Databricks SQL / Lakeview

Build a three-tile analytics dashboard over **Gold** Delta tables using the SQL in `dashboard_queries.sql`.

## Prerequisites

| Step | Notebook / artifact | Status |
|------|---------------------|--------|
| Bronze ingest | `src/bronze/ingest_all.py` | Complete |
| Silver quality | `src/silver/create_silver_tables.py` | Complete |
| Gold analytics | `src/gold/create_gold_tables.py` | Complete |

Verify Gold tables exist:

```sql
SHOW TABLES IN gold;

SELECT COUNT(*) FROM gold.sales_by_product;        -- 500
SELECT COUNT(*) FROM gold.revenue_by_customer;     -- ~9,930
SELECT COUNT(*) FROM gold.customer_segmentation;   -- ~9,930
```

---

## Step 1 — Open SQL workspace

1. In Databricks, go to **SQL** in the left sidebar (or **SQL Editor**).
2. Use an existing **SQL warehouse** or create one:
   - **SQL** → **SQL Warehouses** → **Create SQL Warehouse**
   - Community Edition: a small warehouse is sufficient for this dataset.

---

## Step 2 — Create the three queries

Open `src/dashboard/dashboard_queries.sql` from your repo. Create **one saved query per visualization** (recommended for Lakeview).

### Query A — Top 10 products by revenue

Copy the first `SELECT` block (section 1) into a new SQL query.

- **Name:** `Top 10 Products by Revenue`
- **Expected columns:** `product_name`, `total_revenue`, `order_count`, …

**Test:**

```sql
SELECT product_name, total_revenue, order_count
FROM gold.sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;
```

### Query B — Customer revenue distribution

Copy section 2 (revenue buckets).

- **Name:** `Customer Revenue Distribution`
- **Expected columns:** `revenue_bucket`, `customer_count`, `pct_of_customers`

### Query C — Customer segmentation

Copy section 3.

- **Name:** `Customer Segmentation Summary`
- **Expected columns:** `segment_type`, `customer_count`, `total_revenue`, …

Save each query after it runs successfully.

---

## Step 3 — Create a Lakeview dashboard

1. Go to **Dashboards** (or **Lakeview** → **Create dashboard**).
2. Click **Create dashboard** → name it **E-commerce Medallion Analytics**.
3. Add a visualization for each saved query.

### Tile 1 — Top 10 products (bar chart)

| Setting | Value |
|---------|-------|
| Query | Top 10 Products by Revenue |
| Chart type | **Bar** |
| X-axis | `product_name` |
| Y-axis | `total_revenue` |
| Sort | `total_revenue` descending (query already limits 10) |

Optional: color by `category`.

### Tile 2 — Revenue distribution (histogram-style bar)

| Setting | Value |
|---------|-------|
| Query | Customer Revenue Distribution |
| Chart type | **Bar** |
| X-axis | `revenue_bucket` |
| Y-axis | `customer_count` |
| Sort | Use `bucket_order` from query (already ordered) |

This shows how customers are spread across revenue bands.

### Tile 3 — Segmentation (pie / donut)

| Setting | Value |
|---------|-------|
| Query | Customer Segmentation Summary |
| Chart type | **Pie** or **Donut** |
| Slice / group | `segment_type` |
| Value | `customer_count` (or `total_revenue` for revenue-weighted view) |

Expected segments: **High-Value**, **Repeat**, **One-Time**, **Inactive**.

---

## Step 4 — Arrange and publish

1. Drag tiles into a single row or 2×2 layout.
2. Add a title and short description (e.g. “Gold layer — quality-pass customers & completed orders”).
3. **Publish** or **Share** the dashboard (link for assessment submission).

---

## Optional KPI row

Section 4 in `dashboard_queries.sql` has three small queries for optional KPI counters:

- Total revenue
- Active customers (`order_count > 0`)
- Products with sales

Add each as a **Counter** visualization above the main charts.

---

## Validation checklist

| Check | How to verify |
|-------|----------------|
| Queries use Gold only | No `bronze.*` or `silver.*` in dashboard SQL |
| Top product matches manual check | Compare #1 row to `ORDER BY total_revenue DESC LIMIT 1` |
| Segmentation totals | `SUM(customer_count)` ≈ quality-pass customer count (~9,930) |
| Revenue sum | `SUM(total_revenue)` from segmentation ≈ sum from `revenue_by_customer` |

Example checks you already ran:

```sql
SELECT segment_type, COUNT(*) AS customer_count, SUM(total_revenue) AS total_revenue
FROM gold.customer_segmentation
GROUP BY segment_type
ORDER BY total_revenue DESC;

SELECT product_name, total_revenue, order_count
FROM gold.sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `TABLE_OR_VIEW_NOT_FOUND` | Run `create_gold_tables.py` first |
| Empty charts | Confirm Gold row counts; re-run Gold after Silver |
| Warehouse won’t start | Community Edition limits — wait and retry or use smaller warehouse |
| Bar chart shows too many products | Ensure query includes `LIMIT 10` |
| Buckets out of order | Use the provided query with `bucket_order`; sort by that column in the viz |

---

## Assessment mapping

| Requirement | File / artifact |
|-------------|-----------------|
| Top 10 products by revenue | `dashboard_queries.sql` §1 |
| Customer revenue distribution | `dashboard_queries.sql` §2 |
| Customer segmentation | `dashboard_queries.sql` §3 |
| Queries target Gold tables | `gold.sales_by_product`, `gold.revenue_by_customer`, `gold.customer_segmentation` |
| Lakeview instructions | This guide |

---

## Run order (full pipeline)

```text
Bronze (ingest_all.py) → Silver (create_silver_tables.py) → Gold (create_gold_tables.py) → Dashboard (this guide)
```

After any Silver or Gold logic change, re-run the affected layers before refreshing the dashboard.
