# E-commerce Medallion Pipeline — Databricks Assessment

Batch analytics pipeline: **CSV → Bronze → Silver → Gold → SQL Dashboard** on Databricks Community Edition using Python, PySpark, Delta Lake, and SQL.

## Candidate

See [candidate-info.md](candidate-info.md).

## Repository layout

```text
C1_assestment/
├── config/pipeline_config.yaml      # Paths, schemas, table names
├── data/raw/                        # Source CSVs (customers, orders, products)
├── data/seed-data-notes.md          # How sample data was generated
├── database/                        # Schema DDL and setup notes
├── docs/                            # Assessment documentation (this folder)
├── ai-prompts/                      # AI prompt history by layer
└── src/
    ├── data_generation/               # Local CSV generator (seed 42)
    ├── bronze/                      # Raw ingest → Delta
    ├── silver/                      # Typing + DQ checks + metrics
    ├── gold/                        # Business aggregates
    └── dashboard/                   # SQL queries + Lakeview guide
```

## Prerequisites

| Environment | Requirements |
|-------------|--------------|
| **Local** (data generation only) | Python 3.10+, `pip install -r requirements.txt` |
| **Databricks** | Community Edition workspace, cluster with Delta, Repos linked to GitHub |

## Configuration

Edit `config/pipeline_config.yaml` or override at runtime:

| Setting | Default |
|---------|---------|
| `landing_path` | Workspace path to `Data/raw/` |
| `bronze_schema` / `silver_schema` / `gold_schema` | `bronze`, `silver`, `gold` |
| `catalog` | Empty (Hive metastore on Community Edition) |

**Databricks widget overrides:** `landing_path`, `run_id` on ingest/Silver notebooks.

## Pipeline run order

Run in Databricks Repos **in this order**:

| Step | Notebook | Output |
|------|----------|--------|
| 0 (optional, local) | `python src/data_generation/generate_sample_data.py` | `data/raw/*.csv` |
| 1 | `src/bronze/ingest_all.py` | `bronze.customers`, `bronze.orders`, `bronze.products`, `bronze.ingestion_log` |
| 2 | `src/silver/create_silver_tables.py` | `silver.*` + `silver.quality_metrics` |
| 3 | `src/gold/create_gold_tables.py` | `gold.sales_by_product`, `gold.revenue_by_customer`, `gold.customer_segmentation` |
| 4 | Dashboard | Lakeview — see `src/dashboard/DASHBOARD_GUIDE.md` |

After each layer change, **restart Python** on the cluster before re-running downstream notebooks.

## Quick validation

```sql
-- Bronze row counts
SELECT 'customers' AS t, COUNT(*) FROM bronze.customers
UNION ALL SELECT 'orders', COUNT(*) FROM bronze.orders
UNION ALL SELECT 'products', COUNT(*) FROM bronze.products;

-- Silver quality failures (latest orders metrics)
SELECT check_name, failed_count
FROM silver.quality_metrics
WHERE table_name = 'silver.orders'
  AND measured_at = (SELECT MAX(measured_at) FROM silver.quality_metrics WHERE table_name = 'silver.orders');

-- Gold sanity
SELECT COUNT(*) FROM gold.sales_by_product;
SELECT segment_type, COUNT(*) FROM gold.customer_segmentation GROUP BY 1;
```

**Expected Silver order failures:** ~100 NULL customer_id, ~200 NULL product_id, ~50 orphan customer_id, ~30 orphan product_id, ~40 duplicate order_id rows.

## Key design decisions

- **Bronze:** Business columns as `STRING`; raw fidelity; metadata columns `_ingestion_timestamp`, `_source_file`, `_run_id`.
- **Silver:** Cast + flag bad rows (`quality_check_result`, `is_quality_pass`); no silent deletes.
- **Gold:** Only `is_quality_pass = true`; revenue from `order_status = 'Completed'` using `total_amount`.
- **Dashboard:** Read-only SQL over Gold tables.

## Documentation index

| Document | Description |
|----------|-------------|
| [requirements-analysis.md](requirements-analysis.md) | Functional / non-functional requirements |
| [design-notes.md](design-notes.md) | Architecture and layer design |
| [data-model.md](data-model.md) | Schemas, grains, intentional defects |
| [data-quality-strategy.md](data-quality-strategy.md) | DQ rules, codes, and metrics |
| [debugging-notes.md](debugging-notes.md) | Issues encountered and fixes |
| [tool-workflow.md](tool-workflow.md) | Cursor / AI workflow |
| [reflection.md](reflection.md) | What worked, trade-offs, improvements |
| [final-ai-usage-summary.md](final-ai-usage-summary.md) | Consolidated AI usage summary |

## Dashboard

- SQL: `src/dashboard/dashboard_queries.sql`
- Setup: `src/dashboard/DASHBOARD_GUIDE.md`
- Smoke test: `src/dashboard/validate_dashboard_queries.py`

## AI assistance

Prompt history is in `ai-prompts/` (bronze, silver, gold, dashboard, debugging, documentation).
