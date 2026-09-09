# Database Setup Notes (Databricks Community Edition)

## Schemas

Run once in a Databricks SQL or notebook:

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

Tables are created on first Bronze write via `saveAsTable`.

## CSV landing path

Default configured in `config/pipeline_config.yaml`:

```text
/Workspace/Users/tanushreethapliyal3@gmail.com/databricks-medallion-pipeline/databricks-medallion-pipeline/Data/raw
```

Expected files:

- `customers.csv`
- `orders.csv`
- `products.csv`

Override at runtime with the `landing_path` widget on `ingest_all.py`.

## Run Bronze ingestion

1. Open `src/bronze/ingest_all.py` in Databricks Repos.
2. Attach to a cluster with Delta support.
3. Run all cells.
4. Verify:

```sql
SELECT COUNT(*) FROM bronze.customers;   -- 10000
SELECT COUNT(*) FROM bronze.orders;      -- 100000
SELECT COUNT(*) FROM bronze.products;    -- 500

SELECT * FROM bronze.ingestion_log ORDER BY ingestion_timestamp DESC;
```
