-- Database / schema setup for the e-commerce medallion pipeline.
-- Run in Databricks SQL or a notebook: %sql

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze entity tables are created automatically on first Delta write (saveAsTable).
-- Expected Bronze schemas (business columns stored as STRING + metadata):

-- bronze.customers
--   customer_id, customer_name, email, country, signup_date,
--   customer_segment, lifetime_value,
--   _ingestion_timestamp, _source_file, _run_id

-- bronze.orders
--   order_id, customer_id, order_date, product_id, quantity,
--   unit_price, total_amount, order_status, payment_date,
--   _ingestion_timestamp, _source_file, _run_id

-- bronze.products
--   product_id, product_name, category, price, cost,
--   stock_quantity, reorder_level,
--   _ingestion_timestamp, _source_file, _run_id

-- bronze.ingestion_log (append-only audit)
--   log_id, run_id, source_path, target_table, row_count,
--   ingestion_timestamp, status, error_message
