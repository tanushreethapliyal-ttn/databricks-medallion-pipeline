-- =============================================================================
-- E-commerce Medallion Pipeline — Dashboard SQL (Gold layer)
-- =============================================================================
-- Run in Databricks SQL Editor or attach queries to a Lakeview dashboard.
-- Prerequisites: Bronze, Silver, and Gold pipelines completed.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1) Top 10 products by revenue
--    Visualization: Bar chart
--    X-axis: product_name   Y-axis: total_revenue
--    Source table: gold.sales_by_product
-- -----------------------------------------------------------------------------
SELECT
  product_id,
  product_name,
  category,
  total_revenue,
  order_count,
  total_quantity,
  ROUND(avg_unit_price, 2) AS avg_unit_price
FROM gold.sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 2) Customer revenue distribution
--    Visualization: Bar chart (histogram-style buckets)
--    X-axis: revenue_bucket   Y-axis: customer_count
--    Source table: gold.revenue_by_customer
-- -----------------------------------------------------------------------------
SELECT
  revenue_bucket,
  customer_count,
  ROUND(pct_of_customers * 100, 2) AS pct_of_customers
FROM (
  SELECT
    CASE
      WHEN total_revenue >= 10000 THEN '10,000+'
      WHEN total_revenue >= 5000 THEN '5,000 - 9,999'
      WHEN total_revenue >= 1000 THEN '1,000 - 4,999'
      WHEN total_revenue > 0 THEN '1 - 999'
      ELSE '0 (no revenue)'
    END AS revenue_bucket,
    CASE
      WHEN total_revenue >= 10000 THEN 5
      WHEN total_revenue >= 5000 THEN 4
      WHEN total_revenue >= 1000 THEN 3
      WHEN total_revenue > 0 THEN 2
      ELSE 1
    END AS bucket_order,
    COUNT(*) AS customer_count,
    COUNT(*) / SUM(COUNT(*)) OVER () AS pct_of_customers
  FROM gold.revenue_by_customer
  GROUP BY 1, 2
) buckets
ORDER BY bucket_order;


-- -----------------------------------------------------------------------------
-- 3) Customer segmentation summary
--    Visualization: Pie or donut chart
--    Slice: segment_type   Value: customer_count (or total_revenue)
--    Source table: gold.customer_segmentation
-- -----------------------------------------------------------------------------
SELECT
  segment_type,
  COUNT(*) AS customer_count,
  ROUND(SUM(total_revenue), 2) AS total_revenue,
  ROUND(AVG(total_revenue), 2) AS avg_revenue_per_customer,
  ROUND(
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
    2
  ) AS pct_of_customers
FROM gold.customer_segmentation
GROUP BY segment_type
ORDER BY total_revenue DESC;


-- -----------------------------------------------------------------------------
-- Optional KPI tiles (bonus — not required by assessment)
-- -----------------------------------------------------------------------------
-- Total revenue (completed, quality-pass orders reflected in Gold)
SELECT ROUND(SUM(total_revenue), 2) AS total_revenue
FROM gold.revenue_by_customer;

-- Active customers with at least one completed order
SELECT COUNT(*) AS active_customers
FROM gold.revenue_by_customer
WHERE order_count > 0;

-- Products with sales
SELECT COUNT(*) AS products_with_sales
FROM gold.sales_by_product
WHERE order_count > 0;
