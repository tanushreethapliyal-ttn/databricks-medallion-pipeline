# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard — Validate Gold SQL Queries
# MAGIC Smoke-test the three dashboard queries against Gold tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1) Top 10 products by revenue
# MAGIC SELECT
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   total_revenue,
# MAGIC   order_count
# MAGIC FROM gold.sales_by_product
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2) Customer revenue distribution (histogram buckets)
# MAGIC SELECT
# MAGIC   revenue_bucket,
# MAGIC   customer_count,
# MAGIC   ROUND(pct_of_customers * 100, 2) AS pct_of_customers
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN total_revenue >= 10000 THEN '10,000+'
# MAGIC       WHEN total_revenue >= 5000 THEN '5,000 - 9,999'
# MAGIC       WHEN total_revenue >= 1000 THEN '1,000 - 4,999'
# MAGIC       WHEN total_revenue > 0 THEN '1 - 999'
# MAGIC       ELSE '0 (no revenue)'
# MAGIC     END AS revenue_bucket,
# MAGIC     CASE
# MAGIC       WHEN total_revenue >= 10000 THEN 5
# MAGIC       WHEN total_revenue >= 5000 THEN 4
# MAGIC       WHEN total_revenue >= 1000 THEN 3
# MAGIC       WHEN total_revenue > 0 THEN 2
# MAGIC       ELSE 1
# MAGIC     END AS bucket_order,
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     COUNT(*) / SUM(COUNT(*)) OVER () AS pct_of_customers
# MAGIC   FROM gold.revenue_by_customer
# MAGIC   GROUP BY 1, 2
# MAGIC ) buckets
# MAGIC ORDER BY bucket_order;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) Customer segmentation summary
# MAGIC SELECT
# MAGIC   segment_type,
# MAGIC   COUNT(*) AS customer_count,
# MAGIC   ROUND(SUM(total_revenue), 2) AS total_revenue,
# MAGIC   ROUND(AVG(total_revenue), 2) AS avg_revenue_per_customer
# MAGIC FROM gold.customer_segmentation
# MAGIC GROUP BY segment_type
# MAGIC ORDER BY total_revenue DESC;
