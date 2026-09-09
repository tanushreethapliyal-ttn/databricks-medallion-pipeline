# AI Prompt — Gold Layer

## Context

Databricks Medallion pipeline (e-commerce). Bronze and Silver are complete.
Gold reads `silver.customers`, `silver.orders`, `silver.products`.

## Rules

- Filter `is_quality_pass = true` before aggregating
- Revenue metrics use `order_status = 'Completed'` only
- Use `total_amount` for revenue
- Delta overwrite per run; add `last_updated` timestamp
- Do not read Bronze directly

## Tables

1. `gold.sales_by_product` — one row per quality-pass product
2. `gold.revenue_by_customer` — one row per quality-pass customer
3. `gold.customer_segmentation` — derived tiers + `segment_type` for dashboard

## Prompt used

> Implement the Gold layer for the medallion assessment.
> Create `src/gold/` with aggregations, pipeline orchestration, and a Databricks
> notebook `create_gold_tables.py`. Follow the same patterns as Silver (bootstrap,
> config_loader from bronze, overwrite Delta tables). Use quality-pass Silver rows
> and Completed orders for revenue. Include customer segmentation with revenue
> tiers and segment_type (High-Value / Repeat / One-Time / Inactive).
