# AI Prompt — Dashboard

## Context

Gold layer complete with:
- `gold.sales_by_product`
- `gold.revenue_by_customer`
- `gold.customer_segmentation`

Assessment requires SQL dashboard queries (not custom UI code).

## Deliverables

1. `src/dashboard/dashboard_queries.sql` — three required queries + optional KPIs
2. `src/dashboard/DASHBOARD_GUIDE.md` — Lakeview setup steps
3. `src/dashboard/validate_dashboard_queries.py` — Databricks smoke-test notebook

## Prompt used

> Implement the Dashboard for the medallion assessment.
> Provide SQL queries for: (1) top 10 products by revenue, (2) customer revenue
> distribution histogram, (3) customer segmentation pie chart. All queries must
> read from Gold tables only. Include a Lakeview setup guide and a validation
> notebook for Databricks Community Edition.
