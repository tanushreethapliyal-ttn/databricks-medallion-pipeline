# Requirement Analysis

## Problem Statement

An e-commerce company needs a batch data pipeline on Databricks that ingests three CSV sources—customers, orders, and products—through a Medallion Architecture (Bronze → Silver → Gold) and exposes analytics via SQL for dashboarding.

Source data is intentionally imperfect: missing values, duplicate primary keys, invalid foreign keys, and type inconsistencies are present by design. The pipeline must ingest raw data without silent correction, apply structured quality checks in Silver, flag rather than drop bad records, produce Gold analytics from trustworthy rows, and support operational visibility through ingestion logging and quality metrics.

Technology constraints: Python, PySpark, SQL, Databricks, and Delta Lake where appropriate.

## Functional Requirements

### Data Generation

- Provide three CSV datasets matching specified volumes and schemas:
  - `customers.csv`: 10,000 rows
  - `orders.csv`: 100,000 rows
  - `products.csv`: 500 rows
- Embed intentional defects at specified counts:
  - Customers: 50 NULL emails; 10 duplicate `customer_id` records
  - Orders: 100 NULL `customer_id`; 200 NULL `product_id`; 50 `customer_id` values not in customers; 30 `product_id` values not in products; 20 duplicate `order_id` rows
- Column definitions must match the assessment specification (types, nullable fields, and enumerated values where specified).

### Bronze

- Ingest raw CSV files into the Bronze layer.
- Preserve source data without business transformations.
- Apply explicit schemas where practical.
- Log ingestion metadata including row count and timestamp.
- Support ingestion of all three source entities: customers, orders, products.

### Silver

- Implement four quality check categories:
  1. Completeness
  2. Uniqueness
  3. Type validation
  4. Referential integrity
- Flag bad records rather than silently deleting them.
- Include a `quality_check_result` field (or equivalent) on Silver records.
- Generate quality metrics summarizing check outcomes.
- Produce typed Silver tables for customers, orders, and products.

### Gold

- Create three Gold outputs:
  1. Sales by Product
  2. Revenue by Customer
  3. Customer Segmentation
- Derive Gold tables from cleansed/validated Silver data according to the project's quality-pass rules.

### Dashboard

- Provide SQL queries for:
  1. Top 10 products by revenue
  2. Customer revenue distribution
  3. Customer segmentation
- Queries must target Gold-layer outputs (or documented equivalents).

### Testing

- Include tests validating pipeline behavior.
- Cover data quality rules, transformations, and key analytics logic.
- Use fixture data where full-scale CSVs are impractical for automated runs.

### Error Handling

- Include input validation (e.g., source paths, expected inputs).
- Handle pipeline failures without leaving the system in an undefined state.
- Record failures in ingestion or operational logs where applicable.

### Documentation (assessment deliverables)

- README
- Requirement analysis (this document)
- Design notes
- Data model
- Data quality strategy
- Debugging notes
- Reflection
- AI prompt history

## Non-Functional Requirements

### Maintainability

- Organize code into logical modules or notebooks (Bronze, Silver, Gold, SQL).
- Use configuration for paths, table names, and environment-specific settings.
- Follow consistent naming aligned with Medallion layers (`bronze`, `silver`, `gold`).

### Reliability

- Pipeline steps should fail explicitly on invalid inputs or unrecoverable errors.
- Partial outputs should be traceable via logs and metrics.
- Quality failures in source data must not crash the entire pipeline unless ingestion itself is invalid.

### Data quality

- Quality rules must be explicit, auditable, and repeatable.
- Failed records remain available for inspection with clear failure codes.
- Quality metrics must be generated per run for monitoring and assessment evidence.

### Reproducibility

- Document run order: Bronze → Silver → Gold → Dashboard queries.
- Use versioned schemas and deterministic quality rules so re-runs produce comparable results.
- Fixture data for tests must reproduce known defect patterns.

### Observability

- Bronze ingestion must log row counts and timestamps.
- Silver must emit quality metrics (counts/rates by check type and table).
- Errors and ingestion status must be retrievable from logs or log tables.

### Performance

- Design for batch processing at stated volumes (10k / 100k / 500 rows) without unnecessary complexity.
- Gold aggregations should read filtered Silver data efficiently (appropriate joins and filters).

### Idempotency

- Re-running Bronze ingestion should follow a documented strategy (overwrite or append with run identifier) so targets are not corrupted by duplicate loads.
- Silver and Gold rebuilds should be deterministic given the same Bronze/Silver inputs and quality rules.

## Assumptions

The following are **not** explicit assessment requirements; they are working assumptions to unblock implementation. Confirm or override before coding.

| Assumption | Rationale |
|------------|-----------|
| Revenue in Gold uses `total_amount` on orders | `total_amount` is the stated order revenue field |
| Only `Completed` orders contribute to revenue metrics | Standard e-commerce practice; assessment does not specify status filtering |
| Gold reads from Silver, not directly from Bronze | Medallion pattern and quality-pass filtering |
| Silver retains all ingested rows; exclusion happens only in Gold aggregates | Assessment requires flagging, not silent deletion |
| `quality_check_result` holds one or more standardized failure codes | Enables metrics and Gold filtering |
| CSV files are available at configurable cloud/DBFS paths before pipeline run | Databricks ingestion prerequisite |
| Dashboard SQL runs against Gold Delta tables in Databricks | Technology stack specifies Databricks + SQL |
| Duplicate primary-key rows are all retained in Silver with uniqueness flags | Assessment specifies duplicates exist and bad records must be flagged |
| Local tests use small fixtures; full CSV volumes are validated manually or in Databricks | Practical test runtime |

**Explicit requirements (not assumptions):** layer responsibilities, DQ check types, flag-not-delete behavior, Gold and Dashboard outputs, technology stack, dataset sizes, schemas, intentional defects, and documentation deliverables listed in the assessment.

## Edge Cases

| Edge case | Expected handling |
|-----------|-------------------|
| Empty files | Fail ingestion or log zero-row load with explicit status; do not proceed silently |
| Missing columns | Fail Bronze ingestion or validation with clear error; log failure |
| Duplicate records | Flag `UNIQUENESS_FAIL` in Silver; do not silently deduplicate |
| NULL keys | Flag completeness and/or referential integrity failures on orders |
| Invalid dates | Flag `TYPE_VALIDATION_FAIL` in Silver |
| Invalid numeric values | Flag `TYPE_VALIDATION_FAIL` in Silver |
| Orphan foreign keys | Flag `REF_INTEGRITY_FAIL` for orders referencing missing customers or products |
| NULL email (customers) | Flag completeness failure per Silver rules |
| Multiple failures on one row | Accumulate all applicable codes in `quality_check_result` |
| Reprocessing the same data | Idempotent Bronze strategy documented; Silver/Gold metrics reflect latest run |
| Partial pipeline failures | Bronze success without Silver should not imply Gold is current; logs reflect step status |
| Orders with invalid `customer_id` / `product_id` | Flagged in Silver; excluded from Gold joins that require valid FKs |
| Products or customers with no orders | Present in Silver; zero revenue/sales in Gold where applicable |
| `payment_date` nullable by schema | NULL is valid; consistency with `order_status` may be a Silver business rule |

## Clarifications Needed

| Topic | Why ambiguous |
|-------|----------------|
| Silver duplicate handling | Keep all duplicate rows flagged, or retain one canonical row per key? |
| Order status in revenue | Include `Pending` orders in revenue, or only `Completed`? Exclude `Cancelled`? |
| Email completeness severity | Is NULL email a hard fail (`is_quality_pass = false`) or warn-only? |
| Customer Segmentation (Gold) | Use source `customer_segment` only, or derive segments from behavior (revenue, order count)? |
| Bronze schema strictness | Cast to types in Bronze vs keep strings and cast in Silver while still "preserving raw data" |
| Bronze load mode | Full overwrite per run vs append with `run_id` for audit history |
| Dashboard platform | Databricks SQL/Lakeview only, or export queries for external BI? |
| Unity Catalog vs hive metastore | Catalog and schema naming for tables |
| `total_amount` vs `quantity × unit_price` | Which field is authoritative if they disagree? |
| Quality metrics grain | Per table per run only, or also per check per column? |
