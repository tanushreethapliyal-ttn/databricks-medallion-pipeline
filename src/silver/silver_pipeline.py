"""Orchestrate Bronze -> Silver transformations and quality checks."""

from __future__ import annotations

import uuid
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config_loader import qualified_table_name
from metrics import build_quality_metrics, write_quality_metrics
from quality_checks import (
    apply_business_logic_checks,
    apply_completeness_checks,
    apply_referential_integrity_checks,
    apply_type_validation_checks,
    apply_uniqueness_checks,
    finalize_quality_columns,
    read_bronze_table,
)
from transforms import cast_customers, cast_orders, cast_products


class SilverPipelineError(Exception):
    """Raised when Silver processing fails."""


SILVER_PIPELINE_VERSION = "regexp-int-v3"


def _write_silver_table(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def _empty_flags() -> F.Column:
    return F.array().cast("array<string>")


def build_silver_customers(spark: SparkSession, config: dict[str, Any]) -> tuple[DataFrame, DataFrame]:
    bronze_table = qualified_table_name(config, "bronze", "customers")
    bronze_df = read_bronze_table(spark, bronze_table)

    silver_df = cast_customers(bronze_df)
    silver_df = apply_completeness_checks(silver_df, "customers")
    silver_df = apply_uniqueness_checks(silver_df, "customers", "customer_id")
    silver_df = apply_type_validation_checks(silver_df, "customers")
    silver_df = silver_df.withColumn("_ref_integrity_flags", _empty_flags())
    silver_df = silver_df.withColumn("_business_flags", _empty_flags())
    return finalize_quality_columns(silver_df), silver_df


def build_silver_products(spark: SparkSession, config: dict[str, Any]) -> tuple[DataFrame, DataFrame]:
    bronze_table = qualified_table_name(config, "bronze", "products")
    bronze_df = read_bronze_table(spark, bronze_table)

    silver_df = cast_products(bronze_df)
    silver_df = apply_completeness_checks(silver_df, "products")
    silver_df = apply_uniqueness_checks(silver_df, "products", "product_id")
    silver_df = apply_type_validation_checks(silver_df, "products")
    silver_df = silver_df.withColumn("_ref_integrity_flags", _empty_flags())
    silver_df = silver_df.withColumn("_business_flags", _empty_flags())
    return finalize_quality_columns(silver_df), silver_df


def build_silver_orders(
    spark: SparkSession,
    config: dict[str, Any],
    customers_df: DataFrame,
    products_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    bronze_table = qualified_table_name(config, "bronze", "orders")
    bronze_df = read_bronze_table(spark, bronze_table)

    silver_df = cast_orders(bronze_df)
    non_null_customer_ids = silver_df.filter(F.col("customer_id").isNotNull()).count()
    non_null_product_ids = silver_df.filter(F.col("product_id").isNotNull()).count()
    print(
        f"DEBUG [{SILVER_PIPELINE_VERSION}] orders cast check — "
        f"non-null customer_id: {non_null_customer_ids}, "
        f"non-null product_id: {non_null_product_ids}"
    )
    silver_df = apply_completeness_checks(silver_df, "orders")
    silver_df = apply_uniqueness_checks(silver_df, "orders", "order_id")
    silver_df = apply_type_validation_checks(silver_df, "orders")
    silver_df = apply_referential_integrity_checks(silver_df, customers_df, products_df)
    silver_df = apply_business_logic_checks(silver_df, "orders")
    checked_df = silver_df
    return finalize_quality_columns(silver_df), checked_df


def run_silver_pipeline(spark: SparkSession, config: dict[str, Any], run_id: str | None = None) -> dict[str, int]:
    """Build all Silver tables and quality metrics."""
    effective_run_id = run_id or str(uuid.uuid4())
    metrics_table = qualified_table_name(config, "silver", "quality_metrics")
    row_counts: dict[str, int] = {}

    print(f"Starting Silver pipeline (run_id={effective_run_id}, version={SILVER_PIPELINE_VERSION})")

    products_final, products_checked = build_silver_products(spark, config)
    products_table = qualified_table_name(config, "silver", "products")
    _write_silver_table(products_final, products_table)
    row_counts["products"] = products_final.count()
    print(f"SUCCESS: {products_table} -> {row_counts['products']} rows")

    customers_final, customers_checked = build_silver_customers(spark, config)
    customers_table = qualified_table_name(config, "silver", "customers")
    _write_silver_table(customers_final, customers_table)
    row_counts["customers"] = customers_final.count()
    print(f"SUCCESS: {customers_table} -> {row_counts['customers']} rows")

    orders_final, orders_checked = build_silver_orders(
        spark, config, customers_checked, products_checked
    )
    orders_table = qualified_table_name(config, "silver", "orders")
    _write_silver_table(orders_final, orders_table)
    row_counts["orders"] = orders_final.count()
    print(f"SUCCESS: {orders_table} -> {row_counts['orders']} rows")

    all_metrics = (
        build_quality_metrics(
            spark, effective_run_id, customers_table, "customers", customers_checked
        )
        .unionByName(
            build_quality_metrics(
                spark, effective_run_id, products_table, "products", products_checked
            )
        )
        .unionByName(
            build_quality_metrics(
                spark, effective_run_id, orders_table, "orders", orders_checked
            )
        )
    )
    write_quality_metrics(all_metrics, metrics_table, mode="append")
    print(f"SUCCESS: quality metrics appended to {metrics_table}")

    failed_customers = customers_final.filter(~F.col("is_quality_pass")).count()
    failed_orders = orders_final.filter(~F.col("is_quality_pass")).count()
    print(f"Quality failures — customers: {failed_customers}, orders: {failed_orders}")

    return row_counts
