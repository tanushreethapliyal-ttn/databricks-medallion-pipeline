"""Orchestrate Silver -> Gold analytics table builds."""

from __future__ import annotations

from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession

from aggregations import (
    GoldAggregationError,
    build_customer_segmentation,
    build_revenue_by_customer,
    build_sales_by_product,
    completed_quality_pass_orders,
    current_processing_timestamp,
    quality_pass,
    read_silver_tables,
)
from config_loader import qualified_table_name


class GoldPipelineError(Exception):
    """Raised when Gold processing fails."""


GOLD_PIPELINE_VERSION = "v1"


def _write_gold_table(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def run_gold_pipeline(spark: SparkSession, config: dict[str, Any]) -> dict[str, int]:
    """Build all Gold tables from current Silver snapshots."""
    print(f"Starting Gold pipeline (version={GOLD_PIPELINE_VERSION})")

    try:
        silver_tables = read_silver_tables(spark, config)
    except GoldAggregationError as exc:
        raise GoldPipelineError(str(exc)) from exc

    processed_at = current_processing_timestamp()
    customers = silver_tables["customers"]
    orders = silver_tables["orders"]
    products = silver_tables["products"]

    builders: list[tuple[str, Callable[..., DataFrame], DataFrame]] = [
        ("sales_by_product", build_sales_by_product, products),
        ("revenue_by_customer", build_revenue_by_customer, customers),
        ("customer_segmentation", build_customer_segmentation, customers),
    ]

    row_counts: dict[str, int] = {}
    for table_key, builder, master_df in builders:
        gold_df = builder(master_df, orders, config, processed_at)
        table_name = qualified_table_name(config, "gold", table_key)
        _write_gold_table(gold_df, table_name)
        row_counts[table_key] = gold_df.count()
        print(f"SUCCESS: {table_name} -> {row_counts[table_key]} rows")

    pass_orders = completed_quality_pass_orders(orders, config).count()
    pass_customers = quality_pass(customers).count()
    pass_products = quality_pass(products).count()
    print(
        "Gold inputs — "
        f"quality-pass customers: {pass_customers}, "
        f"products: {pass_products}, "
        f"completed quality-pass orders: {pass_orders}"
    )

    return row_counts
