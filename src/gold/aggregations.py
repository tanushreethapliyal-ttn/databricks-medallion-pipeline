"""Build Gold-layer analytic tables from quality-pass Silver data."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config_loader import qualified_table_name


class GoldAggregationError(Exception):
    """Raised when Gold aggregation inputs are invalid."""


def _gold_settings(config: dict[str, Any]) -> dict[str, Any]:
    defaults = {
        "completed_order_status": "Completed",
        "revenue_tier_high_min": 5000.0,
        "revenue_tier_medium_min": 1000.0,
    }
    settings = config.get("gold_settings") or {}
    return {**defaults, **settings}


def _read_silver_table(spark: SparkSession, table_name: str) -> DataFrame:
    if not spark.catalog.tableExists(table_name):
        raise GoldAggregationError(f"Silver table not found: {table_name}")
    return spark.table(table_name)


def read_silver_tables(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Load Silver entity tables."""
    return {
        "customers": _read_silver_table(
            spark, qualified_table_name(config, "silver", "customers")
        ),
        "orders": _read_silver_table(
            spark, qualified_table_name(config, "silver", "orders")
        ),
        "products": _read_silver_table(
            spark, qualified_table_name(config, "silver", "products")
        ),
    }


def quality_pass(df: DataFrame) -> DataFrame:
    """Keep rows that passed Silver quality checks."""
    return df.filter(F.col("is_quality_pass") == True)


def completed_quality_pass_orders(orders_df: DataFrame, config: dict[str, Any]) -> DataFrame:
    """Quality-pass orders used for revenue and sales metrics."""
    settings = _gold_settings(config)
    return quality_pass(orders_df).filter(
        F.col("order_status") == settings["completed_order_status"]
    )


def build_sales_by_product(
    products_df: DataFrame,
    orders_df: DataFrame,
    config: dict[str, Any],
    processed_at: datetime,
) -> DataFrame:
    """
    One row per quality-pass product with completed-order sales KPIs.

    Products with no completed orders appear with zero metrics.
    """
    products = quality_pass(products_df).select(
        "product_id",
        F.col("product_name").alias("product_name"),
        F.col("category").alias("category"),
    )
    completed_orders = completed_quality_pass_orders(orders_df, config)

    order_metrics = completed_orders.groupBy("product_id").agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_amount").alias("total_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.avg("unit_price").alias("avg_unit_price"),
    )

    return (
        products.join(order_metrics, on="product_id", how="left")
        .select(
            "product_id",
            "product_name",
            "category",
            F.coalesce(F.col("total_quantity"), F.lit(0)).alias("total_quantity"),
            F.coalesce(F.col("total_revenue"), F.lit(0.0)).alias("total_revenue"),
            F.coalesce(F.col("order_count"), F.lit(0)).alias("order_count"),
            F.coalesce(F.col("avg_unit_price"), F.lit(0.0)).alias("avg_unit_price"),
            F.lit(processed_at).alias("last_updated"),
        )
    )


def build_revenue_by_customer(
    customers_df: DataFrame,
    orders_df: DataFrame,
    config: dict[str, Any],
    processed_at: datetime,
) -> DataFrame:
    """One row per quality-pass customer with completed-order revenue KPIs."""
    customers = quality_pass(customers_df).select(
        "customer_id",
        F.col("customer_name").alias("customer_name"),
        F.col("country").alias("country"),
        F.col("customer_segment").alias("customer_segment"),
        F.col("lifetime_value").alias("lifetime_value_actual"),
    )
    completed_orders = completed_quality_pass_orders(orders_df, config)

    order_metrics = completed_orders.groupBy("customer_id").agg(
        F.sum("total_amount").alias("total_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.max("order_date").alias("last_order_date"),
    )

    return (
        customers.join(order_metrics, on="customer_id", how="left")
        .select(
            "customer_id",
            "customer_name",
            "country",
            "customer_segment",
            F.coalesce(F.col("total_revenue"), F.lit(0.0)).alias("total_revenue"),
            F.coalesce(F.col("order_count"), F.lit(0)).alias("order_count"),
            F.when(
                F.coalesce(F.col("order_count"), F.lit(0)) > 0,
                F.col("total_revenue") / F.col("order_count"),
            )
            .otherwise(F.lit(0.0))
            .alias("avg_order_value"),
            F.col("last_order_date"),
            F.col("lifetime_value_actual"),
            F.lit(processed_at).alias("last_updated"),
        )
    )


def build_customer_segmentation(
    customers_df: DataFrame,
    orders_df: DataFrame,
    config: dict[str, Any],
    processed_at: datetime,
) -> DataFrame:
    """One row per quality-pass customer with derived segmentation attributes."""
    settings = _gold_settings(config)
    high_min = float(settings["revenue_tier_high_min"])
    medium_min = float(settings["revenue_tier_medium_min"])

    revenue_by_customer = build_revenue_by_customer(
        customers_df, orders_df, config, processed_at
    ).select(
        "customer_id",
        "customer_segment",
        "total_revenue",
        "order_count",
        "lifetime_value_actual",
    )

    revenue_tier = (
        F.when(F.col("total_revenue") >= high_min, F.lit("High"))
        .when(F.col("total_revenue") >= medium_min, F.lit("Medium"))
        .otherwise(F.lit("Low"))
    )
    activity_tier = F.when(F.col("order_count") > 0, F.lit("Active")).otherwise(
        F.lit("Inactive")
    )
    segment_type = (
        F.when(F.col("total_revenue") >= high_min, F.lit("High-Value"))
        .when(F.col("order_count") > 1, F.lit("Repeat"))
        .when(F.col("order_count") == 1, F.lit("One-Time"))
        .otherwise(F.lit("Inactive"))
    )

    return revenue_by_customer.select(
        "customer_id",
        F.col("customer_segment").alias("source_segment"),
        "total_revenue",
        F.col("lifetime_value_actual").alias("lifetime_value"),
        revenue_tier.alias("revenue_tier"),
        activity_tier.alias("activity_tier"),
        segment_type.alias("segment_type"),
        F.concat_ws(
            " | ",
            F.col("customer_segment"),
            revenue_tier,
            activity_tier,
        ).alias("derived_segment"),
        F.lit(processed_at).alias("last_updated"),
    )


def current_processing_timestamp() -> datetime:
    """UTC timestamp stamped on Gold tables."""
    return datetime.now(timezone.utc)
