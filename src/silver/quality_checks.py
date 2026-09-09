"""Silver layer data quality checks."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.window import Window

from quality_codes import (
    COMPLETENESS_FAIL,
    CONSISTENCY_FAIL,
    CUSTOMER_SEGMENTS,
    ORDER_STATUSES,
    REF_INTEGRITY_FAIL,
    TYPE_VALIDATION_FAIL,
    UNIQUENESS_FAIL,
)
from transforms import type_fail


def _is_blank(column: str) -> Column:
    return F.col(column).isNull() | (F.trim(F.col(column)) == "")


def _quality_array(*flag_columns: tuple[Column, str]) -> Column:
    """Build an array of failure codes from (condition, code) pairs."""
    entries = [F.when(condition, F.lit(code)) for condition, code in flag_columns]
    return F.array_compact(F.array(*entries))


def apply_completeness_checks(df: DataFrame, entity: str) -> DataFrame:
    """Flag rows with NULL/empty required fields."""
    if entity == "customers":
        flags = (
            (_is_blank("_raw_customer_id"), COMPLETENESS_FAIL),
            (_is_blank("customer_name"), COMPLETENESS_FAIL),
            (_is_blank("email"), COMPLETENESS_FAIL),
        )
    elif entity == "products":
        flags = (
            (_is_blank("_raw_product_id"), COMPLETENESS_FAIL),
            (_is_blank("product_name"), COMPLETENESS_FAIL),
            (_is_blank("category"), COMPLETENESS_FAIL),
        )
    elif entity == "orders":
        flags = (
            (_is_blank("_raw_order_id"), COMPLETENESS_FAIL),
            (_is_blank("_raw_customer_id"), COMPLETENESS_FAIL),
            (_is_blank("_raw_product_id"), COMPLETENESS_FAIL),
            (_is_blank("_raw_order_date"), COMPLETENESS_FAIL),
            (_is_blank("_raw_quantity"), COMPLETENESS_FAIL),
            (_is_blank("_raw_unit_price"), COMPLETENESS_FAIL),
            (_is_blank("_raw_total_amount"), COMPLETENESS_FAIL),
            (_is_blank("_raw_order_status"), COMPLETENESS_FAIL),
        )
        return (
            df.withColumn("_completeness_flags", _quality_array(*flags))
            .withColumn("_metric_customer_id_missing", _is_blank("_raw_customer_id"))
            .withColumn("_metric_product_id_missing", _is_blank("_raw_product_id"))
        )
    else:
        raise ValueError(f"Unknown entity: {entity}")

    return df.withColumn("_completeness_flags", _quality_array(*flags))


def apply_uniqueness_checks(df: DataFrame, entity: str, key_column: str) -> DataFrame:
    """Flag duplicate primary-key values."""
    window = Window.partitionBy(key_column)
    duplicate = (F.count(F.lit(1)).over(window) > 1) & F.col(key_column).isNotNull()
    return df.withColumn(
        "_uniqueness_flags",
        F.when(duplicate, F.array(F.lit(UNIQUENESS_FAIL))).otherwise(F.array()),
    )


def apply_type_validation_checks(df: DataFrame, entity: str) -> DataFrame:
    """Flag rows where raw values cannot be cast to expected types or enums."""
    type_flags: list[tuple[Column, str]] = []

    if entity == "customers":
        type_flags = [
            (type_fail("_raw_customer_id", "customer_id"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_signup_date", "signup_date"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_lifetime_value", "lifetime_value"), TYPE_VALIDATION_FAIL),
            (
                ~_is_blank("_raw_customer_segment")
                & ~F.trim(F.col("_raw_customer_segment")).isin(*CUSTOMER_SEGMENTS),
                TYPE_VALIDATION_FAIL,
            ),
        ]
    elif entity == "products":
        type_flags = [
            (type_fail("_raw_product_id", "product_id"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_price", "price"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_cost", "cost"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_stock_quantity", "stock_quantity"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_reorder_level", "reorder_level"), TYPE_VALIDATION_FAIL),
        ]
    elif entity == "orders":
        type_flags = [
            (type_fail("_raw_order_id", "order_id"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_customer_id", "customer_id"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_order_date", "order_date"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_product_id", "product_id"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_quantity", "quantity"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_unit_price", "unit_price"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_total_amount", "total_amount"), TYPE_VALIDATION_FAIL),
            (type_fail("_raw_payment_date", "payment_date"), TYPE_VALIDATION_FAIL),
            (
                ~_is_blank("_raw_order_status")
                & ~F.trim(F.col("_raw_order_status")).isin(*ORDER_STATUSES),
                TYPE_VALIDATION_FAIL,
            ),
        ]
    else:
        raise ValueError(f"Unknown entity: {entity}")

    return df.withColumn("_type_flags", _quality_array(*type_flags))


def apply_referential_integrity_checks(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """Flag orders with customer_id/product_id not present in master tables."""
    valid_customers = (
        customers_df.select(F.col("customer_id").alias("_valid_customer_id"))
        .where(F.col("_valid_customer_id").isNotNull())
        .distinct()
    )
    valid_products = (
        products_df.select(F.col("product_id").alias("_valid_product_id"))
        .where(F.col("_valid_product_id").isNotNull())
        .distinct()
    )

    with_customers = orders_df.join(
        valid_customers,
        F.col("customer_id") == F.col("_valid_customer_id"),
        "left",
    )
    with_refs = with_customers.join(
        valid_products,
        F.col("product_id") == F.col("_valid_product_id"),
        "left",
    )

    invalid_customer = (
        F.col("customer_id").isNotNull() & F.col("_valid_customer_id").isNull()
    )
    invalid_product = F.col("product_id").isNotNull() & F.col("_valid_product_id").isNull()

    return (
        with_refs.withColumn("_invalid_customer_ref", invalid_customer)
        .withColumn("_invalid_product_ref", invalid_product)
        .withColumn(
            "_ref_integrity_flags",
            _quality_array(
                (invalid_customer, REF_INTEGRITY_FAIL),
                (invalid_product, REF_INTEGRITY_FAIL),
            ),
        )
        .drop("_valid_customer_id", "_valid_product_id")
    )


def apply_business_logic_checks(df: DataFrame, entity: str) -> DataFrame:
    """Optional business consistency rules."""
    if entity != "orders":
        return df.withColumn("_business_flags", F.array().cast("array<string>"))

    completed_without_payment = (F.col("order_status") == "Completed") & F.col(
        "payment_date"
    ).isNull()
    cancelled_with_payment = (F.col("order_status") == "Cancelled") & F.col(
        "payment_date"
    ).isNotNull()

    return df.withColumn(
        "_business_flags",
        _quality_array(
            (completed_without_payment, CONSISTENCY_FAIL),
            (cancelled_with_payment, CONSISTENCY_FAIL),
        ),
    )


def finalize_quality_columns(df: DataFrame) -> DataFrame:
    """Combine flag arrays into quality_check_result and is_quality_pass."""
    flag_cols = [c for c in df.columns if c.endswith("_flags")]
    combined = F.concat(*[F.coalesce(F.col(c), F.array()) for c in flag_cols])
    quality_check_result = F.array_distinct(combined)

    result = (
        df.withColumn("quality_check_result", quality_check_result)
        .withColumn("is_quality_pass", F.size(F.col("quality_check_result")) == 0)
        .withColumn("silver_processed_at", F.current_timestamp())
    )

    drop_cols = [
        c
        for c in result.columns
        if c.startswith("_raw_")
        or c.endswith("_flags")
        or c.startswith("_invalid_")
        or c.startswith("_metric_")
    ]
    return result.drop(*drop_cols)


def read_bronze_table(spark: SparkSession, table_name: str) -> DataFrame:
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Bronze table not found: {table_name}")
    return spark.table(table_name)
