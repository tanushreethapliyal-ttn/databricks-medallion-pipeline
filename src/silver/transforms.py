"""Cast Bronze STRING columns to typed Silver columns."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column


def _as_string(column: str) -> Column:
    return F.col(column).cast("string")


def _is_blank_string(column: str) -> Column:
    return F.col(column).isNull() | (F.trim(F.col(column)) == "")


def _typed_int_from_raw(raw_column: str) -> Column:
    """Cast raw string IDs to INT, tolerating pandas float strings like '1640.0'."""
    integer_digits = F.regexp_extract(F.trim(F.col(raw_column)), r"^(-?\d+)", 1)
    return F.when(_is_blank_string(raw_column), None).otherwise(
        F.when(integer_digits == "", None).otherwise(integer_digits.cast("int"))
    )


def _typed_decimal_from_raw(raw_column: str) -> Column:
    return F.when(_is_blank_string(raw_column), None).otherwise(
        F.expr(f"try_cast(`{raw_column}` AS DECIMAL(18,2))")
    )


def _typed_date_from_raw(raw_column: str) -> Column:
    return F.when(_is_blank_string(raw_column), None).otherwise(
        F.expr(f"try_cast(`{raw_column}` AS DATE)")
    )


def type_fail(raw_column: str, typed_column: str) -> Column:
    """True when a non-empty raw value could not be cast."""
    return (
        ~_is_blank_string(raw_column)
        & F.col(typed_column).isNull()
    )


def _with_raw_string_columns(bronze_df: DataFrame, columns: list[str]) -> DataFrame:
    """Create explicit _raw_* string columns from Bronze source columns."""
    selections = [_as_string(column).alias(f"_raw_{column}") for column in columns]
    metadata = [
        F.col("_ingestion_timestamp"),
        F.col("_source_file"),
        F.col("_run_id"),
    ]
    return bronze_df.select(*selections, *metadata)


def cast_customers(bronze_df: DataFrame) -> DataFrame:
    """Cast bronze.customers business columns to Silver types."""
    raw = _with_raw_string_columns(
        bronze_df,
        [
            "customer_id",
            "customer_name",
            "email",
            "country",
            "signup_date",
            "customer_segment",
            "lifetime_value",
        ],
    )
    return raw.select(
        _typed_int_from_raw("_raw_customer_id").alias("customer_id"),
        F.col("_raw_customer_name").alias("customer_name"),
        F.col("_raw_email").alias("email"),
        F.col("_raw_country").alias("country"),
        _typed_date_from_raw("_raw_signup_date").alias("signup_date"),
        F.col("_raw_customer_segment").alias("customer_segment"),
        _typed_decimal_from_raw("_raw_lifetime_value").alias("lifetime_value"),
        F.col("_ingestion_timestamp"),
        F.col("_source_file"),
        F.col("_run_id"),
        F.col("_raw_customer_id"),
        F.col("_raw_signup_date"),
        F.col("_raw_lifetime_value"),
        F.col("_raw_customer_segment"),
    )


def cast_products(bronze_df: DataFrame) -> DataFrame:
    """Cast bronze.products business columns to Silver types."""
    raw = _with_raw_string_columns(
        bronze_df,
        [
            "product_id",
            "product_name",
            "category",
            "price",
            "cost",
            "stock_quantity",
            "reorder_level",
        ],
    )
    return raw.select(
        _typed_int_from_raw("_raw_product_id").alias("product_id"),
        F.col("_raw_product_name").alias("product_name"),
        F.col("_raw_category").alias("category"),
        _typed_decimal_from_raw("_raw_price").alias("price"),
        _typed_decimal_from_raw("_raw_cost").alias("cost"),
        _typed_int_from_raw("_raw_stock_quantity").alias("stock_quantity"),
        _typed_int_from_raw("_raw_reorder_level").alias("reorder_level"),
        F.col("_ingestion_timestamp"),
        F.col("_source_file"),
        F.col("_run_id"),
        F.col("_raw_product_id"),
        F.col("_raw_price"),
        F.col("_raw_cost"),
        F.col("_raw_stock_quantity"),
        F.col("_raw_reorder_level"),
    )


def cast_orders(bronze_df: DataFrame) -> DataFrame:
    """Cast bronze.orders business columns to Silver types."""
    raw = _with_raw_string_columns(
        bronze_df,
        [
            "order_id",
            "customer_id",
            "order_date",
            "product_id",
            "quantity",
            "unit_price",
            "total_amount",
            "order_status",
            "payment_date",
        ],
    )
    return raw.select(
        _typed_int_from_raw("_raw_order_id").alias("order_id"),
        _typed_int_from_raw("_raw_customer_id").alias("customer_id"),
        _typed_date_from_raw("_raw_order_date").alias("order_date"),
        _typed_int_from_raw("_raw_product_id").alias("product_id"),
        _typed_int_from_raw("_raw_quantity").alias("quantity"),
        _typed_decimal_from_raw("_raw_unit_price").alias("unit_price"),
        _typed_decimal_from_raw("_raw_total_amount").alias("total_amount"),
        F.col("_raw_order_status").alias("order_status"),
        _typed_date_from_raw("_raw_payment_date").alias("payment_date"),
        F.col("_ingestion_timestamp"),
        F.col("_source_file"),
        F.col("_run_id"),
        F.col("_raw_order_id"),
        F.col("_raw_customer_id"),
        F.col("_raw_order_date"),
        F.col("_raw_product_id"),
        F.col("_raw_quantity"),
        F.col("_raw_unit_price"),
        F.col("_raw_total_amount"),
        F.col("_raw_order_status"),
        F.col("_raw_payment_date"),
    )
