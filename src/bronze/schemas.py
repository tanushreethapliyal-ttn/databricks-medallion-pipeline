"""Explicit Bronze-layer Spark schemas (business columns as STRING for raw fidelity)."""

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CUSTOMERS_BRONZE_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("lifetime_value", StringType(), True),
    ]
)

ORDERS_BRONZE_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("payment_date", StringType(), True),
    ]
)

PRODUCTS_BRONZE_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", StringType(), True),
        StructField("cost", StringType(), True),
        StructField("stock_quantity", StringType(), True),
        StructField("reorder_level", StringType(), True),
    ]
)

INGESTION_LOG_SCHEMA = StructType(
    [
        StructField("log_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("source_path", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("row_count", LongType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)
