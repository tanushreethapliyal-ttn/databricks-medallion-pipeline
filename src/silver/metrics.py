"""Build silver.quality_metrics summary rows."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from quality_codes import (
    COMPLETENESS_FAIL,
    CONSISTENCY_FAIL,
    REF_INTEGRITY_FAIL,
    TYPE_VALIDATION_FAIL,
    UNIQUENESS_FAIL,
)


def _is_blank_string(column: str) -> Column:
    return F.col(column).isNull() | (F.trim(F.col(column)) == "")


def _is_null(column: str) -> Column:
    return F.col(column).isNull()


def _has_code_in_flags(flag_column: str, code: str) -> Column:
    """Check a pre-finalize *_flags array column for a failure code."""
    return F.array_contains(F.col(flag_column), code)


CHECK_CONDITIONS: dict[str, list[tuple[str, str, Column]]] = {
    "customers": [
        ("Completeness", "customer_id_not_null", _is_blank_string("_raw_customer_id")),
        ("Completeness", "email_not_null", _is_blank_string("email")),
        ("Uniqueness", "customer_id_unique", _has_code_in_flags("_uniqueness_flags", UNIQUENESS_FAIL)),
        ("Type validation", "customer_types_valid", _has_code_in_flags("_type_flags", TYPE_VALIDATION_FAIL)),
    ],
    "products": [
        ("Completeness", "product_id_not_null", _is_blank_string("_raw_product_id")),
        ("Uniqueness", "product_id_unique", _has_code_in_flags("_uniqueness_flags", UNIQUENESS_FAIL)),
        ("Type validation", "product_types_valid", _has_code_in_flags("_type_flags", TYPE_VALIDATION_FAIL)),
    ],
    "orders": [
        ("Completeness", "customer_id_not_null", F.col("_metric_customer_id_missing")),
        ("Completeness", "product_id_not_null", F.col("_metric_product_id_missing")),
        ("Uniqueness", "order_id_unique", _has_code_in_flags("_uniqueness_flags", UNIQUENESS_FAIL)),
        ("Type validation", "order_types_valid", _has_code_in_flags("_type_flags", TYPE_VALIDATION_FAIL)),
        ("Referential integrity", "customer_id_exists", F.col("_invalid_customer_ref")),
        ("Referential integrity", "product_id_exists", F.col("_invalid_product_ref")),
        ("Business logic", "payment_status_consistency", _has_code_in_flags("_business_flags", CONSISTENCY_FAIL)),
    ],
}


def build_quality_metrics(
    spark: SparkSession,
    run_id: str,
    table_name: str,
    entity: str,
    silver_df: DataFrame,
) -> DataFrame:
    """Create quality metric rows for one Silver table."""
    total_count = silver_df.count()
    measured_at = datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []

    for check_type, check_name, condition in CHECK_CONDITIONS[entity]:
        failed_count = silver_df.filter(condition).count()
        rows.append(
            {
                "run_id": run_id,
                "table_name": table_name,
                "check_type": check_type,
                "check_name": check_name,
                "failed_count": failed_count,
                "total_count": total_count,
                "failure_rate": (failed_count / total_count) if total_count else 0.0,
                "measured_at": measured_at,
            }
        )

    return spark.createDataFrame(rows)


def write_quality_metrics(
    metrics_df: DataFrame, table_name: str, mode: str = "append"
) -> None:
    (
        metrics_df.write.format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )
