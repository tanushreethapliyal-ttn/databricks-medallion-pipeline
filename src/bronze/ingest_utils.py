"""
Shared Bronze ingestion utilities for Databricks PySpark pipelines.

Reads raw CSVs with explicit STRING schemas, adds metadata columns, writes Delta
tables, and appends ingestion audit rows. No business transformations.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from config_loader import load_config, qualified_table_name, source_csv_path
from schemas import INGESTION_LOG_SCHEMA


class BronzeIngestionError(Exception):
    """Raised when Bronze ingestion validation or load fails."""


def get_spark() -> SparkSession:
    """Return active Spark session (Databricks) or create one for local smoke tests."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return SparkSession.builder.appName("bronze-ingestion").getOrCreate()


def ensure_schemas_exist(spark: SparkSession, config: dict[str, Any]) -> None:
    """Create bronze/silver/gold schemas if they do not exist."""
    catalog = (config.get("catalog") or "").strip()
    for schema_name in (
        config["bronze_schema"],
        config["silver_schema"],
        config["gold_schema"],
    ):
        if catalog:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
        else:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def validate_source_path(source_path: str) -> None:
    """
    Validate that the source CSV path is reachable before ingestion.

    Uses dbutils when available; otherwise verifies via Spark read.
    """
    filename = source_path.rsplit("/", 1)[-1]
    parent = source_path.rsplit("/", 1)[0]

    try:
        entries = dbutils.fs.ls(parent)  # type: ignore[name-defined]
        names = {row.name.rstrip("/") for row in entries}
        if filename not in names:
            raise BronzeIngestionError(
                f"Source file not found: {source_path} (available: {sorted(names)})"
            )
        return
    except NameError:
        pass
    except BronzeIngestionError:
        raise
    except Exception:
        pass

    spark = get_spark()
    try:
        spark.read.text(source_path).limit(1).collect()
    except Exception as exc:
        raise BronzeIngestionError(f"Source file not readable: {source_path}") from exc


def read_raw_csv(spark: SparkSession, source_path: str, schema: StructType) -> DataFrame:
    """Read CSV with explicit schema; preserve raw values (no business transforms)."""
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .schema(schema)
        .csv(source_path)
    )


def add_metadata_columns(df: DataFrame, source_path: str, run_id: str) -> DataFrame:
    """Add Bronze technical metadata columns."""
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_path))
        .withColumn("_run_id", F.lit(run_id))
    )


def write_bronze_delta(df: DataFrame, table_name: str) -> None:
    """Overwrite Bronze entity Delta table for idempotent full reload."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def build_log_row(
    run_id: str,
    source_path: str,
    target_table: str,
    row_count: int,
    status: str,
    error_message: str | None = None,
) -> dict[str, Any]:
    return {
        "log_id": str(uuid.uuid4()),
        "run_id": run_id,
        "source_path": source_path,
        "target_table": target_table,
        "row_count": int(row_count),
        "ingestion_timestamp": datetime.now(timezone.utc),
        "status": status,
        "error_message": error_message,
    }


def append_ingestion_log(spark: SparkSession, log_row: dict[str, Any], table_name: str) -> None:
    """Append one ingestion audit row to bronze.ingestion_log."""
    log_df = spark.createDataFrame([log_row], schema=INGESTION_LOG_SCHEMA)
    (
        log_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )


def ingest_entity(
    spark: SparkSession,
    config: dict[str, Any],
    entity: str,
    schema: StructType,
    run_id: str,
) -> int:
    """
    Ingest one CSV entity into its Bronze Delta table.

    Returns row count on success. Raises BronzeIngestionError on failure.
    """
    source_path = source_csv_path(config, entity)
    target_table = qualified_table_name(config, "bronze", entity)
    log_table = qualified_table_name(config, "bronze", "ingestion_log")

    validate_source_path(source_path)

    try:
        raw_df = read_raw_csv(spark, source_path, schema)
        bronze_df = add_metadata_columns(raw_df, source_path, run_id)
        row_count = bronze_df.count()
        write_bronze_delta(bronze_df, target_table)
        append_ingestion_log(
            spark,
            build_log_row(run_id, source_path, target_table, row_count, "SUCCESS"),
            log_table,
        )
        return row_count
    except Exception as exc:
        append_ingestion_log(
            spark,
            build_log_row(
                run_id,
                source_path,
                target_table,
                0,
                "FAILED",
                error_message=str(exc),
            ),
            log_table,
        )
        raise BronzeIngestionError(
            f"Bronze ingestion failed for {entity}: {exc}"
        ) from exc


def new_run_id(config: dict[str, Any]) -> str:
    """Use configured run_id or generate a new UUID."""
    configured = config.get("run_id")
    if configured:
        return str(configured)
    return str(uuid.uuid4())
