# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver — Create Silver Tables
# MAGIC Applies completeness, uniqueness, type validation, referential integrity, and business
# MAGIC logic checks. Flags bad rows in `quality_check_result` without deleting them.

# COMMAND ----------

# %pip install pyyaml

# COMMAND ----------

from bootstrap import setup_paths

setup_paths()

# COMMAND ----------

try:
    dbutils.widgets.text("run_id", "", "Pipeline run id (optional)")  # type: ignore[name-defined]
except NameError:
    pass

# COMMAND ----------

from pyspark.sql import SparkSession

from config_loader import find_config_path, load_config
from silver_pipeline import SilverPipelineError, run_silver_pipeline

# COMMAND ----------

def run_all() -> dict[str, int]:
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    try:
        config = load_config()
    except FileNotFoundError as exc:
        raise SilverPipelineError(f"{exc}. Upload config/pipeline_config.yaml and pull in Repos.") from exc

    run_id = config.get("run_id") or None
    print(f"Config loaded from: {find_config_path()}")
    return run_silver_pipeline(spark, config, run_id=run_id)

# COMMAND ----------

counts = run_all()

# COMMAND ----------

# Sanity check — if order metrics still show 100% failures, inspect these outputs.
from pyspark.sql import functions as F

from config_loader import load_config, qualified_table_name

_config = load_config()
_bronze_orders = qualified_table_name(_config, "bronze", "orders")
_silver_orders = qualified_table_name(_config, "silver", "orders")

print("Bronze orders sample (raw strings):")
spark.table(_bronze_orders).select("order_id", "customer_id", "product_id").show(5, truncate=False)

print("Silver orders sample (typed):")
spark.table(_silver_orders).select("order_id", "customer_id", "product_id", "is_quality_pass").show(5, truncate=False)

print("Silver orders non-null customer_id count:")
spark.table(_silver_orders).filter(F.col("customer_id").isNotNull()).agg(F.count(F.lit(1))).show()

counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT check_name, failed_count,
# MAGIC        ROUND(failure_rate * 100, 2) AS failure_pct
# MAGIC FROM silver.quality_metrics
# MAGIC WHERE table_name = 'silver.orders'
# MAGIC   AND measured_at = (
# MAGIC     SELECT MAX(measured_at)
# MAGIC     FROM silver.quality_metrics
# MAGIC     WHERE table_name = 'silver.orders'
# MAGIC   )
# MAGIC ORDER BY check_name;