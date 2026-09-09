# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold — Create Gold Tables
# MAGIC Builds business aggregates from quality-pass Silver data.
# MAGIC Revenue metrics use `order_status = 'Completed'` only.

# COMMAND ----------

# %pip install pyyaml

# COMMAND ----------

from bootstrap import setup_paths

setup_paths()

# COMMAND ----------

from pyspark.sql import SparkSession

from config_loader import find_config_path, load_config
from gold_pipeline import GoldPipelineError, run_gold_pipeline

# COMMAND ----------

def run_all() -> dict[str, int]:
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    try:
        config = load_config()
    except FileNotFoundError as exc:
        raise GoldPipelineError(f"{exc}. Upload config/pipeline_config.yaml and pull in Repos.") from exc

    print(f"Config loaded from: {find_config_path()}")
    return run_gold_pipeline(spark, config)

# COMMAND ----------

counts = run_all()

# COMMAND ----------

# Quick preview of Gold outputs
from config_loader import qualified_table_name

_config = load_config()

for _entity in ("sales_by_product", "revenue_by_customer", "customer_segmentation"):
    _table = qualified_table_name(_config, "gold", _entity)
    print(f"Sample: {_table}")
    spark.table(_table).show(5, truncate=False)

counts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top products by revenue (dashboard preview)
# MAGIC SELECT product_name, total_revenue, order_count
# MAGIC FROM gold.sales_by_product
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;
# MAGIC