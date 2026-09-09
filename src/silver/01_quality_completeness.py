# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Completeness Checks
# MAGIC Flags NULL/empty values in required fields (`COMPLETENESS_FAIL`).

# COMMAND ----------

from bootstrap import setup_paths

setup_paths()

# COMMAND ----------

from quality_checks import apply_completeness_checks

# Completeness rules are applied inside create_silver_tables.py / silver_pipeline.py.
# Run the full Silver pipeline notebook to execute all checks end-to-end.

print("Completeness checks are orchestrated by create_silver_tables.py")
