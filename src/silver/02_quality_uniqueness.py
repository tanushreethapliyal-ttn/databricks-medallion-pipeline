# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Uniqueness Checks
# MAGIC Flags duplicate primary keys (`UNIQUENESS_FAIL`).

# COMMAND ----------

from bootstrap import setup_paths

setup_paths()

# COMMAND ----------

from quality_checks import apply_uniqueness_checks

print("Uniqueness checks are orchestrated by create_silver_tables.py")
