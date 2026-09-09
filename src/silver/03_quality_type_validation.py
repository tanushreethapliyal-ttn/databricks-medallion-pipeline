# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Type Validation Checks
# MAGIC Flags invalid casts and enum values (`TYPE_VALIDATION_FAIL`).

# COMMAND ----------

from bootstrap import setup_paths

setup_paths()

# COMMAND ----------

from quality_checks import apply_type_validation_checks

print("Type validation checks are orchestrated by create_silver_tables.py")
