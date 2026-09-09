# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Business Logic Checks
# MAGIC Flags payment/status inconsistencies on orders (`CONSISTENCY_FAIL`).

# COMMAND ----------

from bootstrap import setup_paths

setup_paths()

# COMMAND ----------

from quality_checks import apply_business_logic_checks

print("Business logic checks are orchestrated by create_silver_tables.py")
