# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Referential Integrity Checks
# MAGIC Flags orphan foreign keys on orders (`REF_INTEGRITY_FAIL`).

# COMMAND ----------

from bootstrap import setup_paths

setup_paths()

# COMMAND ----------

from quality_checks import apply_referential_integrity_checks

print("Referential integrity checks are orchestrated by create_silver_tables.py")
