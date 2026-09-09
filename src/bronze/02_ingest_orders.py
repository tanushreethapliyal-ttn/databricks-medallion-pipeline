# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Ingest Orders
# MAGIC Raw ingest of `orders.csv` into `bronze.orders` (no transformations).

# COMMAND ----------

import os
import sys

try:
    BRONZE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # type: ignore[name-defined]
    workspace_nb = f"/Workspace{notebook_path}" if notebook_path.startswith(("/Repos/", "/Users/")) else notebook_path
    BRONZE_DIR = os.path.dirname(workspace_nb)

if BRONZE_DIR not in sys.path:
    sys.path.insert(0, BRONZE_DIR)

# COMMAND ----------

from config_loader import load_config
from ingest_utils import get_spark, ingest_entity, new_run_id
from schemas import ORDERS_BRONZE_SCHEMA

# COMMAND ----------

def run(run_id: str | None = None) -> int:
    spark = get_spark()
    config = load_config()
    if run_id:
        config["run_id"] = run_id
    effective_run_id = new_run_id(config)
    row_count = ingest_entity(
        spark, config, "orders", ORDERS_BRONZE_SCHEMA, effective_run_id
    )
    print(f"SUCCESS: bronze.orders loaded with {row_count} rows (run_id={effective_run_id})")
    return row_count

# COMMAND ----------

if __name__ == "__main__":
    run()
