# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze — Ingest All Sources
# MAGIC Orchestrates raw ingestion for customers, orders, and products into Delta Bronze tables.
# MAGIC
# MAGIC **Optional widgets**
# MAGIC - `landing_path` — override CSV directory (workspace path)
# MAGIC - `run_id` — optional pipeline run identifier

# COMMAND ----------

# Optional: install PyYAML on cluster if not present (Community Edition).
# %pip install pyyaml

# COMMAND ----------

import os
import sys


def _resolve_bronze_dir() -> str:
    """Resolve src/bronze without importing local modules (Databricks has no __file__)."""
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        pass

    try:
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        if notebook_path.startswith("/Workspace/"):
            workspace_nb = notebook_path
        elif notebook_path.startswith(("/Repos/", "/Users/")):
            workspace_nb = f"/Workspace{notebook_path}"
        else:
            workspace_nb = f"/Workspace/Repos/{notebook_path.lstrip('/')}"
        bronze_dir = os.path.dirname(workspace_nb)
        print(f"Resolved notebook path: {notebook_path}")
        print(f"Resolved bronze dir: {bronze_dir}")
        return bronze_dir
    except Exception as exc:
        raise RuntimeError(
            "Could not resolve Bronze directory. Open this notebook from "
            "src/bronze/ingest_all in Databricks Repos."
        ) from exc


BRONZE_DIR = _resolve_bronze_dir()
if BRONZE_DIR not in sys.path:
    sys.path.insert(0, BRONZE_DIR)

# COMMAND ----------

try:
    dbutils.widgets.text(  # type: ignore[name-defined]
        "landing_path",
        "/Workspace/Users/tanushreethapliyal3@gmail.com/databricks-medallion-pipeline/databricks-medallion-pipeline/Data/raw",
        "CSV landing path",
    )
    dbutils.widgets.text("run_id", "", "Pipeline run id (optional)")  # type: ignore[name-defined]
except NameError:
    pass

# COMMAND ----------

from config_loader import find_config_path, load_config
from ingest_utils import BronzeIngestionError, ensure_schemas_exist, get_spark, ingest_entity, new_run_id
from schemas import CUSTOMERS_BRONZE_SCHEMA, ORDERS_BRONZE_SCHEMA, PRODUCTS_BRONZE_SCHEMA

INGEST_ENTITIES = (
    ("customers", CUSTOMERS_BRONZE_SCHEMA),
    ("orders", ORDERS_BRONZE_SCHEMA),
    ("products", PRODUCTS_BRONZE_SCHEMA),
)

# COMMAND ----------

def run_all() -> dict[str, int]:
    spark = get_spark()
    try:
        config = load_config()
    except FileNotFoundError as exc:
        raise BronzeIngestionError(
            f"{exc}. Upload config/pipeline_config.yaml to the repo root and pull in Databricks."
        ) from exc
    except ImportError as exc:
        raise BronzeIngestionError(
            f"{exc}. Run '%pip install pyyaml' in the notebook, then retry."
        ) from exc

    ensure_schemas_exist(spark, config)
    run_id = new_run_id(config)

    print(f"Starting Bronze ingestion (run_id={run_id})")
    print(f"Landing path: {config['landing_path']}")
    print(f"Config loaded from: {find_config_path()}")

    results: dict[str, int] = {}
    failures: list[str] = []

    for entity, schema in INGEST_ENTITIES:
        try:
            results[entity] = ingest_entity(spark, config, entity, schema, run_id)
            print(f"SUCCESS: bronze.{entity} loaded with {results[entity]} rows")
        except BronzeIngestionError as exc:
            failures.append(f"{entity}: {exc}")
            print(f"FAILED: {entity} — {exc}")

    if failures:
        raise BronzeIngestionError(
            "Bronze ingestion completed with failures:\n  - " + "\n  - ".join(failures)
        )

    print("Bronze ingestion completed successfully:")
    for entity, count in results.items():
        print(f"  {entity}: {count} rows")

    return results

# COMMAND ----------

run_all()