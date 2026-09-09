"""Resolve Databricks workspace CSV paths to Spark-readable locations."""

from __future__ import annotations

from pyspark.sql import SparkSession


class PathResolutionError(Exception):
    """Raised when a source CSV path cannot be resolved for Spark."""


def _path_variants(path: str) -> list[str]:
    """Build likely path variants for workspace, file, and DBFS locations."""
    cleaned = path.strip()
    without_file = cleaned.removeprefix("file:")

    variants: list[str] = []
    for candidate in (cleaned, without_file, f"file:{without_file}"):
        if candidate and candidate not in variants:
            variants.append(candidate)
    return variants


def _spark_can_read(spark: SparkSession, path: str) -> bool:
    try:
        spark.read.text(path).limit(1).collect()
        return True
    except Exception:
        return False


def _copy_workspace_file_to_dbfs(workspace_file_path: str) -> str:
    """
    Copy a workspace file to DBFS FileStore so Spark executors can read it.

    Databricks Community Edition often cannot distribute-read CSVs directly from
    /Workspace/Users/... paths across the cluster.
    """
    clean = workspace_file_path.removeprefix("file:")
    filename = clean.rsplit("/", 1)[-1]
    dest_dir = "dbfs:/FileStore/medallion_pipeline/raw"
    dest_path = f"{dest_dir}/{filename}"

    dbutils.fs.mkdirs(dest_dir)  # type: ignore[name-defined]
    try:
        dbutils.fs.rm(dest_path)  # type: ignore[name-defined]
    except Exception:
        pass
    dbutils.fs.cp(f"file:{clean}", dest_path)  # type: ignore[name-defined]
    return dest_path


def resolve_spark_csv_path(spark: SparkSession, source_path: str) -> str:
    """
    Return a CSV path that Spark can read in Databricks.

    Tries direct workspace/file paths first, then copies to DBFS FileStore.
    """
    for candidate in _path_variants(source_path):
        if _spark_can_read(spark, candidate):
            return candidate

    clean = source_path.removeprefix("file:")
    if clean.startswith("/Workspace/"):
        try:
            dest = _copy_workspace_file_to_dbfs(clean)
            if _spark_can_read(spark, dest):
                print(f"Copied CSV to DBFS for Spark read: {dest}")
                return dest
        except NameError:
            pass
        except Exception as exc:
            raise PathResolutionError(
                f"Unable to copy workspace CSV to DBFS: {clean}"
            ) from exc

    raise PathResolutionError(
        f"Source file not readable by Spark. Tried: {_path_variants(source_path)}. "
        "Confirm CSV exists and landing_path widget points to Data/raw."
    )


def validate_source_exists(source_path: str) -> None:
    """Validate source file exists using dbutils or Spark."""
    filename = source_path.rsplit("/", 1)[-1]
    parent = source_path.rsplit("/", 1)[0]
    clean_parent = parent.removeprefix("file:")

    try:
        for parent_candidate in _path_variants(parent):
            try:
                entries = dbutils.fs.ls(parent_candidate)  # type: ignore[name-defined]
                names = {row.name.rstrip("/") for row in entries}
                if filename in names:
                    return
            except Exception:
                continue
    except NameError:
        pass

    spark = SparkSession.getActiveSession()
    if spark is not None:
        for file_candidate in _path_variants(source_path):
            if _spark_can_read(spark, file_candidate):
                return

    raise PathResolutionError(
        f"Source file not found: {source_path}. "
        f"Check landing_path and filenames (customers.csv, orders.csv, products.csv)."
    )
