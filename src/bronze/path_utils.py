"""Path helpers for Databricks notebook and repo execution."""

from __future__ import annotations

import os
from pathlib import Path


def _notebook_workspace_dir() -> str | None:
    """Resolve the notebook's directory on the Databricks workspace filesystem."""
    try:
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    except Exception:
        return None

    if notebook_path.startswith("/Workspace/"):
        workspace_nb = notebook_path
    elif notebook_path.startswith(("/Repos/", "/Users/")):
        workspace_nb = f"/Workspace{notebook_path}"
    else:
        workspace_nb = f"/Workspace/Repos/{notebook_path.lstrip('/')}"

    return os.path.dirname(workspace_nb)


def get_bronze_dir() -> str:
    """
    Resolve src/bronze when running as a script or Databricks notebook.
    """
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        pass

    notebook_dir = _notebook_workspace_dir()
    if notebook_dir and os.path.isdir(notebook_dir):
        return notebook_dir

    cwd = Path.cwd()
    if (cwd / "ingest_utils.py").exists():
        return str(cwd)
    if (cwd / "src" / "bronze" / "ingest_utils.py").exists():
        return str(cwd / "src" / "bronze")

    raise RuntimeError(
        "Could not resolve Bronze directory. Run this notebook from "
        "src/bronze/ingest_all in Databricks Repos."
    )


def bootstrap_bronze_path() -> str:
    """Add src/bronze to sys.path and return the resolved directory."""
    import sys

    bronze_dir = get_bronze_dir()
    if bronze_dir not in sys.path:
        sys.path.insert(0, bronze_dir)
    return bronze_dir
