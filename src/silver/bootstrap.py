"""Bootstrap sys.path for Silver notebooks on Databricks."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def resolve_silver_dir() -> str:
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        pass

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
    return os.path.dirname(workspace_nb)


def setup_paths() -> tuple[str, str]:
    """Add silver and bronze directories to sys.path; return (silver_dir, bronze_dir)."""
    silver_dir = resolve_silver_dir()
    repo_root = str(Path(silver_dir).resolve().parents[1])
    bronze_dir = os.path.join(repo_root, "src", "bronze")

    for path in (silver_dir, bronze_dir):
        if path not in sys.path:
            sys.path.insert(0, path)

    return silver_dir, bronze_dir
