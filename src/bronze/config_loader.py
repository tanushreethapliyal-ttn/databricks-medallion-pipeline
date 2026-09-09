"""Load pipeline configuration from YAML with optional Databricks widget overrides."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

def find_config_path() -> Path:
    """Locate pipeline_config.yaml from module path or parent directories."""
    candidates: list[Path] = []
    try:
        # src/bronze/config_loader.py -> repo root is parents[2]
        candidates.append(
            Path(__file__).resolve().parents[2] / "config" / "pipeline_config.yaml"
        )
    except NameError:
        pass

    for parent in [Path.cwd(), *Path.cwd().parents]:
        candidates.append(parent / "config" / "pipeline_config.yaml")

    for candidate in candidates:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "pipeline_config.yaml not found. Expected at config/pipeline_config.yaml "
        "relative to the repository root."
    )


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:
        raise ImportError(
            "PyYAML is required to load pipeline_config.yaml. "
            "Install with: pip install pyyaml"
        ) from exc

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid config file: {path}")
    return data


def _widget_override(key: str, default: str | None = None) -> str | None:
    """Read a Databricks widget value when running in a notebook."""
    try:
        # dbutils is injected in Databricks runtime only.
        value = dbutils.widgets.get(key)  # type: ignore[name-defined]
        if value is not None and str(value).strip():
            return str(value).strip()
    except NameError:
        pass
    env_value = os.environ.get(key.upper())
    return env_value if env_value else default


def normalize_path(path: str) -> str:
    """Normalize workspace / local paths for Spark CSV reads."""
    cleaned = path.strip().rstrip("/")
    if cleaned.startswith("dbfs:") or cleaned.startswith("file:"):
        return cleaned
    if cleaned.startswith("/Workspace/") or cleaned.startswith("/Repos/"):
        return f"file:{cleaned}"
    return cleaned


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load config and apply optional runtime overrides."""
    path = Path(config_path) if config_path else find_config_path()
    if not path.exists():
        raise FileNotFoundError(f"Pipeline config not found: {path}")

    config = _load_yaml(path)

    landing_override = _widget_override("landing_path")
    if landing_override:
        config["landing_path"] = landing_override

    run_id_override = _widget_override("run_id")
    if run_id_override:
        config["run_id"] = run_id_override

    config["landing_path"] = normalize_path(config["landing_path"])
    return config


def qualified_table_name(config: dict[str, Any], schema_key: str, table_key: str) -> str:
    """Build catalog.schema.table or schema.table."""
    catalog = (config.get("catalog") or "").strip()
    schema = config[f"{schema_key}_schema"]
    table = config[f"{schema_key}_tables"][table_key]
    if catalog:
        return f"{catalog}.{schema}.{table}"
    return f"{schema}.{table}"


def source_csv_path(config: dict[str, Any], entity: str) -> str:
    """Full path to a source CSV file."""
    filename = config["source_files"][entity]
    base = config["landing_path"].rstrip("/")
    if base.startswith("file:"):
        return f"{base}/{filename}"
    return normalize_path(f"{base}/{filename}")
