"""Shared paths and config for mtbl_prefect flows and tasks."""

from pathlib import Path

REPO_ROOT = Path("/Users/Shared/BaseballHQ/tools")
RESOURCES_ROOT = Path("/Users/Shared/BaseballHQ/resources")
EXTRACT_OUTPUT_DIR = RESOURCES_ROOT / "extract"
TRANSFORM_OUTPUT_DIR = RESOURCES_ROOT / "transform"
