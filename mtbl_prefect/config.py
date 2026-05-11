"""Shared paths and config for mtbl_prefect flows and tasks.

Native paths work in both host and container: docker-compose mounts the host
trees at the same paths inside the container, so hardcoded `/Users/Shared/...`
references resolve correctly in either environment.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path("/Users/Shared/BaseballHQ/tools")
RESOURCES_ROOT = Path("/Users/Shared/BaseballHQ/resources")
EXTRACT_OUTPUT_DIR = RESOURCES_ROOT / "extract"
TRANSFORM_OUTPUT_DIR = RESOURCES_ROOT / "transform"
