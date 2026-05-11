"""Shared paths and config for mtbl_prefect flows and tasks.

Paths default to the host layout (where the dev iteration loop runs) but can
be overridden via env vars for the containerized runner, which mounts the same
trees at different paths inside the container.
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(os.environ.get("MTBL_TOOLS_ROOT", "/Users/Shared/BaseballHQ/tools"))
RESOURCES_ROOT = Path(os.environ.get("MTBL_RESOURCES_ROOT", "/Users/Shared/BaseballHQ/resources"))
EXTRACT_OUTPUT_DIR = RESOURCES_ROOT / "extract"
TRANSFORM_OUTPUT_DIR = RESOURCES_ROOT / "transform"
