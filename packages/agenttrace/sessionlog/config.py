"""Configuration: paths and file constants for ingestion."""

import os
from pathlib import Path

# Paths
CLAUDE_PROJECTS_DIR = Path.home() / ".claude" / "projects"
DB_PATH = Path(
    os.environ.get("CLAUDE_RETRO_DB", Path.home() / ".claude" / "retro.sqlite")
)
