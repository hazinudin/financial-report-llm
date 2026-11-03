"""Test path setup shared by pipeline and chat tests.

The repo intentionally has no installed package: pipeline modules import their
siblings by bare name, and chat tests import `chat_app` with `PYTHONPATH=chat-app`.
This conftest makes both work under a plain `uv run pytest`.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

for path in (
    ROOT / "chat-app",
    ROOT / "scripts" / "analysis_pipeline",
    ROOT / "scripts" / "report_pipeline",
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
