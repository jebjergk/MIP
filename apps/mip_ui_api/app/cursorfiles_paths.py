"""
Resolve workspace paths for subprocess helpers (IB ingest, query_snowflake, etc.).

The UX API may run on Windows or Linux/WSL/Mac (e.g. ./start_api.sh). The agent venv
lives at <workspace>/cursorfiles/.venv with platform-specific interpreter paths.
"""
from __future__ import annotations

import sys
from pathlib import Path

# This file is at MIP/apps/mip_ui_api/app/cursorfiles_paths.py
_WORKSPACE_ROOT = Path(__file__).resolve().parents[4]


def mip_workspace_root() -> Path:
    """Repository root containing ``cursorfiles/`` and ``MIP/``."""
    return _WORKSPACE_ROOT


def cursorfiles_venv_python(workspace_root: Path | None = None) -> Path:
    """
    Interpreter inside ``cursorfiles/.venv`` (snowflake + ib_insync stack).

    Windows: .venv/Scripts/python.exe
    POSIX:   .venv/bin/python3 or .venv/bin/python
    """
    root = workspace_root or _WORKSPACE_ROOT
    venv = root / "cursorfiles" / ".venv"
    if sys.platform == "win32":
        return venv / "Scripts" / "python.exe"
    p3 = venv / "bin" / "python3"
    if p3.exists():
        return p3
    return venv / "bin" / "python"
