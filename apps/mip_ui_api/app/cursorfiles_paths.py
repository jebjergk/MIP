"""
Resolve workspace paths for subprocess helpers (IB ingest, query_snowflake, etc.).

The API may run from Anaconda, Windows, or Git Bash; the agent stack may live in
``cursorfiles/.venv`` or the same interpreter as uvicorn. We locate the repo root by
walking parents for ``cursorfiles/run_ib_manual_daily_job.py`` so ``parents[N]`` is
never wrong if the tree depth changes.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_MARKER = ("cursorfiles", "run_ib_manual_daily_job.py")


def find_mip_workspace_root() -> Path:
    """Directory that contains ``cursorfiles/run_ib_manual_daily_job.py`` (repo root)."""
    here = Path(__file__).resolve().parent
    for d in [here, *here.parents]:
        if (d / _MARKER[0] / _MARKER[1]).is_file():
            return d
    # Fallback: mip_ui_api/app -> parents[4] = repo root in standard MIP layout
    return Path(__file__).resolve().parents[4]


def mip_workspace_root() -> Path:
    """Alias for :func:`find_mip_workspace_root`."""
    return find_mip_workspace_root()


def cursorfiles_venv_python(workspace_root: Path | None = None) -> Path:
    """
    Expected interpreter inside ``cursorfiles/.venv`` (if present).

    Windows: ``.venv/Scripts/python.exe``
    POSIX: ``.venv/bin/python3`` or ``.venv/bin/python``
    """
    root = workspace_root or find_mip_workspace_root()
    venv = root / "cursorfiles" / ".venv"
    if sys.platform == "win32":
        return venv / "Scripts" / "python.exe"
    p3 = venv / "bin" / "python3"
    if p3.is_file():
        return p3
    return venv / "bin" / "python"


def resolve_subprocess_python(workspace_root: Path | None = None) -> Path:
    """
    Python to run ``ingest_ibkr_bars.py`` / ``query_snowflake.py``.

    Resolution order:

    1. ``MIP_SUBPROCESS_PYTHON`` or ``CURSORFILES_PYTHON`` (absolute path to python)
    2. ``sys.executable`` — same environment as uvicorn (often Anaconda). This avoids
       picking a stale or Cursor-only ``cursorfiles/.venv`` when the API already runs
       a full conda env with ``ib_insync`` / Snowflake installed.
    3. ``cursorfiles/.venv`` if present (fallback for installs where subprocess deps
       live only in that venv)
    """
    root = workspace_root or find_mip_workspace_root()
    for key in ("MIP_SUBPROCESS_PYTHON", "CURSORFILES_PYTHON"):
        raw = (os.environ.get(key) or "").strip()
        if not raw:
            continue
        p = Path(raw).expanduser()
        if p.is_file():
            return p
    exe = Path(sys.executable)
    if exe.is_file():
        return exe
    v = cursorfiles_venv_python(root)
    if v.is_file():
        return v
    return exe
