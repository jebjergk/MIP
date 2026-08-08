"""Persist PM V0_1 management ledgers for review-only disposable attempts (local JSON)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[5]
_LEDGER_DIR = _ROOT / "cursorfiles" / "brooks_pm_v01_ledgers"


def ledger_path(simulation_attempt_id: str) -> Path:
    return _LEDGER_DIR / f"{simulation_attempt_id}.json"


def save_management_ledger(simulation_attempt_id: str, payload: dict[str, Any]) -> Path:
    _LEDGER_DIR.mkdir(parents=True, exist_ok=True)
    path = ledger_path(simulation_attempt_id)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def load_management_ledger(simulation_attempt_id: str) -> dict[str, Any] | None:
    path = ledger_path(simulation_attempt_id)
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))
