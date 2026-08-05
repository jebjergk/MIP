"""Canonical dossier record access for learning/review serializers."""

from __future__ import annotations

import json
from typing import Any


def unwrap_dossier(row: dict[str, Any] | None) -> dict[str, Any]:
    """Return the analytical dossier body from either a flat or wrapper record.

    Supports:
      {"paa_verdict": "...", ...}
      {"symbol": "...", "trading_date": "...", "frozen_dossier_json": {...}}
    """
    if not row or not isinstance(row, dict):
        return {}
    nested = row.get("frozen_dossier_json")
    if isinstance(nested, dict):
        return nested
    if isinstance(nested, str) and nested.strip():
        try:
            parsed = json.loads(nested)
            if isinstance(parsed, dict):
                return parsed
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    return row


def dossier_meta(row: dict[str, Any] | None) -> dict[str, Any]:
    """Wrapper metadata (symbol/trading_date) plus unwrapped analytical fields."""
    if not row or not isinstance(row, dict):
        return {}
    body = unwrap_dossier(row)
    out = dict(body)
    if row.get("symbol") is not None:
        out.setdefault("symbol", row.get("symbol"))
    if row.get("trading_date") is not None:
        out.setdefault("trading_date", row.get("trading_date"))
    if row.get("normalized_status") is not None:
        out.setdefault("normalized_status", row.get("normalized_status"))
    if row.get("dossier_version") is not None:
        out.setdefault("dossier_version", row.get("dossier_version"))
    return out


def level_unavailable() -> str:
    return "Not available"


def format_level_value(value: Any) -> Any:
    if value is None or value == "":
        return None
    return value
