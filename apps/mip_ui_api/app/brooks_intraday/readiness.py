from __future__ import annotations

import hashlib
import json
from typing import Any


def compute_readiness_labels(state: dict[str, Any]) -> dict[str, str]:
    prep = state.get("preparation") or {}
    counts = prep.get("counts") or {}
    dossier_ok = (
        counts.get("dossiers_compiled") == counts.get("dossiers_expected")
        and counts.get("dossiers_expected", 0) > 0
        and not prep.get("fatal_errors")
    )
    bar_expected = counts.get("bar_sessions_expected") or 0
    bar_complete = counts.get("bar_sessions_complete") or 0
    hist_ok = bar_expected > 0 and bar_complete == bar_expected

    dossier_readiness = "READY" if dossier_ok else ("FAILED" if prep.get("preparation_status") == "PREPARATION_FAILED" else "INCOMPLETE")
    historical_bar_readiness = "READY" if hist_ok else "INCOMPLETE"
    if hist_ok and dossier_ok and state.get("bars_frozen"):
        replay_readiness = "READY"
    else:
        replay_readiness = "BLOCKED"

    return {
        "dossier_readiness": dossier_readiness,
        "historical_bar_readiness": historical_bar_readiness,
        "replay_readiness": replay_readiness,
    }


def dataset_hash(bars: list) -> str:
    payload = [
        {
            "ts_utc": b.ts_utc.isoformat(),
            "o": b.open,
            "h": b.high,
            "l": b.low,
            "c": b.close,
            "v": b.volume,
        }
        for b in bars
    ]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
