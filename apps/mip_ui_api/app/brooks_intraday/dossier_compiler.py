from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any

from .constants import COMPILER_VERSION_DEFAULT, DOSSIER_VERSION_DEFAULT
from .level_derivation import assess_readiness, derive_levels
from .paa_selection import PaaSelectionResult


def _stable_hash(payload: dict) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _structure_fields(geometry: dict) -> tuple[str | None, str | None, str | None, str | None]:
    structure = geometry.get("structure") or {}
    trend = structure.get("trend")
    highs = structure.get("highs")
    lows = structure.get("lows")
    daily_structure = None
    if highs and lows:
        daily_structure = f"{highs}_{lows}".replace(" ", "_").upper()
    cycle_ctx = geometry.get("market_cycle_context") or {}
    market_cycle = cycle_ctx.get("cycle") or cycle_ctx.get("label")
    location = geometry.get("long_location") or geometry.get("entry_location")
    return trend, daily_structure, market_cycle, location


def compile_frozen_dossier(
    *,
    run_id: str,
    selection: PaaSelectionResult,
    compiled_at_utc: datetime,
) -> dict[str, Any]:
    rec = selection.record or {}
    geometry = rec.get("geometry_summary_json") or {}
    situation = rec.get("situation_model_json") or {}
    methodologist = rec.get("methodologist_output_json") or {}
    verdict = str(rec.get("verdict") or "")
    confidence = float(rec.get("confidence") or 0)

    trend, daily_structure, market_cycle, location = _structure_fields(geometry)
    derived = derive_levels(geometry, situation, methodologist, verdict=verdict)
    latest_close = derived.get("latest_close")

    supports, resistances = [], []
    from .level_derivation import _zones_from_geometry

    sz, rz = _zones_from_geometry(geometry)

    methodologist_summary = ""
    if isinstance(methodologist, dict):
        methodologist_summary = (
            methodologist.get("plain_explanation")
            or (methodologist.get("expert") or {}).get("plain_explanation")
            or methodologist.get("summary")
            or ""
        )

    preferred = (methodologist.get("expert") or {}).get("preferred_scenario") if isinstance(methodologist, dict) else ""
    main_risk = (methodologist.get("expert") or {}).get("main_risk") if isinstance(methodologist, dict) else ""

    dossier: dict[str, Any] = {
        "run_id": run_id,
        "symbol": selection.symbol,
        "trading_date": selection.trading_date.isoformat(),
        "paa_analysis_id": selection.analysis_id,
        "board_run_id": selection.board_run_id,
        "paa_as_of_date": str(selection.as_of_date) if selection.as_of_date else None,
        "paa_scanned_at_utc": selection.scanned_at_utc.isoformat() if selection.scanned_at_utc else None,
        "paa_verdict": verdict,
        "paa_confidence": confidence,
        "daily_trend": trend,
        "daily_structure": daily_structure,
        "market_cycle": market_cycle,
        "location": location,
        "latest_close": latest_close,
        "support_zones": sz,
        "resistance_zones": rz,
        "entry_zone": derived.get("entry_zone"),
        "reclaim_level": (derived.get("reclaim_level") or {}).get("value"),
        "invalidation_level": (derived.get("invalidation_level") or {}).get("value"),
        "do_not_chase_level": (derived.get("do_not_chase_level") or {}).get("value"),
        "preferred_long_scenario": preferred or "",
        "main_risk": main_risk or "",
        "methodologist_summary": methodologist_summary,
        "geometry_summary_json": geometry,
        "situation_model_json": situation,
        "methodologist_output_json": methodologist,
        "rag_card_ids": rec.get("card_ids") or [],
        "dossier_version": DOSSIER_VERSION_DEFAULT,
        "compiler_version": COMPILER_VERSION_DEFAULT,
        "compiled_at_utc": compiled_at_utc.replace(tzinfo=None).isoformat(timespec="seconds"),
        "derived_levels": derived,
        "paa_selection": {
            "selection_rule": selection.selection_rule,
            "candidate_count": selection.candidate_count,
            "rejected": selection.rejected[:50],
        },
        "validation_messages": [],
    }

    obs_ready, sim_ready, val_status, val_messages = assess_readiness(dossier, verdict=verdict)
    dossier["observation_ready"] = obs_ready
    dossier["trade_simulation_ready"] = sim_ready
    dossier["validation_status"] = val_status if obs_ready else val_status
    dossier["validation_messages"] = val_messages

    source_hash = _stable_hash(
        {
            "analysis_id": selection.analysis_id,
            "geometry": geometry,
            "situation": situation,
            "methodologist": methodologist,
            "compiler_version": COMPILER_VERSION_DEFAULT,
        }
    )
    dossier["source_hash"] = source_hash
    return dossier
