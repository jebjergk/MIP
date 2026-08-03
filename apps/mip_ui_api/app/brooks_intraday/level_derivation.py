from __future__ import annotations

import re
from typing import Any


def _zone_mid(zone: dict) -> float | None:
    low = zone.get("lower") if zone.get("lower") is not None else zone.get("low")
    high = zone.get("upper") if zone.get("upper") is not None else zone.get("high")
    if low is None or high is None:
        price = zone.get("price")
        return float(price) if price is not None else None
    return (float(low) + float(high)) / 2.0


def _level_field(
    value: float | None,
    *,
    source_type: str,
    rule_id: str,
    source_paths: list[str],
    confidence: float,
    explanation: str = "",
) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "value": round(float(value), 4),
        "source_type": source_type,
        "rule_id": rule_id,
        "source_paths": source_paths,
        "confidence": confidence,
        "explanation": explanation,
    }


def _zones_from_geometry(geometry: dict) -> tuple[list[dict], list[dict]]:
    supports = list(geometry.get("support_zones") or [])
    resistances = list(geometry.get("resistance_zones") or [])
    if not supports:
        for z in geometry.get("support_resistance_zones") or []:
            if str(z.get("type", "")).upper() == "SUPPORT":
                supports.append(z)
            elif str(z.get("type", "")).upper() == "RESISTANCE":
                resistances.append(z)
    if not resistances:
        levels = geometry.get("levels") or {}
        for item in levels.get("support") or []:
            supports.append({"low": item.get("price_low"), "high": item.get("price_high"), **item})
        for item in levels.get("resistance") or []:
            resistances.append({"low": item.get("price_low"), "high": item.get("price_high"), **item})
    return supports, resistances


def _latest_close(geometry: dict) -> float | None:
    latest = geometry.get("latest") or {}
    if latest.get("close") is not None:
        return float(latest["close"])
    if geometry.get("latest_close") is not None:
        return float(geometry["latest_close"])
    return None


def _recent_swing_high(geometry: dict) -> float | None:
    swings = geometry.get("swings") or geometry.get("swing_points") or []
    highs = [float(s["price"]) for s in swings if str(s.get("kind", s.get("type", ""))).lower() in {"high", "swing_high"}]
    if not highs:
        highs = [
            float(s["price"])
            for s in swings
            if str(s.get("label", "")).upper() in {"HH", "LH", "SWING HIGH"}
        ]
    return highs[-1] if highs else None


def _parse_numeric_from_text(text: str) -> float | None:
    if not text:
        return None
    match = re.search(r"(\d+(?:\.\d+)?)", text.replace(",", ""))
    if not match:
        return None
    return float(match.group(1))


def derive_levels(
    geometry: dict,
    situation: dict,
    methodologist: dict,
    *,
    verdict: str,
) -> dict[str, Any]:
    supports, resistances = _zones_from_geometry(geometry)
    close = _latest_close(geometry)

    primary_support = _level_field(
        _zone_mid(supports[0]) if supports else None,
        source_type="DIRECT",
        rule_id="PAA_SUPPORT_PRIMARY_DIRECT_V0_1",
        source_paths=["geometry_summary_json.support_zones[0]"],
        confidence=0.9,
    )
    secondary_support = _level_field(
        _zone_mid(supports[1]) if len(supports) > 1 else None,
        source_type="DIRECT",
        rule_id="PAA_SUPPORT_SECONDARY_DIRECT_V0_1",
        source_paths=["geometry_summary_json.support_zones[1]"],
        confidence=0.85,
    )
    primary_resistance = _level_field(
        _zone_mid(resistances[0]) if resistances else None,
        source_type="DIRECT",
        rule_id="PAA_RESISTANCE_PRIMARY_DIRECT_V0_1",
        source_paths=["geometry_summary_json.resistance_zones[0]"],
        confidence=0.9,
    )

    inv_direct = geometry.get("invalidation_level")
    invalidation = _level_field(
        float(inv_direct) if inv_direct is not None else (primary_support["value"] if primary_support else None),
        source_type="DIRECT" if inv_direct is not None else "DERIVED",
        rule_id="PAA_INVALIDATION_DIRECT_V0_1" if inv_direct is not None else "INVALIDATION_PRIMARY_SUPPORT_V0_1",
        source_paths=["geometry_summary_json.invalidation_level"]
        if inv_direct is not None
        else ["geometry_summary_json.support_zones[0]"],
        confidence=0.9 if inv_direct is not None else 0.75,
    )

    reclaim_val = _recent_swing_high(geometry)
    reclaim = _level_field(
        reclaim_val,
        source_type="DERIVED",
        rule_id="RECLAIM_RECENT_SWING_V0_1",
        source_paths=["geometry_summary_json.swings", "situation_model_json.location"],
        confidence=0.65,
    )

    range_high = (geometry.get("range") or {}).get("high")
    dnc = _level_field(
        float(range_high) if range_high is not None else None,
        source_type="DERIVED",
        rule_id="DO_NOT_CHASE_RANGE_HIGH_V0_1",
        source_paths=["geometry_summary_json.range.high"],
        confidence=0.7,
    )
    if not dnc and primary_resistance:
        dnc = _level_field(
            primary_resistance["value"],
            source_type="DERIVED",
            rule_id="DO_NOT_CHASE_PRIMARY_RESISTANCE_V0_1",
            source_paths=["geometry_summary_json.resistance_zones[0]"],
            confidence=0.65,
        )

    entry_low = supports[0].get("lower") or supports[0].get("low") if supports else None
    entry_high = supports[0].get("upper") or supports[0].get("high") if supports else None
    entry_zone = None
    if entry_low is not None and entry_high is not None:
        entry_zone = {
            "low": round(float(entry_low), 4),
            "high": round(float(entry_high), 4),
            "source_type": "DIRECT",
            "rule_id": "ENTRY_ZONE_PRIMARY_SUPPORT_V0_1",
            "source_paths": ["geometry_summary_json.support_zones[0]"],
            "confidence": 0.85,
        }

    expert = methodologist.get("expert") or methodologist.get("verdict") or {}
    if isinstance(expert, dict):
        inv_text = expert.get("invalidation_logic") or methodologist.get("invalidation_logic")
    else:
        inv_text = methodologist.get("invalidation_logic")
    if not invalidation and inv_text:
        parsed = _parse_numeric_from_text(str(inv_text))
        invalidation = _level_field(
            parsed,
            source_type="DERIVED",
            rule_id="INVALIDATION_TEXT_NUMERIC_EXTRACT_V0_1",
            source_paths=["methodologist_output_json.invalidation_logic"],
            confidence=0.55,
            explanation="Numeric level extracted from methodologist invalidation prose.",
        )

    return {
        "primary_support": primary_support,
        "secondary_support": secondary_support,
        "primary_resistance": primary_resistance,
        "entry_zone": entry_zone,
        "reclaim_level": reclaim,
        "invalidation_level": invalidation,
        "do_not_chase_level": dnc,
        "latest_close": close,
        "verdict": verdict,
        "long_location": geometry.get("long_location") or situation.get("long_location"),
    }


def assess_readiness(
    dossier: dict,
    *,
    verdict: str,
) -> tuple[bool, bool, str, list[str]]:
    messages: list[str] = []
    geometry = dossier.get("geometry_summary_json") or {}
    supports, resistances = _zones_from_geometry(geometry)
    has_level_ref = bool(supports or resistances or dossier.get("derived_levels", {}).get("primary_support"))
    summary = (
        dossier.get("methodologist_summary")
        or (dossier.get("methodologist_output_json") or {}).get("plain_explanation")
        or ""
    )
    trend = dossier.get("daily_trend") or (geometry.get("structure") or {}).get("trend")
    cycle = dossier.get("market_cycle") or (geometry.get("market_cycle_context") or {}).get("cycle")

    observation_ready = bool(
        (dossier.get("paa_analysis_id") or dossier.get("reconstruction_id"))
        and dossier.get("paa_verdict")
        and (trend or cycle or dossier.get("location"))
        and has_level_ref
        and summary
    )
    if not observation_ready:
        messages.append("Missing core PAA context for observation replay.")

    inv = dossier.get("derived_levels", {}).get("invalidation_level")
    inv_val = inv.get("value") if isinstance(inv, dict) else dossier.get("invalidation_level")
    simulation_ready = observation_ready and inv_val is not None

    v = str(verdict or "").upper()
    if v in {"WAIT_PULLBACK", "WAIT_RECLAIM", "LONG_APPROVE", "LONG_APPROVE_REDUCED"}:
        if not inv_val:
            simulation_ready = False
            messages.append("No deterministic invalidation level — not tradable in simulation.")
        dnc = dossier.get("derived_levels", {}).get("do_not_chase_level")
        if not dnc and v == "WAIT_PULLBACK":
            messages.append("Do-not-chase level not derived — simulation entry should remain blocked.")

    status = "READY"
    if not observation_ready:
        status = "DOSSIER_INCOMPLETE_NOT_TRADABLE"
    elif not simulation_ready:
        status = "DOSSIER_INCOMPLETE_NOT_TRADABLE"

    return observation_ready, simulation_ready, status, messages
