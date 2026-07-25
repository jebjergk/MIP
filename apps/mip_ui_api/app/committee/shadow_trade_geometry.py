"""
Align shadow chair trade prose with executable MIP geometry (entry zone / invalidation / live price).

The overnight dossier may cite stale structural levels (e.g. broken_resistance_as_support from
an earlier leg of the move). Operators must see the same band LPA uses for orders.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from app.committee.engine import _f
from app.committee.shadow_types import ShadowChairRuling, ShadowTradeArtifact

# Dossier reclaim more than this fraction below entry-zone low is treated as legacy context only.
_LEGACY_RECLAIM_MAX_GAP_PCT = 0.03


def resolve_execution_reclaim_level(
    side: str,
    *,
    zone_low: Optional[float],
    zone_high: Optional[float],
    inv_level: Optional[float],
    dossier_payload: Optional[Dict[str, Any]],
    last_price: Optional[float],
) -> Tuple[Optional[float], str, Optional[float]]:
    """
    Pick the reclaim/support level used for intraday substantiation.

    Returns (level, source, dossier_legacy_level).
    """
    dossier_payload = dossier_payload or {}
    levels = dossier_payload.get("levels") or {}
    dossier_legacy = _f(levels.get("broken_resistance_as_support"))
    ns = levels.get("nearest_support") or {}
    dossier_support = _f(ns.get("level_price") if isinstance(ns, dict) else ns)

    side_u = (side or "LONG").upper()

    if side_u == "LONG":
        if inv_level is not None:
            return float(inv_level), "invalidation", dossier_legacy
        if zone_low is not None:
            return float(zone_low), "entry_zone_low", dossier_legacy
        if dossier_support is not None:
            return float(dossier_support), "dossier_support", dossier_legacy
        if dossier_legacy is not None:
            return float(dossier_legacy), "dossier_legacy", dossier_legacy
        return None, "none", dossier_legacy

    # SHORT: prefer entry zone high, then dossier resistance (not implemented deeply yet).
    if zone_high is not None:
        return float(zone_high), "entry_zone_high", dossier_legacy
    nr = levels.get("nearest_resistance") or {}
    dossier_res = _f(nr.get("level_price") if isinstance(nr, dict) else nr)
    if dossier_res is not None:
        return float(dossier_res), "dossier_resistance", dossier_legacy
    return None, "none", dossier_legacy


def price_vs_entry_zone(
    last_price: Optional[float],
    zone_low: Optional[float],
    zone_high: Optional[float],
) -> Optional[str]:
    if last_price is None or zone_low is None or zone_high is None:
        return None
    if float(last_price) > float(zone_high):
        return "ABOVE"
    if float(last_price) < float(zone_low):
        return "BELOW"
    return "IN"


def format_executable_entry_zone_text(
    *,
    side: str,
    zone_low: Optional[float],
    zone_high: Optional[float],
    inv_level: Optional[float],
    latest_price: Optional[float],
) -> str:
    side_u = (side or "LONG").upper()
    if zone_low is not None and zone_high is not None:
        zlo, zhi = float(zone_low), float(zone_high)
        inv = f"; invalidation {float(inv_level):.2f}" if inv_level is not None else ""
        px = float(latest_price) if latest_price is not None else None
        if px is not None and px > zhi:
            return (
                f"Do not chase at {px:.2f}. Wait for pullback into executable zone "
                f"{zlo:.2f}–{zhi:.2f}{inv} or a clean breakout above overhead resistance."
            )
        if px is not None and px < zlo and side_u == "LONG":
            return (
                f"Wait for price to reach executable zone {zlo:.2f}–{zhi:.2f}{inv} "
                f"(currently {px:.2f})."
            )
        if px is not None:
            return f"Executable zone {zlo:.2f}–{zhi:.2f}{inv}; current price {px:.2f}."
        return f"Executable zone {zlo:.2f}–{zhi:.2f}{inv}."
    if inv_level is not None:
        return f"Invalidation / support reference {float(inv_level):.2f}."
    return ""


def format_key_condition_text(
    *,
    side: str,
    zone_low: Optional[float],
    zone_high: Optional[float],
    inv_level: Optional[float],
    latest_price: Optional[float],
    reclaim_level: Optional[float],
    reclaim_status: Optional[str],
) -> str:
    entry_text = format_executable_entry_zone_text(
        side=side,
        zone_low=zone_low,
        zone_high=zone_high,
        inv_level=inv_level,
        latest_price=latest_price,
    )
    status = str(reclaim_status or "").upper()
    if status == "INSUFFICIENT_RTH_DATA":
        return (
            f"{entry_text} Early session: insufficient RTH 15m bars for intraday confirmation — "
            "use daily structure and executable zone, not stale overnight reclaim levels."
        )
    if reclaim_level is not None and status == "HELD":
        return (
            f"{entry_text} Support / invalidation {float(reclaim_level):.2f} is held vs "
            "executable geometry; still require price in zone (not chasing extended levels)."
        )
    if reclaim_level is not None and status == "FAILED":
        return f"{entry_text} Reclaim failed at {float(reclaim_level):.2f} — do not enter."
    return entry_text


def normalize_shadow_trade_artifact(
    trade: Optional[ShadowTradeArtifact],
    *,
    side: str,
    zone_low: Optional[float],
    zone_high: Optional[float],
    inv_level: Optional[float],
    latest_price: Optional[float],
    reclaim_level: Optional[float],
    reclaim_status: Optional[str],
) -> Optional[ShadowTradeArtifact]:
    if trade is None:
        trade = ShadowTradeArtifact(advisory_only=True)
    entry_zone = format_executable_entry_zone_text(
        side=side,
        zone_low=zone_low,
        zone_high=zone_high,
        inv_level=inv_level,
        latest_price=latest_price,
    )
    key_condition = format_key_condition_text(
        side=side,
        zone_low=zone_low,
        zone_high=zone_high,
        inv_level=inv_level,
        latest_price=latest_price,
        reclaim_level=reclaim_level,
        reclaim_status=reclaim_status,
    )
    if not entry_zone and not key_condition:
        return trade
    return trade.model_copy(
        update={
            "entry_zone": entry_zone or trade.entry_zone,
            "key_condition": key_condition or trade.key_condition,
            "advisory_only": True,
        }
    )


def normalize_chair_ruling_for_geometry(
    ruling: ShadowChairRuling,
    pack_slices: Optional[Dict[str, Any]],
    intraday_picture: Optional[Dict[str, Any]],
) -> ShadowChairRuling:
    """Rewrite shadow_trade advisory text to match executable entry geometry."""
    slices = pack_slices or {}
    entry = slices.get("entry_zone") or {}
    live = slices.get("live_price") or {}
    inv = slices.get("invalidation") or {}

    zone_low = _f(entry.get("zone_low"))
    zone_high = _f(entry.get("zone_high"))
    latest_price = _f(entry.get("latest_price") or live.get("latest_price"))
    inv_level = _f(inv.get("invalidation_level"))

    side = str((slices.get("proposal_meta") or {}).get("side") or "LONG")
    vs = (intraday_picture or {}).get("vs_overnight_dossier") or {}
    reclaim_level = _f(vs.get("reclaim_level"))
    reclaim_status = vs.get("reclaim_status")

    normalized_trade = normalize_shadow_trade_artifact(
        ruling.shadow_trade,
        side=side,
        zone_low=zone_low,
        zone_high=zone_high,
        inv_level=inv_level,
        latest_price=latest_price,
        reclaim_level=reclaim_level,
        reclaim_status=reclaim_status,
    )
    if normalized_trade is ruling.shadow_trade:
        return ruling
    return ruling.model_copy(update={"shadow_trade": normalized_trade})
