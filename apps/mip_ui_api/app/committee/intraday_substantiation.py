"""
Evidence-only intraday chart payload for Committee 2.0 (INTRADAY_SUBSTANTIATION_MAP).
Does not influence stance. 15m bars are fetched on-demand from IBKR (subprocess), not Snowflake.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException

from app.committee.engine import _entry_zone, _f, _invalidation, invalidation_breached
from app.services.ibkr_live_bars import infer_ib_market_type, run_agent_ibkr_live_bars

ARTIFACT_KIND = "INTRADAY_SUBSTANTIATION_MAP"
MARKER_CAP = 5

VERDICT_SUPPORTS = "SUPPORTS"
VERDICT_MIXED = "MIXED"
VERDICT_CHALLENGES = "CHALLENGES"

TRADER_VERDICT_LINES = {
    VERDICT_SUPPORTS: "Today supports the proposal",
    VERDICT_MIXED: "Today is mixed vs proposal",
    VERDICT_CHALLENGES: "Today challenges immediate entry",
}


def committee_intraday_ib_use_rth_only() -> bool:
    """If true, committee 15m request uses IB useRTH=1 (regular hours only). Default false = extended where IB provides it."""
    v = os.getenv("COMMITTEE2_INTRADAY_IB_USE_RTH_ONLY", "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _ts_to_iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return v.isoformat(sep=" ", timespec="seconds")
    s = str(v).strip()
    return s or None


def _parse_ts(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.replace(tzinfo=None) if v.tzinfo is None else v.astimezone(timezone.utc).replace(tzinfo=None)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        d = datetime.fromisoformat(s[:26])
        if d.tzinfo is not None:
            d = d.astimezone(timezone.utc).replace(tzinfo=None)
        return d
    except ValueError:
        return None


def fetch_intraday_bars_15m_ib(
    symbol: str,
    market_type: str | None = None,
    *,
    window_bars: int = 48,
    timeout_sec: int = 75,
) -> List[Dict[str, Any]]:
    """
    Latest 15m bars from IBKR at hearing refresh (cursorfiles/fetch_ibkr_live_bars.py).
    Returns Snowflake-shaped rows (TS, OPEN, …) oldest-first. Empty on any failure.
    """
    sym = str(symbol or "").strip()
    if not sym:
        return []
    mkt = infer_ib_market_type(sym, market_type)
    try:
        payload = run_agent_ibkr_live_bars(
            [{"symbol": sym, "market_type": mkt}],
            interval_minutes=15,
            window_bars=max(15, min(int(window_bars), 120)),
            timeout_sec=int(timeout_sec),
            diagnostics_surface="committee2_intraday",
            regular_trading_hours_only=committee_intraday_ib_use_rth_only(),
        )
    except HTTPException:
        return []
    except Exception:
        return []

    symbols = payload.get("symbols") or []
    if not symbols:
        return []
    item = symbols[0]
    if str(item.get("status") or "").upper() != "SUCCESS":
        return []
    bars_raw = item.get("bars") if isinstance(item.get("bars"), list) else []
    out: List[Dict[str, Any]] = []
    for b in bars_raw:
        if not isinstance(b, dict):
            continue
        ts = b.get("ts")
        o = b.get("open")
        h = b.get("high")
        l = b.get("low")
        c = b.get("close")
        v = b.get("volume")
        if ts is None or c is None:
            continue
        out.append(
            {
                "TS": ts,
                "OPEN": o,
                "HIGH": h,
                "LOW": l,
                "CLOSE": c,
                "VOLUME": v,
                "SOURCE": "IBKR_DIRECT",
            }
        )
    return out


def _proposal_expectation_line(setup_family: Optional[str], side: str) -> str:
    sf = (setup_family or "").upper()
    su = (side or "").upper()
    if "PULLBACK" in sf or "PB" in sf:
        return "Expected: constructive pullback then continuation toward the proposal."
    if "BREAK" in sf or "BASE" in sf or "RANGE" in sf:
        return "Expected: resolution / follow-through after a defined base or range."
    if "REVERSAL" in sf or "MEAN" in sf:
        return "Expected: reversal or mean-reversion behavior consistent with the setup."
    if su == "LONG":
        return "Expected: price behavior that supports a long expression without invalidation."
    if su == "SHORT":
        return "Expected: price behavior that supports a short expression without invalidation."
    return "Expected: session behavior aligned with the structural proposal."


def _median_bar_range(bars: List[Dict[str, Any]]) -> float:
    ranges: List[float] = []
    for b in bars:
        lo, hi = _f(b.get("LOW")), _f(b.get("HIGH"))
        if lo is not None and hi is not None and hi > lo:
            ranges.append(hi - lo)
    if not ranges:
        return 0.0
    ranges.sort()
    return ranges[len(ranges) // 2]


def _find_swings(closes: List[float]) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    for i in range(1, len(closes) - 1):
        if closes[i] > closes[i - 1] and closes[i] > closes[i + 1]:
            out.append((i, "SWING_HIGH"))
        elif closes[i] < closes[i - 1] and closes[i] < closes[i + 1]:
            out.append((i, "SWING_LOW"))
    return out


def _chop_score(closes: List[float]) -> int:
    if len(closes) < 3:
        return 0
    flips = 0
    for i in range(2, len(closes)):
        d0 = closes[i - 1] - closes[i - 2]
        d1 = closes[i] - closes[i - 1]
        if d0 * d1 < 0:
            flips += 1
    return flips


def _orderly_continuation_long(closes: List[float]) -> bool:
    if len(closes) < 4:
        return False
    hi = max(closes[-4:])
    lo = min(closes[-4:])
    return closes[-1] >= closes[-4] and hi > closes[-4] and (hi - lo) > 0 and (closes[-1] - lo) / (hi - lo) > 0.35


def _orderly_continuation_short(closes: List[float]) -> bool:
    if len(closes) < 4:
        return False
    hi = max(closes[-4:])
    lo = min(closes[-4:])
    return closes[-1] <= closes[-4] and lo < closes[-4] and (hi - lo) > 0 and (hi - closes[-1]) / (hi - lo) > 0.35


def _cap_markers(
    candidates: List[Dict[str, Any]],
    bars: List[Dict[str, Any]],
    *,
    zone_low: Optional[float],
    zone_high: Optional[float],
    inv_level: Optional[float],
) -> List[Dict[str, Any]]:
    def score(m: Dict[str, Any]) -> float:
        idx = int(m.get("bar_index", -1))
        if idx < 0 or idx >= len(bars):
            return 0.0
        c = float(bars[idx]["c"])
        sc = 1.0 + (idx / max(len(bars), 1)) * 2.0
        if zone_low is not None and zone_high is not None:
            if zone_low <= c <= zone_high:
                sc += 1.5
        if inv_level is not None and abs(c - inv_level) / max(abs(c), 1e-9) < 0.003:
            sc += 2.0
        if m.get("kind") in ("PULLBACK", "RECLAIM", "BREAKAWAY"):
            sc += 0.8
        return sc

    ranked = sorted(candidates, key=score, reverse=True)
    return ranked[:MARKER_CAP]


def build_intraday_substantiation_artifact(
    snapshot: Dict[str, Any],
    live: Any,
    bar_rows: List[Dict[str, Any]],
    proposal_ts_raw: Any,
) -> Optional[Dict[str, Any]]:
    if len(bar_rows) < 2:
        return None

    sym = (snapshot.get("SYMBOL") or "").strip() or "—"
    side = (snapshot.get("SIDE") or "").strip() or "LONG"
    setup = snapshot.get("SETUP_FAMILY")
    market_type = "STOCK"

    bars: List[Dict[str, Any]] = []
    for b in bar_rows:
        ts_iso = _ts_to_iso(b.get("TS"))
        if not ts_iso:
            continue
        o, h, l, c = _f(b.get("OPEN")), _f(b.get("HIGH")), _f(b.get("LOW")), _f(b.get("CLOSE"))
        if c is None:
            continue
        bars.append(
            {
                "ts": ts_iso,
                "o": o,
                "h": h,
                "l": l,
                "c": c,
                "v": float(b["VOLUME"]) if b.get("VOLUME") is not None else 0.0,
            }
        )

    if len(bars) < 2:
        return None

    window_start_ts = bars[0]["ts"]
    window_end_ts = bars[-1]["ts"]
    w0 = bar_rows[0]
    wh, wl = _f(w0.get("HIGH")), _f(w0.get("LOW"))
    if wh is None or wl is None:
        wh, wl = bars[0]["h"], bars[0]["l"]
    window_start_range = {"low": wl, "high": wh}

    zone_low, zone_high = _entry_zone(snapshot)
    inv_level, inv_rule = _invalidation(snapshot)
    last_px = float(bars[-1]["c"])
    breach = invalidation_breached(side, last_px, inv_level)

    hi_win = max((b["h"] or b["c"]) for b in bars)
    lo_win = min((b["l"] or b["c"]) for b in bars)

    support_levels: List[Dict[str, Any]] = [{"price": lo_win, "label": "Window low"}]
    resistance_levels: List[Dict[str, Any]] = [{"price": hi_win, "label": "Window high"}]
    prior = getattr(live, "prior_close", None)
    if prior is not None and lo_win <= float(prior) <= hi_win:
        support_levels.append({"price": float(prior), "label": "Prior close"})

    closes = [float(b["c"]) for b in bars]
    swings = _find_swings(closes)
    med_rng = _median_bar_range(bar_rows)
    chop = _chop_score(closes)
    choppy = chop >= max(4, len(closes) // 4)

    marker_candidates: List[Dict[str, Any]] = []
    for idx, kind in swings[-12:]:
        label = "Local high" if kind == "SWING_HIGH" else "Local low"
        marker_candidates.append(
            {
                "bar_index": idx,
                "ts": bars[idx]["ts"],
                "kind": kind,
                "label": label,
            }
        )

    if len(closes) >= 5:
        leg_hi = max(closes[-5:-1])
        leg_lo = min(closes[-5:-1])
        if leg_hi > leg_lo:
            if side.upper() == "LONG":
                retrace = (leg_hi - closes[-1]) / (leg_hi - leg_lo)
            else:
                retrace = (closes[-1] - leg_lo) / (leg_hi - leg_lo)
            if 0.35 <= retrace <= 0.65:
                marker_candidates.append(
                    {
                        "bar_index": len(bars) - 1,
                        "ts": bars[-1]["ts"],
                        "kind": "PULLBACK",
                        "label": "Pullback",
                    }
                )

    last_range = (bars[-1].get("h") or last_px) - (bars[-1].get("l") or last_px)
    breakout = med_rng > 0 and last_range > med_rng * 1.6 and abs(closes[-1] - closes[-2]) > med_rng * 0.2
    if breakout:
        marker_candidates.append(
            {
                "bar_index": len(bars) - 1,
                "ts": bars[-1]["ts"],
                "kind": "BREAKAWAY",
                "label": "Expansion",
            }
        )

    markers_visible = _cap_markers(marker_candidates, bars, zone_low=zone_low, zone_high=zone_high, inv_level=inv_level)

    extension = False
    if zone_low is not None and zone_high is not None:
        zmid = (zone_low + zone_high) / 2
        zspan = max(zone_high - zone_low, 1e-9)
        dist_mid = abs(last_px - zmid) / zspan
        extension = dist_mid > 0.85
    stretched_run = med_rng > 0 and abs(last_px - closes[0]) > med_rng * 4
    if extension and stretched_run:
        session_badge = "Extension risk elevated"
    elif choppy:
        session_badge = "Choppy / unstable"
    elif side.upper() == "LONG" and _orderly_continuation_long(closes):
        session_badge = "Orderly continuation"
    elif side.upper() == "SHORT" and _orderly_continuation_short(closes):
        session_badge = "Orderly continuation"
    elif any(m.get("kind") == "PULLBACK" for m in markers_visible):
        session_badge = "Constructive pullback"
    else:
        session_badge = "Constructive pullback"

    proposal_expectation = _proposal_expectation_line(str(setup) if setup else None, side)

    verdict = VERDICT_MIXED
    if breach:
        verdict = VERDICT_CHALLENGES
    elif choppy and (extension or stretched_run):
        verdict = VERDICT_CHALLENGES
    elif side.upper() == "LONG" and zone_high is not None and last_px > zone_high * 1.002 and extension:
        verdict = VERDICT_CHALLENGES
    elif side.upper() == "SHORT" and zone_low is not None and last_px < zone_low * 0.998 and extension:
        verdict = VERDICT_CHALLENGES
    elif zone_low is not None and zone_high is not None and zone_low <= last_px <= zone_high and not choppy:
        verdict = VERDICT_SUPPORTS
    elif not choppy and session_badge in ("Constructive pullback", "Orderly continuation"):
        verdict = VERDICT_SUPPORTS

    if session_badge == "Extension risk elevated" and verdict == VERDICT_SUPPORTS:
        verdict = VERDICT_MIXED

    trader_line = TRADER_VERDICT_LINES[verdict]

    interpretation = (
        f"Observed: {session_badge.lower()} — last {last_px:.4f} vs proposal zone"
        + (f" {zone_low:.4f}–{zone_high:.4f}" if zone_low is not None and zone_high is not None else "")
        + "."
    )

    w_start = _parse_ts(window_start_ts)
    w_end = _parse_ts(window_end_ts)
    proposal_marker = None
    prop_dt = _parse_ts(proposal_ts_raw)
    if prop_dt is not None and w_start is not None and w_end is not None:
        if w_start <= prop_dt <= w_end:
            bar_index_prop = 0
            for i, b in enumerate(bars):
                bt = _parse_ts(b["ts"])
                if bt is not None and bt <= prop_dt:
                    bar_index_prop = i
            proposal_marker = {
                "ts": _ts_to_iso(proposal_ts_raw),
                "label": "Proposal",
                "bar_index": bar_index_prop,
            }

    payload: Dict[str, Any] = {
        "schema_version": "1",
        "headline": "Expected vs observed today",
        "interval_minutes": 15,
        "symbol": sym,
        "side": side,
        "market_type": market_type,
        "bars": bars,
        "overlays": {
            "entry_zone": {"low": zone_low, "high": zone_high},
            "invalidation": {"level": inv_level, "rule": inv_rule, "breached": breach},
            "last_price": last_px,
            "window_start_range": window_start_range,
            "window_start_label": "Window start (first bar)",
            "support_levels": support_levels,
            "resistance_levels": resistance_levels,
        },
        "proposal_marker": proposal_marker,
        "markers": markers_visible,
        "captions": {
            "proposal_expectation": proposal_expectation,
            "session_behavior_badge": session_badge,
            "interpretation_line": interpretation,
            "trader_verdict_line": trader_line,
            "verdict_bucket": verdict,
        },
        "meta": {
            "window_start_ts": window_start_ts,
            "window_end_ts": window_end_ts,
            "bar_count": len(bars),
            "source": "IBKR_DIRECT",
            "ib_regular_trading_hours_only": committee_intraday_ib_use_rth_only(),
            "historical_scope": "rolling_multi_day",
            "historical_scope_note": "IB reqHistoricalData uses a multi-day duration ending at request time, not 'today' only.",
        },
    }

    return {
        "artifact_kind": ARTIFACT_KIND,
        "schema_version": "1",
        "payload": payload,
        "evidence_refs": ["IBKR_DIRECT.fetch_ibkr_live_bars", "snapshot.ENTRY_ZONE_JSON", "snapshot.INVALIDATION_JSON"],
    }
