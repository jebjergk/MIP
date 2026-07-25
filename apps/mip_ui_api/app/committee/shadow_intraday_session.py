"""
Shadow Board — deterministic RTH intraday session picture (Stage 0.5).

Computes a compact execution-substantiation slice from 15m bars since today's
RTH open. No pre-market bars. No raw OHLC passed to agents.

Independent of the retired Committee 2.0 intraday_substantiation artifact path.
Only reuses the shared IBKR bar fetch helper (ibkr_live_bars).
"""
from __future__ import annotations

import logging
from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from app.committee.engine import _entry_zone, _f, _invalidation, invalidation_breached
from app.committee.shadow_trade_geometry import (
    price_vs_entry_zone,
    resolve_execution_reclaim_level,
)
from app.services.ibkr_live_bars import infer_ib_market_type, resolve_live_bars_connect, run_agent_ibkr_live_bars

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")
_RTH_OPEN = time(9, 30)
_INTERVAL_MIN = 15
_MIN_BARS_FOR_HIGH_CONF = 4

VERDICT_SUPPORTS = "SUPPORTS"
VERDICT_MIXED = "MIXED"
VERDICT_CHALLENGES = "CHALLENGES"


def _ensure_et(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=_ET)
    return dt.astimezone(_ET)


def _parse_ts(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        s = str(v).strip().replace("Z", "+00:00")
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s[:26])
        except ValueError:
            return None
    return _ensure_et(dt)


def _ts_iso_et(v: datetime) -> str:
    return v.astimezone(_ET).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")


def _today_rth_open_et(now: Optional[datetime] = None) -> datetime:
    now_et = _ensure_et(now or datetime.now(_ET))
    return datetime.combine(now_et.date(), _RTH_OPEN, tzinfo=_ET)


def _normalize_bar_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ts = _parse_ts(row.get("TS") or row.get("ts"))
    o, h, l, c = _f(row.get("OPEN") or row.get("open")), _f(row.get("HIGH") or row.get("high")), _f(
        row.get("LOW") or row.get("low")
    ), _f(row.get("CLOSE") or row.get("close"))
    if ts is None or c is None:
        return None
    return {"ts": ts, "o": o, "h": h, "l": l, "c": c}


def filter_bars_rth_since_open(
    bar_rows: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], Optional[datetime]]:
    """Keep 15m bars from today's RTH open (ET) through now. No pre-market."""
    now_et = _ensure_et(now or datetime.now(_ET))
    rth_open = _today_rth_open_et(now_et)
    session_date = now_et.date()

    out: List[Dict[str, Any]] = []
    for row in bar_rows:
        bar = _normalize_bar_row(row)
        if not bar:
            continue
        ts: datetime = bar["ts"]
        if ts.date() != session_date:
            continue
        if ts < rth_open:
            continue
        if ts > now_et + timedelta(minutes=_INTERVAL_MIN):
            continue
        out.append(bar)

    out.sort(key=lambda b: b["ts"])
    return out, rth_open


def fetch_rth_15m_bars(
    symbol: str,
    market_type: str | None = None,
    *,
    portfolio_id: int | None = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Fetch 15m IBKR bars (RTH-only request) and trim to today's session open → now.

    Returns (filtered_bars, fetch_meta) where fetch_meta includes ib_connect and status.
    """
    meta: Dict[str, Any] = {"status": "SKIPPED", "ib_connect": resolve_live_bars_connect(portfolio_id)}
    sym = str(symbol or "").strip()
    if not sym:
        meta["status"] = "NO_SYMBOL"
        return [], meta
    if infer_ib_market_type(sym, market_type) != "STOCK":
        meta["status"] = "NON_STOCK"
        return [], meta

    try:
        payload = run_agent_ibkr_live_bars(
            [{"symbol": sym, "market_type": "STOCK"}],
            interval_minutes=_INTERVAL_MIN,
            window_bars=32,
            timeout_sec=35,
            diagnostics_surface="shadow_intraday_session",
            regular_trading_hours_only=True,
            portfolio_id=portfolio_id,
        )
        meta["status"] = str(payload.get("status") or "UNKNOWN").upper()
        meta["ib_connect"] = payload.get("ib_connect") or meta["ib_connect"]
    except Exception as exc:
        logger.warning("shadow_intraday: IB fetch failed for %s: %s", sym, exc)
        meta["status"] = "FETCH_FAILED"
        meta["fetch_error"] = str(exc)[:400]
        return [], meta

    symbols = payload.get("symbols") or []
    if not symbols or str(symbols[0].get("status") or "").upper() != "SUCCESS":
        meta["status"] = "NO_SYMBOL_DATA"
        return [], meta
    raw = symbols[0].get("bars") if isinstance(symbols[0].get("bars"), list) else []
    shaped = [
        {"TS": b.get("ts"), "OPEN": b.get("open"), "HIGH": b.get("high"), "LOW": b.get("low"), "CLOSE": b.get("close")}
        for b in raw
        if isinstance(b, dict)
    ]
    filtered, _ = filter_bars_rth_since_open(shaped)
    meta["raw_bar_count"] = len(shaped)
    meta["rth_bar_count"] = len(filtered)
    if len(filtered) >= 1:
        meta["status"] = "SUCCESS"
    elif len(shaped) > 0:
        meta["status"] = "FILTERED_EMPTY"
    return filtered, meta


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


def _count_structure(closes: List[float]) -> Tuple[int, int]:
    hh = hl = 0
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            hl += 1
        if closes[i] > max(closes[:i]):
            hh += 1
    return hh, hl


def _wick_noise_label(bars: List[Dict[str, Any]], side: str) -> str:
    if not bars:
        return "UNKNOWN"
    noisy = 0
    for b in bars:
        o, h, l, c = b.get("o"), b.get("h"), b.get("l"), b.get("c")
        if h is None or l is None or c is None:
            continue
        o = o if o is not None else c
        rng = h - l
        if rng <= 0:
            noisy += 1
            continue
        body = abs(c - o)
        upper = h - max(o, c)
        lower = min(o, c) - l
        if body / rng < 0.25:
            noisy += 1
            continue
        if side.upper() == "LONG" and upper / rng > 0.5:
            noisy += 1
        elif side.upper() == "SHORT" and lower / rng > 0.5:
            noisy += 1
    ratio = noisy / len(bars)
    if ratio >= 0.4:
        return "HIGH"
    if ratio >= 0.2:
        return "MEDIUM"
    return "LOW"


def _rejection_at_overhead(bars: List[Dict[str, Any]], side: str, overhead: Optional[float]) -> bool:
    if not bars or overhead is None or overhead <= 0:
        return False
    last = bars[-1]
    h, l, c = last.get("h"), last.get("l"), last.get("c")
    o = last.get("o") or c
    if h is None or l is None or c is None:
        return False
    rng = h - l
    if rng <= 0:
        return False
    near = abs(h - overhead) / overhead < 0.004
    if not near:
        return False
    if side.upper() == "LONG":
        upper = h - max(o or c, c)
        return upper / rng > 0.45
    lower = min(o or c, c) - l
    return lower / rng > 0.45


def _reclaim_status(
    side: str,
    bars: List[Dict[str, Any]],
    reclaim_level: Optional[float],
    *,
    reference_price: Optional[float] = None,
) -> str:
    if reclaim_level is None:
        return "NOT_APPLICABLE"
    side_u = (side or "LONG").upper()
    ref = reference_price
    if ref is None and bars:
        closes = [float(b["c"]) for b in bars if b.get("c") is not None]
        if closes:
            ref = closes[-1]
    if ref is not None:
        if side_u == "LONG" and float(ref) >= float(reclaim_level) * 0.999:
            return "HELD"
        if side_u == "SHORT" and float(ref) <= float(reclaim_level) * 1.001:
            return "HELD"
    if not bars:
        return "INSUFFICIENT_RTH_DATA"
    closes = [float(b["c"]) for b in bars if b.get("c") is not None]
    if len(closes) < 2:
        return "INSUFFICIENT_RTH_DATA"
    tail = closes[-2:]
    if side_u == "LONG":
        if all(c >= reclaim_level * 0.999 for c in tail):
            return "HELD"
        if closes[-1] < reclaim_level * 0.997:
            return "FAILED"
    else:
        if all(c <= reclaim_level * 1.001 for c in tail):
            return "HELD"
        if closes[-1] > reclaim_level * 1.003:
            return "FAILED"
    return "PENDING"


def _session_character(side: str, closes: List[float], chop: int, choppy: bool) -> str:
    if choppy:
        return "CHOPPY"
    if len(closes) >= 4:
        hi, lo = max(closes[-4:]), min(closes[-4:])
        if hi > lo:
            if side.upper() == "LONG" and closes[-1] >= closes[-4] and (closes[-1] - lo) / (hi - lo) > 0.35:
                return "ORDERLY_CONTINUATION"
            if side.upper() == "SHORT" and closes[-1] <= closes[-4] and (hi - closes[-1]) / (hi - lo) > 0.35:
                return "ORDERLY_CONTINUATION"
    if side.upper() == "LONG" and len(closes) >= 2 and closes[-1] > closes[0]:
        return "CONSTRUCTIVE"
    if side.upper() == "SHORT" and len(closes) >= 2 and closes[-1] < closes[0]:
        return "CONSTRUCTIVE"
    return "MIXED"


def _extract_dossier_levels(dossier_payload: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[float], Optional[float]]:
    act = dossier_payload.get("actionability_context") or {}
    levels = dossier_payload.get("levels") or {}
    reclaim = _f(levels.get("broken_resistance_as_support"))
    if reclaim is None:
        ns = levels.get("nearest_support") or {}
        reclaim = _f(ns.get("level_price") if isinstance(ns, dict) else ns)
    nr = levels.get("nearest_resistance") or {}
    resistance = _f(nr.get("level_price") if isinstance(nr, dict) else nr)
    return act, reclaim, resistance


def _overnight_hostile(act: Dict[str, Any]) -> bool:
    if act.get("confirmation_needed"):
        return True
    if str(act.get("continuation_quality") or "").upper() == "UNCONFIRMED":
        return True
    if str(act.get("entry_location_quality") or "").upper() == "AT_RESISTANCE":
        return True
    return False


def build_shadow_intraday_session_picture(
    *,
    symbol: str,
    side: str,
    snapshot: Dict[str, Any],
    dossier_payload: Optional[Dict[str, Any]] = None,
    bar_rows: Optional[List[Dict[str, Any]]] = None,
    now: Optional[datetime] = None,
    portfolio_id: Optional[int] = None,
    fetch_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build compact intraday_session_picture slice for shadow evidence pack.
    When bar_rows is None, fetches from IBKR. Failures yield session_available=false.
    """
    base: Dict[str, Any] = {
        "temporal_authority": "EXECUTION_SUBSTANTIATION",
        "premarket_excluded": True,
        "interval_minutes": _INTERVAL_MIN,
    }
    now_et = _ensure_et(now or datetime.now(_ET))
    rth_open = _today_rth_open_et(now_et)

    if now_et < rth_open:
        return {
            **base,
            "session_available": False,
            "reason": "BEFORE_RTH_OPEN",
            "ib_fetch": fetch_meta or {"ib_connect": resolve_live_bars_connect(portfolio_id)},
            "operator_line": "RTH session has not opened yet — no intraday substantiation.",
        }

    meta = fetch_meta
    rows = bar_rows
    if rows is None:
        rows, meta = fetch_rth_15m_bars(symbol, portfolio_id=portfolio_id)
    elif bar_rows is not None:
        rows, _ = filter_bars_rth_since_open(rows, now=now_et)
        meta = fetch_meta or {"status": "PROVIDED_BARS"}

    if len(rows) < 1:
        reason = "INSUFFICIENT_RTH_BARS"
        if meta and meta.get("status") == "FETCH_FAILED":
            reason = "IB_FETCH_FAILED"
        elif meta and meta.get("status") == "FILTERED_EMPTY":
            reason = "RTH_FILTER_EMPTY"
        return {
            **base,
            "session_available": False,
            "reason": reason,
            "rth_open_ts": _ts_iso_et(rth_open),
            "as_of_ts": _ts_iso_et(now_et),
            "ib_fetch": meta or {},
            "operator_line": (
                "Insufficient RTH 15m bars since open — defer to live_price and structural prior."
                if reason == "INSUFFICIENT_RTH_BARS"
                else f"Intraday bar fetch issue ({reason}) — defer to live_price and structural prior."
            ),
        }

    side_u = (side or "LONG").upper()
    zone_low, zone_high = _entry_zone(snapshot)
    inv_level, _ = _invalidation(snapshot)
    last_px = float(rows[-1]["c"])
    breach = invalidation_breached(side_u, last_px, inv_level)

    closes = [float(b["c"]) for b in rows]
    first_open = rows[0].get("o") or closes[0]
    session_change_pct = round((closes[-1] - float(first_open)) / float(first_open) * 100, 3) if first_open else 0.0

    chop = _chop_score(closes)
    choppy = chop >= max(3, len(closes) // 3)
    hh, hl = _count_structure(closes)
    wick_noise = _wick_noise_label(rows, side_u)

    dossier_payload = dossier_payload or {}
    act_ctx, _, overhead = _extract_dossier_levels(dossier_payload)
    reclaim_level, reclaim_source, dossier_legacy_reclaim = resolve_execution_reclaim_level(
        side_u,
        zone_low=zone_low,
        zone_high=zone_high,
        inv_level=inv_level,
        dossier_payload=dossier_payload,
        last_price=last_px,
    )
    reclaim = _reclaim_status(side_u, rows, reclaim_level, reference_price=last_px)
    px_vs_zone = price_vs_entry_zone(last_px, zone_low, zone_high)
    rejection = _rejection_at_overhead(rows, side_u, overhead)
    session_char = _session_character(side_u, closes, chop, choppy)

    if side_u == "LONG":
        aligned = session_change_pct > 0.05 and hl >= max(1, len(closes) // 4) and closes[-1] >= closes[0]
        dir_label = "BULLISH_SESSION" if aligned else "NON_BULLISH_SESSION"
    else:
        aligned = session_change_pct < -0.05 and closes[-1] <= closes[0]
        dir_label = "BEARISH_SESSION" if aligned else "NON_BEARISH_SESSION"

    low_sample = len(rows) < _MIN_BARS_FOR_HIGH_CONF
    hostile_overnight = _overnight_hostile(act_ctx)
    reclaim_unconfirmed = reclaim in ("PENDING", "INSUFFICIENT_RTH_DATA", "FAILED")

    verdict = VERDICT_MIXED
    if breach or (choppy and not aligned):
        verdict = VERDICT_CHALLENGES
    elif px_vs_zone == "ABOVE":
        # Price above executable entry ceiling — chasing; do not substantiate immediate entry.
        verdict = VERDICT_CHALLENGES if not low_sample else VERDICT_MIXED
    elif rejection and hostile_overnight:
        verdict = VERDICT_CHALLENGES
    elif aligned and not choppy and wick_noise != "HIGH":
        if hostile_overnight:
            if reclaim in ("HELD", "NOT_APPLICABLE"):
                verdict = VERDICT_SUPPORTS
            elif reclaim in ("PENDING", "INSUFFICIENT_RTH_DATA") and low_sample:
                verdict = VERDICT_MIXED
            else:
                verdict = VERDICT_MIXED
        else:
            verdict = VERDICT_SUPPORTS
    elif aligned and session_char == "ORDERLY_CONTINUATION":
        verdict = VERDICT_MIXED if hostile_overnight and reclaim_unconfirmed else VERDICT_SUPPORTS

    if (
        zone_low is not None
        and zone_high is not None
        and zone_low <= last_px <= zone_high
        and aligned
        and not choppy
        and px_vs_zone != "ABOVE"
    ):
        if not hostile_overnight or reclaim in ("HELD", "NOT_APPLICABLE"):
            verdict = VERDICT_SUPPORTS

    confidence = "LOW" if low_sample else ("HIGH" if verdict == VERDICT_SUPPORTS else "MEDIUM")
    if verdict == VERDICT_CHALLENGES and not low_sample:
        confidence = "HIGH"

    overnight_binding = False
    if hostile_overnight:
        if verdict == VERDICT_SUPPORTS and reclaim in ("HELD", "NOT_APPLICABLE") and not rejection:
            overnight_binding = False
        else:
            overnight_binding = True
    else:
        overnight_binding = False

    tension = None
    if hostile_overnight:
        if verdict == VERDICT_SUPPORTS and not overnight_binding:
            tension = (
                "Overnight dossier flagged confirmation/resistance concerns; "
                "RTH session since open substantiates entry."
            )
        elif verdict == VERDICT_CHALLENGES:
            tension = "Overnight and RTH session both challenge immediate entry."
        else:
            tension = "Overnight flags remain; RTH session is mixed."

    headline_map = {
        VERDICT_SUPPORTS: f"RTH session substantiates {side_u.lower()} setup since open",
        VERDICT_MIXED: f"RTH session mixed for {side_u.lower()} since open",
        VERDICT_CHALLENGES: f"RTH session challenges {side_u.lower()} entry since open",
    }
    operator = (
        f"Since RTH open ({len(rows)}×15m): {session_char.lower().replace('_', ' ')}, "
        f"{'aligned' if aligned else 'not aligned'} with {side_u}."
    )
    if reclaim_level is not None and reclaim != "NOT_APPLICABLE":
        status_label = reclaim.lower().replace("_", " ")
        if reclaim == "INSUFFICIENT_RTH_DATA":
            status_label = "insufficient RTH data (early session — not pending reclaim)"
        operator += f" Support {reclaim_level:.2f} ({reclaim_source}): {status_label}."
    if px_vs_zone == "ABOVE" and zone_low is not None and zone_high is not None:
        operator += f" Price {last_px:.2f} is above executable entry zone {zone_low:.2f}–{zone_high:.2f} — do not chase."
    elif px_vs_zone == "BELOW" and zone_low is not None and zone_high is not None:
        operator += f" Price {last_px:.2f} is below executable entry zone {zone_low:.2f}–{zone_high:.2f}."
    if overnight_binding:
        operator += " Overnight dossier flags still binding for execution."
    elif hostile_overnight:
        operator += " Overnight concerns overridden by today's RTH tape."

    return {
        **base,
        "session_available": True,
        "ib_fetch": meta or {},
        "rth_open_ts": _ts_iso_et(rth_open),
        "as_of_ts": _ts_iso_et(now_et),
        "bar_count": len(rows),
        "low_sample_warning": low_sample,
        "headline": headline_map[verdict],
        "verdict_bucket": verdict,
        "confidence_band": confidence,
        "direction_alignment": {
            "side": side_u,
            "aligned": aligned,
            "label": dir_label,
            "session_change_pct": session_change_pct,
        },
        "session_character": session_char,
        "chop_score": chop,
        "wick_noise": wick_noise,
        "rejection_at_overhead": rejection,
        "structure_counts": {"higher_highs": hh, "higher_lows": hl},
        "vs_overnight_dossier": {
            "overnight_hostile": hostile_overnight,
            "reclaim_level": reclaim_level,
            "reclaim_level_source": reclaim_source,
            "dossier_legacy_reclaim": dossier_legacy_reclaim,
            "reclaim_status": reclaim,
            "overhead_level": overhead,
            "overnight_flags_still_binding": overnight_binding,
            "dominant_tension": tension,
        },
        "executable_geometry": {
            "entry_zone_low": zone_low,
            "entry_zone_high": zone_high,
            "invalidation_level": inv_level,
            "last_price": last_px,
            "price_vs_entry_zone": px_vs_zone,
            "chase_risk": px_vs_zone == "ABOVE",
        },
        "operator_line": operator,
    }
