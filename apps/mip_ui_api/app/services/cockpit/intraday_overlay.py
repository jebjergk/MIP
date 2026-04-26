"""
Cockpit intraday open-position overlay.

Per spec section 5: a bounded, current-day, advisory overlay that gives
operators situational awareness of their currently-open live positions
*without* replacing the canonical daily Position Health verdict.

Core design rules (spec):
  * Only currently-open live positions (from V_LIVE_OPEN_POSITIONS).
  * Only IBKR 15-minute bars for today.
  * Daily verdict remains canonical; the overlay may *escalate*
    attention but never rewrites KEEP/WATCH/EXIT_REVIEW.
  * Deterministic rules first; no agentic black box.
  * No new historical intraday warehouse.
  * Fail-soft: the cockpit must always load even if TWS is down.

Outputs (per row):
  INTRADAY_STATUS  ∈ {CONSTRUCTIVE, NEUTRAL, WEAKENING, BREAKING, UNAVAILABLE}
  INTRADAY_ACTION  ∈ {HOLD, WATCH_NOW, REVIEW_NOW, SELL_NOW, NO_LIVE_CHECK}
  INTRADAY_REASON  short code
  INTRADAY_SUMMARY one-sentence plain English
  INTRADAY_EVALUATED_TS

Top-level overlay_status:
  OK             — every open symbol got bars and was evaluated.
  PARTIAL        — some symbols got bars, some didn't.
  UNAVAILABLE    — TWS down / global timeout / runtime missing.
  MARKET_CLOSED  — fetch ran but no symbol has any of today's bars.

A 60-second per-portfolio TTL cache is used. The cache key is the
portfolio_id; the cached entry holds the full assembled result so a
rapid reload does not re-shell to TWS.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.db import fetch_all, get_connection

from app.services.ibkr.intraday_bars import (
    IntradayBarsResult,
    SymbolBars,
    fetch_today_15m_bars,
)

logger = logging.getLogger(__name__)


# --- Tunables (deliberately conservative) -----------------------------------

# Hard intraday break: today's drop from open is severe enough that we
# escalate to SELL_NOW when daily verdict is already cautious.
_HARD_BREAK_PCT = -0.030      # -3.0% intraday from session open
# Soft intraday break: meaningful deterioration → REVIEW_NOW.
_SOFT_BREAK_PCT = -0.015      # -1.5%
# Watch threshold: mild adverse move → WATCH_NOW.
_WATCH_PCT      = -0.005      # -0.5%
# Constructive threshold: today net-up → CONSTRUCTIVE.
_UP_PCT         = +0.005      # +0.5%
# Cushion thinness for "approaching invalidation" reasoning.
_THIN_CUSHION_PCT = 1.5       # DISTANCE_TO_INVALIDATION_PCT in percent

# Cache: 60s per portfolio (spec refinement #4).
_CACHE_TTL_SEC = 60


# --- Public types -----------------------------------------------------------


@dataclass(frozen=True)
class IntradayOverlayRow:
    position_episode_key: str
    portfolio_id: int
    symbol: str
    intraday_status: str
    intraday_action: str
    intraday_reason: str
    intraday_summary: str
    intraday_evaluated_ts: str
    today_change_pct: Optional[float] = None
    today_low: Optional[float] = None
    today_high: Optional[float] = None
    last_price: Optional[float] = None
    today_open: Optional[float] = None
    # Raw 15m bars for the cockpit's inline chart. Each entry is a dict
    # {ts, open, high, low, close, volume}. Empty when overlay row is
    # UNAVAILABLE / NO_LIVE_CHECK.
    bars: Tuple[Dict[str, Any], ...] = field(default_factory=tuple)


@dataclass
class IntradayOverlayResult:
    overlay_status: str          # OK | PARTIAL | UNAVAILABLE | MARKET_CLOSED
    latest_eval_ts: str
    rows: List[IntradayOverlayRow] = field(default_factory=list)
    note: Optional[str] = None


# --- Cache ------------------------------------------------------------------


_cache_lock = threading.Lock()
_cache: Dict[int, Tuple[float, IntradayOverlayResult]] = {}


def _cache_get(portfolio_id: int) -> Optional[IntradayOverlayResult]:
    with _cache_lock:
        entry = _cache.get(portfolio_id)
        if not entry:
            return None
        ts, result = entry
        if (time.monotonic() - ts) > _CACHE_TTL_SEC:
            _cache.pop(portfolio_id, None)
            return None
        return result


def _cache_put(portfolio_id: int, result: IntradayOverlayResult) -> None:
    with _cache_lock:
        _cache[portfolio_id] = (time.monotonic(), result)


def _cache_clear(portfolio_id: Optional[int] = None) -> None:
    with _cache_lock:
        if portfolio_id is None:
            _cache.clear()
        else:
            _cache.pop(portfolio_id, None)


# --- SQL --------------------------------------------------------------------


_OPEN_POSITIONS_SQL = """
    SELECT
        v.PORTFOLIO_ID,
        v.SYMBOL,
        v.POSITION_EPISODE_KEY,
        v.QUANTITY,
        v.AVG_COST,
        v.MARKET_VALUE,
        v.UNREALIZED_PNL,
        v.AS_OF_TS,
        v.ENTRY_DATE
    FROM MIP.MART.V_LIVE_OPEN_POSITIONS v
    WHERE v.PORTFOLIO_ID = %(portfolio_id)s
"""

# Daily context: latest verdict + cushion to invalidation. The cushion is
# what we surface as DISTANCE_TO_INVALIDATION_PCT — this is the canonical
# "how thin is your safety margin" number computed by the daily verdict
# engine. Per-level proximity (broke nearest structural support today)
# would need MIP.APP.STRUCTURAL_LEVEL_CACHE level lookup; that rule is
# intentionally deferred — current implementation is bounded and
# deterministic on % move + verdict cushion.
_DAILY_CONTEXT_SQL = """
    SELECT
        v.POSITION_EPISODE_KEY,
        v.SYMBOL,
        v.VERDICT                       AS REAL_VERDICT,
        v.HEALTH_STATE                  AS REAL_HEALTH_STATE,
        v.DISTANCE_TO_INVALIDATION_PCT
    FROM MIP.MART.V_DAILY_POSITION_VERDICT_LATEST v
    WHERE v.PORTFOLIO_ID = %(portfolio_id)s
"""


# --- Internal helpers -------------------------------------------------------


def _query_rows(sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            return fetch_all(cur)
        finally:
            cur.close()
    finally:
        conn.close()


def _today_metrics(
    symbol_bars: SymbolBars,
) -> Tuple[
    Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]
]:
    """Return (today_change_pct, today_low, today_high, last_price, today_open)."""
    bars = symbol_bars.bars or []
    if not bars:
        return None, None, None, None, None
    opens = [b.open for b in bars if b.open is not None]
    closes = [b.close for b in bars if b.close is not None]
    lows = [b.low for b in bars if b.low is not None]
    highs = [b.high for b in bars if b.high is not None]
    if not opens or not closes:
        return None, None, None, None, None
    sess_open = opens[0]
    last_close = closes[-1]
    pct = None
    if sess_open and sess_open != 0:
        pct = (last_close - sess_open) / sess_open
    today_low = min(lows) if lows else None
    today_high = max(highs) if highs else None
    return pct, today_low, today_high, last_close, sess_open


def _bars_to_dicts(symbol_bars: SymbolBars) -> Tuple[Dict[str, Any], ...]:
    """JSON-friendly dicts for the cockpit chart."""
    out: List[Dict[str, Any]] = []
    for b in symbol_bars.bars or []:
        out.append(
            {
                "ts": b.ts,
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
                "volume": b.volume,
            }
        )
    return tuple(out)


def _classify(
    *,
    pct: Optional[float],
    distance_to_invalidation_pct: Optional[float],
    real_verdict: Optional[str],
) -> Tuple[str, str, str, str]:
    """
    Apply deterministic rules. Returns
    (intraday_status, intraday_action, intraday_reason, intraday_summary).

    Caller must use the UNAVAILABLE / MARKET_CLOSED branches before
    invoking _classify; this function assumes pct is not None and bars
    exist.
    """
    rv = (real_verdict or "").upper()
    cushion_thin = (
        distance_to_invalidation_pct is not None
        and distance_to_invalidation_pct < _THIN_CUSHION_PCT
    )

    # --- BREAKING / SELL_NOW: hard adverse move stacked on a daily
    # verdict that is already cautious.
    if pct is not None and pct <= _HARD_BREAK_PCT and rv in {"WATCH", "EXIT_REVIEW"}:
        return (
            "BREAKING",
            "SELL_NOW",
            "DAY_DOWN_HARD_ON_CAUTIOUS_VERDICT",
            f"Today down {pct*100:.1f}% from open with daily verdict {rv}. Manual exit advisable.",
        )

    # --- WEAKENING / REVIEW_NOW
    if pct is not None and pct <= _SOFT_BREAK_PCT:
        if cushion_thin:
            return (
                "WEAKENING",
                "REVIEW_NOW",
                "APPROACHING_INVALIDATION",
                f"Today down {pct*100:.1f}% with thin cushion to invalidation — review now.",
            )
        return (
            "WEAKENING",
            "REVIEW_NOW",
            "DAY_DOWN_HARD",
            f"Today down {pct*100:.1f}% from open — review now.",
        )
    if cushion_thin and pct is not None and pct < 0:
        return (
            "WEAKENING",
            "REVIEW_NOW",
            "APPROACHING_INVALIDATION",
            "Cushion to invalidation is thin and price drifting lower — review now.",
        )

    # --- WATCH
    if pct is not None and pct <= _WATCH_PCT:
        return (
            "NEUTRAL",
            "WATCH_NOW",
            "MILD_WEAKNESS",
            f"Mild weakness — today {pct*100:.1f}% from open.",
        )

    # --- CONSTRUCTIVE
    if pct is not None and pct >= _UP_PCT:
        return (
            "CONSTRUCTIVE",
            "HOLD",
            "RECOVERING",
            f"Today constructive — {pct*100:+.1f}% from open.",
        )

    # --- NEUTRAL HOLD
    return (
        "NEUTRAL",
        "HOLD",
        "STABLE",
        "Today stable — no material intraday concern.",
    )


def _row_unavailable(
    *, position_episode_key: str, portfolio_id: int, symbol: str, eval_ts: str, why: str
) -> IntradayOverlayRow:
    return IntradayOverlayRow(
        position_episode_key=position_episode_key,
        portfolio_id=portfolio_id,
        symbol=symbol,
        intraday_status="UNAVAILABLE",
        intraday_action="NO_LIVE_CHECK",
        intraday_reason="NO_LIVE_DATA",
        intraday_summary=why,
        intraday_evaluated_ts=eval_ts,
    )


def _row_market_closed(
    *, position_episode_key: str, portfolio_id: int, symbol: str, eval_ts: str
) -> IntradayOverlayRow:
    return IntradayOverlayRow(
        position_episode_key=position_episode_key,
        portfolio_id=portfolio_id,
        symbol=symbol,
        intraday_status="NEUTRAL",
        intraday_action="NO_LIVE_CHECK",
        intraday_reason="MARKET_CLOSED",
        intraday_summary="Market closed — no current-day intraday check.",
        intraday_evaluated_ts=eval_ts,
    )


# --- Public entry point -----------------------------------------------------


def get_intraday_overlay(portfolio_id: int, *, force_refresh: bool = False) -> IntradayOverlayResult:
    """
    Build the cockpit intraday overlay for a portfolio.

    Always returns a structured result; never raises on TWS / Snowflake
    errors. Cached for ~60s per portfolio.
    """
    if not force_refresh:
        cached = _cache_get(portfolio_id)
        if cached is not None:
            return cached

    eval_ts = datetime.now(timezone.utc).isoformat()

    # 1) Open positions (Snowflake). If this fails, return UNAVAILABLE.
    try:
        open_rows = _query_rows(_OPEN_POSITIONS_SQL, {"portfolio_id": int(portfolio_id)})
    except Exception as exc:
        logger.warning("intraday_overlay: open-position query failed: %s", exc)
        result = IntradayOverlayResult(
            overlay_status="UNAVAILABLE",
            latest_eval_ts=eval_ts,
            rows=[],
            note=f"open_positions_query_failed: {exc}",
        )
        _cache_put(portfolio_id, result)
        return result

    if not open_rows:
        result = IntradayOverlayResult(
            overlay_status="OK",
            latest_eval_ts=eval_ts,
            rows=[],
        )
        _cache_put(portfolio_id, result)
        return result

    # 2) Daily context (best-effort — missing rows just mean fewer rules fire).
    try:
        ctx_rows = _query_rows(_DAILY_CONTEXT_SQL, {"portfolio_id": int(portfolio_id)})
    except Exception as exc:
        logger.warning("intraday_overlay: daily context query failed: %s", exc)
        ctx_rows = []
    ctx_by_symbol: Dict[str, Dict[str, Any]] = {}
    for r in ctx_rows:
        sym = str(r.get("SYMBOL") or "").upper()
        if sym:
            ctx_by_symbol[sym] = r

    # 3) Fetch today's 15m bars (subprocess; fail-soft).
    symbols = sorted({str(r.get("SYMBOL") or "").upper() for r in open_rows if r.get("SYMBOL")})
    bars: IntradayBarsResult = fetch_today_15m_bars(symbols)

    # 4) Decide top-level overlay_status before per-row classification
    #    so we never silently map UNAVAILABLE to "Today OK".
    if bars.status == "UNAVAILABLE":
        overlay_status = "UNAVAILABLE"
    else:
        # OK / PARTIAL: determine if every symbol's bar list is empty
        # (= MARKET_CLOSED) or if some have data.
        any_today_data = any(
            sb.bars for sb in bars.symbols.values() if isinstance(sb, SymbolBars)
        )
        if not any_today_data:
            overlay_status = "MARKET_CLOSED"
        elif bars.status == "OK":
            overlay_status = "OK"
        else:
            overlay_status = "PARTIAL"

    rows: List[IntradayOverlayRow] = []
    for r in open_rows:
        sym = str(r.get("SYMBOL") or "").upper()
        episode_key = str(r.get("POSITION_EPISODE_KEY") or "")
        pid = int(r.get("PORTFOLIO_ID") or portfolio_id)

        if overlay_status == "UNAVAILABLE":
            rows.append(
                _row_unavailable(
                    position_episode_key=episode_key,
                    portfolio_id=pid,
                    symbol=sym,
                    eval_ts=eval_ts,
                    why="Live intraday data unavailable (TWS unreachable or timed out).",
                )
            )
            continue

        sb = bars.symbols.get(sym)
        if not sb or sb.status == "FAILED" or not sb.bars:
            if overlay_status == "MARKET_CLOSED":
                rows.append(
                    _row_market_closed(
                        position_episode_key=episode_key,
                        portfolio_id=pid,
                        symbol=sym,
                        eval_ts=eval_ts,
                    )
                )
            else:
                rows.append(
                    _row_unavailable(
                        position_episode_key=episode_key,
                        portfolio_id=pid,
                        symbol=sym,
                        eval_ts=eval_ts,
                        why="No live check — no current-day bars for this symbol.",
                    )
                )
            continue

        pct, today_low, today_high, last_price, today_open = _today_metrics(sb)
        if pct is None:
            rows.append(
                _row_unavailable(
                    position_episode_key=episode_key,
                    portfolio_id=pid,
                    symbol=sym,
                    eval_ts=eval_ts,
                    why="No live check — bar data is incomplete.",
                )
            )
            continue
        bars_dicts = _bars_to_dicts(sb)

        ctx = ctx_by_symbol.get(sym, {})
        cushion_pct_raw = ctx.get("DISTANCE_TO_INVALIDATION_PCT")
        try:
            cushion_pct = float(cushion_pct_raw) if cushion_pct_raw is not None else None
        except (TypeError, ValueError):
            cushion_pct = None

        status, action, reason, summary = _classify(
            pct=pct,
            distance_to_invalidation_pct=cushion_pct,
            real_verdict=ctx.get("REAL_VERDICT"),
        )

        rows.append(
            IntradayOverlayRow(
                position_episode_key=episode_key,
                portfolio_id=pid,
                symbol=sym,
                intraday_status=status,
                intraday_action=action,
                intraday_reason=reason,
                intraday_summary=summary,
                intraday_evaluated_ts=eval_ts,
                today_change_pct=round(pct, 5) if pct is not None else None,
                today_low=today_low,
                today_high=today_high,
                last_price=last_price,
                today_open=today_open,
                bars=bars_dicts,
            )
        )

    note = bars.error if bars.status == "UNAVAILABLE" else None
    result = IntradayOverlayResult(
        overlay_status=overlay_status,
        latest_eval_ts=eval_ts,
        rows=rows,
        note=note,
    )
    _cache_put(portfolio_id, result)
    return result


def clear_overlay_cache(portfolio_id: Optional[int] = None) -> None:
    """Test/admin hook: drop the cached overlay so the next request
    forces a fresh TWS fetch."""
    _cache_clear(portfolio_id)
