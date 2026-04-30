"""
Cockpit Trade Proposals subsection.

Surfaces the small set of CURRENT deterministic structural proposals
(typically 2-3 at a time) that are operationally relevant for the live
trading workflow, so the operator can see them inside the Live Portfolio
block without flicking to another page.

The cockpit's job here is **pre-entry monitoring**: how close is each
proposal to its entry zone *right now*? That requires a live intraday
price, not yesterday's close. So the panel pulls a live tick snapshot
from IBKR alongside today's 15-minute bars; the daily backbone is kept
only as a fallback when intraday is unavailable.

Sources (no legacy / sim / shadow):
  * Authoritative proposal rows  - MIP.APP.STRUCTURAL_TRADE_PROPOSALS
                                   filtered to STATUS = 'PROPOSED'.
  * Committee stance            - MIP.APP.COMMITTEE_FINAL_DECISION
                                   (latest committed decision); falls
                                   back to MIP.APP.COMMITTEE_HEARING
                                   when the hearing is OPEN but not yet
                                   committed.
  * Live tick + intraday chart  - IBKR via fetch_today_intraday_bars
                                   (15m RTH bars + reqMktData snapshot).
                                   This is what `current_price` and
                                   `intraday_chart_series` come from.
  * Daily backbone fallback     - MIP.MART.MARKET_BARS daily bars
                                   (INTERVAL_MINUTES = 1440). Kept on
                                   the proposal payload as
                                   `last_close` / `last_close_date` and
                                   `daily_chart_series` purely for
                                   context; not used to derive
                                   zone_status.

Filtering:
  * Skip symbols already held in V_LIVE_OPEN_POSITIONS for this
    portfolio.
  * Cap to MAX_PROPOSALS.

Derived fields (live-driven):
  * zone_status     - In zone | Near zone | Above entry | Below entry
                      | Too far | Invalidated | Live unavailable
  * entry_readiness - Ready now | Near ready | Wait | Do not enter
                      | Live unavailable

When the IBKR live fetch fails (TWS down, weekend, pre-open),
`current_price` is None, `zone_status` becomes 'LIVE_UNAVAILABLE', and
the frontend renders a per-proposal 'Live unavailable' banner instead
of stale-derived numbers.

The whole module is fail-soft: any Snowflake error returns an empty
TradeProposalsPayload(available=False, ...).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional

from app.db import fetch_all, get_connection, serialize_row

logger = logging.getLogger(__name__)


# Show at most this many proposals in the compact cockpit panel. The
# product expectation is normally 2-3; this is the safety cap.
MAX_PROPOSALS = 6

# Tail length of the mini-chart price series (daily closes).
MINI_CHART_POINTS = 30

# "Near zone" tolerance: distance from the nearest zone edge, expressed
# as a fraction of the zone width. 0.5 means within half a zone-width.
_NEAR_ZONE_FRAC = 0.5

# "Too far" threshold: distance from the zone midpoint, expressed as
# a multiple of the zone width.
_TOO_FAR_FRAC = 4.0


# --- Public types -----------------------------------------------------------


@dataclass(frozen=True)
class MiniChartPoint:
    """A single point on a proposal mini-chart.

    `kind` distinguishes daily-backbone closes from today's intraday
    bars so the frontend can style them differently (e.g. different
    line opacity, a vertical "today" divider).
    """
    ts: str          # ISO date for daily, ISO timestamp for intraday
    close: float
    kind: str = "DAILY"  # 'DAILY' | 'INTRADAY'


@dataclass(frozen=True)
class TradeProposal:
    proposal_id: int
    symbol: str
    direction: str                       # 'LONG' | 'SHORT'
    setup_family: Optional[str]
    committee_stance: Optional[str]      # APPROVE | APPROVE_REDUCED | DEFER | DENY | WAIT_RECLAIM | None
    committee_stance_source: Optional[str]  # 'final' | 'hearing' | None
    committee_confidence: Optional[float]
    entry_zone_low: Optional[float]
    entry_zone_high: Optional[float]
    invalidation_level: Optional[float]

    # Live truth (the operative numbers for the panel) -----------------
    current_price: Optional[float]       # live tick from IBKR snapshot, else None
    current_price_source: Optional[str]  # 'LIVE_TICK' | 'INTRADAY_BAR' | None
    current_price_ts: Optional[str]      # ISO timestamp of the quote / bar

    # Stale fallback (for footer text when live is unavailable) --------
    last_close: Optional[float]          # most recent daily close (prev trading day)
    last_close_date: Optional[str]       # ISO date of the close used

    # Zone derivation (live-driven; LIVE_UNAVAILABLE when no live price)
    zone_status: str                     # see helper docstring
    zone_status_label: str               # plain English
    entry_readiness: str                 # see helper
    entry_readiness_label: str           # plain English
    distance_to_zone_pct: Optional[float]

    # Per-proposal intraday status -------------------------------------
    intraday_status: str                 # 'OK' | 'EMPTY' | 'FAILED' | 'UNAVAILABLE'

    # Chart series (intraday tail when available, daily backbone always)
    mini_chart_series: List[MiniChartPoint] = field(default_factory=list)

    detail_route: Optional[str] = None
    created_at: Optional[str] = None

    # Board priority signal — comparative board rank relative to other
    # published proposals in the slate. Intentionally orthogonal to
    # entry_readiness above:
    #   priority  = "is this the strongest idea on the slate?"
    #   readiness = "is it actionable right now?"
    # The two are read independently in the UI.
    priority_rank: Optional[int] = None              # 1 = strongest; None when slate empty
    priority_band: Optional[str] = None              # 'HIGH' | 'MEDIUM' | 'LOW'
    priority_band_label: Optional[str] = None        # 'High' | 'Medium' | 'Low'
    priority_reason_code: Optional[str] = None       # board primary reason code
    priority_reason_label: Optional[str] = None      # plain English (one line)
    composite_score: Optional[float] = None          # retired deterministic composite; always None after board cutover


@dataclass(frozen=True)
class TradeProposalsPayload:
    available: bool
    total_count: int                     # before MAX_PROPOSALS cap
    intraday_overlay_status: str = "UNAVAILABLE"   # OK | PARTIAL | UNAVAILABLE | MARKET_CLOSED
    intraday_evaluated_ts: Optional[str] = None    # ISO UTC of the IBKR fetch
    proposals: List[TradeProposal] = field(default_factory=list)
    note: Optional[str] = None


# --- SQL --------------------------------------------------------------------


# Currently-relevant proposals. We keep this narrow to the structural
# pipeline output: STATUS='PROPOSED' is the active set after the daily
# pipeline has expired stale rows. We do NOT join LIVE_ACTIONS here -
# once a proposal moves into the execution queue it stops being a
# "trade proposal" and becomes either a working order or a position.
#
# Board publication columns are authoritative for ordering/explanation.
# The old deterministic composite is intentionally not recomputed here.
_PROPOSALS_SQL = """
    SELECT
        p.PROPOSAL_ID,
        p.SYMBOL,
        p.DIRECTION,
        p.SETUP_FAMILY,
        p.ENTRY_ZONE_LOW,
        p.ENTRY_ZONE_HIGH,
        p.PRICE_INVALIDATION_LEVEL,
        p.CREATED_AT,
        p.STRUCTURE_CONFIDENCE,
        p.LEVEL_SIGNIFICANCE,
        p.REGIME_COMPAT,
        p.MEANINGFUL_HIT_RATE,
        p.PATH_SURVIVAL_HIT_RATE,
        COALESCE(p.COMMITTEE_PAYLOAD:trust_label::STRING, 'RESEARCH') AS TRUST_LABEL,
        s.SETUP_DATE,
        p.BOARD_RUN_ID,
        p.BOARD_CANDIDATE_ID,
        p.BOARD_FINAL_RANK,
        p.BOARD_FINAL_VERDICT,
        p.BOARD_PRIMARY_REASON_CODE,
        p.BOARD_REASON_CODES,
        p.BOARD_RATIONALE
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
    LEFT JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS s
      ON s.SETUP_EVENT_ID = p.SETUP_EVENT_ID
    WHERE p.STATUS = 'PROPOSED'
    ORDER BY p.BOARD_FINAL_RANK NULLS LAST, p.CREATED_AT
"""

# Latest committed committee decision per proposal. We pick the most
# recent DECISION_TS via QUALIFY so we never need an extra dedup step
# in Python.
_COMMITTEE_FINAL_SQL = """
    SELECT
        PROPOSAL_ID,
        STANCE,
        CONFIDENCE,
        DECISION_TS
    FROM MIP.APP.COMMITTEE_FINAL_DECISION
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PROPOSAL_ID ORDER BY DECISION_TS DESC
    ) = 1
"""

# Latest hearing per proposal as fallback when nothing has been
# committed yet. We restrict to OPEN/CLOSED so we don't pick up
# administrative rows.
_COMMITTEE_HEARING_SQL = """
    SELECT
        PROPOSAL_ID,
        STANCE,
        CONFIDENCE,
        STATUS,
        UPDATED_AT
    FROM MIP.APP.COMMITTEE_HEARING
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PROPOSAL_ID ORDER BY UPDATED_AT DESC
    ) = 1
"""

# Symbols already held in this portfolio. We skip proposals on these
# symbols because a fresh entry signal on a symbol you already hold is
# not what the cockpit Trade Proposals subsection is for.
_OPEN_POSITION_SYMBOLS_SQL = """
    SELECT DISTINCT SYMBOL
    FROM MIP.MART.V_LIVE_OPEN_POSITIONS
    WHERE PORTFOLIO_ID = %(portfolio_id)s
"""

# Daily closes per symbol since `since`. Used to draw the compact
# entry-zone mini chart and to pick the most-recent close as the
# proposal's current_price (the structural pipeline already uses the
# 1440m bar as its current_price reference, so this stays consistent).
_DAILY_BARS_SINCE_SQL = """
    SELECT
        SYMBOL,
        DATE(TS) AS BAR_DATE,
        CLOSE
    FROM MIP.MART.MARKET_BARS
    WHERE INTERVAL_MINUTES = 1440
      AND SYMBOL IN ({symbols})
      AND TS >= %(since)s
    ORDER BY SYMBOL, TS
"""


# --- Internal helpers -------------------------------------------------------


def _query(sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            return [serialize_row(r) for r in fetch_all(cur)]
        finally:
            cur.close()
    finally:
        conn.close()


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# --- Zone status / readiness rules -----------------------------------------


def _zone_status(
    *,
    direction: str,
    current: Optional[float],
    entry_low: Optional[float],
    entry_high: Optional[float],
    invalidation: Optional[float],
) -> tuple[str, str, Optional[float]]:
    """
    Classify where `current` sits relative to the entry zone and the
    invalidation level.

    Returns (status_code, status_label, distance_to_zone_pct).
      * distance_to_zone_pct is signed: 0 inside zone, negative when
        price is below the zone, positive when above. Expressed as
        decimal fraction of the zone-edge price (so 0.012 == 1.2%).

    Direction-aware:
      LONG  - invalidation is *below* the zone, so price below
              invalidation = "Invalidated".
      SHORT - invalidation is *above* the zone, so price above
              invalidation = "Invalidated".
    """
    if entry_low is None or entry_high is None:
        return "UNKNOWN", "No zone", None
    if current is None:
        # Live data unavailable — frontend renders the "Live unavailable"
        # banner instead of stale-derived numbers.
        return "LIVE_UNAVAILABLE", "Live unavailable", None

    lo = min(entry_low, entry_high)
    hi = max(entry_low, entry_high)
    width = max(hi - lo, 1e-9)
    mid = (lo + hi) / 2.0
    direction_u = (direction or "").upper()

    # Distance to nearest zone edge, signed. Below zone → negative.
    if current < lo:
        dist_pct = (current - lo) / lo if lo > 0 else None
    elif current > hi:
        dist_pct = (current - hi) / hi if hi > 0 else None
    else:
        dist_pct = 0.0

    if invalidation is not None:
        if direction_u == "LONG" and current <= invalidation:
            return "INVALIDATED", "Invalidated", dist_pct
        if direction_u == "SHORT" and current >= invalidation:
            return "INVALIDATED", "Invalidated", dist_pct

    if lo <= current <= hi:
        return "IN_ZONE", "In zone", 0.0

    near_threshold = width * _NEAR_ZONE_FRAC
    far_threshold = width * _TOO_FAR_FRAC

    if abs(current - mid) > far_threshold:
        return "TOO_FAR", "Too far", dist_pct

    edge_distance = (lo - current) if current < lo else (current - hi)
    if edge_distance <= near_threshold:
        return "NEAR_ZONE", "Near zone", dist_pct

    if direction_u == "LONG":
        # Long entries: price below the zone is the natural "still
        # waiting for pullback" path; price above is "missed entry".
        return ("BELOW_ENTRY", "Below entry", dist_pct) if current < lo \
            else ("ABOVE_ENTRY", "Above entry", dist_pct)

    if direction_u == "SHORT":
        # Short entries: zone sits above current price most of the
        # time; price below the zone means the trade has already moved
        # away from us, and price above means we're still waiting.
        return ("BELOW_ENTRY", "Below entry", dist_pct) if current < lo \
            else ("ABOVE_ENTRY", "Above entry", dist_pct)

    return "UNKNOWN", "No reference", dist_pct


def _entry_readiness(
    *,
    zone_status: str,
    committee_stance: Optional[str],
) -> tuple[str, str]:
    """
    Combine committee stance with zone status into a single
    plain-English readiness signal.

    Stance precedence (committee wins when it says no):
      DENY / WAIT_RECLAIM / Invalidated  - "Do not enter"
      DEFER + not-in-zone                - "Wait"
      DEFER + In zone                    - "Wait"  (committee asked to wait)
      APPROVE / APPROVE_REDUCED + In zone   - "Ready now"
      APPROVE / APPROVE_REDUCED + Near zone - "Near ready"
      everything else                    - "Wait"
    """
    stance = (committee_stance or "").upper() or None
    zs = zone_status

    if zs == "LIVE_UNAVAILABLE":
        return "LIVE_UNAVAILABLE", "Live unavailable"

    if zs == "INVALIDATED":
        return "DO_NOT_ENTER", "Do not enter"

    if stance == "DENY":
        return "DO_NOT_ENTER", "Do not enter"
    if stance == "WAIT_RECLAIM":
        return "DO_NOT_ENTER", "Do not enter"

    if stance in ("APPROVE", "APPROVE_REDUCED"):
        if zs == "IN_ZONE":
            return "READY_NOW", "Ready now"
        if zs == "NEAR_ZONE":
            return "NEAR_READY", "Near ready"
        return "WAIT", "Wait"

    if stance == "DEFER":
        return "WAIT", "Wait"

    # No committee verdict yet (e.g. hearing not opened or final not
    # committed). Be conservative.
    if zs in ("IN_ZONE", "NEAR_ZONE"):
        return "WAIT", "Wait"
    return "WAIT", "Wait"


# --- Loaders ---------------------------------------------------------------


def _load_proposal_rows() -> List[Dict[str, Any]]:
    return _query(_PROPOSALS_SQL, {})


def _load_committee_stance() -> Dict[int, Dict[str, Any]]:
    final: Dict[int, Dict[str, Any]] = {}
    try:
        for r in _query(_COMMITTEE_FINAL_SQL, {}):
            pid = r.get("PROPOSAL_ID")
            if pid is None:
                continue
            try:
                final[int(pid)] = {
                    "stance": r.get("STANCE"),
                    "confidence": _f(r.get("CONFIDENCE")),
                    "source": "final",
                }
            except (TypeError, ValueError):
                continue
    except Exception as exc:
        logger.warning("trade_proposals: final-decision query failed: %s", exc)

    try:
        hearings = _query(_COMMITTEE_HEARING_SQL, {})
    except Exception as exc:
        logger.warning("trade_proposals: hearing query failed: %s", exc)
        hearings = []
    for r in hearings:
        pid = r.get("PROPOSAL_ID")
        if pid is None:
            continue
        try:
            ipid = int(pid)
        except (TypeError, ValueError):
            continue
        if ipid in final:
            continue  # final wins
        stance = r.get("STANCE")
        if not stance:
            continue
        final[ipid] = {
            "stance": stance,
            "confidence": _f(r.get("CONFIDENCE")),
            "source": "hearing",
        }
    return final


def _load_open_position_symbols(portfolio_id: int) -> set:
    try:
        rows = _query(_OPEN_POSITION_SYMBOLS_SQL, {"portfolio_id": int(portfolio_id)})
    except Exception as exc:
        logger.warning("trade_proposals: open-positions query failed: %s", exc)
        return set()
    return {str(r.get("SYMBOL") or "").upper() for r in rows if r.get("SYMBOL")}


def _load_daily_bars(symbols: List[str]) -> Dict[str, List[MiniChartPoint]]:
    """Tail of MINI_CHART_POINTS daily closes per symbol."""
    if not symbols:
        return {}
    placeholders = ", ".join([f"%(s{i})s" for i in range(len(symbols))])
    # Pull a generous window so we always have at least MINI_CHART_POINTS
    # closes after weekend gaps; we tail in Python.
    params: Dict[str, Any] = {"since": _since_for_chart()}
    for i, s in enumerate(symbols):
        params[f"s{i}"] = s
    sql = _DAILY_BARS_SINCE_SQL.format(symbols=placeholders)
    try:
        rows = _query(sql, params)
    except Exception as exc:
        logger.warning("trade_proposals: daily-bars query failed: %s", exc)
        return {}

    by_symbol: Dict[str, List[MiniChartPoint]] = {}
    for r in rows:
        sym = str(r.get("SYMBOL") or "").upper()
        bd = r.get("BAR_DATE")
        cl = _f(r.get("CLOSE"))
        if not sym or bd is None or cl is None:
            continue
        by_symbol.setdefault(sym, []).append(
            MiniChartPoint(ts=str(bd)[:10], close=cl, kind="DAILY")
        )

    # Tail to MINI_CHART_POINTS most recent closes per symbol.
    return {s: pts[-MINI_CHART_POINTS:] for s, pts in by_symbol.items()}


def _since_for_chart() -> str:
    """ISO date 60 days ago - generous enough to absorb weekends / holidays."""
    from datetime import date, timedelta
    return (date.today() - timedelta(days=60)).isoformat()


# --- Live intraday fetch ----------------------------------------------------


@dataclass
class _IntradayFetch:
    """Per-call result of the IBKR live fetch (fail-soft)."""
    overlay_status: str
    evaluated_ts: Optional[str]
    by_symbol: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


def _fetch_intraday_for_proposals(symbols: List[str]) -> _IntradayFetch:
    """
    Fetch today's 15-minute bars + a live tick snapshot for each
    proposal symbol. Fail-soft: any error returns
    overlay_status='UNAVAILABLE' with empty by_symbol and a populated
    `error` so the cockpit can surface the real reason in the UI.
    """
    if not symbols:
        return _IntradayFetch(overlay_status="OK", evaluated_ts=None)

    try:
        # Local import keeps this module importable even if the IBKR
        # service stack has issues (preserves the broader fail-soft
        # contract of the cockpit overview).
        from app.services.ibkr.intraday_bars import fetch_today_intraday_bars
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning(
            "trade_proposals: intraday import failed for %s: %s",
            symbols, exc,
        )
        return _IntradayFetch(
            overlay_status="UNAVAILABLE",
            evaluated_ts=None,
            error=f"intraday_import_failed: {exc}",
        )

    logger.info(
        "trade_proposals: fetching IBKR intraday for %s (live_quote=True)",
        symbols,
    )
    try:
        result = fetch_today_intraday_bars(
            symbols,
            interval_minutes=15,
            window_bars=80,
            timeout_sec=45,
            # Must be one of the registered IB host surface names
            # (live_portfolio_activity | live_intelligence | living_charts |
            # tape_observer). Trade proposals are semantically a "live
            # intelligence" surface — same classification as the
            # held-position intraday overlay.
            diagnostics_surface="live_intelligence",
            include_live_quote=True,
            snapshot_wait_sec=2.5,
        )
    except Exception as exc:
        # Promote to WARNING so default API log levels show *why*
        # the proposal panel is rendering "Live unavailable".
        logger.warning(
            "trade_proposals: intraday fetch raised: %s", exc, exc_info=True,
        )
        return _IntradayFetch(
            overlay_status="UNAVAILABLE",
            evaluated_ts=None,
            error=str(exc)[:300],
        )

    # Decide overlay-level status. If every symbol is EMPTY/FAILED with
    # no bars and no live quote, the market is closed (or TWS down).
    by_sym = result.symbols or {}
    any_bars_or_quote = False
    for sb in by_sym.values():
        if (sb.bars and len(sb.bars) > 0) or (sb.live_quote and sb.live_quote.best is not None):
            any_bars_or_quote = True
            break

    if result.status == "UNAVAILABLE":
        overlay_status = "UNAVAILABLE"
    elif not any_bars_or_quote:
        overlay_status = "MARKET_CLOSED"
    elif result.status == "OK":
        overlay_status = "OK"
    else:
        overlay_status = "PARTIAL"

    if overlay_status != "OK":
        logger.warning(
            "trade_proposals: intraday overlay_status=%s symbols=%s error=%s",
            overlay_status, symbols, getattr(result, "error", None),
        )
    else:
        logger.info(
            "trade_proposals: intraday OK (%d symbols, fetched_at=%s)",
            len(by_sym), result.fetched_at_utc,
        )

    return _IntradayFetch(
        overlay_status=overlay_status,
        evaluated_ts=result.fetched_at_utc,
        by_symbol=by_sym,
        error=getattr(result, "error", None),
    )


def _intraday_chart_points(symbol_bars: Any) -> List[MiniChartPoint]:
    """Convert IBKR intraday bars to MiniChartPoints tagged INTRADAY."""
    out: List[MiniChartPoint] = []
    if not symbol_bars or not getattr(symbol_bars, "bars", None):
        return out
    for b in symbol_bars.bars:
        cl = _f(b.close)
        if cl is None or not b.ts:
            continue
        out.append(MiniChartPoint(ts=str(b.ts), close=cl, kind="INTRADAY"))
    return out


def _resolve_live_price(symbol_bars: Any) -> tuple[Optional[float], Optional[str], Optional[str]]:
    """
    Pick (price, source, ts) for the proposal's live "Now".

    Preference order (most live → least):
      1. live_quote.best  - true tick snapshot from reqMktData
      2. latest 15m bar close - up to ~15min lag, but solid intraday
      3. None             - "Live unavailable"
    """
    if symbol_bars is None:
        return None, None, None

    lq = getattr(symbol_bars, "live_quote", None)
    if lq is not None and lq.best is not None:
        return float(lq.best), "LIVE_TICK", lq.quote_ts

    bars = getattr(symbol_bars, "bars", None) or []
    if bars:
        last = bars[-1]
        if last.close is not None:
            return float(last.close), "INTRADAY_BAR", str(last.ts)

    return None, None, None


# --- Public entry point ----------------------------------------------------


def load_trade_proposals(portfolio_id: int) -> TradeProposalsPayload:
    """
    Build the cockpit's Trade Proposals payload.

    Fail-soft: any Snowflake error returns
    TradeProposalsPayload(available=False, ...) with a short note.
    """
    try:
        rows = _load_proposal_rows()
    except Exception as exc:
        logger.warning("trade_proposals: proposal query failed: %s", exc)
        return TradeProposalsPayload(
            available=False,
            total_count=0,
            note=f"proposals_query_failed: {exc}",
        )

    if not rows:
        return TradeProposalsPayload(available=True, total_count=0)

    held_symbols = _load_open_position_symbols(int(portfolio_id))

    # Skip proposals on symbols already held; a duplicate entry signal
    # is noise, not an actionable proposal in this dashboard.
    actionable_rows = [
        r for r in rows
        if str(r.get("SYMBOL") or "").upper() not in held_symbols
    ]

    total_count = len(actionable_rows)
    actionable_rows = actionable_rows[:MAX_PROPOSALS]

    if not actionable_rows:
        note = (
            "All current proposals are on symbols you already hold."
            if rows else None
        )
        return TradeProposalsPayload(available=True, total_count=0, note=note)

    stances = _load_committee_stance()
    symbols = sorted({str(r.get("SYMBOL") or "").upper() for r in actionable_rows})
    daily_bars_by_symbol = _load_daily_bars(symbols)

    # Fetch live intraday bars + tick snapshots for the proposal symbols.
    # Fail-soft: any error → overlay_status='UNAVAILABLE' and proposals
    # render with the "Live unavailable" banner.
    intraday = _fetch_intraday_for_proposals(symbols)

    proposals: List[TradeProposal] = []
    for r in actionable_rows:
        try:
            pid = int(r.get("PROPOSAL_ID"))
        except (TypeError, ValueError):
            continue
        symbol = str(r.get("SYMBOL") or "").upper()
        direction = str(r.get("DIRECTION") or "").upper() or "LONG"
        entry_low = _f(r.get("ENTRY_ZONE_LOW"))
        entry_high = _f(r.get("ENTRY_ZONE_HIGH"))
        invalidation = _f(r.get("PRICE_INVALIDATION_LEVEL"))

        # --- Daily backbone (for fallback footer + mini-chart context) -
        daily_pts = daily_bars_by_symbol.get(symbol, [])
        last_close = daily_pts[-1].close if daily_pts else None
        last_close_date = daily_pts[-1].ts if daily_pts else None

        # --- Live tick + today's intraday bars -------------------------
        sb = intraday.by_symbol.get(symbol)
        current_price, price_source, price_ts = _resolve_live_price(sb)
        intraday_bars = _intraday_chart_points(sb)

        if sb is None:
            per_proposal_intraday_status = "UNAVAILABLE"
        elif sb.status == "FAILED":
            per_proposal_intraday_status = "FAILED"
        elif intraday_bars or current_price is not None:
            per_proposal_intraday_status = "OK"
        else:
            per_proposal_intraday_status = "EMPTY"

        # --- Zone status / readiness (live-driven) ---------------------
        zone_code, zone_label, dist_pct = _zone_status(
            direction=direction,
            current=current_price,
            entry_low=entry_low,
            entry_high=entry_high,
            invalidation=invalidation,
        )

        stance_info = stances.get(pid) or {}
        stance = stance_info.get("stance")

        readiness_code, readiness_label = _entry_readiness(
            zone_status=zone_code,
            committee_stance=stance,
        )

        # --- Chart series ---------------------------------------------
        # Always include the daily backbone for context, then append
        # today's intraday bars (when available) so the chart shows
        # price approaching the zone in real time.
        chart_series: List[MiniChartPoint] = []
        chart_series.extend(daily_pts)
        chart_series.extend(intraday_bars)

        proposals.append(
            TradeProposal(
                proposal_id=pid,
                symbol=symbol,
                direction=direction,
                setup_family=r.get("SETUP_FAMILY"),
                committee_stance=stance,
                committee_stance_source=stance_info.get("source"),
                committee_confidence=stance_info.get("confidence"),
                entry_zone_low=entry_low,
                entry_zone_high=entry_high,
                invalidation_level=invalidation,
                current_price=current_price,
                current_price_source=price_source,
                current_price_ts=price_ts,
                last_close=last_close,
                last_close_date=last_close_date,
                zone_status=zone_code,
                zone_status_label=zone_label,
                entry_readiness=readiness_code,
                entry_readiness_label=readiness_label,
                distance_to_zone_pct=(
                    round(dist_pct, 5) if dist_pct is not None else None
                ),
                intraday_status=per_proposal_intraday_status,
                mini_chart_series=chart_series,
                detail_route=f"/structural-market-timeline?symbol={symbol}",
                created_at=str(r.get("CREATED_AT")) if r.get("CREATED_AT") else None,
                priority_rank=r.get("BOARD_FINAL_RANK"),
                priority_band=r.get("BOARD_FINAL_VERDICT"),
                priority_band_label=r.get("BOARD_FINAL_VERDICT"),
                priority_reason_code=r.get("BOARD_PRIMARY_REASON_CODE"),
                priority_reason_label=r.get("BOARD_RATIONALE"),
                composite_score=None,
            )
        )

    note: Optional[str] = None
    if intraday.overlay_status == "UNAVAILABLE" and intraday.error:
        note = f"Live fetch unavailable: {intraday.error}"
    elif intraday.overlay_status == "MARKET_CLOSED":
        note = "Market closed — showing previous close only."

    return TradeProposalsPayload(
        available=True,
        total_count=total_count,
        intraday_overlay_status=intraday.overlay_status,
        intraday_evaluated_ts=intraday.evaluated_ts,
        proposals=proposals,
        note=note,
    )


def compute_proposal_priority_context(proposal_id: int) -> Optional[Dict[str, Any]]:
    """Look up `proposal_id` in the current ranked slate.

    Returns a small dict suitable for showing a "Priority #N of M" pill
    on a per-proposal detail page (LPA Committee 2 Exhibits masthead),
    or None when the proposal is not in the active slate (already
    cancelled / executed / expired, or stale id).

    Slate definition for this lookup is intentionally *all* PROPOSED
    rows — not the cockpit's held-symbol-filtered slate — because the
    operator looking at one proposal wants to know "how does this rank
    against everything in flight right now", not "against what I haven't
    already taken".

    Fail-soft: any DB error returns None and logs a warning, so the
    LPA page can render without the priority pill rather than failing.
    """
    try:
        rows = _load_proposal_rows()
    except Exception as exc:
        logger.warning("priority_context: proposal query failed: %s", exc)
        return None
    if not rows:
        return None
    total = len(rows)
    target_pid = int(proposal_id)
    for r in rows:
        try:
            pid = int(r.get("PROPOSAL_ID") or 0)
        except (TypeError, ValueError):
            continue
        if pid == target_pid:
            return {
                "proposal_id":           pid,
                "priority_rank":         r.get("BOARD_FINAL_RANK"),
                "total":                 total,
                "priority_band":         r.get("BOARD_FINAL_VERDICT"),
                "priority_band_label":   r.get("BOARD_FINAL_VERDICT"),
                "priority_reason_code":  r.get("BOARD_PRIMARY_REASON_CODE"),
                "priority_reason_label": r.get("BOARD_RATIONALE"),
                "composite_score":       None,
                "in_slate":              True,
            }
    return {
        "proposal_id": target_pid,
        "total":       total,
        "in_slate":    False,
    }


def to_payload_dict(payload: TradeProposalsPayload) -> Dict[str, Any]:
    """Serialise to a JSON-friendly dict for FastAPI."""
    return {
        "available": payload.available,
        "total_count": payload.total_count,
        "intraday_overlay_status": payload.intraday_overlay_status,
        "intraday_evaluated_ts": payload.intraday_evaluated_ts,
        "note": payload.note,
        "proposals": [
            {
                "proposal_id": p.proposal_id,
                "symbol": p.symbol,
                "direction": p.direction,
                "setup_family": p.setup_family,
                "committee_stance": p.committee_stance,
                "committee_stance_source": p.committee_stance_source,
                "committee_confidence": p.committee_confidence,
                "entry_zone_low": p.entry_zone_low,
                "entry_zone_high": p.entry_zone_high,
                "invalidation_level": p.invalidation_level,
                "current_price": p.current_price,
                "current_price_source": p.current_price_source,
                "current_price_ts": p.current_price_ts,
                "last_close": p.last_close,
                "last_close_date": p.last_close_date,
                "zone_status": p.zone_status,
                "zone_status_label": p.zone_status_label,
                "entry_readiness": p.entry_readiness,
                "entry_readiness_label": p.entry_readiness_label,
                "distance_to_zone_pct": p.distance_to_zone_pct,
                "intraday_status": p.intraday_status,
                "mini_chart_series": [
                    {"ts": pt.ts, "close": pt.close, "kind": pt.kind}
                    for pt in p.mini_chart_series
                ],
                "detail_route": p.detail_route,
                "created_at": p.created_at,
                "priority_rank": p.priority_rank,
                "priority_band": p.priority_band,
                "priority_band_label": p.priority_band_label,
                "priority_reason_code": p.priority_reason_code,
                "priority_reason_label": p.priority_reason_label,
                "composite_score": p.composite_score,
            }
            for p in payload.proposals
        ],
    }
