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

    # Phase 2 Sprint 4 / option 4a — same-symbol multiplicity warning.
    # When the active slate carries more than one PROPOSED row on the
    # same SYMBOL (legitimate when two distinct setup events fire on
    # the same name, sometimes in opposite directions), the cockpit
    # shows a non-blocking badge so the operator notices and can pick
    # one. The board itself is unchanged — same-symbol is allowed,
    # not deduplicated. Counts/directions exclude this proposal.
    same_symbol_other_active: bool = False
    same_symbol_other_count: int = 0
    same_symbol_other_directions: List[str] = field(default_factory=list)
    same_symbol_other_proposal_ids: List[int] = field(default_factory=list)

    # Phase 4 thesis-health surfacing (UI-only; never affects trading).
    # Populated when the latest Phase 4 board has a verdict for this
    # proposal — preferentially the verdict whose prior_thesis_reference
    # explicitly points at this PROPOSAL_ID (linkage = PRIOR_THESIS_MATCH),
    # otherwise the most recent symbol-level verdict (linkage =
    # SYMBOL_LATEST_ONLY). None when no Phase 4 data exists.
    phase4_health: Optional[Dict[str, Any]] = None


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
# Latest-board-run lineage gate (Patch Group A, post-Phase-3 operator
# safety): we additionally require the proposal's BOARD_RUN_ID to match
# MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN.RUN_ID. This protects the
# cockpit even when SP_EXPIRE_STALE_DAILY_PROPOSALS has not yet run -
# any leaked stale PROPOSED row simply doesn't appear in the panel
# instead of confusingly competing with the canonical slate.
#
# Fail-closed: when the view is empty (cold start, board outage), the
# JOIN drops every row and the panel renders empty rather than showing
# stale rows that could be misread as actionable.
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
    JOIN MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN latest
      ON latest.RUN_ID = p.BOARD_RUN_ID
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

# Phase 4 thesis-health lineage surfacing (UI-only; never affects
# trading). Two queries, both bounded to recent runs to keep the
# scan tight. We then dispatch in Python:
#   1) Prefer the latest verdict whose CHAIR_OUTPUT_JSON.prior_thesis_reference
#      .proposal_id matches the active cockpit proposal — this is the
#      "lineage match" case (e.g. WATCH_LONG_FAILURE referencing
#      proposal #2803).
#   2) Otherwise fall back to the latest symbol-level verdict and tag
#      it linkage='SYMBOL_LATEST_ONLY' so the UI can show "(symbol-level)".
#
# Lookback (14 days) mirrors the recall window used by the
# WATCH_LONG_FAILURE rule in the Chair prompt — older verdicts are not
# considered representative of the current thesis.
_PH4_LOOKBACK_DAYS = 14

# Latest Phase 4 verdict per PRIOR_PROPOSAL_ID (lineage-match path).
_PHASE4_LINKED_SQL = """
    SELECT
        tv.CHAIR_OUTPUT_JSON:prior_thesis_reference:proposal_id::INT       AS PRIOR_PROPOSAL_ID,
        tv.SYMBOL,
        tv.RUN_ID,
        tv.DOSSIER_ID,
        tv.FINAL_ACTION,
        tv.FINAL_DIRECTION,
        tv.FINAL_THESIS,
        tv.PRIMARY_REASON_CODE,
        tv.CHAIR_OUTPUT_JSON:thesis_health::STRING                        AS THESIS_HEALTH,
        tv.CHAIR_OUTPUT_JSON:prior_thesis_reference                        AS PRIOR_THESIS_REF,
        tv.CHAIR_OUTPUT_JSON:actionability_summary:continuation_quality::STRING       AS CONTINUATION_QUALITY,
        tv.CHAIR_OUTPUT_JSON:actionability_summary:resistance_overhead_risk::STRING   AS RESISTANCE_OVERHEAD_RISK,
        tv.CHAIR_OUTPUT_JSON:actionability_summary:recent_cluster::STRING             AS RECENT_CLUSTER,
        tv.CHAIR_OUTPUT_JSON:actionability_summary:current_range_position_pct::FLOAT  AS CURRENT_RANGE_POSITION_PCT,
        tv.CREATED_AT                                                      AS RUN_AT,
        ds.AS_OF_DATE                                                      AS AS_OF_DATE,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_price::FLOAT  AS BROKEN_R_LEVEL,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_low::FLOAT    AS BROKEN_R_ZONE_LOW,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_high::FLOAT   AS BROKEN_R_ZONE_HIGH,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:confidence::FLOAT   AS BROKEN_R_CONFIDENCE,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:role::STRING        AS BROKEN_R_ROLE,
        ds.DOSSIER_PAYLOAD_JSON:levels:nearest_support:level_price::FLOAT     AS NEAREST_SUPPORT,
        ds.DOSSIER_PAYLOAD_JSON:levels:nearest_resistance:level_price::FLOAT  AS NEAREST_RESISTANCE
    FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT tv
    JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT ds
      ON ds.RUN_ID = tv.RUN_ID AND ds.DOSSIER_ID = tv.DOSSIER_ID
    WHERE tv.MARKET_TYPE = 'STOCK'
      AND tv.CREATED_AT >= DATEADD('day', -%(lookback_days)s, CURRENT_TIMESTAMP())
      AND tv.CHAIR_OUTPUT_JSON:prior_thesis_reference:proposal_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tv.CHAIR_OUTPUT_JSON:prior_thesis_reference:proposal_id::INT
        ORDER BY tv.CREATED_AT DESC
    ) = 1
"""

# Latest Phase 4 verdict per SYMBOL (fallback path).
_PHASE4_LATEST_BY_SYMBOL_SQL = """
    SELECT
        tv.SYMBOL,
        tv.RUN_ID,
        tv.DOSSIER_ID,
        tv.FINAL_ACTION,
        tv.FINAL_DIRECTION,
        tv.FINAL_THESIS,
        tv.PRIMARY_REASON_CODE,
        tv.CHAIR_OUTPUT_JSON:thesis_health::STRING                        AS THESIS_HEALTH,
        tv.CHAIR_OUTPUT_JSON:prior_thesis_reference                        AS PRIOR_THESIS_REF,
        tv.CHAIR_OUTPUT_JSON:actionability_summary:continuation_quality::STRING       AS CONTINUATION_QUALITY,
        tv.CHAIR_OUTPUT_JSON:actionability_summary:resistance_overhead_risk::STRING   AS RESISTANCE_OVERHEAD_RISK,
        tv.CHAIR_OUTPUT_JSON:actionability_summary:recent_cluster::STRING             AS RECENT_CLUSTER,
        tv.CHAIR_OUTPUT_JSON:actionability_summary:current_range_position_pct::FLOAT  AS CURRENT_RANGE_POSITION_PCT,
        tv.CREATED_AT                                                      AS RUN_AT,
        ds.AS_OF_DATE                                                      AS AS_OF_DATE,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_price::FLOAT  AS BROKEN_R_LEVEL,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_low::FLOAT    AS BROKEN_R_ZONE_LOW,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:level_high::FLOAT   AS BROKEN_R_ZONE_HIGH,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:confidence::FLOAT   AS BROKEN_R_CONFIDENCE,
        ds.DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support:role::STRING        AS BROKEN_R_ROLE,
        ds.DOSSIER_PAYLOAD_JSON:levels:nearest_support:level_price::FLOAT     AS NEAREST_SUPPORT,
        ds.DOSSIER_PAYLOAD_JSON:levels:nearest_resistance:level_price::FLOAT  AS NEAREST_RESISTANCE
    FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT tv
    JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT ds
      ON ds.RUN_ID = tv.RUN_ID AND ds.DOSSIER_ID = tv.DOSSIER_ID
    WHERE tv.MARKET_TYPE = 'STOCK'
      AND tv.CREATED_AT >= DATEADD('day', -%(lookback_days)s, CURRENT_TIMESTAMP())
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tv.SYMBOL ORDER BY tv.CREATED_AT DESC) = 1
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


def _build_same_symbol_map(rows: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """For each PROPOSED row, list other PROPOSED rows with the same
    SYMBOL.

    Phase 2 Sprint 4 / option 4a — non-blocking same-symbol warning.
    Counts/directions/ids are restricted to the *other* rows so the
    UI badge speaks in the operator's voice ("AAPL — 2 other active
    proposals (LONG, SHORT)").

    The board itself is unchanged: same-symbol candidates are allowed
    through, the UI just makes them visible.

    Returns: {proposal_id: {"count": int, "directions": [str],
                              "proposal_ids": [int]}}
    """
    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        sym = str(r.get("SYMBOL") or "").upper()
        if not sym:
            continue
        by_symbol.setdefault(sym, []).append(r)

    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        try:
            pid = int(r.get("PROPOSAL_ID"))
        except (TypeError, ValueError):
            continue
        sym = str(r.get("SYMBOL") or "").upper()
        if not sym:
            continue
        siblings = [
            s for s in by_symbol.get(sym, [])
            if s.get("PROPOSAL_ID") not in (None, r.get("PROPOSAL_ID"))
        ]
        if not siblings:
            continue
        # De-dup directions while preserving order so the UI shows
        # "LONG, SHORT" not "LONG, LONG, SHORT".
        seen_dirs: List[str] = []
        for s in siblings:
            d = str(s.get("DIRECTION") or "").upper()
            if d and d not in seen_dirs:
                seen_dirs.append(d)
        sibling_ids: List[int] = []
        for s in siblings:
            try:
                sibling_ids.append(int(s.get("PROPOSAL_ID")))
            except (TypeError, ValueError):
                continue
        out[pid] = {
            "count": len(siblings),
            "directions": seen_dirs,
            "proposal_ids": sibling_ids,
        }
    return out


def _parse_variant_json(value: Any) -> Optional[Dict[str, Any]]:
    """Best-effort decode of a Snowflake VARIANT cell.

    Snowflake VARIANT columns come back from the Python connector as
    JSON strings; this helper tolerates that shape, an already-decoded
    dict (defensive — happens with some connector configs), and returns
    None for empty / null / unparseable input.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s or s.lower() == "null":
            return None
        try:
            import json
            obj = json.loads(s)
            if isinstance(obj, dict):
                return obj
            return None
        except Exception:  # noqa: BLE001
            return None
    return None


def _build_phase4_health_payload(row: Dict[str, Any], linkage: str) -> Dict[str, Any]:
    """Convert a Snowflake `_PHASE4_*_SQL` row into the JSON payload
    consumed by the cockpit ProposalRow.

    `linkage` is one of `PRIOR_THESIS_MATCH` | `SYMBOL_LATEST_ONLY` and
    drives the UI's "(symbol-level)" suffix.
    """
    return {
        "linkage": linkage,
        "latest_board_action": row.get("FINAL_ACTION"),
        "latest_board_direction": row.get("FINAL_DIRECTION"),
        "latest_thesis_health": row.get("THESIS_HEALTH"),
        "latest_final_thesis": row.get("FINAL_THESIS"),
        "primary_reason_code": row.get("PRIMARY_REASON_CODE"),
        "prior_thesis_ref": _parse_variant_json(row.get("PRIOR_THESIS_REF")),
        "continuation_quality": row.get("CONTINUATION_QUALITY"),
        "resistance_overhead_risk": row.get("RESISTANCE_OVERHEAD_RISK"),
        "recent_cluster": row.get("RECENT_CLUSTER"),
        "current_range_position_pct": _f(row.get("CURRENT_RANGE_POSITION_PCT")),
        "broken_resistance_level": _f(row.get("BROKEN_R_LEVEL")),
        "broken_resistance_zone_low": _f(row.get("BROKEN_R_ZONE_LOW")),
        "broken_resistance_zone_high": _f(row.get("BROKEN_R_ZONE_HIGH")),
        "broken_resistance_confidence": _f(row.get("BROKEN_R_CONFIDENCE")),
        "broken_resistance_role": row.get("BROKEN_R_ROLE"),
        "nearest_support": _f(row.get("NEAREST_SUPPORT")),
        "nearest_resistance": _f(row.get("NEAREST_RESISTANCE")),
        "as_of_date": row.get("AS_OF_DATE"),
        "run_at": row.get("RUN_AT"),
        "run_id": row.get("RUN_ID"),
    }


def _load_phase4_health(
    proposal_id_to_symbol: Dict[int, str],
) -> Dict[int, Dict[str, Any]]:
    """Resolve the latest Phase 4 thesis-health verdict for each cockpit
    proposal, lineage-aware.

    Strategy:
      1. Run `_PHASE4_LINKED_SQL` once. Every row carries a
         PRIOR_PROPOSAL_ID. If it matches an active proposal id, that's
         the lineage-match verdict (linkage = PRIOR_THESIS_MATCH).
      2. Run `_PHASE4_LATEST_BY_SYMBOL_SQL` once. For active proposals
         that did NOT find a lineage match, dispatch the symbol's
         latest verdict (linkage = SYMBOL_LATEST_ONLY).

    Both queries are bounded by `_PH4_LOOKBACK_DAYS`. Failures are
    fail-soft: an empty dict is returned and the cockpit renders without
    Phase 4 surfacing rather than failing.
    """
    if not proposal_id_to_symbol:
        return {}

    out: Dict[int, Dict[str, Any]] = {}
    params = {"lookback_days": int(_PH4_LOOKBACK_DAYS)}

    try:
        linked_rows = _query(_PHASE4_LINKED_SQL, params)
    except Exception as exc:  # noqa: BLE001
        logger.warning("phase4_health: linked query failed: %s", exc)
        linked_rows = []

    by_prior: Dict[int, Dict[str, Any]] = {}
    for r in linked_rows:
        try:
            ppid = int(r.get("PRIOR_PROPOSAL_ID"))
        except (TypeError, ValueError):
            continue
        by_prior[ppid] = r

    # First pass: lineage match.
    for pid in proposal_id_to_symbol.keys():
        match = by_prior.get(pid)
        if match is not None:
            out[pid] = _build_phase4_health_payload(match, "PRIOR_THESIS_MATCH")

    # Identify proposals that still need a fallback.
    unmatched_symbols = {
        proposal_id_to_symbol[pid]
        for pid in proposal_id_to_symbol
        if pid not in out
    }
    if not unmatched_symbols:
        return out

    try:
        latest_rows = _query(_PHASE4_LATEST_BY_SYMBOL_SQL, params)
    except Exception as exc:  # noqa: BLE001
        logger.warning("phase4_health: latest-by-symbol query failed: %s", exc)
        return out

    by_symbol: Dict[str, Dict[str, Any]] = {}
    for r in latest_rows:
        sym = str(r.get("SYMBOL") or "").upper()
        if sym:
            by_symbol[sym] = r

    for pid, sym in proposal_id_to_symbol.items():
        if pid in out:
            continue
        sym_u = sym.upper() if sym else ""
        latest = by_symbol.get(sym_u)
        if latest is not None:
            out[pid] = _build_phase4_health_payload(latest, "SYMBOL_LATEST_ONLY")

    return out


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

    # Same-symbol multiplicity is computed against the FULL slate (not
    # the held-symbol-filtered actionable subset) because the badge
    # speaks to operator awareness of competing setups, not to what
    # this particular cockpit panel chose to render.
    same_symbol_map = _build_same_symbol_map(rows)

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

    # Phase 4 thesis-health surfacing — fail-soft and lineage-aware.
    pid_symbol_map: Dict[int, str] = {}
    for r in actionable_rows:
        try:
            pid_symbol_map[int(r.get("PROPOSAL_ID"))] = str(r.get("SYMBOL") or "").upper()
        except (TypeError, ValueError):
            continue
    try:
        phase4_health_by_pid = _load_phase4_health(pid_symbol_map)
    except Exception as exc:  # noqa: BLE001
        logger.warning("trade_proposals: phase4_health load failed: %s", exc)
        phase4_health_by_pid = {}

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
                same_symbol_other_active=bool(same_symbol_map.get(pid)),
                same_symbol_other_count=int(same_symbol_map.get(pid, {}).get("count") or 0),
                same_symbol_other_directions=list(same_symbol_map.get(pid, {}).get("directions") or []),
                same_symbol_other_proposal_ids=list(same_symbol_map.get(pid, {}).get("proposal_ids") or []),
                phase4_health=phase4_health_by_pid.get(pid),
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
    same_symbol_map = _build_same_symbol_map(rows)
    for r in rows:
        try:
            pid = int(r.get("PROPOSAL_ID") or 0)
        except (TypeError, ValueError):
            continue
        if pid == target_pid:
            ss = same_symbol_map.get(pid) or {}
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
                "same_symbol_other_active":      bool(ss),
                "same_symbol_other_count":       int(ss.get("count") or 0),
                "same_symbol_other_directions":  list(ss.get("directions") or []),
                "same_symbol_other_proposal_ids": list(ss.get("proposal_ids") or []),
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
                "same_symbol_other_active": p.same_symbol_other_active,
                "same_symbol_other_count": p.same_symbol_other_count,
                "same_symbol_other_directions": p.same_symbol_other_directions,
                "same_symbol_other_proposal_ids": p.same_symbol_other_proposal_ids,
                "phase4_health": p.phase4_health,
            }
            for p in payload.proposals
        ],
    }
