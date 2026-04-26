"""
Daily Position Health V1 - per-position payload builder.

Builds a compact PositionPayload for each open position on AS_OF_DATE by:
  - reading deterministic verdict rows (DAILY_POSITION_VERDICT) so the
    shadow agent sees the same structural / regime facts
  - looking up committee baseline context via the live execution
    lineage exposed in MIP.MART.V_LIVE_OPEN_POSITIONS (PROPOSAL_ID
    sourced from MIP.LIVE.LIVE_ACTIONS, NOT from the legacy horizon
    research book MIP.APP.PORTFOLIO_TRADES)
  - computing a 20-trading-day path summary from MARKET_BARS

Inputs are read in 3 batched queries; per-position payloads are stitched
in Python to avoid N+1 round trips.

Phase 1 re-anchor (2026-04-26): horizon scoring fields
(EXPECTED_HORIZON_DAYS, HORIZON_SOURCE_CODE, TIME_EFFICIENCY) are
no longer projected into the payload or the shadow LLM prompt.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from app.db import fetch_all, get_connection

from .types import (
    CommitteeBaselineContext,
    PathSummary,
    PositionPayload,
    RealVerdictContext,
)

logger = logging.getLogger(__name__)

# 20 trading days, per spec.
DEFAULT_PATH_LOOKBACK_BARS = 20


# ---------------------------------------------------------------------------
# 1) Verdict + identity rows
# ---------------------------------------------------------------------------

_VERDICT_ROWS_SQL = """
    SELECT
        v.AS_OF_DATE,
        v.POSITION_EPISODE_KEY,
        v.PORTFOLIO_ID,
        v.EPISODE_ID,
        v.SYMBOL,
        v.SIDE,
        v.ENTRY_DATE,
        v.ENTRY_PRICE,
        v.DAYS_HELD,
        v.VERDICT,
        v.HEALTH_STATE,
        v.SEVERITY,
        v.BASELINE_QUALITY,
        v.THESIS_INTEGRITY,
        v.PATH_QUALITY,
        v.REGIME_ALIGNMENT,
        v.FRAGILITY,
        v.DISTANCE_TO_INVALIDATION_PCT,
        v.UNREALIZED_PNL_PCT,
        v.PRIMARY_REASON_CODE,
        v.OBSERVATION_SUMMARY,
        v.WHY_SUMMARY,
        v.DETAIL_JSON
    FROM MIP.APP.DAILY_POSITION_VERDICT v
    WHERE v.AS_OF_DATE = %(as_of_date)s
"""


# ---------------------------------------------------------------------------
# 2) Committee baseline (best-effort) - sourced from the live execution
#    lineage (V_LIVE_OPEN_POSITIONS.PROPOSAL_ID), NOT from PORTFOLIO_TRADES.
# ---------------------------------------------------------------------------

_BASELINE_SQL = """
    WITH live_props AS (
        SELECT
            PORTFOLIO_ID,
            SYMBOL,
            PROPOSAL_ID
        FROM MIP.MART.V_LIVE_OPEN_POSITIONS
        WHERE PROPOSAL_ID IS NOT NULL
    ),
    latest_decision AS (
        SELECT
            cfd.PROPOSAL_ID,
            cfd.STANCE,
            cfd.CONFIDENCE,
            cfd.CHAIR_OUTPUT_JSON,
            ROW_NUMBER() OVER (
                PARTITION BY cfd.PROPOSAL_ID
                ORDER BY cfd.DECISION_TS DESC
            ) AS RN
        FROM MIP.APP.COMMITTEE_FINAL_DECISION cfd
    )
    SELECT
        p.PORTFOLIO_ID,
        p.SYMBOL,
        p.PROPOSAL_ID,
        d.STANCE,
        d.CONFIDENCE,
        d.CHAIR_OUTPUT_JSON
    FROM live_props p
    LEFT JOIN latest_decision d
           ON d.PROPOSAL_ID = p.PROPOSAL_ID
          AND d.RN = 1
"""


# ---------------------------------------------------------------------------
# 3) Path bars (20 trading-day window per position)
# ---------------------------------------------------------------------------

_PATH_BARS_SQL = """
    WITH ranked AS (
        SELECT
            mb.SYMBOL,
            mb.TS::DATE AS BAR_DATE,
            mb.CLOSE,
            ROW_NUMBER() OVER (
                PARTITION BY mb.SYMBOL
                ORDER BY mb.TS DESC
            ) AS RN_DESC
        FROM MIP.MART.MARKET_BARS mb
        WHERE mb.INTERVAL_MINUTES = 1440
          AND mb.TS::DATE <= %(as_of_date)s
          AND mb.SYMBOL IN (
              SELECT DISTINCT SYMBOL
                FROM MIP.APP.DAILY_POSITION_VERDICT
               WHERE AS_OF_DATE = %(as_of_date)s
          )
    )
    SELECT SYMBOL, BAR_DATE, CLOSE
      FROM ranked
     WHERE RN_DESC <= %(lookback_bars)s
     ORDER BY SYMBOL, BAR_DATE ASC
"""


# ---------------------------------------------------------------------------
# Build payloads
# ---------------------------------------------------------------------------

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


def _safe_pct_change(numer: Optional[float], denom: Optional[float]) -> Optional[float]:
    if numer is None or denom is None or denom == 0:
        return None
    return ((numer - denom) / denom) * 100.0


def _build_path_summary(closes_for_symbol: List[Dict[str, Any]]) -> PathSummary:
    if not closes_for_symbol:
        return PathSummary()

    closes = [float(r["CLOSE"]) for r in closes_for_symbol if r.get("CLOSE") is not None]
    if not closes:
        return PathSummary()

    bars_observed = len(closes)
    up_days = 0
    comparable_days = 0
    for prev, cur in zip(closes, closes[1:]):
        comparable_days += 1
        if cur > prev:
            up_days += 1

    high_water = max(closes)
    trough = min(closes)
    first_close = closes[0]
    last_close = closes[-1]

    # Compute running drawdown from running high.
    max_dd_pct = 0.0
    running_high = closes[0]
    for c in closes:
        if c > running_high:
            running_high = c
        dd_pct = ((c - running_high) / running_high) * 100.0
        if dd_pct < max_dd_pct:
            max_dd_pct = dd_pct

    up_days_pct = (up_days / comparable_days) if comparable_days > 0 else None
    net_change_pct = _safe_pct_change(last_close, first_close)

    return PathSummary(
        bars_observed=bars_observed,
        up_days=up_days,
        comparable_days=comparable_days,
        up_days_pct=round(up_days_pct, 4) if up_days_pct is not None else None,
        high_water_close=round(high_water, 4),
        trough_close=round(trough, 4),
        first_close=round(first_close, 4),
        last_close=round(last_close, 4),
        max_drawdown_pct=round(max_dd_pct, 4),
        net_change_pct=round(net_change_pct, 4) if net_change_pct is not None else None,
        recent_5d_closes=[round(c, 4) for c in closes[-5:]],
    )


def _build_baseline_index(rows: List[Dict[str, Any]]) -> Dict[Tuple[int, str], CommitteeBaselineContext]:
    out: Dict[Tuple[int, str], CommitteeBaselineContext] = {}
    for r in rows:
        chair_text: Optional[str] = None
        chair_obj = r.get("CHAIR_OUTPUT_JSON")
        if isinstance(chair_obj, dict):
            cand = chair_obj.get("chair_summary") or chair_obj.get("summary")
            if isinstance(cand, str):
                chair_text = cand[:1000]
        out[(int(r["PORTFOLIO_ID"]), r["SYMBOL"])] = CommitteeBaselineContext(
            proposal_id=r.get("PROPOSAL_ID"),
            stance=r.get("STANCE"),
            confidence=float(r["CONFIDENCE"]) if r.get("CONFIDENCE") is not None else None,
            chair_summary=chair_text,
            has_record=True,
        )
    return out


def _build_path_index(path_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in path_rows:
        out.setdefault(r["SYMBOL"], []).append(r)
    return out


def build_position_payloads(
    as_of_date: date,
    lookback_bars: int = DEFAULT_PATH_LOOKBACK_BARS,
) -> List[PositionPayload]:
    """
    Build one PositionPayload per open position on as_of_date.
    All Snowflake reads are performed in 3 batched queries.
    """
    verdict_rows = _query_rows(_VERDICT_ROWS_SQL, {"as_of_date": as_of_date})
    if not verdict_rows:
        logger.info("position_health.payload: no DAILY_POSITION_VERDICT rows for %s", as_of_date)
        return []

    baseline_rows = _query_rows(_BASELINE_SQL, {})
    baseline_index = _build_baseline_index(baseline_rows)

    path_rows = _query_rows(
        _PATH_BARS_SQL,
        {"as_of_date": as_of_date, "lookback_bars": int(lookback_bars)},
    )
    path_index = _build_path_index(path_rows)

    payloads: List[PositionPayload] = []
    for v in verdict_rows:
        symbol = v["SYMBOL"]
        portfolio_id = int(v["PORTFOLIO_ID"])

        baseline = baseline_index.get((portfolio_id, symbol), CommitteeBaselineContext())
        path_summary = _build_path_summary(path_index.get(symbol, []))

        detail = v.get("DETAIL_JSON") or {}
        if not isinstance(detail, dict):
            detail = {}
        latest_close = detail.get("latest_close")
        structural_state_now = detail.get("structural_state_now")
        trend_regime_now = detail.get("trend_regime_now")
        vol_regime_now = detail.get("vol_regime_now")

        real_ctx = RealVerdictContext(
            verdict=v["VERDICT"],
            health_state=v["HEALTH_STATE"],
            severity=v["SEVERITY"],
            baseline_quality=v.get("BASELINE_QUALITY"),
            thesis_integrity=v.get("THESIS_INTEGRITY"),
            path_quality=v.get("PATH_QUALITY"),
            regime_alignment=v.get("REGIME_ALIGNMENT"),
            fragility=v.get("FRAGILITY"),
            primary_reason_code=v.get("PRIMARY_REASON_CODE"),
            observation_summary=v.get("OBSERVATION_SUMMARY"),
            why_summary=v.get("WHY_SUMMARY"),
        )

        payloads.append(
            PositionPayload(
                position_episode_key=v["POSITION_EPISODE_KEY"],
                portfolio_id=portfolio_id,
                episode_id=int(v["EPISODE_ID"]) if v.get("EPISODE_ID") is not None else None,
                symbol=symbol,
                side=v.get("SIDE") or "LONG",
                as_of_date=v["AS_OF_DATE"] if isinstance(v["AS_OF_DATE"], date) else as_of_date,
                entry_date=v["ENTRY_DATE"],
                entry_price=float(v["ENTRY_PRICE"]) if v.get("ENTRY_PRICE") is not None else None,
                days_held=int(v.get("DAYS_HELD") or 0),
                latest_close=float(latest_close) if latest_close is not None else None,
                unrealized_pnl_pct=float(v["UNREALIZED_PNL_PCT"]) if v.get("UNREALIZED_PNL_PCT") is not None else None,
                structural_state_now=structural_state_now,
                trend_regime_now=trend_regime_now,
                vol_regime_now=vol_regime_now,
                range_regime_now=detail.get("range_regime_now"),
                distance_to_invalidation_pct=float(v["DISTANCE_TO_INVALIDATION_PCT"])
                    if v.get("DISTANCE_TO_INVALIDATION_PCT") is not None else None,
                baseline_context=baseline,
                path_summary=path_summary,
                real_verdict_context=real_ctx,
            )
        )

    logger.info(
        "position_health.payload: built %d payloads for as_of=%s", len(payloads), as_of_date
    )
    return payloads
