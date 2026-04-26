"""
Cockpit overview composer.

Builds the full /cockpit/overview payload by joining:

  * status         — broker connectivity / market-data freshness / daily
                     pipeline / intraday eval / shadow run summary
  * live_portfolio_overview — single canonical NAV/cash/positions block
  * market_pulse   — STOCK-only compact mode of the existing endpoint
  * position_health_summary_rows — flattened, plain-English PH rows
                     merged with intraday overlay
  * priority_review — high-signal exception lists (capped at 5 each
                     with total_count + detail_route)
  * shadow_summary — aggregate counts (uses SHADOW_ACTION_BIAS for
                     exit_now_count, NOT raw SHADOW_VERDICT)
  * intraday_summary — counts of HOLD / WATCH_NOW / REVIEW_NOW /
                     SELL_NOW + the top-level overlay_status

All Snowflake reads are bounded; the intraday overlay is fail-soft.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.db import fetch_all, get_connection, serialize_row

from app.services.cockpit.intraday_overlay import (
    IntradayOverlayResult,
    IntradayOverlayRow,
    get_intraday_overlay,
)
from app.services.cockpit.position_health_summary import (
    PositionHealthSummaryRow,
    build_position_health_summary,
)
from app.services.cockpit.trade_plan import TradePlanIndex, load_trade_plans

logger = logging.getLogger(__name__)


# Cap each priority review side-list at 5 rows (spec refinement #6); the
# UI surfaces "+N more" with a link to the full Position Health page.
PRIORITY_REVIEW_MAX_ROWS = 5

# Threshold beyond which we consider the broker snapshot stale.
_BROKER_FRESH_SEC = 600    # 10 minutes
# Threshold beyond which we consider the daily-bar feed stale.
_MARKET_FRESH_HOURS = 30   # one trading day with weekend slack


# --- SQL (kept narrow & explicit) ------------------------------------------

_ACTIVE_PORTFOLIO_SQL = """
    SELECT
        PORTFOLIO_ID, IBKR_ACCOUNT_ID, BASE_CURRENCY, ADAPTER_MODE
    FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    WHERE COALESCE(IS_ACTIVE, TRUE) = TRUE
    ORDER BY PORTFOLIO_ID
    LIMIT 1
"""

_NAV_SQL = """
    SELECT
        SNAPSHOT_TS,
        NET_LIQUIDATION_EUR,
        TOTAL_CASH_EUR,
        GROSS_POSITION_VALUE_EUR
    FROM MIP.LIVE.BROKER_SNAPSHOTS
    WHERE SNAPSHOT_TYPE = 'NAV'
      AND IBKR_ACCOUNT_ID = %(account_id)s
    ORDER BY SNAPSHOT_TS DESC
    LIMIT 1
"""

_LATEST_BROKER_SNAPSHOT_SQL = """
    SELECT MAX(SNAPSHOT_TS) AS LATEST_TS
    FROM MIP.LIVE.BROKER_SNAPSHOTS
    WHERE IBKR_ACCOUNT_ID = %(account_id)s
"""

_OPEN_ORDER_COUNT_SQL = """
    WITH latest AS (
        SELECT MAX(SNAPSHOT_TS) AS TS
        FROM MIP.LIVE.BROKER_SNAPSHOTS
        WHERE SNAPSHOT_TYPE = 'OPEN_ORDER'
          AND IBKR_ACCOUNT_ID = %(account_id)s
    )
    SELECT COUNT(*) AS N
    FROM MIP.LIVE.BROKER_SNAPSHOTS bs
    INNER JOIN latest l ON l.TS = bs.SNAPSHOT_TS
    WHERE bs.SNAPSHOT_TYPE = 'OPEN_ORDER'
      AND bs.IBKR_ACCOUNT_ID = %(account_id)s
"""

_OPEN_POSITION_COUNT_SQL = """
    SELECT COUNT(*) AS N
    FROM MIP.MART.V_LIVE_OPEN_POSITIONS
    WHERE PORTFOLIO_ID = %(portfolio_id)s
"""

# Entry dates per open position. Used to bound the daily-since-entry
# chart series for the cockpit's inline expand panel.
_OPEN_POSITION_ENTRY_DATES_SQL = """
    SELECT
        POSITION_EPISODE_KEY,
        SYMBOL,
        ENTRY_DATE
    FROM MIP.MART.V_LIVE_OPEN_POSITIONS
    WHERE PORTFOLIO_ID = %(portfolio_id)s
"""

# Daily bars (close only) for currently-open symbols since their
# earliest entry date. The cockpit only needs the last few weeks of
# context per position so we cap with the earliest entry date in the
# portfolio + a small buffer.
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

_LATEST_MARKET_BAR_SQL = """
    SELECT MAX(TS) AS LATEST_TS
    FROM MIP.MART.MARKET_BARS
    WHERE INTERVAL_MINUTES = 1440
"""

# Daily pipeline runs — last successful pipeline event. Tolerates the
# multiple naming styles SP_RUN_DAILY_PIPELINE has used historically.
_LATEST_DAILY_RUN_SQL = """
    SELECT
        EVENT_TS,
        EVENT_NAME,
        STATUS
    FROM MIP.APP.MIP_AUDIT_LOG
    WHERE (
            UPPER(EVENT_NAME) LIKE 'SP_RUN_DAILY_PIPELINE%%'
         OR UPPER(EVENT_NAME) LIKE '%%DAILY_PIPELINE_END%%'
         OR UPPER(EVENT_NAME) LIKE 'DAILY_PIPELINE_RUN%%'
        )
      AND EVENT_TS >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    ORDER BY EVENT_TS DESC
    LIMIT 1
"""


def _query_one(sql: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            rows = fetch_all(cur)
            return serialize_row(rows[0]) if rows else None
        finally:
            cur.close()
    finally:
        conn.close()


def _query_rows(sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
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


# --- Top-status block ------------------------------------------------------


def _seconds_since(ts_iso: Optional[str]) -> Optional[int]:
    if not ts_iso:
        return None
    try:
        ts = datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return int((datetime.now(timezone.utc) - ts).total_seconds())
    except Exception:
        return None


def _build_status(
    *,
    account_id: Optional[str],
    portfolio_id: Optional[int],
    intraday: IntradayOverlayResult,
    shadow_summary_counts: Dict[str, int],
) -> Dict[str, Any]:
    latest_broker = (
        _query_one(_LATEST_BROKER_SNAPSHOT_SQL, {"account_id": account_id})
        if account_id else None
    )
    latest_market = _query_one(_LATEST_MARKET_BAR_SQL, {})
    latest_daily = _query_one(_LATEST_DAILY_RUN_SQL, {})

    broker_ts = (latest_broker or {}).get("LATEST_TS") if latest_broker else None
    broker_age = _seconds_since(broker_ts)
    broker_connected = bool(broker_age is not None and broker_age <= _BROKER_FRESH_SEC)

    market_ts = (latest_market or {}).get("LATEST_TS")
    market_age_h = (_seconds_since(market_ts) or 999_999) / 3600.0
    market_data_fresh = market_age_h <= _MARKET_FRESH_HOURS

    daily_status = (latest_daily or {}).get("STATUS")
    daily_ts = (latest_daily or {}).get("EVENT_TS")

    return {
        "broker_connected": broker_connected,
        "latest_broker_snapshot_ts": broker_ts,
        "latest_market_data_ts": market_ts,
        "market_data_fresh": market_data_fresh,
        "daily_pipeline_status": daily_status,
        "latest_daily_run_ts": daily_ts,
        "latest_intraday_eval_ts": intraday.latest_eval_ts,
        "intraday_overlay_status": intraday.overlay_status,
        "shadow_run_status_summary": shadow_summary_counts,
        "active_portfolio_id": portfolio_id,
    }


# --- Live portfolio overview block -----------------------------------------


def _build_live_portfolio_overview(
    *, portfolio_id: int, account_id: Optional[str]
) -> Dict[str, Any]:
    nav_row = (
        _query_one(_NAV_SQL, {"account_id": account_id}) if account_id else None
    ) or {}
    open_orders = _query_one(_OPEN_ORDER_COUNT_SQL, {"account_id": account_id}) if account_id else {"N": 0}
    open_positions = _query_one(_OPEN_POSITION_COUNT_SQL, {"portfolio_id": int(portfolio_id)}) or {"N": 0}

    nav = nav_row.get("NET_LIQUIDATION_EUR")
    cash = nav_row.get("TOTAL_CASH_EUR")
    gross = nav_row.get("GROSS_POSITION_VALUE_EUR")
    invested_pct: Optional[float] = None
    try:
        if nav and float(nav) > 0 and gross is not None:
            invested_pct = max(0.0, min(1.0, float(gross) / float(nav)))
    except (TypeError, ValueError):
        invested_pct = None

    return {
        "portfolio_id": int(portfolio_id),
        "ibkr_account_id": account_id,
        "nav": float(nav) if nav is not None else None,
        "cash": float(cash) if cash is not None else None,
        "gross_exposure": float(gross) if gross is not None else None,
        "invested_pct": round(invested_pct, 4) if invested_pct is not None else None,
        "open_position_count": int((open_positions or {}).get("N") or 0),
        "working_order_count": int((open_orders or {}).get("N") or 0),
        "freshness_ts": nav_row.get("SNAPSHOT_TS"),
    }


# --- Market pulse compact block --------------------------------------------


# Cap the number of mover sparklines so the cockpit payload stays
# small. We pass top-3 and bottom-3 only; each sparkline is also
# trimmed to the most recent N points for compactness.
_MARKET_PULSE_MOVER_COUNT = 3
_MARKET_PULSE_SPARKLINE_POINTS = 30
_MARKET_PULSE_INDEX_POINTS = 30


def _slim_sparkline(points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not points:
        return []
    tail = points[-_MARKET_PULSE_SPARKLINE_POINTS:]
    out: List[Dict[str, Any]] = []
    for p in tail:
        ts = p.get("ts") or p.get("TS")
        close = p.get("close")
        if ts is None or close is None:
            continue
        try:
            close_f = float(close)
        except (TypeError, ValueError):
            continue
        out.append({"ts": str(ts), "close": close_f})
    return out


def _build_market_pulse_compact() -> Dict[str, Any]:
    """
    Reuse the existing /market/pulse endpoint logic in stock_only mode.
    We call it as a regular Python function so we don't take the HTTP
    round-trip and can keep transactional Snowflake usage minimal.

    Forwards a compact view of the existing payload plus three small
    chart series for the cockpit:
      * `index_series`         — equal-weight index pct over lookback
      * `top_movers` (×3)      — symbol + day_return + sparkline
      * `bottom_movers` (×3)   — symbol + day_return + sparkline
    """
    try:
        from app.routers.market_pulse import get_market_pulse  # type: ignore
    except Exception as exc:  # pragma: no cover
        logger.warning("cockpit: market pulse import failed: %s", exc)
        return {"available": False, "error": str(exc)}
    try:
        full = get_market_pulse(lookback_days=30, stock_only=True)
    except Exception as exc:
        logger.warning("cockpit: market pulse query failed: %s", exc)
        return {"available": False, "error": str(exc)}
    compact = (full or {}).get("compact") or {}
    aggregate = (full or {}).get("aggregate") or {}
    symbols = (full or {}).get("symbols") or []
    sparklines = (full or {}).get("sparklines") or {}
    raw_index = (full or {}).get("index_series") or []

    index_series: List[Dict[str, Any]] = []
    for p in raw_index[-_MARKET_PULSE_INDEX_POINTS:]:
        ts = p.get("ts") or p.get("TS")
        val = p.get("index_return_pct")
        if ts is None or val is None:
            continue
        try:
            val_f = float(val)
        except (TypeError, ValueError):
            continue
        index_series.append({"ts": str(ts), "index_return_pct": val_f})

    movers_with_return = [s for s in symbols if s.get("day_return") is not None]

    def _mover(sym: Dict[str, Any]) -> Dict[str, Any]:
        s = sym.get("symbol")
        return {
            "symbol": s,
            "day_return_pct": (
                round(float(sym["day_return"]) * 100, 2)
                if sym.get("day_return") is not None else None
            ),
            "last_close": sym.get("close"),
            "sparkline": _slim_sparkline(sparklines.get(s) or []),
        }

    top_movers = [_mover(s) for s in movers_with_return[:_MARKET_PULSE_MOVER_COUNT]]
    bottom_movers = [
        _mover(s) for s in list(reversed(movers_with_return))[:_MARKET_PULSE_MOVER_COUNT]
    ]

    return {
        "available": True,
        "market_type": "STOCK",
        "breadth_up": compact.get("breadth_up"),
        "breadth_total": compact.get("breadth_total"),
        "avg_return_pct": compact.get("avg_return_pct"),
        "top_symbol": compact.get("top_symbol"),
        "top_return_pct": compact.get("top_return_pct"),
        "bottom_symbol": compact.get("bottom_symbol"),
        "bottom_return_pct": compact.get("bottom_return_pct"),
        "pulse_label": (full or {}).get("pulse_label") or compact.get("pulse_label") or "Mixed",
        "direction": aggregate.get("direction"),
        "index_series": index_series,
        "top_movers": top_movers,
        "bottom_movers": bottom_movers,
    }


# --- Priority review derivation --------------------------------------------


def _capped(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "items": rows[:PRIORITY_REVIEW_MAX_ROWS],
        "total_count": len(rows),
        "more_count": max(0, len(rows) - PRIORITY_REVIEW_MAX_ROWS),
    }


def _row_short(row: PositionHealthSummaryRow) -> Dict[str, Any]:
    return {
        "position_episode_key": row.position_episode_key,
        "symbol": row.symbol,
        "real_verdict": row.real_verdict,
        "shadow_verdict": row.shadow_verdict,
        "shadow_action_bias": row.shadow_action_bias,
        "intraday_action": row.intraday_action,
        "why_text": row.why_text,
        "attention_label": row.recommendation_label,
        "today_label": row.today_label,
        "detail_route": f"/position-health#{row.position_episode_key}",
    }


def _build_priority_review(
    rows: List[PositionHealthSummaryRow],
) -> Dict[str, Any]:
    real_exit_review = [r for r in rows if (r.real_verdict or "").upper() == "EXIT_REVIEW"]
    shadow_exit_now  = [r for r in rows if r.has_shadow_exit_now_bias]
    disagreement     = [
        r for r in rows
        if (r.real_verdict or "").upper() != (r.shadow_verdict or "").upper()
        and r.shadow_verdict
    ]
    intraday_review_now = [r for r in rows if (r.intraday_action or "") == "REVIEW_NOW"]
    intraday_sell_now   = [r for r in rows if (r.intraday_action or "") == "SELL_NOW"]
    pending_shadow      = [
        r for r in rows
        if not r.shadow_verdict
        and (r.shadow_run_status or "").upper() in {"PENDING", "RUNNING", "QUEUED"}
    ]

    detail_index = "/position-health"
    return {
        "real_exit_review_rows": _capped([_row_short(r) for r in real_exit_review]),
        "shadow_exit_now_rows":  _capped([_row_short(r) for r in shadow_exit_now]),
        "disagreement_rows":     _capped([_row_short(r) for r in disagreement]),
        "intraday_review_now_rows": _capped([_row_short(r) for r in intraday_review_now]),
        "intraday_sell_now_rows":   _capped([_row_short(r) for r in intraday_sell_now]),
        "pending_shadow_rows":   _capped([_row_short(r) for r in pending_shadow]),
        "detail_index_route": detail_index,
    }


# --- Shadow / intraday summaries -------------------------------------------


def _build_shadow_summary(rows: List[PositionHealthSummaryRow]) -> Dict[str, Any]:
    harsher = 0
    softer = 0
    no_shadow = 0
    exit_now_count = 0
    pending = 0
    for r in rows:
        rv = (r.real_verdict or "").upper()
        sv = (r.shadow_verdict or "").upper()
        if not sv:
            srs = (r.shadow_run_status or "").upper()
            if srs in {"PENDING", "RUNNING", "QUEUED"}:
                pending += 1
            else:
                no_shadow += 1
            continue
        if r.has_shadow_exit_now_bias:
            exit_now_count += 1
        # Harshness ordering: EXIT_REVIEW > WATCH > KEEP
        order = {"KEEP": 0, "WATCH": 1, "EXIT_REVIEW": 2}
        rs = order.get(sv, -1)
        rr = order.get(rv, -1)
        if rs > rr:
            harsher += 1
        elif rs < rr:
            softer += 1
    return {
        "harsher_count": harsher,
        "softer_count": softer,
        "no_shadow_count": no_shadow,
        "pending_shadow_count": pending,
        "exit_now_count": exit_now_count,
    }


def _build_shadow_run_status_counts(rows: List[PositionHealthSummaryRow]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for r in rows:
        key = (r.shadow_run_status or "MISSING").upper()
        counts[key] = counts.get(key, 0) + 1
    return counts


def _build_intraday_summary(intraday: IntradayOverlayResult) -> Dict[str, Any]:
    hold = watch = review = sell = unavail = closed = 0
    for ir in intraday.rows:
        action = (ir.intraday_action or "").upper()
        reason = (ir.intraday_reason or "").upper()
        if action == "SELL_NOW":
            sell += 1
        elif action == "REVIEW_NOW":
            review += 1
        elif action == "WATCH_NOW":
            watch += 1
        elif action == "HOLD":
            hold += 1
        elif action == "NO_LIVE_CHECK":
            if reason == "MARKET_CLOSED":
                closed += 1
            else:
                unavail += 1
    return {
        "overlay_status": intraday.overlay_status,
        "latest_eval_ts": intraday.latest_eval_ts,
        "hold_count": hold,
        "watch_now_count": watch,
        "review_now_count": review,
        "sell_now_count": sell,
        "no_live_check_count": unavail,
        "market_closed_count": closed,
        "note": intraday.note,
    }


# --- Chart context loader --------------------------------------------------


def _load_chart_context(
    portfolio_id: int,
) -> tuple[Dict[str, str], Dict[str, List[Dict[str, Any]]]]:
    """
    Load entry dates per episode and daily-since-entry close series per
    symbol. All queries are bounded by the open-position set so they
    scale linearly with portfolio size, not universe size.

    Returns ({episode_key: entry_date_iso}, {symbol: [{date, close}, ...]}).
    Fail-soft: any error returns empty maps so the cockpit still renders.
    """
    entry_dates: Dict[str, str] = {}
    daily_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    try:
        ed_rows = _query_rows(_OPEN_POSITION_ENTRY_DATES_SQL, {"portfolio_id": portfolio_id})
    except Exception as exc:
        logger.warning("cockpit: entry-date query failed: %s", exc)
        return entry_dates, daily_by_symbol

    symbols: List[str] = []
    earliest: Optional[str] = None
    for r in ed_rows:
        key = str(r.get("POSITION_EPISODE_KEY") or "")
        sym = str(r.get("SYMBOL") or "").upper()
        ed = r.get("ENTRY_DATE")
        if key and ed is not None:
            entry_dates[key] = str(ed)
        if sym and sym not in symbols:
            symbols.append(sym)
        if ed is not None:
            ed_str = str(ed)[:10]
            if earliest is None or ed_str < earliest:
                earliest = ed_str

    if not symbols or earliest is None:
        return entry_dates, daily_by_symbol

    placeholders = ", ".join([f"%(s{i})s" for i in range(len(symbols))])
    params: Dict[str, Any] = {"since": earliest}
    for i, s in enumerate(symbols):
        params[f"s{i}"] = s
    sql = _DAILY_BARS_SINCE_SQL.format(symbols=placeholders)
    try:
        bar_rows = _query_rows(sql, params)
    except Exception as exc:
        logger.warning("cockpit: daily-since-entry query failed: %s", exc)
        return entry_dates, daily_by_symbol

    for r in bar_rows:
        sym = str(r.get("SYMBOL") or "").upper()
        bd = r.get("BAR_DATE")
        cl = r.get("CLOSE")
        if not sym or bd is None or cl is None:
            continue
        try:
            close_val = float(cl)
        except (TypeError, ValueError):
            continue
        daily_by_symbol.setdefault(sym, []).append(
            {"date": str(bd)[:10], "close": close_val}
        )
    return entry_dates, daily_by_symbol


# --- Public entry point ----------------------------------------------------


def build_cockpit_overview(portfolio_id: Optional[int] = None) -> Dict[str, Any]:
    """
    Compose the full /cockpit/overview payload for the active live
    portfolio (or the explicitly-passed portfolio_id).
    """
    if portfolio_id is None:
        cfg = _query_one(_ACTIVE_PORTFOLIO_SQL, {})
        if cfg:
            portfolio_id = int(cfg.get("PORTFOLIO_ID"))
            account_id: Optional[str] = cfg.get("IBKR_ACCOUNT_ID")
        else:
            return _empty_overview()
    else:
        cfg_row = _query_one(
            "SELECT IBKR_ACCOUNT_ID FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG WHERE PORTFOLIO_ID = %(p)s",
            {"p": int(portfolio_id)},
        )
        account_id = (cfg_row or {}).get("IBKR_ACCOUNT_ID")

    # Intraday overlay first — it's fail-soft and we feed it into the PH
    # summary so each row's Today chip is consistent with the overlay.
    intraday = get_intraday_overlay(int(portfolio_id))

    entry_dates_by_key, daily_bars_by_symbol = _load_chart_context(int(portfolio_id))

    try:
        trade_plans = load_trade_plans(int(portfolio_id))
    except Exception as exc:
        logger.warning("cockpit: trade_plan load failed: %s", exc)
        trade_plans = TradePlanIndex()

    ph_rows = build_position_health_summary(
        portfolio_id=int(portfolio_id),
        intraday_rows=list(intraday.rows),
        entry_dates_by_key=entry_dates_by_key,
        daily_bars_by_symbol=daily_bars_by_symbol,
        trade_plans=trade_plans,
    )

    shadow_status_counts = _build_shadow_run_status_counts(ph_rows)

    return {
        "schema_version": "1.0.0",
        "as_of_ts": datetime.now(timezone.utc).isoformat(),
        "status": _build_status(
            account_id=account_id,
            portfolio_id=int(portfolio_id),
            intraday=intraday,
            shadow_summary_counts=shadow_status_counts,
        ),
        "live_portfolio_overview": _build_live_portfolio_overview(
            portfolio_id=int(portfolio_id), account_id=account_id
        ),
        "market_pulse": _build_market_pulse_compact(),
        "position_health_summary_rows": [r.to_dict() for r in ph_rows],
        "priority_review": _build_priority_review(ph_rows),
        "shadow_summary": _build_shadow_summary(ph_rows),
        "intraday_summary": _build_intraday_summary(intraday),
    }


def _empty_overview() -> Dict[str, Any]:
    """Returned when no active live portfolio is configured. The cockpit
    still renders, but with empty blocks and a clear note in status."""
    eval_ts = datetime.now(timezone.utc).isoformat()
    return {
        "schema_version": "1.0.0",
        "as_of_ts": eval_ts,
        "status": {
            "broker_connected": False,
            "latest_broker_snapshot_ts": None,
            "latest_market_data_ts": None,
            "market_data_fresh": False,
            "daily_pipeline_status": None,
            "latest_daily_run_ts": None,
            "latest_intraday_eval_ts": eval_ts,
            "intraday_overlay_status": "UNAVAILABLE",
            "shadow_run_status_summary": {},
            "active_portfolio_id": None,
            "note": "No active live portfolio configured.",
        },
        "live_portfolio_overview": {
            "portfolio_id": None,
            "ibkr_account_id": None,
            "nav": None,
            "cash": None,
            "gross_exposure": None,
            "invested_pct": None,
            "open_position_count": 0,
            "working_order_count": 0,
            "freshness_ts": None,
        },
        "market_pulse": _build_market_pulse_compact(),
        "position_health_summary_rows": [],
        "priority_review": {
            "real_exit_review_rows":   {"items": [], "total_count": 0, "more_count": 0},
            "shadow_exit_now_rows":    {"items": [], "total_count": 0, "more_count": 0},
            "disagreement_rows":       {"items": [], "total_count": 0, "more_count": 0},
            "intraday_review_now_rows": {"items": [], "total_count": 0, "more_count": 0},
            "intraday_sell_now_rows":   {"items": [], "total_count": 0, "more_count": 0},
            "pending_shadow_rows":     {"items": [], "total_count": 0, "more_count": 0},
            "detail_index_route": "/position-health",
        },
        "shadow_summary": {
            "harsher_count": 0,
            "softer_count": 0,
            "no_shadow_count": 0,
            "pending_shadow_count": 0,
            "exit_now_count": 0,
        },
        "intraday_summary": {
            "overlay_status": "UNAVAILABLE",
            "latest_eval_ts": eval_ts,
            "hold_count": 0,
            "watch_now_count": 0,
            "review_now_count": 0,
            "sell_now_count": 0,
            "no_live_check_count": 0,
            "market_closed_count": 0,
            "note": "No active live portfolio configured.",
        },
    }
