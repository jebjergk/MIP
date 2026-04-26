"""
Position Health summary for the cockpit (live-trade-intelligence shape).

This module composes the cockpit's primary operational object: one row
per currently-open live position, enriched with everything an operator
needs to make a judgment without leaving the page:

  * broker truth          — qty, avg_cost, current_price, unrealized P&L
                            (dollars + decimal-fraction percent)
  * protective levels     — TP, SL (with trailing-stop awareness)
  * thesis context        — invalidation level, distilled thesis lines
  * structural verdicts   — daily real verdict + plain-English label
  * shadow context        — relation chip (agrees / harsher / softer
                            / wants out / pending), kept secondary
  * intraday overlay      — today's status / change-from-open / bars
  * plan status           — ON_PLAN | AT_RISK | OFF_PLAN | NO_PLAN
                            (decoupled from the recommendation so the
                             two columns answer different questions)
  * recommendation        — HOLD | WATCH | REVIEW | SELL plus a 4-line
                            framing ({plan, now, on_plan, advice})
  * combined chart        — single trade_chart_series with both daily-
                            since-entry and current-day 15m points,
                            ordered ascending; session_open_ts marks
                            the visual divider for the frontend.

P&L source rule
---------------
P&L is sourced exclusively from broker truth (V_LIVE_OPEN_POSITIONS via
`trade_plan.py`). The legacy UNREALIZED_PNL_PCT in
V_POSITION_HEALTH_COMPARISON_LATEST is in **percent points** (1.23 →
1.23%); using it as if it were a fraction was the exact bug that made
the cockpit untrustworthy. We recompute the percentage as a decimal
fraction (0.0123) so the frontend can apply a single `* 100` formatter.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from app.db import fetch_all, get_connection, serialize_row

from app.services.cockpit.intraday_overlay import IntradayOverlayRow
from app.services.cockpit.trade_plan import TradePlan, TradePlanIndex

logger = logging.getLogger(__name__)


# Shadow action-bias values that mean "act now". Matches what the shadow
# orchestrator emits today; broader than just EXIT_NOW so the cockpit
# can flag urgency without waiting for an explicit EXIT verdict.
_EXIT_NOW_BIASES = {"EXIT_NOW", "EXIT_SOON", "TRIM_NOW"}


# --- Public types -----------------------------------------------------------


@dataclass
class PositionHealthSummaryRow:
    # Identity
    position_episode_key: str
    portfolio_id: int
    symbol: str
    side: Optional[str]                # 'LONG' | 'SHORT'
    days_held: Optional[int]
    entry_date: Optional[str]

    # Broker truth (P&L canonical source)
    quantity: Optional[float]
    avg_cost: Optional[float]
    current_price: Optional[float]
    unrealized_pnl: Optional[float]    # dollars
    unrealized_pnl_pct: Optional[float]  # DECIMAL fraction
    market_value: Optional[float]

    # Protective levels (LIVE_ORDERS)
    tp_price: Optional[float]
    tp_label: Optional[str]
    sl_price: Optional[float]
    sl_label: Optional[str]
    sl_is_dynamic: bool

    # Thesis (LIVE_ACTIONS)
    invalidation_level: Optional[float]
    supporting_level: Optional[float]
    thesis_line: Optional[str]
    expectation_line: Optional[str]
    distance_to_invalidation_pct: Optional[float]

    # Real verdict (daily)
    real_verdict: Optional[str]
    real_health_state: Optional[str]
    real_verdict_label: str

    # Shadow (kept secondary)
    shadow_verdict: Optional[str]
    shadow_action_bias: Optional[str]
    shadow_run_status: Optional[str]
    shadow_verdict_label: str
    shadow_relation: str               # AGREES | HARSHER | SOFTER | EXIT_NOW | PENDING | NO_SHADOW | FAILED
    shadow_relation_label: str
    shadow_relation_level: str         # neutral | info | warning | critical
    shadow_summary_text: str

    # Intraday overlay
    intraday_status: Optional[str]
    intraday_action: Optional[str]
    intraday_reason: Optional[str]
    today_label: str
    today_level: str
    today_change_pct: Optional[float]
    today_open: Optional[float]
    last_price: Optional[float]
    intraday_summary_text: str

    # Why text (legacy short reason)
    why_text: str

    # Plan status (decoupled from recommendation)
    plan_status: str                   # ON_PLAN | AT_RISK | OFF_PLAN | NO_PLAN
    plan_status_label: str
    plan_status_level: str             # ok | warning | critical | neutral

    # Recommendation
    recommendation: str                # HOLD | WATCH | REVIEW | SELL
    recommendation_label: str
    recommendation_level: str          # ok | info | warning | critical
    recommendation_text: str
    recommendation_framing: Dict[str, str] = field(default_factory=dict)

    # Combined trade chart (single series, segmented by `kind`)
    trade_chart_series: List[Dict[str, Any]] = field(default_factory=list)
    session_open_ts: Optional[str] = None

    # Long-form text for the inline expand panel.
    real_summary_text: str = ""
    invalidation_summary_text: str = ""

    @property
    def has_shadow_exit_now_bias(self) -> bool:
        return (self.shadow_action_bias or "").upper() in _EXIT_NOW_BIASES

    def to_dict(self) -> Dict[str, Any]:
        return {
            "position_episode_key": self.position_episode_key,
            "portfolio_id": self.portfolio_id,
            "symbol": self.symbol,
            "side": self.side,
            "days_held": self.days_held,
            "entry_date": self.entry_date,
            "quantity": self.quantity,
            "avg_cost": self.avg_cost,
            "current_price": self.current_price,
            "unrealized_pnl": self.unrealized_pnl,
            "unrealized_pnl_pct": self.unrealized_pnl_pct,
            "market_value": self.market_value,
            "tp_price": self.tp_price,
            "tp_label": self.tp_label,
            "sl_price": self.sl_price,
            "sl_label": self.sl_label,
            "sl_is_dynamic": self.sl_is_dynamic,
            "invalidation_level": self.invalidation_level,
            "supporting_level": self.supporting_level,
            "thesis_line": self.thesis_line,
            "expectation_line": self.expectation_line,
            "distance_to_invalidation_pct": self.distance_to_invalidation_pct,
            "real_verdict": self.real_verdict,
            "real_health_state": self.real_health_state,
            "real_verdict_label": self.real_verdict_label,
            "shadow_verdict": self.shadow_verdict,
            "shadow_action_bias": self.shadow_action_bias,
            "shadow_run_status": self.shadow_run_status,
            "shadow_verdict_label": self.shadow_verdict_label,
            "shadow_relation": self.shadow_relation,
            "shadow_relation_label": self.shadow_relation_label,
            "shadow_relation_level": self.shadow_relation_level,
            "shadow_summary_text": self.shadow_summary_text,
            "intraday_status": self.intraday_status,
            "intraday_action": self.intraday_action,
            "intraday_reason": self.intraday_reason,
            "today_label": self.today_label,
            "today_level": self.today_level,
            "today_change_pct": self.today_change_pct,
            "today_open": self.today_open,
            "last_price": self.last_price,
            "intraday_summary_text": self.intraday_summary_text,
            "why_text": self.why_text,
            "plan_status": self.plan_status,
            "plan_status_label": self.plan_status_label,
            "plan_status_level": self.plan_status_level,
            "recommendation": self.recommendation,
            "recommendation_label": self.recommendation_label,
            "recommendation_level": self.recommendation_level,
            "recommendation_text": self.recommendation_text,
            "recommendation_framing": self.recommendation_framing,
            "trade_chart_series": self.trade_chart_series,
            "session_open_ts": self.session_open_ts,
            "real_summary_text": self.real_summary_text,
            "invalidation_summary_text": self.invalidation_summary_text,
        }


# --- SQL --------------------------------------------------------------------


_COMPARISON_SQL = """
    SELECT
        c.POSITION_EPISODE_KEY,
        c.PORTFOLIO_ID,
        c.SYMBOL,
        c.SIDE,
        c.DAYS_HELD,
        c.UNREALIZED_PNL_PCT,
        c.DISTANCE_TO_INVALIDATION_PCT,

        c.REAL_VERDICT,
        c.REAL_HEALTH_STATE,
        c.REAL_PRIMARY_REASON_CODE,
        c.REAL_VERDICT_SUMMARY,
        c.REAL_WHY_SUMMARY,

        c.SHADOW_VERDICT,
        c.SHADOW_ACTION_BIAS,
        c.SHADOW_THESIS_STATUS,
        c.SHADOW_RUN_STATUS,
        c.SHADOW_VERDICT_SUMMARY,
        c.SHADOW_WHY_SUMMARY,

        c.AGREEMENT_LABEL
    FROM MIP.MART.V_POSITION_HEALTH_COMPARISON_LATEST c
    WHERE c.PORTFOLIO_ID = %(portfolio_id)s
"""


# --- Plain-English helpers --------------------------------------------------


_VERDICT_LABELS = {
    "KEEP": "Keep",
    "WATCH": "Watch",
    "EXIT_REVIEW": "Exit review",
}

_SHADOW_VERDICT_LABELS = {
    "KEEP": "Shadow: Keep",
    "WATCH": "Shadow: Watch",
    "EXIT_REVIEW": "Shadow: Exit review",
}

_VERDICT_ORDER = {"KEEP": 0, "WATCH": 1, "EXIT_REVIEW": 2}


def _real_verdict_label(verdict: Optional[str]) -> str:
    return _VERDICT_LABELS.get((verdict or "").upper(), verdict or "—")


def _shadow_verdict_label(verdict: Optional[str], run_status: Optional[str]) -> str:
    rs = (run_status or "").upper()
    if not verdict:
        if rs in {"PENDING", "RUNNING", "QUEUED"}:
            return "Shadow: pending"
        if rs in {"FAILED", "ERROR"}:
            return "Shadow: failed"
        return "Shadow: —"
    return _SHADOW_VERDICT_LABELS.get((verdict or "").upper(), f"Shadow: {verdict}")


def _why_text(row: Dict[str, Any]) -> str:
    real_summary = (row.get("REAL_WHY_SUMMARY") or "").strip()
    if real_summary:
        return real_summary[:200]
    verdict = (row.get("REAL_VERDICT") or "").upper()
    code = (row.get("REAL_PRIMARY_REASON_CODE") or "").upper()
    if verdict == "EXIT_REVIEW":
        return f"Daily verdict says exit review ({code or 'no_code'})."
    if verdict == "WATCH":
        return f"Daily verdict watching ({code or 'no_code'})."
    if verdict == "KEEP":
        return "Daily verdict keep — thesis intact."
    return "No verdict yet."


def _shadow_relation(
    *,
    real_verdict: Optional[str],
    shadow_verdict: Optional[str],
    shadow_action_bias: Optional[str],
    shadow_run_status: Optional[str],
) -> Tuple[str, str, str]:
    rv = (real_verdict or "").upper()
    sv = (shadow_verdict or "").upper()
    sab = (shadow_action_bias or "").upper()
    srs = (shadow_run_status or "").upper()

    if sab in _EXIT_NOW_BIASES:
        return ("EXIT_NOW", "Shadow wants out", "warning")
    if not sv:
        if srs in {"PENDING", "RUNNING", "QUEUED"}:
            return ("PENDING", "Shadow pending", "info")
        if srs in {"FAILED", "ERROR"}:
            return ("FAILED", "Shadow failed", "warning")
        return ("NO_SHADOW", "No shadow yet", "neutral")
    rs = _VERDICT_ORDER.get(sv, -1)
    rr = _VERDICT_ORDER.get(rv, -1)
    if rs > rr:
        return ("HARSHER", "Shadow harsher", "warning")
    if rs < rr:
        return ("SOFTER", "Shadow softer", "info")
    return ("AGREES", "Shadow agrees", "info")


def _real_summary_text(row: Dict[str, Any]) -> str:
    summary = (row.get("REAL_VERDICT_SUMMARY") or "").strip()
    if summary:
        return summary
    why = (row.get("REAL_WHY_SUMMARY") or "").strip()
    if why:
        return why
    return _why_text(row)


def _shadow_summary_text(row: Dict[str, Any]) -> str:
    summary = (row.get("SHADOW_VERDICT_SUMMARY") or "").strip()
    if summary:
        return summary
    why = (row.get("SHADOW_WHY_SUMMARY") or "").strip()
    if why:
        return why
    srs = (row.get("SHADOW_RUN_STATUS") or "").upper()
    if srs in {"PENDING", "RUNNING", "QUEUED"}:
        return "Shadow run pending — no result yet."
    if srs in {"FAILED", "ERROR"}:
        return "Shadow run failed — see Bake-off diagnostics."
    return "No shadow result for this position."


def _invalidation_text(distance_pct: Optional[float]) -> str:
    if distance_pct is None:
        return "Cushion to invalidation not available."
    try:
        v = float(distance_pct)
    except (TypeError, ValueError):
        return "Cushion to invalidation not available."
    if v < 0:
        return f"Through invalidation by {abs(v):.1f}% — thesis is breached."
    if v < 1.5:
        return f"Thin cushion to invalidation: {v:.1f}%."
    if v < 3.0:
        return f"Moderate cushion: {v:.1f}% to invalidation."
    return f"Healthy cushion: {v:.1f}% to invalidation."


def _today_chip(intraday_row: Optional[IntradayOverlayRow]) -> Tuple[str, str]:
    if intraday_row is None:
        return ("No live check", "neutral")
    status = (intraday_row.intraday_status or "").upper()
    action = (intraday_row.intraday_action or "").upper()
    reason = (intraday_row.intraday_reason or "").upper()
    if action == "NO_LIVE_CHECK":
        if reason == "MARKET_CLOSED":
            return ("Market closed", "neutral")
        return ("No live check", "neutral")
    if action == "SELL_NOW":
        return ("Sell now", "critical")
    if action == "REVIEW_NOW":
        return ("Review now", "warning")
    if action == "WATCH_NOW":
        return ("Watching", "info")
    if status == "CONSTRUCTIVE":
        return ("Today constructive", "info")
    return ("Today OK", "neutral")


# --- Plan-status / recommendation derivation --------------------------------

# Plan status answers: "Are we still on plan?"
# Recommendation answers: "What should we do right now?"
# These are deliberately decoupled — a position can be on-plan and still
# warrant Watch (e.g. early signs of softening), and one can be off-plan
# and still rate Hold (e.g. invalidation breached but no live data).


def _plan_status(
    *,
    real_verdict: Optional[str],
    intraday_action: Optional[str],
    pnl_pct: Optional[float],
    distance_pct: Optional[float],
) -> Tuple[str, str, str]:
    """Returns (code, label, level)."""
    rv = (real_verdict or "").upper()
    ia = (intraday_action or "").upper()

    if not rv:
        return ("NO_PLAN", "No plan yet", "neutral")

    off_plan = (
        rv == "EXIT_REVIEW"
        or ia in ("REVIEW_NOW", "SELL_NOW")
        or (distance_pct is not None and distance_pct < 0)
        or (pnl_pct is not None and pnl_pct < -0.03)
    )
    if off_plan:
        return ("OFF_PLAN", "Off plan", "critical")

    at_risk = (
        rv == "WATCH"
        or ia == "WATCH_NOW"
        or (distance_pct is not None and distance_pct < 1.5)
        or (pnl_pct is not None and -0.03 <= pnl_pct < -0.015)
    )
    if at_risk:
        return ("AT_RISK", "At risk", "warning")

    return ("ON_PLAN", "On plan", "ok")


_RECOMMENDATION_LABELS = {
    "SELL": ("Sell now", "critical"),
    "REVIEW": ("Review now", "warning"),
    "WATCH": ("Watch closely", "info"),
    "HOLD": ("Hold", "ok"),
}


def _recommendation(
    *,
    real_verdict: Optional[str],
    intraday_action: Optional[str],
    shadow_relation_code: str,
) -> Tuple[str, str, str]:
    """Returns (code, label, level)."""
    rv = (real_verdict or "").upper()
    ia = (intraday_action or "").upper()

    if ia == "SELL_NOW":
        code = "SELL"
    elif ia == "REVIEW_NOW" or rv == "EXIT_REVIEW":
        code = "REVIEW"
    elif ia == "WATCH_NOW" or rv == "WATCH" or shadow_relation_code == "EXIT_NOW":
        code = "WATCH"
    else:
        code = "HOLD"

    label, level = _RECOMMENDATION_LABELS[code]
    return code, label, level


def _recommendation_text_for(
    *,
    code: str,
    real_verdict: Optional[str],
    intraday_action: Optional[str],
    shadow_relation_code: str,
) -> str:
    rv = (real_verdict or "").upper()
    ia = (intraday_action or "").upper()
    if code == "SELL":
        return "Manual exit advisable — intraday breakdown stacked on a cautious daily verdict."
    if code == "REVIEW":
        if ia == "REVIEW_NOW" and rv == "EXIT_REVIEW":
            return "Review and act — daily exit-review and intraday deterioration today."
        if ia == "REVIEW_NOW":
            return "Review now — meaningful intraday deterioration today."
        return "Reduce or exit on plan — daily verdict says exit-review."
    if code == "WATCH":
        if shadow_relation_code == "EXIT_NOW":
            return "Watch closely — shadow is biased to exit; daily is calmer."
        if ia == "WATCH_NOW" and rv == "WATCH":
            return "Watch closely — daily watch and today softening."
        if ia == "WATCH_NOW":
            return "Watch today — daily verdict still constructive."
        return "Watch closely — daily verdict is on watch."
    if rv == "KEEP":
        return "Hold — daily keep and today stable."
    return "Hold — no material concern in current data."


def _format_money(v: Optional[float]) -> str:
    if v is None:
        return "—"
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return "—"


def _format_pct(v: Optional[float], dp: int = 2) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v) * 100:+.{dp}f}%"
    except (TypeError, ValueError):
        return "—"


def _build_recommendation_framing(
    *,
    symbol: str,
    direction: Optional[str],
    quantity: Optional[float],
    avg_cost: Optional[float],
    current_price: Optional[float],
    pnl_pct: Optional[float],
    today_change_pct: Optional[float],
    tp_price: Optional[float],
    sl_price: Optional[float],
    sl_label: Optional[str],
    invalidation_level: Optional[float],
    real_verdict_label: str,
    plan_status_code: str,
    distance_pct: Optional[float],
    recommendation_text: str,
) -> Dict[str, str]:
    """The 4-line framing the cockpit displays under the trade panel:

      plan    — what we set out to do
      now     — what is happening right now
      on_plan — are we still on plan
      advice  — one plain-English action stance
    """
    side = (direction or "").lower() or "position"
    qty_text = (
        f"{abs(int(quantity))}" if quantity is not None and quantity == int(quantity)
        else (f"{abs(float(quantity)):.4f}" if quantity is not None else "—")
    )
    avg_text = _format_money(avg_cost)
    tp_text = _format_money(tp_price)
    sl_text = _format_money(sl_price)
    inv_text = _format_money(invalidation_level)

    if avg_cost is not None and (tp_price is not None or sl_price is not None or invalidation_level is not None):
        sl_part_label = (sl_label or "stop").lower()
        sl_part = (
            f"{sl_part_label} {sl_text}" if sl_price is not None
            else (f"invalidation {inv_text}" if invalidation_level is not None else "no stop set")
        )
        tp_part = f"target {tp_text}" if tp_price is not None else "no target set"
        plan = f"{side.title()} {qty_text} {symbol} from {avg_text}; {tp_part}; {sl_part}."
    else:
        plan = f"{side.title()} {qty_text} {symbol} held — no structured plan recorded."

    last_text = _format_money(current_price)
    pnl_text = _format_pct(pnl_pct)
    today_text = _format_pct(today_change_pct)
    now = (
        f"Last {last_text} ({pnl_text}); today {today_text} from open. "
        f"Daily verdict: {real_verdict_label}."
    )

    if plan_status_code == "ON_PLAN":
        on_plan = "Yes — daily verdict still constructive and price holding."
    elif plan_status_code == "AT_RISK":
        if distance_pct is not None and distance_pct < 1.5:
            on_plan = f"Mostly — only {distance_pct:.1f}% cushion to invalidation."
        else:
            on_plan = "Mostly — verdict softening or P&L drawing down."
    elif plan_status_code == "OFF_PLAN":
        if distance_pct is not None and distance_pct < 0:
            on_plan = f"No — through invalidation by {abs(distance_pct):.1f}%."
        else:
            on_plan = "No — daily verdict or intraday signal says exit-review."
    else:
        on_plan = "Plan not yet established."

    return {
        "plan": plan,
        "now": now,
        "on_plan": on_plan,
        "advice": recommendation_text,
    }


# --- Combined trade chart series -------------------------------------------


def _build_trade_chart_series(
    *,
    daily_bars: List[Dict[str, Any]],
    intraday_bars: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Build a single chronological series suitable for one chart that
    shows daily-since-entry continuing into today's 15m structure.

    Each output entry has:
      kind   : 'DAILY' | 'INTRADAY'
      ts     : ISO-ish timestamp (daily uses '<date>T16:00:00')
      label  : display label (daily 'MMM DD', intraday 'HH:MM')
      close  : numeric close
      date   : ISO date for the bar (helps the frontend group)

    The intraday segment supersedes any daily entry on the same date,
    so today's daily bar (if present) is dropped in favour of the live
    15m points. `session_open_ts` is the timestamp of the first
    intraday bar — the frontend draws it as a vertical divider.
    """
    today_date: Optional[str] = None
    intraday_normalized: List[Dict[str, Any]] = []
    for b in intraday_bars or []:
        ts = b.get("ts")
        close = b.get("close")
        if ts is None or close is None:
            continue
        try:
            close_f = float(close)
        except (TypeError, ValueError):
            continue
        ts_str = str(ts)
        date_part = ts_str[:10]
        time_part = ts_str[11:16] if len(ts_str) >= 16 else ts_str
        if today_date is None:
            today_date = date_part
        intraday_normalized.append({
            "kind": "INTRADAY",
            "ts": ts_str,
            "label": time_part,
            "close": close_f,
            "date": date_part,
        })

    daily_normalized: List[Dict[str, Any]] = []
    for d in daily_bars or []:
        date = d.get("date")
        close = d.get("close")
        if date is None or close is None:
            continue
        try:
            close_f = float(close)
        except (TypeError, ValueError):
            continue
        date_str = str(date)[:10]
        if today_date is not None and date_str == today_date:
            continue
        daily_normalized.append({
            "kind": "DAILY",
            "ts": f"{date_str}T16:00:00",
            "label": date_str,
            "close": close_f,
            "date": date_str,
        })

    daily_normalized.sort(key=lambda x: x["ts"])
    intraday_normalized.sort(key=lambda x: x["ts"])

    series = daily_normalized + intraday_normalized
    session_open_ts = intraday_normalized[0]["ts"] if intraday_normalized else None
    return series, session_open_ts


# --- Implementation ---------------------------------------------------------


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


def build_position_health_summary(
    portfolio_id: int,
    *,
    intraday_rows: Optional[List[IntradayOverlayRow]] = None,
    entry_dates_by_key: Optional[Dict[str, str]] = None,
    daily_bars_by_symbol: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    trade_plans: Optional[TradePlanIndex] = None,
) -> List[PositionHealthSummaryRow]:
    """
    Return one trade-intelligence row per currently-open live position.
    Rows are ordered with the most attention-worthy first.

    `entry_dates_by_key`, `daily_bars_by_symbol`, and `trade_plans` are
    supplied by the caller (overview composer) so this function never
    re-queries data that's already loaded for other parts of the
    cockpit payload.
    """
    rows = _query_rows(_COMPARISON_SQL, {"portfolio_id": int(portfolio_id)})

    intraday_by_key: Dict[str, IntradayOverlayRow] = {}
    for ir in intraday_rows or []:
        if ir.position_episode_key:
            intraday_by_key[ir.position_episode_key] = ir

    entry_dates_by_key = entry_dates_by_key or {}
    daily_bars_by_symbol = daily_bars_by_symbol or {}
    plans = trade_plans or TradePlanIndex()

    out: List[PositionHealthSummaryRow] = []
    for r in rows:
        key = str(r.get("POSITION_EPISODE_KEY") or "")
        symbol = str(r.get("SYMBOL") or "").upper()
        side_view = r.get("SIDE")
        days_held_raw = r.get("DAYS_HELD")
        try:
            days_held = int(days_held_raw) if days_held_raw is not None else None
        except (TypeError, ValueError):
            days_held = None

        try:
            distance_pct = (
                float(r["DISTANCE_TO_INVALIDATION_PCT"])
                if r.get("DISTANCE_TO_INVALIDATION_PCT") is not None else None
            )
        except (TypeError, ValueError):
            distance_pct = None

        plan = plans.get(symbol)

        # Broker-truth P&L (canonical). Falls back to view value only if
        # broker data is entirely absent for the symbol.
        if plan is not None:
            quantity = plan.quantity
            avg_cost = plan.avg_cost
            current_price = plan.current_price
            unrealized_pnl = plan.unrealized_pnl
            unrealized_pnl_pct = plan.unrealized_pnl_pct
            market_value = plan.market_value
            tp_price = plan.tp_price
            tp_label = plan.tp_label
            sl_price = plan.sl_price
            sl_label = plan.sl_label
            sl_is_dynamic = plan.sl_is_dynamic
            invalidation_level = plan.invalidation_level
            supporting_level = plan.supporting_level
            thesis_line = plan.thesis_line
            expectation_line = plan.expectation_line
            side = plan.direction or side_view
        else:
            quantity = avg_cost = current_price = None
            unrealized_pnl = market_value = None
            unrealized_pnl_pct = None
            tp_price = sl_price = None
            tp_label = sl_label = None
            sl_is_dynamic = False
            invalidation_level = supporting_level = None
            thesis_line = expectation_line = None
            side = side_view

        intraday = intraday_by_key.get(key)
        intraday_action = intraday.intraday_action if intraday else None
        intraday_status = intraday.intraday_status if intraday else None
        intraday_reason = intraday.intraday_reason if intraday else None
        intraday_summary_text = (
            (intraday.intraday_summary if intraday else None)
            or "No intraday check available."
        )
        today_change_pct = intraday.today_change_pct if intraday else None
        today_open_val = intraday.today_open if intraday else None
        last_price = intraday.last_price if intraday else None
        intraday_bars = list(intraday.bars) if intraday and intraday.bars else []

        if last_price is not None:
            current_price = last_price

        relation_code, relation_label, relation_level = _shadow_relation(
            real_verdict=r.get("REAL_VERDICT"),
            shadow_verdict=r.get("SHADOW_VERDICT"),
            shadow_action_bias=r.get("SHADOW_ACTION_BIAS"),
            shadow_run_status=r.get("SHADOW_RUN_STATUS"),
        )

        today_label, today_level = _today_chip(intraday)

        plan_status_code, plan_status_label, plan_status_level = _plan_status(
            real_verdict=r.get("REAL_VERDICT"),
            intraday_action=intraday_action,
            pnl_pct=unrealized_pnl_pct,
            distance_pct=distance_pct,
        )

        rec_code, rec_label, rec_level = _recommendation(
            real_verdict=r.get("REAL_VERDICT"),
            intraday_action=intraday_action,
            shadow_relation_code=relation_code,
        )
        rec_text = _recommendation_text_for(
            code=rec_code,
            real_verdict=r.get("REAL_VERDICT"),
            intraday_action=intraday_action,
            shadow_relation_code=relation_code,
        )

        chart_series, session_open_ts = _build_trade_chart_series(
            daily_bars=daily_bars_by_symbol.get(symbol, []),
            intraday_bars=intraday_bars,
        )

        framing = _build_recommendation_framing(
            symbol=symbol,
            direction=side,
            quantity=quantity,
            avg_cost=avg_cost,
            current_price=current_price,
            pnl_pct=unrealized_pnl_pct,
            today_change_pct=today_change_pct,
            tp_price=tp_price,
            sl_price=sl_price,
            sl_label=sl_label,
            invalidation_level=invalidation_level,
            real_verdict_label=_real_verdict_label(r.get("REAL_VERDICT")),
            plan_status_code=plan_status_code,
            distance_pct=distance_pct,
            recommendation_text=rec_text,
        )

        out.append(
            PositionHealthSummaryRow(
                position_episode_key=key,
                portfolio_id=int(r.get("PORTFOLIO_ID") or portfolio_id),
                symbol=symbol,
                side=side,
                days_held=days_held,
                entry_date=entry_dates_by_key.get(key),
                quantity=quantity,
                avg_cost=avg_cost,
                current_price=current_price,
                unrealized_pnl=unrealized_pnl,
                unrealized_pnl_pct=unrealized_pnl_pct,
                market_value=market_value,
                tp_price=tp_price,
                tp_label=tp_label,
                sl_price=sl_price,
                sl_label=sl_label,
                sl_is_dynamic=sl_is_dynamic,
                invalidation_level=invalidation_level,
                supporting_level=supporting_level,
                thesis_line=thesis_line,
                expectation_line=expectation_line,
                distance_to_invalidation_pct=distance_pct,
                real_verdict=r.get("REAL_VERDICT"),
                real_health_state=r.get("REAL_HEALTH_STATE"),
                real_verdict_label=_real_verdict_label(r.get("REAL_VERDICT")),
                shadow_verdict=r.get("SHADOW_VERDICT"),
                shadow_action_bias=r.get("SHADOW_ACTION_BIAS"),
                shadow_run_status=r.get("SHADOW_RUN_STATUS"),
                shadow_verdict_label=_shadow_verdict_label(
                    r.get("SHADOW_VERDICT"), r.get("SHADOW_RUN_STATUS")
                ),
                shadow_relation=relation_code,
                shadow_relation_label=relation_label,
                shadow_relation_level=relation_level,
                shadow_summary_text=_shadow_summary_text(r),
                intraday_status=intraday_status,
                intraday_action=intraday_action,
                intraday_reason=intraday_reason,
                today_label=today_label,
                today_level=today_level,
                today_change_pct=today_change_pct,
                today_open=today_open_val,
                last_price=last_price,
                intraday_summary_text=intraday_summary_text,
                why_text=_why_text(r),
                plan_status=plan_status_code,
                plan_status_label=plan_status_label,
                plan_status_level=plan_status_level,
                recommendation=rec_code,
                recommendation_label=rec_label,
                recommendation_level=rec_level,
                recommendation_text=rec_text,
                recommendation_framing=framing,
                trade_chart_series=chart_series,
                session_open_ts=session_open_ts,
                real_summary_text=_real_summary_text(r),
                invalidation_summary_text=_invalidation_text(distance_pct),
            )
        )

    out.sort(key=_attention_sort_key)
    return out


_PLAN_STATUS_RANK = {
    "OFF_PLAN": 0,
    "AT_RISK": 1,
    "NO_PLAN": 2,
    "ON_PLAN": 3,
}

_RECOMMENDATION_RANK = {
    "SELL": 0,
    "REVIEW": 1,
    "WATCH": 2,
    "HOLD": 3,
}


def _attention_sort_key(row: PositionHealthSummaryRow) -> tuple:
    return (
        _RECOMMENDATION_RANK.get(row.recommendation, 9),
        _PLAN_STATUS_RANK.get(row.plan_status, 9),
        -(row.days_held or 0),
        row.symbol,
    )
