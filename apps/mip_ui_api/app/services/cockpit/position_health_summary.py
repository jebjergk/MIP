"""
Position Health summary for the cockpit.

Cockpit-shaped flattening of MIP.MART.V_POSITION_HEALTH_COMPARISON_LATEST,
merged with the intraday overlay so the operator sees one row per
currently-open live position with:

  * plain-English Why text
  * a single Attention chip ("Needs review", "Shadow says exit",
    "Watching", "Pending shadow", "—")
  * Today status (from the intraday overlay; "Today OK" is **only**
    used when the overlay actually evaluated this row — never for
    UNAVAILABLE / MARKET_CLOSED / NO_LIVE_CHECK rows)

This module deliberately uses the live-gated mart view so legacy
horizon/sim verdict rows never leak through.

Action-bias semantics (spec refinement #2)
------------------------------------------
SHADOW_VERDICT='EXIT_REVIEW' alone is **not** treated as "exit now".
The cockpit only shows "Shadow says exit" when SHADOW_ACTION_BIAS
indicates a true exit-now bias (EXIT_NOW / EXIT_SOON / TRIM_NOW). When
SHADOW_VERDICT='EXIT_REVIEW' but SHADOW_ACTION_BIAS is MONITOR/HOLD/None,
the chip is "Shadow says review".
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from app.db import fetch_all, get_connection, serialize_row

from app.services.cockpit.intraday_overlay import IntradayOverlayRow

logger = logging.getLogger(__name__)


# Shadow action-bias values that mean "act now". Matches what the shadow
# orchestrator emits today; broader than just EXIT_NOW so the cockpit
# can flag urgency without waiting for an explicit EXIT verdict.
_EXIT_NOW_BIASES = {"EXIT_NOW", "EXIT_SOON", "TRIM_NOW"}


# --- Public types -----------------------------------------------------------


@dataclass
class PositionHealthSummaryRow:
    position_episode_key: str
    portfolio_id: int
    symbol: str
    side: Optional[str]
    days_held: Optional[int]
    unrealized_pnl_pct: Optional[float]
    entry_date: Optional[str]
    distance_to_invalidation_pct: Optional[float]

    real_verdict: Optional[str]
    real_health_state: Optional[str]
    real_verdict_label: str            # plain English

    shadow_verdict: Optional[str]
    shadow_action_bias: Optional[str]
    shadow_run_status: Optional[str]
    shadow_verdict_label: str          # plain English
    # Shadow relation to real verdict, used for the Shadow chip on
    # the cockpit table. Values: AGREES | HARSHER | SOFTER | EXIT_NOW
    # | PENDING | NO_SHADOW | FAILED.
    shadow_relation: str
    shadow_relation_label: str
    shadow_relation_level: str         # neutral | info | warning | critical

    why_text: str
    attention_label: str
    attention_level: str               # neutral | info | warning | critical

    intraday_status: Optional[str]
    intraday_action: Optional[str]
    intraday_reason: Optional[str]
    today_label: str                   # short plain-English status
    today_level: str                   # neutral | info | warning | critical
    today_change_pct: Optional[float]
    today_open: Optional[float]
    last_price: Optional[float]

    # Long-form text for the inline expand panel.
    real_summary_text: str
    shadow_summary_text: str
    intraday_summary_text: str
    invalidation_summary_text: str
    recommendation_text: str

    # Chart-ready series for the inline expand panel. Both are
    # JSON-friendly lists of dicts; never None.
    intraday_bars: List[Dict[str, Any]] = field(default_factory=list)
    daily_since_entry: List[Dict[str, Any]] = field(default_factory=list)

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
            "unrealized_pnl_pct": self.unrealized_pnl_pct,
            "entry_date": self.entry_date,
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
            "why_text": self.why_text,
            "attention_label": self.attention_label,
            "attention_level": self.attention_level,
            "intraday_status": self.intraday_status,
            "intraday_action": self.intraday_action,
            "intraday_reason": self.intraday_reason,
            "today_label": self.today_label,
            "today_level": self.today_level,
            "today_change_pct": self.today_change_pct,
            "today_open": self.today_open,
            "last_price": self.last_price,
            "real_summary_text": self.real_summary_text,
            "shadow_summary_text": self.shadow_summary_text,
            "intraday_summary_text": self.intraday_summary_text,
            "invalidation_summary_text": self.invalidation_summary_text,
            "recommendation_text": self.recommendation_text,
            "intraday_bars": self.intraday_bars,
            "daily_since_entry": self.daily_since_entry,
        }


# --- SQL --------------------------------------------------------------------

# Pull only what the cockpit needs. AGREEMENT_LABEL is computed in the
# view but we also surface SHADOW_ACTION_BIAS so the cockpit can use the
# more precise action-bias semantics for "exit now" urgency.
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
    """One short sentence explaining the dominant signal. Prefers the
    daily real WHY_SUMMARY when present, else falls back to a code-based
    sentence."""
    real_summary = (row.get("REAL_WHY_SUMMARY") or "").strip()
    if real_summary:
        # Daily real WHY_SUMMARY is already operator-tone and short.
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


def _attention(
    *,
    real_verdict: Optional[str],
    shadow_verdict: Optional[str],
    shadow_action_bias: Optional[str],
    shadow_run_status: Optional[str],
    intraday_action: Optional[str],
) -> tuple[str, str]:
    """
    Compute (attention_label, attention_level). Order of precedence:
      1. Intraday SELL_NOW           → critical "Sell now (intraday)"
      2. Intraday REVIEW_NOW         → warning  "Review (intraday)"
      3. Real EXIT_REVIEW            → warning  "Needs review"
      4. Shadow exit-now bias        → warning  "Shadow says exit"
      5. Shadow EXIT_REVIEW (no bias)→ info     "Shadow says review"
      6. Intraday WATCH_NOW          → info     "Watching"
      7. Real WATCH                  → info     "Watching"
      8. Pending shadow              → info     "Pending shadow"
      9. otherwise                   → neutral  "—"
    """
    rv = (real_verdict or "").upper()
    sv = (shadow_verdict or "").upper()
    sab = (shadow_action_bias or "").upper()
    srs = (shadow_run_status or "").upper()
    ia = (intraday_action or "").upper()

    if ia == "SELL_NOW":
        return ("Sell now (intraday)", "critical")
    if ia == "REVIEW_NOW":
        return ("Review (intraday)", "warning")
    if rv == "EXIT_REVIEW":
        return ("Needs review", "warning")
    if sab in _EXIT_NOW_BIASES:
        return ("Shadow says exit", "warning")
    if sv == "EXIT_REVIEW":
        return ("Shadow says review", "info")
    if ia == "WATCH_NOW":
        return ("Watching", "info")
    if rv == "WATCH":
        return ("Watching", "info")
    if not sv and srs in {"PENDING", "RUNNING", "QUEUED", ""}:
        # No shadow result yet — surface so operators don't mistake "no
        # disagreement" for "no shadow run".
        return ("Pending shadow", "info") if srs else ("—", "neutral")
    return ("—", "neutral")


_VERDICT_ORDER = {"KEEP": 0, "WATCH": 1, "EXIT_REVIEW": 2}


def _shadow_relation(
    *,
    real_verdict: Optional[str],
    shadow_verdict: Optional[str],
    shadow_action_bias: Optional[str],
    shadow_run_status: Optional[str],
) -> tuple[str, str, str]:
    """
    Compute (relation_code, relation_label, level).

    Code values are stable so the frontend can style chips off of them:
        AGREES | HARSHER | SOFTER | EXIT_NOW | PENDING | NO_SHADOW | FAILED
    """
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


def _recommendation_text(
    *,
    real_verdict: Optional[str],
    shadow_relation: str,
    intraday_action: Optional[str],
) -> str:
    """One operator-tone sentence summarising the stance. Deterministic
    on inputs so it's reproducible."""
    rv = (real_verdict or "").upper()
    ia = (intraday_action or "").upper()

    if ia == "SELL_NOW":
        return "Manual exit advisable — intraday breakdown stacked on a cautious daily verdict."
    if ia == "REVIEW_NOW":
        return "Review now — meaningful intraday deterioration today."
    if rv == "EXIT_REVIEW":
        if ia == "WATCH_NOW":
            return "Reduce or exit on plan — daily exit-review and today softening."
        return "Reduce or exit on plan — daily verdict says exit-review."
    if shadow_relation == "EXIT_NOW":
        return "Hold but escalate — shadow is biased to exit even if daily is calmer."
    if rv == "WATCH":
        if ia == "WATCH_NOW":
            return "Watch closely — daily watch and today mildly weaker."
        if ia == "HOLD":
            return "Hold and watch — daily watch but today stable."
        return "Watch closely — daily verdict is on watch."
    if rv == "KEEP":
        if ia == "WATCH_NOW":
            return "Hold — daily keep, but watch today for further weakness."
        return "Hold — daily keep and today stable."
    return "Hold — no material concern in current data."


def _today_chip(intraday_row: Optional[IntradayOverlayRow]) -> tuple[str, str]:
    """
    Map the intraday overlay row into a short Today chip. Critically,
    we never return "Today OK" when the overlay didn't actually
    evaluate this row.
    """
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
) -> List[PositionHealthSummaryRow]:
    """
    Return one summary row per currently-open live position. Rows are
    ordered with the most attention-worthy first.

    `entry_dates_by_key` and `daily_bars_by_symbol` are supplied by the
    caller (overview composer) so the inline expand panel can chart
    daily-since-entry without each row issuing its own query.
    """
    rows = _query_rows(_COMPARISON_SQL, {"portfolio_id": int(portfolio_id)})
    intraday_by_key: Dict[str, IntradayOverlayRow] = {}
    for ir in intraday_rows or []:
        if ir.position_episode_key:
            intraday_by_key[ir.position_episode_key] = ir

    entry_dates_by_key = entry_dates_by_key or {}
    daily_bars_by_symbol = daily_bars_by_symbol or {}

    out: List[PositionHealthSummaryRow] = []
    for r in rows:
        key = str(r.get("POSITION_EPISODE_KEY") or "")
        symbol = str(r.get("SYMBOL") or "").upper()
        side = r.get("SIDE")
        days_held_raw = r.get("DAYS_HELD")
        try:
            days_held = int(days_held_raw) if days_held_raw is not None else None
        except (TypeError, ValueError):
            days_held = None

        try:
            pnl_pct = float(r["UNREALIZED_PNL_PCT"]) if r.get("UNREALIZED_PNL_PCT") is not None else None
        except (TypeError, ValueError):
            pnl_pct = None

        try:
            distance_pct = (
                float(r["DISTANCE_TO_INVALIDATION_PCT"])
                if r.get("DISTANCE_TO_INVALIDATION_PCT") is not None else None
            )
        except (TypeError, ValueError):
            distance_pct = None

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

        attention_label, attention_level = _attention(
            real_verdict=r.get("REAL_VERDICT"),
            shadow_verdict=r.get("SHADOW_VERDICT"),
            shadow_action_bias=r.get("SHADOW_ACTION_BIAS"),
            shadow_run_status=r.get("SHADOW_RUN_STATUS"),
            intraday_action=intraday_action,
        )

        relation_code, relation_label, relation_level = _shadow_relation(
            real_verdict=r.get("REAL_VERDICT"),
            shadow_verdict=r.get("SHADOW_VERDICT"),
            shadow_action_bias=r.get("SHADOW_ACTION_BIAS"),
            shadow_run_status=r.get("SHADOW_RUN_STATUS"),
        )

        today_label, today_level = _today_chip(intraday)

        recommendation = _recommendation_text(
            real_verdict=r.get("REAL_VERDICT"),
            shadow_relation=relation_code,
            intraday_action=intraday_action,
        )

        out.append(
            PositionHealthSummaryRow(
                position_episode_key=key,
                portfolio_id=int(r.get("PORTFOLIO_ID") or portfolio_id),
                symbol=symbol,
                side=side,
                days_held=days_held,
                unrealized_pnl_pct=pnl_pct,
                entry_date=entry_dates_by_key.get(key),
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
                why_text=_why_text(r),
                attention_label=attention_label,
                attention_level=attention_level,
                intraday_status=intraday_status,
                intraday_action=intraday_action,
                intraday_reason=intraday_reason,
                today_label=today_label,
                today_level=today_level,
                today_change_pct=today_change_pct,
                today_open=today_open_val,
                last_price=last_price,
                real_summary_text=_real_summary_text(r),
                shadow_summary_text=_shadow_summary_text(r),
                intraday_summary_text=intraday_summary_text,
                invalidation_summary_text=_invalidation_text(distance_pct),
                recommendation_text=recommendation,
                intraday_bars=intraday_bars,
                daily_since_entry=list(daily_bars_by_symbol.get(symbol, [])),
            )
        )

    out.sort(key=_attention_sort_key)
    return out


_ATTENTION_RANK = {
    "critical": 0,
    "warning": 1,
    "info": 2,
    "neutral": 3,
}


def _attention_sort_key(row: PositionHealthSummaryRow) -> tuple:
    return (
        _ATTENTION_RANK.get(row.attention_level, 9),
        _ATTENTION_RANK.get(row.today_level, 9),
        -(row.days_held or 0),
        row.symbol,
    )
