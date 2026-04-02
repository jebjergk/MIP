"""
Operator-facing projection of entry intelligence + committee + closeout for LIC bootstrap.
Pure transforms + one Snowflake batch query helper; no polling.
"""

from __future__ import annotations

import json
from typing import Any

from app.entry_intel_hooks import build_closeout_summary_for_api


def _parse_variant(val: Any) -> dict:
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            o = json.loads(val)
            return o if isinstance(o, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _fmt_pct_fraction(x: Any, digits: int = 2) -> str | None:
    if x is None:
        return None
    try:
        v = float(x) * 100.0
    except (TypeError, ValueError):
        return None
    if not v == v:  # NaN
        return None
    return f"{v:.{digits}f}%"


def _band_words(band: str | None, kind: str) -> str:
    b = (band or "").upper()
    if b == "HIGH":
        return "High" if kind == "confidence" else "Meaningful downside risk was present in similar setups."
    if b == "MEDIUM":
        return "Medium" if kind == "confidence" else "Moderate downside showed up in history."
    if b == "LOW":
        return "Low" if kind == "confidence" else "Downside in similar setups was relatively contained."
    return "—"


def _sample_strength(n: int | None) -> tuple[str, str]:
    if n is None or n <= 0:
        return "Unknown", "Not enough historical rows to grade sample strength."
    if n < 12:
        return "Weak", "Small sample — treat historical read as exploratory."
    if n < 25:
        return "Medium", "Moderate sample — directional read is useful but noisy."
    return "Strong", "Larger sample — historical distribution is more reliable."


def _similar_setups_narrative(
    p_up: float | None,
    p_base: float | None,
    p_down: float | None,
    sample_label: str,
) -> str:
    if p_up is None and p_base is None and p_down is None:
        return "Historical bucket mix not available for this snapshot."
    u = float(p_up or 0)
    b = float(p_base or 0)
    d = float(p_down or 0)
    dom = max((u, "up"), (b, "base"), (d, "down"), key=lambda x: x[0])
    if dom[1] == "up" and dom[0] >= 0.38:
        return f"Similar setups leaned favorable more often than not ({sample_label} sample)."
    if dom[1] == "down" and dom[0] >= 0.38:
        return f"Similar setups showed adverse outcomes more often ({sample_label} sample)."
    if dom[1] == "base" or abs(u - d) < 0.12:
        return f"Similar setups were often near flat / balanced ({sample_label} sample)."
    return f"Mixed historical outcomes across upside, flat, and down buckets ({sample_label} sample)."


def _override_operator(override: str | None) -> tuple[str, str]:
    o = (override or "").upper()
    if o == "ACCEPT_ALPHA":
        return "Followed baseline", "Committee decision matched the pre-trade analysis posture."
    if o == "INCREASE_VS_ALPHA":
        return "Stronger than baseline", "Committee approved action when the baseline was more cautious (e.g. SKIP or REDUCE)."
    if o == "REDUCE_VS_ALPHA":
        return "More cautious than baseline", "Committee tightened size or blocked relative to a full ENTER baseline."
    if o == "BLOCK_DESPITE_ALPHA":
        return "Blocked despite enter signal", "Baseline suggested proceeding; committee blocked."
    if o == "NO_ALPHA_BASELINE":
        return "No baseline", "Snapshot was missing, stubbed, or not actionable — committee decided without a firm alpha posture."
    if o == "UNKNOWN_OVERRIDE":
        return "Unclear mapping", "Alpha vs committee pairing needs manual review."
    return "—", "Override class not available."


def _justification_phrase(status: str | None) -> str:
    s = (status or "").upper()
    if s in ("NOT_REQUIRED", ""):
        return "No extra deviation note required for this pairing."
    if s == "PRESENT":
        return "Committee output referenced the deviation from baseline."
    if s == "MISSING":
        return "Baseline deviation was not clearly explained in committee output (audit flag)."
    if s == "NOT_EVALUATED_MANUAL_APPLY":
        return "Manual apply path — justification not evaluated the same way."
    if s == "NOT_APPLICABLE_EXIT":
        return "Not applicable (exit flow)."
    return status or "—"


def build_operator_entry_lifecycle(sym_upper: str, row: dict[str, Any]) -> dict[str, Any]:
    """
    row: one Snowflake result row from bootstrap lifecycle query (uppercase keys).
    """
    entry_action_id = row.get("ENTRY_ACTION_ID")
    if not entry_action_id:
        return {
            "symbol": sym_upper,
            "has_entry_intel_link": False,
            "unavailable_reason": "No executed entry action linked to entry intelligence for this symbol.",
        }

    worlds = _parse_variant(row.get("WORLDS_SPEC"))
    alpha = _parse_variant(row.get("ALPHA_SPEC"))
    dist = worlds.get("historical_distribution") if isinstance(worlds.get("historical_distribution"), dict) else {}
    sup = worlds.get("supporting") if isinstance(worlds.get("supporting"), dict) else {}

    p_up = dist.get("upside_probability")
    p_base = dist.get("base_probability")
    p_down = dist.get("downside_probability")
    n = dist.get("sample_size")
    try:
        n_int = int(n) if n is not None else None
    except (TypeError, ValueError):
        n_int = None
    sample_label, sample_note = _sample_strength(n_int)

    rec_action = alpha.get("recommended_action")
    conf = alpha.get("confidence_band")
    dsb = alpha.get("downside_risk_band")
    ev_net = alpha.get("expected_value_net")
    has_alpha = bool(alpha.get("alpha_schema_version") or rec_action)

    verdict_raw = _parse_variant(row.get("VERDICT_JSON"))
    audit = verdict_raw.get("entry_intel_audit_v1") if isinstance(verdict_raw.get("entry_intel_audit_v1"), dict) else {}
    override = verdict_raw.get("alpha_override_class") or audit.get("alpha_override_class")
    justification_status = audit.get("alpha_deviation_justification_status")
    consensus_note = audit.get("alpha_override_consensus_note_v1") or verdict_raw.get("alpha_override_consensus_note_v1")

    if not row.get("COMMITTEE_RUN_ID"):
        committee_headline, committee_detail = (
            "No committee record",
            "No committee run is linked to this entry action yet.",
        )
    else:
        committee_headline, committee_detail = _override_operator(str(override) if override else None)
    committee_rec = row.get("COMMITTEE_RECOMMENDATION")

    closeout_row = None
    if row.get("CLOSEOUT_ID"):
        closeout_row = {
            "CLOSEOUT_ID": row.get("CLOSEOUT_ID"),
            "ENTRY_ACTION_ID": row.get("ENTRY_ACTION_ID"),
            "SNAPSHOT_ID": row.get("CO_SNAPSHOT_ID"),
            "PROPOSAL_ID": row.get("CO_PROPOSAL_ID"),
            "SYMBOL": row.get("CO_SYMBOL"),
            "EXIT_ACTION_ID": row.get("CO_EXIT_ACTION_ID"),
            "EXIT_TYPE": row.get("CO_EXIT_TYPE"),
            "REALIZED_RETURN_PCT": row.get("CO_REALIZED_RETURN_PCT"),
            "REALIZED_SIZE": row.get("CO_REALIZED_SIZE"),
            "REALIZED_PNL": row.get("CO_REALIZED_PNL"),
            "HOLDING_PERIOD_SEC": row.get("CO_HOLDING_PERIOD_SEC"),
            "ENTRY_TS": row.get("CO_ENTRY_TS"),
            "EXIT_TS": row.get("CO_EXIT_TS"),
            "FROZEN_ENTRY_EXPECTATION": row.get("CO_FROZEN_ENTRY_EXPECTATION"),
            "ALIGNMENT_JSON": row.get("CO_ALIGNMENT_JSON"),
        }
    outcome_summary = build_closeout_summary_for_api(closeout_row) if closeout_row else None

    alignment_class = (outcome_summary or {}).get("alignment_summary", {}).get("alignment_class") if outcome_summary else None
    realized_class = (outcome_summary or {}).get("realized_outcome_summary", {}).get("realized_outcome_class") if outcome_summary else None

    return {
        "symbol": sym_upper,
        "has_entry_intel_link": True,
        "entry_action_id": str(entry_action_id),
        "entry_analysis": {
            "recommended_action": rec_action,
            "recommended_action_label": f"System recommendation: {rec_action or '—'}",
            "size_band": alpha.get("recommended_size_band"),
            "confidence_band": conf,
            "confidence_note": _band_words(str(conf) if conf else None, "confidence"),
            "downside_risk_band": dsb,
            "downside_note": _band_words(str(dsb) if dsb else None, "downside"),
            "expected_value_net": ev_net,
            "net_edge_display": _fmt_pct_fraction(ev_net),
            "alpha_summary_text": alpha.get("alpha_summary_text"),
            "has_actionable_baseline": has_alpha and rec_action not in (None, ""),
        },
        "similar_setups": {
            "sample_size": n_int,
            "sample_strength": sample_label,
            "sample_strength_note": sample_note,
            "upside_probability": p_up,
            "base_probability": p_base,
            "downside_probability": p_down,
            "avg_up": dist.get("upside_avg_return"),
            "avg_base": dist.get("base_avg_return"),
            "avg_down": dist.get("downside_avg_return"),
            "horizon_bars": sup.get("horizon_bars"),
            "insufficient_sample": sup.get("insufficient_sample"),
            "narrative": _similar_setups_narrative(
                float(p_up) if p_up is not None else None,
                float(p_base) if p_base is not None else None,
                float(p_down) if p_down is not None else None,
                sample_label,
            ),
        },
        "committee_vs_baseline": {
            "headline": committee_headline,
            "detail": committee_detail,
            "committee_recommendation": committee_rec,
            "justification_phrase": _justification_phrase(str(justification_status) if justification_status else None),
            "consensus_note": consensus_note,
            "alpha_override_class": override,
        },
        "outcome": (
            None
            if not outcome_summary
            else {
                "is_closed_record": True,
                "realized_outcome_class": realized_class,
                "realized_outcome_label": {
                    "FAVORABLE": "Favorable",
                    "FLAT": "Roughly flat",
                    "UNFAVORABLE": "Unfavorable",
                }.get(str(realized_class or "").upper(), str(realized_class or "—")),
                "alignment_class": alignment_class,
                "alignment_label": {
                    "ALIGNED": "Aligned with expectation",
                    "NEUTRAL": "Neutral vs expectation",
                    "ADVERSE": "Adverse vs expectation",
                }.get(str(alignment_class or "").upper(), str(alignment_class or "—")),
                "alignment_explainer": "Aligned means the realized path matched what the pre-trade analysis implied; adverse means it materially diverged.",
                "exit_type": outcome_summary.get("exit_type"),
                "realized_return_display": _fmt_pct_fraction(outcome_summary.get("realized_return_pct")),
                "summary_line": (outcome_summary.get("alignment_summary") or {}).get("summary"),
            }
        ),
        "position_hint_open": True,
        "debug": {
            "snapshot_id": row.get("LINK_SNAPSHOT_ID") or row.get("SNAPSHOT_ID"),
            "proposal_id": row.get("PROPOSAL_ID"),
            "eis_source_version": row.get("SOURCE_VERSION"),
            "eis_version": row.get("EIS_VERSION"),
            "alpha_override_class_raw": override,
            "comparison_rule_version": (outcome_summary.get("alignment_summary") or {}).get("comparison_rule_version")
            if outcome_summary
            else None,
            "alignment_reason_codes": (outcome_summary.get("alignment_summary") or {}).get("alignment_reason_codes")
            if outcome_summary
            else None,
            "committee_run_id": row.get("COMMITTEE_RUN_ID"),
        },
    }


def fetch_entry_lifecycle_rows(cur, portfolio_id: int, symbols_upper: list[str]) -> dict[str, dict[str, Any]]:
    """Returns SYM -> raw row dict for bootstrap merge."""
    from app.db import fetch_all
    from app.routers.symbol_tracker import _in_placeholders

    sym_params = list(dict.fromkeys(s.upper() for s in symbols_upper if s))
    if not sym_params:
        return {}
    ph = _in_placeholders(sym_params)
    cur.execute(
        f"""
        with picked as (
          select
            upper(la.SYMBOL) as SYM,
            la.ACTION_ID as ENTRY_ACTION_ID,
            la.COMMITTEE_RUN_ID,
            l.SNAPSHOT_ID as LINK_SNAPSHOT_ID,
            row_number() over (
              partition by upper(la.SYMBOL)
              order by la.UPDATED_AT desc nulls last, la.ACTION_ID desc
            ) as RN
          from MIP.LIVE.LIVE_ACTIONS la
          inner join MIP.LIVE.ENTRY_INTEL_ACTION_LINK l
            on l.ENTRY_ACTION_ID = la.ACTION_ID
          where la.PORTFOLIO_ID = %s
            and upper(la.SYMBOL) in ({ph})
            and upper(coalesce(la.ACTION_INTENT, '')) = 'ENTRY'
            and upper(coalesce(la.STATUS, '')) = 'EXECUTED'
        )
        select
          p.SYM,
          p.ENTRY_ACTION_ID,
          p.COMMITTEE_RUN_ID,
          p.LINK_SNAPSHOT_ID,
          e.PROPOSAL_ID,
          e.WORLDS_SPEC,
          e.ALPHA_SPEC,
          e.SOURCE_VERSION,
          e.EIS_VERSION,
          cv.VERDICT_JSON,
          cv.RECOMMENDATION as COMMITTEE_RECOMMENDATION,
          tc.CLOSEOUT_ID,
          tc.SNAPSHOT_ID as CO_SNAPSHOT_ID,
          tc.PROPOSAL_ID as CO_PROPOSAL_ID,
          tc.SYMBOL as CO_SYMBOL,
          tc.EXIT_ACTION_ID as CO_EXIT_ACTION_ID,
          tc.EXIT_TYPE as CO_EXIT_TYPE,
          tc.REALIZED_RETURN_PCT as CO_REALIZED_RETURN_PCT,
          tc.REALIZED_SIZE as CO_REALIZED_SIZE,
          tc.REALIZED_PNL as CO_REALIZED_PNL,
          tc.HOLDING_PERIOD_SEC as CO_HOLDING_PERIOD_SEC,
          tc.ENTRY_TS as CO_ENTRY_TS,
          tc.EXIT_TS as CO_EXIT_TS,
          tc.FROZEN_ENTRY_EXPECTATION as CO_FROZEN_ENTRY_EXPECTATION,
          tc.ALIGNMENT_JSON as CO_ALIGNMENT_JSON
        from picked p
        left join MIP.LIVE.ENTRY_INTEL_SNAPSHOT e
          on e.SNAPSHOT_ID = p.LINK_SNAPSHOT_ID
        left join MIP.LIVE.COMMITTEE_VERDICT cv
          on cv.RUN_ID = p.COMMITTEE_RUN_ID
        left join MIP.LIVE.TRADE_CLOSEOUT tc
          on tc.ENTRY_ACTION_ID = p.ENTRY_ACTION_ID
        where p.RN = 1
        """,
        [portfolio_id, *sym_params],
    )
    rows = fetch_all(cur)
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        sym = str(r.get("SYM") or r.get("sym") or "").upper()
        if sym:
            out[sym] = r
    return out
