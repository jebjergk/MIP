from fastapi import APIRouter

from app.db import get_connection, serialize_row

router = APIRouter(prefix="/briefs", tags=["briefs"])


def _build_summary(brief_json: dict, risk_gate: dict) -> dict:
    """Build executive summary from brief data and risk gate state."""
    risk = brief_json.get("risk", {}).get("latest", {}) or {}
    proposals = brief_json.get("proposals", {}).get("summary", {}) or {}
    signals = brief_json.get("signals", {}) or {}
    trusted_now = signals.get("trusted_now", []) or []

    # Determine portfolio status
    risk_status = risk.get("risk_status", "OK")
    entries_blocked = risk_gate.get("entries_blocked", False) if risk_gate else False
    block_reason = risk_gate.get("block_reason") if risk_gate else None

    if entries_blocked:
        status = "STOPPED"
        status_explanation = f"Entries blocked: {block_reason or 'drawdown stop active'}. Only exits allowed."
    elif risk_status == "WARN":
        status = "CAUTION"
        status_explanation = "Drawdown approaching threshold. Consider reducing exposure."
    else:
        status = "SAFE"
        status_explanation = "Portfolio operating normally. Entries allowed."

    return {
        "status": status,
        "status_label": {"SAFE": "Safe", "CAUTION": "Caution", "STOPPED": "Stopped"}.get(status, status),
        "entries_allowed": not entries_blocked,
        "new_suggestions_today": len(trusted_now),
        "explanation": status_explanation,
        "executed_count": proposals.get("executed", 0),
        "rejected_count": proposals.get("rejected", 0),
    }


def _build_opportunities(brief_json: dict) -> list:
    """Build opportunities list from trusted signals."""
    signals = brief_json.get("signals", {}) or {}
    trusted_now = signals.get("trusted_now", []) or []
    opportunities = []

    for sig in trusted_now:
        reason = sig.get("reason", {}) or {}
        pattern_id = sig.get("pattern_id", "")
        # Extract symbol from pattern_id (format: SYMBOL_MARKETTYPE_INTERVAL or similar)
        symbol = pattern_id.split("_")[0] if pattern_id else "—"

        # Determine side from pattern or default
        # Patterns typically indicate direction in name or we infer from return
        avg_return = reason.get("avg_return", 0) or 0
        side = "BUY" if avg_return >= 0 else "SELL"

        opportunities.append({
            "pattern_id": pattern_id,
            "symbol": symbol,
            "side": side,
            "market_type": sig.get("market_type", "—"),
            "interval_minutes": sig.get("interval_minutes"),
            "horizon_bars": sig.get("horizon_bars"),
            "trust_label": sig.get("trust_label", "TRUSTED"),
            "recommended_action": sig.get("recommended_action", "ENABLE"),
            "why": _format_why(reason),
            "confidence": _format_confidence(reason),
        })

    return opportunities


def _format_why(reason: dict) -> str:
    """Format a simple 'why' string from reason data."""
    if not reason:
        return "Meets trust criteria"
    parts = []
    if reason.get("n_success"):
        parts.append(f"{reason['n_success']} successful outcomes")
    if reason.get("avg_return") is not None:
        pct = reason["avg_return"] * 100
        parts.append(f"{pct:+.2f}% avg return")
    if reason.get("hit_rate") is not None:
        parts.append(f"{reason['hit_rate']*100:.0f}% hit rate")
    return ", ".join(parts) if parts else "Meets trust criteria"


def _format_confidence(reason: dict) -> str:
    """Determine confidence label from reason metrics."""
    n_success = reason.get("n_success", 0) or 0
    coverage = reason.get("coverage_rate", 0) or 0
    if n_success >= 50 and coverage >= 0.85:
        return "HIGH"
    elif n_success >= 30 and coverage >= 0.7:
        return "MEDIUM"
    return "LOW"


def _build_risk(brief_json: dict, risk_gate: dict, profile: dict) -> dict:
    """Build risk & guardrails section."""
    risk = brief_json.get("risk", {}).get("latest", {}) or {}

    # Profile thresholds
    drawdown_stop_pct = profile.get("drawdown_stop_pct", 0.10) if profile else 0.10
    max_positions = profile.get("max_positions") if profile else None
    max_position_pct = profile.get("max_position_pct") if profile else None
    bust_equity_pct = profile.get("bust_equity_pct") if profile else None

    # Current state
    max_drawdown = risk.get("max_drawdown")
    risk_status = risk.get("risk_status", "OK")
    entries_blocked = risk_gate.get("entries_blocked", False) if risk_gate else False
    open_positions = risk_gate.get("open_positions", 0) if risk_gate else 0

    # Build action items
    actions = []
    if entries_blocked:
        actions.append("Wait for positions to close before new entries.")
    if risk_status == "WARN":
        actions.append("Consider reducing position sizes or taking profits.")
    if max_drawdown and drawdown_stop_pct and max_drawdown >= drawdown_stop_pct * 0.8:
        remaining = (drawdown_stop_pct - max_drawdown) * 100
        actions.append(f"Drawdown within {remaining:.1f}% of stop threshold.")
    if not actions:
        actions.append("No immediate actions required.")

    return {
        "current_state": {
            "risk_status": risk_status,
            "max_drawdown": max_drawdown,
            "max_drawdown_pct": f"{max_drawdown*100:.2f}%" if max_drawdown else "—",
            "entries_blocked": entries_blocked,
            "open_positions": open_positions,
            "drawdown_stop_ts": risk.get("drawdown_stop_ts"),
        },
        "thresholds": {
            "drawdown_stop_pct": drawdown_stop_pct,
            "drawdown_stop_label": f"{drawdown_stop_pct*100:.0f}%",
            "max_positions": max_positions,
            "max_position_pct": max_position_pct,
            "max_position_pct_label": f"{max_position_pct*100:.0f}%" if max_position_pct else None,
            "bust_equity_pct": bust_equity_pct,
            "bust_equity_pct_label": f"{bust_equity_pct*100:.0f}%" if bust_equity_pct else None,
        },
        "actions": actions,
    }


def _build_deltas(brief_json: dict, prev_brief: dict) -> dict:
    """Build deltas section comparing current vs previous brief."""
    portfolio = brief_json.get("portfolio", {}) or {}
    kpis = portfolio.get("kpis", {}) or {}
    exposure = portfolio.get("exposure", {}) or {}
    signals = brief_json.get("signals", {}) or {}
    changes = signals.get("changes", {}) or {}

    # Get delta data from changes if available
    delta_section = changes.get("delta", {}) or {}

    has_prior = prev_brief is not None

    # Extract deltas from kpis and exposure (they already have curr/prev/delta structure)
    return {
        "has_prior_brief": has_prior,
        "prior_as_of_ts": changes.get("prev_meta", {}).get("prev_as_of_ts") if has_prior else None,
        "equity": {
            "curr": exposure.get("total_equity", {}).get("curr"),
            "prev": exposure.get("total_equity", {}).get("prev"),
            "delta": exposure.get("total_equity", {}).get("delta"),
        },
        "total_return": {
            "curr": kpis.get("total_return", {}).get("curr"),
            "prev": kpis.get("total_return", {}).get("prev"),
            "delta": kpis.get("total_return", {}).get("delta"),
        },
        "max_drawdown": {
            "curr": kpis.get("max_drawdown", {}).get("curr"),
            "prev": kpis.get("max_drawdown", {}).get("prev"),
            "delta": kpis.get("max_drawdown", {}).get("delta"),
        },
        "open_positions": {
            "curr": exposure.get("open_positions", {}).get("curr"),
            "prev": exposure.get("open_positions", {}).get("prev"),
            "delta": exposure.get("open_positions", {}).get("delta"),
        },
        "trusted_signals": {
            "added": delta_section.get("trusted_added", []),
            "removed": delta_section.get("trusted_removed", []),
        },
    }


@router.get("/latest")
def get_latest_brief(portfolio_id: int):
    """
    Latest morning brief for portfolio from MIP.AGENT_OUT.MORNING_BRIEF.
    Returns normalized structure with summary, opportunities, risk, deltas, and raw_json.
    """
    # Main query to get brief + risk gate state + profile thresholds
    sql = """
    with latest_brief as (
        select
            mb.PORTFOLIO_ID,
            coalesce(
                try_cast(mb.BRIEF:as_of_ts::varchar as timestamp_ntz),
                try_cast(get_path(mb.BRIEF, 'attribution.as_of_ts')::varchar as timestamp_ntz),
                mb.AS_OF_TS
            ) as as_of_ts,
            coalesce(
                get_path(mb.BRIEF, 'attribution.pipeline_run_id')::varchar,
                mb.PIPELINE_RUN_ID
            ) as pipeline_run_id,
            mb.AGENT_NAME,
            mb.BRIEF
        from MIP.AGENT_OUT.MORNING_BRIEF mb
        where mb.PORTFOLIO_ID = %s
          and coalesce(mb.AGENT_NAME, '') = 'MORNING_BRIEF'
        order by mb.AS_OF_TS desc
        limit 1
    ),
    prev_brief as (
        select
            mb.PORTFOLIO_ID,
            mb.AS_OF_TS,
            mb.BRIEF
        from MIP.AGENT_OUT.MORNING_BRIEF mb
        where mb.PORTFOLIO_ID = %s
          and coalesce(mb.AGENT_NAME, '') = 'MORNING_BRIEF'
        order by mb.AS_OF_TS desc
        limit 1 offset 1
    )
    select
        lb.PORTFOLIO_ID as portfolio_id,
        lb.as_of_ts,
        lb.pipeline_run_id,
        lb.AGENT_NAME as agent_name,
        lb.BRIEF as brief_json,
        rg.ENTRIES_BLOCKED as entries_blocked,
        rg.BLOCK_REASON as block_reason,
        rg.RISK_STATUS as risk_status,
        rg.OPEN_POSITIONS as open_positions,
        rg.MAX_DRAWDOWN as max_drawdown_current,
        pp.DRAWDOWN_STOP_PCT as drawdown_stop_pct,
        pp.MAX_POSITIONS as max_positions,
        pp.MAX_POSITION_PCT as max_position_pct,
        pp.BUST_EQUITY_PCT as bust_equity_pct,
        pp.NAME as profile_name,
        pb.AS_OF_TS as prev_as_of_ts,
        pb.BRIEF as prev_brief_json
    from latest_brief lb
    left join MIP.MART.V_PORTFOLIO_RISK_GATE rg
        on rg.PORTFOLIO_ID = lb.PORTFOLIO_ID
    left join MIP.APP.PORTFOLIO p
        on p.PORTFOLIO_ID = lb.PORTFOLIO_ID
    left join MIP.APP.PORTFOLIO_PROFILE pp
        on pp.PROFILE_ID = p.PROFILE_ID
    left join prev_brief pb
        on pb.PORTFOLIO_ID = lb.PORTFOLIO_ID
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (portfolio_id, portfolio_id))
        row = cur.fetchone()
        if not row:
            return {
                "found": False,
                "message": "No brief exists yet for this portfolio.",
            }
        columns = [d[0] for d in cur.description]
        data = serialize_row(dict(zip(columns, row)))

        brief_json = data.get("brief_json") or {}
        prev_brief_json = data.get("prev_brief_json")

        # Build risk gate dict
        risk_gate = {
            "entries_blocked": data.get("entries_blocked", False),
            "block_reason": data.get("block_reason"),
            "risk_status": data.get("risk_status"),
            "open_positions": data.get("open_positions", 0),
        }

        # Build profile dict
        profile = {
            "drawdown_stop_pct": data.get("drawdown_stop_pct"),
            "max_positions": data.get("max_positions"),
            "max_position_pct": data.get("max_position_pct"),
            "bust_equity_pct": data.get("bust_equity_pct"),
            "name": data.get("profile_name"),
        }

        return {
            "found": True,
            "portfolio_id": data.get("portfolio_id"),
            "as_of_ts": data.get("as_of_ts"),
            "pipeline_run_id": data.get("pipeline_run_id"),
            "agent_name": data.get("agent_name"),
            "summary": _build_summary(brief_json, risk_gate),
            "opportunities": _build_opportunities(brief_json),
            "risk": _build_risk(brief_json, risk_gate, profile),
            "deltas": _build_deltas(brief_json, prev_brief_json),
            "raw_json": brief_json,
        }
    finally:
        conn.close()
