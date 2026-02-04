import json
from datetime import datetime

from fastapi import APIRouter

from app.db import get_connection, serialize_row


def _parse_json(value):
    """Parse JSON value - handles string, dict, or None."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}

router = APIRouter(prefix="/briefs", tags=["briefs"])


def _get_verified_trades(cur, portfolio_id: int, run_id: str, brief_executed_count: int) -> dict:
    """
    Query PORTFOLIO_TRADES to get verified trade data for this run.
    Returns verification status and actual trade rows.
    
    Returns:
    - verification_status: VERIFIED, MISMATCH, EMPTY, UNVERIFIABLE
    - verified_count: int
    - verified_trades_preview: list of trade dicts
    """
    if not run_id:
        return {
            "verification_status": "UNVERIFIABLE",
            "verified_count": 0,
            "verified_trades_preview": [],
        }
    
    try:
        # Query actual trades from PORTFOLIO_TRADES for this run
        cur.execute("""
            select
                TRADE_ID,
                SYMBOL,
                MARKET_TYPE,
                SIDE,
                QUANTITY,
                PRICE,
                NOTIONAL,
                TRADE_TS,
                RUN_ID
            from MIP.APP.PORTFOLIO_TRADES
            where PORTFOLIO_ID = %s
              and RUN_ID = %s
            order by TRADE_TS desc
            limit 10
        """, (portfolio_id, run_id))
        
        rows = cur.fetchall()
        columns = [d[0].lower() for d in cur.description]
        
        # Get total count for this run
        cur.execute("""
            select count(*) as cnt
            from MIP.APP.PORTFOLIO_TRADES
            where PORTFOLIO_ID = %s
              and RUN_ID = %s
        """, (portfolio_id, run_id))
        count_row = cur.fetchone()
        verified_count = count_row[0] if count_row else 0
        
        # Build preview list
        verified_trades_preview = []
        for row in rows:
            trade = dict(zip(columns, row))
            verified_trades_preview.append({
                "trade_id": trade.get("trade_id"),
                "symbol": trade.get("symbol"),
                "market_type": trade.get("market_type"),
                "side": trade.get("side"),
                "quantity": float(trade.get("quantity")) if trade.get("quantity") is not None else None,
                "price": float(trade.get("price")) if trade.get("price") is not None else None,
                "notional": float(trade.get("notional")) if trade.get("notional") is not None else None,
                "trade_ts": trade.get("trade_ts").isoformat() if hasattr(trade.get("trade_ts"), 'isoformat') else trade.get("trade_ts"),
                "run_id": trade.get("run_id"),
            })
        
        # Determine verification status
        if verified_count > 0:
            if verified_count == brief_executed_count:
                verification_status = "VERIFIED"
            else:
                verification_status = "MISMATCH"
        else:
            verification_status = "EMPTY"
        
        return {
            "verification_status": verification_status,
            "verified_count": verified_count,
            "verified_trades_preview": verified_trades_preview,
        }
    except Exception:
        return {
            "verification_status": "UNVERIFIABLE",
            "verified_count": 0,
            "verified_trades_preview": [],
        }


def _build_summary(brief_json: dict, risk_gate: dict, verified_trades: dict = None) -> dict:
    """
    Build executive summary from brief data and risk gate state.
    
    verified_trades dict contains:
    - verified_count: int (count from PORTFOLIO_TRADES)
    - verified_trades_preview: list (actual trades from PORTFOLIO_TRADES)
    - verification_status: str (VERIFIED, UNVERIFIABLE, EMPTY)
    """
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

    # Get executed from brief record (for comparison/fallback)
    brief_executed_trades = brief_json.get("proposals", {}).get("executed_trades", []) or []
    brief_executed_count = proposals.get("executed", 0) or len(brief_executed_trades)

    # Use verified trades if available, otherwise use brief record
    verified_trades = verified_trades or {}
    verification_status = verified_trades.get("verification_status", "UNVERIFIABLE")
    verified_count = verified_trades.get("verified_count", 0)
    verified_trades_preview = verified_trades.get("verified_trades_preview", [])

    # Determine what to display
    if verification_status == "VERIFIED":
        # We have actual trade rows from PORTFOLIO_TRADES
        executed_count = verified_count
        executed_trades_preview = verified_trades_preview
        executed_source = "MIP.APP.PORTFOLIO_TRADES"
        executed_label = "trades"  # We have actual trades
        executed_note = None
    elif verification_status == "MISMATCH":
        # Brief says X trades but PORTFOLIO_TRADES shows different count
        # Trust the actual trades table
        executed_count = verified_count
        executed_trades_preview = verified_trades_preview
        executed_source = "MIP.APP.PORTFOLIO_TRADES (brief record shows different count)"
        executed_label = "trades"
        executed_note = f"Brief record shows {brief_executed_count}, but trade table shows {verified_count}"
    elif verification_status == "EMPTY" and brief_executed_count > 0:
        # Trade history is empty but brief says there were executions
        # This likely means a reset occurred
        executed_count = brief_executed_count
        # Build preview from brief record
        executed_trades_preview = []
        for trade in brief_executed_trades[:10]:
            executed_trades_preview.append({
                "trade_id": trade.get("trade_id"),
                "symbol": trade.get("symbol"),
                "market_type": trade.get("market_type"),
                "side": trade.get("side"),
                "quantity": trade.get("quantity"),
                "price": trade.get("price"),
                "notional": trade.get("notional"),
                "trade_ts": trade.get("trade_ts"),
                "score": trade.get("score"),
            })
        executed_source = "Brief record (trade history cleared by reset)"
        executed_label = "actions"  # Not verified as trades
        executed_note = "trade history cleared by reset"
    else:
        # No verified trades, no brief record, or can't verify
        executed_count = 0
        executed_trades_preview = []
        executed_source = "No data"
        executed_label = "trades"
        executed_note = "no trades recorded" if verification_status != "UNVERIFIABLE" else "unverifiable"

    return {
        "status": status,
        "status_label": {"SAFE": "Safe", "CAUTION": "Caution", "STOPPED": "Stopped"}.get(status, status),
        "entries_allowed": not entries_blocked,
        "new_suggestions_today": len(trusted_now),
        "explanation": status_explanation,
        # Executed info
        "executed_count": executed_count,
        "executed_label": executed_label,  # "trades" or "actions"
        "executed_trades_preview": executed_trades_preview,
        "executed_trades_source": executed_source,
        "executed_trades_note": executed_note,  # Optional warning/explanation
        "verification_status": verification_status,  # VERIFIED, MISMATCH, EMPTY, UNVERIFIABLE
        "brief_record_count": brief_executed_count,  # What the brief record says
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
        # Convert to string in case it's an int
        pattern_id_str = str(pattern_id) if pattern_id else ""
        # Extract symbol from pattern_id (format: SYMBOL_MARKETTYPE_INTERVAL or similar)
        symbol = pattern_id_str.split("_")[0] if pattern_id_str and "_" in pattern_id_str else pattern_id_str or "—"

        # Determine side from pattern or default
        # Patterns typically indicate direction in name or we infer from return
        avg_return = reason.get("avg_return", 0) or 0
        side = "BUY" if avg_return >= 0 else "SELL"

        opportunities.append({
            "pattern_id": pattern_id_str,
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

    # Profile thresholds - NO hardcoded defaults, show actual DB values
    profile = profile or {}
    drawdown_stop_pct = profile.get("drawdown_stop_pct")
    max_positions = profile.get("max_positions")
    max_position_pct = profile.get("max_position_pct")
    bust_equity_pct = profile.get("bust_equity_pct")
    profile_name = profile.get("name")

    # Current state
    max_drawdown = risk.get("max_drawdown")
    risk_status = risk.get("risk_status") or "OK"
    entries_blocked = risk_gate.get("entries_blocked", False) if risk_gate else False
    open_positions = risk_gate.get("open_positions", 0) if risk_gate else 0

    # Build action items
    actions = []
    if entries_blocked:
        actions.append("Wait for positions to close before new entries.")
    if risk_status == "WARN":
        actions.append("Consider reducing position sizes or taking profits.")
    if max_drawdown is not None and drawdown_stop_pct is not None and max_drawdown >= drawdown_stop_pct * 0.8:
        remaining = (drawdown_stop_pct - max_drawdown) * 100
        actions.append(f"Drawdown within {remaining:.1f}% of stop threshold.")
    if not actions:
        actions.append("No immediate actions required.")

    # Warning if profile not configured
    if drawdown_stop_pct is None:
        actions.insert(0, "Warning: Portfolio profile not configured. Risk thresholds unknown.")

    return {
        "current_state": {
            "risk_status": risk_status,
            "max_drawdown": max_drawdown,
            "max_drawdown_pct": f"{max_drawdown*100:.2f}%" if max_drawdown is not None else "—",
            "entries_blocked": entries_blocked,
            "open_positions": open_positions,
            "drawdown_stop_ts": risk.get("drawdown_stop_ts"),
        },
        "thresholds": {
            "profile_name": profile_name,
            "drawdown_stop_pct": drawdown_stop_pct,
            "drawdown_stop_label": f"{drawdown_stop_pct*100:.0f}%" if drawdown_stop_pct is not None else "Not configured",
            "max_positions": max_positions,
            "max_position_pct": max_position_pct,
            "max_position_pct_label": f"{max_position_pct*100:.0f}%" if max_position_pct is not None else "Not configured",
            "bust_equity_pct": bust_equity_pct,
            "bust_equity_pct_label": f"{bust_equity_pct*100:.0f}%" if bust_equity_pct is not None else "Not configured",
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
    
    Selection: Latest by CREATED_AT (not AS_OF_TS).
    Staleness: Brief is stale if its pipeline_run_id differs from the latest successful pipeline run.
              (NOT from PORTFOLIO.LAST_SIMULATION_RUN_ID, which is set by a different procedure.)
    Reset boundary: Warning if brief is from before active episode START_TS.
    """
    # Main query to get brief + risk gate state + profile thresholds + episode info
    sql = """
    with latest_brief as (
        select
            mb.PORTFOLIO_ID,
            coalesce(
                try_cast(mb.BRIEF:as_of_ts::varchar as timestamp_ntz),
                try_cast(get_path(mb.BRIEF, 'attribution.as_of_ts')::varchar as timestamp_ntz),
                mb.AS_OF_TS
            ) as as_of_ts,
            coalesce(mb.CREATED_AT, mb.AS_OF_TS) as created_at,
            coalesce(
                get_path(mb.BRIEF, 'attribution.pipeline_run_id')::varchar,
                mb.PIPELINE_RUN_ID,
                mb.RUN_ID
            ) as pipeline_run_id,
            mb.AGENT_NAME,
            mb.BRIEF
        from MIP.AGENT_OUT.MORNING_BRIEF mb
        where mb.PORTFOLIO_ID = %s
          and coalesce(mb.AGENT_NAME, '') = 'MORNING_BRIEF'
        order by coalesce(mb.CREATED_AT, mb.AS_OF_TS) desc
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
        order by coalesce(mb.CREATED_AT, mb.AS_OF_TS) desc
        limit 1 offset 1
    ),
    latest_pipeline_run as (
        -- Get the latest successful pipeline run (SUCCESS or SUCCESS_WITH_SKIPS)
        -- This is the correct source for staleness check, not PORTFOLIO.LAST_SIMULATION_RUN_ID
        select 
            RUN_ID,
            EVENT_TS
        from MIP.APP.MIP_AUDIT_LOG
        where EVENT_TYPE = 'PIPELINE'
          and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
          and STATUS in ('SUCCESS', 'SUCCESS_WITH_SKIPS')
        order by EVENT_TS desc
        limit 1
    )
    select
        lb.PORTFOLIO_ID as portfolio_id,
        lb.as_of_ts,
        lb.created_at,
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
        pb.BRIEF as prev_brief_json,
        -- Latest pipeline run for staleness check (from audit log, not PORTFOLIO table)
        lpr.RUN_ID as latest_run_id,
        lpr.EVENT_TS as latest_run_ts,
        -- Active episode info for reset boundary check
        e.EPISODE_ID as episode_id,
        e.START_TS as episode_start_ts
    from latest_brief lb
    left join MIP.MART.V_PORTFOLIO_RISK_GATE rg
        on rg.PORTFOLIO_ID = lb.PORTFOLIO_ID
    left join MIP.APP.PORTFOLIO p
        on p.PORTFOLIO_ID = lb.PORTFOLIO_ID
    left join MIP.APP.PORTFOLIO_PROFILE pp
        on pp.PROFILE_ID = p.PROFILE_ID
    left join prev_brief pb
        on pb.PORTFOLIO_ID = lb.PORTFOLIO_ID
    left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE e
        on e.PORTFOLIO_ID = lb.PORTFOLIO_ID
    cross join latest_pipeline_run lpr
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
        # Lowercase column names for consistent key access
        columns = [d[0].lower() for d in cur.description]
        data = serialize_row(dict(zip(columns, row)))

        # Parse JSON fields (Snowflake VARIANT can come back as string or dict)
        brief_json = _parse_json(data.get("brief_json"))
        prev_brief_json = _parse_json(data.get("prev_brief_json")) if data.get("prev_brief_json") else None

        # Timestamps
        as_of_ts = data.get("as_of_ts")
        created_at = data.get("created_at")
        pipeline_run_id = data.get("pipeline_run_id")

        # Staleness check: brief is stale if RUN_ID differs from portfolio's LAST_SIMULATION_RUN_ID
        latest_run_id = data.get("latest_run_id")
        latest_run_ts = data.get("latest_run_ts")
        is_stale = False
        stale_reason = None
        if latest_run_id and pipeline_run_id and str(pipeline_run_id) != str(latest_run_id):
            is_stale = True
            stale_reason = f"Brief from run {pipeline_run_id[:8]}... but latest run is {latest_run_id[:8]}..."

        # Reset boundary check: brief is from before reset if CREATED_AT < episode_start_ts
        # Note: Use CREATED_AT (when brief was generated), not AS_OF_TS (market date).
        # A brief generated after reset is current even if it covers a pre-reset market date.
        episode_id = data.get("episode_id")
        episode_start_ts = data.get("episode_start_ts")
        is_before_reset = False
        reset_warning = None
        if episode_start_ts and created_at:
            # Compare timestamps - need to handle string/datetime comparison
            try:
                ep_start = episode_start_ts if isinstance(episode_start_ts, datetime) else datetime.fromisoformat(str(episode_start_ts).replace('Z', '+00:00'))
                brief_generated = created_at if isinstance(created_at, datetime) else datetime.fromisoformat(str(created_at).replace('Z', '+00:00'))
                if brief_generated < ep_start:
                    is_before_reset = True
                    reset_warning = "This brief is from before the last portfolio reset. Trades and events may have been cleared."
            except (ValueError, TypeError):
                pass  # If comparison fails, don't show warning

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

        # Get brief record's executed count for comparison
        proposals = brief_json.get("proposals", {}).get("summary", {}) or {}
        brief_executed_trades = brief_json.get("proposals", {}).get("executed_trades", []) or []
        brief_executed_count = proposals.get("executed", 0) or len(brief_executed_trades)

        # Verify executed trades against actual PORTFOLIO_TRADES table
        verified_trades = _get_verified_trades(cur, portfolio_id, pipeline_run_id, brief_executed_count)

        # Build summary with verified trades info
        summary = _build_summary(brief_json, risk_gate, verified_trades)
        
        # Add reset context if applicable
        if is_before_reset:
            if summary.get("verification_status") == "EMPTY":
                summary["executed_trades_note"] = "trade history cleared by reset"
            elif summary.get("executed_count", 0) > 0:
                summary["executed_trades_note"] = (summary.get("executed_trades_note") or "") + " (brief from before reset)"

        # Portfolio actions: current state from actual portfolio tables (not brief record)
        # This provides ground truth for "what actually happened" vs brief content
        portfolio_actions = None
        try:
            # Get open positions count from canonical view
            cur.execute("""
                select count(*) as open_positions
                from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL
                where PORTFOLIO_ID = %s
            """, (portfolio_id,))
            pos_row = cur.fetchone()
            open_positions = pos_row[0] if pos_row else 0
            
            # Get trades count for latest run
            cur.execute("""
                select 
                    count(*) as trades_today,
                    max(TRADE_TS) as last_trade_ts
                from MIP.APP.PORTFOLIO_TRADES
                where PORTFOLIO_ID = %s
                  and RUN_ID = %s
            """, (portfolio_id, pipeline_run_id))
            trades_row = cur.fetchone()
            trades_today = trades_row[0] if trades_row else 0
            last_trade_ts = trades_row[1] if trades_row and len(trades_row) > 1 else None
            if last_trade_ts and hasattr(last_trade_ts, 'isoformat'):
                last_trade_ts = last_trade_ts.isoformat()
            
            # Get latest simulation run from portfolio
            cur.execute("""
                select LAST_SIMULATION_RUN_ID, LAST_SIMULATED_AT
                from MIP.APP.PORTFOLIO
                where PORTFOLIO_ID = %s
            """, (portfolio_id,))
            port_row = cur.fetchone()
            last_sim_run_id = port_row[0] if port_row else None
            last_sim_at = port_row[1] if port_row and len(port_row) > 1 else None
            if last_sim_at and hasattr(last_sim_at, 'isoformat'):
                last_sim_at = last_sim_at.isoformat()
            
            portfolio_actions = {
                "open_positions": open_positions,
                "trades_this_run": trades_today,
                "last_trade_ts": last_trade_ts,
                "last_simulation_run_id": last_sim_run_id,
                "last_simulated_at": last_sim_at,
            }
        except Exception:
            portfolio_actions = None

        return {
            "found": True,
            "portfolio_id": data.get("portfolio_id"),
            "as_of_ts": as_of_ts,
            "created_at": created_at,
            "pipeline_run_id": pipeline_run_id,
            "agent_name": data.get("agent_name"),
            # Staleness info
            "is_stale": is_stale,
            "stale_reason": stale_reason,
            "latest_run_id": latest_run_id,
            "latest_run_ts": latest_run_ts,
            # Reset boundary info
            "is_before_reset": is_before_reset,
            "reset_warning": reset_warning,
            "episode_id": episode_id,
            "episode_start_ts": episode_start_ts,
            # Content
            "summary": summary,
            "opportunities": _build_opportunities(brief_json),
            "risk": _build_risk(brief_json, risk_gate, profile),
            "deltas": _build_deltas(brief_json, prev_brief_json),
            "raw_json": brief_json,
            # Portfolio actions (ground truth from actual tables)
            "portfolio_actions": portfolio_actions,
        }
    finally:
        conn.close()
