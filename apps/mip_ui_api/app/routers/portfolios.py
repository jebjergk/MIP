from datetime import date, datetime, timedelta

from fastapi import APIRouter, HTTPException, Query

from app.db import get_connection, fetch_all, serialize_rows, serialize_row

router = APIRouter(prefix="/portfolios", tags=["portfolios"])


def _compute_health_state(last_run_ts, outcomes_updated_ts, stale_hours: int = 24):
    """
    Compute health state based on freshness thresholds.
    - OK: last run within threshold AND outcomes updated within threshold
    - STALE: data is older than threshold
    - BROKEN: no data or very old
    """
    now = datetime.utcnow()
    threshold = timedelta(hours=stale_hours)
    broken_threshold = timedelta(hours=stale_hours * 3)  # 72h for BROKEN
    
    # Parse timestamps if needed
    if last_run_ts and isinstance(last_run_ts, str):
        try:
            last_run_ts = datetime.fromisoformat(last_run_ts.replace('Z', '+00:00').replace('+00:00', ''))
        except ValueError:
            last_run_ts = None
    if outcomes_updated_ts and isinstance(outcomes_updated_ts, str):
        try:
            outcomes_updated_ts = datetime.fromisoformat(outcomes_updated_ts.replace('Z', '+00:00').replace('+00:00', ''))
        except ValueError:
            outcomes_updated_ts = None
    
    # No run data at all
    if last_run_ts is None:
        return {
            "health_state": "BROKEN",
            "health_reason": "No pipeline run recorded",
        }
    
    run_age = now - last_run_ts
    
    # Check if BROKEN (very old)
    if run_age > broken_threshold:
        return {
            "health_state": "BROKEN",
            "health_reason": f"Last run {run_age.days}d ago (threshold: {broken_threshold.days}d)",
        }
    
    # Check if STALE
    if run_age > threshold:
        return {
            "health_state": "STALE",
            "health_reason": f"Last run {run_age.total_seconds() / 3600:.0f}h ago",
        }
    
    # OK - within threshold
    return {
        "health_state": "OK",
        "health_reason": f"Last run {run_age.total_seconds() / 3600:.0f}h ago",
    }


@router.get("")
def list_portfolios():
    """
    Portfolio Control Tower: list all portfolios with gate state, health state, equity, and cumulative paid out.
    
    Gate: Risk regime (SAFE/CAUTION/STOPPED) based on drawdown and risk gate.
    Health: System/data freshness (OK/STALE/BROKEN) based on last run timestamps.
    """
    sql = """
    with episode_payouts as (
        select
            r.PORTFOLIO_ID,
            sum(coalesce(r.DISTRIBUTION_AMOUNT, 0)) as total_paid_out
        from MIP.APP.PORTFOLIO_EPISODE_RESULTS r
        group by r.PORTFOLIO_ID
    )
    select
        p.PORTFOLIO_ID,
        p.NAME,
        p.STATUS,
        p.LAST_SIMULATED_AT,
        p.LAST_SIMULATION_RUN_ID,
        p.PROFILE_ID,
        p.STARTING_CASH,
        p.FINAL_EQUITY,
        p.TOTAL_RETURN,
        p.MAX_DRAWDOWN,
        -- Gate state from risk gate view
        case
            when coalesce(g.ENTRIES_BLOCKED, false) then 'STOPPED'
            when g.RISK_STATUS = 'WARN' then 'CAUTION'
            else 'SAFE'
        end as GATE_STATE,
        g.BLOCK_REASON as GATE_REASON,
        -- Active episode
        e.EPISODE_ID as ACTIVE_EPISODE_ID,
        e.START_TS as ACTIVE_EPISODE_START_TS,
        e.PROFILE_ID as ACTIVE_EPISODE_PROFILE_ID,
        -- Cumulative paid out
        coalesce(ep.total_paid_out, 0) as TOTAL_PAID_OUT
    from MIP.APP.PORTFOLIO p
    left join MIP.MART.V_PORTFOLIO_RISK_GATE g on g.PORTFOLIO_ID = p.PORTFOLIO_ID
    left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE e on e.PORTFOLIO_ID = p.PORTFOLIO_ID
    left join episode_payouts ep on ep.PORTFOLIO_ID = p.PORTFOLIO_ID
    order by p.PORTFOLIO_ID
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = fetch_all(cur)
        rows = serialize_rows(rows)
        
        result = []
        for row in rows:
            # Active episode
            ep_id = row.get("ACTIVE_EPISODE_ID") or row.get("active_episode_id")
            ep_ts = row.get("ACTIVE_EPISODE_START_TS") or row.get("active_episode_start_ts")
            ep_prof = row.get("ACTIVE_EPISODE_PROFILE_ID") or row.get("active_episode_profile_id")
            if ep_id is not None or ep_ts is not None or ep_prof is not None:
                row["active_episode"] = {
                    "episode_id": ep_id,
                    "start_ts": ep_ts if isinstance(ep_ts, str) else (ep_ts.isoformat() if hasattr(ep_ts, "isoformat") else ep_ts),
                    "profile_id": ep_prof,
                }
            else:
                row["active_episode"] = None
            
            # Health state based on last run timestamp
            last_run_ts = row.get("LAST_SIMULATED_AT") or row.get("last_simulated_at")
            health = _compute_health_state(last_run_ts, None)
            row["health_state"] = health["health_state"]
            row["health_reason"] = health["health_reason"]
            
            # Gate tooltip
            gate_state = (row.get("GATE_STATE") or row.get("gate_state") or "SAFE").upper()
            gate_reason = row.get("GATE_REASON") or row.get("gate_reason")
            if gate_state == "STOPPED":
                row["gate_tooltip"] = f"Entries blocked: {gate_reason or 'drawdown stop active'}. Only exits allowed."
            elif gate_state == "CAUTION":
                row["gate_tooltip"] = "Drawdown approaching threshold. Consider reducing exposure."
            else:
                row["gate_tooltip"] = "Portfolio operating normally. Entries allowed."
            
            # Ensure numeric fields are present
            row["latest_equity"] = row.get("FINAL_EQUITY") or row.get("final_equity") or 0
            row["total_paid_out"] = row.get("TOTAL_PAID_OUT") or row.get("total_paid_out") or 0
            
            result.append(row)
        
        return result
    finally:
        conn.close()


@router.get("/{portfolio_id}")
def get_portfolio(portfolio_id: int):
    """Portfolio header: all columns from MIP.APP.PORTFOLIO."""
    sql = """
    select
        PORTFOLIO_ID,
        PROFILE_ID,
        NAME,
        BASE_CURRENCY,
        STARTING_CASH,
        LAST_SIMULATION_RUN_ID,
        LAST_SIMULATED_AT,
        FINAL_EQUITY,
        TOTAL_RETURN,
        MAX_DRAWDOWN,
        WIN_DAYS,
        LOSS_DAYS,
        STATUS,
        BUST_AT,
        NOTES,
        CREATED_AT,
        UPDATED_AT
    from MIP.APP.PORTFOLIO
    where PORTFOLIO_ID = %s
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (portfolio_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Portfolio not found")
        columns = [d[0] for d in cur.description]
        return serialize_row(dict(zip(columns, row)))
    finally:
        conn.close()


def _first(d: list) -> dict | None:
    """First element of list or None."""
    return d[0] if d else None


def _enrich_positions_side(positions: list[dict]) -> list[dict]:
    """Add side (BUY/SELL/FLAT) and side_label (Long/Short/Flat) from QUANTITY."""
    for row in positions:
        q = _get(row, "QUANTITY", "quantity")
        if q is None:
            row["side"] = "FLAT"
            row["side_label"] = "Flat"
        else:
            try:
                n = float(q)
                if n > 0:
                    row["side"] = "BUY"
                    row["side_label"] = "Long"
                elif n < 0:
                    row["side"] = "SELL"
                    row["side_label"] = "Short"
                else:
                    row["side"] = "FLAT"
                    row["side_label"] = "Flat"
            except (TypeError, ValueError):
                row["side"] = "FLAT"
                row["side_label"] = "Flat"
    return positions


def _enrich_positions_hold_until_ts(positions: list[dict], cur) -> list[dict]:
    """
    Resolve HOLD_UNTIL_INDEX to bar date (hold_until_ts) using MIP.MART.V_BAR_INDEX.TS.
    Query by (SYMBOL, BAR_INDEX) only with INTERVAL_MINUTES=1440 so we don't depend on MARKET_TYPE.
    """
    keys_to_lookup = []
    for row in positions:
        s = _get(row, "SYMBOL", "symbol")
        h = _get(row, "HOLD_UNTIL_INDEX", "hold_until_index")
        if s is not None and h is not None:
            try:
                bar = int(float(h))
                keys_to_lookup.append((s, bar))
            except (TypeError, ValueError):
                pass
    if not keys_to_lookup:
        return positions
    seen = set()
    unique_keys = []
    for t in keys_to_lookup:
        if t not in seen:
            seen.add(t)
            unique_keys.append(t)
    placeholders = ", ".join(["(%s, %s)"] * len(unique_keys))
    params = []
    for (s, b) in unique_keys:
        params.extend([s, b])
    try:
        cur.execute(
            f"""
            select SYMBOL, BAR_INDEX, TS
            from MIP.MART.V_BAR_INDEX
            where INTERVAL_MINUTES = 1440 and (SYMBOL, BAR_INDEX) in ({placeholders})
            qualify row_number() over (partition by SYMBOL, BAR_INDEX order by TS) = 1
            """,
            params,
        )
        rows = fetch_all(cur)
        key_to_ts = {}
        for r in rows:
            s_ = r.get("SYMBOL") or r.get("symbol")
            b_ = r.get("BAR_INDEX") or r.get("bar_index")
            ts_ = r.get("TS") or r.get("ts")
            if s_ is not None and b_ is not None and ts_ is not None:
                try:
                    k = (s_, int(float(b_)))
                    if hasattr(ts_, "isoformat"):
                        ts_ = ts_.isoformat()
                    key_to_ts[k] = ts_
                except (TypeError, ValueError):
                    pass
        for row in positions:
            s = _get(row, "SYMBOL", "symbol")
            h = _get(row, "HOLD_UNTIL_INDEX", "hold_until_index")
            if s is None or h is None:
                continue
            try:
                k = (s, int(float(h)))
                if k in key_to_ts:
                    row["hold_until_ts"] = key_to_ts[k]
            except (TypeError, ValueError):
                pass
    except Exception:
        pass
    return positions


def _position_still_open_by_date(position: dict) -> bool:
    """
    True if position should be shown as open: no hold_until_ts (unresolved) or hold_until date is today or in the future.
    Filters out positions whose hold-until bar date is in the past (canonical view can be stale).
    """
    ts = position.get("hold_until_ts")
    if ts is None:
        return True
    try:
        hold_date = str(ts)[:10]
        return hold_date >= date.today().isoformat()
    except Exception:
        return True


def _get(row: dict | None, *keys: str):
    """First value from row for any of the given keys (case-insensitive fallback)."""
    if not row:
        return None
    for k in keys:
        v = row.get(k)
        if v is not None:
            return v
        # Try lowercase
        v = row.get(k.lower()) if isinstance(k, str) else None
        if v is not None:
            return v
    return None


def _normalize_risk_gate(risk_gate_row: dict | None, risk_state_row: dict | None) -> dict:
    """
    Build normalized risk_gate object from V_PORTFOLIO_RISK_GATE / V_PORTFOLIO_RISK_STATE.
    Deterministic mapping; no raw codes exposed. Plain-language labels and what_to_do_now bullets.
    """
    # Prefer risk_state (has ALLOWED_ACTIONS, STOP_REASON); fallback to risk_gate
    r = risk_state_row or risk_gate_row or {}
    entries_blocked = _get(r, "ENTRIES_BLOCKED", "entries_blocked")
    entries_blocked = bool(entries_blocked) if entries_blocked is not None else False
    allowed_actions = _get(r, "ALLOWED_ACTIONS", "allowed_actions") or "ALLOW_ENTRIES"
    risk_status = _get(r, "RISK_STATUS", "risk_status") or "OK"
    block_reason = _get(r, "BLOCK_REASON", "block_reason")
    stop_reason = _get(r, "STOP_REASON", "stop_reason")

    entries_allowed = not entries_blocked
    exits_allowed = True
    mode = allowed_actions if allowed_actions in ("ALLOW_ENTRIES", "ALLOW_EXITS_ONLY") else "ALLOW_ENTRIES"

    if entries_blocked or (allowed_actions == "ALLOW_EXITS_ONLY"):
        risk_label = "DEFENSIVE"
    elif risk_status == "WARN":
        risk_label = "CAUTION"
    else:
        risk_label = "NORMAL"

    reason_code = block_reason if block_reason is not None else (stop_reason if stop_reason is not None else None)

    if reason_code == "DRAWDOWN_STOP_ACTIVE":
        reason_text = "We hit the portfolio loss limit, so we pause new entries."
    elif reason_code == "ALLOW_EXITS_ONLY":
        reason_text = "Safety mode: you can exit positions, but not open new ones."
    else:
        reason_text = "Portfolio is within safe limits."

    what_to_do_now = []
    if risk_label == "NORMAL":
        what_to_do_now.append("You can open new positions normally.")
        what_to_do_now.append("Use Suggestions to focus on the strongest ideas.")
    elif risk_label == "CAUTION":
        what_to_do_now.append("New positions are allowed, but be cautious.")
        what_to_do_now.append("Prefer higher maturity signals (more tested).")
        what_to_do_now.append("Avoid opening many new positions at once.")
    else:
        what_to_do_now.append("Do not open new positions (paused for safety).")
        what_to_do_now.append("You can still close or reduce existing positions.")
        what_to_do_now.append("Wait for the portfolio to stabilize, or restart the episode if needed.")
        if reason_code == "DRAWDOWN_STOP_ACTIVE":
            what_to_do_now.append("This was triggered by drawdown protection.")

    debug = {
        "block_reason": block_reason,
        "stop_reason": stop_reason,
        "entries_blocked": entries_blocked,
    }

    return {
        "risk_label": risk_label,
        "risk_status": risk_status,
        "entries_allowed": entries_allowed,
        "exits_allowed": exits_allowed,
        "mode": mode,
        "reason_code": reason_code,
        "reason_text": reason_text,
        "what_to_do_now": what_to_do_now,
        "debug": debug,
    }


def _format_pct(value) -> str | None:
    """Format a decimal fraction as percentage string (e.g. 0.1 -> '10%')."""
    if value is None:
        return None
    try:
        n = float(value)
        return f"{round(n * 100)}%"
    except (TypeError, ValueError):
        return None


def _build_risk_strategy(
    profile_row: dict | None,
    risk_gate_normalized: dict,
) -> dict:
    """
    Build render-ready risk_strategy from PORTFOLIO_PROFILE row and normalized risk_gate.
    Only includes rules for which the profile has a non-null value.
    """
    state_label = "SAFE"
    rl = risk_gate_normalized.get("risk_label")
    if rl == "DEFENSIVE":
        state_label = "STOPPED"
    elif rl == "CAUTION":
        state_label = "WARNING"
    elif rl == "NORMAL":
        state_label = "SAFE"

    state = {
        "state_label": state_label,
        "reason_label": risk_gate_normalized.get("reason_code"),
        "reason_text": risk_gate_normalized.get("reason_text") or "Portfolio is within safe limits.",
    }

    # Treat as "no profile" only if we have no row or neither profile_id nor profile name (join failed or portfolio not linked)
    profile_id = _get(profile_row, "PROFILE_ID", "profile_id") if profile_row else None
    profile_name = _get(profile_row, "NAME", "name") if profile_row else None
    if not profile_row or (profile_id is None and profile_name is None):
        return {
            "profile_id": None,
            "profile_name": None,
            "summary": "No risk profile linked. Thresholds are not shown.",
            "rules": [],
            "state": state,
        }

    profile_id = profile_id or _get(profile_row, "PROFILE_ID", "profile_id")
    profile_name = profile_name or _get(profile_row, "NAME", "name")
    description = _get(profile_row, "DESCRIPTION", "description")
    summary = (description and str(description).strip()) or (
        "This profile sets limits on drawdown, bust level, and position size."
    )

    rules: list[dict] = []

    # Drawdown stop
    v = _get(profile_row, "DRAWDOWN_STOP_PCT", "drawdown_stop_pct")
    if v is not None:
        formatted = _format_pct(v)
        if formatted:
            rules.append({
                "key": "drawdown_stop_pct",
                "label": "Drawdown stop",
                "value": formatted,
                "tooltip": "Maximum loss from peak before new entries are paused.",
            })

    # Bust threshold
    v = _get(profile_row, "BUST_EQUITY_PCT", "bust_equity_pct")
    if v is not None:
        formatted = _format_pct(v)
        if formatted:
            rules.append({
                "key": "bust_equity_pct",
                "label": "Bust threshold",
                "value": formatted,
                "tooltip": "Equity level (vs starting) that triggers bust; trading may be stopped.",
            })

    # Max positions
    v = _get(profile_row, "MAX_POSITIONS", "max_positions")
    if v is not None:
        try:
            n = int(float(v))
            rules.append({
                "key": "max_positions",
                "label": "Max positions",
                "value": str(n),
                "tooltip": "Maximum number of open positions allowed.",
            })
        except (TypeError, ValueError):
            pass

    # Max position %
    v = _get(profile_row, "MAX_POSITION_PCT", "max_position_pct")
    if v is not None:
        formatted = _format_pct(v)
        if formatted:
            rules.append({
                "key": "max_position_pct",
                "label": "Max position %",
                "value": formatted,
                "tooltip": "Maximum share of portfolio value in a single position.",
            })

    # Bust action (optional; human-readable)
    v = _get(profile_row, "BUST_ACTION", "bust_action")
    if v is not None and str(v).strip():
        action_label = str(v).replace("_", " ").title()
        if v == "ALLOW_EXITS_ONLY":
            action_label = "Exits only"
        elif v == "LIQUIDATE_NEXT_BAR":
            action_label = "Liquidate next bar"
        elif v == "LIQUIDATE_IMMEDIATE":
            action_label = "Liquidate immediately"
        rules.append({
            "key": "bust_action",
            "label": "Bust action",
            "value": action_label,
            "tooltip": "What the system does when the bust threshold is hit.",
        })

    return {
        "profile_id": profile_id,
        "profile_name": profile_name,
        "summary": summary,
        "rules": rules,
        "state": state,
    }


# =============================================================================
# Run-ID Semantics (IMPORTANT):
# =============================================================================
# There are TWO different run-id formats in the system:
#
# 1. SIMULATION RUN IDs (UUIDs):
#    - Format: UUID like 'a6c97afb-ec42-4652-8015-2aca2ba8ed85'
#    - Used in: PORTFOLIO_TRADES, PORTFOLIO_POSITIONS, PORTFOLIO_DAILY, MIP_AUDIT_LOG
#    - Source: Generated by SP_RUN_DAILY_PIPELINE and stored in PORTFOLIO.LAST_SIMULATION_RUN_ID
#    - This is the authoritative run-id for portfolio snapshot/positions/trades display.
#
# 2. PROPOSAL RUN IDs (Timestamp strings):
#    - Format: Timestamp string like '20260124T005306'
#    - Used in: ORDER_PROPOSALS.RUN_ID_VARCHAR
#    - Source: Signal/recommendation runs (legacy format)
#    - DO NOT use this as portfolio run anchor - proposals are shown separately.
#
# The API always uses UUID run-ids for portfolio operations. Proposals are
# displayed separately and do not affect the portfolio's "current run" state.
# =============================================================================

# Snapshot semantics:
# - pos_as_of_col: AS_OF_TS in V_PORTFOLIO_OPEN_POSITIONS_CANONICAL (latest bar snapshot)
# - trade_ts_col: TRADE_TS in PORTFOLIO_TRADES
# - run_id_col: RUN_ID in both tables (UUID format)
# - open_filter: use canonical view (IS_OPEN) so we return only open positions


@router.get("/{portfolio_id}/snapshot")
def get_portfolio_snapshot(
    portfolio_id: int,
    run_id: str | None = None,
    lookback_days: int = Query(30, ge=-1, description="Days to look back (trade_ts_col); use -1 for all"),
):
    """
    Combined read: latest open positions (canonical view), trades by lookback, daily, KPIs, risk + cards.
    Positions: only from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL (single source of truth for open vs closed).
    Trades: trade_ts_col=TRADE_TS, run_id_col=RUN_ID. Filter by lookback_days (default 30); return trades_total, last_trade_ts.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Resolve effective run for trades/daily/kpis (run_id param, or latest from portfolio)
        # Priority: 1) run_id param, 2) LAST_SIMULATION_RUN_ID, 3) PORTFOLIO_DAILY, 4) PORTFOLIO_TRADES, 5) PORTFOLIO_POSITIONS
        effective_run_id = run_id
        if effective_run_id is None:
            # Try LAST_SIMULATION_RUN_ID first (most authoritative)
            cur.execute(
                "select LAST_SIMULATION_RUN_ID from MIP.APP.PORTFOLIO where PORTFOLIO_ID = %s",
                (portfolio_id,),
            )
            row = cur.fetchone()
            if row and row[0]:
                effective_run_id = row[0]
        
        if effective_run_id is None:
            # Fallback to PORTFOLIO_DAILY
            cur.execute(
                """
                select RUN_ID from MIP.APP.PORTFOLIO_DAILY
                where PORTFOLIO_ID = %s
                order by TS desc
                limit 1
                """,
                (portfolio_id,),
            )
            r = cur.fetchone()
            if r and r[0]:
                effective_run_id = r[0]
        
        if effective_run_id is None:
            # Fallback to PORTFOLIO_TRADES
            cur.execute(
                """
                select RUN_ID from MIP.APP.PORTFOLIO_TRADES
                where PORTFOLIO_ID = %s
                order by TRADE_TS desc
                limit 1
                """,
                (portfolio_id,),
            )
            r = cur.fetchone()
            if r and r[0]:
                effective_run_id = r[0]
        
        if effective_run_id is None:
            # Fallback to PORTFOLIO_POSITIONS
            cur.execute(
                """
                select RUN_ID from MIP.APP.PORTFOLIO_POSITIONS
                where PORTFOLIO_ID = %s
                order by ENTRY_TS desc
                limit 1
                """,
                (portfolio_id,),
            )
            r = cur.fetchone()
            if r and r[0]:
                effective_run_id = r[0]

        # Open positions: only from canonical view (IS_OPEN = true by definition there).
        # MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL is the single source of truth for which positions are open.
        positions = []
        snapshot_ts = None
        try:
            cur.execute(
                """
                select PORTFOLIO_ID, RUN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES,
                       ENTRY_TS, ENTRY_PRICE, QUANTITY, COST_BASIS, ENTRY_SCORE,
                       ENTRY_INDEX, HOLD_UNTIL_INDEX, AS_OF_TS, CURRENT_BAR_INDEX, IS_OPEN, OPEN_POSITIONS
                from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL
                where PORTFOLIO_ID = %s
                order by ENTRY_TS desc
                """,
                (portfolio_id,),
            )
            positions = serialize_rows(fetch_all(cur))
            if positions:
                snapshot_ts = positions[0].get("AS_OF_TS") or positions[0].get("as_of_ts")
        except Exception:
            pass
        # No fallback to PORTFOLIO_POSITIONS: that table has no IS_OPEN filter and would show closed positions.
        positions = _enrich_positions_side(positions)
        positions = _enrich_positions_hold_until_ts(positions, cur)

        # Trades: trade_ts_col=TRADE_TS, run_id_col=RUN_ID. Total + last_ts for portfolio; list filtered by lookback_days
        cur.execute(
            "select count(*) as n, max(TRADE_TS) as last_ts from MIP.APP.PORTFOLIO_TRADES where PORTFOLIO_ID = %s",
            (portfolio_id,),
        )
        agg_row = cur.fetchone()
        trades_total = int(agg_row[0]) if agg_row and agg_row[0] is not None else 0
        _last_ts = agg_row[1] if agg_row and len(agg_row) > 1 and agg_row[1] is not None else None
        last_trade_ts = _last_ts.isoformat() if _last_ts is not None and hasattr(_last_ts, "isoformat") else _last_ts

        if lookback_days < 0:
            cur.execute(
                """
                select * from MIP.APP.PORTFOLIO_TRADES
                where PORTFOLIO_ID = %s
                order by TRADE_TS desc
                limit 500
                """,
                (portfolio_id,),
            )
        else:
            cur.execute(
                """
                select * from MIP.APP.PORTFOLIO_TRADES
                where PORTFOLIO_ID = %s and TRADE_TS >= dateadd(day, -%s, current_timestamp())
                order by TRADE_TS desc
                limit 500
                """,
                (portfolio_id, lookback_days),
            )
        trades = serialize_rows(fetch_all(cur))

        # Daily: same run
        cur.execute(
            """
            select * from MIP.APP.PORTFOLIO_DAILY
            where PORTFOLIO_ID = %s and (%s is null or RUN_ID = %s)
            order by TS desc
            """,
            (portfolio_id, effective_run_id, effective_run_id),
        )
        daily = serialize_rows(fetch_all(cur))

        # KPIs: same run
        cur.execute(
            """
            select * from MIP.MART.V_PORTFOLIO_RUN_KPIS
            where PORTFOLIO_ID = %s and (%s is null or RUN_ID = %s)
            order by TO_TS desc
            """,
            (portfolio_id, effective_run_id, effective_run_id),
        )
        kpis = serialize_rows(fetch_all(cur))

        # Risk (gate)
        cur.execute(
            "select * from MIP.MART.V_PORTFOLIO_RISK_GATE where PORTFOLIO_ID = %s",
            (portfolio_id,),
        )
        risk_gate_rows = serialize_rows(fetch_all(cur))

        # Risk (state)
        cur.execute(
            "select * from MIP.MART.V_PORTFOLIO_RISK_STATE where PORTFOLIO_ID = %s",
            (portfolio_id,),
        )
        risk_state = serialize_rows(fetch_all(cur))

        # Normalized risk_gate (plain-language; no raw codes to UI)
        rs_first = _first(risk_state)
        rg_first = _first(risk_gate_rows)
        risk_gate_normalized = _normalize_risk_gate(rg_first, rs_first)

        # Current bar index: from canonical positions or from risk gate (when no open positions)
        current_bar_index = None
        if positions:
            current_bar_index = positions[0].get("CURRENT_BAR_INDEX") or positions[0].get("current_bar_index")
        if current_bar_index is None and rg_first:
            current_bar_index = rg_first.get("CURRENT_BAR_INDEX") or rg_first.get("current_bar_index")
        if current_bar_index is not None:
            try:
                current_bar_index = int(float(current_bar_index))
            except (TypeError, ValueError):
                current_bar_index = None

        # Positions that closed on the latest bar (hold_until reached this bar) — show in UI with different color
        closed_this_bar_positions = []
        if current_bar_index is not None:
            try:
                cur.execute(
                    """
                    select PORTFOLIO_ID, RUN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES,
                           ENTRY_TS, ENTRY_PRICE, QUANTITY, COST_BASIS, ENTRY_SCORE,
                           ENTRY_INDEX, HOLD_UNTIL_INDEX, CREATED_AT
                    from MIP.APP.PORTFOLIO_POSITIONS
                    where PORTFOLIO_ID = %s and HOLD_UNTIL_INDEX = %s
                    order by ENTRY_TS desc
                    """,
                    (portfolio_id, current_bar_index),
                )
                closed_this_bar_positions = serialize_rows(fetch_all(cur))
                closed_this_bar_positions = _enrich_positions_side(closed_this_bar_positions)
                closed_this_bar_positions = _enrich_positions_hold_until_ts(closed_this_bar_positions, cur)
                for row in closed_this_bar_positions:
                    row["closed_this_bar"] = True
            except Exception:
                pass

        # Mark trades from the latest run for UI highlighting
        for t in trades:
            t["from_last_run"] = (
                effective_run_id is not None
                and str(t.get("RUN_ID") or t.get("run_id") or "") == str(effective_run_id)
            )

        # Only show as "open" positions whose hold_until date is today or in the future (canonical can be stale)
        open_positions_filtered = [p for p in positions if _position_still_open_by_date(p)]

        # Portfolio profile (for risk strategy: thresholds from PORTFOLIO_PROFILE)
        # Only show the profile actually linked to this portfolio; no fallback to avoid showing wrong thresholds.
        profile_row = None
        try:
            cur.execute(
                """
                select prof.PROFILE_ID, prof.NAME, prof.DESCRIPTION,
                       prof.DRAWDOWN_STOP_PCT, prof.BUST_EQUITY_PCT, prof.BUST_ACTION,
                       prof.MAX_POSITIONS, prof.MAX_POSITION_PCT
                from MIP.APP.PORTFOLIO p
                left join MIP.APP.PORTFOLIO_PROFILE prof on prof.PROFILE_ID = p.PROFILE_ID
                where p.PORTFOLIO_ID = %s
                """,
                (portfolio_id,),
            )
            row = cur.fetchone()
            if row is not None and cur.description:
                cols = [d[0] for d in cur.description]
                profile_row = dict(zip(cols, row))
                profile_row = serialize_row(profile_row) if profile_row else None
        except Exception:
            pass
        risk_strategy = _build_risk_strategy(profile_row, risk_gate_normalized)

        # --- Operator-clarity cards ---
        latest_daily = _first(daily)

        cash_and_exposure = None
        if latest_daily:
            cash = latest_daily.get("CASH") or latest_daily.get("cash")
            equity_value = latest_daily.get("EQUITY_VALUE") or latest_daily.get("equity_value")
            total_equity = latest_daily.get("TOTAL_EQUITY") or latest_daily.get("total_equity")
            cash_and_exposure = {
                "cash": cash,
                "exposure": equity_value,
                "total_equity": total_equity,
                "as_of_ts": latest_daily.get("TS") or latest_daily.get("ts"),
            }
        else:
            latest_kpi = _first(kpis)
            if latest_kpi:
                fe = latest_kpi.get("FINAL_EQUITY") or latest_kpi.get("final_equity")
                cash_and_exposure = {
                    "cash": None,
                    "exposure": None,
                    "total_equity": fe,
                    "as_of_ts": snapshot_ts or latest_kpi.get("TO_TS") or latest_kpi.get("to_ts"),
                }
            elif snapshot_ts is not None:
                cash_and_exposure = {"cash": None, "exposure": None, "total_equity": None, "as_of_ts": snapshot_ts}

        risk_gate_status = {
            "entries_blocked": not risk_gate_normalized["entries_allowed"],
            "exits_allowed": risk_gate_normalized["exits_allowed"],
            "summary": risk_gate_normalized["reason_text"],
            "stop_reason": risk_gate_normalized["reason_code"],
            "risk_label": risk_gate_normalized["risk_label"],
            "what_to_do_now": risk_gate_normalized["what_to_do_now"],
        }

        cards = {
            "cash_and_exposure": cash_and_exposure,
            "open_positions": open_positions_filtered,
            "closed_this_bar_positions": closed_this_bar_positions,
            "recent_trades": trades[:20],
            "risk_gate_status": risk_gate_status,
            "snapshot_ts": snapshot_ts,
            "as_of_ts": snapshot_ts,
            "current_bar_index": current_bar_index,
            "run_id": effective_run_id,
            "trades_total": trades_total,
            "last_trade_ts": last_trade_ts,
            "lookback_days": lookback_days,
        }

        # Active episode (for evolution timeline; KPIs/risk are already episode-scoped via MART views)
        active_episode = None
        try:
            cur.execute(
                """
                select EPISODE_ID, PROFILE_ID, START_TS, 'ACTIVE' as STATUS
                from MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE
                where PORTFOLIO_ID = %s
                """,
                (portfolio_id,),
            )
            row = cur.fetchone()
            if row and cur.description:
                cols = [d[0] for d in cur.description]
                active_episode = dict(zip(cols, row))
                st = active_episode.get("START_TS")
                if st is not None and hasattr(st, "isoformat"):
                    active_episode["start_ts"] = st.isoformat()
                active_episode = serialize_row(active_episode)
        except Exception:
            pass

        return {
            "positions": open_positions_filtered,
            "closed_this_bar_positions": closed_this_bar_positions,
            "trades": trades,
            "trades_total": trades_total,
            "last_trade_ts": last_trade_ts,
            "lookback_days": lookback_days,
            "daily": daily,
            "kpis": kpis,
            "risk_gate": risk_gate_normalized,
            "risk_gate_raw": risk_gate_rows,
            "risk_state": risk_state,
            "risk_strategy": risk_strategy,
            "cards": cards,
            "active_episode": active_episode,
        }
    finally:
        conn.close()


@router.get("/{portfolio_id}/episodes")
def get_portfolio_episodes(portfolio_id: int):
    """
    List episodes (profile generations) for a portfolio, most recent first.
    Each episode includes summary stats, distribution_amount, distribution_mode from PORTFOLIO_EPISODE_RESULTS when present.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select e.EPISODE_ID, e.PORTFOLIO_ID, e.PROFILE_ID, e.START_TS, e.END_TS, e.STATUS, e.END_REASON, e.CREATED_AT,
                   e.START_EQUITY,
                   p.NAME as PROFILE_NAME,
                   r.END_EQUITY as RESULT_END_EQUITY, r.REALIZED_PNL, r.RETURN_PCT, r.MAX_DRAWDOWN_PCT,
                   r.TRADES_COUNT, r.WIN_DAYS, r.LOSS_DAYS,
                   r.DISTRIBUTION_AMOUNT, r.DISTRIBUTION_MODE, r.ENDED_AT_TS
            from MIP.APP.PORTFOLIO_EPISODE e
            left join MIP.APP.PORTFOLIO_PROFILE p on p.PROFILE_ID = e.PROFILE_ID
            left join MIP.APP.PORTFOLIO_EPISODE_RESULTS r
              on r.PORTFOLIO_ID = e.PORTFOLIO_ID and r.EPISODE_ID = e.EPISODE_ID
            where e.PORTFOLIO_ID = %s
            order by e.START_TS desc
            """,
            (portfolio_id,),
        )
        rows = fetch_all(cur)
        if not rows:
            return []
        episodes = []
        for row in rows:
            start_ts = row.get("START_TS")
            end_ts = row.get("END_TS")
            ep = {**row}
            if start_ts and hasattr(start_ts, "isoformat"):
                ep["start_ts"] = start_ts.isoformat()
            if end_ts and hasattr(end_ts, "isoformat"):
                ep["end_ts"] = end_ts.isoformat()
            # Prefer persisted results; map to response shape
            if row.get("RETURN_PCT") is not None:
                ep["total_return"] = float(row["RETURN_PCT"]) if row.get("RETURN_PCT") is not None else None
                ep["max_drawdown"] = float(row["MAX_DRAWDOWN_PCT"]) if row.get("MAX_DRAWDOWN_PCT") is not None else None
                ep["win_days"] = int(row["WIN_DAYS"]) if row.get("WIN_DAYS") is not None else None
                ep["loss_days"] = int(row["LOSS_DAYS"]) if row.get("LOSS_DAYS") is not None else None
                ep["trades_count"] = int(row["TRADES_COUNT"]) if row.get("TRADES_COUNT") is not None else None
                ep["start_equity"] = float(row["START_EQUITY"]) if row.get("START_EQUITY") is not None else None
            else:
                ep["total_return"] = None
                ep["max_drawdown"] = None
                ep["win_days"] = None
                ep["loss_days"] = None
                ep["trades_count"] = None
                ep["start_equity"] = float(row["START_EQUITY"]) if row.get("START_EQUITY") is not None else None
            ep["distribution_amount"] = float(row["DISTRIBUTION_AMOUNT"]) if row.get("DISTRIBUTION_AMOUNT") is not None else None
            ep["distribution_mode"] = row.get("DISTRIBUTION_MODE")

            # Fallback: compute from daily/trades when no results row
            if ep.get("total_return") is None and start_ts is not None:
                try:
                    cur.execute(
                        """
                        select
                            max_by(TOTAL_EQUITY, TS) as final_equity,
                            max(DRAWDOWN) as max_drawdown,
                            count_if((TOTAL_EQUITY - PREV_TOTAL_EQUITY) > 0) as win_days,
                            count_if((TOTAL_EQUITY - PREV_TOTAL_EQUITY) < 0) as loss_days
                        from (
                            select TS, TOTAL_EQUITY, DRAWDOWN,
                                lag(TOTAL_EQUITY) over (order by TS) as PREV_TOTAL_EQUITY
                            from MIP.APP.PORTFOLIO_DAILY
                            where PORTFOLIO_ID = %s and TS >= %s and (%s is null or TS <= %s)
                        )
                        """,
                        (portfolio_id, start_ts, end_ts, end_ts),
                    )
                    daily_row = cur.fetchone()
                    if daily_row and cur.description:
                        cols = [d[0] for d in cur.description]
                        d = dict(zip(cols, daily_row))
                        start_cash = ep.get("start_equity")
                        if start_cash is None:
                            cur.execute(
                                "select STARTING_CASH from MIP.APP.PORTFOLIO where PORTFOLIO_ID = %s",
                                (portfolio_id,),
                            )
                            sc = cur.fetchone()
                            start_cash = float(sc[0]) if sc and sc[0] is not None else None
                            ep["start_equity"] = start_cash
                        final_equity = d.get("FINAL_EQUITY")
                        if start_cash and final_equity and start_cash != 0:
                            ep["total_return"] = (float(final_equity) / start_cash) - 1
                        ep["max_drawdown"] = d.get("MAX_DRAWDOWN")
                        ep["win_days"] = int(d["WIN_DAYS"]) if d.get("WIN_DAYS") is not None else None
                        ep["loss_days"] = int(d["LOSS_DAYS"]) if d.get("LOSS_DAYS") is not None else None
                    cur.execute(
                        """
                        select count(*) from MIP.APP.PORTFOLIO_TRADES
                        where PORTFOLIO_ID = %s and TRADE_TS >= %s and (%s is null or TRADE_TS <= %s)
                        """,
                        (portfolio_id, start_ts, end_ts, end_ts),
                    )
                    tc = cur.fetchone()
                    ep["trades_count"] = int(tc[0]) if tc and tc[0] is not None else 0
                except Exception:
                    pass

            episodes.append(serialize_row(ep))
        return episodes
    finally:
        conn.close()


@router.get("/{portfolio_id}/timeline")
def get_portfolio_timeline(portfolio_id: int):
    """
    Cumulative evolution timeline: per-episode results and cumulative series for charts.
    Returns total_paid_out_amount (sum of distribution_amount), per_episode list, and cumulative_series.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select e.EPISODE_ID, e.START_TS, e.END_TS, e.STATUS, e.END_REASON,
                   r.START_EQUITY, r.END_EQUITY, r.REALIZED_PNL, r.RETURN_PCT,
                   r.MAX_DRAWDOWN_PCT, r.TRADES_COUNT, r.WIN_DAYS, r.LOSS_DAYS,
                   r.DISTRIBUTION_AMOUNT, r.DISTRIBUTION_MODE
            from MIP.APP.PORTFOLIO_EPISODE e
            left join MIP.APP.PORTFOLIO_EPISODE_RESULTS r
              on r.PORTFOLIO_ID = e.PORTFOLIO_ID and r.EPISODE_ID = e.EPISODE_ID
            where e.PORTFOLIO_ID = %s
            order by e.START_TS asc
            """,
            (portfolio_id,),
        )
        rows = fetch_all(cur)
        if not rows:
            return {
                "per_episode": [],
                "cumulative_series": [],
                "total_paid_out_amount": 0.0,
                "total_paid_out_series": [],
                "total_realized_pnl": 0.0,
                "episode_count": 0,
            }

        total_paid_out = 0.0
        total_realized_pnl = 0.0
        cum_distributed = 0.0
        cum_realized_pnl = 0.0
        per_episode = []
        cumulative_series = []
        total_paid_out_series = []

        for row in rows:
            dist = float(row["DISTRIBUTION_AMOUNT"] or 0)
            realized = float(row["REALIZED_PNL"] or 0)
            total_paid_out += dist
            total_realized_pnl += realized
            cum_distributed += dist
            cum_realized_pnl += realized
            start_ts = row.get("START_TS")
            end_ts = row.get("END_TS")
            status = row.get("STATUS") or "ENDED"
            end_reason = row.get("END_REASON")
            
            # Compute lifecycle status label
            if status == "ACTIVE":
                lifecycle_label = "Active"
            elif end_reason == "PROFIT_TARGET_HIT":
                lifecycle_label = "Crystallized"
            elif end_reason == "DRAWDOWN_STOP":
                lifecycle_label = "Stopped"
            elif end_reason == "MANUAL_RESET":
                lifecycle_label = "Reset"
            else:
                lifecycle_label = "Ended"
            
            per_episode.append({
                "episode_id": row["EPISODE_ID"],
                "start_ts": start_ts.isoformat() if start_ts and hasattr(start_ts, "isoformat") else None,
                "end_ts": end_ts.isoformat() if end_ts and hasattr(end_ts, "isoformat") else None,
                "status": status,
                "end_reason": end_reason,
                "lifecycle_label": lifecycle_label,
                "start_equity": float(row["START_EQUITY"]) if row.get("START_EQUITY") is not None else None,
                "end_equity": float(row["END_EQUITY"]) if row.get("END_EQUITY") is not None else None,
                "return_pct": float(row["RETURN_PCT"]) if row.get("RETURN_PCT") is not None else None,
                "max_drawdown_pct": float(row["MAX_DRAWDOWN_PCT"]) if row.get("MAX_DRAWDOWN_PCT") is not None else None,
                "realized_pnl": float(row["REALIZED_PNL"]) if row.get("REALIZED_PNL") is not None else None,
                "distribution_amount": float(row["DISTRIBUTION_AMOUNT"]) if row.get("DISTRIBUTION_AMOUNT") is not None else None,
                "distribution_mode": row.get("DISTRIBUTION_MODE"),
                "trades_count": int(row["TRADES_COUNT"]) if row.get("TRADES_COUNT") is not None else 0,
                "win_days": int(row["WIN_DAYS"]) if row.get("WIN_DAYS") is not None else 0,
                "loss_days": int(row["LOSS_DAYS"]) if row.get("LOSS_DAYS") is not None else 0,
            })
            ts = end_ts if end_ts else start_ts
            if ts:
                point_ts = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
                cumulative_series.append({
                    "ts": point_ts,
                    "cum_distributed_amount": cum_distributed,
                    "cum_realized_pnl": cum_realized_pnl,
                    "episode_id": row["EPISODE_ID"],
                })
                total_paid_out_series.append({"ts": point_ts, "cum_distributed_amount": cum_distributed})

        return {
            "per_episode": per_episode,
            "cumulative_series": cumulative_series,
            "total_paid_out_amount": total_paid_out,
            "total_paid_out_series": total_paid_out_series,
            "total_realized_pnl": total_realized_pnl,
            "episode_count": len(per_episode),
        }
    finally:
        conn.close()


@router.get("/{portfolio_id}/episodes/{episode_id}")
def get_episode_detail(portfolio_id: int, episode_id: int):
    """
    Episode analytics for the timeline card: equity series, drawdown series,
    trades per day, regime strip, thresholds, and events.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            select e.EPISODE_ID, e.PORTFOLIO_ID, e.PROFILE_ID, e.START_TS, e.END_TS, e.STATUS, e.END_REASON,
                   p.NAME as PROFILE_NAME, p.DRAWDOWN_STOP_PCT, p.BUST_EQUITY_PCT,
                   port.STARTING_CASH, port.BUST_AT
            from MIP.APP.PORTFOLIO_EPISODE e
            join MIP.APP.PORTFOLIO_PROFILE p on p.PROFILE_ID = e.PROFILE_ID
            join MIP.APP.PORTFOLIO port on port.PORTFOLIO_ID = e.PORTFOLIO_ID
            where e.PORTFOLIO_ID = %s and e.EPISODE_ID = %s
            """,
            (portfolio_id, episode_id),
        )
        row = cur.fetchone()
        if not row or not cur.description:
            raise HTTPException(status_code=404, detail="Episode not found")
        cols = [d[0] for d in cur.description]
        ep = dict(zip(cols, row))
        start_ts = ep.get("START_TS")
        end_ts = ep.get("END_TS")
        if start_ts is None:
            raise HTTPException(status_code=404, detail="Episode missing START_TS")
        drawdown_stop_pct = float(ep["DRAWDOWN_STOP_PCT"]) if ep.get("DRAWDOWN_STOP_PCT") is not None else 0.10
        bust_equity_pct = float(ep["BUST_EQUITY_PCT"]) if ep.get("BUST_EQUITY_PCT") is not None else None
        start_equity = float(ep["STARTING_CASH"]) if ep.get("STARTING_CASH") is not None else None

        # Equity series: PORTFOLIO_DAILY in window
        cur.execute(
            """
            select TS, TOTAL_EQUITY, PEAK_EQUITY, DRAWDOWN, OPEN_POSITIONS
            from MIP.APP.PORTFOLIO_DAILY
            where PORTFOLIO_ID = %s and TS >= %s and (%s is null or TS <= %s)
            order by TS
            """,
            (portfolio_id, start_ts, end_ts, end_ts),
        )
        daily_rows = fetch_all(cur)
        equity_series = []
        drawdown_series = []
        regime_per_day = []
        drawdown_stop_ts = None
        for r in daily_rows:
            ts = r.get("TS")
            te = r.get("TOTAL_EQUITY")
            pe = r.get("PEAK_EQUITY")
            dd = r.get("DRAWDOWN")
            if ts is not None:
                tss = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
                if te is not None:
                    equity_series.append({"ts": tss, "equity": float(te)})
                dd_pct = (-float(dd) * 100) if dd is not None else None
                drawdown_series.append({
                    "ts": tss,
                    "drawdown_pct": dd_pct,
                    "high_watermark_equity": float(pe) if pe is not None else None,
                })
                gate = "STOPPED" if (dd is not None and float(dd) >= drawdown_stop_pct) else "SAFE"
                regime_per_day.append({"ts": tss, "gate_state": gate})
                if drawdown_stop_ts is None and dd is not None and float(dd) >= drawdown_stop_pct:
                    drawdown_stop_ts = tss

        # Trades per day
        cur.execute(
            """
            select date_trunc('day', TRADE_TS) as day_ts, count(*) as trades_count
            from MIP.APP.PORTFOLIO_TRADES
            where PORTFOLIO_ID = %s and TRADE_TS >= %s and (%s is null or TRADE_TS <= %s)
            group by date_trunc('day', TRADE_TS)
            order by day_ts
            """,
            (portfolio_id, start_ts, end_ts, end_ts),
        )
        trade_rows = fetch_all(cur)
        trades_per_day = []
        for r in trade_rows:
            day_ts = r.get("day_ts") or r.get("DAY_TS")
            cnt = r.get("trades_count") or r.get("TRADES_COUNT")
            if day_ts is not None:
                tss = day_ts.isoformat() if hasattr(day_ts, "isoformat") else str(day_ts)
                trades_per_day.append({"ts": tss, "trades_count": int(cnt) if cnt is not None else 0})

        # Events: entries_blocked (drawdown_stop), drawdown_stop, bust, episode_ended
        events = []
        if drawdown_stop_ts:
            events.append({"ts": drawdown_stop_ts, "type": "entries_blocked"})
            events.append({"ts": drawdown_stop_ts, "type": "drawdown_stop_triggered"})
        bust_at = ep.get("BUST_AT")
        if bust_at is not None and (end_ts is None or bust_at <= end_ts) and bust_at >= start_ts:
            bust_ts = bust_at.isoformat() if hasattr(bust_at, "isoformat") else str(bust_at)
            events.append({"ts": bust_ts, "type": "bust_triggered"})
        if end_ts is not None:
            end_ts_str = end_ts.isoformat() if hasattr(end_ts, "isoformat") else str(end_ts)
            events.append({"ts": end_ts_str, "type": "episode_ended"})
        events.sort(key=lambda x: x["ts"])

        # Thresholds
        thresholds = {
            "drawdown_stop_pct": drawdown_stop_pct * 100 if drawdown_stop_pct is not None else None,
            "bust_threshold_pct": bust_equity_pct * 100 if bust_equity_pct is not None else None,
            "start_equity": start_equity,
        }

        # Summary stats (reuse logic from list)
        final_equity = None
        max_drawdown = None
        win_days = None
        loss_days = None
        peak_open = None
        if daily_rows:
            final_equity = daily_rows[-1].get("TOTAL_EQUITY")
            if final_equity is not None:
                final_equity = float(final_equity)
            max_dd = max((float(r["DRAWDOWN"]) for r in daily_rows if r.get("DRAWDOWN") is not None), default=None)
            max_drawdown = max_dd * 100 if max_dd is not None else None
            prev = None
            wins = losses = 0
            for r in daily_rows:
                te = r.get("TOTAL_EQUITY")
                if prev is not None and te is not None:
                    if float(te) > float(prev):
                        wins += 1
                    elif float(te) < float(prev):
                        losses += 1
                prev = te
            win_days = wins
            loss_days = losses
            peak_open = max((r.get("OPEN_POSITIONS") or 0) for r in daily_rows)
            if isinstance(peak_open, (int, float)):
                peak_open = int(peak_open)
        cur.execute(
            "select count(*) from MIP.APP.PORTFOLIO_TRADES where PORTFOLIO_ID = %s and TRADE_TS >= %s and (%s is null or TRADE_TS <= %s)",
            (portfolio_id, start_ts, end_ts, end_ts),
        )
        tc = cur.fetchone()
        trades_count = int(tc[0]) if tc and tc[0] is not None else 0

        start_ts_str = start_ts.isoformat() if hasattr(start_ts, "isoformat") else str(start_ts)
        end_ts_str = end_ts.isoformat() if hasattr(end_ts, "isoformat") else str(end_ts) if end_ts else None

        return serialize_row({
            "episode_id": episode_id,
            "portfolio_id": portfolio_id,
            "profile_id": ep.get("PROFILE_ID"),
            "profile_name": ep.get("PROFILE_NAME"),
            "start_ts": start_ts_str,
            "end_ts": end_ts_str,
            "status": ep.get("STATUS"),
            "end_reason": ep.get("END_REASON"),
            "equity_series": equity_series,
            "drawdown_series": drawdown_series,
            "trades_per_day": trades_per_day,
            "regime_per_day": regime_per_day,
            "thresholds": thresholds,
            "events": events,
            "start_equity": start_equity,
            "end_equity": final_equity,
            "total_return": (final_equity / start_equity - 1) if (start_equity and final_equity and start_equity != 0) else None,
            "max_drawdown": max_drawdown,
            "trades_count": trades_count,
            "win_days": win_days,
            "loss_days": loss_days,
            "peak_open_symbols": peak_open,
        })
    finally:
        conn.close()
