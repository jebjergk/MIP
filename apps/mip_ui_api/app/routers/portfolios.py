from fastapi import APIRouter, HTTPException, Query

from app.db import get_connection, fetch_all, serialize_rows, serialize_row

router = APIRouter(prefix="/portfolios", tags=["portfolios"])


@router.get("")
def list_portfolios():
    """Portfolio list: PORTFOLIO_ID, NAME, STATUS, LAST_SIMULATED_AT, etc."""
    sql = """
    select
        PORTFOLIO_ID,
        NAME,
        STATUS,
        LAST_SIMULATED_AT,
        PROFILE_ID,
        STARTING_CASH,
        FINAL_EQUITY,
        TOTAL_RETURN
    from MIP.APP.PORTFOLIO
    order by PORTFOLIO_ID
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = fetch_all(cur)
        return serialize_rows(rows)
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


# Snapshot semantics:
# - pos_as_of_col: AS_OF_TS in V_PORTFOLIO_OPEN_POSITIONS_CANONICAL (latest bar snapshot)
# - trade_ts_col: TRADE_TS in PORTFOLIO_TRADES
# - run_id_col: RUN_ID in both tables
# - open_filter: use canonical view (IS_OPEN) so we return only open positions


@router.get("/{portfolio_id}/snapshot")
def get_portfolio_snapshot(
    portfolio_id: int,
    run_id: str | None = None,
    lookback_days: int = Query(30, ge=-1, description="Days to look back (trade_ts_col); use -1 for all"),
):
    """
    Combined read: latest open positions (canonical view), trades by lookback, daily, KPIs, risk + cards.
    Positions: from V_PORTFOLIO_OPEN_POSITIONS_CANONICAL (only open); fallback to raw positions for effective run if empty.
    Trades: trade_ts_col=TRADE_TS, run_id_col=RUN_ID. Filter by lookback_days (default 30); return trades_total, last_trade_ts.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Resolve effective run for trades/daily/kpis (run_id param, or latest from portfolio)
        effective_run_id = run_id
        if effective_run_id is None:
            cur.execute(
                "select LAST_SIMULATION_RUN_ID from MIP.APP.PORTFOLIO where PORTFOLIO_ID = %s",
                (portfolio_id,),
            )
            row = cur.fetchone()
            if row and row[0]:
                effective_run_id = row[0]
            else:
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

        # Open positions: all positions still open as of latest bar (HOLD_UNTIL_INDEX not reached), from any run.
        # So you see everything you still possess, including positions opened in earlier runs.
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
        # Fallback: when canonical returns 0 or view fails, show latest run's positions so UI isn't blank
        if not positions and effective_run_id:
            cur.execute(
                """
                select * from MIP.APP.PORTFOLIO_POSITIONS
                where PORTFOLIO_ID = %s and RUN_ID = %s
                order by ENTRY_TS desc
                """,
                (portfolio_id, effective_run_id),
            )
            positions = serialize_rows(fetch_all(cur))
        # Second fallback: if still no positions (e.g. effective run has no rows), use latest run that has any positions
        if not positions:
            cur.execute(
                """
                select RUN_ID from MIP.APP.PORTFOLIO_POSITIONS
                where PORTFOLIO_ID = %s
                order by RUN_ID desc
                limit 1
                """,
                (portfolio_id,),
            )
            row = cur.fetchone()
            if row and row[0]:
                cur.execute(
                    """
                    select * from MIP.APP.PORTFOLIO_POSITIONS
                    where PORTFOLIO_ID = %s and RUN_ID = %s
                    order by ENTRY_TS desc
                    """,
                    (portfolio_id, row[0]),
                )
                positions = serialize_rows(fetch_all(cur))
        positions = _enrich_positions_side(positions)

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
            "open_positions": positions,
            "recent_trades": trades[:20],
            "risk_gate_status": risk_gate_status,
            "snapshot_ts": snapshot_ts,
            "run_id": effective_run_id,
            "trades_total": trades_total,
            "last_trade_ts": last_trade_ts,
            "lookback_days": lookback_days,
        }

        return {
            "positions": positions,
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
        }
    finally:
        conn.close()
