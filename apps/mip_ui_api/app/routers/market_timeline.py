"""
Market Timeline: End-to-end symbol observability page API.

Shows market development (OHLC) with overlays for signals, proposals, trades,
and trust state. Includes decision narrative explaining why MIP acted or didn't.

Endpoints:
- GET /market-timeline/overview - Grid summary for all symbols
- GET /market-timeline/detail - Full OHLC + overlays + narrative for one symbol
"""
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Query

from app.db import get_connection, fetch_all, serialize_rows

router = APIRouter(prefix="/market-timeline", tags=["market-timeline"])


# =============================================================================
# Reason codes for decision narrative
# =============================================================================
REASON_CODES = {
    "EXECUTED": {
        "title": "Trade executed",
        "detail": "A trade was placed for this symbol.",
    },
    "PROPOSED": {
        "title": "Proposal generated",
        "detail": "An order proposal was created but may not have been executed yet.",
    },
    "NOT_TRUSTED_YET": {
        "title": "Signal not trusted yet",
        "detail": "The pattern has not accumulated enough evidence to be trusted.",
    },
    "RISK_GATE_BLOCKED": {
        "title": "Risk gate blocked entries",
        "detail": "Portfolio risk controls prevented new entries (e.g., drawdown stop).",
    },
    "COOLDOWN_ACTIVE": {
        "title": "Cooldown period active",
        "detail": "Symbol is in cooldown after a recent trade; waiting before re-entry.",
    },
    "MAX_POSITIONS_REACHED": {
        "title": "Maximum positions reached",
        "detail": "Portfolio is at maximum allowed open positions.",
    },
    "ALREADY_HELD": {
        "title": "Position already held",
        "detail": "This symbol is already in the portfolio.",
    },
    "SIZING_ZERO": {
        "title": "Position sizing returned zero",
        "detail": "Risk/sizing rules computed zero shares for this opportunity.",
    },
    "MIN_NOTIONAL": {
        "title": "Below minimum notional",
        "detail": "Trade value is below the minimum notional threshold.",
    },
    "NO_SIGNALS": {
        "title": "No signals generated",
        "detail": "No pattern signals were generated for this symbol in the window.",
    },
    "NO_PROPOSALS": {
        "title": "No proposals generated",
        "detail": "Signals existed but no order proposals were created.",
    },
    "PROPOSAL_REJECTED": {
        "title": "Proposal rejected",
        "detail": "Proposal was generated but rejected during validation.",
    },
    # New diagnostic reason codes from proposer
    "NO_RECOMMENDATIONS_IN_LOG": {
        "title": "No recommendations in log",
        "detail": "RECOMMENDATION_LOG has no entries. Pipeline may not have generated any signals.",
    },
    "REC_TS_STALE": {
        "title": "Recommendations are stale",
        "detail": "Latest recommendation TS doesn't match latest bar TS. Recommendations may not have been generated for recent market data.",
    },
    "NO_SIGNALS_AT_LATEST_TS": {
        "title": "No signals at latest bar",
        "detail": "Recommendations exist but none at the latest bar timestamp. Check if patterns matched any data.",
    },
    "NO_TRUSTED_PATTERNS": {
        "title": "No trusted patterns",
        "detail": "No pattern/horizon combos have passed the training gate thresholds yet.",
    },
    "SIGNALS_NOT_FROM_TRUSTED_PATTERNS": {
        "title": "Signals not from trusted patterns",
        "detail": "Signals exist at latest TS but none are from patterns that have been trusted.",
    },
    "ENTRIES_BLOCKED_DRAWDOWN_STOP": {
        "title": "Entries blocked (drawdown stop)",
        "detail": "Portfolio has hit drawdown stop and entries are blocked until recovery or reset.",
    },
}


def _get_reason(code: str, evidence: dict = None) -> dict:
    """Build a reason object with code, title, detail, and evidence."""
    info = REASON_CODES.get(code, {"title": code, "detail": ""})
    return {
        "code": code,
        "title": info["title"],
        "detail": info["detail"],
        "evidence": evidence or {},
    }


# =============================================================================
# GET /market-timeline/overview
# =============================================================================
@router.get("/overview")
def get_overview(
    portfolio_id: Optional[int] = Query(None, description="Filter proposals/trades to this portfolio"),
    market_type: Optional[str] = Query(None, description="Filter by market type (STOCK, ETF, FX)"),
    window_bars: int = Query(30, description="Number of bars to look back"),
    interval_minutes: int = Query(1440, description="Bar interval (1440 = daily)"),
):
    """
    Overview grid for all tracked symbols.
    
    Returns list of symbols with counts for signals, proposals, trades in the window.
    Includes trust label if available.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Get the latest bar date to establish the window
        cur.execute(
            """
            select max(TS) as latest_ts
            from MIP.MART.MARKET_BARS
            where INTERVAL_MINUTES = %s
            """,
            (interval_minutes,),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return {"symbols": [], "window": {"bars": window_bars, "latest_ts": None}}
        latest_ts = row[0]
        
        # Calculate window start (approximate: window_bars days back for daily)
        if interval_minutes == 1440:
            window_start = latest_ts - timedelta(days=window_bars)
        else:
            window_start = latest_ts - timedelta(minutes=interval_minutes * window_bars)
        
        # Build market type filter
        market_filter = ""
        params = [interval_minutes, window_start]
        if market_type:
            market_filter = "and b.MARKET_TYPE = %s"
            params.append(market_type)
        
        # Get all symbols with bar data in window
        sql = f"""
        with symbols_in_window as (
            select distinct SYMBOL, MARKET_TYPE
            from MIP.MART.MARKET_BARS b
            where b.INTERVAL_MINUTES = %s
              and b.TS >= %s
              {market_filter}
        ),
        signal_counts as (
            select 
                r.SYMBOL,
                r.MARKET_TYPE,
                count(*) as signal_count
            from MIP.APP.RECOMMENDATION_LOG r
            where r.TS >= %s
              and r.INTERVAL_MINUTES = %s
            group by r.SYMBOL, r.MARKET_TYPE
        ),
        proposal_counts as (
            select
                p.SYMBOL,
                p.MARKET_TYPE,
                count(*) as proposal_count,
                count(case when p.PROPOSED_AT::date = current_date() 
                           and p.STATUS in ('PROPOSED', 'APPROVED', 'EXECUTED') then 1 end) as today_proposal_count
            from MIP.AGENT_OUT.ORDER_PROPOSALS p
            where p.PROPOSED_AT >= %s
              {"and p.PORTFOLIO_ID = %s" if portfolio_id else ""}
            group by p.SYMBOL, p.MARKET_TYPE
        ),
        trade_counts as (
            select
                t.SYMBOL,
                t.MARKET_TYPE,
                count(*) as trade_count
            from MIP.APP.PORTFOLIO_TRADES t
            where t.TRADE_TS >= %s
              {"and t.PORTFOLIO_ID = %s" if portfolio_id else ""}
            group by t.SYMBOL, t.MARKET_TYPE
        ),
        trust_labels as (
            select
                SYMBOL,
                MARKET_TYPE,
                max(case when TRUST_LABEL = 'TRUSTED' then 1 
                         when TRUST_LABEL = 'WATCH' then 2 
                         else 3 end) as trust_rank,
                max_by(TRUST_LABEL, TS) as latest_trust_label
            from MIP.APP.V_TRUSTED_SIGNAL_CLASSIFICATION
            group by SYMBOL, MARKET_TYPE
        ),
        latest_bars as (
            select
                SYMBOL,
                MARKET_TYPE,
                max(TS) as latest_bar_ts,
                max_by(CLOSE, TS) as latest_close
            from MIP.MART.MARKET_BARS
            where INTERVAL_MINUTES = %s
            group by SYMBOL, MARKET_TYPE
        )
        select
            s.SYMBOL,
            s.MARKET_TYPE,
            coalesce(sc.signal_count, 0) as signal_count,
            coalesce(pc.proposal_count, 0) as proposal_count,
            coalesce(pc.today_proposal_count, 0) as today_proposal_count,
            coalesce(tc.trade_count, 0) as trade_count,
            tl.latest_trust_label as trust_label,
            lb.latest_bar_ts,
            lb.latest_close
        from symbols_in_window s
        left join signal_counts sc on sc.SYMBOL = s.SYMBOL and sc.MARKET_TYPE = s.MARKET_TYPE
        left join proposal_counts pc on pc.SYMBOL = s.SYMBOL and pc.MARKET_TYPE = s.MARKET_TYPE
        left join trade_counts tc on tc.SYMBOL = s.SYMBOL and tc.MARKET_TYPE = s.MARKET_TYPE
        left join trust_labels tl on tl.SYMBOL = s.SYMBOL and tl.MARKET_TYPE = s.MARKET_TYPE
        left join latest_bars lb on lb.SYMBOL = s.SYMBOL and lb.MARKET_TYPE = s.MARKET_TYPE
        order by s.MARKET_TYPE, s.SYMBOL
        """
        
        # Build params list
        query_params = [
            interval_minutes,  # symbols_in_window
            window_start,      # symbols_in_window
        ]
        if market_type:
            query_params.append(market_type)
        query_params.extend([
            window_start,      # signal_counts
            interval_minutes,  # signal_counts
            window_start,      # proposal_counts
        ])
        if portfolio_id:
            query_params.append(portfolio_id)
        query_params.append(window_start)  # trade_counts
        if portfolio_id:
            query_params.append(portfolio_id)
        query_params.append(interval_minutes)  # latest_bars
        
        cur.execute(sql, query_params)
        rows = fetch_all(cur)
        symbols = serialize_rows(rows)
        
        # Format for frontend
        result = []
        for row in symbols:
            result.append({
                "symbol": row.get("SYMBOL") or row.get("symbol"),
                "market_type": row.get("MARKET_TYPE") or row.get("market_type"),
                "signal_count": row.get("SIGNAL_COUNT") or row.get("signal_count") or 0,
                "proposal_count": row.get("PROPOSAL_COUNT") or row.get("proposal_count") or 0,
                "today_proposal_count": row.get("TODAY_PROPOSAL_COUNT") or row.get("today_proposal_count") or 0,
                "trade_count": row.get("TRADE_COUNT") or row.get("trade_count") or 0,
                "trust_label": row.get("TRUST_LABEL") or row.get("trust_label"),
                "latest_bar_ts": row.get("LATEST_BAR_TS") or row.get("latest_bar_ts"),
                "latest_close": row.get("LATEST_CLOSE") or row.get("latest_close"),
            })
        
        return {
            "symbols": result,
            "window": {
                "bars": window_bars,
                "interval_minutes": interval_minutes,
                "start_ts": window_start.isoformat() if hasattr(window_start, "isoformat") else str(window_start),
                "latest_ts": latest_ts.isoformat() if hasattr(latest_ts, "isoformat") else str(latest_ts),
            },
            "filters": {
                "portfolio_id": portfolio_id,
                "market_type": market_type,
            },
        }
    finally:
        conn.close()


# =============================================================================
# GET /market-timeline/detail
# =============================================================================
@router.get("/detail")
def get_detail(
    symbol: str = Query(..., description="Symbol to query"),
    market_type: str = Query(..., description="Market type (STOCK, ETF, FX)"),
    portfolio_id: Optional[int] = Query(None, description="Portfolio for proposals/trades/narrative"),
    window_bars: int = Query(60, description="Number of bars to return"),
    interval_minutes: int = Query(1440, description="Bar interval (1440 = daily)"),
    horizon_bars: int = Query(5, description="Horizon for trust classification"),
):
    """
    Detailed OHLC chart data with event overlays and decision narrative.
    
    Returns:
    - ohlc: OHLC series for chart
    - events: Overlay events (signals, proposals, trades, trust changes)
    - narrative: Decision explanation with reason codes
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Get OHLC data for the symbol
        cur.execute(
            """
            select TS, OPEN, HIGH, LOW, CLOSE, VOLUME
            from MIP.MART.MARKET_BARS
            where SYMBOL = %s
              and MARKET_TYPE = %s
              and INTERVAL_MINUTES = %s
            order by TS desc
            limit %s
            """,
            (symbol, market_type, interval_minutes, window_bars),
        )
        ohlc_rows = fetch_all(cur)
        ohlc = []
        for r in reversed(ohlc_rows):  # Reverse to chronological order
            ts = r.get("TS")
            ohlc.append({
                "ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                "open": float(r.get("OPEN")) if r.get("OPEN") is not None else None,
                "high": float(r.get("HIGH")) if r.get("HIGH") is not None else None,
                "low": float(r.get("LOW")) if r.get("LOW") is not None else None,
                "close": float(r.get("CLOSE")) if r.get("CLOSE") is not None else None,
                "volume": int(r.get("VOLUME")) if r.get("VOLUME") is not None else None,
            })
        
        # Determine window bounds
        if ohlc:
            window_start = ohlc[0]["ts"]
            window_end = ohlc[-1]["ts"]
        else:
            window_start = None
            window_end = None
        
        # Get signal events
        signals = []
        if window_start:
            cur.execute(
                """
                select 
                    r.RECOMMENDATION_ID,
                    r.TS,
                    r.PATTERN_ID,
                    r.SCORE,
                    r.GENERATED_AT
                from MIP.APP.RECOMMENDATION_LOG r
                where r.SYMBOL = %s
                  and r.MARKET_TYPE = %s
                  and r.TS >= %s
                order by r.TS
                """,
                (symbol, market_type, window_start),
            )
            for row in fetch_all(cur):
                ts = row.get("TS")
                signals.append({
                    "type": "SIGNAL",
                    "ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                    "recommendation_id": row.get("RECOMMENDATION_ID"),
                    "pattern_id": row.get("PATTERN_ID"),
                    "score": float(row.get("SCORE")) if row.get("SCORE") is not None else None,
                })
        
        # Get proposal events
        proposals = []
        if window_start:
            proposal_sql = """
                select 
                    p.PROPOSAL_ID,
                    p.PORTFOLIO_ID,
                    p.PROPOSED_AT,
                    p.SIDE,
                    p.TARGET_WEIGHT,
                    p.STATUS,
                    p.EXECUTED_AT,
                    p.SIGNAL_TS,
                    p.RECOMMENDATION_ID
                from MIP.AGENT_OUT.ORDER_PROPOSALS p
                where p.SYMBOL = %s
                  and p.MARKET_TYPE = %s
                  and p.PROPOSED_AT >= %s
            """
            params = [symbol, market_type, window_start]
            if portfolio_id:
                proposal_sql += " and p.PORTFOLIO_ID = %s"
                params.append(portfolio_id)
            proposal_sql += " order by p.PROPOSED_AT"
            
            cur.execute(proposal_sql, params)
            for row in fetch_all(cur):
                # Use SIGNAL_TS for chart alignment (when signal fired), fall back to PROPOSED_AT
                signal_ts = row.get("SIGNAL_TS")
                proposed_at = row.get("PROPOSED_AT")
                chart_ts = signal_ts if signal_ts else proposed_at
                proposals.append({
                    "type": "PROPOSAL",
                    "ts": chart_ts.isoformat() if hasattr(chart_ts, "isoformat") else str(chart_ts),
                    "proposed_at": proposed_at.isoformat() if hasattr(proposed_at, "isoformat") else str(proposed_at),
                    "proposal_id": row.get("PROPOSAL_ID"),
                    "recommendation_id": row.get("RECOMMENDATION_ID"),
                    "portfolio_id": row.get("PORTFOLIO_ID"),
                    "side": row.get("SIDE"),
                    "target_weight": float(row.get("TARGET_WEIGHT")) if row.get("TARGET_WEIGHT") is not None else None,
                    "status": row.get("STATUS"),
                    "executed_at": row.get("EXECUTED_AT").isoformat() if row.get("EXECUTED_AT") else None,
                })
        
        # Get trade events
        trades = []
        if window_start:
            trade_sql = """
                select 
                    t.TRADE_ID,
                    t.PORTFOLIO_ID,
                    t.TRADE_TS,
                    t.SIDE,
                    t.QUANTITY,
                    t.PRICE,
                    t.NOTIONAL,
                    t.REALIZED_PNL,
                    t.PROPOSAL_ID
                from MIP.APP.PORTFOLIO_TRADES t
                where t.SYMBOL = %s
                  and t.MARKET_TYPE = %s
                  and t.TRADE_TS >= %s
            """
            params = [symbol, market_type, window_start]
            if portfolio_id:
                trade_sql += " and t.PORTFOLIO_ID = %s"
                params.append(portfolio_id)
            trade_sql += " order by t.TRADE_TS"
            
            cur.execute(trade_sql, params)
            for row in fetch_all(cur):
                ts = row.get("TRADE_TS")
                trades.append({
                    "type": "TRADE",
                    "ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                    "trade_id": row.get("TRADE_ID"),
                    "proposal_id": row.get("PROPOSAL_ID"),
                    "portfolio_id": row.get("PORTFOLIO_ID"),
                    "side": row.get("SIDE"),
                    "quantity": float(row.get("QUANTITY")) if row.get("QUANTITY") is not None else None,
                    "price": float(row.get("PRICE")) if row.get("PRICE") is not None else None,
                    "notional": float(row.get("NOTIONAL")) if row.get("NOTIONAL") is not None else None,
                    "realized_pnl": float(row.get("REALIZED_PNL")) if row.get("REALIZED_PNL") is not None else None,
                })
        
        # Get trust classification history (latest per pattern)
        trust_events = []
        try:
            cur.execute(
                """
                select 
                    PATTERN_ID,
                    HORIZON_BARS,
                    TRUST_LABEL,
                    N_SUCCESS,
                    COVERAGE_RATE,
                    AVG_RETURN
                from MIP.MART.V_TRUSTED_SIGNAL_POLICY
                where MARKET_TYPE = %s
                  and INTERVAL_MINUTES = %s
                  and HORIZON_BARS = %s
                order by PATTERN_ID
                """,
                (market_type, interval_minutes, horizon_bars),
            )
            for row in fetch_all(cur):
                trust_events.append({
                    "type": "TRUST",
                    "pattern_id": row.get("PATTERN_ID"),
                    "trust_label": row.get("TRUST_LABEL"),
                    "n_success": row.get("N_SUCCESS"),
                    "coverage_rate": float(row.get("COVERAGE_RATE")) if row.get("COVERAGE_RATE") else None,
                    "avg_return": float(row.get("AVG_RETURN")) if row.get("AVG_RETURN") else None,
                })
        except Exception:
            pass  # Trust view may not exist
        
        # Combine events for overlay
        events = signals + proposals + trades
        events.sort(key=lambda e: e.get("ts", ""))
        
        # Build signal chains: link signal -> proposal -> buy -> sell
        proposal_by_rec = {}
        for p in proposals:
            rid = p.get("recommendation_id")
            if rid is not None:
                proposal_by_rec.setdefault(rid, []).append(p)
        trade_by_proposal = {}
        for t in trades:
            pid = t.get("proposal_id")
            if pid is not None:
                trade_by_proposal.setdefault(pid, []).append(t)
        sell_trades = sorted(
            [t for t in trades if t.get("side") == "SELL"],
            key=lambda t: t.get("ts", ""),
        )

        chains = []
        used_sell_ids = set()
        for sig in signals:
            rec_id = sig.get("recommendation_id")
            chain = {
                "signal": sig,
                "proposal": None,
                "buy": None,
                "sell": None,
                "status": "SIGNAL_ONLY",
            }
            matched_proposals = proposal_by_rec.get(rec_id, [])
            best_proposal = None
            for mp in matched_proposals:
                if mp.get("status") == "EXECUTED":
                    best_proposal = mp
                    break
                if best_proposal is None:
                    best_proposal = mp
            if best_proposal:
                chain["proposal"] = best_proposal
                chain["status"] = (
                    "REJECTED" if best_proposal.get("status") == "REJECTED" else "PROPOSED"
                )
                buy_trades = trade_by_proposal.get(best_proposal.get("proposal_id"), [])
                buy_trade = next((t for t in buy_trades if t.get("side") == "BUY"), None)
                if buy_trade:
                    chain["buy"] = buy_trade
                    chain["status"] = "OPEN"
                    buy_qty = buy_trade.get("quantity")
                    for st in sell_trades:
                        if st.get("trade_id") in used_sell_ids:
                            continue
                        if st.get("ts", "") > buy_trade.get("ts", ""):
                            sell_qty = st.get("quantity")
                            if buy_qty and sell_qty and abs(buy_qty - sell_qty) < 0.0001:
                                chain["sell"] = st
                                chain["status"] = "CLOSED"
                                used_sell_ids.add(st.get("trade_id"))
                                break
            chains.append(chain)

        # Build decision narrative
        narrative = _build_narrative(
            cur,
            symbol=symbol,
            market_type=market_type,
            portfolio_id=portfolio_id,
            signal_count=len(signals),
            proposal_count=len(proposals),
            trade_count=len(trades),
            trust_events=trust_events,
            window_start=window_start,
        )
        
        return {
            "symbol": symbol,
            "market_type": market_type,
            "portfolio_id": portfolio_id,
            "window": {
                "bars": window_bars,
                "interval_minutes": interval_minutes,
                "start_ts": window_start,
                "end_ts": window_end,
            },
            "ohlc": ohlc,
            "events": events,
            "chains": chains,
            "trust_summary": trust_events,
            "narrative": narrative,
            "counts": {
                "signals": len(signals),
                "proposals": len(proposals),
                "trades": len(trades),
            },
        }
    finally:
        conn.close()


def _build_narrative(
    cur,
    symbol: str,
    market_type: str,
    portfolio_id: Optional[int],
    signal_count: int,
    proposal_count: int,
    trade_count: int,
    trust_events: list,
    window_start: str,
) -> dict:
    """
    Build decision narrative explaining why MIP acted or didn't.
    
    Returns:
    - decision_status: EXECUTED, PROPOSED, SKIPPED
    - reasons: list of reason objects with evidence
    - bullets: plain language summary
    """
    reasons = []
    bullets = []
    
    # Determine decision status
    if trade_count > 0:
        decision_status = "EXECUTED"
        reasons.append(_get_reason("EXECUTED", {"trade_count": trade_count}))
        bullets.append(f"Executed {trade_count} trade(s) for this symbol in the window.")
    elif proposal_count > 0:
        decision_status = "PROPOSED"
        reasons.append(_get_reason("PROPOSED", {"proposal_count": proposal_count}))
        bullets.append(f"Generated {proposal_count} proposal(s) but no trades were executed.")
    else:
        decision_status = "SKIPPED"
    
    # Explain why if skipped
    if decision_status == "SKIPPED":
        if signal_count == 0:
            reasons.append(_get_reason("NO_SIGNALS"))
            bullets.append("No pattern signals were generated for this symbol in the window.")
        elif proposal_count == 0:
            reasons.append(_get_reason("NO_PROPOSALS", {
                "signal_count": signal_count,
                "proposal_count": 0,
            }))
            bullets.append(f"{signal_count} signal(s) existed, but no proposals were generated.")
            
            # Check for trust issues
            trusted_patterns = [t for t in trust_events if t.get("trust_label") == "TRUSTED"]
            if not trusted_patterns:
                reasons.append(_get_reason("NOT_TRUSTED_YET", {
                    "trust_labels": [t.get("trust_label") for t in trust_events],
                }))
                bullets.append("Pattern(s) are not yet TRUSTED. Evidence is still accumulating.")
            
            # Check for portfolio-specific blocks
            if portfolio_id:
                # Check risk gate
                try:
                    cur.execute(
                        """
                        select ENTRIES_BLOCKED, BLOCK_REASON
                        from MIP.MART.V_PORTFOLIO_RISK_GATE
                        where PORTFOLIO_ID = %s
                        """,
                        (portfolio_id,),
                    )
                    row = cur.fetchone()
                    if row and row[0]:  # ENTRIES_BLOCKED = True
                        reasons.append(_get_reason("RISK_GATE_BLOCKED", {
                            "block_reason": row[1],
                        }))
                        bullets.append(f"Risk gate blocked entries: {row[1] or 'drawdown stop active'}.")
                except Exception:
                    pass
                
                # Check if already held
                try:
                    cur.execute(
                        """
                        select count(*) 
                        from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL
                        where PORTFOLIO_ID = %s and SYMBOL = %s
                        """,
                        (portfolio_id, symbol),
                    )
                    row = cur.fetchone()
                    if row and row[0] > 0:
                        reasons.append(_get_reason("ALREADY_HELD"))
                        bullets.append("This symbol is already held in the portfolio.")
                except Exception:
                    pass
                
                # Check max positions
                try:
                    cur.execute(
                        """
                        select 
                            op.open_count,
                            prof.MAX_POSITIONS
                        from (
                            select count(*) as open_count
                            from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL
                            where PORTFOLIO_ID = %s
                        ) op
                        cross join (
                            select coalesce(prof.MAX_POSITIONS, 10) as MAX_POSITIONS
                            from MIP.APP.PORTFOLIO p
                            left join MIP.APP.PORTFOLIO_PROFILE prof on prof.PROFILE_ID = p.PROFILE_ID
                            where p.PORTFOLIO_ID = %s
                        ) prof
                        """,
                        (portfolio_id, portfolio_id),
                    )
                    row = cur.fetchone()
                    if row and row[0] >= row[1]:
                        reasons.append(_get_reason("MAX_POSITIONS_REACHED", {
                            "open_count": row[0],
                            "max_positions": row[1],
                        }))
                        bullets.append(f"Portfolio at max positions ({row[0]}/{row[1]}).")
                except Exception:
                    pass
    
    # If still no reasons found, add generic
    if not reasons:
        bullets.append("Unable to determine specific reason for inactivity.")
    
    # Summary bullet
    if decision_status == "SKIPPED" and signal_count > 0:
        bullets.append(f"Summary: {signal_count} signals → {proposal_count} proposals → {trade_count} trades.")
    
    return {
        "decision_status": decision_status,
        "reasons": reasons,
        "bullets": bullets,
    }


# =============================================================================
# GET /market-timeline/diagnostics
# =============================================================================
@router.get("/diagnostics")
def get_diagnostics(
    portfolio_id: Optional[int] = Query(None, description="Portfolio ID to check"),
):
    """
    Get system-wide proposer diagnostics from the latest pipeline run.
    
    Returns:
    - Latest proposer event details from audit log
    - Candidate counts (raw, trusted, rejected)
    - Reason for no proposals if applicable
    - Timestamps for latest bars, recs, etc.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Get latest proposer events from audit log
        if portfolio_id:
            cur.execute(
                """
                select 
                    EVENT_TS,
                    RUN_ID,
                    STATUS,
                    ROWS_AFFECTED,
                    DETAILS
                from MIP.APP.MIP_AUDIT_LOG
                where EVENT_NAME = 'SP_AGENT_PROPOSE_TRADES'
                  and DETAILS:portfolio_id = %s
                order by EVENT_TS desc
                limit 5
                """,
                (str(portfolio_id),),
            )
        else:
            cur.execute(
                """
                select 
                    EVENT_TS,
                    RUN_ID,
                    STATUS,
                    ROWS_AFFECTED,
                    DETAILS
                from MIP.APP.MIP_AUDIT_LOG
                where EVENT_NAME = 'SP_AGENT_PROPOSE_TRADES'
                order by EVENT_TS desc
                limit 10
                """
            )
        
        rows = fetch_all(cur)
        events = []
        for row in rows:
            details = row.get("DETAILS") or row.get("details") or {}
            if isinstance(details, str):
                import json
                try:
                    details = json.loads(details)
                except:
                    details = {}
            
            events.append({
                "event_ts": row.get("EVENT_TS") or row.get("event_ts"),
                "run_id": row.get("RUN_ID") or row.get("run_id"),
                "status": row.get("STATUS") or row.get("status"),
                "rows_affected": row.get("ROWS_AFFECTED") or row.get("rows_affected"),
                "candidate_count_raw": details.get("candidate_count_raw"),
                "candidate_count_trusted": details.get("candidate_count_trusted"),
                "trusted_rejected_count": details.get("trusted_rejected_count"),
                "no_candidates_reason": details.get("no_candidates_reason"),
                "remaining_capacity": details.get("remaining_capacity"),
                "max_positions": details.get("max_positions"),
                "open_positions": details.get("open_positions"),
                "entries_blocked": details.get("entries_blocked"),
                "stop_reason": details.get("stop_reason"),
                "diagnostics": details.get("diagnostics"),
            })
        
        # Get current candidate counts (live)
        cur.execute("select count(*) from MIP.MART.V_SIGNALS_LATEST_TS")
        raw_count = cur.fetchone()[0] or 0
        
        cur.execute("select count(*) from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS")
        trusted_count = cur.fetchone()[0] or 0
        
        cur.execute("select count(*) from MIP.MART.V_TRUSTED_PATTERN_HORIZONS")
        trusted_patterns = cur.fetchone()[0] or 0
        
        cur.execute("select max(TS) from MIP.MART.MARKET_BARS where INTERVAL_MINUTES = 1440")
        latest_bar = cur.fetchone()[0]
        
        cur.execute("select max(TS) from MIP.APP.RECOMMENDATION_LOG where INTERVAL_MINUTES = 1440")
        latest_rec = cur.fetchone()[0]
        
        # Recommendation freshness by market type
        cur.execute(
            """
            select MARKET_TYPE, count(*) as cnt, max(TS) as max_ts
            from MIP.APP.RECOMMENDATION_LOG
            group by MARKET_TYPE
            """
        )
        rec_freshness = serialize_rows(fetch_all(cur))
        
        return {
            "proposer_events": events,
            "current_state": {
                "candidate_count_raw": raw_count,
                "candidate_count_trusted": trusted_count,
                "trusted_pattern_count": trusted_patterns,
                "latest_bar_ts": latest_bar.isoformat() if hasattr(latest_bar, "isoformat") else str(latest_bar) if latest_bar else None,
                "latest_rec_ts": latest_rec.isoformat() if hasattr(latest_rec, "isoformat") else str(latest_rec) if latest_rec else None,
                "rec_ts_matches_bar_ts": latest_rec == latest_bar if latest_rec and latest_bar else False,
            },
            "rec_freshness_by_market_type": rec_freshness,
        }
    finally:
        conn.close()
