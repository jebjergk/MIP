"""Bulk Phase 7 simulation replay — gates on persisted context V0.2 only."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from app.db import get_connection

from .context_repository import load_context_observations
from .context_ruleset_v02 import RULESET_VERSION as CONTEXT_V02
from .errors import BrooksIntradayError
from .replay_engine import (
    _bars_index,
    _load_session_dossiers,
    _normalize_utc,
    _schedule_cache,
    verify_replay_ready,
    verify_schedule_bars,
)
from .simulation_engine import (
    BLOCK_REASON_POSITION_OPEN,
    BLOCK_REASON_TIE_BREAK,
    PortfolioSimState,
    execute_entry,
    fill_pending_exit,
    process_symbol_bar,
    record_blocked_entry,
    tie_break_pick,
)
from .simulation_repository import (
    clear_run_simulation_artifacts,
    complete_simulation_attempt,
    create_simulation_attempt,
    insert_blocked_signal,
    insert_sim_trade,
    simulation_sequence_hash,
    verify_context_attempt,
)
from .simulation_ruleset_v01 import (
    DEFAULT_CONTEXT_ATTEMPT_ID,
    REQUIRED_CONTEXT_RULESET,
    resolve_params,
)

logger = logging.getLogger(__name__)

PHASE6B_CONTEXT = DEFAULT_CONTEXT_ATTEMPT_ID


def _context_index(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict] = {}
    for r in rows:
        sym = str(r.get("symbol", "")).upper()
        ts = str(_normalize_utc(r.get("bar_ts")) or r.get("bar_ts"))[:19]
        out[(sym, ts)] = r
    return out


def run_simulation_replay_bulk(
    state: dict[str, Any],
    *,
    context_attempt_id: str = PHASE6B_CONTEXT,
    progress_every: int = 50,
) -> str:
    verify_replay_ready(state)
    verify_schedule_bars(state)
    run_id = state["run_id"]
    params = resolve_params(state.get("configuration", {}).get("simulation_parameters"))

    meta = verify_context_attempt(
        run_id=run_id,
        context_attempt_id=context_attempt_id,
        required_ruleset=REQUIRED_CONTEXT_RULESET,
    )
    if meta["context_ruleset_version"] == "BROOKS_CONTEXT_RULESET_V0_1":
        raise BrooksIntradayError(
            "SIMULATION_CONTEXT_V01_FORBIDDEN",
            "Phase 7 gating must not use BROOKS_CONTEXT_RULESET_V0_1.",
            run_id=run_id,
        )

    ctx_rows = load_context_observations(run_id, context_attempt_id=context_attempt_id, limit=10000)
    ctx_index = _context_index(ctx_rows)
    schedule = _schedule_cache(state)
    index = _bars_index(state)
    symbols = state["symbols"]
    symbol_order = list(symbols)
    starting_cash = float(state.get("starting_cash") or params["starting_cash"])
    portfolio = PortfolioSimState(cash=starting_cash)

    clear_run_simulation_artifacts(run_id)
    sim_attempt_id = create_simulation_attempt(
        run_id=run_id,
        context_attempt_id=context_attempt_id,
        context_ruleset_version=str(meta["context_ruleset_version"]),
        starting_cash=starting_cash,
        notes="Phase 7 pilot week simulation (V0.2 context gating)",
    )

    dossiers_cache: dict[str, dict[str, Any]] = {}
    prev_td: date | None = None
    session_bar_count = 0
    entry_action = str(params["entry_signal_action"])

    for step_idx, step in enumerate(schedule):
        if prev_td != step.trading_date:
            td_key = step.trading_date.isoformat()
            dossiers_cache[td_key] = _load_session_dossiers(state, step.trading_date)
            session_bar_count = 0
            prev_td = step.trading_date
        is_last = step.bar_index_in_session >= 77

        # 1) Fill pending exits at this bar open (per symbol)
        for sym in symbols:
            bar_key = (sym, step.trading_date, _normalize_utc(step.bar_timestamp_utc))
            bar = index.get(bar_key)
            if not bar:
                continue
            fill_pending_exit(portfolio, bar=bar)

        # 2) Per-symbol context processing
        entry_candidates: list[dict[str, Any]] = []
        for sym in symbols:
            bar_key = (sym, step.trading_date, _normalize_utc(step.bar_timestamp_utc))
            bar = index.get(bar_key)
            if not bar:
                raise BrooksIntradayError(
                    "FROZEN_DATASET_MISMATCH",
                    f"Missing bar for {sym} at {step.bar_timestamp_utc}.",
                    run_id=run_id,
                )
            ts_key = str(_normalize_utc(bar.ts_utc) or bar.ts_utc)[:19]
            ctx = ctx_index.get((sym.upper(), ts_key))
            if not ctx:
                raise BrooksIntradayError(
                    "SIMULATION_MISSING_CONTEXT",
                    f"No V0.2 context row for {sym} at {ts_key}.",
                    run_id=run_id,
                )
            dossier = dossiers_cache[step.trading_date.isoformat()][sym]
            step_res = process_symbol_bar(
                portfolio=portfolio,
                ctx=ctx,
                bar=bar,
                dossier=dossier,
                bar_index_in_session=step.bar_index_in_session,
                is_last_bar_in_session=is_last,
                params=params,
            )
            if step_res.entry_candidate:
                entry_candidates.append({"symbol": sym, "bar": bar, "context": ctx, "dossier": dossier})

        # 3) Entries — one position max
        if entry_candidates:
            if portfolio.open_position is not None:
                for c in entry_candidates:
                    record_blocked_entry(
                        portfolio,
                        symbol=c["symbol"],
                        signal_ts=c["bar"].ts_utc,
                        reason=BLOCK_REASON_POSITION_OPEN,
                        candidate_action=entry_action,
                    )
            else:
                winner, losers = tie_break_pick(entry_candidates, params=params, symbol_order=symbol_order)
                execute_entry(portfolio, candidate=winner, bar=winner["bar"], params=params)
                for loser in losers:
                    record_blocked_entry(
                        portfolio,
                        symbol=loser["symbol"],
                        signal_ts=loser["bar"].ts_utc,
                        reason=BLOCK_REASON_TIE_BREAK,
                        candidate_action=entry_action,
                        tie_break_json={"winner": winner["symbol"]},
                    )

        portfolio.bars_processed += 1
        session_bar_count += 1
        if progress_every and (step_idx + 1) % progress_every == 0:
            print(f"simulation step {step_idx + 1}/{len(schedule)}", flush=True)

    # Force-close any open position at week end (final bar close)
    if portfolio.open_position and not portfolio.pending_exit:
        pos = portfolio.open_position
        last_bar = None
        for sym in symbols:
            bar_key = (sym, schedule[-1].trading_date, _normalize_utc(schedule[-1].bar_timestamp_utc))
            last_bar = index.get(bar_key)
            if last_bar and last_bar.symbol.upper() == pos.symbol:
                break
        if last_bar:
            fill_price = float(last_bar.close)
            pnl = (fill_price - pos.entry_price) * pos.quantity
            portfolio.cash += fill_price * pos.quantity
            portfolio.realized_pnl += pnl
            portfolio.closed_trades.append(
                {
                    "trade_id": pos.trade_id,
                    "symbol": pos.symbol,
                    "direction": "LONG",
                    "quantity": pos.quantity,
                    "signal_ts": pos.signal_ts,
                    "entry_ts": pos.entry_ts,
                    "entry_price": pos.entry_price,
                    "exit_decision_ts": last_bar.ts_utc,
                    "exit_ts": last_bar.ts_utc,
                    "exit_price": fill_price,
                    "exit_reason": "FORCED_WEEK_END_FLATTEN",
                    "initial_cash": pos.initial_cash,
                    "remaining_cash": portfolio.cash,
                    "realized_pnl": round(pnl, 4),
                }
            )
            portfolio.open_position = None

    conn = get_connection()
    try:
        for i, trade in enumerate(portfolio.closed_trades, start=1):
            insert_sim_trade(run_id=run_id, trade=trade, conn=conn, commit=False)
        open_sym = portfolio.open_position.symbol if portfolio.open_position else None
        for i, sig in enumerate(portfolio.blocked_signals, start=1):
            insert_blocked_signal(
                run_id=run_id,
                signal=sig,
                active_position_symbol=sig.get("active_position_symbol") or open_sym,
                conn=conn,
                commit=False,
            )
            if i % 200 == 0:
                conn.commit()
        conn.commit()
    finally:
        conn.close()

    seq = simulation_sequence_hash(portfolio.closed_trades, portfolio.blocked_signals)
    complete_simulation_attempt(
        simulation_attempt_id=sim_attempt_id,
        trade_count=len(portfolio.closed_trades),
        blocked_count=len(portfolio.blocked_signals),
        ending_cash=portfolio.cash,
        realized_pnl=portfolio.realized_pnl,
        sequence_hash=seq,
    )

    state["current_cash"] = portfolio.cash
    state["realized_pnl"] = portfolio.realized_pnl
    state["open_position_symbol"] = None
    state["open_position_qty"] = None
    state["phase7_simulation_attempt_id"] = sim_attempt_id
    cfg = state.setdefault("configuration", {})
    cfg["phase7_simulation_attempt_id"] = sim_attempt_id
    cfg["phase6b_context_attempt_id"] = context_attempt_id
    cfg["context_ruleset_version"] = CONTEXT_V02
    cfg["simulation_ruleset_version"] = params["ruleset_version"]
    cfg["simulation_summary"] = {
        "trade_count": len(portfolio.closed_trades),
        "blocked_signal_count": len(portfolio.blocked_signals),
        "ending_cash": portfolio.cash,
        "realized_pnl": portfolio.realized_pnl,
        "sequence_hash": seq,
    }

    return sim_attempt_id
