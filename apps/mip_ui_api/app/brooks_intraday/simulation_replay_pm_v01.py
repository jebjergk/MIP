"""Disposable simulation replay with BROOKS_POSITION_MANAGEMENT_RULESET_V0_1 (review-only)."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from app.db import get_connection

from .context_repository import load_context_observations
from .context_ruleset_v03 import RULESET_VERSION as CONTEXT_V03
from .errors import BrooksIntradayError
from .pattern_repository import load_patterns
from .persist_integrity import fail_simulation_attempt
from .position_management_engine import (
    PMPortfolio,
    on_entry_filled,
    process_pm_bar_close,
    process_pm_bar_open,
    schedule_swing_confirmations_for_bar,
)
from .position_management_ledger_store import save_management_ledger
from .position_management_ruleset_v01 import RULESET_VERSION as PM_RULESET
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
    entry_permitted,
    execute_entry,
    record_blocked_entry,
    tie_break_pick,
)
from .simulation_repository import (
    complete_simulation_attempt,
    create_simulation_attempt,
    insert_blocked_signals_batch,
    insert_sim_trades_batch,
    simulation_sequence_hash,
    verify_context_attempt,
)
from .simulation_ruleset_v01 import resolve_params as resolve_sim_params
from .reentry_policy_v01 import (
    evaluate_reentry_for_consider_entry,
    get_reentry_tracker,
    observe_reentry_context_bar,
    plain_language_for_block,
)
from .trade_management_review import _excursion_stats

logger = logging.getLogger(__name__)


def _context_index(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict] = {}
    for r in rows:
        sym = str(r.get("symbol", "")).upper()
        ts = str(_normalize_utc(r.get("bar_ts")) or r.get("bar_ts"))[:19]
        out[(sym, ts)] = r
    return out


def run_simulation_replay_pm_v01(
    state: dict[str, Any],
    *,
    context_attempt_id: str,
    notes: str | None = None,
    pattern_replay_attempt_id: str | None = None,
) -> str:
    """Replay entries like V0_1; exits via PM01–PM06. Does not mutate official sim attempts."""
    verify_replay_ready(state)
    verify_schedule_bars(state)
    run_id = state["run_id"]
    sim_params = resolve_sim_params(state.get("configuration", {}).get("simulation_parameters"))
    meta = verify_context_attempt(
        run_id=run_id,
        context_attempt_id=context_attempt_id,
        required_ruleset=CONTEXT_V03,
    )
    ctx_rows = load_context_observations(run_id, context_attempt_id=context_attempt_id, limit=10000)
    ctx_index = _context_index(ctx_rows)
    schedule = _schedule_cache(state)
    index = _bars_index(state)
    symbols = state["symbols"]
    symbol_order = list(symbols)
    starting_cash = float(state.get("starting_cash") or sim_params["starting_cash"])
    portfolio = PMPortfolio(cash=starting_cash)
    entry_action = str(sim_params["entry_signal_action"])

    pat_attempt = pattern_replay_attempt_id or state.get("configuration", {}).get(
        "phase5_pattern_v03_attempt_id"
    )
    patterns: list[dict[str, Any]] = []
    if pat_attempt:
        patterns = load_patterns(run_id, replay_attempt_id=str(pat_attempt), limit=12000)

    sim_attempt_id = create_simulation_attempt(
        run_id=run_id,
        context_attempt_id=context_attempt_id,
        context_ruleset_version=str(meta["context_ruleset_version"]),
        starting_cash=starting_cash,
        notes=notes or "PM V0_1 review-only disposable simulation",
        simulation_ruleset_version=PM_RULESET,
    )

    dossiers_cache: dict[str, dict[str, Any]] = {}
    prev_td: date | None = None

    for step in schedule:
        if prev_td != step.trading_date:
            td_key = step.trading_date.isoformat()
            dossiers_cache[td_key] = _load_session_dossiers(state, step.trading_date)
            prev_td = step.trading_date
        is_last = step.bar_index_in_session >= 77

        if portfolio.pm_position:
            sym = portfolio.pm_position.symbol
            bar_key = (sym, step.trading_date, _normalize_utc(step.bar_timestamp_utc))
            bar = index.get(bar_key)
            if bar:
                process_pm_bar_open(portfolio, bar=bar, bar_index_in_session=step.bar_index_in_session)

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
                    f"No context row for {sym} at {ts_key}.",
                    run_id=run_id,
                )
            dossier = dossiers_cache[step.trading_date.isoformat()][sym]
            ok, _block = entry_permitted(
                ctx=ctx,
                dossier=dossier,
                bar_index_in_session=step.bar_index_in_session,
                params=sim_params,
            )
            observe_reentry_context_bar(
                portfolio,
                symbol=sym,
                trading_date=step.trading_date,
                bar_index_in_session=step.bar_index_in_session,
                ctx=ctx,
            )
            if ok:
                ok_re, re_block = evaluate_reentry_for_consider_entry(
                    portfolio,
                    symbol=sym,
                    trading_date=step.trading_date,
                    bar_index_in_session=step.bar_index_in_session,
                    ctx=ctx,
                )
                if not ok_re and re_block:
                    st = get_reentry_tracker(portfolio).state_for(sym, step.trading_date)
                    bars_since = (
                        step.bar_index_in_session - st.last_exit_bar_index
                        if st.last_exit_bar_index is not None
                        else None
                    )
                    record_blocked_entry(
                        portfolio,
                        symbol=sym,
                        signal_ts=bar.ts_utc,
                        reason=re_block,
                        candidate_action=entry_action,
                        tie_break_json={
                            "reentry_policy": "V0_1",
                            "plain_language": plain_language_for_block(re_block, bars_since_exit=bars_since),
                        },
                    )
                else:
                    entry_candidates.append({"symbol": sym, "bar": bar, "context": ctx, "dossier": dossier})

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
                winner, losers = tie_break_pick(entry_candidates, params=sim_params, symbol_order=symbol_order)
                execute_entry(portfolio, candidate=winner, bar=winner["bar"], params=sim_params)
                pos = portfolio.open_position
                if pos:
                    on_entry_filled(
                        portfolio,
                        ctx=winner["context"],
                        entry_bar=winner["bar"],
                        quantity=pos.quantity,
                    )
                for loser in losers:
                    record_blocked_entry(
                        portfolio,
                        symbol=loser["symbol"],
                        signal_ts=loser["bar"].ts_utc,
                        reason=BLOCK_REASON_TIE_BREAK,
                        candidate_action=entry_action,
                        tie_break_json={"winner": winner["symbol"]},
                    )

        if portfolio.pm_position:
            sym = portfolio.pm_position.symbol
            bar_key = (sym, step.trading_date, _normalize_utc(step.bar_timestamp_utc))
            bar = index.get(bar_key)
            ts_key = str(_normalize_utc(bar.ts_utc) or bar.ts_utc)[:19]
            ctx = ctx_index.get((sym.upper(), ts_key))
            swings = schedule_swing_confirmations_for_bar(patterns, symbol=sym, bar_ts=ts_key)
            if bar and ctx:
                process_pm_bar_close(
                    portfolio,
                    ctx=ctx,
                    bar=bar,
                    is_last_bar_in_session=is_last,
                    swing_low_confirms=swings,
                    bar_index_in_session=step.bar_index_in_session,
                )

        portfolio.bars_processed += 1

    if portfolio.pm_position and not portfolio.closed_trades:
        pos = portfolio.pm_position
        last_bar = None
        for step in reversed(schedule):
            bar_key = (pos.symbol, step.trading_date, _normalize_utc(step.bar_timestamp_utc))
            last_bar = index.get(bar_key)
            if last_bar:
                break
        if last_bar:
            from .position_management_engine import _close_trade

            _close_trade(
                portfolio,
                exit_ts=last_bar.ts_utc,
                exit_price=float(last_bar.close),
                exit_reason="FORCED_WEEK_END_FLATTEN",
                exit_decision_ts=last_bar.ts_utc,
                precedence="week_end_flatten",
                bar_index_in_session=step.bar_index_in_session if step else None,
                trading_date=last_bar.trading_date,
            )

    mfe_mae_by_trade: dict[str, Any] = {}
    for t in portfolio.closed_trades:
        exc = _excursion_stats(t)
        mfe_mae_by_trade[str(t.get("trade_id"))] = {
            **exc,
            "review_only_analytics": True,
            "not_used_as_rule_input": True,
        }

    ledger_payload = {
        "simulation_attempt_id": sim_attempt_id,
        "context_attempt_id": context_attempt_id,
        "position_management_ruleset": PM_RULESET,
        "parent_entry_ruleset": "BROOKS_SIMULATION_RULESET_V0_1",
        "events": portfolio.management_ledger,
        "trades": portfolio.closed_trades,
        "mfe_mae_post_trade": mfe_mae_by_trade,
    }
    save_management_ledger(sim_attempt_id, ledger_payload)

    try:
        conn = get_connection()
        try:
            trade_n = insert_sim_trades_batch(
                run_id=run_id,
                simulation_attempt_id=sim_attempt_id,
                trades=portfolio.closed_trades,
                conn=conn,
                commit=True,
            )
            blocked_n = insert_blocked_signals_batch(
                run_id=run_id,
                simulation_attempt_id=sim_attempt_id,
                signals=portfolio.blocked_signals,
                conn=conn,
                commit=True,
            )
        finally:
            conn.close()
    except Exception as exc:
        fail_simulation_attempt(
            simulation_attempt_id=sim_attempt_id,
            notes=f"pm_v01_fail:{type(exc).__name__}:{exc}",
        )
        raise

    seq = simulation_sequence_hash(portfolio.closed_trades, portfolio.blocked_signals)
    complete_simulation_attempt(
        simulation_attempt_id=sim_attempt_id,
        trade_count=len(portfolio.closed_trades),
        blocked_count=len(portfolio.blocked_signals),
        ending_cash=portfolio.cash,
        realized_pnl=portfolio.realized_pnl,
        sequence_hash=seq,
    )
    return sim_attempt_id
