"""Bulk Phase 7 simulation replay — gates on persisted context V0.2 only."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from app.db import get_connection

from .context_repository import load_context_observations
from .context_ruleset_v02 import RULESET_VERSION as CONTEXT_V02
from .context_ruleset_v03 import RULESET_VERSION as CONTEXT_V03
from .context_ruleset_v04 import RULESET_VERSION as CONTEXT_V04
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
from .experiment_progress import (
    PROGRESS_PHASE_COMPUTE,
    PROGRESS_PHASE_INTEGRITY,
    PROGRESS_PHASE_PERSIST,
    emit_progress,
)
from .persist_integrity import fail_simulation_attempt
from .simulation_repository import (
    complete_simulation_attempt,
    create_simulation_attempt,
    insert_blocked_signals_batch,
    insert_sim_trades_batch,
    simulation_sequence_hash,
    verify_context_attempt,
)
from .simulation_ruleset_v01 import (
    DEFAULT_CONTEXT_ATTEMPT_ID,
    REQUIRED_CONTEXT_RULESET,
    resolve_params,
)
from .reentry_policy_v01 import (
    evaluate_reentry_for_consider_entry,
    get_reentry_tracker,
    note_reentry_exit,
    observe_reentry_context_bar,
    plain_language_for_block,
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
    on_progress=None,
    required_context_ruleset: str | None = None,
    notes: str | None = None,
    allow_diagnostic_legacy: bool = False,
) -> str:
    verify_replay_ready(state)
    from .lab_execution_policy import require_diagnostic_legacy

    require_diagnostic_legacy(state, "phase7_simulation_bulk", allow_diagnostic_legacy=allow_diagnostic_legacy)
    verify_schedule_bars(state)
    run_id = state["run_id"]
    params = resolve_params(state.get("configuration", {}).get("simulation_parameters"))

    # Default remains Freeze-V1 / Phase 7 gate on V0.2. Phase E may pass V0.3 explicitly.
    required = required_context_ruleset or REQUIRED_CONTEXT_RULESET
    if required not in (CONTEXT_V02, CONTEXT_V03, CONTEXT_V04):
        raise BrooksIntradayError(
            "SIMULATION_CONTEXT_RULESET_FORBIDDEN",
            f"Simulation gating refuses {required}; allowed: {CONTEXT_V02}, {CONTEXT_V03}, {CONTEXT_V04}.",
            run_id=run_id,
        )

    meta = verify_context_attempt(
        run_id=run_id,
        context_attempt_id=context_attempt_id,
        required_ruleset=required,
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

    # No run-wide DELETE — isolation via SIMULATION_ATTEMPT_ID on new writes.
    default_notes = (
        "V0.4 simulation (intraday-led context; disposable)"
        if required == CONTEXT_V04
        else (
            "Phase E simulation (V0.3 context gating; disposable)"
            if required == CONTEXT_V03
            else "Phase 7 pilot week simulation (V0.2 context gating)"
        )
    )
    sim_attempt_id = create_simulation_attempt(
        run_id=run_id,
        context_attempt_id=context_attempt_id,
        context_ruleset_version=str(meta["context_ruleset_version"]),
        starting_cash=starting_cash,
        notes=notes or default_notes,
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
            fill_pending_exit(portfolio, bar=bar, bar_index_in_session=step.bar_index_in_session)

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
            observe_reentry_context_bar(
                portfolio,
                symbol=sym,
                trading_date=step.trading_date,
                bar_index_in_session=step.bar_index_in_session,
                ctx=ctx,
            )
            if step_res.entry_candidate:
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
            emit_progress(
                on_progress,
                step_idx + 1,
                phase=PROGRESS_PHASE_COMPUTE,
                total=len(schedule),
                unit="schedule_steps",
            )

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
            note_reentry_exit(
                portfolio,
                symbol=pos.symbol,
                trading_date=last_bar.trading_date,
                bar_index_in_session=schedule[-1].bar_index_in_session,
                exit_reason="FORCED_WEEK_END_FLATTEN",
            )

    emit_progress(
        on_progress,
        len(schedule),
        phase=PROGRESS_PHASE_COMPUTE,
        total=len(schedule),
        unit="schedule_steps",
    )
    try:
        conn = get_connection()
        try:
            open_sym = portfolio.open_position.symbol if portfolio.open_position else None
            for sig in portfolio.blocked_signals:
                if not sig.get("active_position_symbol"):
                    sig["active_position_symbol"] = open_sym
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
            emit_progress(
                on_progress,
                trade_n + blocked_n,
                phase=PROGRESS_PHASE_PERSIST,
                total=max(trade_n + blocked_n, 1),
                unit="rows",
            )
        finally:
            conn.close()

        emit_progress(on_progress, 0, phase=PROGRESS_PHASE_INTEGRITY, total=1, unit="checks")
        if trade_n != len(portfolio.closed_trades) or blocked_n != len(portfolio.blocked_signals):
            raise ValueError(
                f"SIM count mismatch trades={trade_n}/{len(portfolio.closed_trades)} "
                f"blocked={blocked_n}/{len(portfolio.blocked_signals)}"
            )
        emit_progress(on_progress, 1, phase=PROGRESS_PHASE_INTEGRITY, total=1, unit="checks")
    except Exception as exc:
        fail_simulation_attempt(
            simulation_attempt_id=sim_attempt_id,
            notes=f"phase_c_fail:{type(exc).__name__}:{exc}",
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

    cfg = state.setdefault("configuration", {})
    summary = {
        "trade_count": len(portfolio.closed_trades),
        "blocked_signal_count": len(portfolio.blocked_signals),
        "ending_cash": portfolio.cash,
        "realized_pnl": portfolio.realized_pnl,
        "sequence_hash": seq,
        "context_attempt_id": context_attempt_id,
        "context_ruleset_version": str(meta["context_ruleset_version"]),
        "simulation_attempt_id": sim_attempt_id,
    }
    if required == CONTEXT_V03:
        # Disposable Phase E / E1 — do not overwrite Freeze V1 cash or phase6b / phase7 pins.
        note_text = str(notes or cfg.get("simulation_replay_notes") or "")
        if "Phase E1" in note_text:
            state["phase_e1_simulation_v03_attempt_id"] = sim_attempt_id
            cfg["phase_e1_simulation_v03_attempt_id"] = sim_attempt_id
            cfg["phase_e1_context_v03_attempt_id"] = context_attempt_id
            cfg["phase_e1_simulation_v03_summary"] = summary
        else:
            state["phase_e_simulation_v03_attempt_id"] = sim_attempt_id
            cfg["phase_e_simulation_v03_attempt_id"] = sim_attempt_id
            cfg["phase_e_context_v03_attempt_id"] = context_attempt_id
            cfg["phase_e_simulation_v03_summary"] = summary
    elif required == CONTEXT_V04:
        state["phase_v04_simulation_attempt_id"] = sim_attempt_id
        cfg["phase_v04_simulation_attempt_id"] = sim_attempt_id
        cfg["phase_v04_context_attempt_id"] = context_attempt_id
        cfg["phase_v04_simulation_summary"] = summary
    else:
        state["current_cash"] = portfolio.cash
        state["realized_pnl"] = portfolio.realized_pnl
        state["open_position_symbol"] = None
        state["open_position_qty"] = None
        state["phase7_simulation_attempt_id"] = sim_attempt_id
        cfg["phase7_simulation_attempt_id"] = sim_attempt_id
        cfg["phase6b_context_attempt_id"] = context_attempt_id
        cfg["context_ruleset_version"] = CONTEXT_V02
        cfg["simulation_ruleset_version"] = params["ruleset_version"]
        cfg["simulation_summary"] = summary

    return sim_attempt_id
