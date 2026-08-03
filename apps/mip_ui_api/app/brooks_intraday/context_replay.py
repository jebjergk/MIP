"""Bulk and single-step Phase 6 context replay."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from app.db import get_connection

from .context_engine import advance_context_for_bar, reset_context_session
from .context_repository import (
    complete_context_attempt,
    context_sequence_hash,
    count_context_rows,
    create_context_attempt,
    insert_context_observation,
    load_pattern_snapshot_index,
)
from .context_ruleset_v01 import RULESET_VERSION as RULESET_V01
from .context_ruleset_v02 import DEFAULT_PARAMETERS as V02_DEFAULT_PARAMETERS
from .context_ruleset_v02 import RULESET_VERSION as RULESET_V02
from .errors import BrooksIntradayError
from .observation_repository import load_baseline_observation_index
from .replay_engine import (
    _bars_index,
    _load_session_dossiers,
    _normalize_utc,
    _schedule_cache,
    get_cursor,
    save_cursor,
    verify_replay_ready,
    verify_schedule_bars,
)

logger = logging.getLogger(__name__)

PHASE4_BASELINE = "53a502f5-dec4-4bff-8106-f6637574163e"
PHASE5C_PATTERN = "d83d5bab-4e02-4aec-9bd7-0da54b1395d3"


def ensure_context_attempt_ids(state: dict[str, Any]) -> tuple[str, str]:
    obj = (
        state.get("phase4_review_baseline_attempt_id")
        or state.get("configuration", {}).get("phase4_review_baseline_attempt_id")
        or PHASE4_BASELINE
    )
    pat = (
        state.get("phase5_pattern_v03_attempt_id")
        or state.get("configuration", {}).get("phase5_pattern_v03_attempt_id")
        or PHASE5C_PATTERN
    )
    return str(obj), str(pat)


def run_context_replay_bulk(
    state: dict[str, Any],
    *,
    progress_every: int = 50,
    ruleset_version: str = RULESET_V01,
    notes: str | None = None,
) -> str:
    verify_replay_ready(state)
    verify_schedule_bars(state)
    run_id = state["run_id"]
    objective_id, pattern_id = ensure_context_attempt_ids(state)
    state.pop("_context_session_state", None)

    context_attempt_id = create_context_attempt(
        run_id=run_id,
        objective_attempt_id=objective_id,
        pattern_attempt_id=pattern_id,
        notes=notes or ("Phase 6B bulk context evaluation (V0.2)" if ruleset_version == RULESET_V02 else "Phase 6 bulk context evaluation"),
        ruleset_version=ruleset_version,
        parameters=V02_DEFAULT_PARAMETERS if ruleset_version == RULESET_V02 else None,
    )

    obs_index = load_baseline_observation_index(run_id, objective_id)
    pat_index = load_pattern_snapshot_index(run_id, pattern_attempt_id=pattern_id)
    schedule = _schedule_cache(state)
    index = _bars_index(state)
    symbols = state["symbols"]
    cursor = get_cursor(state)
    dossiers_cache: dict[str, dict[str, Any]] = {}
    pending: list[tuple] = []
    seq = 0
    prev_td: date | None = None

    for step_idx, step in enumerate(schedule):
        if prev_td != step.trading_date:
            for s in symbols:
                reset_context_session(state, s, step.trading_date)
            prev_td = step.trading_date
        td_key = step.trading_date.isoformat()
        if td_key not in dossiers_cache:
            dossiers_cache[td_key] = _load_session_dossiers(state, step.trading_date)

        for sym_index, sym in enumerate(symbols):
            bar_key = (sym, step.trading_date, _normalize_utc(step.bar_timestamp_utc))
            bar = index.get(bar_key)
            if not bar:
                raise BrooksIntradayError(
                    "FROZEN_DATASET_MISMATCH",
                    f"Missing bar for {sym} at {step.bar_timestamp_utc}.",
                    run_id=run_id,
                    symbol=sym,
                )
            obs_key = (sym.upper(), str(_normalize_utc(bar.ts_utc) or bar.ts_utc)[:19])
            objective_obs = obs_index.get(obs_key)
            if not objective_obs:
                raise BrooksIntradayError(
                    "CONTEXT_MISSING_OBJECTIVE",
                    f"No objective observation for {sym} at {bar.ts_utc}.",
                    run_id=run_id,
                )
            patterns = pat_index.get(obs_key, [])
            dossier = dossiers_cache[td_key][sym]
            result = advance_context_for_bar(
                state=state,
                dossier=dossier,
                symbol=sym,
                trading_date=step.trading_date,
                bar=bar,
                bar_index_in_session=step.bar_index_in_session,
                objective_obs=objective_obs,
                patterns=patterns,
                ruleset_version=ruleset_version,
            )
            pending.append(
                (
                    sym,
                    step.trading_date,
                    _normalize_utc(bar.ts_utc) or bar.ts_utc,
                    seq,
                    dossier.get("paa_analysis_id") or dossier.get("reconstruction_id"),
                    result,
                )
            )
            seq += 1

        if progress_every and (step_idx + 1) % progress_every == 0:
            print(f"context step {step_idx + 1}/{len(schedule)}", flush=True)

    conn = get_connection()
    try:
        for i, (sym, td, ts, sequence_num, dossier_id, result) in enumerate(pending, start=1):
            insert_context_observation(
                run_id=run_id,
                context_attempt_id=context_attempt_id,
                symbol=sym,
                trading_date=td,
                bar_ts_utc=ts,
                sequence_num=sequence_num,
                objective_attempt_id=objective_id,
                pattern_attempt_id=pattern_id,
                dossier_id=str(dossier_id) if dossier_id else None,
                result=result,
                conn=conn,
                commit=False,
                ruleset_version=ruleset_version,
            )
            if i % 400 == 0:
                conn.commit()
                print(f"context flush {i}/{len(pending)}", flush=True)
        conn.commit()
    finally:
        conn.close()

    row_count = count_context_rows(run_id, context_attempt_id=context_attempt_id)
    seq_hash = context_sequence_hash(run_id, context_attempt_id=context_attempt_id)
    complete_context_attempt(context_attempt_id=context_attempt_id, row_count=row_count, sequence_hash=seq_hash)

    state.setdefault("configuration", {})["context_ruleset_version"] = ruleset_version
    if ruleset_version == RULESET_V02:
        state["phase6b_context_attempt_id"] = context_attempt_id
        state.setdefault("configuration", {})["phase6b_context_attempt_id"] = context_attempt_id
    else:
        state["phase6_context_attempt_id"] = context_attempt_id
        state.setdefault("configuration", {})["phase6_context_attempt_id"] = context_attempt_id
    cursor["context_completed_steps"] = len(schedule)
    save_cursor(state, cursor)
    return context_attempt_id


def run_context_step_interactive(state: dict[str, Any]) -> dict[str, Any]:
    """Process one schedule slice (all symbols) for context — mirrors bulk slice."""
    verify_replay_ready(state)
    run_id = state["run_id"]
    objective_id, pattern_id = ensure_context_attempt_ids(state)
    context_attempt_id = state.get("phase6_context_attempt_id") or state.get("configuration", {}).get(
        "phase6_context_attempt_id"
    )
    if not context_attempt_id:
        context_attempt_id = create_context_attempt(
            run_id=run_id,
            objective_attempt_id=objective_id,
            pattern_attempt_id=pattern_id,
            notes="Phase 6 interactive context",
        )
        state["phase6_context_attempt_id"] = context_attempt_id

    cursor = get_cursor(state)
    schedule = _schedule_cache(state)
    completed = int(cursor.get("context_completed_steps") or cursor.get("completed_steps") or 0)
    if completed >= len(schedule):
        return {"status": "COMPLETED", "context_attempt_id": context_attempt_id}

    step = schedule[completed]
    dossiers = _load_session_dossiers(state, step.trading_date)
    obs_index = load_baseline_observation_index(run_id, objective_id)
    pat_index = load_pattern_snapshot_index(run_id, pattern_attempt_id=pattern_id)
    index = _bars_index(state)
    results = []
    seq_base = completed * len(state["symbols"])

    for sym_index, sym in enumerate(state["symbols"]):
        bar = index.get((sym, step.trading_date, _normalize_utc(step.bar_timestamp_utc)))
        if not bar:
            continue
        obs_key = (sym.upper(), str(_normalize_utc(bar.ts_utc) or bar.ts_utc)[:19])
        objective_obs = obs_index.get(obs_key)
        if not objective_obs:
            continue
        patterns = pat_index.get(obs_key, [])
        dossier = dossiers[sym]
        result = advance_context_for_bar(
            state=state,
            dossier=dossier,
            symbol=sym,
            trading_date=step.trading_date,
            bar=bar,
            bar_index_in_session=step.bar_index_in_session,
            objective_obs=objective_obs,
            patterns=patterns,
        )
        insert_context_observation(
            run_id=run_id,
            context_attempt_id=context_attempt_id,
            symbol=sym,
            trading_date=step.trading_date,
            bar_ts_utc=_normalize_utc(bar.ts_utc) or bar.ts_utc,
            sequence_num=seq_base + sym_index,
            objective_attempt_id=objective_id,
            pattern_attempt_id=pattern_id,
            dossier_id=str(dossier.get("paa_analysis_id") or dossier.get("reconstruction_id") or "") or None,
            result=result,
        )
        results.append({"symbol": sym, "action": result.selected_action, "state": result.state_after})

    cursor["context_completed_steps"] = completed + 1
    save_cursor(state, cursor)
    return {"status": "OK", "context_attempt_id": context_attempt_id, "symbols": results}
