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
    insert_context_observations_batch,
    load_pattern_snapshot_index,
)
from .experiment_progress import (
    PROGRESS_PHASE_COMPUTE,
    PROGRESS_PHASE_INTEGRITY,
    PROGRESS_PHASE_PERSIST,
    emit_progress,
)
from .persist_integrity import (
    assert_schedule_keys_match,
    fail_context_attempt,
    load_context_keys,
    schedule_key_set,
)
from .persist_mode import is_bulk_persist
from .context_engine_v03 import reset_v03_session
from .context_engine_v04 import reset_v04_session
from .context_ruleset_v01 import RULESET_VERSION as RULESET_V01
from .context_ruleset_v02 import DEFAULT_PARAMETERS as V02_DEFAULT_PARAMETERS
from .context_ruleset_v02 import RULESET_VERSION as RULESET_V02
from .context_ruleset_v03 import DEFAULT_PARAMETERS as V03_DEFAULT_PARAMETERS
from .context_ruleset_v03 import RULESET_VERSION as RULESET_V03
from .context_ruleset_v04 import DEFAULT_PARAMETERS as V04_DEFAULT_PARAMETERS
from .context_ruleset_v04 import RULESET_VERSION as RULESET_V04
from .context_ruleset_v01 import ACTION_CONSIDER_ENTRY
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
    on_progress=None,
    allow_diagnostic_legacy: bool = False,
) -> str:
    verify_replay_ready(state)
    from .lab_execution_policy import require_diagnostic_legacy

    require_diagnostic_legacy(
        state,
        f"phase6_context_bulk ({ruleset_version})",
        allow_diagnostic_legacy=allow_diagnostic_legacy,
    )
    verify_schedule_bars(state)
    run_id = state["run_id"]
    objective_id, pattern_id = ensure_context_attempt_ids(state)
    state.pop("_context_session_state", None)

    if ruleset_version == RULESET_V04:
        default_notes = "V0.4 intraday-led context replay (disposable; not pinned)"
        params = V04_DEFAULT_PARAMETERS
    elif ruleset_version == RULESET_V03:
        default_notes = "Phase E V0.3 context replay (disposable; not Freeze V1 pin)"
        params = V03_DEFAULT_PARAMETERS
    elif ruleset_version == RULESET_V02:
        default_notes = "Phase 6B bulk context evaluation (V0.2)"
        params = V02_DEFAULT_PARAMETERS
    else:
        default_notes = "Phase 6 bulk context evaluation"
        params = None

    context_attempt_id = create_context_attempt(
        run_id=run_id,
        objective_attempt_id=objective_id,
        pattern_attempt_id=pattern_id,
        notes=notes or default_notes,
        ruleset_version=ruleset_version,
        parameters=params,
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
    state.pop("_context_v03_session_state", None)
    shadow_open_symbol: str | None = None
    v03_priority = list(
        (V04_DEFAULT_PARAMETERS if ruleset_version == RULESET_V04 else V03_DEFAULT_PARAMETERS).get(
            "symbol_priority"
        )
        or []
    )

    for step_idx, step in enumerate(schedule):
        if prev_td != step.trading_date:
            for s in symbols:
                reset_context_session(state, s, step.trading_date)
                if ruleset_version == RULESET_V04:
                    reset_v04_session(state, s, step.trading_date)
                elif ruleset_version == RULESET_V03:
                    reset_v03_session(state, s, step.trading_date)
            prev_td = step.trading_date
            shadow_open_symbol = None
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
            open_sym = shadow_open_symbol if ruleset_version in (RULESET_V03, RULESET_V04) else None
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
                open_position_symbol=open_sym,
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

        if ruleset_version in (RULESET_V03, RULESET_V04) and shadow_open_symbol is None:
            step_ce = [
                str(sym).upper()
                for sym, *_rest, res in pending[-len(symbols) :]
                if res.selected_action == ACTION_CONSIDER_ENTRY
            ]
            if step_ce:

                def _pri(s: str) -> int:
                    return v03_priority.index(s) if s in v03_priority else 999

                shadow_open_symbol = min(step_ce, key=_pri)

        if progress_every and (step_idx + 1) % progress_every == 0:
            print(f"context step {step_idx + 1}/{len(schedule)}", flush=True)
            emit_progress(
                on_progress,
                seq,
                phase=PROGRESS_PHASE_COMPUTE,
                total=len(schedule) * len(symbols),
                unit="observations",
            )

    emit_progress(
        on_progress,
        seq,
        phase=PROGRESS_PHASE_COMPUTE,
        total=len(pending),
        unit="observations",
    )

    expected_keys = schedule_key_set(schedule, symbols, normalize_utc=_normalize_utc)
    try:
        conn = get_connection()
        try:
            if is_bulk_persist():
                batch_rows = [
                    {
                        "run_id": run_id,
                        "context_attempt_id": context_attempt_id,
                        "symbol": sym,
                        "trading_date": td,
                        "bar_ts_utc": ts,
                        "sequence_num": sequence_num,
                        "objective_attempt_id": objective_id,
                        "pattern_attempt_id": pattern_id,
                        "dossier_id": str(dossier_id) if dossier_id else None,
                        "result": result,
                        "ruleset_version": ruleset_version,
                    }
                    for sym, td, ts, sequence_num, dossier_id, result in pending
                ]
                written = 0
                for i in range(0, len(batch_rows), 500):
                    chunk = batch_rows[i : i + 500]
                    written += insert_context_observations_batch(
                        chunk,
                        conn=conn,
                        commit=True,
                        chunk_size=500,
                        ruleset_version=ruleset_version,
                    )
                    emit_progress(
                        on_progress,
                        written,
                        phase=PROGRESS_PHASE_PERSIST,
                        total=len(batch_rows),
                        unit="rows",
                    )
            else:
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
                        emit_progress(
                            on_progress,
                            i,
                            phase=PROGRESS_PHASE_PERSIST,
                            total=len(pending),
                            unit="rows",
                        )
                conn.commit()
                emit_progress(
                    on_progress,
                    len(pending),
                    phase=PROGRESS_PHASE_PERSIST,
                    total=len(pending),
                    unit="rows",
                )
        finally:
            conn.close()

        emit_progress(on_progress, 0, phase=PROGRESS_PHASE_INTEGRITY, total=1, unit="checks")
        persisted = load_context_keys(run_id, context_attempt_id=context_attempt_id)
        assert_schedule_keys_match(expected=expected_keys, persisted=persisted, label="CONTEXT_OBSERVATION")
        emit_progress(on_progress, 1, phase=PROGRESS_PHASE_INTEGRITY, total=1, unit="checks")
    except Exception as exc:
        fail_context_attempt(
            context_attempt_id=context_attempt_id,
            notes=f"phase_c_fail:{type(exc).__name__}:{exc}; expected={len(expected_keys)}",
        )
        raise

    row_count = count_context_rows(run_id, context_attempt_id=context_attempt_id)
    seq_hash = context_sequence_hash(run_id, context_attempt_id=context_attempt_id)
    complete_context_attempt(context_attempt_id=context_attempt_id, row_count=row_count, sequence_hash=seq_hash)

    if ruleset_version == RULESET_V02:
        state.setdefault("configuration", {})["context_ruleset_version"] = ruleset_version
        state["phase6b_context_attempt_id"] = context_attempt_id
        state.setdefault("configuration", {})["phase6b_context_attempt_id"] = context_attempt_id
    elif ruleset_version == RULESET_V04:
        state["phase_v04_context_attempt_id"] = context_attempt_id
        state.setdefault("configuration", {})["phase_v04_context_attempt_id"] = context_attempt_id
    elif ruleset_version == RULESET_V03:
        # Disposable Phase E / E1 attempt — never overwrite Freeze V1 phase6b pin.
        note_text = notes or ""
        if "Phase E1" in note_text:
            state["phase_e1_context_v03_attempt_id"] = context_attempt_id
            state.setdefault("configuration", {})["phase_e1_context_v03_attempt_id"] = context_attempt_id
        else:
            state["phase_e_context_v03_attempt_id"] = context_attempt_id
            state.setdefault("configuration", {})["phase_e_context_v03_attempt_id"] = context_attempt_id
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
