"""Phase 6B read-only audits over persisted context observations (no replay mutation)."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from typing import Any

from .context_engine_v01 import _dossier_levels
from .context_ruleset_v01 import (
    ACTION_CONSIDER_ENTRY,
    ACTION_DO_NOT_CHASE,
    ACTION_DO_NOT_ENTER,
    ACTION_ENTRY_ARMED,
    ACTION_THESIS_INVALIDATED,
)
from .level_derivation import assess_readiness

TRANSITION_ACTIONS = frozenset(
    {
        ACTION_DO_NOT_ENTER,
        ACTION_DO_NOT_CHASE,
        ACTION_THESIS_INVALIDATED,
        ACTION_ENTRY_ARMED,
        ACTION_CONSIDER_ENTRY,
    }
)

FUNNEL_STATES = [
    "DOSSIER_READY",
    "OBSERVING_OPEN",
    "WAITING_FOR_PULLBACK",
    "WAITING_FOR_SUPPORT_TEST",
    "WAITING_FOR_RTH_CONFIRMATION",
    "WAITING_FOR_STRONGER_CONFIRMATION",
    "SETUP_DEVELOPING",
    "WAITING_FOR_RECLAIM",
    "WAITING_FOR_FOLLOW_THROUGH",
    "ENTRY_ARMED",
    "CONSIDER_ENTRY",
    "ENTRY_BLOCKED",
    "DO_NOT_CHASE",
    "THESIS_WEAKENED",
    "THESIS_INVALIDATED",
    "OBSERVATION_ONLY",
    "WAITING_FOR_NEW_SETUP",
]

PATTERN_FAMILIES = [
    "STRUCTURAL_DOUBLE_BOTTOM",
    "LOCAL_DOUBLE_BOTTOM",
    "POSSIBLE_H2_LONG",
    "TWO_LEGGED_PULLBACK",
    "CONFIRMED_WEDGE_BOTTOM",
    "STRUCTURAL_BREAKOUT",
    "FAILED_BEAR_BREAKOUT",
    "BEAR_MICRO_CHANNEL_COMPLETION",
    "BULL_MICRO_CHANNEL",
]

V03_ALIASES = {
    "H2_LONG": "POSSIBLE_H2_LONG",
    "CONFIRMED_H2_LONG": "POSSIBLE_H2_LONG",
    "WEDGE_BOTTOM": "CONFIRMED_WEDGE_BOTTOM",
    "BEAR_MICRO_CHANNEL": "BEAR_MICRO_CHANNEL_COMPLETION",
}


def ui_locator(
    run_id: str,
    symbol: str,
    bar_ts: Any,
    *,
    context_attempt_id: str | None = None,
    simulation_attempt_id: str | None = None,
) -> str:
    ts = str(bar_ts).replace(" ", "T")[:19]
    base = f"brooks-lab/{run_id}/{symbol}@{ts}#CTX"
    if context_attempt_id and simulation_attempt_id:
        return (
            f"{base}?context_attempt_id={context_attempt_id}"
            f"&simulation_attempt_id={simulation_attempt_id}"
        )
    return base


def parse_ui_locator(locator: str) -> dict[str, str | None]:
    """Parse brooks-lab locator, including optional review-chain attempt ids."""
    text = str(locator or "").strip()
    out: dict[str, str | None] = {
        "run_id": None,
        "symbol": None,
        "bar_ts": None,
        "context_attempt_id": None,
        "simulation_attempt_id": None,
    }
    if not text.startswith("brooks-lab/"):
        return out
    body = text[len("brooks-lab/") :]
    path, _, query = body.partition("?")
    path = path.split("#", 1)[0]
    run_part, _, rest = path.partition("/")
    out["run_id"] = run_part or None
    if "@" in rest:
        sym, _, ts = rest.partition("@")
        out["symbol"] = sym or None
        out["bar_ts"] = ts.replace(" ", "T")[:19] if ts else None
    if query:
        from urllib.parse import parse_qs

        qs = parse_qs(query, keep_blank_values=False)
        if qs.get("context_attempt_id"):
            out["context_attempt_id"] = qs["context_attempt_id"][0]
        if qs.get("simulation_attempt_id"):
            out["simulation_attempt_id"] = qs["simulation_attempt_id"][0]
    return out


def _payload(r: dict) -> dict:
    return r.get("payload_json") or {}


def _level_value(dossier: dict, key: str) -> float | None:
    derived = dossier.get("derived_levels") or {}
    if key in derived:
        v = derived[key]
        if isinstance(v, dict):
            return float(v["value"]) if v.get("value") is not None else None
        if v is not None:
            return float(v)
    flat = {
        "primary_support": dossier.get("support_level"),
        "primary_resistance": dossier.get("resistance_level"),
        "reclaim_level": dossier.get("reclaim_level"),
        "do_not_chase_level": dossier.get("do_not_chase_level"),
        "invalidation_level": dossier.get("invalidation_level"),
    }.get(key)
    if flat is not None:
        return float(flat)
    lv = _dossier_levels(dossier)
    m = {
        "primary_support": "primary_support",
        "primary_resistance": "primary_resistance",
        "reclaim_level": "reclaim",
        "do_not_chase_level": "do_not_chase",
        "invalidation_level": "daily_thesis_invalidation",
    }
    k = m.get(key)
    if k and lv.get(k) is not None:
        return float(lv[k])
    return None


def dossier_readiness_matrix(
    dossier_rows: list[dict],
    *,
    run_id: str,
) -> list[dict]:
    out = []
    for rec in dossier_rows:
        d = rec.get("frozen_dossier_json") or {}
        sym = rec.get("symbol")
        td = rec.get("trading_date")
        verdict = str(d.get("paa_verdict") or "")
        obs_r, sim_r, status, msgs = assess_readiness(d, verdict=verdict)
        frozen_obs = d.get("observation_ready")
        frozen_sim = d.get("trade_simulation_ready")
        missing: list[str] = []
        if not d.get("paa_verdict"):
            missing.append("paa_verdict")
        if not (d.get("paa_analysis_id") or d.get("reconstruction_id")):
            missing.append("provenance_id")
        inv = _level_value(d, "invalidation_level")
        if inv is None:
            missing.append("invalidation_level")
        phase6_v01_gate = bool(d.get("trade_simulation_ready"))
        discrepancy = None
        if frozen_sim is True and not sim_r:
            discrepancy = "assess_readiness_would_downgrade_frozen_sim"
        elif frozen_sim is True and not phase6_v01_gate:
            discrepancy = "impossible"
        elif frozen_sim is not True and phase6_v01_gate:
            discrepancy = "v01_uses_truthy_frozen_flag_only"
        out.append(
            {
                "symbol": sym,
                "trading_date": str(td),
                "verdict": verdict,
                "observation_ready_frozen": frozen_obs,
                "simulation_ready_frozen": frozen_sim,
                "observation_ready_assess": obs_r,
                "simulation_ready_assess": sim_r,
                "phase6_v01_simulation_gate": phase6_v01_gate,
                "missing_required_fields": missing,
                "validation_messages": msgs,
                "validation_status": status,
                "support_level": _level_value(d, "primary_support"),
                "resistance_level": _level_value(d, "primary_resistance"),
                "reclaim_level": _level_value(d, "reclaim_level"),
                "do_not_chase_level": _level_value(d, "do_not_chase_level"),
                "daily_thesis_invalidation": _level_value(d, "invalidation_level"),
                "origin": d.get("paa_analysis_id") or d.get("reconstruction_id"),
                "reconstruction_version": rec.get("dossier_version") or d.get("dossier_version"),
                "compiler_version": rec.get("compiler_version"),
                "phase6_interpretation": (
                    "simulation_ready_via_frozen_flag"
                    if phase6_v01_gate
                    else "blocked_by_missing_trade_simulation_ready_flag"
                ),
                "readiness_discrepancy": discrepancy,
                "ui_locator": f"brooks-lab/{run_id}/{sym}@{td}#DOSSIER",
            }
        )
    return out


def _session_key(r: dict) -> tuple:
    return (r.get("symbol"), str(r.get("trading_date")))


def action_event_and_duration(rows: list[dict]) -> dict[str, Any]:
    action_duration = Counter(r.get("selected_action") for r in rows)
    action_events: Counter[str] = Counter()
    state_duration: Counter[str] = Counter()
    state_transitions: Counter[str] = Counter()
    by_session: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by_session[_session_key(r)].append(r)
    for _sk, sess_rows in by_session.items():
        sess_rows.sort(key=lambda x: (x.get("bar_ts"), x.get("sequence_num") or 0))
        prev_action = None
        prev_state = None
        for r in sess_rows:
            act = r.get("selected_action")
            st = r.get("state_after")
            state_duration[st] += 1
            if act != prev_action and act in TRANSITION_ACTIONS:
                action_events[act] += 1
            if st != prev_state:
                state_transitions[st] += 1
            prev_action = act
            prev_state = st
    return {
        "action_duration_counts": dict(action_duration),
        "action_transition_event_counts": dict(action_events),
        "state_duration_counts": dict(state_duration),
        "state_transition_into_counts": dict(state_transitions),
    }


def state_funnel(rows: list[dict]) -> dict[str, Any]:
    by_session: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by_session[_session_key(r)].append(r)
    sessions_entering: Counter[str] = Counter()
    transition_into: Counter[str] = Counter()
    bars_in_state: Counter[str] = Counter()
    transition_pairs: Counter[tuple[str, str]] = Counter()
    for _sk, sess_rows in by_session.items():
        sess_rows.sort(key=lambda x: (x.get("bar_ts"), x.get("sequence_num") or 0))
        seen: set[str] = set()
        prev_state = None
        for r in sess_rows:
            st = r.get("state_after")
            if st in FUNNEL_STATES and st not in seen:
                sessions_entering[st] += 1
                seen.add(st)
            if st != prev_state and st is not None:
                transition_into[st] += 1
                if prev_state is not None:
                    transition_pairs[(prev_state, st)] += 1
            bars_in_state[st] += 1
            prev_state = st
    avg_bars = {s: bars_in_state[s] / max(sessions_entering[s], 1) for s in FUNNEL_STATES}
    never_reached = [s for s in FUNNEL_STATES if sessions_entering[s] == 0]
    early_permanent = []
    for sk, sess_rows in by_session.items():
        if not sess_rows:
            continue
        sess_rows.sort(key=lambda x: x.get("bar_ts"))
        first = sess_rows[0].get("state_after")
        if len(sess_rows) >= 10 and all(r.get("state_after") == first for r in sess_rows[5:]):
            if first in ("ENTRY_BLOCKED", "DO_NOT_CHASE", "THESIS_INVALIDATED", "OBSERVATION_ONLY"):
                early_permanent.append({"symbol": sk[0], "trading_date": sk[1], "state": first})
    top_gate = transition_into.most_common(5)
    return {
        "sessions_entering_state": dict(sessions_entering),
        "transitions_into_state": dict(transition_into),
        "average_bars_in_state": avg_bars,
        "states_never_reached": never_reached,
        "transition_pairs_top": [{"from": a, "to": b, "count": c} for (a, b), c in transition_pairs.most_common(30)],
        "sessions_permanent_early": early_permanent[:40],
        "likely_gate": top_gate[0][0] if top_gate else None,
    }


def classify_do_not_enter_event(r: dict, dossier: dict | None) -> str:
    pj = _payload(r)
    blockers = pj.get("blockers_json") or []
    room = pj.get("room_class") or (pj.get("context_classifications_json") or [None])[-1]
    ctx = pj.get("contextual_interpretation") or pj.get("layers_json", {}).get("contextual_interpretation") or {}
    if isinstance(ctx, dict):
        room = room or ctx.get("room_class")
    st_after = r.get("state_after")
    if "DOSSIER_NOT_SIMULATION_READY" in blockers:
        return "dossier_not_simulation_ready"
    if dossier and str(dossier.get("paa_verdict", "")).upper() in ("NO_CLEAR_LONG", "DEFER"):
        return "no_clear_long_verdict"
    if "LIMITED_ROOM_TO_RESISTANCE" in blockers or st_after == "ENTRY_BLOCKED":
        if room == "AT_RESISTANCE":
            return "resistance_blocker"
        return "insufficient_room"
    if room == "AT_RESISTANCE":
        return "resistance_blocker"
    if room == "LIMITED_ROOM":
        return "insufficient_room"
    if r.get("thesis_effect") == "INVALIDATES":
        return "daily_thesis_invalidation"
    patterns = (pj.get("layers_json") or {}).get("pattern_instance", {}).get("patterns") or []
    bearish = any("BEAR" in str(p.get("family", "")).upper() for p in patterns)
    if bearish:
        return "bearish_structural_pattern"
    if not dossier or not dossier.get("invalidation_level") and not _level_value(dossier or {}, "invalidation_level"):
        return "missing_invalidation"
    if ctx.get("opening_period"):
        return "opening_restriction"
    return "other"


def do_not_enter_audit(rows: list[dict], dossiers: dict[tuple, dict], *, run_id: str) -> dict[str, Any]:
    by_session: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by_session[_session_key(r)].append(r)
    event_reason: Counter[str] = Counter()
    duration_reason: Counter[str] = Counter()
    by_sym_day: Counter[tuple] = Counter()
    examples: list[dict] = []
    for sk, sess_rows in by_session.items():
        sess_rows.sort(key=lambda x: x.get("bar_ts"))
        prev = None
        doss = dossiers.get(sk)
        for r in sess_rows:
            act = r.get("selected_action")
            reason = classify_do_not_enter_event(r, doss) if act == ACTION_DO_NOT_ENTER else None
            if act == ACTION_DO_NOT_ENTER:
                duration_reason[reason or "other"] += 1
            if act == ACTION_DO_NOT_ENTER and act != prev:
                event_reason[reason or "other"] += 1
                by_sym_day[(sk[0], sk[1], reason)] += 1
                if len(examples) < 15:
                    examples.append(_review_record(r, doss, run_id=run_id, reason=reason))
            prev = act
    return {
        "event_counts_by_reason": dict(event_reason),
        "duration_counts_by_reason": dict(duration_reason),
        "events_by_symbol_day_reason": [
            {"symbol": a, "trading_date": b, "reason": c, "events": n}
            for (a, b, c), n in by_sym_day.most_common(50)
        ],
        "examples": examples,
        "finding": (
            "V0.1 maps LIMITED_ROOM/ENTRY_BLOCKED to DO_NOT_ENTER via action_precedence; "
            "missing confirmation usually yields OBSERVE/WAIT_FOR_PULLBACK, not DO_NOT_ENTER."
        ),
    }


def _review_record(r: dict, dossier: dict | None, *, run_id: str, reason: str | None = None) -> dict:
    pj = _payload(r)
    layers = pj.get("layers_json") or {}
    return {
        "symbol": r.get("symbol"),
        "timestamp": str(r.get("bar_ts")),
        "dossier_summary": {
            "verdict": (dossier or {}).get("paa_verdict"),
            "trade_simulation_ready": (dossier or {}).get("trade_simulation_ready"),
        },
        "active_levels": pj.get("active_levels_json"),
        "active_patterns": layers.get("pattern_instance"),
        "condition_matrix": layers.get("diagnostics", {}).get("entry_condition_matrix"),
        "state_before": r.get("state_before"),
        "state_after": r.get("state_after"),
        "selected_action": r.get("selected_action"),
        "candidate_actions": pj.get("candidate_actions_json"),
        "blocker": pj.get("blockers_json"),
        "do_not_enter_reason": reason,
        "explanation": r.get("explanation"),
        "ui_locator": ui_locator(run_id, str(r.get("symbol")), r.get("bar_ts")),
    }


def thesis_invalidation_audit(rows: list[dict], run_id: str) -> dict[str, Any]:
    events: list[dict] = []
    by_session: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by_session[_session_key(r)].append(r)
    for sk, sess_rows in by_session.items():
        sess_rows.sort(key=lambda x: x.get("bar_ts"))
        prev_action = None
        invalidated_idx = None
        for i, r in enumerate(sess_rows):
            pj = _payload(r)
            act = r.get("selected_action")
            if act == ACTION_THESIS_INVALIDATED and act != prev_action:
                daily = pj.get("daily_thesis_invalidation")
                setup = pj.get("intraday_setup_invalidation")
                opp = pj.get("opposing_evidence_json") or []
                rule = opp[0].get("rule_id") if opp else None
                inv_type = "daily" if r.get("thesis_effect") == "INVALIDATES" else "intraday"
                if any(e.get("type") == "INTRADAY_SETUP_INVALIDATION" for e in opp):
                    inv_type = "intraday_as_weakens_in_v01"
                recovered = any(
                    sess_rows[j].get("selected_action") != ACTION_THESIS_INVALIDATED
                    for j in range(i + 1, len(sess_rows))
                )
                layers = pj.get("layers_json") or {}
                events.append(
                    {
                        "symbol": r.get("symbol"),
                        "timestamp": str(r.get("bar_ts")),
                        "daily_thesis_invalidation": daily,
                        "close": (layers.get("objective_fact") or {}).get("ohlcv", {}).get("close"),
                        "intraday_setup_invalidation": setup,
                        "rule_id": rule,
                        "invalidation_scope": inv_type,
                        "active_patterns": layers.get("pattern_instance"),
                        "state_before": r.get("state_before"),
                        "remaining_bars": len(sess_rows) - i - 1,
                        "recovered_later": recovered,
                        "repeat_while_already_invalidated": prev_action == ACTION_THESIS_INVALIDATED,
                        "ui_locator": ui_locator(run_id, str(r.get("symbol")), r.get("bar_ts")),
                    }
                )
                invalidated_idx = i
            prev_action = act
    repeat_rows = sum(1 for r in rows if r.get("selected_action") == ACTION_THESIS_INVALIDATED) - len(events)
    return {
        "transition_event_count": len(events),
        "duration_rows_thesis_invalidated_action": sum(
            1 for r in rows if r.get("selected_action") == ACTION_THESIS_INVALIDATED
        ),
        "repeat_action_rows_after_first_event": max(0, repeat_rows),
        "events": events,
    }


def near_entry_score_v01(r: dict) -> tuple[int, dict[str, str]]:
    pj = _payload(r)
    layers = pj.get("layers_json") or {}
    ctx = layers.get("contextual_interpretation") or {}
    patterns = layers.get("pattern_instance", {}).get("patterns") or []
    room = ctx.get("room_class") or pj.get("room_class")
    support = ctx.get("support_status") or pj.get("support_status")
    reclaim = ctx.get("reclaim_stage") or pj.get("reclaim_stage")
    matrix: dict[str, str] = {}
    thesis_ok = r.get("thesis_effect") != "INVALIDATES"
    matrix["daily_thesis_valid"] = "passed" if thesis_ok else "failed"
    sim = ctx.get("simulation_ready")
    if sim is None:
        sim = True  # v01 rows omit; dossiers frozen true
    matrix["dossier_simulation_ready"] = "passed" if sim else "failed"
    matrix["opening_complete"] = "failed" if ctx.get("opening_period") else "passed"
    meaningful = bool(patterns) and not all("MICRO" in str(p.get("family", "")) for p in patterns)
    matrix["meaningful_long_pattern"] = "passed" if meaningful else "failed"
    matrix["support_held"] = "passed" if support == "SUPPORT_HELD" else ("not_assessed" if not support else "failed")
    matrix["intraday_setup_invalidation_available"] = (
        "passed" if pj.get("intraday_setup_invalidation") else "not_assessed"
    )
    matrix["reclaim_status"] = "passed" if reclaim in ("CLOSED_ABOVE_RECLAIM", "RECLAIM_CONFIRMED") else "not_assessed"
    matrix["room_acceptable"] = "passed" if room in ("AMPLE_ROOM", "ACCEPTABLE_ROOM") else "failed"
    dnc = (pj.get("active_levels_json") or {}).get("do_not_chase")
    close = (layers.get("objective_fact") or {}).get("ohlcv", {}).get("close")
    if dnc is None or close is None:
        matrix["below_do_not_chase"] = "not_assessed"
    else:
        matrix["below_do_not_chase"] = "passed" if float(close) < float(dnc) else "failed"
    blockers = pj.get("blockers_json") or []
    matrix["no_higher_priority_blocker"] = "passed" if not blockers else "failed"
    score = sum(1 for v in matrix.values() if v == "passed")
    return score, matrix


def nearest_entry_events(rows: list[dict], *, run_id: str, top_n: int = 30) -> list[dict]:
    scored: list[tuple[int, dict]] = []
    for r in rows:
        if r.get("selected_action") in (ACTION_ENTRY_ARMED, ACTION_CONSIDER_ENTRY):
            continue
        score, matrix = near_entry_score_v01(r)
        scored.append((score, {**_review_record(r, None, run_id=run_id), "entry_score": score, "condition_matrix": matrix}))
    scored.sort(key=lambda x: (-x[0], str(x[1].get("timestamp"))))
    return [x[1] for x in scored[:top_n]]


def pattern_utilization(rows: list[dict], pattern_index: dict[tuple, list[dict]]) -> dict[str, Any]:
    stats: dict[str, Counter] = {fam: Counter() for fam in PATTERN_FAMILIES}
    for key, pats in pattern_index.items():
        for p in pats:
            raw = str(p.get("pattern_family", "")).upper()
            fam = V03_ALIASES.get(raw, raw)
            if fam not in stats:
                continue
            stats[fam]["pattern_bars"] += 1
    for r in rows:
        pj = _payload(r)
        layers = pj.get("layers_json") or {}
        pats = layers.get("pattern_instance", {}).get("patterns") or []
        room = pj.get("room_class")
        st = r.get("state_after")
        for p in pats:
            raw = str(p.get("family", "")).upper()
            fam = V03_ALIASES.get(raw, raw)
            if fam not in stats:
                continue
            stats[fam]["considered_by_context"] += 1
            if st == "SETUP_DEVELOPING":
                stats[fam]["progressed_state"] += 1
            if r.get("selected_action") == ACTION_ENTRY_ARMED:
                stats[fam]["entry_armed"] += 1
            if room in ("LIMITED_ROOM", "AT_RESISTANCE"):
                stats[fam]["blocked_by_room"] += 1
    return {fam: dict(stats[fam]) for fam in PATTERN_FAMILIES}


def level_quality_sessions(
    dossier_rows: list[dict],
    rows: list[dict],
) -> list[dict]:
    session_ohlc: dict[tuple, dict] = defaultdict(lambda: {"high": None, "low": None, "open": None})
    for r in rows:
        sk = _session_key(r)
        pj = _payload(r)
        ohlcv = (pj.get("layers_json") or {}).get("objective_fact", {}).get("ohlcv") or {}
        h, l, o, c = ohlcv.get("high"), ohlcv.get("low"), ohlcv.get("open"), ohlcv.get("close")
        agg = session_ohlc[sk]
        if h is not None:
            agg["high"] = h if agg["high"] is None else max(agg["high"], h)
        if l is not None:
            agg["low"] = l if agg["low"] is None else min(agg["low"], l)
        if o is None and ohlcv.get("open") is not None:
            agg["open"] = ohlcv["open"]
        if agg["open"] is None and o is not None:
            agg["open"] = o
        if c is not None and agg["open"] is None:
            agg["open"] = c
    out = []
    for rec in dossier_rows:
        d = rec.get("frozen_dossier_json") or {}
        sk = (rec.get("symbol"), str(rec.get("trading_date")))
        ohlc = session_ohlc.get(sk, {})
        hi, lo, op = ohlc.get("high"), ohlc.get("low"), ohlc.get("open")
        sup = _level_value(d, "primary_support")
        res = _level_value(d, "primary_resistance")
        rec_lv = _level_value(d, "reclaim_level")
        dnc = _level_value(d, "do_not_chase_level")
        inv = _level_value(d, "invalidation_level")
        flags = []
        if hi is not None and lo is not None and sup is not None and (sup > hi * 1.02 or sup < lo * 0.98):
            flags.append("support_far_from_session_range")
        if hi is not None and rec_lv is not None and rec_lv > hi:
            flags.append("reclaim_above_session_high")
        if op is not None and dnc is not None and dnc < op:
            flags.append("do_not_chase_below_session_open")
        if sup is not None and res is not None and res < sup:
            flags.append("resistance_below_support")
        if hi is not None and inv is not None and lo is not None and inv < lo * 0.9:
            flags.append("invalidation_far_below_session")
        out.append(
            {
                "symbol": rec.get("symbol"),
                "trading_date": str(rec.get("trading_date")),
                "session_high": hi,
                "session_low": lo,
                "session_open": op,
                "support": sup,
                "resistance": res,
                "reclaim": rec_lv,
                "do_not_chase": dnc,
                "invalidation": inv,
                "quality_flags": flags,
            }
        )
    return out


def oscillation_flags(rows: list[dict]) -> dict[str, Any]:
    by_session: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by_session[_session_key(r)].append(r)
    pair_flips: Counter[tuple] = Counter()
    stuck_dne: list[dict] = []
    stuck_obs: list[dict] = []
    immediate_reversal = 0
    opening_invalidation = 0
    dnc_late: list[dict] = []
    arm_cycles: list[dict] = []
    for sk, sess_rows in by_session.items():
        sess_rows.sort(key=lambda x: x.get("bar_ts"))
        prev_st = None
        prev_act = None
        arm_count = 0
        for i, r in enumerate(sess_rows):
            st = r.get("state_after")
            act = r.get("selected_action")
            if prev_st and st != prev_st:
                pair_flips[(prev_st, st)] += 1
            if i > 0 and act != prev_act and sess_rows[i - 1].get("selected_action") != act:
                if i == 1 or True:
                    if (
                        sess_rows[i - 1].get("state_after") != st
                        and act in TRANSITION_ACTIONS
                        and sess_rows[i - 1].get("selected_action") in TRANSITION_ACTIONS
                    ):
                        immediate_reversal += 1
            if act == ACTION_ENTRY_ARMED:
                arm_count += 1
            if act == ACTION_THESIS_INVALIDATED and i < 3:
                opening_invalidation += 1
            prev_st = st
            prev_act = act
        if arm_count > 3:
            arm_cycles.append({"symbol": sk[0], "date": sk[1], "entry_armed_bars": arm_count})
        if all(r.get("selected_action") == ACTION_DO_NOT_ENTER for r in sess_rows[:5]):
            if len(sess_rows) >= 8 and all(r.get("selected_action") == ACTION_DO_NOT_ENTER for r in sess_rows):
                stuck_dne.append({"symbol": sk[0], "date": sk[1]})
        if all(r.get("state_after") == "OBSERVATION_ONLY" for r in sess_rows):
            stuck_obs.append({"symbol": sk[0], "date": sk[1]})
    return {
        "top_state_pair_transitions": [{"from": a, "to": b, "count": c} for (a, b), c in pair_flips.most_common(20)],
        "immediate_action_reversals": immediate_reversal,
        "sessions_stuck_do_not_enter_all_day": stuck_dne,
        "sessions_stuck_observation_only": stuck_obs,
        "invalidated_during_opening_window_events": opening_invalidation,
        "entry_armed_bar_counts_over_3": arm_cycles,
        "do_not_chase_late_samples": dnc_late,
    }


def build_review_pack(
    rows: list[dict],
    dossiers: dict[tuple, dict],
    *,
    run_id: str,
    nearest: list[dict],
) -> dict[str, Any]:
    dne_events = []
    inv_events = []
    dnc_events = []
    support_holds = []
    reclaim_closes = []
    reclaim_confirms = []
    bullish_no_progress = []

    by_session: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by_session[_session_key(r)].append(r)

    for sk, sess_rows in by_session.items():
        sess_rows.sort(key=lambda x: x.get("bar_ts"))
        prev = None
        doss = dossiers.get(sk)
        had_bull = False
        progressed = False
        for r in sess_rows:
            act = r.get("selected_action")
            pj = _payload(r)
            markers = pj.get("marker_flags_json") or []
            if act == ACTION_DO_NOT_ENTER and act != prev:
                dne_events.append(_review_record(r, doss, run_id=run_id, reason=classify_do_not_enter_event(r, doss)))
            if act == ACTION_THESIS_INVALIDATED and act != prev:
                inv_events.append(_review_record(r, doss, run_id=run_id))
            if act == ACTION_DO_NOT_CHASE and act != prev:
                dnc_events.append(_review_record(r, doss, run_id=run_id))
            if "support_hold" in markers:
                support_holds.append(_review_record(r, doss, run_id=run_id))
            layers = pj.get("layers_json") or {}
            reclaim = pj.get("reclaim_stage")
            if reclaim == "CLOSED_ABOVE_RECLAIM":
                reclaim_closes.append(_review_record(r, doss, run_id=run_id))
            if reclaim == "RECLAIM_CONFIRMED":
                reclaim_confirms.append(_review_record(r, doss, run_id=run_id))
            pats = layers.get("pattern_instance", {}).get("patterns") or []
            if any("DOUBLE" in str(p.get("family", "")) or "H2" in str(p.get("family", "")) for p in pats):
                had_bull = True
            if r.get("state_after") in ("SETUP_DEVELOPING", "WAITING_FOR_RECLAIM", "ENTRY_ARMED"):
                progressed = True
            prev = act
        if had_bull and not progressed:
            bullish_no_progress.append({"symbol": sk[0], "trading_date": sk[1]})

    return {
        "do_not_enter_transition_events": dne_events,
        "thesis_invalidation_transition_events": inv_events,
        "do_not_chase_transition_events": dnc_events,
        "nearest_to_entry_events": nearest,
        "support_holds": support_holds,
        "reclaim_closes": reclaim_closes,
        "reclaim_confirmations": reclaim_confirms,
        "bullish_pattern_no_setup_progression_sessions": bullish_no_progress,
    }


def compare_attempts(v01_rows: list[dict], v02_rows: list[dict]) -> dict[str, Any]:
    a1 = action_event_and_duration(v01_rows)
    a2 = action_event_and_duration(v02_rows)
    f1 = state_funnel(v01_rows)
    f2 = state_funnel(v02_rows)
    return {
        "v01_action_duration": a1["action_duration_counts"],
        "v02_action_duration": a2["action_duration_counts"],
        "v01_action_events": a1["action_transition_event_counts"],
        "v02_action_events": a2["action_transition_event_counts"],
        "v01_sessions_setup_developing": f1["sessions_entering_state"].get("SETUP_DEVELOPING", 0),
        "v02_sessions_setup_developing": f2["sessions_entering_state"].get("SETUP_DEVELOPING", 0),
        "v01_sessions_waiting_reclaim": f1["sessions_entering_state"].get("WAITING_FOR_RECLAIM", 0),
        "v02_sessions_waiting_reclaim": f2["sessions_entering_state"].get("WAITING_FOR_RECLAIM", 0),
        "v01_entry_armed_events": a1["action_transition_event_counts"].get(ACTION_ENTRY_ARMED, 0),
        "v02_entry_armed_events": a2["action_transition_event_counts"].get(ACTION_ENTRY_ARMED, 0),
        "v01_consider_entry_events": a1["action_transition_event_counts"].get(ACTION_CONSIDER_ENTRY, 0),
        "v02_consider_entry_events": a2["action_transition_event_counts"].get(ACTION_CONSIDER_ENTRY, 0),
        "do_not_enter_event_delta": a2["action_transition_event_counts"].get(ACTION_DO_NOT_ENTER, 0)
        - a1["action_transition_event_counts"].get(ACTION_DO_NOT_ENTER, 0),
        "thesis_invalidation_event_delta": a2["action_transition_event_counts"].get(ACTION_THESIS_INVALIDATED, 0)
        - a1["action_transition_event_counts"].get(ACTION_THESIS_INVALIDATED, 0),
    }
