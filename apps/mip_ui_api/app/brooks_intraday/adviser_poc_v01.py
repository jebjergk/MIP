"""Brooks INTRADAY Adviser V1.0 session runner."""

from __future__ import annotations

import json
import re
import time
import uuid
from datetime import date, datetime
from typing import Any

from app.db import get_connection

from .adviser_baseline_v01 import (
    ADVISER_VERSION,
    CANONICAL_QUERY_TAG,
    RAG_CORPUS_VERSION,
    canonical_attempt_config,
    canonical_cost_metadata,
)
from .adviser_foundation_ui import FOUNDATION_CONTEXT_RULESET, FOUNDATION_SIMULATION_RULESET
from .adviser_methodology_guard_v01 import guard_premature_failed_breakout_market_state
from .adviser_rag_v01 import search_adviser_literature_balanced, short_execution_leak
from .adviser_regime_v01 import IntradayRegimeState, update_intraday_regime
from .adviser_retrieval_query_v01 import (
    build_retrieval_query,
    regime_continuation_supplement_query,
    regime_failure_companion_query,
)
from .adviser_repository import (
    complete_adviser_attempt,
    create_adviser_attempt,
    insert_adviser_call,
    insert_sim_trade,
    pin_run_adviser_attempt,
    upsert_adviser_bar,
)
from .historical_bar_repository import load_bars_from_store
from .objective_ruleset_v01 import compute_geometry
from .adviser_paa_policy_v01 import (
    assert_no_short_action,
    daily_bias_note,
    sanitize_confirmation_predicates,
    sanitize_confirmation_strings,
)
from .adviser_sim_lab_v01 import (
    LAB_STARTING_CASH,
    LabSimState,
    summarize_sim_trade_rows,
    try_close_long,
    try_open_long,
)
from .adviser_watch_v01 import WatchProcessStats, normalize_operational_watch_conditions, process_watch_predicates
from .adviser_invalidation_v01 import InvalidationProcessStats, process_invalidation_predicates
from .adviser_setup_contract_v01 import (
    SetupContract,
    build_confirmation_complete_wake_detail,
    entry_allowed_consider_entry,
    evaluate_confirmation_complete_wake,
    mark_confirmation_complete_fired,
    process_adviser_setup_response,
    tick_mandatory_confirmations,
)
from .adviser_wake_v01 import (
    WAKE_CONFIRMATION_COMPLETE,
    WAKE_INVALIDATED,
    WAKE_POSITION,
    WAKE_SAFETY,
    WAKE_SESSION_OPEN,
    WAKE_WATCH,
    ThesisWakeState,
    evaluate_wake,
    legacy_material_change,
    parse_predicates_from_llm,
)
from .repository import get_dossier

POC_LLM_MODEL = "claude-haiku-4-5"
CORPUS_VERSION = RAG_CORPUS_VERSION
VALID_ACTIONS = frozenset(
    {
        "OBSERVE",
        "WAIT",
        "WATCH_LONG",
        "ARM_LONG",
        "CONSIDER_ENTRY",
        "HOLD",
        "EXIT",
        "INVALIDATE",
    }
)


def _uuid() -> str:
    return str(uuid.uuid4())


def _et_hm(ts_ny: datetime | None) -> str:
    if ts_ny is None:
        return ""
    return ts_ny.strftime("%H:%M")


def _baseline_credits(query_tag: str) -> float:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(SUM(credits_used_cloud_services), 0)
            FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
                END_TIME_RANGE_START => DATEADD(minute, -30, CURRENT_TIMESTAMP()),
                RESULT_LIMIT => 5000
            ))
            WHERE query_tag = %s
            """,
            (query_tag,),
        )
        row = cur.fetchone()
        return float(row[0] or 0) if row else 0.0
    except Exception:
        return 0.0
    finally:
        conn.close()


def _build_packet(
    bar: Any,
    *,
    recent: list[Any],
    dossier: dict | None,
    position_qty: int,
    prev_thesis: str | None,
    wake_reason: str,
    change_note: str,
    intraday_regime: dict[str, Any] | None = None,
) -> dict[str, Any]:
    g = compute_geometry(open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume)
    swings = {"session_high": max(b.high for b in recent), "session_low": min(b.low for b in recent)}
    d = (dossier or {}).get("frozen_dossier_json") or dossier or {}
    return {
        "time_et": _et_hm(bar.ts_ny),
        "bar_ts_ny": str(bar.ts_ny),
        "ohlc": {"o": bar.open, "h": bar.high, "l": bar.low, "c": bar.close},
        "bar_character": {
            "direction": g.direction,
            "body_fraction": g.body_fraction,
            "close_location": g.close_location,
            "range": g.total_range,
        },
        "recent_swings": swings,
        "overlap_state": "tight" if len(recent) >= 3 and (swings["session_high"] - swings["session_low"]) < 2.0 else "normal",
        "daily_paa_verdict": d.get("paa_verdict"),
        "daily_intraday_context": daily_bias_note(d.get("paa_verdict")),
        "major_levels": d.get("support_zones") or d.get("resistance_zones"),
        "position_state": "IN_TRADE" if position_qty > 0 else "FLAT",
        "position_qty": position_qty,
        "previous_thesis": prev_thesis,
        "what_changed": change_note,
        "wake_reason": wake_reason,
        "intraday_regime": intraday_regime or {},
    }


def _parse_llm_json(text: str) -> dict[str, Any]:
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return {}


def _call_llm(prompt: str, *, query_tag: str) -> tuple[str, int]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER SESSION SET QUERY_TAG = '{query_tag}'")
        t0 = time.perf_counter()
        cur.execute(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
            (POC_LLM_MODEL, prompt[:12000]),
        )
        raw = cur.fetchone()[0]
        ms = int((time.perf_counter() - t0) * 1000)
        if isinstance(raw, dict):
            choices = raw.get("choices") or []
            if choices:
                msg = choices[0].get("messages") or choices[0].get("message") or ""
                if isinstance(msg, dict):
                    return str(msg.get("content", "")), ms
                return str(msg), ms
        return str(raw), ms
    finally:
        conn.close()


def run_adviser_session(
    *,
    run_id: str,
    symbol: str = "AMZN",
    trading_date: date | None = None,
) -> dict[str, Any]:
    """Canonical Adviser replay for one symbol/session (thesis wake + boolean confirmation + lab sim)."""
    trading_date = trading_date or date(2026, 7, 13)
    query_tag = CANONICAL_QUERY_TAG
    bars = [b for b in load_bars_from_store(symbol, trading_date) if b.rth]
    if not bars:
        raise ValueError(f"No RTH bars for {symbol} {trading_date}")

    baseline_cloud = _baseline_credits(query_tag)
    aid = create_adviser_attempt(
        run_id=run_id,
        symbol=symbol,
        trading_date=trading_date,
        corpus_version=CORPUS_VERSION,
        query_tag=query_tag,
        config=canonical_attempt_config(symbol=symbol, trading_date=str(trading_date)),
    )
    sim_id = _uuid()
    dossier = get_dossier(run_id, symbol, trading_date)

    thesis = ""
    watch: list[str] = []
    watch_predicates: list[dict[str, Any]] = []
    inv_predicates: list[dict[str, Any]] = []
    action = "OBSERVE"
    position_qty = 0
    position_avg: float | None = None
    stop_price: float | None = None
    open_trade_id: str | None = None
    call_num = 0
    wake_events = 0
    rag_calls = 0
    llm_calls = 0
    cards_retrieved: list[int] = []
    latencies: list[int] = []
    timeline: list[dict[str, Any]] = []
    prev_geom: dict | None = None
    wake_state = ThesisWakeState()
    stats = {
        "safety_refreshes": 0,
        "watch_matches": 0,
        "invalidations": 0,
        "position_events": 0,
        "legacy_suppressed": 0,
        "arm_long": 0,
        "consider_entry": 0,
        "confirmation_complete": 0,
    }
    inv_proc_stats = InvalidationProcessStats()
    watch_proc_stats = WatchProcessStats()
    setup_contract: SetupContract | None = None
    setup_audit_events: list[dict[str, Any]] = []
    sim_trade_rows: list[dict[str, Any]] = []
    lab = LabSimState()
    starting_cash = LAB_STARTING_CASH
    regime_state = IntradayRegimeState()

    def _sync_position_from_lab() -> None:
        nonlocal position_qty, position_avg, stop_price, open_trade_id
        position_qty = lab.position_qty
        position_avg = lab.position_avg
        stop_price = lab.stop_price
        open_trade_id = lab.open_trade_id

    def _record_sim_row(row: dict[str, Any]) -> None:
        sim_trade_rows.append(dict(row))

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.BROOKS_INTRADAY_SIMULATION_ATTEMPT (
                SIMULATION_ATTEMPT_ID, RUN_ID, CONTEXT_ATTEMPT_ID, CONTEXT_RULESET_VERSION,
                SIMULATION_RULESET_VERSION, STATUS, STARTING_CASH, NOTES
            ) VALUES (%s, %s, %s, %s, %s, 'RUNNING', %s, %s)
            """,
            (
                sim_id,
                run_id,
                aid,
                FOUNDATION_CONTEXT_RULESET,
                FOUNDATION_SIMULATION_RULESET,
                starting_cash,
                "Adviser V1.0 canonical session",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    for i, bar in enumerate(bars):
        recent = bars[max(0, i - 19) : i + 1]
        g = compute_geometry(open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume)
        update_intraday_regime(
            regime_state,
            bar=bar,
            g=g,
            recent=recent,
            bar_index=i,
        )
        legacy_change = legacy_material_change(prev_geom, bar, g)

        wake_reason = None
        wake_detail = ""
        decision = None
        entry_exec: dict[str, Any] = {"allowed": False, "reason": "no_adviser_call"}
        qty_before_bar = position_qty
        if setup_contract is not None and setup_contract.armed and i > 0:
            tick_mandatory_confirmations(
                setup_contract,
                bar_index=i,
                bar=bar,
                recent=recent,
                prev_bar=bars[i - 1],
                prev_geom=prev_geom,
                g=g,
            )
        if i == 0:
            wake_reason = WAKE_SESSION_OPEN
            wake_detail = "session open"
            wake_state.bars_since_meaningful_wake = 0
        else:
            if setup_contract is not None and setup_contract.armed:
                if evaluate_confirmation_complete_wake(
                    setup_contract,
                    bar_index=i,
                    bar=bar,
                    recent=recent,
                    prev_bar=bars[i - 1],
                    prev_geom=prev_geom,
                    g=g,
                    last_action=action,
                ):
                    wake_reason = WAKE_CONFIRMATION_COMPLETE
                    wake_detail = build_confirmation_complete_wake_detail(setup_contract)
                    mark_confirmation_complete_fired(setup_contract)
            if wake_reason is None:
                decision = evaluate_wake(
                    wake_state,
                    bar_index=i,
                    bar=bar,
                    g=g,
                    prev_geom=prev_geom,
                    recent=recent,
                    prev_bar=bars[i - 1] if i > 0 else None,
                )
                if decision:
                    wake_reason = decision.reason
                    wake_detail = decision.detail
                elif legacy_change:
                    stats["legacy_suppressed"] += 1

        prev_geom = {"direction": g.direction, "range": g.total_range, "high": bar.high, "low": bar.low}

        if wake_reason:
            if wake_reason == WAKE_SAFETY:
                stats["safety_refreshes"] += 1
            elif wake_reason == WAKE_CONFIRMATION_COMPLETE:
                stats["confirmation_complete"] += 1
            elif wake_reason == WAKE_WATCH:
                stats["watch_matches"] += 1
            elif wake_reason == WAKE_INVALIDATED:
                stats["invalidations"] += 1
            elif wake_reason == WAKE_POSITION:
                stats["position_events"] += 1
            wake_events += 1
            call_num += 1
            pos_state = "IN_TRADE" if position_qty > 0 else "FLAT"
            setup_id = setup_contract.setup_id if setup_contract else None
            retrieval_query = build_retrieval_query(
                wake_reason=wake_reason,
                bar_direction=g.direction,
                bar_close=float(bar.close),
                position_state=pos_state,
                intraday_regime=regime_state.regime,
                wake_detail=wake_detail or "",
                setup_id=setup_id,
                thesis_context=thesis or None,
            )
            supplement = regime_continuation_supplement_query(regime_state.regime)
            if wake_reason == WAKE_INVALIDATED and regime_state.regime:
                companion = regime_failure_companion_query(regime_state.regime)
                supplement = " ".join(
                    x for x in (supplement, companion) if x
                ) or companion
            t0 = time.perf_counter()
            rag_hits = search_adviser_literature_balanced(
                retrieval_query,
                supplement,
                limit=6,
                position_state=pos_state,
                query_tag=query_tag,
            )
            rag_ms = int((time.perf_counter() - t0) * 1000)
            rag_calls += 1
            cards_retrieved.append(len(rag_hits))
            if any(short_execution_leak(h) for h in rag_hits):
                rag_hits = [h for h in rag_hits if not short_execution_leak(h)]

            invalidation_reassess = ""
            if wake_reason == WAKE_INVALIDATED:
                invalidation_reassess = """
THESIS_INVALIDATED wake: what_changed states an objective edge only (e.g. close below a reference level).
Do NOT label brooks_market_state as FAILED_BULL_BREAKOUT, BULL_TRAP, or similar solely from that level break.
Reassess with Brooks context: trend strength, always-in direction, pullback depth, signal/follow-through quality,
whether the pullback low actually failed, credible bear reversal vs normal pullback / breakout test / trend resumption.
EXIT or HOLD remain valid when follow-through and structure support them; strong bull regime is not a veto on exits.
"""
            packet = _build_packet(
                bar,
                recent=recent,
                dossier=dossier,
                position_qty=position_qty,
                prev_thesis=thesis,
                wake_reason=wake_reason,
                change_note=wake_detail or legacy_change or "scheduled wake",
                intraday_regime=regime_state.to_dict(),
            )
            card_text = "\n\n".join(
                f"- {h.get('CONCEPT_NAME')} ({h.get('EXECUTION_RELEVANCE')}): "
                f"{(h.get('DISPLAY_TEXT') or '')[:400]}"
                for h in rag_hits
            )
            inv_rules = """
invalidation_predicates (max 2): future edge-triggered transitions only — not states already true.
Types: BREAK_BELOW_LEVEL, BREAK_ABOVE_LEVEL (and BEAR_FOLLOW_THROUGH if needed).
Do NOT list multiple nearby support levels for the same failure idea.
If price is already below a level, do NOT use that level; define what would invalidate from NOW onward.
Each predicate must have predicate_is_future_actionable meaning at current close.
Example: {{"type":"BREAK_BELOW_LEVEL","level":246.00,"description":"pullback support fails"}}
"""
            watch_rules = """
watch_predicates (max 3): future edge-triggered Brooks transitions only — not states already true.
Prefer types: SECOND_ENTRY_CONFIRMED, PULLBACK_LEG_COMPLETED, FAILED_BREAKOUT_CONFIRMED,
SIGNAL_BAR_CONFIRMED, BULL_FOLLOW_THROUGH_AFTER_SIGNAL, BREAKOUT_PULLBACK_HOLD_CONFIRMED.
Each must be future-actionable at current close (predicate_is_future_actionable).
"""
            action_semantics = """
Action semantics (strict, same bar only):
- WATCH_LONG: interesting setup; continue observing.
- ARM_LONG: valid setup but one or more explicit confirmations still pending on this bar.
- CONSIDER_ENTRY: all confirmations required for entry are satisfied on THIS bar only.
Do NOT use CONSIDER_ENTRY if confirmation_conditions or your reasoning mention next bar or pending confirmation.
"""
            setup_lifecycle_rules = """
confirmation_predicates (required when action is ARM_LONG): structured objects only — execution uses these, not prose.
confirmation_logic (required when action is ARM_LONG): executable boolean tree — ALL_OF, ANY_OF, nested groups, and PREDICATE leaves.
Do NOT infer boolean logic from confirmation_conditions at runtime; express it explicitly in confirmation_logic.
PREDICATE leaf example: {"type":"PREDICATE","predicate_type":"BULL_FOLLOW_THROUGH_AFTER_SIGNAL","level":246.68,
"valid_from_bar_offset":1,"valid_until_bar_offset":2}
ANY_OF example (alternative Brooks paths): {"type":"ANY_OF","children":[{...PREDICATE...},{...PREDICATE...}]}
ALL_OF example (cumulative requirements): {"type":"ALL_OF","children":[{...PREDICATE...},{...PREDICATE...}]}
Nested example: {"type":"ALL_OF","children":[{"type":"PREDICATE","predicate_type":"SIGNAL_BAR_CONFIRMED","level":246.68},
{"type":"ANY_OF","children":[{...BULL_FOLLOW_THROUGH...},{...FAILED_BEAR...}]}]}
Supported predicate types include LEVEL_BREAK, BULL_FOLLOW_THROUGH_AFTER_SIGNAL, BREAKOUT_PULLBACK_HOLD_CONFIRMED,
FAILED_BEAR_BREAKOUT_CONFIRMED, SIGNAL_BAR_CONFIRMED, PULLBACK_LEG_COMPLETED.
Encode timing with valid_from_bar_offset and valid_until_bar_offset (bar indices relative to setup signal bar).
confirmation_conditions strings are human-readable explanation only.
Optional JSON key confirmation_extension_reason — required only if you add NEW mandatory confirmations
after a CONFIRMATION_COMPLETE wake while staying ARM_LONG.
"""
            confirmation_wake_note = ""
            if wake_reason == WAKE_CONFIRMATION_COMPLETE and setup_contract is not None:
                confirmation_wake_note = f"""
The mandatory confirmation conditions for setup {setup_contract.setup_id} are now satisfied (frozen contract).
Reassess this setup using current market information. Choose CONSIDER_ENTRY, WATCH_LONG, or INVALIDATE.
"""
            prompt = f"""You are Brooks INTRADAY Adviser (LONG_ONLY). No short actions.
Return JSON only with keys:
action, brooks_market_state, current_thesis,
watch_conditions (array of plain-language strings for Learning View),
watch_predicates (array of machine-readable objects — required),
confirmation_conditions (array of intraday-only plain strings — NO PAA/CLEAR_LONG/daily verdict changes),
confirmation_predicates (array of intraday-only objects — must match PREDICATE leaves in confirmation_logic),
confirmation_logic (object — executable boolean tree when action is ARM_LONG),
invalidation_conditions (array of plain strings),
invalidation_predicates (array of objects, max 2 active),
daily_intraday_disagreement (string or null),
confirmation_extension_reason (string or null),
brooks_reasoning_summary.

Daily PAA is frozen for the session: use it only for bias, major levels, and risk context.
Do NOT require daily PAA to become CLEAR_LONG, WAIT_PULLBACK, or any daily verdict change intraday.
If daily is cautious (e.g. NO_CLEAR_LONG), say so in daily_intraday_disagreement and list
intraday evidence needed (signal bar, follow-through, failed bear breakout, etc.).
{inv_rules}{watch_rules}{action_semantics}{setup_lifecycle_rules}{confirmation_wake_note}{invalidation_reassess}
Persist intraday_regime in reasoning; it is Brooks context, not a hard trading signal.
watch_predicates types (legacy ok): LEVEL_BREAK, PULLBACK_LEG_COMPLETED, BULL_FOLLOW_THROUGH,
BAR_DIRECTION_FLIP, RANGE_EXPANSION, SWING_BREAK (only when explicitly watching that event).
Example watch: {{"type":"LEVEL_BREAK","level":248.27,"direction":"ABOVE"}}

Valid actions: {sorted(VALID_ACTIONS)}.

Observation:
{json.dumps(packet, indent=2)}

Literature cards:
{card_text}
"""
            thesis_before = thesis
            prior_action = action
            llm_text, llm_ms = _call_llm(prompt, query_tag=query_tag)
            llm_calls += 1
            parsed = _parse_llm_json(llm_text)
            action = assert_no_short_action(
                str(parsed.get("action") or "OBSERVE"),
                str(parsed.get("brooks_reasoning_summary") or ""),
            )
            thesis = str(parsed.get("current_thesis") or thesis)
            watch_p, inv_raw, watch = parse_predicates_from_llm(
                parsed,
                infer_invalidation=False,
            )
            inv_predicates, inv_batch = process_invalidation_predicates(inv_raw, bar)
            inv_proc_stats.merge(inv_batch)
            daily_ctx_inv = {
                "generated": inv_batch.generated,
                "stored": inv_batch.stored,
                "rejected_already_true": inv_batch.rejected_already_true,
            }
            watch_predicates, watch_batch = process_watch_predicates(watch_p, bar)
            watch, watch_context_notes = normalize_operational_watch_conditions(
                [str(x) for x in (parsed.get("watch_conditions") or [])],
                watch_predicates,
            )
            watch_proc_stats.generated += watch_batch.generated
            watch_proc_stats.stored += watch_batch.stored
            watch_proc_stats.rejected_already_true += watch_batch.rejected_already_true
            watch_proc_stats.rejected_cap += watch_batch.rejected_cap
            conf_text, removed_conf = sanitize_confirmation_strings(
                list(parsed.get("confirmation_conditions") or [])
            )
            conf_p, removed_conf_p = sanitize_confirmation_predicates(
                list(parsed.get("confirmation_predicates") or [])
            )
            conf_logic_raw = parsed.get("confirmation_logic")
            if isinstance(conf_logic_raw, str):
                try:
                    conf_logic_raw = json.loads(conf_logic_raw)
                except json.JSONDecodeError:
                    conf_logic_raw = None
            if conf_logic_raw is not None and not isinstance(conf_logic_raw, dict):
                conf_logic_raw = None
            market_state = str(parsed.get("brooks_market_state") or "")
            market_state, guard_note = guard_premature_failed_breakout_market_state(
                wake_reason=wake_reason,
                wake_detail=wake_detail or "",
                market_state=market_state,
            )
            summary = str(parsed.get("brooks_reasoning_summary") or "")[:2000]
            if guard_note:
                summary = f"{guard_note} {summary}"[:2000]
                daily_ctx_prep = {"methodology_guard": guard_note}
            else:
                daily_ctx_prep = {}
            if watch_context_notes:
                daily_ctx_prep = {**daily_ctx_prep, "watch_context_notes": watch_context_notes}
            thesis_invalidated = wake_reason == WAKE_INVALIDATED or action == "INVALIDATE"
            setup_events: list[dict[str, Any]] = []
            wake_state.replace_predicates_from_adviser(
                watch_predicates=watch_predicates,
                invalidation_predicates=inv_predicates,
                watch_text=watch,
                prior_action=prior_action,
                new_action=action,
                thesis_invalidated=thesis_invalidated,
                watch_issued_at_bar=i,
            )
            setup_contract, setup_events = process_adviser_setup_response(
                setup_contract,
                prior_action=prior_action,
                new_action=action,
                wake_reason=wake_reason,
                mandatory_preds_from_llm=conf_p,
                confirmation_logic_raw=conf_logic_raw if isinstance(conf_logic_raw, dict) else None,
                optional_text=conf_text,
                confirmation_extension_reason=str(parsed.get("confirmation_extension_reason") or ""),
                signal_bar_index=i,
                signal_bar_et=_et_hm(bar.ts_ny),
                thesis_invalidated=thesis_invalidated,
                position_changed=position_qty != qty_before_bar,
                position_qty_before=qty_before_bar,
                position_qty_after=position_qty,
                reasoning_summary=summary,
                current_thesis=thesis,
            )
            setup_audit_events.extend(setup_events)
            allowed, entry_reason = entry_allowed_consider_entry(
                action,
                setup_contract,
                bar=bar,
                bar_index=i,
                recent=recent,
                prev_bar=bars[i - 1] if i > 0 else None,
                prev_geom=prev_geom,
                g=g,
            )
            entry_exec = {"allowed": allowed, "reason": entry_reason}
            packet = dict(packet)
            packet["entry_execution"] = entry_exec
            if setup_contract is not None:
                packet["setup_contract"] = setup_contract.to_dict()
            d = (dossier or {}).get("frozen_dossier_json") or {}
            daily_ctx = daily_bias_note(d.get("paa_verdict"))
            if parsed.get("daily_intraday_disagreement"):
                daily_ctx["daily_intraday_disagreement"] = str(parsed.get("daily_intraday_disagreement"))
            daily_ctx["removed_frozen_confirmations"] = removed_conf + [json.dumps(x) for x in removed_conf_p]
            if daily_ctx_inv:
                daily_ctx["invalidation_predicate_stats"] = daily_ctx_inv
            daily_ctx["watch_predicate_stats"] = {
                "generated": watch_proc_stats.generated,
                "stored": watch_proc_stats.stored,
                "rejected_already_true": watch_proc_stats.rejected_already_true,
                "rejected_cap": watch_proc_stats.rejected_cap,
            }
            if setup_contract is not None:
                daily_ctx["setup_contract"] = setup_contract.to_dict()
            if setup_events:
                daily_ctx["setup_audit"] = setup_events
            daily_ctx["confirmation_logic_emitted"] = bool(
                isinstance(conf_logic_raw, dict) and conf_logic_raw.get("type")
            )
            daily_ctx["intraday_regime"] = regime_state.to_dict()
            daily_ctx.update(daily_ctx_prep)
            if action == "ARM_LONG":
                stats["arm_long"] += 1
            if action == "CONSIDER_ENTRY":
                stats["consider_entry"] += 1
            total_ms = rag_ms + llm_ms
            latencies.append(total_ms)

            cid = _uuid()
            inv_text = [str(x) for x in (parsed.get("invalidation_conditions") or [])]
            insert_adviser_call(
                {
                    "call_id": cid,
                    "adviser_attempt_id": aid,
                    "call_number": call_num,
                    "bar_ts_ny": bar.ts_ny,
                    "bar_ts_et": _et_hm(bar.ts_ny),
                    "wake_reason": wake_reason,
                    "position_state": pos_state,
                    "retrieval_query": retrieval_query,
                    "rag_card_ids": [h.get("CARD_ID") for h in rag_hits],
                    "retrieved_concepts": [h.get("CONCEPT_NAME") for h in rag_hits],
                    "current_thesis": thesis,
                    "brooks_market_state": market_state,
                    "action": action,
                    "watch_conditions": watch,
                    "confirmation_conditions": conf_text,
                    "invalidation_conditions": inv_text,
                    "watch_predicates": watch_predicates,
                    "invalidation_predicates": inv_predicates,
                    "confirmation_predicates": conf_p,
                    "daily_intraday_context": daily_ctx,
                    "brooks_reasoning_summary": summary,
                    "observation_packet": packet,
                    "llm_model": POC_LLM_MODEL,
                    "llm_input_tokens": None,
                    "llm_output_tokens": None,
                    "rag_latency_ms": rag_ms,
                    "llm_latency_ms": llm_ms,
                    "total_latency_ms": total_ms,
                }
            )
            timeline.append(
                {
                    "time_et": _et_hm(bar.ts_ny),
                    "wake_reason": wake_reason,
                    "wake_detail": wake_detail,
                    "market_state": market_state,
                    "retrieved": [h.get("CONCEPT_NAME") for h in rag_hits],
                    "thesis_before": thesis_before,
                    "conclusion": summary,
                    "action": action,
                    "watch_next": watch,
                    "watch_predicates": watch_predicates,
                }
            )
            bar_note = f"Brooks called: {wake_reason}"
            called = True
        else:
            cid = None
            bar_note = "Thesis quiet — continuing current Brooks thesis."
            called = False
            wake_state.bars_since_meaningful_wake += 1

        qty_before = position_qty
        if called and action == "CONSIDER_ENTRY":
            entry_px = float(bar.close)
            stop = float(min(b.low for b in recent[-5:]))
            block = None
            if not entry_exec.get("allowed"):
                block = str(entry_exec.get("reason") or "entry_blocked")
            if try_open_long(
                lab,
                entry_price=entry_px,
                stop_price=stop,
                bar_ts=bar.ts_ny,
                block_reason=block,
            ):
                row = {
                    "trade_id": lab.open_trade_id,
                    "adviser_attempt_id": aid,
                    "symbol": symbol,
                    "entry_ts_ny": bar.ts_ny,
                    "entry_price": entry_px,
                    "quantity": lab.position_qty,
                    "stop_price": stop,
                }
                insert_sim_trade(row)
                _record_sim_row(row)
            _sync_position_from_lab()
        elif lab.position_qty > 0 and called and action in ("EXIT", "INVALIDATE"):
            exit_px = float(bar.close)
            entry_avg = lab.position_avg
            tid = lab.open_trade_id or open_trade_id
            closed = try_close_long(lab, exit_price=exit_px, reason=action)
            if closed:
                pnl, qty = closed
                row = {
                    "trade_id": tid or _uuid(),
                    "adviser_attempt_id": aid,
                    "symbol": symbol,
                    "entry_ts_ny": bar.ts_ny,
                    "exit_ts_ny": bar.ts_ny,
                    "entry_price": entry_avg,
                    "exit_price": exit_px,
                    "quantity": qty,
                    "stop_price": stop_price,
                    "exit_reason": action,
                    "realized_pnl": pnl,
                }
                insert_sim_trade(row)
                _record_sim_row(row)
            lab.open_trade_id = None
            _sync_position_from_lab()
        elif lab.position_qty > 0 and lab.stop_price and bar.low <= lab.stop_price:
            exit_px = float(lab.stop_price)
            entry_avg = lab.position_avg
            tid = open_trade_id or lab.open_trade_id
            closed = try_close_long(lab, exit_price=exit_px, reason="STOP")
            if closed:
                pnl, qty = closed
                row = {
                    "trade_id": tid or _uuid(),
                    "adviser_attempt_id": aid,
                    "symbol": symbol,
                    "entry_ts_ny": bar.ts_ny,
                    "exit_ts_ny": bar.ts_ny,
                    "entry_price": entry_avg,
                    "exit_price": exit_px,
                    "quantity": qty,
                    "stop_price": lab.stop_price,
                    "exit_reason": "STOP",
                    "realized_pnl": pnl,
                }
                insert_sim_trade(row)
                _record_sim_row(row)
            lab.open_trade_id = None
            _sync_position_from_lab()

        if position_qty != qty_before:
            wake_state.pending_position_event = "ENTER" if position_qty > qty_before else "EXIT"

        upsert_adviser_bar(
            {
                "adviser_attempt_id": aid,
                "bar_ts_ny": bar.ts_ny,
                "bar_ts_et": _et_hm(bar.ts_ny),
                "adviser_called": called,
                "call_id": cid,
                "thesis_snapshot": thesis,
                "action_snapshot": action,
                "watch_snapshot": "; ".join(watch[:3]) if isinstance(watch, list) else str(watch),
                "bar_note": bar_note,
                "sim_position_qty": position_qty,
                "sim_position_avg": position_avg,
            }
        )

    sim_metrics = summarize_sim_trade_rows(sim_trade_rows)
    after_cloud = _baseline_credits(query_tag)
    logic_repairs = sum(1 for e in setup_audit_events if e.get("type") == "CONFIRMATION_LOGIC_REPAIRED")
    logic_rejections = sum(1 for e in setup_audit_events if e.get("type") == "CONFIRMATION_LOGIC_REJECTED")
    logic_inferred = sum(1 for e in setup_audit_events if e.get("type") == "CONFIRMATION_LOGIC_INFERRED")
    cost = {
        "query_tag": query_tag,
        **canonical_cost_metadata(),
        "baseline_cloud_credits_30m": baseline_cloud,
        "after_cloud_credits_30m": after_cloud,
        "delta_cloud_credits_approx": max(0.0, after_cloud - baseline_cloud),
        "rag_calls": rag_calls,
        "llm_calls": llm_calls,
        "wake_events": wake_events,
        "adviser_calls": call_num,
        **stats,
        "invalidation_predicates_generated": inv_proc_stats.generated,
        "invalidation_rejected_already_true": inv_proc_stats.rejected_already_true,
        "invalidation_rejected_dedupe_or_cap": inv_proc_stats.rejected_dedupe + inv_proc_stats.rejected_cap,
        "invalidation_suppressed_already_fired": wake_state.invalidation_blocked_already_fired,
        "watch_predicates_generated": watch_proc_stats.generated,
        "watch_rejected_already_true": watch_proc_stats.rejected_already_true,
        "watch_rejected_cap": watch_proc_stats.rejected_cap,
        "watch_suppressed_already_fired": wake_state.watch_blocked_already_fired,
        "sim_trades": sim_metrics,
        "sim_trade_row_count": len(sim_trade_rows),
        "confirmation_complete_wakes": stats.get("confirmation_complete", 0),
        "boolean_logic_repairs": logic_repairs,
        "boolean_logic_rejections": logic_rejections,
        "boolean_logic_inferred": logic_inferred,
        "setup_audit_events": setup_audit_events,
    }
    complete_adviser_attempt(aid, cost_summary=cost)
    pin_run_adviser_attempt(run_id, aid, sim_id)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.BROOKS_INTRADAY_SIMULATION_ATTEMPT
            SET STATUS = 'COMPLETED', COMPLETED_AT_UTC = CURRENT_TIMESTAMP(),
                TRADE_COUNT = %s
            WHERE SIMULATION_ATTEMPT_ID = %s
            """,
            (sim_metrics["completed_round_trips"], sim_id),
        )
        conn.commit()
    finally:
        conn.close()

    return {
        "adviser_attempt_id": aid,
        "simulation_attempt_id": sim_id,
        "run_id": run_id,
        "rth_bars": len(bars),
        "wake_events": wake_events,
        "adviser_calls": call_num,
        "rag_calls": rag_calls,
        "llm_calls": llm_calls,
        "avg_cards": sum(cards_retrieved) / len(cards_retrieved) if cards_retrieved else 0,
        "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0,
        "max_latency_ms": max(latencies) if latencies else 0,
        "cost": cost,
        "wake_stats": stats,
        "sim_trades": sim_metrics,
        "timeline": timeline,
    }
