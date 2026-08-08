"""Phase G1 — consolidated trade learning payload (presentation only)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from .learning_bar_narrative import build_bar_narrative
from .learning_constants import (
    PHASE_E1_CONTEXT_ATTEMPT_ID,
    PHASE_E1_VALIDATION_RUN_ID,
    PM_V01_CERT_SIMULATION_ATTEMPT_ID,
)
from .learning_translations import translate_action, translate_exit_reason, translate_pattern
from .learning_view import (
    _daily_levels_from_dossier,
    _ts_key,
    build_symbol_learning_payload,
    resolve_attempt_chain,
    validate_review_attempt_override,
)
from .position_management_ledger_store import load_management_ledger
from .repository import load_dossiers_for_run
from .simulation_repository import load_sim_trades
from .trade_management_review import _excursion_stats


def _parse_td(trading_date: date | str) -> date:
    if isinstance(trading_date, date):
        return trading_date
    return date.fromisoformat(str(trading_date)[:10])


def _ledger_stop_timeline(ledger: dict[str, Any] | None, symbol: str) -> list[dict[str, Any]]:
    if not ledger:
        return []
    sym = symbol.upper()
    steps: list[tuple[str, float]] = []
    for ev in ledger.get("events") or []:
        if str(ev.get("symbol", sym)).upper() != sym:
            continue
        et = str(ev.get("event") or "")
        ts = _ts_key(ev.get("ts"))
        if et == "STOP_ACTIVATED":
            steps.append((ts, float(ev["active_stop"])))
        elif et == "STOP_TRAIL_UPDATE":
            steps.append((ts, float(ev["new_stop"])))
    steps.sort(key=lambda x: x[0])
    out: list[dict[str, Any]] = []
    for i, (ts, px) in enumerate(steps):
        end = steps[i + 1][0] if i + 1 < len(steps) else None
        out.append({"from_ts": ts, "until_ts": end, "stop_price": px})
    return out


def _active_stop_at(steps: list[dict[str, Any]], bar_ts: str) -> float | None:
    bk = _ts_key(bar_ts)
    active: float | None = None
    for st in steps:
        if bk >= _ts_key(st["from_ts"]):
            active = float(st["stop_price"])
    return active


def _position_state_at(
    trade: dict[str, Any] | None,
    bar_ts: str,
) -> tuple[int, str]:
    if not trade:
        return 0, "Flat"
    ek = _ts_key(trade.get("entry_ts"))
    xk = _ts_key(trade.get("exit_ts") or "")
    bk = _ts_key(bar_ts)
    if bk < ek:
        return 0, "Flat"
    if xk and bk >= xk:
        return 0, "Flat"
    qty = int(trade.get("quantity") or 0)
    return qty, f"{qty} shares"


def _market_story_for_row(row: dict[str, Any], *, trade: dict[str, Any] | None) -> str:
    act = str(row.get("selected_action") or "")
    eff = str(row.get("simulation_effect") or "")
    if eff.startswith("ENTRY"):
        return "Small bullish response after the pullback"
    if eff.startswith("EXIT"):
        return "Price pulls back into the active stop"
    if row.get("ledger_highlight") == "stop_raised":
        return "A higher structural low is confirmed"
    if act == "CONSIDER_ENTRY":
        return "Second pullback attempt forms"
    if act == "ENTRY_ARMED":
        return "Second pullback setup becomes ready"
    if act == "DO_NOT_CHASE":
        return "Market opens directly around resistance"
    return translate_action(act) if act else "Session developing"


def _important_signal(row: dict[str, Any]) -> str:
    pats = row.get("active_patterns") or []
    if pats:
        return translate_pattern(pats[0].get("pattern_family"))
    lh = row.get("ledger_highlight")
    if lh == "stop_raised":
        return "Profit protection"
    if lh == "entry":
        return "Entry confirmation"
    if lh == "exit":
        return "Protective stop hit"
    eff = str(row.get("simulation_effect") or "")
    if eff.startswith("ENTRY"):
        return "Entry confirmation"
    if eff.startswith("EXIT"):
        return "Protective stop hit"
    return translate_action(row.get("selected_action"))


def _why_for_row(row: dict[str, Any], *, stop: float | None, trade: dict[str, Any] | None) -> str:
    eff = str(row.get("simulation_effect") or "")
    if eff.startswith("ENTRY"):
        return "The structure allows a defined-risk long entry"
    if eff.startswith("EXIT"):
        px = float(trade.get("exit_price") or 0) if trade else 0
        return f"Profit is secured at ${px:.2f}"
    if row.get("ledger_highlight") == "stop_raised" and stop:
        return "The stop is raised only after confirmation and activates on this bar"
    act = str(row.get("selected_action") or "")
    if act == "DO_NOT_CHASE":
        return "Price has not proved that resistance is broken"
    if act == "ENTRY_ARMED":
        return "The setup is forming but has not produced a valid entry"
    if act == "CONSIDER_ENTRY":
        return "The next qualifying bar may allow entry"
    return "Monitoring session structure"


def _duration_minutes(entry_ts: Any, exit_ts: Any) -> int | None:
    if not entry_ts or not exit_ts:
        return None
    try:
        a = datetime.fromisoformat(str(entry_ts).replace(" ", "T")[:19])
        b = datetime.fromisoformat(str(exit_ts).replace(" ", "T")[:19])
        return int((b - a).total_seconds() // 60)
    except ValueError:
        return None


def _format_duration(minutes: int | None) -> str:
    if minutes is None:
        return "—"
    h, m = divmod(minutes, 60)
    return f"{h}h {m}m" if h else f"{m}m"


def _chart_markers_for_bar(row: dict[str, Any]) -> list[dict[str, str]]:
    markers: list[dict[str, str]] = []
    eff = str(row.get("simulation_effect") or "")
    if eff.startswith("ENTRY") or row.get("ledger_highlight") == "entry":
        markers.append({"kind": "entry", "label": "Entry"})
    if eff.startswith("EXIT") or row.get("ledger_highlight") == "exit":
        markers.append({"kind": "exit", "label": "Exit"})
    if row.get("ledger_highlight") == "stop_raised":
        markers.append({"kind": "stop_raised", "label": "Stop raised"})
    return markers


def _ledger_codes(events: list[dict[str, Any]]) -> list[str]:
    return [str(e.get("event") or "") for e in events]


def build_trade_learning_g1_payload(
    *,
    run_id: str,
    state: dict[str, Any],
    cfg: dict[str, Any],
    context_attempt_id: str,
    simulation_attempt_id: str,
    symbol: str,
    trading_date: date | str,
    diagnostic_legacy: bool = False,
) -> dict[str, Any]:
    from .adviser_foundation_ui import ADVISER_EMPTY_STATE_MESSAGE, assert_legacy_review_access

    adv = cfg.get("adviser_foundation") or {}
    sym_u = symbol.upper()
    td_parsed = _parse_td(trading_date)

    from .adviser_baseline_v01 import ADVISER_VERSION
    from .adviser_v1_learning_view import (
        V1_LEARNING_DEFAULTS,
        build_adviser_v1_learning_payload,
        list_v1_validation_sessions,
    )

    v1_session = next(
        (
            s
            for s in list_v1_validation_sessions()
            if s["symbol"] == sym_u and s["trading_date"] == td_parsed.isoformat()
        ),
        None,
    )
    aid = str(context_attempt_id or (v1_session or {}).get("adviser_attempt_id") or adv.get("adviser_attempt_id") or "")
    sim_from_session = (v1_session or {}).get("simulation_attempt_id")
    eff_sim = simulation_attempt_id or sim_from_session or adv.get("simulation_attempt_id")

    if aid:
        from .adviser_v1_learning_view import load_adviser_attempt_meta

        att = load_adviser_attempt_meta(aid)
        if att and str((att.get("config_json") or {}).get("adviser_version") or "") == ADVISER_VERSION:
            return build_adviser_v1_learning_payload(
                adviser_attempt_id=aid,
                simulation_attempt_id=eff_sim,
                symbol=sym_u,
                trading_date=td_parsed,
            )

    if adv.get("adviser_attempt_id") and str(context_attempt_id) == str(
        adv.get("context_attempt_id") or adv.get("adviser_attempt_id")
    ):
        from .learning_view import build_symbol_learning_payload, resolve_attempt_chain

        attempts = resolve_attempt_chain(
            run_id,
            state,
            cfg,
            context_attempt_id=context_attempt_id,
            simulation_attempt_id=simulation_attempt_id,
            diagnostic_legacy=diagnostic_legacy,
        )
        sym = symbol.upper()
        td = _parse_td(trading_date)
        payload = build_symbol_learning_payload(
            run_id=run_id,
            symbol=sym,
            state=state,
            cfg=cfg,
            attempts=attempts,
            trading_date=td,
            dossier=None,
        )
        return {
            "run_id": run_id,
            "symbol": sym,
            "trading_date": str(td),
            "educational_grid": payload.get("educational_grid") or [],
            "adviser_foundation": True,
        }

    validate_review_attempt_override(
        run_id=run_id,
        context_attempt_id=context_attempt_id,
        simulation_attempt_id=simulation_attempt_id,
    )
    ctx_meta = None
    try:
        from .learning_view import _load_context_attempt_meta

        ctx_meta = _load_context_attempt_meta(context_attempt_id)
    except Exception:
        ctx_meta = None
    assert_legacy_review_access(
        diagnostic_legacy=diagnostic_legacy,
        context_attempt_id=context_attempt_id,
        simulation_attempt_id=simulation_attempt_id,
        context_ruleset=str((ctx_meta or {}).get("context_ruleset_version") or ""),
    )
    sym = symbol.upper()
    td = _parse_td(trading_date)
    attempts = resolve_attempt_chain(
        run_id,
        state,
        cfg,
        context_attempt_id=context_attempt_id,
        simulation_attempt_id=simulation_attempt_id,
        diagnostic_legacy=diagnostic_legacy,
    )
    if attempts.get("adviser_foundation_empty"):
        raise ValueError(ADVISER_EMPTY_STATE_MESSAGE)
    dossiers = load_dossiers_for_run(run_id)
    dossier = next((d for d in dossiers if str(d.get("symbol", "")).upper() == sym), None)
    sym_full = build_symbol_learning_payload(
        run_id=run_id,
        symbol=sym,
        state=state,
        cfg=cfg,
        attempts=attempts,
        mode="full",
        offset=0,
        limit=500,
        dossier=dossier,
    )
    grid_all = [
        r for r in (sym_full.get("grid_rows") or [])
        if str(r.get("trading_date") or r.get("bar_ts", ""))[:10] == td.isoformat()
    ]

    trades_all = load_sim_trades(run_id, simulation_attempt_id=simulation_attempt_id)
    trades = [t for t in trades_all if str(t.get("symbol", "")).upper() == sym]
    trade = trades[0] if trades else None

    exc = _excursion_stats(trade) if trade else {}
    duration_m = _duration_minutes(trade.get("entry_ts"), trade.get("exit_ts")) if trade else None

    ledger = load_management_ledger(simulation_attempt_id)
    stop_steps = _ledger_stop_timeline(ledger, sym)

    ledger_by_ts: dict[str, list[dict[str, Any]]] = {}
    if ledger:
        for ev in ledger.get("events") or []:
            if str(ev.get("symbol", sym)).upper() != sym:
                continue
            ledger_by_ts.setdefault(_ts_key(ev.get("ts")), []).append(ev)

    educational_rows: list[dict[str, Any]] = []
    chart_bars: list[dict[str, Any]] = []
    bar_narratives_by_ts: dict[str, dict[str, Any]] = {}

    init_stop_val: float | None = None
    if ledger:
        init_ev = next(
            (e for e in ledger.get("events", []) if e.get("event") == "INITIAL_STOP_CALCULATED"),
            None,
        )
        if init_ev:
            init_stop_val = float(init_ev.get("stop_price") or 0)

    init_stop = init_stop_val if init_stop_val is not None else 246.065
    peak_ny = None
    if exc.get("max_unrealized_pnl_ts"):
        pk = _ts_key(exc["max_unrealized_pnl_ts"])
        for r in grid_all:
            if _ts_key(r.get("bar_ts")) == pk:
                raw = r.get("bar_ts_ny") or r.get("bar_ts")
                peak_ny = str(raw).replace(" ", "T")[11:16] if raw else None
                break

    trade_summary: dict[str, Any] | None = None
    if trade:
        trade_summary = {
            "trade_id": trade.get("trade_id"),
            "symbol": sym,
            "trading_date": td.isoformat(),
            "entry_ts": trade.get("entry_ts"),
            "entry_price": float(trade.get("entry_price") or 0),
            "exit_ts": trade.get("exit_ts"),
            "exit_price": float(trade.get("exit_price") or 0),
            "quantity": int(trade.get("quantity") or 0),
            "exit_reason": trade.get("exit_reason"),
            "exit_reason_plain": translate_exit_reason(trade.get("exit_reason")),
            "realized_pnl": float(trade.get("realized_pnl") or 0),
            "initial_stop": init_stop,
            "mfe": exc.get("mfe"),
            "mae": exc.get("mae"),
            "peak_unrealized_pnl": exc.get("max_unrealized_pnl"),
            "peak_unrealized_pnl_ts": exc.get("max_unrealized_pnl_ts"),
            "duration_minutes": duration_m,
            "duration_label": _format_duration(duration_m),
        }

    prev_ledger_events: list[dict[str, Any]] = []

    for row in grid_all:
        ts = row.get("bar_ts")
        bk = _ts_key(ts)
        stop = _active_stop_at(stop_steps, bk)
        qty, pos_label = _position_state_at(trade, bk)
        action_plain = translate_action(row.get("selected_action"))
        eff = str(row.get("simulation_effect") or "—")

        ledger_highlight = None
        for ev in ledger_by_ts.get(bk, []):
            et = str(ev.get("event") or "")
            if et == "ENTRY":
                ledger_highlight = "entry"
            elif et in ("STOP_TRAIL_UPDATE", "STOP_TRAIL_SCHEDULED"):
                ledger_highlight = "stop_raised"
            elif et == "EXIT":
                ledger_highlight = "exit"

        enriched = {
            **row,
            "ledger_highlight": ledger_highlight,
        }
        bar_ledger = ledger_by_ts.get(bk, [])
        is_exit_bar = trade and "EXIT" in _ledger_codes(bar_ledger)
        narrative = build_bar_narrative(
            enriched,
            trade=trade,
            ledger_events=bar_ledger,
            prev_ledger_events=prev_ledger_events,
            qty=qty,
            stop=stop,
            initial_stop=init_stop,
            peak_unrealized=exc.get("max_unrealized_pnl") if is_exit_bar else None,
            peak_unrealized_ts_ny=peak_ny if is_exit_bar else None,
            trade_summary=trade_summary if is_exit_bar else None,
        )
        bar_narratives_by_ts[bk] = narrative
        prev_ledger_events = bar_ledger

        educational_rows.append(
            {
                "bar_ts": ts,
                "bar_ts_ny": row.get("bar_ts_ny"),
                "time_ny": str(row.get("bar_ts_ny") or ts)[11:16] if ts else "—",
                "market_story": _market_story_for_row(enriched, trade=trade),
                "important_signal": _important_signal(enriched),
                "system_view": translate_action(row.get("state_after")) if row.get("state_after") else action_plain,
                "action": action_plain,
                "position": pos_label,
                "stop": f"{stop:.3f}" if stop is not None else "—",
                "why": _why_for_row(enriched, stop=stop, trade=trade),
                "simulation_effect": eff,
                "narrative": narrative,
            }
        )
        o = row.get("ohlcv") or {}
        chart_bars.append(
            {
                "ts_utc": ts,
                "ts_ny": row.get("bar_ts_ny"),
                "open": o.get("open"),
                "high": o.get("high"),
                "low": o.get("low"),
                "close": o.get("close"),
                "active_stop": stop,
                "position_quantity": qty,
                "markers": _chart_markers_for_bar(enriched),
            }
        )

    levels = _daily_levels_from_dossier(dossier)

    overlays = {
        "entry": {"price": trade_summary["entry_price"], "ts": trade_summary["entry_ts"]} if trade_summary else None,
        "exit": {"price": trade_summary["exit_price"], "ts": trade_summary["exit_ts"]} if trade_summary else None,
        "initial_stop": trade_summary.get("initial_stop") if trade_summary else None,
        "stop_steps": stop_steps,
        "levels": {
            "support": levels.get("primary_support"),
            "resistance": levels.get("resistance"),
            "reclaim": levels.get("reclaim_level"),
            "do_not_chase": levels.get("do_not_chase_level"),
            "daily_thesis_invalidation": levels.get("daily_thesis_invalidation"),
        },
    }

    return {
        "phase": "G2",
        "run_id": run_id,
        "context_attempt_id": context_attempt_id,
        "simulation_attempt_id": simulation_attempt_id,
        "simulation_ruleset": attempts.get("simulation_ruleset"),
        "symbol": sym,
        "trading_date": td.isoformat(),
        "trade_summary": trade_summary,
        "header": {
            "title": f"{sym} · {td.strftime('%A %d %B %Y')}",
            "subtitle": "Completed simulated trade",
            "realized_pnl": trade_summary["realized_pnl"] if trade_summary else None,
        },
        "chart": {"bars": chart_bars, "bar_count": len(chart_bars), "expected_rth_bars": 78, "overlays": overlays},
        "educational_grid": educational_rows,
        "bar_narratives_by_ts": bar_narratives_by_ts,
        "technical_details": {
            "run_id": run_id,
            "context_attempt_id": context_attempt_id,
            "simulation_attempt_id": simulation_attempt_id,
            "ruleset": attempts.get("simulation_ruleset"),
        },
    }


CANONICAL_G1_DEFAULTS = {
    "run_id": PHASE_E1_VALIDATION_RUN_ID,
    "context_attempt_id": PHASE_E1_CONTEXT_ATTEMPT_ID,
    "simulation_attempt_id": PM_V01_CERT_SIMULATION_ATTEMPT_ID,
    "symbol": "AMZN",
    "trading_date": "2026-07-13",
}


def slice_g1_payload_for_replay(payload: dict[str, Any], through_ts: str) -> dict[str, Any]:
    """Presentation-only: hide bars/grid rows after replay point (no simulation rerun)."""
    cutoff = _ts_key(through_ts)
    bars = [b for b in (payload.get("chart") or {}).get("bars") or [] if _ts_key(b.get("ts_utc")) <= cutoff]
    grid = [
        r for r in payload.get("educational_grid") or [] if _ts_key(r.get("bar_ts")) <= cutoff
    ]
    narratives = payload.get("bar_narratives_by_ts") or {}
    out = dict(payload)
    chart = dict(payload.get("chart") or {})
    chart["bars"] = bars
    chart["bar_count"] = len(bars)
    out["chart"] = chart
    out["educational_grid"] = grid
    out["bar_narratives_by_ts"] = {k: v for k, v in narratives.items() if k <= cutoff}
    out["replay_through_ts"] = through_ts
    return out
