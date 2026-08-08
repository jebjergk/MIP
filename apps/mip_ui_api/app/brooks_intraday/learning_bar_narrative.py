"""Phase G2 — deterministic per-bar learning narratives (presentation only)."""

from __future__ import annotations

from typing import Any

from .learning_translations import (
    LEDGER_EVENT_LABELS,
    translate_action,
    translate_exit_reason,
    translate_ledger_event,
    translate_pattern,
    translate_term,
)
from .brooks_timestamp import ny_hm
from .learning_view import _ts_key


def _ny_hm(bar_ts_ny: Any, bar_ts: Any) -> str:
    return ny_hm(bar_ts_ny=bar_ts_ny, bar_ts_utc=bar_ts)


def _collect_evidence(row: dict[str, Any]) -> list[dict[str, str]]:
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for t in row.get("objective_terms") or []:
        code = str(t.get("term") if isinstance(t, dict) else t or "").upper()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append({"code": code, "plain": translate_term(code)})
    for p in row.get("active_patterns") or []:
        fam = str(p.get("pattern_family") or "").upper()
        if not fam or fam in seen:
            continue
        seen.add(fam)
        lc = str(p.get("lifecycle") or p.get("lifecycle_status") or "").upper()
        plain = translate_pattern(fam)
        if lc == "DEVELOPING" and "possible" not in plain.lower():
            plain = f"Possible {plain[0].lower()}{plain[1:]}" if plain else plain
        elif lc == "CONFIRMED":
            plain = plain.replace("may be", "was").replace("Possible ", "")
        out.append({"code": fam, "plain": plain, "lifecycle": lc})
    return out


def _ledger_codes(events: list[dict[str, Any]]) -> list[str]:
    return [str(e.get("event") or "") for e in events]


def _unrealized_pnl(trade: dict[str, Any] | None, qty: int, close: Any) -> float | None:
    if not trade or qty <= 0 or close is None:
        return None
    try:
        entry = float(trade.get("entry_price") or 0)
        return round((float(close) - entry) * qty, 2)
    except (TypeError, ValueError):
        return None


def _position_snapshot(
    *,
    trade: dict[str, Any] | None,
    bar_ts: str,
    qty: int,
    stop: float | None,
    ohlcv: dict[str, Any],
    ledger_events: list[dict[str, Any]],
    initial_stop: float | None,
) -> dict[str, Any]:
    bk = _ts_key(bar_ts)
    side = "long" if qty > 0 else "flat"
    entry_price = None
    if trade and qty > 0:
        entry_price = float(trade.get("entry_price") or 0)
    elif trade and any(e.get("event") == "ENTRY" for e in ledger_events):
        entry_price = float(trade.get("entry_price") or 0)
        side = "long"
        qty = int(trade.get("quantity") or 0)

    display_stop = stop
    if display_stop is None and initial_stop is not None and side == "long":
        display_stop = initial_stop

    unrealized = _unrealized_pnl(trade, qty if side == "long" else 0, ohlcv.get("close"))
    realized = None
    if any(e.get("event") == "EXIT" for e in ledger_events):
        realized = float(trade.get("realized_pnl") or 0) if trade else None
        side = "flat"
        qty = 0
        unrealized = None

    return {
        "side": side,
        "quantity": qty,
        "entry_price": entry_price,
        "active_stop": display_stop,
        "unrealized_pnl": unrealized,
        "realized_pnl": realized,
    }


def _technical_block(row: dict[str, Any], ledger_events: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "objective_terms": row.get("objective_terms") or [],
        "objective_summary": row.get("objective_summary"),
        "active_patterns": row.get("active_patterns") or [],
        "state_before": row.get("state_before"),
        "state_after": row.get("state_after"),
        "selected_action": row.get("selected_action"),
        "blockers": row.get("blockers") or [],
        "simulation_effect": row.get("simulation_effect"),
        "ledger_events": ledger_events,
        "explanation_sections": row.get("explanation_sections"),
        "symbol": row.get("symbol"),
    }


def _entry_narrative(
    row: dict[str, Any],
    *,
    trade: dict[str, Any],
    initial_stop: float,
    evidence: list[dict[str, str]],
) -> dict[str, Any]:
    qty = int(trade.get("quantity") or 0)
    entry_px = float(trade.get("entry_price") or 0)
    sym = str(trade.get("symbol") or "SHARES").upper()
    risk_per = max(entry_px - initial_stop, 0)
    risk_total = round(risk_per * qty, 2)
    return {
        "headline": "Entry conditions became valid",
        "market_story": (
            "The earlier breakout had pulled back rather than continuing straight upward. "
            "The pullback developed a second attempt to resume the upward move."
        ),
        "system_noticed": [
            "The setup was already armed before this bar.",
            "This small bullish bar completed the conditions required for a controlled long entry.",
        ],
        "decision": f"Buy {qty} {sym} shares at ${entry_px:.2f}.",
        "position_risk": [
            f"Initial structural stop: ${initial_stop:.3f}.",
            f"Initial risk: approximately ${risk_per:.3f} per share and ${risk_total:.2f} total.",
        ],
        "evidence": evidence,
    }


def _trail_activation_narrative(
    row: dict[str, Any],
    *,
    trade: dict[str, Any],
    ledger_events: list[dict[str, Any]],
    prev_ledger_events: list[dict[str, Any]],
    new_stop: float,
    qty: int,
) -> dict[str, Any]:
    scheduled_prev = any(e.get("event") == "STOP_TRAIL_SCHEDULED" for e in prev_ledger_events)
    calc_bar = next((e.get("calculation_bar") for e in ledger_events if e.get("event") == "STOP_TRAIL_UPDATE"), None)
    system = [
        "The structural swing low was confirmed on the previous bar.",
        "To avoid lookahead, the tighter stop became active only on this bar.",
    ]
    if scheduled_prev:
        system.insert(0, "A stop raise was scheduled on the prior bar after structure was confirmed.")
    timeline = []
    if calc_bar:
        timeline.append(
            {
                "role": "confirmation_bar",
                "label": "Structure confirmed (prior bar)",
                "ts": _ts_key(calc_bar),
            }
        )
    if scheduled_prev:
        timeline.append(
            {
                "role": "scheduled",
                "label": "Stop adjustment scheduled",
                "ts": _ts_key(prev_ledger_events[0].get("ts")) if prev_ledger_events else None,
            }
        )
    timeline.append(
        {
            "role": "activation_bar",
            "label": "Stop adjustment active",
            "ts": _ts_key(row.get("bar_ts")),
        }
    )
    return {
        "headline": "Profit protection increased",
        "market_story": (
            "The trade moved upward and then pulled back. A meaningful higher low was confirmed, "
            "showing that buyers were still defending the move."
        ),
        "system_noticed": system,
        "decision": (
            f"Continue holding the {qty}-share position.\n"
            f"Raise the active stop to ${new_stop:.2f}."
        ),
        "position_risk": [
            f"Active protective stop is now ${new_stop:.2f}.",
            "The system cannot use a swing low until later price action confirms it.",
        ],
        "stop_timeline": timeline,
        "evidence": _collect_evidence(row),
    }


def _exit_narrative(
    row: dict[str, Any],
    *,
    trade: dict[str, Any],
    active_stop: float | None,
    peak_unrealized: float | None,
    peak_ts_ny: str | None,
) -> dict[str, Any]:
    qty = int(trade.get("quantity") or 0)
    exit_px = float(trade.get("exit_price") or 0)
    realized = float(trade.get("realized_pnl") or 0)
    pos_risk = [
        f"Realized profit: {'+' if realized >= 0 else ''}${realized:.2f}.",
    ]
    if peak_unrealized is not None and peak_ts_ny:
        pos_risk.append(
            f"Peak unrealized profit had been +${peak_unrealized:.2f} at {peak_ts_ny} New York."
        )
    stop_txt = f"${active_stop:.2f}" if active_stop is not None else "the active stop"
    return {
        "headline": "Trade closed at the protective stop",
        "market_story": (
            "Price pulled back after the earlier advance and reached the active profit-protecting stop."
        ),
        "system_noticed": [
            f"The active stop was {stop_txt}.",
            "The current bar traded down to that protective level.",
        ],
        "decision": f"Exit {qty} shares at ${exit_px:.2f}.",
        "position_risk": pos_risk,
        "why": (
            "The stop had already been raised to protect part of the gain. "
            "The current bar traded down to that active stop."
        ),
        "evidence": _collect_evidence(row),
    }


def _ordinary_narrative(
    row: dict[str, Any],
    *,
    qty: int,
    in_trade: bool,
    ledger_events: list[dict[str, Any]],
) -> dict[str, Any]:
    action = str(row.get("selected_action") or "")
    codes = _ledger_codes(ledger_events)
    evidence = _collect_evidence(row)

    if "STOP_TRAIL_SCHEDULED" in codes:
        cand = next((e.get("candidate_stop") for e in ledger_events if e.get("event") == "STOP_TRAIL_SCHEDULED"), None)
        return {
            "headline": "Stop raise scheduled",
            "market_story": "Structure confirmed on this bar justifies a tighter stop, but the adjustment activates on the next bar.",
            "system_noticed": [
                translate_ledger_event("STOP_TRAIL_SCHEDULED"),
                "This avoids using unconfirmed structure before the bar closes.",
            ],
            "decision": "Continue holding; stop update scheduled for next bar."
            if in_trade
            else translate_action(action),
            "position_risk": [f"Candidate stop: ${float(cand):.2f}." if cand else "Stop update pending."],
            "evidence": evidence,
        }

    if "STOP_EVAL" in codes and in_trade and "STOP_TRAIL_UPDATE" not in codes and "EXIT" not in codes:
        return {
            "headline": "Hold position",
            "market_story": "The trade remains open. Price is being compared to the active protective stop.",
            "system_noticed": [translate_ledger_event("STOP_EVAL")],
            "decision": translate_action("HOLD_POSITION"),
            "position_risk": ["No confirmed structural event on this bar requires moving the stop."],
            "evidence": evidence,
        }

    if in_trade and action == "HOLD_POSITION":
        return {
            "headline": "Hold position",
            "market_story": "The trade remains open. No confirmed structural event justifies moving the stop on this bar.",
            "system_noticed": ["Advisory action unchanged."],
            "decision": translate_action("HOLD_POSITION"),
            "position_risk": [],
            "evidence": evidence,
        }

    if not in_trade:
        if action in ("ENTRY_ARMED", "CONSIDER_ENTRY", "WAIT_FOR_FOLLOW_THROUGH"):
            return {
                "headline": translate_action(action),
                "market_story": "Price remains inside the developing setup. The system is waiting because entry conditions are not yet complete."
                if action != "CONSIDER_ENTRY"
                else "Entry conditions are close; the next bar may confirm or reject the setup.",
                "system_noticed": [translate_action(action)],
                "decision": "Wait — no entry yet.",
                "position_risk": [],
                "evidence": evidence,
            }
        return {
            "headline": "No decision change",
            "market_story": "Session structure is developing. No trade action applies on this bar.",
            "system_noticed": [translate_action(action) if action else "Monitoring price action."],
            "decision": "Wait.",
            "position_risk": [],
            "evidence": evidence,
        }

    return {
        "headline": translate_action(action) if action else "Monitoring",
        "market_story": row.get("market_story") or "Price action continues within the open trade.",
        "system_noticed": [translate_action(action)] if action else [],
        "decision": translate_action(action),
        "position_risk": [],
        "evidence": evidence,
    }


def build_bar_narrative(
    row: dict[str, Any],
    *,
    trade: dict[str, Any] | None,
    ledger_events: list[dict[str, Any]],
    prev_ledger_events: list[dict[str, Any]],
    qty: int,
    stop: float | None,
    initial_stop: float | None,
    peak_unrealized: float | None = None,
    peak_unrealized_ts_ny: str | None = None,
    trade_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build G2 story + evidence + technical for one bar (no future facts in story)."""
    bar_ts = row.get("bar_ts")
    ohlcv = row.get("ohlcv") or {}
    codes = _ledger_codes(ledger_events)
    in_trade = qty > 0

    eff = str(row.get("simulation_effect") or "")
    story: dict[str, Any]

    if trade and ("ENTRY" in codes or eff.startswith("ENTRY")):
        story = _entry_narrative(
            row,
            trade=trade,
            initial_stop=float(initial_stop or 246.065),
            evidence=_collect_evidence(row),
        )
    elif trade and ("STOP_TRAIL_UPDATE" in codes):
        new_stop = float(
            next(e.get("new_stop") for e in ledger_events if e.get("event") == "STOP_TRAIL_UPDATE")
        )
        story = _trail_activation_narrative(
            row,
            trade=trade,
            ledger_events=ledger_events,
            prev_ledger_events=prev_ledger_events,
            new_stop=new_stop,
            qty=int(trade.get("quantity") or 0),
        )
    elif trade and ("EXIT" in codes or eff.startswith("EXIT")):
        peak_ny = peak_unrealized_ts_ny
        if trade_summary and peak_ny is None and trade_summary.get("peak_unrealized_pnl_ts"):
            peak_ny = _ny_hm(trade_summary.get("peak_unrealized_pnl_ts"), trade_summary.get("peak_unrealized_pnl_ts"))
        story = _exit_narrative(
            row,
            trade=trade,
            active_stop=stop,
            peak_unrealized=peak_unrealized or (trade_summary or {}).get("peak_unrealized_pnl"),
            peak_ts_ny=peak_ny,
        )
    else:
        story = _ordinary_narrative(row, qty=qty, in_trade=in_trade, ledger_events=ledger_events)

    position = _position_snapshot(
        trade=trade,
        bar_ts=str(bar_ts),
        qty=qty,
        stop=stop,
        ohlcv=ohlcv,
        ledger_events=ledger_events,
        initial_stop=initial_stop,
    )

    return {
        "bar_ts": bar_ts,
        "time_ny": _ny_hm(row.get("bar_ts_ny"), bar_ts),
        "headline": story.get("headline"),
        "story": {
            "market_story": story.get("market_story"),
            "system_noticed": story.get("system_noticed") or [],
            "decision": story.get("decision"),
            "position_risk": story.get("position_risk") or [],
            "why": story.get("why"),
            "stop_timeline": story.get("stop_timeline"),
        },
        "evidence": story.get("evidence") or _collect_evidence(row),
        "technical": _technical_block(row, ledger_events),
        "position_at_bar": position,
        "ledger_events_plain": [
            {"code": c, "plain": translate_ledger_event(c)} for c in codes if c
        ],
    }
