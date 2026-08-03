"""Phase 7B — deterministic certification fixtures (in-memory only)."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Callable

from .context_ruleset_v01 import ACTION_CONSIDER_ENTRY, ACTION_ENTRY_ARMED, ACTION_OBSERVE, ACTION_THESIS_INVALIDATED
from .simulation_engine import BLOCK_REASON_INSUFFICIENT_CASH, BLOCK_REASON_OPENING, BLOCK_REASON_POSITION_OPEN
from .simulation_engine import BLOCK_REASON_EOD_CUTOFF
from .simulation_repository import simulation_sequence_hash
from .simulation_runner import FixtureStep, make_bar, make_ctx, run_simulation_fixture

CERTIFICATION_RUN_ID = "CERTIFICATION_FIXTURE_LOCAL"
HISTORICAL_CONTEXT = "63ecc779-3dbb-4468-8bc1-a8bbd0f17342"
HISTORICAL_SIMULATION = "4bd264d3-c061-4bee-8534-c4eb99532438"

_DOSSIER = {"trade_simulation_ready": True, "paa_verdict": "LONG_APPROVE"}


def _ts(h: int, m: int) -> datetime:
    return datetime(2026, 7, 21, h, m, 0)


def _result(name: str, passed: bool, expected: Any, actual: Any, **extra: Any) -> dict:
    return {"fixture": name, "pass": passed, "expected": expected, "actual": actual, **extra}


def scenario_01_profitable_trade() -> dict:
    sym = "AAPL"
    steps = [
        FixtureStep(
            bar_index_in_session=10,
            is_last_bar_in_session=False,
            bars={sym: make_bar(sym, _ts(10, 30), o=99, h=100.5, l=99, c=100.0)},
            context_by_symbol={sym: make_ctx(ACTION_CONSIDER_ENTRY)},
            dossiers_by_symbol={sym: _DOSSIER},
        ),
        FixtureStep(
            bar_index_in_session=11,
            is_last_bar_in_session=False,
            bars={sym: make_bar(sym, _ts(10, 35), o=100, h=101, l=99.5, c=100.5)},
            context_by_symbol={sym: make_ctx(ACTION_THESIS_INVALIDATED)},
            dossiers_by_symbol={sym: _DOSSIER},
        ),
        FixtureStep(
            bar_index_in_session=12,
            is_last_bar_in_session=False,
            bars={sym: make_bar(sym, _ts(10, 40), o=105, h=106, l=104, c=105.5)},
            context_by_symbol={sym: make_ctx(ACTION_OBSERVE)},
            dossiers_by_symbol={sym: _DOSSIER},
        ),
    ]
    r = run_simulation_fixture(steps, symbol_order=[sym], starting_cash=1000.0)
    p = r.portfolio
    trade = p.closed_trades[0] if p.closed_trades else {}
    qty = 10
    expected_pnl = (105.0 - 100.0) * qty
    passed = (
        len(p.closed_trades) == 1
        and trade.get("entry_price") == 100.0
        and trade.get("exit_price") == 105.0
        and trade.get("quantity") == qty
        and p.cash == 1000.0 + expected_pnl
        and abs(p.realized_pnl - expected_pnl) < 0.01
        and p.cash == 0 + qty * 105.0
    )
    return _result(
        "01_basic_profitable_trade",
        passed,
        {"entry": 100, "exit_open": 105, "qty": 10, "final_cash": 1050, "pnl": 50},
        {"trade": trade, "cash": p.cash, "pnl": p.realized_pnl},
        steps=len(steps),
        cash_after_entry=0.0,
    )


def scenario_02_losing_trade() -> dict:
    sym = "AAPL"
    steps = [
        FixtureStep(
            10,
            False,
            bars={sym: make_bar(sym, _ts(11, 0), o=50, h=50.5, l=49.5, c=50.0)},
            context_by_symbol={sym: make_ctx(ACTION_CONSIDER_ENTRY)},
            dossiers_by_symbol={sym: _DOSSIER},
        ),
        FixtureStep(
            11,
            False,
            bars={sym: make_bar(sym, _ts(11, 5), o=50, h=50.2, l=49, c=49.8)},
            context_by_symbol={sym: make_ctx(ACTION_THESIS_INVALIDATED)},
            dossiers_by_symbol={sym: _DOSSIER},
        ),
        FixtureStep(
            12,
            False,
            bars={sym: make_bar(sym, _ts(11, 10), o=48, h=49, l=47.5, c=48.5)},
            context_by_symbol={sym: make_ctx(ACTION_OBSERVE)},
            dossiers_by_symbol={sym: _DOSSIER},
        ),
    ]
    r = run_simulation_fixture(steps, symbol_order=[sym], starting_cash=1000.0)
    t = r.portfolio.closed_trades[0]
    pnl = (48.0 - 50.0) * 20
    passed = t["exit_price"] == 48.0 and t["entry_price"] == 50.0 and r.portfolio.cash < 1000 and r.portfolio.realized_pnl < 0
    return _result("02_basic_losing_trade", passed, {"pnl": pnl, "final_cash": 1000 + pnl}, {"trade": t, "cash": r.portfolio.cash},)


def scenario_03_thesis_exit_next_bar() -> dict:
    sym = "AAPL"
    steps = [
        FixtureStep(10, False, bars={sym: make_bar(sym, _ts(12, 0), o=10, h=10.2, l=9.8, c=10.0)}, context_by_symbol={sym: make_ctx(ACTION_CONSIDER_ENTRY)}, dossiers_by_symbol={sym: _DOSSIER}),
        FixtureStep(11, False, bars={sym: make_bar(sym, _ts(12, 5), o=10, h=10.1, l=9.9, c=10.05)}, context_by_symbol={sym: make_ctx(ACTION_THESIS_INVALIDATED)}, dossiers_by_symbol={sym: _DOSSIER}),
        FixtureStep(12, False, bars={sym: make_bar(sym, _ts(12, 10), o=9.5, h=9.8, l=9.4, c=9.6)}, context_by_symbol={sym: make_ctx(ACTION_OBSERVE)}, dossiers_by_symbol={sym: _DOSSIER}),
    ]
    r1 = run_simulation_fixture(steps[:2], symbol_order=[sym], starting_cash=1000.0)
    cash_after_inv = r1.portfolio.cash
    pending = r1.portfolio.pending_exit is not None
    r2 = run_simulation_fixture(steps, symbol_order=[sym], starting_cash=1000.0)
    passed = pending and cash_after_inv == 0.0 and len(r2.portfolio.closed_trades) == 1 and r2.portfolio.closed_trades[0]["exit_price"] == 9.5
    return _result(
        "03_daily_thesis_invalidation_exit",
        passed,
        "pending after invalidate bar; fill at next open not same-bar close",
        {"cash_after_invalidation_bar": cash_after_inv, "pending": pending, "exit_price": r2.portfolio.closed_trades[0].get("exit_price")},
    )


def scenario_04_eod_flatten() -> dict:
    sym = "AAPL"
    steps = [
        FixtureStep(70, False, bars={sym: make_bar(sym, _ts(15, 20), o=20, h=20.5, l=19.8, c=20.0)}, context_by_symbol={sym: make_ctx(ACTION_CONSIDER_ENTRY)}, dossiers_by_symbol={sym: _DOSSIER}),
        FixtureStep(77, True, bars={sym: make_bar(sym, _ts(15, 55), o=21, h=22, l=20.5, c=21.5)}, context_by_symbol={sym: make_ctx(ACTION_OBSERVE)}, dossiers_by_symbol={sym: _DOSSIER}),
    ]
    r = run_simulation_fixture(steps, symbol_order=[sym], starting_cash=1000.0)
    t = r.portfolio.closed_trades[0]
    passed = t["exit_reason"] == "FORCED_END_OF_DAY_EXIT" and t["exit_price"] == 21.5 and r.portfolio.open_position is None
    return _result("04_end_of_day_flatten", passed, {"exit_reason": "FORCED_END_OF_DAY_EXIT", "exit_at": "close"}, t)


def scenario_05_week_end_flatten() -> dict:
    sym = "AAPL"
    steps = [
        FixtureStep(10, False, is_last_bar_of_week=False, bars={sym: make_bar(sym, _ts(14, 0), o=30, h=30.5, l=29.5, c=30.0)}, context_by_symbol={sym: make_ctx(ACTION_CONSIDER_ENTRY)}, dossiers_by_symbol={sym: _DOSSIER}),
        FixtureStep(77, False, is_last_bar_of_week=True, bars={sym: make_bar(sym, _ts(15, 55), o=31, h=32, l=30.5, c=31.5)}, context_by_symbol={sym: make_ctx(ACTION_OBSERVE)}, dossiers_by_symbol={sym: _DOSSIER}),
    ]
    r = run_simulation_fixture(steps, symbol_order=[sym])
    passed = r.portfolio.open_position is None and r.portfolio.closed_trades[-1]["exit_reason"] == "FORCED_WEEK_END_FLATTEN"
    return _result("05_week_end_flatten", passed, "no position after week-end step", {"open": r.portfolio.open_position, "last_exit": r.portfolio.closed_trades[-1]["exit_reason"]})


def scenario_06_one_position() -> dict:
    steps = [
        FixtureStep(10, False, bars={"AAPL": make_bar("AAPL", _ts(13, 0), o=100, h=100.5, l=99.5, c=100)}, context_by_symbol={"AAPL": make_ctx(ACTION_CONSIDER_ENTRY), "AMZN": make_ctx(ACTION_OBSERVE)}, dossiers_by_symbol={"AAPL": _DOSSIER, "AMZN": _DOSSIER}),
        FixtureStep(11, False, bars={"AMZN": make_bar("AMZN", _ts(13, 5), o=200, h=201, l=199, c=200)}, context_by_symbol={"AAPL": make_ctx(ACTION_OBSERVE), "AMZN": make_ctx(ACTION_CONSIDER_ENTRY)}, dossiers_by_symbol={"AAPL": _DOSSIER, "AMZN": _DOSSIER}),
    ]
    r = run_simulation_fixture(steps, symbol_order=["AAPL", "AMZN"])
    blocked = [b for b in r.portfolio.blocked_signals if b["block_reason"] == BLOCK_REASON_POSITION_OPEN]
    passed = r.portfolio.open_position and r.portfolio.open_position.symbol == "AAPL" and len(blocked) == 1 and blocked[0]["symbol"] == "AMZN"
    return _result("06_one_position_constraint", passed, BLOCK_REASON_POSITION_OPEN, blocked)


def scenario_07_simultaneous_tiebreak() -> dict:
    shared = _ts(13, 30)
    step = FixtureStep(
        10,
        False,
        step_key="shared-ts",
        bars={
            "AAPL": make_bar("AAPL", shared, o=10, h=10.2, l=9.8, c=10),
            "AMZN": make_bar("AMZN", shared, o=10, h=10.2, l=9.8, c=10),
        },
        context_by_symbol={
            "AAPL": make_ctx(ACTION_CONSIDER_ENTRY, entry_score=5),
            "AMZN": make_ctx(ACTION_CONSIDER_ENTRY, entry_score=9),
        },
        dossiers_by_symbol={"AAPL": {**_DOSSIER, "paa_verdict": "WAIT_PULLBACK"}, "AMZN": {**_DOSSIER, "paa_verdict": "LONG_APPROVE"}},
    )
    r1 = run_simulation_fixture([step], symbol_order=["AAPL", "AMZN", "JPM", "MCD"])
    r2 = run_simulation_fixture([step], symbol_order=["AAPL", "AMZN", "JPM", "MCD"])
    winner1 = r1.portfolio.open_position.symbol if r1.portfolio.open_position else None
    winner2 = r2.portfolio.open_position.symbol if r2.portfolio.open_position else None
    tie_blocks = [b for b in r1.portfolio.blocked_signals if b["block_reason"] == "TIE_BREAK_LOSER"]
    passed = winner1 == winner2 == "AMZN" and tie_blocks and "winner" in (tie_blocks[0].get("tie_break_json") or {})
    return _result("07_simultaneous_signals", passed, "AMZN wins tie-break twice", {"winner1": winner1, "winner2": winner2, "tie_break": tie_blocks})


def scenario_08_insufficient_cash() -> dict:
    sym = "AAPL"
    step = FixtureStep(10, False, bars={sym: make_bar(sym, _ts(13, 35), o=100, h=100, l=100, c=100)}, context_by_symbol={sym: make_ctx(ACTION_CONSIDER_ENTRY)}, dossiers_by_symbol={sym: _DOSSIER})
    r = run_simulation_fixture([step], symbol_order=[sym], starting_cash=50.0)
    b = r.portfolio.blocked_signals
    passed = not r.portfolio.open_position and r.portfolio.cash == 50.0 and b and b[0]["block_reason"] == BLOCK_REASON_INSUFFICIENT_CASH
    return _result("08_insufficient_cash", passed, BLOCK_REASON_INSUFFICIENT_CASH, b)


def scenario_09_whole_share_sizing() -> dict:
    cases = []
    for price, cash, expected_qty in [(100, 1000, 10), (250, 1000, 4), (333, 999, 3), (100, 105, 1)]:
        sym = "AAPL"
        step = FixtureStep(10, False, bars={sym: make_bar(sym, _ts(14, 0), o=price, h=price, l=price, c=price)}, context_by_symbol={sym: make_ctx(ACTION_CONSIDER_ENTRY)}, dossiers_by_symbol={sym: _DOSSIER})
        r = run_simulation_fixture([step], symbol_order=[sym], starting_cash=cash)
        qty = r.portfolio.open_position.quantity if r.portfolio.open_position else 0
        residual = r.portfolio.cash
        cases.append({"price": price, "cash": cash, "qty": qty, "expected": expected_qty, "residual": residual})
    passed = all(c["qty"] == c["expected"] for c in cases)
    return _result("09_whole_share_sizing", passed, "floor(cash/price)", cases)


def scenario_10_entry_armed_not_sufficient() -> dict:
    sym = "AAPL"
    step = FixtureStep(10, False, bars={sym: make_bar(sym, _ts(14, 5), o=10, h=10, l=10, c=10)}, context_by_symbol={sym: make_ctx(ACTION_ENTRY_ARMED)}, dossiers_by_symbol={sym: _DOSSIER})
    r = run_simulation_fixture([step], symbol_order=[sym])
    passed = r.portfolio.open_position is None and r.portfolio.cash == 1000.0 and not r.portfolio.blocked_signals
    return _result("10_entry_armed_not_sufficient", passed, "no trade, no block", {"cash": r.portfolio.cash, "blocks": len(r.portfolio.blocked_signals)})


def scenario_11_opening_window() -> dict:
    sym = "AAPL"
    steps = [
        FixtureStep(0, False, bars={sym: make_bar(sym, _ts(9, 30), o=10, h=10, l=10, c=10)}, context_by_symbol={sym: make_ctx(ACTION_CONSIDER_ENTRY)}, dossiers_by_symbol={sym: _DOSSIER}),
        FixtureStep(5, False, bars={sym: make_bar(sym, _ts(9, 55), o=10, h=10, l=10, c=10)}, context_by_symbol={sym: make_ctx(ACTION_CONSIDER_ENTRY)}, dossiers_by_symbol={sym: _DOSSIER}),
    ]
    r = run_simulation_fixture(steps, symbol_order=[sym])
    blocks = r.portfolio.blocked_signals
    passed = any(b["block_reason"] == BLOCK_REASON_OPENING for b in blocks) and r.portfolio.open_position is not None
    return _result("11_opening_window_block", passed, "block then enter", blocks)


def scenario_12_entry_cutoff() -> dict:
    sym = "AAPL"
    step = FixtureStep(73, False, bars={sym: make_bar(sym, _ts(15, 35), o=10, h=10, l=10, c=10)}, context_by_symbol={sym: make_ctx(ACTION_CONSIDER_ENTRY)}, dossiers_by_symbol={sym: _DOSSIER})
    r = run_simulation_fixture([step], symbol_order=[sym])
    passed = not r.portfolio.open_position and any(b["block_reason"] == BLOCK_REASON_EOD_CUTOFF for b in r.portfolio.blocked_signals)
    return _result("12_entry_cutoff_block", passed, BLOCK_REASON_EOD_CUTOFF, r.portfolio.blocked_signals)


def scenario_13_context_version() -> dict:
    """Uses repository verify_context_attempt when Snowflake available."""
    from .simulation_ruleset_v01 import REQUIRED_CONTEXT_RULESET

    outcomes = {"v02_accepted": False, "v01_rejected": False, "missing_rejected": False}
    try:
        from .simulation_repository import verify_context_attempt

        verify_context_attempt(
            run_id="4eababc8-88fe-4ebc-b47c-b65f6ec10c74",
            context_attempt_id=HISTORICAL_CONTEXT,
            required_ruleset=REQUIRED_CONTEXT_RULESET,
        )
        outcomes["v02_accepted"] = True
    except Exception:
        outcomes["v02_accepted"] = False
    try:
        from .simulation_repository import verify_context_attempt

        verify_context_attempt(
            run_id="4eababc8-88fe-4ebc-b47c-b65f6ec10c74",
            context_attempt_id="40d797ff-f707-4094-b195-16b0ec5e2434",
            required_ruleset=REQUIRED_CONTEXT_RULESET,
        )
        outcomes["v01_rejected"] = False
    except ValueError:
        outcomes["v01_rejected"] = True
    try:
        from .simulation_repository import verify_context_attempt

        verify_context_attempt(
            run_id="4eababc8-88fe-4ebc-b47c-b65f6ec10c74",
            context_attempt_id="00000000-0000-0000-0000-000000000099",
            required_ruleset=REQUIRED_CONTEXT_RULESET,
        )
    except ValueError:
        outcomes["missing_rejected"] = True
    passed = outcomes["v02_accepted"] and outcomes["v01_rejected"] and outcomes["missing_rejected"]
    return _result("13_context_version_rejection", passed, outcomes, outcomes)


def scenario_14_idempotency() -> dict:
    sym = "AAPL"
    step = FixtureStep(
        10,
        False,
        step_key="dup-key",
        bars={sym: make_bar(sym, _ts(14, 10), o=10, h=10, l=10, c=10)},
        context_by_symbol={sym: make_ctx(ACTION_CONSIDER_ENTRY)},
        dossiers_by_symbol={sym: _DOSSIER},
    )
    seen: set[str] = set()
    r = run_simulation_fixture([step, step], symbol_order=[sym], processed_step_keys=seen)
    passed = (
        r.skipped_duplicate_steps == 1
        and r.portfolio.open_position is not None
        and r.portfolio.cash == 0.0
        and len(r.portfolio.blocked_signals) == 0
    )
    return _result(
        "14_duplicate_idempotency",
        passed,
        "one entry; duplicate step skipped in same run",
        {"skipped": r.skipped_duplicate_steps, "cash": r.portfolio.cash, "qty": r.portfolio.open_position.quantity if r.portfolio.open_position else 0},
    )


ALL_SCENARIOS: list[Callable[[], dict]] = [
    scenario_01_profitable_trade,
    scenario_02_losing_trade,
    scenario_03_thesis_exit_next_bar,
    scenario_04_eod_flatten,
    scenario_05_week_end_flatten,
    scenario_06_one_position,
    scenario_07_simultaneous_tiebreak,
    scenario_08_insufficient_cash,
    scenario_09_whole_share_sizing,
    scenario_10_entry_armed_not_sufficient,
    scenario_11_opening_window,
    scenario_12_entry_cutoff,
    scenario_13_context_version,
    scenario_14_idempotency,
]


def phase6b_reference_summary() -> dict:
    return {
        "context_attempt_id": HISTORICAL_CONTEXT,
        "context_ruleset": "BROOKS_CONTEXT_RULESET_V0_2",
        "v02_action_duration": {
            "DO_NOT_ENTER": 708,
            "WAIT": 539,
            "WAIT_FOR_PULLBACK": 241,
            "DO_NOT_CHASE": 61,
            "THESIS_INVALIDATED": 11,
        },
        "v02_action_transition_events": {
            "DO_NOT_ENTER": 17,
            "THESIS_INVALIDATED": 11,
            "DO_NOT_CHASE": 1,
            "ENTRY_ARMED": 0,
            "CONSIDER_ENTRY": 0,
        },
        "nearest_entry_blocker_top30": {"support_held": 30},
        "pilot_week_no_entry_reason": "Zero CONSIDER_ENTRY/ENTRY_ARMED rows; support/room/reclaim gates never aligned (Phase 6B review pack).",
        "source": "cursorfiles/brooks_phase6b_completion_report.json",
    }


def run_certification() -> dict:
    fixtures = [fn() for fn in ALL_SCENARIOS]
    payload = json.dumps(fixtures, sort_keys=True, default=str)
    fixture_hash = hashlib.sha256(payload.encode()).hexdigest()
    sim_hash = simulation_sequence_hash(
        [f for f in fixtures if f.get("actual") and isinstance(f.get("actual"), dict) and f["actual"].get("trade")],
        [],
    )
    all_pass = all(f["pass"] for f in fixtures)
    return {
        "phase": "7B",
        "certification_run_id": CERTIFICATION_RUN_ID,
        "persistence_isolation": "All fixtures in-memory only; historical run 4eababc8-88fe-4ebc-b47c-b65f6ec10c74 not modified",
        "historical_simulation_attempt_preserved": HISTORICAL_SIMULATION,
        "historical_context_attempt_preserved": HISTORICAL_CONTEXT,
        "fixture_sequence_hash": fixture_hash,
        "simulation_result_hash": sim_hash,
        "all_pass": all_pass,
        "fixtures": fixtures,
        "phase6b_reference": phase6b_reference_summary(),
        "idempotency": next((f for f in fixtures if f["fixture"] == "14_duplicate_idempotency"), {}),
    }
