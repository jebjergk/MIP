"""UI launcher for frozen BROOKS_INTRADAY_ADVISER_V1_0 historical validation (orchestration only)."""

from __future__ import annotations

from datetime import date
from typing import Any

from .adviser_baseline_v01 import ADVISER_VERSION, LAB_STARTING_CASH
from .adviser_runner_v1_0 import run_adviser_session
from .adviser_v1_learning_view import find_v1_session_for_symbol_date
from .historical_bar_repository import load_bars_from_store
from .learning_constants import PHASE_E1_VALIDATION_RUN_ID

PILOT_SYMBOLS = ("AAPL", "AMZN", "JPM", "MCD")
DEFAULT_VALIDATION_RUN_ID = PHASE_E1_VALIDATION_RUN_ID


def frozen_validation_config() -> dict[str, Any]:
    return {
        "adviser_version": ADVISER_VERSION,
        "starting_cash_usd": LAB_STARTING_CASH,
        "whole_shares": True,
        "long_only": True,
        "one_position": True,
        "bar_interval": "5-minute RTH",
        "symbols": list(PILOT_SYMBOLS),
    }


def check_rth_bars(symbol: str, trading_date: date | str) -> dict[str, Any]:
    sym = symbol.upper()
    td = date.fromisoformat(str(trading_date)[:10])
    bars = [b for b in load_bars_from_store(sym, td) if b.rth]
    existing = find_v1_session_for_symbol_date(sym, td)
    return {
        "symbol": sym,
        "trading_date": td.isoformat(),
        "ok": len(bars) > 0,
        "rth_bar_count": len(bars),
        "expected_rth_bars": 78,
        "already_validated": existing is not None,
        "existing_session": (
            {
                "adviser_attempt_id": existing.get("adviser_attempt_id"),
                "simulation_attempt_id": existing.get("simulation_attempt_id"),
                "selector_label": existing.get("selector_label") or existing.get("label"),
                "trade_count": existing.get("trade_count"),
            }
            if existing
            else None
        ),
    }


def run_frozen_validation(
    *,
    symbol: str,
    trading_date: date | str,
    run_id: str | None = None,
) -> dict[str, Any]:
    sym = symbol.upper()
    if sym not in PILOT_SYMBOLS:
        raise ValueError(f"Symbol not in pilot universe: {sym}")
    td = date.fromisoformat(str(trading_date)[:10])
    check = check_rth_bars(sym, td)
    if not check["ok"]:
        raise ValueError(f"No 5-minute RTH bars for {sym} on {td.isoformat()}")
    if check.get("already_validated"):
        label = (check.get("existing_session") or {}).get("selector_label") or f"{sym} · {td.isoformat()}"
        raise ValueError(
            f"V1.0 validation already exists for {sym} on {td.isoformat()} ({label}). "
            "Select it in the session dropdown or remove the duplicate attempt before re-running."
        )
    rid = run_id or DEFAULT_VALIDATION_RUN_ID
    result = run_adviser_session(run_id=rid, symbol=sym, trading_date=td)
    return {
        "run_id": rid,
        "symbol": sym,
        "trading_date": td.isoformat(),
        "adviser_attempt_id": result.get("adviser_attempt_id"),
        "simulation_attempt_id": result.get("simulation_attempt_id"),
        "status": result.get("status") or "COMPLETED",
        "summary": result.get("summary") or {},
    }
