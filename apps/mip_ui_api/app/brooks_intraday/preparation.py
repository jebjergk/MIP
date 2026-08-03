from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from .acquisition import refresh_bar_statuses
from .calendar import resolve_week_sessions, sessions_to_dict
from .constants import COMPILER_VERSION_DEFAULT, DOSSIER_VERSION_DEFAULT
from .dossier_compiler import compile_frozen_dossier
from .errors import BrooksIntradayError
from .paa_selection import select_pre_rth_paa
from .readiness import compute_readiness_labels
from .repository import insert_dossier_if_absent

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def prepare_run(state: dict[str, Any], *, bar_provider=None) -> dict[str, Any]:
    """Compile frozen dossiers only. Historical bars are acquired separately (Phase 2B)."""
    del bar_provider
    run_id = state["run_id"]
    week_start = state.get("selected_week_start")
    if not week_start:
        raise BrooksIntradayError(
            "WEEK_NOT_CONFIGURED",
            "Select a historical week before preparing the run.",
            run_id=run_id,
        )

    if state.get("dossiers_frozen") and state.get("preparation"):
        refresh_bar_statuses(state)
        return state.get("preparation") or {}

    symbols = state["symbols"]
    week = resolve_week_sessions(week_start)
    preparation: dict[str, Any] = {
        "preparation_status": "PREPARING",
        "week_resolution_status": week.status,
        "week_messages": week.messages,
        "sessions": sessions_to_dict(week.sessions),
        "matrix": [],
        "counts": {
            "dossiers_expected": len(symbols) * len(week.trading_dates),
            "dossiers_compiled": 0,
            "observation_ready": 0,
            "simulation_ready": 0,
            "bar_sessions_expected": len(symbols) * len(week.sessions),
            "bar_sessions_complete": 0,
        },
        "warnings": [],
        "fatal_errors": [],
        "historical_bar_source": None,
    }

    if week.status != "READY":
        preparation["preparation_status"] = "PREPARATION_FAILED"
        preparation["fatal_errors"].append(week.status)
        state["preparation"] = preparation
        state["preparation_status"] = "PREPARATION_FAILED"
        state["status"] = "PREPARATION_FAILED"
        return preparation

    compiled = 0
    obs_ready = 0
    sim_ready = 0
    fatals: list[str] = []

    for trading_date in week.trading_dates:
        for symbol in symbols:
            matrix_row: dict[str, Any] = {
                "trading_date": trading_date.isoformat(),
                "symbol": symbol,
            }
            try:
                selection = select_pre_rth_paa(symbol, trading_date)
                matrix_row["paa_status"] = selection.status
                matrix_row["paa_analysis_id"] = selection.analysis_id
                matrix_row["paa_scan_time"] = (
                    selection.scanned_at_utc.isoformat() if selection.scanned_at_utc else None
                )

                if selection.status != "SELECTED":
                    fatals.append(f"PAA missing for {symbol} on {trading_date}: {selection.status}")
                    matrix_row["dossier_status"] = selection.status
                    matrix_row["observation_ready"] = False
                    matrix_row["simulation_ready"] = False
                    preparation["matrix"].append(matrix_row)
                    continue

                dossier = compile_frozen_dossier(
                    run_id=run_id,
                    selection=selection,
                    compiled_at_utc=_utc_now(),
                )
                insert_dossier_if_absent(
                    run_id=run_id,
                    symbol=symbol,
                    trading_date=trading_date,
                    dossier=dossier,
                    paa_analysis_id=str(selection.analysis_id),
                    board_run_id=selection.board_run_id,
                    normalized_status=dossier.get("validation_status") or "READY",
                    source_hash=dossier.get("source_hash") or "",
                    dossier_version=DOSSIER_VERSION_DEFAULT,
                    compiler_version=COMPILER_VERSION_DEFAULT,
                    run_frozen=bool(state.get("dossiers_frozen")),
                )
                compiled += 1
                if dossier.get("observation_ready"):
                    obs_ready += 1
                if dossier.get("trade_simulation_ready"):
                    sim_ready += 1

                matrix_row["dossier_status"] = dossier.get("validation_status")
                matrix_row["observation_ready"] = dossier.get("observation_ready")
                matrix_row["simulation_ready"] = dossier.get("trade_simulation_ready")
                matrix_row["paa_verdict"] = dossier.get("paa_verdict")
                matrix_row["paa_confidence"] = dossier.get("paa_confidence")
                matrix_row["daily_trend"] = dossier.get("daily_trend")
                matrix_row["validation_messages"] = dossier.get("validation_messages")
            except BrooksIntradayError as exc:
                fatals.append(exc.message)
                matrix_row["dossier_status"] = exc.code
                matrix_row["observation_ready"] = False
                matrix_row["simulation_ready"] = False
            preparation["matrix"].append(matrix_row)

    preparation["counts"].update(
        {
            "dossiers_compiled": compiled,
            "observation_ready": obs_ready,
            "simulation_ready": sim_ready,
        }
    )
    preparation["fatal_errors"] = fatals

    expected_dossiers = len(symbols) * len(week.trading_dates)
    if compiled == expected_dossiers and not fatals:
        preparation["preparation_status"] = "DOSSIERS_READY"
        state["preparation_status"] = "DOSSIERS_READY"
        state["status"] = "DOSSIERS_READY"
        state["dossiers_frozen"] = True
    else:
        preparation["preparation_status"] = "PREPARATION_FAILED"
        state["preparation_status"] = "PREPARATION_FAILED"
        state["status"] = "PREPARATION_FAILED"

    state["preparation"] = preparation
    state["configuration"]["sessions"] = preparation["sessions"]
    refresh_bar_statuses(state)
    labels = state.get("readiness") or compute_readiness_labels(state)
    preparation["readiness"] = labels
    return preparation
