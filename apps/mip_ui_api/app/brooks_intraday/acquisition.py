from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Callable

from .bars import bar_validation_to_dict, validate_bars
from .calendar import TradingSession, resolve_week_sessions
from .historical_bar_repository import load_bars_from_store, persist_bars
from .ib_historical_provider import (
    IbHistoricalProviderError,
    fetch_session_bars_ib,
    ib_payload_to_historical_bars,
)
from .readiness import compute_readiness_labels, dataset_hash

logger = logging.getLogger(__name__)


def _session_map(state: dict[str, Any]) -> dict[date, TradingSession]:
    week = resolve_week_sessions(state["selected_week_start"])
    return {s.trading_date: s for s in week.sessions}


def validate_stored_session(symbol: str, trading_date: date, session: TradingSession) -> dict[str, Any]:
    bars = load_bars_from_store(symbol, trading_date)
    source = bars[0].source if bars else "NONE"
    result = validate_bars(bars, session, source=source)
    out = bar_validation_to_dict(result)
    out["retrieval_status"] = "STORED"
    return out


def list_sessions_for_run(state: dict[str, Any]) -> list[tuple[str, date, TradingSession]]:
    symbols = state["symbols"]
    sessions = _session_map(state)
    out: list[tuple[str, date, TradingSession]] = []
    for td in sorted(sessions.keys()):
        for sym in symbols:
            out.append((sym, td, sessions[td]))
    return out


def _update_matrix_row(state: dict[str, Any], symbol: str, trading_date: date, patch: dict) -> None:
    prep = state.setdefault("preparation", {})
    matrix = prep.setdefault("matrix", [])
    key = f"{trading_date.isoformat()}|{symbol}"
    for row in matrix:
        if row.get("symbol") == symbol and row.get("trading_date") == trading_date.isoformat():
            row.update(patch)
            return
    matrix.append({"symbol": symbol, "trading_date": trading_date.isoformat(), **patch})


def refresh_bar_statuses(state: dict[str, Any]) -> None:
    """Recompute bar session counts from dedicated store."""
    prep = state.setdefault("preparation", {})
    counts = prep.setdefault("counts", {})
    complete = 0
    expected = 0
    for sym, td, session in list_sessions_for_run(state):
        expected += 1
        val = validate_stored_session(sym, td, session)
        freeze = (state.get("bar_dataset_freeze") or {}).get(f"{sym}|{td.isoformat()}", {})
        _update_matrix_row(
            state,
            sym,
            td,
            {
                "bar_count": val.get("bar_count"),
                "bar_expected": val.get("expected_bar_count"),
                "bar_data_status": val.get("status"),
                "missing_timestamps_count": val.get("missing_timestamps_count"),
                "missing_timestamps": val.get("missing_timestamps"),
                "bar_source": val.get("source"),
                "retrieval_status": val.get("retrieval_status"),
                "data_frozen": bool(freeze.get("frozen")),
                "dataset_hash": val.get("dataset_hash") or freeze.get("dataset_hash"),
            },
        )
        if val.get("status") == "COMPLETE":
            complete += 1
    counts["bar_sessions_expected"] = expected
    counts["bar_sessions_complete"] = complete
    labels = compute_readiness_labels(state)
    prep["readiness"] = labels
    state["readiness"] = labels
    if labels["historical_bar_readiness"] == "READY" and not state.get("bars_frozen"):
        _freeze_bar_dataset(state)
        labels = compute_readiness_labels(state)
        prep["readiness"] = labels
        state["readiness"] = labels


def _freeze_bar_dataset(state: dict[str, Any]) -> None:
    freeze: dict[str, Any] = {}
    for sym, td, session in list_sessions_for_run(state):
        bars = load_bars_from_store(sym, td)
        val = validate_stored_session(sym, td, session)
        if val.get("status") != "COMPLETE":
            return
        key = f"{sym}|{td.isoformat()}"
        freeze[key] = {
            "frozen": True,
            "frozen_at_utc": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
            "dataset_hash": val.get("dataset_hash"),
            "source": val.get("source"),
            "bar_count": val.get("bar_count"),
            "first_bar_ts": val.get("first_bar_ts"),
            "last_bar_ts": val.get("last_bar_ts"),
            "validation_version": val.get("validation_version"),
            "timestamp_convention": val.get("timestamp_convention"),
        }
    state["bar_dataset_freeze"] = freeze
    state["bars_frozen"] = True
    prep = state.setdefault("preparation", {})
    prep["bar_dataset_frozen"] = True


def acquire_historical_bars(
    state: dict[str, Any],
    *,
    dry_run: bool = False,
    ib_fetch: Callable | None = None,
    retry_failed_only: bool = False,
) -> dict[str, Any]:
    if state.get("bars_frozen"):
        return {
            "status": "ALREADY_FROZEN",
            "message": "Bar dataset is frozen for this run.",
            "readiness": state.get("readiness"),
        }

    fetch_fn = ib_fetch or fetch_session_bars_ib
    plan: list[dict[str, Any]] = []
    acquired = 0
    failed: list[dict] = []

    for sym, td, session in list_sessions_for_run(state):
        existing = validate_stored_session(sym, td, session)
        if existing.get("status") == "COMPLETE":
            continue
        if retry_failed_only and existing.get("status") not in {"NO_DATA", "MISSING_BARS", None}:
            if existing.get("bar_count", 0) > 0 and existing.get("status") != "COMPLETE":
                pass
            elif existing.get("status") == "COMPLETE":
                continue
        plan.append({"symbol": sym, "trading_date": td.isoformat(), "current_status": existing.get("status")})

    if dry_run:
        return {
            "dry_run": True,
            "sessions_to_request": len(plan),
            "plan": plan,
            "readiness": state.get("readiness") or compute_readiness_labels(state),
        }

    for item in plan:
        sym = item["symbol"]
        td = date.fromisoformat(item["trading_date"])
        session = _session_map(state)[td]
        try:
            payload = fetch_fn(sym, td)
            bars = ib_payload_to_historical_bars(payload, td)
            val = validate_bars(bars, session, source="IBKR_HISTORICAL_5M_RTH_V0_1")
            if val.status != "COMPLETE":
                failed.append({"symbol": sym, "trading_date": td.isoformat(), "status": val.status, "messages": val.messages})
                continue
            persist_bars(
                bars,
                source_request_id=payload.get("source_request_id"),
                source_metadata={
                    "ib_payload_status": payload.get("status"),
                    "fetched_at_utc": payload.get("fetched_at_utc"),
                    "timestamp_convention": payload.get("timestamp_convention"),
                },
                data_quality_status=val.status,
            )
            acquired += 1
        except IbHistoricalProviderError as exc:
            failed.append(
                {"symbol": sym, "trading_date": td.isoformat(), "code": exc.code, "message": exc.message}
            )

    refresh_bar_statuses(state)
    state.setdefault("configuration", {})["bar_dataset_freeze"] = state.get("bar_dataset_freeze", {})
    state["configuration"]["readiness"] = state.get("readiness")

    labels = state.get("readiness") or {}
    if labels.get("replay_readiness") == "READY":
        state["preparation_status"] = "READY"
        state["status"] = "READY"

    return {
        "sessions_requested": len(plan),
        "sessions_acquired": acquired,
        "sessions_failed": failed,
        "readiness": labels,
        "counts": (state.get("preparation") or {}).get("counts"),
    }
