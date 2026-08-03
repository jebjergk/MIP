from __future__ import annotations

import json
import logging
import threading
import uuid
from copy import deepcopy
from datetime import date, datetime, timezone
from typing import Any

from app.db import get_connection

from .constants import (
    DEFAULT_MODE,
    DEFAULT_PILOT_SYMBOLS,
    DEFAULT_STARTING_CASH,
    DOSSIER_VERSION_DEFAULT,
    RULESET_VERSION_DEFAULT,
    TIE_BREAK_VERSION_DEFAULT,
)
from .models import CreateRunRequest, RunDetail, SymbolSnapshot
from .objective_ruleset_v01 import DEFAULT_PARAMETERS, RULESET_VERSION
from .preparation import prepare_run
from .paa_selection import select_pre_rth_paa
from .repository import get_dossier, load_dossiers_for_run

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_runs: dict[str, dict[str, Any]] = {}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


def _normalize_symbols(symbols: list[str]) -> list[str]:
    cleaned = [s.strip().upper() for s in symbols if s and s.strip()]
    if not cleaned:
        cleaned = list(DEFAULT_PILOT_SYMBOLS)
    # Stable unique order
    seen: set[str] = set()
    out: list[str] = []
    for sym in cleaned:
        if sym not in seen:
            seen.add(sym)
            out.append(sym)
    return out


def _symbol_snapshots(state: dict[str, Any]) -> list[SymbolSnapshot]:
    symbols = state["symbols"]
    blocked = state.get("open_position_symbol")
    matrix = (state.get("preparation") or {}).get("matrix") or []
    latest_by_symbol: dict[str, dict] = {}
    for row in matrix:
        sym = row.get("symbol")
        if sym:
            latest_by_symbol[sym] = row

    snapshots: list[SymbolSnapshot] = []
    for sym in symbols:
        row = latest_by_symbol.get(sym)
        snapshots.append(
            SymbolSnapshot(
                symbol=sym,
                paa_verdict=row.get("paa_verdict") if row else None,
                paa_confidence=row.get("paa_confidence") if row else None,
                daily_trend=row.get("daily_trend") if row else None,
                brooks_state="DOSSIER_READY" if row and row.get("dossier_status") else "PENDING",
                latest_finding=row.get("dossier_status") if row else None,
                latest_action="OBSERVE",
                thesis_status="READY" if row and row.get("observation_ready") else "PENDING",
                blocked_by_other_position=bool(blocked and blocked != sym),
            )
        )
    return snapshots


def _run_to_detail(state: dict[str, Any]) -> RunDetail:
    symbols = state["symbols"]
    open_sym = state.get("open_position_symbol")
    return RunDetail(
        run_id=state["run_id"],
        mode=state["mode"],
        status=state["status"],
        preparation_status=state.get("preparation_status", "NOT_STARTED"),
        readiness=deepcopy(state.get("readiness")),
        selected_week_start=state.get("selected_week_start"),
        ruleset_version=state["ruleset_version"],
        dossier_version=state["dossier_version"],
        symbols=symbols,
        starting_cash=state["starting_cash"],
        current_cash=state["current_cash"],
        open_position_symbol=open_sym,
        open_position_qty=state.get("open_position_qty"),
        realized_pnl=state.get("realized_pnl", 0.0),
        unrealized_pnl=state.get("unrealized_pnl", 0.0),
        replay_timestamp=state.get("replay_timestamp"),
        active_trading_date=state.get("active_trading_date"),
        playback_speed=state.get("playback_speed", "manual"),
        data_quality_status=state.get("data_quality_status", "OK"),
        error_message=state.get("error_message"),
        configuration=deepcopy(state.get("configuration", {})),
        symbol_snapshots=_symbol_snapshots(state),
        preparation=deepcopy(state.get("preparation")),
    )


def _sync_config_blob(state: dict[str, Any]) -> None:
    cfg = state.setdefault("configuration", {})
    cfg["preparation_status"] = state.get("preparation_status")
    cfg["preparation"] = state.get("preparation")
    cfg["historical_data"] = state.get("historical_data", {})
    cfg["readiness"] = state.get("readiness")
    cfg["bar_dataset_freeze"] = state.get("bar_dataset_freeze", {})
    cfg["bars_frozen"] = state.get("bars_frozen", False)
    cfg["dossiers_frozen"] = state.get("dossiers_frozen", False)
    cfg["replay_cursor"] = state.get("replay_cursor")
    cfg["replay_timestamp"] = state.get("replay_timestamp")
    cfg["active_replay_attempt_id"] = state.get("active_replay_attempt_id")
    cfg["phase4_review_baseline_attempt_id"] = state.get("phase4_review_baseline_attempt_id")
    cfg["phase5_pattern_attempt_id"] = state.get("phase5_pattern_attempt_id")
    cfg["replay_mode"] = state.get("replay_mode")
    cfg["ruleset_version"] = state.get("ruleset_version")
    if state.get("active_trading_date"):
        ad = state["active_trading_date"]
        cfg["active_trading_date"] = ad.isoformat() if hasattr(ad, "isoformat") else ad


def _persist_run_header(state: dict[str, Any]) -> None:
    _sync_config_blob(state)
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                MERGE INTO MIP.APP.BROOKS_INTRADAY_RUN t
                USING (
                    SELECT
                        %s AS RUN_ID,
                        %s AS MODE,
                        %s AS STATUS,
                        %s AS SELECTED_WEEK_START,
                        %s AS RULESET_VERSION,
                        %s AS DOSSIER_VERSION,
                        PARSE_JSON(%s) AS SYMBOL_LIST,
                        %s AS STARTING_CASH,
                        %s AS ENDING_CASH,
                        %s AS REALIZED_PNL,
                        PARSE_JSON(%s) AS CONFIG_JSON,
                        %s AS CREATED_BY,
                        %s AS ERROR_MESSAGE
                ) s
                ON t.RUN_ID = s.RUN_ID
                WHEN MATCHED THEN UPDATE SET
                    STATUS = s.STATUS,
                    ENDING_CASH = s.ENDING_CASH,
                    REALIZED_PNL = s.REALIZED_PNL,
                    CONFIG_JSON = s.CONFIG_JSON,
                    ERROR_MESSAGE = s.ERROR_MESSAGE,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    RUN_ID, MODE, STATUS, SELECTED_WEEK_START,
                    RULESET_VERSION, DOSSIER_VERSION, SYMBOL_LIST,
                    STARTING_CASH, ENDING_CASH, REALIZED_PNL,
                    CONFIG_JSON, CREATED_BY, ERROR_MESSAGE
                ) VALUES (
                    s.RUN_ID, s.MODE, s.STATUS, s.SELECTED_WEEK_START,
                    s.RULESET_VERSION, s.DOSSIER_VERSION, s.SYMBOL_LIST,
                    s.STARTING_CASH, s.ENDING_CASH, s.REALIZED_PNL,
                    s.CONFIG_JSON, s.CREATED_BY, s.ERROR_MESSAGE
                )
                """,
                (
                    state["run_id"],
                    state["mode"],
                    state["status"],
                    state.get("selected_week_start"),
                    state["ruleset_version"],
                    state["dossier_version"],
                    json.dumps(state["symbols"]),
                    state["starting_cash"],
                    state.get("current_cash"),
                    state.get("realized_pnl", 0.0),
                    json.dumps(state.get("configuration", {})),
                    state.get("created_by"),
                    state.get("error_message"),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "brooks_intraday persist_run_header skipped run_id=%s error=%s",
            state.get("run_id"),
            exc,
        )


def create_run(request: CreateRunRequest) -> RunDetail:
    symbols = _normalize_symbols(request.symbols)
    if len(symbols) != 4:
        raise ValueError("Brooks Intraday Lab v0.1 requires exactly four symbols.")

    run_id = str(uuid.uuid4())
    state: dict[str, Any] = {
        "run_id": run_id,
        "mode": request.mode or DEFAULT_MODE,
        "status": "CREATED",
        "preparation_status": "NOT_STARTED",
        "dossiers_frozen": False,
        "historical_data": {},
        "preparation": None,
        "selected_week_start": request.week_start,
        "ruleset_version": request.ruleset_version or RULESET_VERSION_DEFAULT,
        "dossier_version": request.dossier_version or DOSSIER_VERSION_DEFAULT,
        "symbols": symbols,
        "starting_cash": float(request.starting_cash or DEFAULT_STARTING_CASH),
        "current_cash": float(request.starting_cash or DEFAULT_STARTING_CASH),
        "open_position_symbol": None,
        "open_position_qty": None,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "replay_timestamp": None,
        "active_trading_date": None,
        "playback_speed": "manual",
        "data_quality_status": "OK",
        "error_message": None,
        "created_by": request.created_by,
        "created_at": _utc_now_iso(),
        "configuration": {
            "tie_break_version": TIE_BREAK_VERSION_DEFAULT,
            "max_concurrent_positions": 1,
            "direction": "LONG",
            "whole_shares_only": True,
            "overnight_holding": False,
            "bar_interval_minutes": 5,
            "replay_integrity": "no_future_bars",
        },
    }
    if request.experiment_role:
        state["configuration"]["experiment_role"] = request.experiment_role
    if request.experiment_freeze_id:
        state["configuration"]["experiment_freeze_id"] = request.experiment_freeze_id
    if request.week_start:
        state["configuration"]["selected_week_start"] = request.week_start.isoformat()

    with _lock:
        _runs[run_id] = state

    _persist_run_header(state)
    return _run_to_detail(state)


def get_run(run_id: str) -> RunDetail | None:
    with _lock:
        state = _runs.get(run_id)
        if state:
            return _run_to_detail(state)

    # Optional reload from Snowflake header only (no session rehydration yet)
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT RUN_ID, MODE, STATUS, SELECTED_WEEK_START,
                       RULESET_VERSION, DOSSIER_VERSION, SYMBOL_LIST,
                       STARTING_CASH, ENDING_CASH, REALIZED_PNL, CONFIG_JSON,
                       ERROR_MESSAGE
                FROM MIP.APP.BROOKS_INTRADAY_RUN
                WHERE RUN_ID = %s
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0].lower() for d in cur.description]
            rec = dict(zip(cols, row))
            cfg_raw = rec.get("config_json")
            if isinstance(cfg_raw, str):
                cfg = json.loads(cfg_raw)
            elif isinstance(cfg_raw, dict):
                cfg = cfg_raw
            else:
                cfg = {}
            symbols_raw = rec.get("symbol_list") or []
            if isinstance(symbols_raw, str):
                symbols_raw = json.loads(symbols_raw)
            loaded: dict[str, Any] = {
                "run_id": rec["run_id"],
                "mode": rec["mode"],
                "status": rec["status"],
                "preparation_status": cfg.get("preparation_status", "NOT_STARTED"),
                "dossiers_frozen": cfg.get("dossiers_frozen", False),
                "preparation": cfg.get("preparation"),
                "readiness": cfg.get("readiness"),
                "bar_dataset_freeze": cfg.get("bar_dataset_freeze", {}),
                "bars_frozen": cfg.get("bars_frozen", False),
                "historical_data": cfg.get("historical_data", {}),
                "replay_cursor": cfg.get("replay_cursor"),
                "selected_week_start": rec.get("selected_week_start"),
                "ruleset_version": rec["ruleset_version"],
                "dossier_version": rec["dossier_version"],
                "symbols": list(symbols_raw),
                "starting_cash": float(rec["starting_cash"]),
                "current_cash": float(rec.get("ending_cash") or rec["starting_cash"]),
                "realized_pnl": float(rec.get("realized_pnl") or 0),
                "configuration": cfg,
                "error_message": rec.get("error_message"),
                "replay_timestamp": cfg.get("replay_timestamp"),
                "active_trading_date": cfg.get("active_trading_date"),
                "playback_speed": cfg.get("playback_speed", "manual"),
                "data_quality_status": "OK",
                "active_replay_attempt_id": cfg.get("active_replay_attempt_id"),
                "phase4_review_baseline_attempt_id": cfg.get("phase4_review_baseline_attempt_id"),
                "ruleset_version": cfg.get("ruleset_version") or rec.get("ruleset_version") or RULESET_VERSION,
            }
            readiness = loaded.get("readiness") or {}
            if not loaded["dossiers_frozen"] and readiness.get("dossier_readiness") == "READY":
                if readiness.get("replay_readiness") == "READY":
                    loaded["dossiers_frozen"] = True
            if loaded["status"] == "RUNNING":
                loaded["status"] = "PAUSED"
            from .replay_attempt_repository import get_review_baseline_attempt_id

            baseline = get_review_baseline_attempt_id(run_id) or cfg.get("phase4_review_baseline_attempt_id")
            if baseline:
                loaded["phase4_review_baseline_attempt_id"] = baseline
            loaded["phase5_pattern_attempt_id"] = cfg.get("phase5_pattern_attempt_id")
            loaded["replay_mode"] = cfg.get("replay_mode")
            with _lock:
                _runs[run_id] = loaded
            try:
                from .replay_engine import recover_run_on_load

                recover_run_on_load(loaded)
            except Exception as exc:
                from .errors import BrooksIntradayError

                if isinstance(exc, BrooksIntradayError):
                    loaded["status"] = "ERROR"
                    loaded["error_message"] = exc.code
                else:
                    logger.warning("brooks replay recover failed run_id=%s err=%s", run_id, exc)
            return _run_to_detail(loaded)
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("brooks_intraday get_run snowflake fallback failed run_id=%s err=%s", run_id, exc)
        return None


def _mutate_run(run_id: str, mutator) -> RunDetail:
    with _lock:
        state = _runs.get(run_id)
        if not state:
            raise KeyError(run_id)
        mutator(state)
        _persist_run_header(state)
        return _run_to_detail(state)


def set_run_status(run_id: str, status: str, message: str | None = None) -> RunDetail:
    def _apply(state: dict[str, Any]) -> None:
        state["status"] = status
        if message is not None:
            state["error_message"] = message

    return _mutate_run(run_id, _apply)


def reset_run(run_id: str) -> RunDetail:
    from .replay_engine import replay_reset

    def _apply(state: dict[str, Any]) -> None:
        replay_reset(state)
        prep = state.get("preparation_status")
        state["status"] = "READY" if prep == "READY" else "CREATED"
        state["current_cash"] = state["starting_cash"]
        state["open_position_symbol"] = None
        state["open_position_qty"] = None
        state["realized_pnl"] = 0.0
        state["unrealized_pnl"] = 0.0
        state["playback_speed"] = state.get("playback_speed", "manual")
        state["data_quality_status"] = "OK"
        state["error_message"] = None
        cfg = state.setdefault("configuration", {})
        cfg["replay_timestamp"] = None
        cfg["active_trading_date"] = None

    return _mutate_run(run_id, _apply)


def reset_pattern_run(run_id: str) -> RunDetail:
    from .replay_engine import replay_reset_pattern

    def _apply(state: dict[str, Any]) -> None:
        replay_reset_pattern(state)
        state["status"] = "READY"
        state["replay_mode"] = "PATTERN"

    return _mutate_run(run_id, _apply)


def reset_pattern_run_v02(run_id: str) -> RunDetail:
    from .pattern_ruleset_v02 import RULESET_VERSION as PATTERN_V02
    from .replay_engine import replay_reset_pattern

    def _apply(state: dict[str, Any]) -> None:
        state.setdefault("configuration", {})["phase5_pattern_v01_attempt_id"] = (
            state.get("phase5_pattern_v01_attempt_id")
            or state.get("configuration", {}).get("phase5_pattern_v01_attempt_id")
            or "91d9919a-02e2-4a70-a4c4-2c7a63118636"
        )
        state["phase5_pattern_v01_attempt_id"] = state["configuration"]["phase5_pattern_v01_attempt_id"]
        replay_reset_pattern(state, ruleset_version=PATTERN_V02)
        state["status"] = "READY"
        state["replay_mode"] = "PATTERN"
        state["pattern_ruleset_version"] = PATTERN_V02

    return _mutate_run(run_id, _apply)


def reset_pattern_run_v03(run_id: str) -> RunDetail:
    from .pattern_ruleset_v03 import RULESET_VERSION as PATTERN_V03
    from .replay_engine import replay_reset_pattern

    def _apply(state: dict[str, Any]) -> None:
        state.setdefault("configuration", {})["phase4_review_baseline_attempt_id"] = (
            state.get("phase4_review_baseline_attempt_id")
            or state.get("configuration", {}).get("phase4_review_baseline_attempt_id")
            or "53a502f5-dec4-4bff-8106-f6637574163e"
        )
        state["phase4_review_baseline_attempt_id"] = state["configuration"]["phase4_review_baseline_attempt_id"]
        state.setdefault("configuration", {})["phase5_pattern_v01_attempt_id"] = (
            state.get("phase5_pattern_v01_attempt_id")
            or state.get("configuration", {}).get("phase5_pattern_v01_attempt_id")
            or "91d9919a-02e2-4a70-a4c4-2c7a63118636"
        )
        state["phase5_pattern_v01_attempt_id"] = state["configuration"]["phase5_pattern_v01_attempt_id"]
        state.setdefault("configuration", {})["phase5_pattern_v02_attempt_id"] = (
            state.get("phase5_pattern_v02_attempt_id")
            or state.get("configuration", {}).get("phase5_pattern_v02_attempt_id")
            or "aa43c519-f0fd-4fa3-b748-87e88417941d"
        )
        state["phase5_pattern_v02_attempt_id"] = state["configuration"]["phase5_pattern_v02_attempt_id"]
        replay_reset_pattern(state, ruleset_version=PATTERN_V03)
        state["status"] = "READY"
        state["replay_mode"] = "PATTERN"
        state["pattern_ruleset_version"] = PATTERN_V03

    return _mutate_run(run_id, _apply)


def run_objective_bulk(run_id: str) -> dict[str, Any]:
    from .replay_engine import run_objective_replay_bulk

    get_run(run_id)
    result_holder: dict[str, Any] = {}

    def _apply(state: dict[str, Any]) -> None:
        result_holder["objective_attempt_id"] = run_objective_replay_bulk(state, progress_every=50)

    _mutate_run(run_id, _apply)
    return {"run_id": run_id, "objective_attempt_id": result_holder.get("objective_attempt_id")}


def run_context_bulk(run_id: str) -> dict[str, Any]:
    from .context_replay import run_context_replay_bulk
    from .context_ruleset_v01 import RULESET_VERSION as RULESET_V01

    get_run(run_id)
    result_holder: dict[str, Any] = {}

    def _apply(state: dict[str, Any]) -> None:
        _wire_context_prerequisites(state)
        result_holder["context_attempt_id"] = run_context_replay_bulk(state, ruleset_version=RULESET_V01)

    _mutate_run(run_id, _apply)
    return {"run_id": run_id, "context_attempt_id": result_holder["context_attempt_id"]}


def run_context_bulk_v02(run_id: str) -> dict[str, Any]:
    from .context_replay import run_context_replay_bulk
    from .context_ruleset_v02 import RULESET_VERSION as RULESET_V02

    get_run(run_id)
    result_holder: dict[str, Any] = {}

    def _apply(state: dict[str, Any]) -> None:
        _wire_context_prerequisites(state)
        result_holder["context_attempt_id"] = run_context_replay_bulk(
            state, ruleset_version=RULESET_V02, notes="Phase 6B V0.2 calibrated context replay"
        )

    _mutate_run(run_id, _apply)
    return {"run_id": run_id, "context_attempt_id": result_holder["context_attempt_id"], "ruleset_version": RULESET_V02}


def _wire_context_prerequisites(state: dict[str, Any]) -> None:
    state.setdefault("configuration", {})["phase4_review_baseline_attempt_id"] = (
        state.get("phase4_review_baseline_attempt_id")
        or state.get("configuration", {}).get("phase4_review_baseline_attempt_id")
        or "53a502f5-dec4-4bff-8106-f6637574163e"
    )
    state.setdefault("configuration", {})["phase5_pattern_v03_attempt_id"] = (
        state.get("phase5_pattern_v03_attempt_id")
        or state.get("configuration", {}).get("phase5_pattern_v03_attempt_id")
        or "d83d5bab-4e02-4aec-9bd7-0da54b1395d3"
    )


def run_simulation_bulk(
    run_id: str,
    *,
    context_attempt_id: str = "63ecc779-3dbb-4468-8bc1-a8bbd0f17342",
) -> dict[str, Any]:
    from .simulation_replay import run_simulation_replay_bulk

    get_run(run_id)
    result_holder: dict[str, Any] = {}

    def _apply(state: dict[str, Any]) -> None:
        _wire_context_prerequisites(state)
        state.setdefault("configuration", {})["phase6b_context_attempt_id"] = context_attempt_id
        result_holder["simulation_attempt_id"] = run_simulation_replay_bulk(
            state, context_attempt_id=context_attempt_id
        )
        result_holder["summary"] = dict(state.get("configuration", {}).get("simulation_summary") or {})

    _mutate_run(run_id, _apply)
    summary = result_holder.get("summary") or {}
    return {
        "run_id": run_id,
        "context_attempt_id": context_attempt_id,
        "simulation_attempt_id": result_holder.get("simulation_attempt_id"),
        **summary,
    }


def run_replay_start(run_id: str) -> RunDetail:
    from .replay_engine import replay_start

    get_run(run_id)

    def _apply(s: dict[str, Any]) -> None:
        replay_start(s)

    return _mutate_run(run_id, _apply)


def run_replay_pause(run_id: str) -> RunDetail:
    from .replay_engine import replay_pause

    get_run(run_id)

    def _apply(s: dict[str, Any]) -> None:
        replay_pause(s)

    return _mutate_run(run_id, _apply)


def run_replay_resume(run_id: str) -> RunDetail:
    from .replay_engine import replay_resume

    get_run(run_id)

    def _apply(s: dict[str, Any]) -> None:
        replay_resume(s)

    return _mutate_run(run_id, _apply)


def run_replay_stop(run_id: str) -> RunDetail:
    from .replay_engine import replay_stop

    get_run(run_id)

    def _apply(s: dict[str, Any]) -> None:
        replay_stop(s)

    return _mutate_run(run_id, _apply)


def run_next_bar(run_id: str, *, request_token: str | None = None) -> dict:
    from .replay_engine import process_next_bar

    get_run(run_id)
    with _lock:
        state = _runs[run_id]
    result = process_next_bar(state, request_token=request_token)
    _persist_run_header(state)
    return result


def get_replay_state(run_id: str) -> dict:
    from .replay_engine import get_replay_state as _state

    get_run(run_id)
    with _lock:
        return _state(_runs[run_id])


def get_visible_bars(run_id: str, symbol: str | None = None) -> dict:
    from .replay_engine import get_visible_bars as _bars

    get_run(run_id)
    with _lock:
        return _bars(_runs[run_id], symbol)


def list_rulesets() -> list[dict[str, str]]:
    from .pattern_ruleset_v01 import DEFAULT_PARAMETERS as PATTERN_PARAMS
    from .pattern_ruleset_v01 import RULESET_VERSION as PATTERN_RS

    return [
        {
            "ruleset_version": RULESET_VERSION,
            "description": "Objective bar geometry and Brooks vocabulary V0.1 (experiment operationalizations).",
            "tie_break_version": TIE_BREAK_VERSION_DEFAULT,
            "phase": "4_objective_observations",
            "parameters": DEFAULT_PARAMETERS,
        },
        {
            "ruleset_version": PATTERN_RS,
            "description": "Stateful Brooks pattern instances V0.1 (observation-only, builds on Phase 4 baseline).",
            "tie_break_version": TIE_BREAK_VERSION_DEFAULT,
            "phase": "5_pattern_engine",
            "parameters": PATTERN_PARAMS,
        },
        {
            "ruleset_version": "BROOKS_PATTERN_RULESET_V0_2",
            "description": "Phase 5B tuned pattern lifecycle, swings, double-bottoms, expiry (validation-driven).",
            "tie_break_version": TIE_BREAK_VERSION_DEFAULT,
            "phase": "5b_pattern_validation",
            "parameters": __import__(
                "app.brooks_intraday.pattern_ruleset_v02", fromlist=["DEFAULT_PARAMETERS"]
            ).DEFAULT_PARAMETERS,
        },
        {
            "ruleset_version": "BROOKS_PATTERN_RULESET_V0_3",
            "description": "Phase 5C double-bottom hierarchy, deduplication, symmetric micro channels.",
            "tie_break_version": TIE_BREAK_VERSION_DEFAULT,
            "phase": "5c_pattern_validation",
            "parameters": __import__(
                "app.brooks_intraday.pattern_ruleset_v03", fromlist=["DEFAULT_PARAMETERS"]
            ).DEFAULT_PARAMETERS,
        },
        {
            "ruleset_version": "BROOKS_SIMULATION_RULESET_V0_1",
            "description": "Phase 7 simulated portfolio (CONSIDER_ENTRY gating on context V0.2 only).",
            "tie_break_version": TIE_BREAK_VERSION_DEFAULT,
            "phase": "7_simulation_engine",
            "parameters": __import__(
                "app.brooks_intraday.simulation_ruleset_v01", fromlist=["DEFAULT_PARAMETERS"]
            ).DEFAULT_PARAMETERS,
        },
        {
            "ruleset_version": "BROOKS_CONTEXT_RULESET_V0_2",
            "description": "Phase 6B calibrated context (WAIT vs DO_NOT_ENTER, intraday vs daily invalidation).",
            "tie_break_version": TIE_BREAK_VERSION_DEFAULT,
            "phase": "6b_context_calibration",
            "parameters": __import__(
                "app.brooks_intraday.context_ruleset_v02", fromlist=["DEFAULT_PARAMETERS"]
            ).DEFAULT_PARAMETERS,
        },
        {
            "ruleset_version": "BROOKS_CONTEXT_RULESET_V0_1",
            "description": "Phase 6 PAA thesis alignment, advisory state machine (no simulated trades).",
            "tie_break_version": TIE_BREAK_VERSION_DEFAULT,
            "phase": "6_context_engine",
            "parameters": __import__(
                "app.brooks_intraday.context_ruleset_v01", fromlist=["DEFAULT_PARAMETERS"]
            ).DEFAULT_PARAMETERS,
        },
        {
            "ruleset_version": "BROOKS_REPLAY_TRANSPORT_V0_1",
            "description": "Phase 3 replay transport placeholders (legacy).",
            "tie_break_version": TIE_BREAK_VERSION_DEFAULT,
            "phase": "3_replay_engine",
        },
    ]


def run_prepare(run_id: str) -> dict[str, Any]:
    with _lock:
        state = _runs.get(run_id)
        if not state:
            raise KeyError(run_id)
        if state.get("dossiers_frozen"):
            return state.get("preparation") or {}
        state["preparation_status"] = "PREPARING"
        state["status"] = "PREPARING"

    preparation = prepare_run(state)
    with _lock:
        _runs[run_id] = state
        _persist_run_header(state)
    return preparation


def run_reconstruct_dossiers(run_id: str) -> dict:
    from .paa_reconstruction import reconstruct_all_for_run

    detail = get_run(run_id)
    if not detail:
        raise KeyError(run_id)
    with _lock:
        state = _runs[run_id]
    if state.get("bars_frozen") is False:
        logger.warning("brooks reconstruct dossiers while bars not marked frozen run_id=%s", run_id)
    result = reconstruct_all_for_run(state)
    with _lock:
        _runs[run_id] = state
        _persist_run_header(state)
    return result


def run_acquire(run_id: str, *, dry_run: bool = False, retry_failed: bool = False) -> dict:
    from .acquisition import acquire_historical_bars

    with _lock:
        state = _runs.get(run_id)
        if not state:
            raise KeyError(run_id)
    result = acquire_historical_bars(state, dry_run=dry_run, retry_failed_only=retry_failed)
    with _lock:
        _runs[run_id] = state
        _persist_run_header(state)
    return result


def get_frozen_dossier(run_id: str, symbol: str, trading_date: date) -> dict | None:
    row = get_dossier(run_id, symbol, trading_date)
    if row:
        return row.get("frozen_dossier_json")
    return None


def dossier_preview(symbol: str, trading_date: date) -> dict[str, Any]:
    """Phase 2 will compile frozen dossiers from PAA audit; shell returns availability probe only."""
    sym = symbol.strip().upper()
    selection = select_pre_rth_paa(sym, trading_date)
    return {
        "symbol": sym,
        "trading_date": trading_date.isoformat(),
        "available": selection.status == "SELECTED",
        "message": "Pre-RTH PAA audit record available." if selection.status == "SELECTED" else selection.status,
        "dossier_version": DOSSIER_VERSION_DEFAULT,
        "paa_analysis_id": selection.analysis_id,
        "kind": "source_preview",
    }
