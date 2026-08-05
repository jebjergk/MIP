"""Phase 9 — IB connection preflight and session probe (read-only first)."""

from __future__ import annotations

import json
import logging
import re
import subprocess
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any

from .bars import validate_bars
from .calendar import resolve_week_sessions
from .constants import EXPECTED_RTH_5M_BARS_FULL_SESSION
from .historical_bar_repository import load_bars_from_store
from .ib_historical_provider import (
    IbHistoricalProviderError,
    fetch_session_bars_ib,
    ib_payload_to_historical_bars,
)
from .interval_validation import expected_interval_starts_ny, interval_sets_match
from .brooks_ib_connect import resolve_brooks_phase9_ib_connect
from app.services.ibkr_live_bars import parse_json_payload, project_root

logger = logging.getLogger(__name__)

PHASE2B_REFERENCE = {
    "source": "cursorfiles/brooks_phase2b_acquire_week.py",
    "provider_module": "app.brooks_intraday.ib_historical_provider.fetch_session_bars_ib",
    "subprocess_script": "cursorfiles/fetch_ibkr_historical_session_bars.py",
    "reported_successful_connection": {"host": "127.0.0.1", "port": 7496},
    "reported_historical_client_id": 9437,
    "ib_pace_sec": 12,
    "max_acquire_passes": 25,
    "contract": "Stock(symbol, SMART, USD)",
    "what_to_show": "TRADES",
    "bar_size": "5 mins",
    "duration": "1 D",
    "end_date_time_pattern": "YYYYMMDD 16:00:00 US/Eastern",
    "use_rth": True,
    "timestamp_convention": "INTERVAL_START_US_EASTERN",
    "timeout_sec": 90,
}


def phase9_effective_ib_config() -> dict[str, Any]:
    conn = resolve_brooks_phase9_ib_connect()
    historical_client_id = int(conn["historical_subprocess_client_id"])
    return {
        "provider_module": "app.brooks_intraday.ib_historical_provider",
        "subprocess_script": "cursorfiles/fetch_ibkr_historical_session_bars.py",
        "resolve_connect": "app.brooks_intraday.brooks_ib_connect.resolve_brooks_phase9_ib_connect()",
        "host": conn.get("host"),
        "port": int(conn.get("port", 7497)),
        "live_bars_client_id": int(conn.get("live_bars_client_id", 9436)),
        "historical_subprocess_client_id": historical_client_id,
        "connect_timeout_sec": conn.get("connect_timeout_sec", 10),
        "contract": "Stock(symbol, SMART, USD)",
        "what_to_show": "TRADES",
        "bar_size": "5 mins",
        "duration": "1 D",
        "end_date_time_pattern": "YYYYMMDD 16:00:00 US/Eastern",
        "use_rth": True,
        "interval_minutes": 5,
        "fetch_timeout_sec": 90,
        "intentional_client_id_offset": "Brooks Phase 9 uses BROOKS_PHASE9_IB_CLIENT_ID (default 9447), not live_bars+1",
    }


def compare_phase2b_phase9_config() -> dict[str, Any]:
    p9 = phase9_effective_ib_config()
    diffs: list[dict[str, str]] = []
    if p9["port"] != PHASE2B_REFERENCE["reported_successful_connection"]["port"]:
        diffs.append(
            {
                "field": "port",
                "phase2b_observed": str(PHASE2B_REFERENCE["reported_successful_connection"]["port"]),
                "phase9_current": str(p9["port"]),
                "note": "Phase 9 uses resolve_live_bars_connect (LIVE_PORTFOLIO_CONFIG gateway when configured).",
            }
        )
    if p9["historical_subprocess_client_id"] != PHASE2B_REFERENCE["reported_historical_client_id"]:
        diffs.append(
            {
                "field": "historical_client_id",
                "phase2b_observed": str(PHASE2B_REFERENCE["reported_historical_client_id"]),
                "phase9_current": str(p9["historical_subprocess_client_id"]),
                "note": "Brooks Phase 9 uses dedicated BROOKS_PHASE9_IB_CLIENT_ID (default 9447), not live_bars+1.",
            }
        )
    same_fields = [
        "provider_module",
        "subprocess_script",
        "contract",
        "what_to_show",
        "bar_size",
        "duration",
        "use_rth",
    ]
    for f in same_fields:
        p2 = PHASE2B_REFERENCE.get(f.replace("contract", "contract"), PHASE2B_REFERENCE.get("contract"))
        p9v = p9.get(f if f != "provider_module" else "provider_module")
        if f == "provider_module":
            p2 = PHASE2B_REFERENCE["provider_module"].split(".")[-1]
            p9v = "fetch_session_bars_ib"
    return {
        "phase2b_reference": PHASE2B_REFERENCE,
        "phase9_effective": p9,
        "differences": diffs,
        "unchanged_vs_phase2b_subprocess": [
            "Stock SMART USD",
            "5 mins",
            "1 D",
            "end 16:00 US/Eastern",
            "useRTH",
            "TRADES",
            "formatDate=1 interval-start NY",
        ],
    }


def _mask_accounts(accounts: list[str]) -> list[str]:
    out = []
    for a in accounts or []:
        s = str(a)
        if len(s) <= 4:
            out.append("****")
        else:
            out.append(f"{'*' * (len(s) - 4)}{s[-4:]}")
    return out


def _preflight_subprocess_paths() -> tuple[Any, Any, Any]:
    root = project_root()
    py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "ibkr_connection_preflight.py"
    return root, py, script


def run_ib_connection_preflight(*, probe_client_id: int | None = None) -> dict[str, Any]:
    """Connect to TWS/Gateway via isolated subprocess (no ib_insync in API worker thread)."""
    cfg = phase9_effective_ib_config()
    client_id = probe_client_id if probe_client_id is not None else cfg["historical_subprocess_client_id"]
    base: dict[str, Any] = {
        "host": cfg["host"],
        "port": cfg["port"],
        "client_id": client_id,
        "connected": False,
        "server_version": None,
        "managed_accounts_masked": [],
        "historical_data_capability": "unknown",
        "client_id_collision_suspected": False,
        "ib_error_codes": [],
        "ib_messages": [],
        "warnings": [],
        "execution_path": "subprocess",
    }
    _root, py, script = _preflight_subprocess_paths()
    if not py.exists() or not script.exists():
        base["ib_messages"].append("IB preflight runtime not found (cursorfiles/.venv or script missing).")
        base["error_class"] = "IB_RUNTIME_MISSING"
        return base

    cmd = [
        str(py),
        str(script),
        "--host",
        str(cfg["host"]),
        "--port",
        str(int(cfg["port"])),
        "--client-id",
        str(int(client_id)),
        "--connect-timeout-sec",
        str(int(cfg.get("connect_timeout_sec") or 15)),
    ]
    timeout_sec = int(cfg.get("fetch_timeout_sec") or 90)
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            cwd=str(_root),
        )
    except subprocess.TimeoutExpired as exc:
        base["ib_messages"].append("IB preflight subprocess timed out.")
        base["error_class"] = "PREFLIGHT_TIMEOUT"
        base["technical_detail"] = str(exc)
        return base
    except Exception as exc:
        base["ib_messages"].append(str(exc))
        base["error_class"] = type(exc).__name__
        return base

    payload = parse_json_payload(proc.stdout or "", proc.stderr or "")
    if not payload:
        tail = (proc.stderr or proc.stdout or "")[:500]
        base["ib_messages"].append(tail or "Empty preflight subprocess output.")
        base["error_class"] = "PREFLIGHT_PARSE_ERROR"
        base["exit_code"] = proc.returncode
        return base

    merged = {**base, **payload}
    merged.setdefault("managed_accounts_masked", _mask_accounts([]))
    if payload.get("managed_accounts") and not merged.get("managed_accounts_masked"):
        merged["managed_accounts_masked"] = _mask_accounts(list(payload.get("managed_accounts") or []))
    merged.pop("managed_accounts", None)
    if proc.returncode != 0 and payload.get("status") == "FAIL":
        merged.setdefault("connected", False)
    return merged


def analyze_session_probe(
    symbol: str,
    trading_date: date,
    payload: dict[str, Any],
    bars_historical: list,
) -> dict[str, Any]:
    week = resolve_week_sessions(trading_date)
    session = next((s for s in week.sessions if s.trading_date == trading_date), None)
    if not session:
        return {"error": "no_session_for_date"}
    val = validate_bars(bars_historical, session, source="IBKR_HISTORICAL_5M_RTH_V0_1")
    expected = expected_interval_starts_ny(trading_date)
    actual_ny = [b.ts_ny for b in bars_historical]
    ok, missing, extra = interval_sets_match(actual_ny, expected)
    raw_count = len(payload.get("bars") or [])
    ts_list = [str(b.get("ts")) for b in (payload.get("bars") or []) if b.get("ts")]
    dupes = len(ts_list) - len(set(ts_list))
    first_ts = ts_list[0] if ts_list else None
    last_ts = ts_list[-1] if ts_list else None
    return {
        "symbol": symbol.upper(),
        "trading_date": trading_date.isoformat(),
        "raw_bars_returned": raw_count,
        "filtered_rth_bars": len(bars_historical),
        "expected_bars": EXPECTED_RTH_5M_BARS_FULL_SESSION,
        "exactly_78": len(bars_historical) == EXPECTED_RTH_5M_BARS_FULL_SESSION and val.status == "COMPLETE",
        "validation_status": val.status,
        "validation_messages": val.messages,
        "first_timestamp": first_ts,
        "last_timestamp": last_ts,
        "expected_first_ny": "09:30",
        "expected_last_ny": "15:55",
        "duplicates_in_raw": dupes,
        "missing_expected_timestamps": missing[:20],
        "missing_count": len(missing),
        "extra_timestamps": extra[:10],
        "interval_match": ok,
        "payload_status": payload.get("status"),
        "payload_error": payload.get("error"),
        "exit_code": payload.get("exit_code"),
        "request_duration_note": payload.get("fetched_at_utc"),
    }


def probe_session_readonly(symbol: str, trading_date: date) -> dict[str, Any]:
    """Fetch IB bars without Snowflake persistence."""
    t0 = time.perf_counter()
    out: dict[str, Any] = {"persisted": False}
    try:
        payload = fetch_session_bars_ib(symbol, trading_date)
        out["fetch_ok"] = True
        out["payload_summary"] = {
            "status": payload.get("status"),
            "bar_count": payload.get("bar_count"),
            "source_request_id": payload.get("source_request_id"),
        }
        bars = ib_payload_to_historical_bars(payload, trading_date)
        out["analysis"] = analyze_session_probe(symbol, trading_date, payload, bars)
        out["duration_sec"] = round(time.perf_counter() - t0, 3)
    except IbHistoricalProviderError as exc:
        out["fetch_ok"] = False
        out["duration_sec"] = round(time.perf_counter() - t0, 3)
        out["ib_error"] = {"code": exc.code, "message": exc.message, "details": exc.details}
    return out


def verify_persisted_session(symbol: str, trading_date: date) -> dict[str, Any]:
    week = resolve_week_sessions(trading_date)
    session = next(s for s in week.sessions if s.trading_date == trading_date)
    bars = load_bars_from_store(symbol, trading_date)
    val = validate_bars(bars, session, source=bars[0].source if bars else "STORED")
    unique_ts = len({str(b.ts_utc)[:19] for b in bars})
    return {
        "symbol": symbol.upper(),
        "trading_date": trading_date.isoformat(),
        "stored_bar_count": len(bars),
        "unique_timestamps": unique_ts,
        "validation_status": val.status,
        "dataset_hash": val.dataset_hash,
        "first_bar_ts": val.first_bar_ts,
        "last_bar_ts": val.last_bar_ts,
        "exactly_78_unique": unique_ts == EXPECTED_RTH_5M_BARS_FULL_SESSION and val.status == "COMPLETE",
    }
