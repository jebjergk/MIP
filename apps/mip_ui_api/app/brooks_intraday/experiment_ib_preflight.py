"""Phase 9 — IB connection preflight and session probe (read-only first)."""

from __future__ import annotations

import json
import logging
import re
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
from app.services.ibkr_live_bars import resolve_live_bars_connect

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
    conn = resolve_live_bars_connect(portfolio_id=None)
    historical_client_id = int(conn.get("client_id", 9437)) + 1
    return {
        "provider_module": "app.brooks_intraday.ib_historical_provider",
        "subprocess_script": "cursorfiles/fetch_ibkr_historical_session_bars.py",
        "resolve_connect": "app.services.ibkr_live_bars.resolve_live_bars_connect(None)",
        "host": conn.get("host"),
        "port": int(conn.get("port", 7497)),
        "live_bars_client_id": int(conn.get("client_id", 9436)),
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
        "intentional_client_id_offset": "historical uses live_bars_client_id + 1 (same as Phase 2B path)",
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
                "note": "Derived from ibkr_host_config live_bars id + 1; may differ if env changed.",
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


def run_ib_connection_preflight(*, probe_client_id: int | None = None) -> dict[str, Any]:
    """Connect to TWS/Gateway and report capability (no bar persistence)."""
    cfg = phase9_effective_ib_config()
    client_id = probe_client_id if probe_client_id is not None else cfg["historical_subprocess_client_id"]
    result: dict[str, Any] = {
        "host": cfg["host"],
        "port": cfg["port"],
        "client_id": client_id,
        "connected": False,
        "server_version": None,
        "connection_time": None,
        "managed_accounts_masked": [],
        "historical_data_capability": "unknown",
        "client_id_collision_suspected": False,
        "ib_error_codes": [],
        "ib_messages": [],
        "warnings": [],
    }
    try:
        from ib_insync import IB, Stock  # type: ignore
    except ImportError as exc:
        result["ib_messages"].append(f"ib_insync not available: {exc}")
        return result

    ib = IB()
    t0 = time.perf_counter()
    try:
        ib.connect(
            host=str(cfg["host"]),
            port=int(cfg["port"]),
            clientId=int(client_id),
            readonly=True,
            timeout=int(cfg.get("connect_timeout_sec") or 15),
        )
        result["connected"] = True
        result["connection_time_sec"] = round(time.perf_counter() - t0, 3)
        sv = getattr(ib, "serverVersion", None)
        if callable(sv):
            result["server_version"] = sv()
        elif ib.client is not None:
            csv = getattr(ib.client, "serverVersion", None)
            result["server_version"] = csv() if callable(csv) else csv
        elif sv is not None:
            result["server_version"] = sv
        accounts = list(ib.managedAccounts() or [])
        result["managed_accounts_masked"] = _mask_accounts(accounts)
        result["managed_accounts_count"] = len(accounts)
        contract = Stock("AAPL", "SMART", "USD")
        ib.qualifyContracts(contract)
        result["historical_data_capability"] = "contract_qualified"
        result["qualified_contract"] = {
            "symbol": contract.symbol,
            "secType": contract.secType,
            "exchange": contract.exchange,
            "currency": contract.currency,
            "primaryExchange": getattr(contract, "primaryExchange", None),
        }
    except Exception as exc:
        msg = str(exc)
        result["ib_messages"].append(msg)
        result["error_class"] = type(exc).__name__
        code_match = re.search(r"error\s*(\d+)", msg, re.I)
        if code_match:
            result["ib_error_codes"].append(int(code_match.group(1)))
        if "326" in msg or "already in use" in msg.lower() or "Peer closed" in msg:
            result["client_id_collision_suspected"] = True
        if "502" in msg or "Couldn't connect" in msg:
            result["connection_refused"] = True
    finally:
        if ib.isConnected():
            ib.disconnect()
    return result


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
