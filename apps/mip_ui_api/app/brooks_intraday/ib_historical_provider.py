from __future__ import annotations

import hashlib
import json
import logging
import subprocess
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


from .bars import HistoricalBar, BAR_SIZE_MINUTES
from .calendar import NY_TZ
from .brooks_ib_connect import resolve_brooks_phase9_ib_connect
from app.services.ibkr_live_bars import (
    parse_json_payload,
    project_root,
)

UTC = timezone.utc

logger = logging.getLogger(__name__)


class IbHistoricalProviderError(Exception):
    def __init__(self, code: str, message: str, *, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


def fetch_session_bars_ib(
    symbol: str,
    trading_date: date,
    *,
    timeout_sec: int = 90,
) -> dict[str, Any]:
    """Read-only IB historical 5m RTH bars for one session."""
    root = project_root()
    py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "fetch_ibkr_historical_session_bars.py"
    if not py.exists() or not script.exists():
        raise IbHistoricalProviderError(
            "IB_RUNTIME_MISSING",
            "IBKR historical fetch runtime not found.",
        )

    conn = resolve_brooks_phase9_ib_connect()
    request_id = str(uuid.uuid4())
    cmd = [
        str(py),
        str(script),
        "--host",
        str(conn.get("host", "127.0.0.1")),
        "--port",
        str(int(conn.get("port", 7497))),
        "--client-id",
        str(int(conn.get("client_id", 9447))),
        "--symbol",
        symbol.upper(),
        "--trading-date",
        trading_date.isoformat(),
        "--interval-minutes",
        "5",
        "--use-rth",
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            cwd=str(root),
        )
    except subprocess.TimeoutExpired as exc:
        raise IbHistoricalProviderError(
            "IB_PACING_OR_TIMEOUT",
            "IB historical request timed out.",
            details={"request_id": request_id},
        ) from exc

    payload = parse_json_payload(proc.stdout or "", proc.stderr or "")
    payload["source_request_id"] = request_id
    payload["exit_code"] = proc.returncode
    if proc.returncode != 0 or payload.get("status") != "SUCCESS":
        err_text = payload.get("error") or (proc.stderr or "")[:500]
        code = "IB_PACING_OR_TIMEOUT" if "pacing" in err_text.lower() else "IB_HISTORICAL_FAILED"
        raise IbHistoricalProviderError(code, err_text or "IB historical fetch failed.", details=payload)
    return payload


def ib_payload_to_historical_bars(
    payload: dict[str, Any],
    trading_date: date,
) -> list[HistoricalBar]:
    sym = str(payload.get("symbol") or "").upper()
    source = "IBKR_HISTORICAL_5M_RTH_V0_1"
    bars: list[HistoricalBar] = []
    for row in payload.get("bars") or []:
        ts_raw = row.get("ts")
        if not ts_raw:
            continue
        ts_s = str(ts_raw).replace("Z", "")
        if " " in ts_s and "T" not in ts_s:
            ts_ny = datetime.fromisoformat(ts_s[:19])
        else:
            ts_ny = datetime.fromisoformat(ts_s[:19].replace("T", " "))
        if ts_ny.tzinfo:
            ts_ny = ts_ny.astimezone(NY_TZ).replace(tzinfo=None)
        if ts_ny.date() != trading_date:
            continue
        ts_utc = ts_ny.replace(tzinfo=NY_TZ).astimezone(UTC).replace(tzinfo=None)
        o, h, l, c = row.get("open"), row.get("high"), row.get("low"), row.get("close")
        if None in (o, h, l, c):
            continue
        bars.append(
            HistoricalBar(
                symbol=sym,
                ts_utc=ts_utc,
                ts_ny=ts_ny,
                trading_date=trading_date,
                open=float(o),
                high=float(h),
                low=float(l),
                close=float(c),
                volume=float(row["volume"]) if row.get("volume") is not None else None,
                source=source,
                bar_size_minutes=BAR_SIZE_MINUTES,
                rth=True,
            )
        )
    bars.sort(key=lambda b: b.ts_ny)
    return bars
