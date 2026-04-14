"""Shared IBKR subprocess live bar fetch for Symbol Tracker and Live Intelligence Cockpit."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.integrations.ibkr_read_host import (
    diagnostics_live_bars_subprocess_result,
    get_live_bars_subprocess_args,
)


def project_root() -> Path:
    path = Path(__file__).resolve()
    for parent in (path, *path.parents):
        if (parent / "cursorfiles").exists():
            return parent
    return path.parents[5]


def parse_json_payload(stdout: str, stderr: str) -> dict[str, Any]:
    for stream in (stdout or "", stderr or ""):
        idx = stream.find("{")
        if idx < 0:
            continue
        try:
            value = json.loads(stream[idx:])
            if isinstance(value, dict):
                return value
        except Exception:
            continue
    return {}


def normalize_ib_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if len(raw) == 6 and "/" not in raw and raw.isalpha():
        return f"{raw[:3]}/{raw[3:]}"
    return raw


def infer_ib_market_type(symbol: str, market_type: str | None) -> str:
    mt = str(market_type or "").upper().strip()
    if mt in {"FX", "CASH", "FOREX"}:
        return "FX"
    if "/" in str(symbol or ""):
        return "FX"
    return "STOCK"


def run_agent_ibkr_live_bars(
    symbol_specs: list[dict[str, str]],
    *,
    interval_minutes: int,
    window_bars: int,
    bar_seconds: int | None = None,
    timeout_sec: int = 60,
    diagnostics_surface: str = "living_charts",
) -> dict[str, Any]:
    root = project_root()
    py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "fetch_ibkr_live_bars.py"
    if not py.exists() or not script.exists():
        raise HTTPException(
            status_code=500,
            detail="IBKR live fetch runtime not found (cursorfiles venv/script missing).",
        )

    symbols: list[str] = []
    market_types: list[str] = []
    seen: set[tuple[str, str]] = set()
    for spec in symbol_specs:
        symbol = normalize_ib_symbol(spec.get("symbol") or "")
        if not symbol:
            continue
        mkt = infer_ib_market_type(symbol, spec.get("market_type"))
        key = (symbol, mkt)
        if key in seen:
            continue
        seen.add(key)
        symbols.append(symbol)
        market_types.append(mkt)

    if not symbols:
        diag = diagnostics_live_bars_subprocess_result(
            surface_name=diagnostics_surface,
            subprocess_ok=True,
            payload_status="SUCCESS",
        )
        return {"status": "SUCCESS", "symbols": [], "ib_host_diagnostics": diag}

    try:
        conn = get_live_bars_subprocess_args()
    except (ImportError, ModuleNotFoundError):
        conn = {"host": "127.0.0.1", "port": 7497, "client_id": 9436, "connect_timeout_sec": 10}

    cmd = [
        str(py),
        str(script),
        "--host",
        str(conn["host"]),
        "--port",
        str(int(conn["port"])),
        "--client-id",
        str(int(conn["client_id"])),
        "--connect-timeout-sec",
        str(int(conn.get("connect_timeout_sec") or 10)),
        "--symbols",
        ",".join(symbols),
        "--market-types",
        ",".join(market_types),
        "--window-bars",
        str(window_bars),
    ]
    if bar_seconds:
        cmd.extend(["--bar-seconds", str(int(bar_seconds)), "--use-rth"])
    else:
        cmd.extend(["--interval-minutes", str(interval_minutes)])
    proc = subprocess.run(
        cmd,
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=timeout_sec,
    )
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    payload = parse_json_payload(stdout, stderr)
    st = str((payload or {}).get("status") or "").upper() or None
    ok = proc.returncode == 0
    diag = diagnostics_live_bars_subprocess_result(
        surface_name=diagnostics_surface,
        subprocess_ok=ok,
        payload_status=st,
    )
    if proc.returncode != 0:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "IBKR live bar fetch failed.",
                "stdout": stdout[-2000:],
                "stderr": stderr[-2000:],
                "payload": payload or None,
                "ib_host_diagnostics": diag,
            },
        )
    out = payload or {"status": "SUCCESS", "symbols": []}
    out["ib_host_diagnostics"] = diag
    return out
