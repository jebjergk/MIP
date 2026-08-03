"""Shared IBKR subprocess live bar fetch for Symbol Tracker and Live Intelligence Cockpit."""

from __future__ import annotations

import json
import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.integrations.ibkr_read_host import (
    diagnostics_live_bars_subprocess_result,
    get_live_bars_subprocess_args,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ConnectEndpoint:
    host: str
    port: int
    client_id: int


def project_root() -> Path:
    """Repo root containing cursorfiles (prefer tree with cursorfiles/.venv)."""
    path = Path(__file__).resolve()
    cursorfile_roots: list[Path] = []
    venv_roots: list[Path] = []
    for parent in path.parents:
        cf = parent / "cursorfiles"
        if not cf.is_dir():
            continue
        cursorfile_roots.append(parent)
        if (cf / ".venv").is_dir():
            venv_roots.append(parent)
    if venv_roots:
        return venv_roots[0]
    if cursorfile_roots:
        return cursorfile_roots[-1]
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


def resolve_live_bars_connect(portfolio_id: int | None = None) -> dict[str, Any]:
    """
    IB socket target for live-bar subprocess fetches.

    Matches live.py bar refresh: env defaults via ibkr_host_config, overridden by
    LIVE_PORTFOLIO_CONFIG host/port when portfolio_id is set (real-money gateway
    is often 7496 while .env IB_API_PORT stays 7497 for paper/TWS).
    Client id stays the dedicated live-bars id (9436) unless env overrides.

    When portfolio_id is omitted (portfolio-agnostic market data), prefer the
    first active LIVE gateway from LIVE_PORTFOLIO_CONFIG if one is configured.
    Symbol-level bars do not depend on which portfolio is being analysed.
    """
    try:
        conn = dict(get_live_bars_subprocess_args())
    except (ImportError, ModuleNotFoundError):
        conn = {
            "host": "127.0.0.1",
            "port": 7497,
            "client_id": 9436,
            "connect_timeout_sec": 10,
        }

    if portfolio_id is not None:
        try:
            from app.db import fetch_all, get_connection

            db = get_connection()
            try:
                cur = db.cursor()
                cur.execute(
                    """
                    SELECT IB_GATEWAY_HOST, IB_GATEWAY_PORT
                      FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                     WHERE PORTFOLIO_ID = %s
                    """,
                    (int(portfolio_id),),
                )
                rows = fetch_all(cur)
            finally:
                db.close()
            if rows:
                row = rows[0]
                if row.get("IB_GATEWAY_HOST"):
                    conn["host"] = str(row["IB_GATEWAY_HOST"])
                if row.get("IB_GATEWAY_PORT") is not None:
                    conn["port"] = int(row["IB_GATEWAY_PORT"])
        except Exception as exc:
            logger.warning(
                "resolve_live_bars_connect: portfolio %s lookup failed: %s",
                portfolio_id,
                exc,
            )
        return conn

    gateway = _preferred_market_data_gateway()
    if gateway:
        conn["host"] = gateway["host"]
        conn["port"] = gateway["port"]
    return conn


def _preferred_market_data_gateway() -> dict[str, Any] | None:
    """First active IB gateway for portfolio-agnostic symbol bar reads."""
    try:
        from app.db import fetch_all, get_connection

        db = get_connection()
        try:
            cur = db.cursor()
            cur.execute(
                """
                SELECT IB_GATEWAY_HOST, IB_GATEWAY_PORT, ADAPTER_MODE
                  FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                 WHERE IS_ACTIVE = TRUE
                   AND IB_GATEWAY_PORT IS NOT NULL
                 ORDER BY
                   CASE WHEN UPPER(COALESCE(ADAPTER_MODE, '')) = 'LIVE' THEN 0 ELSE 1 END,
                   PORTFOLIO_ID
                 LIMIT 1
                """,
            )
            rows = fetch_all(cur)
        finally:
            db.close()
    except Exception as exc:
        logger.warning("resolve_live_bars_connect: market-data gateway lookup failed: %s", exc)
        return None

    if not rows:
        return None
    row = rows[0]
    return {
        "host": str(row.get("IB_GATEWAY_HOST") or "127.0.0.1"),
        "port": int(row["IB_GATEWAY_PORT"]),
        "adapter_mode": row.get("ADAPTER_MODE"),
    }


def run_agent_ibkr_live_bars(
    symbol_specs: list[dict[str, str]],
    *,
    interval_minutes: int,
    window_bars: int,
    bar_seconds: int | None = None,
    timeout_sec: int = 60,
    diagnostics_surface: str = "living_charts",
    regular_trading_hours_only: bool = False,
    include_snapshot_quote: bool = False,
    snapshot_wait_sec: float = 2.5,
    portfolio_id: int | None = None,
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

    conn = resolve_live_bars_connect(portfolio_id)

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
        # Sub-minute path: keep prior default (RTH-only) unless callers opt into extended via flag later.
        cmd.extend(["--bar-seconds", str(int(bar_seconds)), "--use-rth"])
    else:
        cmd.extend(["--interval-minutes", str(interval_minutes)])
        if regular_trading_hours_only:
            cmd.append("--use-rth")
    if include_snapshot_quote:
        cmd.extend([
            "--include-snapshot-quote",
            "--snapshot-wait-sec",
            f"{float(snapshot_wait_sec):.2f}",
        ])
    child_env = dict(os.environ)
    for key in list(child_env.keys()):
        if key.startswith("SNOWFLAKE_"):
            child_env.pop(key, None)

    proc = subprocess.run(
        cmd,
        cwd=str(root),
        env=child_env,
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
        endpoint=_ConnectEndpoint(
            str(conn["host"]),
            int(conn["port"]),
            int(conn["client_id"]),
        ),
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
                "ib_connect": {
                    "host": conn["host"],
                    "port": int(conn["port"]),
                    "client_id": int(conn["client_id"]),
                    "portfolio_id": portfolio_id,
                },
            },
        )
    out = payload or {"status": "SUCCESS", "symbols": []}
    out["ib_host_diagnostics"] = diag
    out["ib_connect"] = {
        "host": conn["host"],
        "port": int(conn["port"]),
        "client_id": int(conn["client_id"]),
        "portfolio_id": portfolio_id,
    }
    return out
