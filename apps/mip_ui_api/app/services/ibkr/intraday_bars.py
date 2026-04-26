"""
Importable IBKR intraday-bar fetch service for cockpit/operator surfaces.

Why this module exists
----------------------
The cockpit's intraday open-position overlay needs today's 15-minute IBKR
bars for *only the currently-open live positions*. Building subprocess
command-lines for `cursorfiles/fetch_ibkr_live_bars.py` ad-hoc inside
router code would (a) leak bar-fetch concerns into router code and (b)
make fail-soft handling inconsistent across surfaces.

This module exposes one clean entry point — `fetch_today_15m_bars` —
that:

  * delegates to the existing subprocess wrapper
    `app.services.ibkr_live_bars.run_agent_ibkr_live_bars`, which keeps
    `ib_insync` isolated in the cursorfiles venv (the API venv does not
    bundle `ib_insync`),
  * filters to **today's RTH session only** (no multi-day history),
  * **never raises** on TWS-down / sparse-data / timeout — it returns a
    structured result with `status` ∈ {OK, PARTIAL, UNAVAILABLE} so the
    cockpit endpoint never 500s on broker connectivity issues.

The `cursorfiles/fetch_ibkr_live_bars.py` script remains as a CLI / debug
wrapper (and is what this service shells out to today). If we ever bring
`ib_insync` into the API venv, only this module needs to change.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# --- Public types ----------------------------------------------------------


@dataclass(frozen=True)
class IntradayBar:
    ts: str
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[float]


@dataclass
class SymbolBars:
    symbol: str
    status: str  # SUCCESS | FAILED | EMPTY
    bars: List[IntradayBar] = field(default_factory=list)
    current_price: Optional[float] = None
    error: Optional[str] = None


@dataclass
class IntradayBarsResult:
    """
    Top-level result. `status` summarises overall fetch health:
      * OK         — every requested symbol returned at least one bar.
      * PARTIAL    — fetch ran, but some symbols are missing or empty.
      * UNAVAILABLE — fetch could not run at all (TWS down, timeout,
                      runtime missing, hard error). All per-symbol
                      entries will be FAILED.

    `MARKET_CLOSED` is *not* signalled here — that is determined by the
    overlay layer based on whether today's session has produced any bars.
    Empty bars on a weekend simply produce status=PARTIAL and per-symbol
    EMPTY entries; the overlay maps that to MARKET_CLOSED.
    """

    status: str  # OK | PARTIAL | UNAVAILABLE
    fetched_at_utc: str
    symbols: Dict[str, SymbolBars]
    error: Optional[str] = None


# --- Implementation --------------------------------------------------------


def _filter_to_today(bars: List[Dict[str, Any]], session_date: date) -> List[IntradayBar]:
    """Keep only bars whose date matches today (NY-time approximated by
    naive comparison; IBKR's 15m bars carry local-session timestamps)."""
    out: List[IntradayBar] = []
    target_str = session_date.isoformat()
    for raw in bars or []:
        ts = raw.get("ts")
        if not ts:
            continue
        # IBKR ts looks like '2026-04-25 09:30:00' or ISO; compare date prefix.
        try:
            ts_str = str(ts)
            if ts_str[:10] != target_str:
                continue
            out.append(
                IntradayBar(
                    ts=ts_str,
                    open=raw.get("open"),
                    high=raw.get("high"),
                    low=raw.get("low"),
                    close=raw.get("close"),
                    volume=raw.get("volume"),
                )
            )
        except Exception:
            continue
    return out


def fetch_today_15m_bars(
    symbols: List[str],
    *,
    session_date: Optional[date] = None,
    timeout_sec: int = 25,
    diagnostics_surface: str = "live_intelligence",
) -> IntradayBarsResult:
    """
    Fetch today's 15-minute RTH bars for the given symbols.

    Fail-soft contract:
      * Any exception below is caught and surfaced as
        IntradayBarsResult(status='UNAVAILABLE', error=...).
      * Partial per-symbol failures inside the subprocess produce
        per-symbol FAILED entries and a top-level status='PARTIAL'.
      * Empty bar arrays (e.g. weekend) produce per-symbol EMPTY entries
        and a top-level status='PARTIAL'. The overlay layer translates
        an all-EMPTY result into MARKET_CLOSED.

    Parameters
    ----------
    symbols : list[str]
        Currently-open live symbols (typically 0-15 STOCKs). Empty list
        is allowed and short-circuits to status='OK', symbols={}.
    session_date : date | None
        The trading day to filter to. Defaults to today (UTC date — IBKR
        15m bar dates align with the local exchange session, so this
        works for US equities during normal operating hours).
    timeout_sec : int
        Subprocess timeout. Default 25s. The cockpit endpoint expects
        sub-30s response times.
    diagnostics_surface : str
        Surface name for IB host diagnostics templates.
    """
    # Local imports to keep top-of-module side effects minimal.
    fetched_at_utc = datetime.now(timezone.utc).isoformat()
    target_date = session_date or datetime.now(timezone.utc).date()

    if not symbols:
        return IntradayBarsResult(
            status="OK",
            fetched_at_utc=fetched_at_utc,
            symbols={},
        )

    try:
        # Imported lazily so a missing dependency in the subprocess wrapper
        # cannot crash module import time.
        from app.services.ibkr_live_bars import run_agent_ibkr_live_bars
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("intraday_bars: subprocess wrapper import failed: %s", exc)
        return IntradayBarsResult(
            status="UNAVAILABLE",
            fetched_at_utc=fetched_at_utc,
            symbols={s: SymbolBars(symbol=s, status="FAILED", error="bar_fetch_runtime_missing") for s in symbols},
            error=f"runtime_missing: {exc}",
        )

    # Build per-symbol specs. All cockpit positions are STOCK (V_LIVE_OPEN_POSITIONS
    # hard-codes MARKET_TYPE='STOCK' today), but we still pass market_type
    # so the subprocess script normalises correctly for any FX positions
    # added later.
    symbol_specs = [{"symbol": s, "market_type": "STOCK"} for s in symbols]

    try:
        payload = run_agent_ibkr_live_bars(
            symbol_specs,
            interval_minutes=15,
            window_bars=80,  # 80 * 15min = 20h, comfortably covers a full RTH session.
            timeout_sec=timeout_sec,
            diagnostics_surface=diagnostics_surface,
            regular_trading_hours_only=True,
        )
    except Exception as exc:
        # run_agent_ibkr_live_bars raises HTTPException on subprocess
        # failure. The cockpit must NOT propagate that — fail soft.
        msg = _extract_subprocess_error(exc)
        logger.info("intraday_bars: fetch unavailable (%s)", msg)
        return IntradayBarsResult(
            status="UNAVAILABLE",
            fetched_at_utc=fetched_at_utc,
            symbols={s: SymbolBars(symbol=s, status="FAILED", error=msg) for s in symbols},
            error=msg,
        )

    raw_status = str((payload or {}).get("status") or "").upper()
    raw_symbols = (payload or {}).get("symbols") or []
    if raw_status == "FAIL":
        err = str((payload or {}).get("error") or "fetch_fail")
        return IntradayBarsResult(
            status="UNAVAILABLE",
            fetched_at_utc=fetched_at_utc,
            symbols={s: SymbolBars(symbol=s, status="FAILED", error=err) for s in symbols},
            error=err,
        )

    out: Dict[str, SymbolBars] = {s: SymbolBars(symbol=s, status="EMPTY") for s in symbols}
    any_success = False
    any_failure = False
    for row in raw_symbols:
        sym = str(row.get("symbol") or "").strip().upper()
        if not sym:
            continue
        row_status = str(row.get("status") or "").upper()
        if row_status == "SUCCESS":
            today_bars = _filter_to_today(row.get("bars") or [], target_date)
            current_price = today_bars[-1].close if today_bars else row.get("current_price")
            out[sym] = SymbolBars(
                symbol=sym,
                status="SUCCESS" if today_bars else "EMPTY",
                bars=today_bars,
                current_price=current_price,
            )
            if today_bars:
                any_success = True
            else:
                any_failure = True  # got bars but none for today → treat as missing
        else:
            any_failure = True
            out[sym] = SymbolBars(
                symbol=sym,
                status="FAILED",
                error=str(row.get("error") or "fetch_failed"),
            )

    if any_success and not any_failure:
        top_status = "OK"
    elif any_success and any_failure:
        top_status = "PARTIAL"
    else:
        # No symbol returned a bar for today. This is the weekend / pre-open path.
        top_status = "PARTIAL"

    return IntradayBarsResult(
        status=top_status,
        fetched_at_utc=fetched_at_utc,
        symbols=out,
    )


def _extract_subprocess_error(exc: Exception) -> str:
    """Best-effort extraction of a short error string from HTTPException
    raised by run_agent_ibkr_live_bars."""
    try:
        detail = getattr(exc, "detail", None)
        if isinstance(detail, dict):
            msg = detail.get("message") or detail.get("error")
            if msg:
                return str(msg)
            stderr = (detail.get("stderr") or "").strip()
            if stderr:
                return stderr.splitlines()[-1][:200]
        if detail:
            return str(detail)[:200]
    except Exception:
        pass
    return str(exc)[:200]
