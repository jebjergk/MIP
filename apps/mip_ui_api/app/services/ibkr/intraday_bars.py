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


@dataclass(frozen=True)
class LiveQuote:
    """Live tick snapshot from IBKR's reqMktData (snapshot=True).

    `best` is the consumer-friendly single number to render — it prefers
    last trade, falls back to bid/ask mid, then ib_insync's marketPrice,
    then the session close. Any of these may be None if the snapshot
    populated only partially.
    """
    best: Optional[float]
    last: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    mid: Optional[float] = None
    market_price: Optional[float] = None
    session_close: Optional[float] = None
    quote_ts: Optional[str] = None


@dataclass
class SymbolBars:
    symbol: str
    status: str  # SUCCESS | FAILED | EMPTY
    bars: List[IntradayBar] = field(default_factory=list)
    current_price: Optional[float] = None
    live_quote: Optional[LiveQuote] = None
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


def _parse_live_quote(raw: Any) -> Optional[LiveQuote]:
    """Convert the cursorfiles script's `live_quote` dict into a LiveQuote."""
    if not isinstance(raw, dict):
        return None
    best = raw.get("best")
    if best is None:
        # Try to derive a best from the remaining fields if the script
        # did not populate `best` (defensive against script revisions).
        for key in ("last", "mid", "market_price", "session_close"):
            v = raw.get(key)
            if v is not None:
                best = v
                break
    if best is None:
        return None
    return LiveQuote(
        best=_safe_float(best),
        last=_safe_float(raw.get("last")),
        bid=_safe_float(raw.get("bid")),
        ask=_safe_float(raw.get("ask")),
        mid=_safe_float(raw.get("mid")),
        market_price=_safe_float(raw.get("market_price")),
        session_close=_safe_float(raw.get("session_close")),
        quote_ts=str(raw.get("quote_ts")) if raw.get("quote_ts") is not None else None,
    )


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def fetch_today_intraday_bars(
    symbols: List[str],
    *,
    interval_minutes: int = 15,
    window_bars: int = 80,
    session_date: Optional[date] = None,
    timeout_sec: int = 30,
    diagnostics_surface: str = "live_intelligence",
    include_live_quote: bool = False,
    snapshot_wait_sec: float = 2.5,
) -> IntradayBarsResult:
    """
    Generic intraday bar fetch with optional live tick snapshot.

    Identical fail-soft contract as `fetch_today_15m_bars`. When
    `include_live_quote=True`, each successful per-symbol entry gets a
    populated `live_quote` (or None if the snapshot did not arrive in
    time / the symbol has no current quote).
    """
    return _fetch_intraday(
        symbols,
        interval_minutes=interval_minutes,
        window_bars=window_bars,
        session_date=session_date,
        timeout_sec=timeout_sec,
        diagnostics_surface=diagnostics_surface,
        include_live_quote=include_live_quote,
        snapshot_wait_sec=snapshot_wait_sec,
    )


def fetch_today_15m_bars(
    symbols: List[str],
    *,
    session_date: Optional[date] = None,
    timeout_sec: int = 25,
    diagnostics_surface: str = "live_intelligence",
) -> IntradayBarsResult:
    """
    Fetch today's 15-minute RTH bars for the given symbols (no live
    tick). Existing intraday-overlay callers use this entry point.

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
    return _fetch_intraday(
        symbols,
        interval_minutes=15,
        window_bars=80,
        session_date=session_date,
        timeout_sec=timeout_sec,
        diagnostics_surface=diagnostics_surface,
        include_live_quote=False,
        snapshot_wait_sec=0.0,
    )


def _fetch_intraday(
    symbols: List[str],
    *,
    interval_minutes: int,
    window_bars: int,
    session_date: Optional[date],
    timeout_sec: int,
    diagnostics_surface: str,
    include_live_quote: bool,
    snapshot_wait_sec: float,
) -> IntradayBarsResult:
    fetched_at_utc = datetime.now(timezone.utc).isoformat()
    target_date = session_date or datetime.now(timezone.utc).date()

    if not symbols:
        return IntradayBarsResult(
            status="OK",
            fetched_at_utc=fetched_at_utc,
            symbols={},
        )

    try:
        from app.services.ibkr_live_bars import run_agent_ibkr_live_bars
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("intraday_bars: subprocess wrapper import failed: %s", exc)
        return IntradayBarsResult(
            status="UNAVAILABLE",
            fetched_at_utc=fetched_at_utc,
            symbols={s: SymbolBars(symbol=s, status="FAILED", error="bar_fetch_runtime_missing") for s in symbols},
            error=f"runtime_missing: {exc}",
        )

    symbol_specs = [{"symbol": s, "market_type": "STOCK"} for s in symbols]

    try:
        payload = run_agent_ibkr_live_bars(
            symbol_specs,
            interval_minutes=interval_minutes,
            window_bars=window_bars,
            timeout_sec=timeout_sec,
            diagnostics_surface=diagnostics_surface,
            regular_trading_hours_only=True,
            include_snapshot_quote=include_live_quote,
            snapshot_wait_sec=snapshot_wait_sec,
        )
    except Exception as exc:
        msg = _extract_subprocess_error(exc)
        # WARNING-level so default log config surfaces *why* the cockpit
        # fell back to "Live unavailable" without needing DEBUG turned on.
        logger.warning(
            "intraday_bars: fetch unavailable for %s (surface=%s): %s",
            symbols, diagnostics_surface, msg,
        )
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
        live_quote = _parse_live_quote(row.get("live_quote")) if include_live_quote else None
        if row_status == "SUCCESS":
            today_bars = _filter_to_today(row.get("bars") or [], target_date)
            current_price = today_bars[-1].close if today_bars else row.get("current_price")
            out[sym] = SymbolBars(
                symbol=sym,
                status="SUCCESS" if today_bars else "EMPTY",
                bars=today_bars,
                current_price=current_price,
                live_quote=live_quote,
            )
            if today_bars:
                any_success = True
            else:
                any_failure = True
        else:
            any_failure = True
            out[sym] = SymbolBars(
                symbol=sym,
                status="FAILED",
                error=str(row.get("error") or "fetch_failed"),
                live_quote=live_quote,
            )

    if any_success and not any_failure:
        top_status = "OK"
    elif any_success and any_failure:
        top_status = "PARTIAL"
    else:
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
