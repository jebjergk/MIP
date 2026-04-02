"""Single Snowflake flow for Live Intelligence Cockpit page load."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from app.db import fetch_all, get_connection
from app.routers.symbol_tracker import _in_placeholders, _to_float, assemble_symbol_tracker_tiles
from app.services.live_intelligence.entry_lifecycle_ui import (
    build_operator_entry_lifecycle,
    fetch_entry_lifecycle_rows,
)
from app.services.live_intelligence.lifecycle_reconciliation_v1 import run_lifecycle_reconciliation

BOOTSTRAP_VERSION = "1.1.0"
_MAX_ANALOG_GLOBAL = 2500
_MAX_ANALOG_PER_SYMBOL = 400

_log = logging.getLogger(__name__)


def _fetch_analog_episodes(cur, symbols: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not symbols:
        return {}
    symbol_params = list(dict.fromkeys(s.upper() for s in symbols if s))
    if not symbol_params:
        return {}
    ph = _in_placeholders(symbol_params)
    cur.execute(
        f"""
        select
          upper(r.SYMBOL) as SYMBOL,
          r.MARKET_TYPE,
          o.HORIZON_BARS,
          o.REALIZED_RETURN,
          o.HIT_FLAG,
          o.DIRECTION,
          o.ENTRY_PRICE,
          o.EXIT_PRICE,
          o.ENTRY_TS,
          o.EXIT_TS,
          o.CALCULATED_AT
        from MIP.APP.RECOMMENDATION_OUTCOMES o
        join MIP.APP.RECOMMENDATION_LOG r
          on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
        where r.INTERVAL_MINUTES = 1440
          and upper(r.SYMBOL) in ({ph})
        order by o.CALCULATED_AT desc nulls last
        limit {_MAX_ANALOG_GLOBAL}
        """,
        [*symbol_params],
    )
    rows = fetch_all(cur)
    by_sym: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        sym = str(row.get("SYMBOL") or "").upper()
        if not sym:
            continue
        if len(by_sym[sym]) >= _MAX_ANALOG_PER_SYMBOL:
            continue
        ret = _to_float(row.get("REALIZED_RETURN"))
        by_sym[sym].append(
            {
                "symbol": sym,
                "market_type": row.get("MARKET_TYPE"),
                "horizon_bars": int(row.get("HORIZON_BARS") or 0),
                "realized_return": ret,
                "hit_flag": row.get("HIT_FLAG"),
                "direction": row.get("DIRECTION"),
                "entry_price": _to_float(row.get("ENTRY_PRICE")),
                "exit_price": _to_float(row.get("EXIT_PRICE")),
                "feature_vol_proxy": abs(ret) if ret is not None else 0.0,
                "outcome_winner": bool(row.get("HIT_FLAG") is True),
            }
        )
    return dict(by_sym)


def _aligned_closes(series_a: list[dict], series_b: list[dict]) -> tuple[list[float], list[float]]:
    """Match bars by ts string; return paired closes."""
    map_b = {b.get("ts"): _to_float(b.get("close")) for b in series_b if b.get("ts")}
    ax, bx = [], []
    for bar in series_a:
        ts = bar.get("ts")
        ca = _to_float(bar.get("close"))
        cb = map_b.get(ts) if ts else None
        if ca is not None and cb is not None:
            ax.append(ca)
            bx.append(cb)
    return ax, bx


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 5:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    denx = sum((x - mx) ** 2 for x in xs) ** 0.5
    deny = sum((y - my) ** 2 for y in ys) ** 0.5
    if denx == 0 or deny == 0:
        return None
    return num / (denx * deny)


def _active_live_portfolio(cur) -> tuple[int | None, str | None]:
    cur.execute(
        """
        select PORTFOLIO_ID, IBKR_ACCOUNT_ID
        from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
        where coalesce(IS_ACTIVE, true) = true
        order by PORTFOLIO_ID
        limit 1
        """
    )
    rows = fetch_all(cur)
    if not rows:
        return None, None
    r = rows[0]
    try:
        pid = int(r.get("PORTFOLIO_ID"))
    except (TypeError, ValueError):
        return None, None
    acc = r.get("IBKR_ACCOUNT_ID") or r.get("ibkr_account_id")
    return pid, (str(acc).strip() if acc else None)


def _active_portfolio_id(cur) -> int | None:
    pid, _acc = _active_live_portfolio(cur)
    return pid


def _portfolio_context(tiles: list[dict[str, Any]]) -> dict[str, Any]:
    symbols = [str(t.get("symbol") or "").upper() for t in tiles if t.get("symbol")]
    gross_notional = 0.0
    by_sec: dict[str, float] = defaultdict(float)
    for t in tiles:
        q = abs(_to_float(t.get("quantity")) or 0)
        px = _to_float(t.get("current_price")) or 0
        n = q * px
        gross_notional += n
        sec = str(t.get("security_type") or "UNK")
        by_sec[sec] += n

    bars_by = {
        str(t.get("symbol") or "").upper(): (t.get("chart") or {}).get("bars") or []
        for t in tiles
        if t.get("symbol")
    }
    corr: dict[str, dict[str, float | None]] = {}
    syms = [s for s in symbols if bars_by.get(s)]
    for i, a in enumerate(syms):
        corr[a] = {}
        for b in syms[i + 1 :]:
            xa, xb = _aligned_closes(bars_by[a], bars_by[b])
            p = _pearson(xa, xb)
            corr[a][b] = p

    return {
        "symbols": symbols,
        "gross_notional_usd": round(gross_notional, 2),
        "notional_by_security_type": {k: round(v, 2) for k, v in by_sec.items()},
        "pairwise_return_correlation": corr,
        "bar_overlap_note": "Correlations use overlapping bar timestamps from bootstrap history.",
    }


def build_bootstrap_payload() -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        tracker = assemble_symbol_tracker_tiles(
            cur,
            mode="intraday",
            chart_style="line",
            horizon_bars=20,
            interval_minutes=15,
            window_bars=96,
            query_bar_seconds=None,
            projection_mode="stitched",
        )
        tiles = tracker.get("tiles") or []
        symbols = [str(t.get("symbol") or "").upper() for t in tiles if t.get("symbol")]
        analog = _fetch_analog_episodes(cur, symbols)
        portfolio_ctx = _portfolio_context(tiles)
        entry_lifecycle_by_symbol: dict[str, Any] = {}
        reconciliation_by_symbol: dict[str, Any] = {}
        reconciliation_meta: dict[str, Any] = {}
        pid, ibkr_account_id = _active_live_portfolio(cur)
        if pid and symbols:
            try:
                raw_by_sym = fetch_entry_lifecycle_rows(cur, pid, symbols)
                for sym in symbols:
                    if sym in raw_by_sym:
                        entry_lifecycle_by_symbol[sym] = build_operator_entry_lifecycle(sym, raw_by_sym[sym])
            except Exception as exc:
                # Missing DDL, role grants, or wrong database — do not fail the whole LIC bootstrap.
                _log.warning(
                    "entry_lifecycle_by_symbol skipped (need MIP.LIVE.ENTRY_INTEL_ACTION_LINK + related "
                    "objects from MIP/SQL/app/410_entry_intel_lifecycle.sql and grants for the API role): %s",
                    exc,
                    exc_info=True,
                )
        if pid and ibkr_account_id and tiles:
            try:
                reconciliation_by_symbol, reconciliation_meta = run_lifecycle_reconciliation(
                    cur,
                    pid,
                    ibkr_account_id,
                    tiles,
                    entry_lifecycle_by_symbol,
                )
            except Exception as exc:
                _log.warning(
                    "reconciliation_by_symbol skipped (see MIP/SQL/migrations/20260401_lifecycle_reconciliation_v1.sql): %s",
                    exc,
                    exc_info=True,
                )
        news_snapshot = []
        for t in tiles:
            sym = str(t.get("symbol") or "").upper()
            for ev in t.get("events") or []:
                if str(ev.get("type") or "").upper() == "NEWS":
                    news_snapshot.append({"symbol": sym, **ev})
        return {
            "ok": True,
            "bootstrap_version": BOOTSTRAP_VERSION,
            "bootstrap_at": datetime.now(timezone.utc).isoformat(),
            "tracker": tracker,
            "analog_episodes_by_symbol": analog,
            "portfolio_context": portfolio_ctx,
            "entry_lifecycle_by_symbol": entry_lifecycle_by_symbol,
            "reconciliation_by_symbol": reconciliation_by_symbol,
            "reconciliation_meta": reconciliation_meta,
            "news_snapshot": news_snapshot[:50],
        }
    finally:
        conn.close()
