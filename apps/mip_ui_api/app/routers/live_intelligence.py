"""Live Intelligence Cockpit — bootstrap (Snowflake once), IB live, deterministic + AI enrichment."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Body

from app.services.ibkr_live_bars import infer_ib_market_type, normalize_ib_symbol, run_agent_ibkr_live_bars
from app.services.live_intelligence.ai_committee import run_ai_enrichment
from app.services.live_intelligence.bootstrap import build_bootstrap_payload
from app.services.live_intelligence.engine import run_deterministic_step
from app.services.live_intelligence.schemas import (
    AiEnrichRequest,
    AiEnrichResponse,
    DeterministicStepRequest,
    DeterministicStepResponse,
)
from app.routers.symbol_tracker import _iso, _to_float

router = APIRouter(prefix="/live-intelligence", tags=["live-intelligence"])

_INTRADAY_INTERVALS = {1, 15, 60}
_INTRADAY_BAR_SECONDS = {30}


@router.get("/bootstrap")
def live_intelligence_bootstrap():
    """Single Snowflake session: tracker tiles + analog packs + portfolio context."""
    return build_bootstrap_payload()


@router.post("/ib-live")
def live_intelligence_ib_live(payload: dict[str, Any] = Body(default_factory=dict)):
    """IBKR only — no Snowflake (same behavior as /symbol-tracker/ib-live)."""
    mode = str(payload.get("mode") or "intraday").lower()
    if mode not in {"intraday", "daily"}:
        mode = "intraday"

    bar_seconds: int | None = None
    if mode == "intraday":
        raw_bs = payload.get("intraday_bar_seconds")
        if raw_bs is not None:
            try:
                bs = int(raw_bs)
                if bs in _INTRADAY_BAR_SECONDS:
                    bar_seconds = bs
            except (TypeError, ValueError):
                bar_seconds = None

    if mode == "daily":
        interval_minutes = 1440
    elif bar_seconds is not None:
        interval_minutes = 0
    else:
        interval_minutes = int(payload.get("intraday_interval_minutes") or 60)
        if interval_minutes not in _INTRADAY_INTERVALS:
            interval_minutes = 60

    if mode == "daily":
        window_cap = 300
        default_window = 120
    elif bar_seconds is not None:
        window_cap = 800
        default_window = 780
    else:
        window_cap = 400
        default_window = 24

    window_bars = int(payload.get("window_bars") or default_window)
    if mode == "daily":
        window_bars = max(30, min(window_bars, window_cap))
    else:
        window_bars = max(15, min(window_bars, window_cap))

    raw_symbols = payload.get("symbols") or []
    symbol_specs: list[dict[str, str]] = []
    if isinstance(raw_symbols, list):
        for item in raw_symbols:
            if isinstance(item, dict):
                symbol_specs.append(
                    {
                        "symbol": str(item.get("symbol") or ""),
                        "market_type": str(item.get("market_type") or ""),
                    }
                )
            elif item is not None:
                symbol_specs.append({"symbol": str(item), "market_type": ""})

    # Slightly generous timeout: multi-symbol 30s bars can exceed 60s on cold IB / RTH edges.
    ib_payload = run_agent_ibkr_live_bars(
        symbol_specs,
        interval_minutes=interval_minutes if interval_minutes > 0 else 1,
        window_bars=window_bars,
        bar_seconds=bar_seconds,
        timeout_sec=120 if bar_seconds else 75,
    )

    rows: list[dict[str, Any]] = []
    for item in ib_payload.get("symbols") or []:
        symbol = normalize_ib_symbol(item.get("symbol") or "")
        if not symbol:
            continue
        bars_raw = item.get("bars") if isinstance(item.get("bars"), list) else []
        bars = []
        for b in bars_raw[-window_bars:]:
            if not isinstance(b, dict):
                continue
            bars.append(
                {
                    "ts": _iso(b.get("ts")),
                    "open": _to_float(b.get("open")),
                    "high": _to_float(b.get("high")),
                    "low": _to_float(b.get("low")),
                    "close": _to_float(b.get("close")),
                    "volume": _to_float(b.get("volume")),
                }
            )
        current_price = _to_float(item.get("current_price"))
        if current_price is None and bars:
            current_price = bars[-1].get("close")
        rows.append(
            {
                "symbol": symbol,
                "market_type": infer_ib_market_type(symbol, item.get("market_type")),
                "status": str(item.get("status") or "SUCCESS"),
                "error": item.get("error"),
                "bars": bars,
                "current_price": current_price,
                "last_bar_ts": bars[-1]["ts"] if bars else None,
            }
        )

    return {
        "ok": True,
        "source": "IBKR_DIRECT",
        "mode": mode,
        "interval_minutes": interval_minutes if bar_seconds is None else 0,
        "intraday_bar_seconds": bar_seconds,
        "window_bars": window_bars,
        "rows": rows,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.post("/deterministic-step", response_model=DeterministicStepResponse)
def live_intelligence_deterministic_step(req: DeterministicStepRequest):
    out = run_deterministic_step(req.model_dump())
    return DeterministicStepResponse(
        intelligence_by_symbol=out["intelligence_by_symbol"],
        feed_events=out["feed_events"],
        portfolio_regime=out["portfolio_regime"],
    )


@router.post("/ai/enrich", response_model=AiEnrichResponse)
def live_intelligence_ai_enrich(req: AiEnrichRequest):
    payload = {
        "symbol": req.symbol,
        "intelligence": req.intelligence,
        "deterministic_snapshot": req.deterministic_snapshot,
        "force": req.force,
    }
    out = run_ai_enrichment(payload, last_ai_by_symbol=None)
    return AiEnrichResponse(
        agents=out.get("agents") or [],
        playbook=out.get("playbook") or [],
        committee_headline=out.get("committee_headline") or "",
        used_ollama=bool(out.get("used_ollama")),
        skipped=bool(out.get("skipped")),
    )
