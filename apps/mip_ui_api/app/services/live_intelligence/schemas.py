"""
Live Intelligence Cockpit — request/response contracts.

Bootstrap returns:
- tracker: same shape as GET /symbol-tracker/tiles (positions, bars, expectation, events, etc.)
- bootstrap_version: str
- analog_episodes_by_symbol: preloaded rows for k-NN without Snowflake after load
- portfolio_context: exposure summary + return correlation matrix + optional regime hint
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class LiveBar(BaseModel):
    ts: str | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None


class LiveSymbolPayload(BaseModel):
    """Merged live tape for one symbol (client sends after IB merge)."""

    symbol: str
    market_type: str | None = None
    bars: list[dict[str, Any]] = Field(default_factory=list)
    current_price: float | None = None
    bar_seconds: int | None = None
    interval_minutes: int | None = None
    unrealized_pnl: float | None = None


class DeterministicStepRequest(BaseModel):
    """Stateless step: client sends prior intelligence + current merged positions context."""

    bootstrap_version: str = "1.0.0"
    positions: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Tile-like dicts: symbol, side, chart.bars, overlays, expectation, progress_metrics, volatility_context, events, unrealized_pnl, ...",
    )
    prior_intelligence: dict[str, dict[str, Any]] = Field(default_factory=dict)
    session_peak_pnl_by_symbol: dict[str, float] = Field(default_factory=dict)
    analog_episodes_by_symbol: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    portfolio_context: dict[str, Any] = Field(default_factory=dict)
    sensitivity: str = Field(default="BALANCED")


class FeedEvent(BaseModel):
    ts: str
    symbol: str
    transition: str
    why_now_human: str
    why_now_machine: dict[str, Any] = Field(default_factory=dict)
    action_implication: str = ""
    urgency: str = "HOLD"


class DeterministicStepResponse(BaseModel):
    intelligence_by_symbol: dict[str, dict[str, Any]]
    feed_events: list[dict[str, Any]]
    portfolio_regime: dict[str, Any] = Field(default_factory=dict)


class AiEnrichRequest(BaseModel):
    symbol: str
    intelligence: dict[str, Any] = Field(default_factory=dict)
    deterministic_snapshot: dict[str, Any] = Field(default_factory=dict)
    analog_summary: dict[str, Any] = Field(default_factory=dict)
    worlds_summary: dict[str, Any] = Field(default_factory=dict)
    simulator_summary: dict[str, Any] = Field(default_factory=dict)
    force: bool = False
    narrative_mode: str = Field(default="concise", pattern="^(concise|plain|forensic)$")


class AiEnrichResponse(BaseModel):
    agents: list[dict[str, Any]]
    playbook: list[dict[str, Any]]
    committee_headline: str = ""
    used_ollama: bool = False
    skipped: bool = False
