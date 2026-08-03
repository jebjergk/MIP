from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field

from .constants import (
    DEFAULT_MODE,
    DEFAULT_PILOT_SYMBOLS,
    DEFAULT_STARTING_CASH,
    DOSSIER_VERSION_DEFAULT,
    RULESET_VERSION_DEFAULT,
    SIMULATION_BANNER,
)


class CreateRunRequest(BaseModel):
    mode: Literal["HISTORICAL_REPLAY"] = DEFAULT_MODE
    week_start: date | None = None
    symbols: list[str] = Field(default_factory=lambda: list(DEFAULT_PILOT_SYMBOLS))
    ruleset_version: str = RULESET_VERSION_DEFAULT
    dossier_version: str = DOSSIER_VERSION_DEFAULT
    starting_cash: float = DEFAULT_STARTING_CASH
    created_by: str | None = None
    experiment_role: str | None = None
    experiment_freeze_id: str | None = None


class SymbolSnapshot(BaseModel):
    symbol: str
    paa_verdict: str | None = None
    paa_confidence: float | None = None
    daily_trend: str | None = None
    brooks_state: str = "DOSSIER_READY"
    latest_finding: str | None = None
    latest_action: str = "OBSERVE"
    position_status: str = "NONE"
    thesis_status: str = "PENDING"
    blocked_by_other_position: bool = False


class RunSummary(BaseModel):
    run_id: str
    mode: str
    status: str
    preparation_status: str = "NOT_STARTED"
    readiness: dict[str, str] | None = None
    simulation_banner: str = SIMULATION_BANNER
    selected_week_start: date | None = None
    ruleset_version: str
    dossier_version: str
    symbols: list[str]
    starting_cash: float
    current_cash: float
    open_position_symbol: str | None = None
    open_position_qty: int | None = None
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    replay_timestamp: str | None = None
    active_trading_date: date | None = None
    playback_speed: str = "manual"
    data_quality_status: str = "OK"
    error_message: str | None = None


class RunDetail(RunSummary):
    configuration: dict[str, Any] = Field(default_factory=dict)
    symbol_snapshots: list[SymbolSnapshot] = Field(default_factory=list)
    preparation: dict[str, Any] | None = None


class PreparationResponse(BaseModel):
    run_id: str
    preparation_status: str
    preparation: dict[str, Any]


class RulesetInfo(BaseModel):
    ruleset_version: str
    description: str
    tie_break_version: str
    phase: str = "v0.1_shell"
    parameters: dict[str, Any] | None = None


class CreateRunResponse(BaseModel):
    run: RunDetail


class SimpleStatusResponse(BaseModel):
    run_id: str
    status: str
    message: str | None = None


class ObservationsPage(BaseModel):
    run_id: str
    symbol: str
    observations: list[dict[str, Any]] = Field(default_factory=list)
    total: int = 0


class TradesPage(BaseModel):
    run_id: str
    trades: list[dict[str, Any]] = Field(default_factory=list)


class DossierPreview(BaseModel):
    symbol: str
    trading_date: date
    available: bool
    message: str
    dossier_version: str = DOSSIER_VERSION_DEFAULT
    paa_analysis_id: str | None = None
    kind: str = "source_preview"
