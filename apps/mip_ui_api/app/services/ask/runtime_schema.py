"""
Ask MIP 2.0 — structured runtime context from the browser (page-aware assistance).

All fields are optional for backward compatibility; the UI should populate what it can.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class HistoryMessageV3(BaseModel):
    role: str = Field(..., pattern="^(user|assistant)$")
    content: str


class AskRuntimePayload(BaseModel):
    """Payload sent with Ask MIP requests for page-aware retrieval and fact lookup."""

    session_id: str | None = None
    page_id: str | None = Field(
        default=None,
        description="Stable page key, e.g. symbol_tracker, training_status",
    )
    session_mode: str | None = Field(
        default=None,
        description="sim, live, research, etc.",
    )
    symbol: str | None = None
    portfolio_id: int | None = None
    strategy_id: str | None = None
    as_of_timestamp: str | None = None
    active_filters: dict[str, Any] = Field(default_factory=dict)
    visible_widget_ids: list[str] = Field(default_factory=list)
    selected_widget_id: str | None = None
    selected_row_context: dict[str, Any] | None = None
    selected_card_context: dict[str, Any] | None = None
    current_kpi_snapshot: dict[str, Any] | None = None
    current_badges_or_statuses: list[str] = Field(default_factory=list)
    page_route: str | None = None
    app_version: str | None = None
    hovered_field_label: str | None = None
    hovered_help_text_id: str | None = None
    selected_chart_series: str | None = None
    selected_chart_point_context: dict[str, Any] | None = None
    ui_language: str | None = None
    debug_context_enabled: bool | None = None

    model_config = {"extra": "ignore"}


class AskV3RequestBody(BaseModel):
    """POST /ask/v3 body: question plus legacy v2 fields plus optional runtime."""

    question: str = Field(..., min_length=1, max_length=4000)
    route: str | None = None
    page_title: str | None = None
    page_hint: str | None = None
    history: list[HistoryMessageV3] = Field(default_factory=list)
    runtime: AskRuntimePayload | None = None

    model_config = {"extra": "ignore"}


def utc_iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
