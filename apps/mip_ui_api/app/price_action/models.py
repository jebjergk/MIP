from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

AnnotationSource = Literal["detected_geometry", "methodologist_interpretation"]
LongVerdict = Literal[
    "LONG_APPROVE",
    "LONG_APPROVE_REDUCED",
    "WAIT_PULLBACK",
    "WAIT_RECLAIM",
    "DEFER",
    "REJECT",
    "NO_CLEAR_LONG",
]
AnnotationType = Literal[
    "HORIZONTAL_LEVEL",
    "ZONE_BOX",
    "TRENDLINE",
    "CHANNEL_LINE",
    "WEDGE_LINE",
    "RANGE_BOX",
    "MARKER",
    "TEXT_NOTE",
    "MEASURED_MOVE",
    "EMA_LINE",
]


class AnalyseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, populate_by_name=True)

    symbol: str = Field(min_length=1, max_length=32, pattern=r"^[A-Za-z0-9._-]+$")
    lookback_bars: Literal[90, 120, 180, 250] = Field(
        default=120,
        validation_alias=AliasChoices("lookback_bars", "lookback", "lookback_days"),
    )
    side: str = Field(default="LONG", min_length=1, max_length=16)
    as_of_date: date | None = None

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.upper()

    @field_validator("side")
    @classmethod
    def normalize_side(cls, value: str) -> str:
        return value.upper()


class Annotation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: AnnotationType
    label: str = Field(max_length=240)
    source: AnnotationSource
    date: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    price: float | None = None
    y_min: float | None = None
    y_max: float | None = None
    confidence: Literal["LOW", "MEDIUM", "HIGH"] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DailyBar(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: date
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    ema20: float


class ExpertRead(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market_context: str = Field(max_length=1200)
    trend_state: str = Field(max_length=600)
    current_pattern: str = Field(max_length=1200)
    support_resistance_read: str = Field(max_length=1200)
    long_location_quality: str = Field(max_length=600)
    continuation_vs_failure_risk: str = Field(max_length=1200)
    what_supports_the_long: list[str] = Field(default_factory=list, max_length=6)
    what_weakens_the_long: list[str] = Field(default_factory=list, max_length=6)
    confirmation_needed: str = Field(max_length=1200)
    invalidation_logic: str = Field(max_length=1200)


class PlainExplanation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str = Field(max_length=1200)
    what_the_chart_is_doing: str = Field(max_length=1200)
    why_it_matters: str = Field(max_length=1200)
    what_to_wait_for: str = Field(max_length=1200)
    simple_risk_warning: str = Field(max_length=1200)


class LongVerdictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision: LongVerdict
    confidence: float = Field(ge=0.0, le=1.0)
    reason_summary: str = Field(max_length=1200)
    not_a_short_recommendation: Literal[True] = True


class MethodologistOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    timeframe: Literal["1D"] = "1D"
    side: Literal["LONG"] = "LONG"
    analysis_status: Literal[
        "OK", "INSUFFICIENT_DATA", "ERROR", "GEOMETRY_ONLY", "METHODOLOGIST_UNAVAILABLE"
    ]
    expert_read: ExpertRead
    plain_explanation: PlainExplanation
    verdict: LongVerdictModel
    annotations: list[Annotation] = Field(default_factory=list, max_length=6)


class AnalyseResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    analysis_id: str
    symbol: str
    side: Literal["LONG"] = "LONG"
    timeframe: Literal["DAILY"] = "DAILY"
    lookback_bars: Literal[90, 120, 180, 250]
    as_of_date: date
    bar_count: int
    bars: list[DailyBar]
    current_price: float
    detected_geometry: dict[str, Any]
    situation_model: dict[str, Any]
    annotations: list[Annotation]
    rag_status: str
    retrieval_count: int
    card_count: int
    total_chars: int
    methodologist_knowledge: list[dict[str, Any]]
    methodologist: MethodologistOutput
