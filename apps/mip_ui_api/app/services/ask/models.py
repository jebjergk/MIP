from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SourceAttribution:
    source_type: str
    source_ref: str
    label: str
    confidence: float


@dataclass
class ConfidenceScores:
    docs_confidence: float = 0.0
    glossary_confidence: float = 0.0
    web_confidence: float = 0.0
    overall: float = 0.0


@dataclass
class AnswerSection:
    section_type: str
    title: str
    text: str
    sources: list[SourceAttribution] = field(default_factory=list)


@dataclass
class AskContext:
    question: str
    route: str | None
    history: list[dict[str, str]]
    normalized_tokens: list[str]
    intent: str


@dataclass
class AskResolution:
    answer: str
    sections: list[AnswerSection]
    sources: list[SourceAttribution]
    confidence: ConfidenceScores
    did_you_mean: list[str]
    unknown_terms: list[str]
    fallback_used: bool
