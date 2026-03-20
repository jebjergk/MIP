from __future__ import annotations

from app.services.ask.normalize import normalize_text


_TERM_CUES = ("what is", "define", "meaning", "term", "stands for")
_MIP_FEATURE_CUES = ("in mip", "where in the ui", "on this page", "what does this page")
_METRIC_CUES = ("metric", "ratio", "drawdown", "confidence", "conviction")
_TRADING_CUES = ("trading", "slippage", "gtc", "exposure", "volatility", "catalyst")
_FOLLOW_UP_CUES = ("can you clarify", "explain more", "what about", "and for")


def classify_intent(question: str, history: list[dict[str, str]]) -> str:
    q = normalize_text(question)
    if any(cue in q for cue in _FOLLOW_UP_CUES) and history:
        return "follow_up_clarification"
    if any(cue in q for cue in _TERM_CUES):
        return "term_definition"
    if any(cue in q for cue in _MIP_FEATURE_CUES):
        return "mip_feature_behavior"
    if any(cue in q for cue in _METRIC_CUES):
        return "metric_explanation"
    if any(cue in q for cue in _TRADING_CUES):
        return "trading_concept"
    if "research" in q or "market" in q:
        return "market_research_concept"
    return "mixed"
