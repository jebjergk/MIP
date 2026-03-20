from __future__ import annotations

from app.config import (
    askmip_doc_min_confidence,
    askmip_enable_web_fallback,
    askmip_glossary_min_confidence,
    askmip_web_allowed_intents,
)
from app.services.ask.normalize import normalize_text


_BLOCKED_MIP_INTERNAL_CUES = (
    "threshold",
    "formula",
    "exact rule",
    "how does mip decide",
    "internal",
    "feature flag",
)


def should_allow_web_fallback(question: str, intent: str, docs_conf: float, glossary_conf: float) -> bool:
    if not askmip_enable_web_fallback():
        return False
    if intent not in askmip_web_allowed_intents():
        return False
    if docs_conf >= askmip_doc_min_confidence() or glossary_conf >= askmip_glossary_min_confidence():
        return False
    q_norm = normalize_text(question)
    if any(cue in q_norm for cue in _BLOCKED_MIP_INTERNAL_CUES):
        return False
    return True
