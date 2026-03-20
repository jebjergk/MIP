from __future__ import annotations

from difflib import get_close_matches

from app.services.ask.glossary_repository import list_glossary
from app.services.ask.normalize import tokenize


def suggest_terms(question: str, limit: int = 5) -> list[str]:
    vocab: list[str] = []
    for row in list_glossary(limit=500):
        term = row.get("DISPLAY_TERM") or row.get("TERM_KEY")
        if term:
            vocab.append(str(term))
    token_phrase = " ".join(tokenize(question))
    if not token_phrase or not vocab:
        return []
    return get_close_matches(token_phrase, vocab, n=limit, cutoff=0.45)
