from __future__ import annotations

import re


_TOKEN_SPLIT_RE = re.compile(r"[^a-z0-9]+")


def normalize_text(value: str) -> str:
    lowered = (value or "").strip().lower()
    return " ".join(lowered.split())


def tokenize(value: str) -> list[str]:
    norm = normalize_text(value)
    if not norm:
        return []
    return [t for t in _TOKEN_SPLIT_RE.split(norm) if t]


def singularize(token: str) -> str:
    if token.endswith("ies") and len(token) > 3:
        return f"{token[:-3]}y"
    if token.endswith("s") and len(token) > 3:
        return token[:-1]
    return token


def expand_variants(tokens: list[str]) -> set[str]:
    out: set[str] = set(tokens)
    for token in tokens:
        out.add(singularize(token))
    return {item for item in out if item}
