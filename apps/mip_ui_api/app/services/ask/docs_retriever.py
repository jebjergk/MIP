from __future__ import annotations

from app.guide_loader import get_guide_content
from app.services.ask.models import SourceAttribution
from app.services.ask.normalize import normalize_text, tokenize


def retrieve_docs(question: str, route: str | None) -> tuple[list[str], float, list[SourceAttribution]]:
    content = get_guide_content()
    if not content.strip():
        return [], 0.0, []
    chunks = [c.strip() for c in content.split("\n\n---\n\n") if c.strip()]
    q_tokens = set(tokenize(question))
    scored: list[tuple[float, str]] = []
    for chunk in chunks:
        chunk_tokens = set(tokenize(chunk[:2500]))
        overlap = len(q_tokens.intersection(chunk_tokens))
        if route:
            route_norm = normalize_text(route.replace("/", " "))
            if route_norm and route_norm in normalize_text(chunk):
                overlap += 2
        if overlap > 0:
            scored.append((float(overlap), chunk))
    if not scored:
        return [], 0.0, []
    scored.sort(key=lambda x: x[0], reverse=True)
    top = [item[1] for item in scored[:3]]
    conf = min(1.0, scored[0][0] / 8.0)
    sources = [
        SourceAttribution(
            source_type="DOC",
            source_ref=f"guide_chunk_{idx+1}",
            label="MIP guide",
            confidence=conf,
        )
        for idx, _ in enumerate(top)
    ]
    return top, conf, sources
