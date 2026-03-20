from __future__ import annotations

from app.guide_loader import get_guide_sections
from app.services.ask.models import SourceAttribution
from app.services.ask.normalize import normalize_text, tokenize

_ROUTE_FILE_HINTS = {
    "/live-portfolio-activity": "29-live-portfolio-activity.md",
    "/live-portfolio-config": "27-live-portfolio-config.md",
    "/symbol-tracker": "17-symbol-tracker.md",
    "/training": "15-training-status.md",
    "/decision-console": "28-ai-agent-decisions.md",
    "/news-intelligence": "26-news-intelligence.md",
    "/performance-dashboard": "16-performance-dashboard.md",
    "/runs": "19-runs.md",
    "/cockpit": "12-cockpit.md",
    "/home": "11-home.md",
}

def retrieve_docs(question: str, route: str | None, page_title: str | None, page_hint: str | None) -> tuple[list[str], float, list[SourceAttribution]]:
    sections = get_guide_sections()
    if not sections:
        return [], 0.0, []
    q_tokens = set(tokenize(question))
    title_tokens = set(tokenize(page_title or ""))
    hint_tokens = set(tokenize(page_hint or ""))
    route_file = _ROUTE_FILE_HINTS.get(route or "", "")
    scored: list[tuple[float, str]] = []
    for section in sections:
        chunk = str(section.get("content") or "").strip()
        if not chunk:
            continue
        chunk_tokens = set(tokenize(chunk[:3000]))
        overlap = float(len(q_tokens.intersection(chunk_tokens)))
        overlap += float(len(title_tokens.intersection(chunk_tokens))) * 0.7
        overlap += float(len(hint_tokens.intersection(chunk_tokens))) * 0.45
        if route:
            route_norm = normalize_text(route.replace("/", " "))
            if route_norm and route_norm in normalize_text(chunk):
                overlap += 1.4
        if route_file and section.get("file") == route_file:
            overlap += 6.0
        if overlap > 0:
            scored.append((overlap, chunk))
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
