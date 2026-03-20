from __future__ import annotations

from app.services.ask.models import SourceAttribution


def retrieve_web_clarification(question: str) -> tuple[str, float, list[SourceAttribution]]:
    # Controlled fallback stub: external lookup integration remains opt-in and can be
    # wired to a curated provider without changing the orchestration contract.
    return (
        "",
        0.0,
        [
            SourceAttribution(
                source_type="WEB",
                source_ref="disabled",
                label="External clarification disabled",
                confidence=0.0,
            )
        ],
    )
