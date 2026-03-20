from __future__ import annotations

from app.services.ask.models import AnswerSection, ConfidenceScores


def compute_overall_confidence(conf: ConfidenceScores) -> float:
    weighted = (conf.docs_confidence * 0.5) + (conf.glossary_confidence * 0.35) + (conf.web_confidence * 0.15)
    return min(1.0, max(0.0, weighted))


def build_markdown_answer(sections: list[AnswerSection]) -> str:
    lines: list[str] = []
    for section in sections:
        if not section.text.strip():
            continue
        lines.append(f"### {section.title}")
        lines.append(section.text.strip())
        lines.append("")
    return "\n".join(lines).strip()
