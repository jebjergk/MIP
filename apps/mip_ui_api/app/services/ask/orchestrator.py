from __future__ import annotations

from app.services.ask.assembler import build_markdown_answer, compute_overall_confidence
from app.services.ask.docs_retriever import retrieve_docs
from app.services.ask.glossary_repository import find_glossary_matches
from app.services.ask.intent import classify_intent
from app.services.ask.models import AnswerSection, AskContext, AskResolution, ConfidenceScores, SourceAttribution
from app.services.ask.normalize import expand_variants, tokenize
from app.services.ask.policy import should_allow_web_fallback
from app.services.ask.suggestions import suggest_terms
from app.services.ask.web_fallback import retrieve_web_clarification


def _sections_from_docs(doc_chunks: list[str], doc_sources: list[SourceAttribution]) -> AnswerSection | None:
    if not doc_chunks:
        return None
    snippet = doc_chunks[0][:700].strip()
    return AnswerSection(
        section_type="mip_specific",
        title="From MIP knowledge",
        text=snippet,
        sources=doc_sources,
    )


def _sections_from_glossary(glossary_matches: list[dict], confidence: float) -> AnswerSection | None:
    if not glossary_matches:
        return None
    top = glossary_matches[0]
    label = str(top.get("DISPLAY_TERM") or top.get("TERM_KEY") or "term")
    short = str(top.get("DEFINITION_SHORT") or "").strip()
    mip_specific = str(top.get("MIP_SPECIFIC_MEANING") or "").strip()
    general = str(top.get("GENERAL_MARKET_MEANING") or "").strip()
    body_lines = [f"**{label}**"]
    if short:
        body_lines.append(short)
    if mip_specific:
        body_lines.append(f"MIP meaning: {mip_specific}")
    if general:
        body_lines.append(f"General market meaning: {general}")
    return AnswerSection(
        section_type="terminology",
        title="MIP terminology clarification",
        text="\n\n".join(body_lines),
        sources=[
            SourceAttribution(
                source_type="GLOSSARY",
                source_ref=str(top.get("TERM_KEY") or label).lower(),
                label=label,
                confidence=confidence,
            )
        ],
    )


def resolve_question(question: str, route: str | None, history: list[dict[str, str]]) -> tuple[AskContext, AskResolution]:
    tokens = tokenize(question)
    variants = sorted(expand_variants(tokens))
    intent = classify_intent(question, history)
    ctx = AskContext(
        question=question,
        route=route,
        history=history,
        normalized_tokens=variants,
        intent=intent,
    )

    doc_chunks, docs_conf, doc_sources = retrieve_docs(question, route)
    glossary_matches, glossary_conf = find_glossary_matches(question)
    glossary_section = _sections_from_glossary(glossary_matches, glossary_conf)

    use_web = should_allow_web_fallback(question, intent, docs_conf, glossary_conf)
    web_text = ""
    web_conf = 0.0
    web_sources: list[SourceAttribution] = []
    if use_web:
        web_text, web_conf, web_sources = retrieve_web_clarification(question)

    sections: list[AnswerSection] = []
    doc_section = _sections_from_docs(doc_chunks, doc_sources)
    if doc_section:
        sections.append(doc_section)
    if glossary_section:
        sections.append(glossary_section)
    if web_text.strip():
        sections.append(
            AnswerSection(
                section_type="general_clarification",
                title="General market clarification",
                text=web_text,
                sources=web_sources,
            )
        )
    if not sections:
        sections.append(
            AnswerSection(
                section_type="uncertainty_note",
                title="Not documented in MIP",
                text=(
                    "I could not find a reliable MIP doc or approved glossary match for this yet. "
                    "Try one of the suggestions below or ask with a specific page/metric context."
                ),
                sources=[SourceAttribution("INFERENCE", "coverage_gap", "Coverage gap", 0.2)],
            )
        )

    did_you_mean = suggest_terms(question)
    unknown_terms = [] if glossary_matches else variants[:5]
    conf = ConfidenceScores(
        docs_confidence=docs_conf,
        glossary_confidence=glossary_conf,
        web_confidence=web_conf,
    )
    conf.overall = compute_overall_confidence(conf)

    all_sources = []
    for section in sections:
        all_sources.extend(section.sources)

    resolution = AskResolution(
        answer=build_markdown_answer(sections),
        sections=sections,
        sources=all_sources,
        confidence=conf,
        did_you_mean=did_you_mean,
        unknown_terms=unknown_terms,
        fallback_used=use_web,
    )
    return ctx, resolution
