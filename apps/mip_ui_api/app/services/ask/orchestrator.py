from __future__ import annotations

import json
import logging

from app.config import get_askmip_model
from app.db import get_connection
from app.services.ask.assembler import compute_overall_confidence
from app.services.ask.docs_retriever import retrieve_docs
from app.services.ask.glossary_repository import find_glossary_matches
from app.services.ask.intent import classify_intent
from app.services.ask.models import AnswerSection, AskContext, AskResolution, ConfidenceScores, SourceAttribution
from app.services.ask.normalize import expand_variants, tokenize
from app.services.ask.policy import should_allow_web_fallback
from app.services.ask.suggestions import suggest_terms
from app.services.ask.web_fallback import retrieve_web_clarification

logger = logging.getLogger(__name__)


_V2_SYSTEM_PROMPT = """\
You are **MIP Assistant**, the built-in help system for the Market Intelligence Platform (MIP).

## Your knowledge
You have access to relevant MIP documentation excerpts and a glossary of MIP/trading terms below.
Use them as your primary knowledge. Only state things as facts when supported by this context.

{context_block}

## Instructions
1. Answer the question directly and clearly. Start with a short, concrete answer.
2. The user is currently viewing: **{page_context}**. Frame your answer in the context of that page
   when the question relates to something visible there.
3. If the question is about a UI label, metric, chart, or status indicator, define the term first,
   then explain what it means on the current page and what values to look for.
4. For trading/finance concepts (P&L, NAV, drawdown, slippage, etc.), give a plain-English
   definition, then explain how MIP uses or displays it.
5. Use this response structure:
   - **Short answer** (1-2 sentences)
   - **Detail** (why, how it works, what to look for)
   - **Where to verify** (specific UI location)
6. Never invent live values, thresholds, or internal formulas not in the provided context.
7. If the concept is not covered in the provided context, say so clearly. You may still explain
   general market/trading concepts in plain language — just label them as general knowledge.
8. Keep answers concise but complete. Do not truncate mid-sentence.
"""


def _build_context_block(
    doc_chunks: list[str],
    glossary_matches: list[dict],
) -> str:
    parts: list[str] = []

    if doc_chunks:
        parts.append("<mip_documentation>")
        for i, chunk in enumerate(doc_chunks[:2]):
            trimmed = chunk[:3000].strip()
            parts.append(f"--- Doc excerpt {i + 1} ---")
            parts.append(trimmed)
        parts.append("</mip_documentation>")

    if glossary_matches:
        parts.append("")
        parts.append("<glossary_terms>")
        for row in glossary_matches[:5]:
            display = str(row.get("DISPLAY_TERM") or row.get("TERM_KEY") or "")
            short_def = str(row.get("DEFINITION_SHORT") or "").strip()
            mip_meaning = str(row.get("MIP_SPECIFIC_MEANING") or "").strip()
            general_meaning = str(row.get("GENERAL_MARKET_MEANING") or "").strip()
            example = str(row.get("EXAMPLE_IN_MIP") or "").strip()
            entry_lines = [f"Term: {display}"]
            if short_def:
                entry_lines.append(f"  Definition: {short_def}")
            if mip_meaning:
                entry_lines.append(f"  MIP meaning: {mip_meaning}")
            if general_meaning:
                entry_lines.append(f"  General market meaning: {general_meaning}")
            if example:
                entry_lines.append(f"  Example in MIP: {example}")
            parts.append("\n".join(entry_lines))
        parts.append("</glossary_terms>")

    if not parts:
        parts.append("No specific MIP documentation or glossary matches were found for this query.")

    return "\n".join(parts)


def _build_page_context(route: str | None, page_title: str | None) -> str:
    if page_title and route:
        return f"{page_title} (route: {route})"
    if page_title:
        return page_title
    if route:
        return route
    return "unknown page"


def _call_cortex(prompt: str) -> str:
    model_name = get_askmip_model()
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 120")
        sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s) AS response"
        cur.execute(sql, (model_name, prompt))
        row = cur.fetchone()
        if not row or not row[0]:
            return "I was unable to generate a response. Please try again."
        raw = row[0]
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    choices = parsed.get("choices", [])
                    if choices:
                        msg = choices[0].get("messages", "") or choices[0].get("message", "")
                        if isinstance(msg, dict):
                            return msg.get("content", str(msg))
                        return str(msg)
            except (json.JSONDecodeError, TypeError):
                pass
            return raw
        if isinstance(raw, dict):
            choices = raw.get("choices", [])
            if choices:
                msg = choices[0].get("messages", "") or choices[0].get("message", "")
                if isinstance(msg, dict):
                    return msg.get("content", str(msg))
                return str(msg)
        return str(raw)
    except Exception as exc:
        logger.error("Cortex COMPLETE call failed in orchestrator: %s", exc, exc_info=True)
        return "I encountered an error generating the answer. Please try again."
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()


def _determine_provenance(doc_chunks: list[str], glossary_matches: list[dict]) -> list[SourceAttribution]:
    sources: list[SourceAttribution] = []
    if doc_chunks:
        sources.append(SourceAttribution("DOC", "mip_guide", "Based on MIP docs", 1.0))
    if glossary_matches:
        sources.append(SourceAttribution("GLOSSARY", "mip_glossary", "Based on MIP glossary", 1.0))
    if not sources:
        sources.append(SourceAttribution("INFERENCE", "general_knowledge", "General knowledge", 0.5))
    return sources


def resolve_question(
    question: str,
    route: str | None,
    history: list[dict[str, str]],
    page_title: str | None = None,
    page_hint: str | None = None,
) -> tuple[AskContext, AskResolution]:
    tokens = tokenize(question)
    variants = sorted(expand_variants(tokens))
    intent = classify_intent(question, history)
    ctx = AskContext(
        question=question,
        route=route,
        page_title=page_title,
        page_hint=page_hint,
        history=history,
        normalized_tokens=variants,
        intent=intent,
    )

    doc_chunks, docs_conf, doc_sources = retrieve_docs(question, route, page_title, page_hint)
    glossary_matches, glossary_conf = find_glossary_matches(question)

    context_block = _build_context_block(doc_chunks, glossary_matches)
    page_context = _build_page_context(route, page_title)

    system_prompt = _V2_SYSTEM_PROMPT.format(
        context_block=context_block,
        page_context=page_context,
    )

    prompt_parts = [system_prompt]
    for msg in history[-10:]:
        label = "User" if msg.get("role") == "user" else "MIP Assistant"
        prompt_parts.append(f"\n{label}: {msg.get('content', '')}")
    if not history or history[-1].get("content") != question:
        prompt_parts.append(f"\nUser: {question}")
    prompt_parts.append("\nMIP Assistant:")

    full_prompt = "\n".join(prompt_parts)
    llm_answer = _call_cortex(full_prompt)

    provenance_sources = _determine_provenance(doc_chunks, glossary_matches)
    sections = [
        AnswerSection(
            section_type="answer",
            title="Answer",
            text=llm_answer,
            sources=provenance_sources,
        )
    ]

    did_you_mean = suggest_terms(question) if (docs_conf < 0.3 and glossary_conf < 0.3) else []
    unknown_terms = [] if glossary_matches else variants[:5]

    conf = ConfidenceScores(
        docs_confidence=docs_conf,
        glossary_confidence=glossary_conf,
        web_confidence=0.0,
    )
    conf.overall = compute_overall_confidence(conf)

    resolution = AskResolution(
        answer=llm_answer,
        sections=sections,
        sources=provenance_sources,
        confidence=conf,
        did_you_mean=did_you_mean,
        unknown_terms=unknown_terms,
        fallback_used=False,
    )
    return ctx, resolution
