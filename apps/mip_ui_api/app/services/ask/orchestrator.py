from __future__ import annotations

import json
import logging

from app.config import get_askmip_model
from app.db import get_connection
from app.services.ask.assembler import compute_overall_confidence
from app.services.ask.docs_retriever import retrieve_docs
from app.services.ask.fact_fetcher import facts_block_for_prompt, fetch_ask_facts
from app.services.ask.glossary_repository import find_glossary_matches
from app.services.ask.intent import classify_intent
from app.services.ask.models import AnswerSection, AskContext, AskResolution, ConfidenceScores, SourceAttribution
from app.services.ask.normalize import expand_variants, tokenize
from app.services.ask.policy import should_allow_web_fallback
from app.services.ask.retrieval_router import effective_page_id, retrieve_for_ask_v3
from app.services.ask.runtime_schema import AskRuntimePayload
from app.services.ask.suggestions import suggest_terms
from app.services.ask.web_fallback import retrieve_web_clarification

logger = logging.getLogger(__name__)


_V2_SYSTEM_PROMPT = """\
You are **MIP Assistant**, the built-in help system for the Market Intelligence Platform (MIP).
MIP is a market intelligence and algorithmic trading platform that manages research signals,
portfolio simulation, committee-based trade decisions, and live/paper execution via IBKR.

## Your knowledge
You have access to relevant MIP documentation excerpts, a glossary of MIP/trading terms,
and information about the page the user is currently viewing.
Use them as your primary source. Only state things as MIP-specific facts when they are
supported by the documentation or glossary. For general trading/finance concepts, you may
use your broad knowledge — just be transparent about what is MIP-specific vs general.

{context_block}

{page_hint_block}

## Instructions
1. Answer the question directly and clearly. Start with a short, concrete answer.
2. The user is currently viewing: **{page_context}**. Frame your answer in the context of that page
   when the question relates to something visible there.
3. If the question is about a UI label, metric, chart, or status indicator, define the term first,
   then explain what it means on the current page and what values to look for.
4. For trading/finance concepts (P&L, NAV, drawdown, slippage, unrealized gains, exposure, etc.),
   give a plain-English definition, then explain how MIP uses or displays it if you have that context.
5. Use this response structure:
   - **Short answer** (1-2 sentences)
   - **Detail** (why, how it works, what to look for)
   - **Where to verify** (specific UI location if applicable)
6. Never invent live values, thresholds, or internal formulas not in the provided context.
7. If a concept is not specifically documented in MIP, you should still explain it as a
   general trading/finance concept — just note that the explanation is general knowledge.
   Do NOT refuse to answer just because the exact term isn't in MIP docs.
8. Keep answers concise but complete. Do not truncate mid-sentence.
"""

_V3_SYSTEM_PROMPT = """\
You are **MIP Assistant** (Ask MIP 2.0), the knowledge layer for the Market Intelligence Platform.

## Truth zones (must label in your reasoning, not necessarily with headers every time)
1. **App truth** — page/widget/metric contracts, MIP docs excerpts, glossary, and any \
`<snowflake_runtime_facts>` block. Treat these as authoritative for MIP.
2. **General domain** — content in `<domain_knowledge>` is broad markets education; \
say it is general, not a substitute for MIP configuration.
3. **Inference** — if you connect dots not explicitly documented, prefix with \
**Likely explanation:** and do not present it as app truth.

## Rules
- Plain language first. Never invent formulas, thresholds, or undocumented rules; \
if missing, say **that exact logic is not currently documented** and give the closest \
documented explanation.
- Distinguish **In MIP, …** vs **In general markets, …** when both apply.
- Snowflake facts are **current snapshot values** for the user's session context only; \
do not extrapolate to other portfolios/symbols.
- This is educational / system explanation support, not personalized trading advice.

## Your knowledge (retrieval-fed)
{context_block}

## Page / UI runtime (from browser)
{runtime_block}

{facts_block}

{page_hint_block}

## Instructions
1. Answer directly. Use this structure when it fits: **Short answer** → **Detail** → \
**Where to verify** (UI location) → **Caveats**.
2. Current page context for routing: **{page_context}**.
3. If the user asks about undocumented internals, refuse to fabricate and cite the gap explicitly.
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


def _build_artifact_domain_block(artifact_blocks: list[str], domain_blocks: list[str]) -> str:
    parts: list[str] = []
    if artifact_blocks:
        parts.append("<page_widget_metric_contracts>")
        for i, block in enumerate(artifact_blocks[:8]):
            parts.append(f"--- Contract block {i + 1} ---\n{block[:4500]}")
        parts.append("</page_widget_metric_contracts>")
    if domain_blocks:
        parts.append("<domain_knowledge>")
        for i, block in enumerate(domain_blocks[:4]):
            parts.append(f"--- Domain {i + 1} ---\n{block[:3500]}")
        parts.append("</domain_knowledge>")
    if not parts:
        return ""
    return "\n".join(parts)


def _runtime_block_for_prompt(runtime: AskRuntimePayload | None) -> str:
    if not runtime:
        return "(No structured runtime payload was sent.)"
    try:
        payload = runtime.model_dump(exclude_none=True)
    except Exception:
        payload = {}
    if not payload:
        return "(Runtime payload empty.)"
    return (
        "<ui_runtime_context>\n"
        f"{json.dumps(payload, default=str, indent=2)[:6000]}\n"
        "</ui_runtime_context>"
    )


def _merge_v3_provenance(
    doc_chunks: list[str],
    glossary_matches: list[dict],
    bundle_artifact_sources: list[SourceAttribution],
    bundle_domain_sources: list[SourceAttribution],
    doc_sources: list[SourceAttribution],
    web_sources: list[SourceAttribution],
    used_snowflake_facts: bool,
) -> list[SourceAttribution]:
    seen: set[tuple[str, str]] = set()
    out: list[SourceAttribution] = []

    def add(src: SourceAttribution) -> None:
        key = (src.source_type, src.source_ref)
        if key in seen:
            return
        seen.add(key)
        out.append(src)

    for s in doc_sources:
        add(s)
    if glossary_matches:
        add(SourceAttribution("GLOSSARY", "mip_glossary", "MIP glossary", 1.0))
    for s in bundle_artifact_sources:
        add(s)
    for s in bundle_domain_sources:
        add(s)
    for s in web_sources:
        add(s)
    if used_snowflake_facts:
        add(SourceAttribution("SNOWFLAKE_FACT", "runtime_facts", "Snowflake runtime snapshot", 1.0))
    if not out:
        add(SourceAttribution("INFERENCE", "general_knowledge", "Weak coverage", 0.45))
    return out


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
    page_hint_block = ""
    if page_hint:
        page_hint_block = (
            "<current_page_guide>\n"
            f"The user is on the '{page_title or route}' page. "
            f"Here is a summary of what this page covers:\n"
            f"{page_hint[:1200]}\n"
            "</current_page_guide>"
        )

    system_prompt = _V2_SYSTEM_PROMPT.format(
        context_block=context_block,
        page_context=page_context,
        page_hint_block=page_hint_block,
    )

    prompt_parts = [system_prompt]
    for msg in history[-6:]:
        label = "User" if msg.get("role") == "user" else "MIP Assistant"
        content = msg.get("content", "")
        if msg.get("role") == "assistant" and len(content) > 600:
            content = content[:600].rstrip() + "..."
        prompt_parts.append(f"\n{label}: {content}")
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


def resolve_question_v3(
    question: str,
    route: str | None,
    history: list[dict[str, str]],
    page_title: str | None = None,
    page_hint: str | None = None,
    runtime: AskRuntimePayload | None = None,
) -> tuple[AskContext, AskResolution]:
    tokens = tokenize(question)
    variants = sorted(expand_variants(tokens))
    intent, bundle = retrieve_for_ask_v3(question, route, page_title, page_hint, runtime, history)

    effective_route = route or (runtime.page_route if runtime else None)
    eff_page = effective_page_id(effective_route, runtime)

    runtime_dict = runtime.model_dump(exclude_none=True) if runtime else None

    glossary_matches, glossary_conf = find_glossary_matches(question)

    facts_payload: dict = {}
    used_sf = False
    sf_attempted = False
    if runtime and (runtime.portfolio_id is not None or runtime.symbol):
        if intent not in ("trading_concept", "market_research_concept"):
            facts_payload, sf_attempted = fetch_ask_facts(
                runtime.portfolio_id,
                runtime.symbol,
                runtime.session_mode,
            )
            used_sf = bool(sf_attempted and (facts_payload.get("portfolio") or facts_payload.get("symbol")))

    facts_block = ""
    if facts_payload and sf_attempted:
        facts_block = facts_block_for_prompt(facts_payload)

    artifact_domain = _build_artifact_domain_block(bundle.artifact_blocks, bundle.domain_blocks)
    base_ctx = _build_context_block(bundle.doc_chunks, glossary_matches)
    context_block = base_ctx
    if artifact_domain:
        context_block = (context_block + "\n\n" + artifact_domain).strip()

    page_context = _build_page_context(effective_route, page_title)
    page_hint_block = ""
    if page_hint:
        page_hint_block = (
            "<current_page_guide>\n"
            f"The user is on the '{page_title or effective_route}' page. "
            f"Summary from embedded guide:\n"
            f"{page_hint[:1200]}\n"
            "</current_page_guide>"
        )

    rt_block = _runtime_block_for_prompt(runtime)
    system_prompt = _V3_SYSTEM_PROMPT.format(
        context_block=context_block or "No documentation excerpts matched.",
        runtime_block=rt_block,
        facts_block=facts_block if facts_block else "",
        page_hint_block=page_hint_block,
        page_context=page_context,
    )

    prompt_parts = [system_prompt]
    for msg in history[-6:]:
        label = "User" if msg.get("role") == "user" else "MIP Assistant"
        content = msg.get("content", "")
        if msg.get("role") == "assistant" and len(content) > 600:
            content = content[:600].rstrip() + "..."
        prompt_parts.append(f"\n{label}: {content}")
    if not history or history[-1].get("content") != question:
        prompt_parts.append(f"\nUser: {question}")
    prompt_parts.append("\nMIP Assistant:")

    full_prompt = "\n".join(prompt_parts)
    llm_answer = _call_cortex(full_prompt)

    artifact_boost = min(1.0, 0.25 + 0.15 * min(5, len(bundle.artifact_blocks)))
    blended_docs = max(bundle.docs_confidence, artifact_boost if bundle.artifact_blocks else 0.0)

    web_text, web_conf, web_sources = ("", 0.0, [])
    fallback_used = False
    if should_allow_web_fallback(question, intent, blended_docs, glossary_conf):
        web_text, web_conf, web_sources = retrieve_web_clarification(question)
        if web_text and web_conf > 0:
            llm_answer = f"{llm_answer}\n\n**Additional context:**\n{web_text}"
            fallback_used = True

    provenance_sources = _merge_v3_provenance(
        bundle.doc_chunks,
        glossary_matches,
        bundle.artifact_sources,
        bundle.domain_sources,
        bundle.doc_sources,
        web_sources,
        bool(facts_block),
    )

    sections = [
        AnswerSection(
            section_type="answer",
            title="Answer",
            text=llm_answer,
            sources=provenance_sources,
        )
    ]

    did_you_mean = suggest_terms(question) if (blended_docs < 0.3 and glossary_conf < 0.3) else []
    unknown_terms = [] if glossary_matches else variants[:5]

    conf = ConfidenceScores(
        docs_confidence=blended_docs,
        glossary_confidence=glossary_conf,
        web_confidence=web_conf,
    )
    conf.overall = compute_overall_confidence(conf)

    resolution = AskResolution(
        answer=llm_answer,
        sections=sections,
        sources=provenance_sources,
        confidence=conf,
        did_you_mean=did_you_mean,
        unknown_terms=unknown_terms,
        fallback_used=fallback_used,
    )

    ctx = AskContext(
        question=question,
        route=route,
        page_title=page_title,
        page_hint=page_hint,
        history=history,
        normalized_tokens=variants,
        intent=intent,
        effective_page_id=eff_page,
        runtime=runtime_dict,
        snowflake_fact_lookup=sf_attempted,
        retrieval_source_groups=bundle.source_groups_used,
    )
    return ctx, resolution
