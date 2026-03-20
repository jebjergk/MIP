"""
POST /ask — Ask MIP endpoint.

Accepts a user question (with optional conversation history and current route),
builds a system prompt grounded in the full User Guide, and calls Snowflake
Cortex COMPLETE to produce a knowledgeable answer.
"""

import json
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.config import get_askmip_model
from app.db import get_connection
from app.guide_loader import get_guide_content
from app.services.ask.glossary_repository import list_glossary, search_glossary, upsert_glossary_entry
from app.services.ask.orchestrator import resolve_question
from app.services.ask.telemetry import log_resolution_event

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ask", tags=["ask"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class HistoryMessage(BaseModel):
    role: str = Field(..., pattern="^(user|assistant)$")
    content: str


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=4000)
    route: str | None = None
    page_title: str | None = None
    page_hint: str | None = None
    history: list[HistoryMessage] = Field(default_factory=list)


class AskResponse(BaseModel):
    answer: str
    model: str
    section_id: str | None = None


class AskSource(BaseModel):
    source_type: str
    source_ref: str
    label: str
    confidence: float


class AskAnswerSection(BaseModel):
    section_type: str
    title: str
    text: str
    sources: list[AskSource] = Field(default_factory=list)


class AskConfidence(BaseModel):
    docs_confidence: float = 0.0
    glossary_confidence: float = 0.0
    web_confidence: float = 0.0
    overall: float = 0.0


class AskV2Response(BaseModel):
    answer: str
    model: str
    section_id: str | None = None
    sections: list[AskAnswerSection] = Field(default_factory=list)
    sources: list[AskSource] = Field(default_factory=list)
    confidence: AskConfidence = Field(default_factory=AskConfidence)
    did_you_mean: list[str] = Field(default_factory=list)
    unknown_terms: list[str] = Field(default_factory=list)
    fallback_used: bool = False


class GlossaryUpsertRequest(BaseModel):
    term_key: str = Field(..., min_length=1, max_length=150)
    display_term: str = Field(..., min_length=1, max_length=200)
    aliases: list[str] = Field(default_factory=list)
    category: str = "ui"
    definition_short: str = ""
    definition_long: str = ""
    mip_specific_meaning: str = ""
    general_market_meaning: str = ""
    example_in_mip: str = ""
    related_terms: list[str] = Field(default_factory=list)
    source_type: str = "MANUAL"
    source_ref: str = "ask_admin"
    is_approved: bool = False
    review_status: str = "pending"


class GlossaryReviewRequest(BaseModel):
    decision: str = Field(..., pattern="^(approved|rejected)$")
    notes: str = ""


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """\
You are **MIP Assistant**, the built-in help system for the Market Intelligence Platform (MIP).

## Knowledge Policy
Use the User Guide below as your primary source. Only state things as facts when they are
supported by the guide. If something is not covered, say that clearly.
The guide content may include an app-wide UI terms reference; use it to explain labels and metrics consistently.

<user_guide>
{guide_content}
</user_guide>

## Instructions
1. Answer clearly and thoroughly, using simple language first, then deeper detail.
2. If the question lacks scope (portfolio, symbol, time window, page, or mode), ask one
   short clarifying question before giving a detailed explanation.
3. When relevant, cite section names (for example: "See section 15: Training Status").
4. Use concrete examples and beginner-friendly analogies for technical concepts.
5. Never invent live values. For live-state questions, tell the user exactly where in the UI
   to check the answer (page + panel/table).
6. If a concept or term is not covered in the guide, say so explicitly and label any extra explanation
   as a best-effort inference. You may explain common finance/MIP terms in plain language when needed.
7. If the user asks about a UI label/metric (for example "Open R", "Thesis", or status badges),
   define the term first, then explain how to interpret it on the current page.
8. Prefer this response structure:
   - Short answer
   - Why this is true
   - Where to verify in UI
   - Optional deeper detail
9. The user is currently on route "{current_route}". Use route context only if it matches the
   question. If route context is not available or not relevant, say so.
"""


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post("", response_model=AskResponse)
def ask_mip(req: AskRequest):
    """Answer a user question using Cortex COMPLETE grounded in the User Guide."""
    model_name = get_askmip_model()
    guide_content = get_guide_content()

    # Build system prompt
    system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
        guide_content=guide_content,
        current_route=req.route or "/",
    )

    # Build a single prompt string (matches existing Cortex usage in stored procs)
    # System instructions + conversation history + current question
    prompt_parts = [system_prompt]

    # Append conversation history (last 10 turns max)
    for msg in req.history[-10:]:
        label = "User" if msg.role == "user" else "MIP Assistant"
        prompt_parts.append(f"\n{label}: {msg.content}")

    # Ensure the latest user question is the final message
    if not req.history or req.history[-1].content != req.question:
        prompt_parts.append(f"\nUser: {req.question}")

    prompt_parts.append("\nMIP Assistant:")

    full_prompt = "\n".join(prompt_parts)

    # Call Snowflake Cortex COMPLETE (single-prompt form, matching existing SP pattern)
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        # 120s statement timeout prevents indefinite hangs on slow Cortex responses
        cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 120")
        sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s) AS response"
        cur.execute(sql, (model_name, full_prompt))
        row = cur.fetchone()

        if not row or not row[0]:
            raise HTTPException(status_code=502, detail="Cortex returned an empty response.")

        raw_response = row[0]

        # Cortex may return a JSON string with a "choices" structure or plain text
        answer = _extract_answer(raw_response)

        logger.info(
            "Ask MIP answered question (model=%s, route=%s, answer_len=%d)",
            model_name,
            req.route,
            len(answer),
        )

        return AskResponse(answer=answer, model=model_name, section_id=None)

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Cortex COMPLETE failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=502,
            detail=f"Failed to get a response from the AI model: {exc}",
        ) from exc
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()


def _extract_answer(raw: str | dict) -> str:
    """
    Cortex COMPLETE returns different formats depending on the model and call style.
    Handle both plain text and JSON-wrapped responses.
    """
    if isinstance(raw, dict):
        # {"choices": [{"messages": "..."}]} format
        choices = raw.get("choices", [])
        if choices:
            msg = choices[0].get("messages", "") or choices[0].get("message", "")
            if isinstance(msg, dict):
                return msg.get("content", str(msg))
            return str(msg)
        return str(raw)

    if isinstance(raw, str):
        # Try to parse as JSON in case it's a stringified response
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return _extract_answer(parsed)
        except (json.JSONDecodeError, TypeError):
            pass
        return raw

    return str(raw)


@router.post("/v2", response_model=AskV2Response)
def ask_mip_v2(req: AskRequest):
    """
    Ask MIP v2:
    docs -> glossary -> confidence check -> optional web clarification (policy-gated),
    then assemble a provenance-aware response.
    """
    model_name = get_askmip_model()
    try:
        history = [{"role": m.role, "content": m.content} for m in req.history[-10:]]
        ctx, resolution = resolve_question(
            req.question,
            req.route,
            history,
            page_title=req.page_title,
            page_hint=req.page_hint,
        )
        log_resolution_event(ctx, resolution)
        return AskV2Response(
            answer=resolution.answer,
            model=model_name,
            section_id=None,
            sections=[
                AskAnswerSection(
                    section_type=section.section_type,
                    title=section.title,
                    text=section.text,
                    sources=[
                        AskSource(
                            source_type=source.source_type,
                            source_ref=source.source_ref,
                            label=source.label,
                            confidence=source.confidence,
                        )
                        for source in section.sources
                    ],
                )
                for section in resolution.sections
            ],
            sources=[
                AskSource(
                    source_type=source.source_type,
                    source_ref=source.source_ref,
                    label=source.label,
                    confidence=source.confidence,
                )
                for source in resolution.sources
            ],
            confidence=AskConfidence(
                docs_confidence=resolution.confidence.docs_confidence,
                glossary_confidence=resolution.confidence.glossary_confidence,
                web_confidence=resolution.confidence.web_confidence,
                overall=resolution.confidence.overall,
            ),
            did_you_mean=resolution.did_you_mean,
            unknown_terms=resolution.unknown_terms,
            fallback_used=resolution.fallback_used,
        )
    except Exception as exc:
        logger.error("Ask MIP v2 failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Ask MIP v2 failed.") from exc


@router.get("/glossary/search")
def ask_glossary_search(term: str):
    return {"items": search_glossary(term)}


@router.get("/glossary")
def ask_glossary_list(limit: int = 200):
    return {"items": list_glossary(limit)}


@router.post("/glossary")
def ask_glossary_upsert(req: GlossaryUpsertRequest):
    upsert_glossary_entry(
        {
            "term_key": req.term_key.lower().strip(),
            "display_term": req.display_term.strip(),
            "aliases": json.dumps(req.aliases),
            "category": req.category,
            "definition_short": req.definition_short,
            "definition_long": req.definition_long,
            "mip_specific_meaning": req.mip_specific_meaning,
            "general_market_meaning": req.general_market_meaning,
            "example_in_mip": req.example_in_mip,
            "related_terms": json.dumps(req.related_terms),
            "source_type": req.source_type,
            "source_ref": req.source_ref,
            "is_approved": req.is_approved,
            "review_status": req.review_status,
        }
    )
    return {"ok": True}


@router.patch("/glossary/{term_key}")
def ask_glossary_patch(term_key: str, req: GlossaryUpsertRequest):
    upsert_glossary_entry(
        {
            "term_key": term_key.lower().strip(),
            "display_term": req.display_term.strip(),
            "aliases": json.dumps(req.aliases),
            "category": req.category,
            "definition_short": req.definition_short,
            "definition_long": req.definition_long,
            "mip_specific_meaning": req.mip_specific_meaning,
            "general_market_meaning": req.general_market_meaning,
            "example_in_mip": req.example_in_mip,
            "related_terms": json.dumps(req.related_terms),
            "source_type": req.source_type,
            "source_ref": req.source_ref,
            "is_approved": req.is_approved,
            "review_status": req.review_status,
        }
    )
    return {"ok": True}


@router.get("/glossary/review-queue")
def ask_glossary_review_queue(limit: int = 200):
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT CANDIDATE_ID, TERM_TEXT, CATEGORY, SOURCE_TYPE, SOURCE_REF, RECOMMENDED_DEFINITION, REVIEW_STATUS, CREATED_AT
            FROM MIP.AGENT_OUT.GLOSSARY_CANDIDATE_TERM
            WHERE REVIEW_STATUS IN ('pending', 'needs_info')
            ORDER BY CREATED_AT DESC
            LIMIT %s
            """,
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return {"items": rows}
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()


@router.post("/glossary/review/{candidate_id}")
def ask_glossary_review(candidate_id: int, req: GlossaryReviewRequest):
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.AGENT_OUT.GLOSSARY_CANDIDATE_TERM
            SET REVIEW_STATUS = %s, REVIEWED_AT = CURRENT_TIMESTAMP()
            WHERE CANDIDATE_ID = %s
            """,
            (req.decision, candidate_id),
        )
        cur.execute(
            """
            INSERT INTO MIP.AGENT_OUT.GLOSSARY_REVIEW_EVENT (EVENT_TS, CANDIDATE_ID, DECISION, REVIEWER_NOTES)
            SELECT CURRENT_TIMESTAMP(), %s, %s, %s
            """,
            (candidate_id, req.decision, req.notes),
        )
        return {"ok": True}
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()


@router.get("/telemetry/coverage")
def ask_telemetry_coverage(limit: int = 30):
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM MIP.MART.V_ASK_COVERAGE_METRICS
            ORDER BY DAY DESC
            LIMIT %s
            """,
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return {"items": rows}
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()


@router.get("/telemetry/unknown-terms")
def ask_telemetry_unknown_terms(limit: int = 50):
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM MIP.MART.V_ASK_UNKNOWN_TERMS
            ORDER BY ASK_COUNT DESC
            LIMIT %s
            """,
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return {"items": rows}
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()
