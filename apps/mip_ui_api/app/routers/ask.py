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
    history: list[HistoryMessage] = []


class AskResponse(BaseModel):
    answer: str
    model: str
    section_id: str | None = None


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """\
You are **MIP Assistant**, the built-in help system for the Market Intelligence Platform (MIP).

## Your Knowledge Base
The complete MIP User Guide is provided below. Use it as your primary source of truth
when answering questions. Every concept, metric, page, and workflow described in the
guide is accurate and current.

<user_guide>
{guide_content}
</user_guide>

## Instructions
1. Answer the user's question thoroughly, clearly, and accurately based on the User Guide above.
2. When relevant, cite the section name (e.g. "See section 6: Trust & Eligibility") so the user
   can find more detail in the guide.
3. Use concrete examples and analogies to explain complex concepts like z-score, hit rate,
   training stages, trust labels, and the daily pipeline.
4. If the user asks about live data (e.g. "what is portfolio 1's status today?"), explain
   where they can find this information in the UI (which page, which panel) rather than
   inventing data you don't have.
5. If the user asks about something not covered in the guide, say so honestly and suggest
   where they might look.
6. Keep answers well-structured: use headings, bullet points, and bold text for readability.
7. Be conversational but precise — you are an expert system, not a chatbot.
8. The user is currently viewing the "{current_route}" page in the MIP UI. If their question
   seems related to this page, prioritize information about that page.
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
    try:
        cur = conn.cursor()
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
