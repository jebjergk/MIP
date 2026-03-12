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
    history: list[HistoryMessage] = Field(default_factory=list)


class AskResponse(BaseModel):
    answer: str
    model: str
    section_id: str | None = None


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """\
You are **MIP Assistant**, the built-in help system for the Market Intelligence Platform (MIP).

## Knowledge Policy
Use the User Guide below as your primary source. Only state things as facts when they are
supported by the guide. If something is not covered, say that clearly.

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
6. If a concept is not covered in the guide, say so explicitly and label any extra explanation
   as a best-effort inference.
7. Prefer this response structure:
   - Short answer
   - Why this is true
   - Where to verify in UI
   - Optional deeper detail
8. The user is currently on route "{current_route}". Use route context only if it matches the
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
