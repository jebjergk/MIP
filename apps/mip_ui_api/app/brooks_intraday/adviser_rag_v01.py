"""INTRADAY_ADVISER Cortex Search retrieval with stage-aware execution_relevance filters."""

from __future__ import annotations

import json
import re
from typing import Any, Literal

from app.db import get_connection

SERVICE = "MIP.KNOWLEDGE.LITERATURE_INTRADAY_ADVISER_SEARCH_SERVICE"

RELEVANCE_ENTRY_WATCH = frozenset(
    {"LONG_ENTRY", "LONG_CONTEXT", "LONG_FAILURE", "BEARISH_CONTEXT_ONLY"}
)
RELEVANCE_WITH_MANAGEMENT = RELEVANCE_ENTRY_WATCH | frozenset({"LONG_MANAGEMENT"})

SHORT_EXEC_RE = re.compile(
    r"\b(sell short|short entry|go short|take a short|short at market)\b", re.I
)

RetrievalIntent = Literal["entry_watch", "management"]


def allowed_relevance(
    *,
    position_state: str,
    intent: RetrievalIntent | None = None,
    explicit_management_question: bool = False,
) -> frozenset[str]:
    in_trade = position_state.upper() in ("IN_TRADE", "LONG")
    if intent == "management" or explicit_management_question or in_trade:
        return RELEVANCE_WITH_MANAGEMENT
    return RELEVANCE_ENTRY_WATCH


def is_management_query(query: str) -> bool:
    q = query.lower()
    return any(
        w in q
        for w in (
            "trade management",
            "protect",
            "exit",
            "climax",
            "scale out",
            "stop",
            "holding",
            "in trade",
            "existing long",
        )
    )


def filter_hits(
    hits: list[dict[str, Any]],
    allowed: frozenset[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for h in hits:
        rel = h.get("EXECUTION_RELEVANCE") or h.get("execution_relevance")
        if rel and rel not in allowed:
            continue
        if rel == "EXCLUDED_SHORT_EXECUTION":
            continue
        out.append(h)
    return out


def short_execution_leak(hit: dict[str, Any]) -> bool:
    text = " ".join(
        str(hit.get(k) or "")
        for k in ("DISPLAY_TEXT", "display_text", "CONCEPT_NAME", "concept_name", "SEARCH_TEXT")
    ).lower()
    return bool(SHORT_EXEC_RE.search(text))


def search_adviser_literature(
    query: str,
    *,
    limit: int = 8,
    position_state: str = "FLAT",
    intent: RetrievalIntent | None = None,
    explicit_management_question: bool = False,
    query_tag: str | None = None,
) -> list[dict[str, Any]]:
    """Search + post-filter. Returns up to `limit` cards after relevance filter."""
    mgmt = explicit_management_question or is_management_query(query)
    eff_intent: RetrievalIntent = intent or ("management" if mgmt else "entry_watch")
    allowed = allowed_relevance(
        position_state=position_state,
        intent=eff_intent,
        explicit_management_question=mgmt,
    )
    fetch = min(max(limit * 3, 12), 24)
    req = json.dumps(
        {
            "query": query[:2000],
            "columns": [
                "CARD_ID",
                "CONCEPT_NAME",
                "CONCEPT_FAMILY",
                "SOURCE_BOOK",
                "EXECUTION_RELEVANCE",
                "ADVISER_CLASS",
                "DISPLAY_TEXT",
            ],
            "limit": fetch,
        }
    )
    conn = get_connection()
    try:
        cur = conn.cursor()
        if query_tag:
            cur.execute(f"ALTER SESSION SET QUERY_TAG = '{query_tag}'")
        cur.execute(
            f"""
            SELECT PARSE_JSON(
                SNOWFLAKE.CORTEX.SEARCH_PREVIEW('{SERVICE}', %s)
            ) AS RAW
            """,
            (req,),
        )
        raw = cur.fetchone()[0]
        if isinstance(raw, str):
            raw = json.loads(raw)
        hits = raw.get("results") or []
    finally:
        conn.close()

    enriched: list[dict[str, Any]] = []
    for h in hits:
        cid = h.get("CARD_ID") or h.get("card_id")
        if cid:
            conn = get_connection()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT
                      RAW_CARD:execution_relevance::STRING,
                      RAW_CARD:execution_direction::STRING,
                      RAW_CARD:adviser_class::STRING
                    FROM MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT
                    WHERE RAG_SCOPE = 'INTRADAY_ADVISER' AND CARD_ID = %s
                    """,
                    (cid,),
                )
                row = cur.fetchone()
                if row:
                    h["EXECUTION_RELEVANCE"] = row[0]
                    h["EXECUTION_DIRECTION"] = row[1]
                    h["ADVISER_CLASS"] = row[2]
            finally:
                conn.close()
        enriched.append(h)

    filtered = filter_hits(enriched, allowed)
    return filtered[:limit]


def search_adviser_literature_balanced(
    primary_query: str,
    supplement_query: str | None,
    *,
    limit: int = 8,
    position_state: str = "FLAT",
    intent: RetrievalIntent | None = None,
    explicit_management_question: bool = False,
    query_tag: str | None = None,
) -> list[dict[str, Any]]:
    """Primary retrieval plus optional regime supplement, interleaved (no bearish suppression)."""
    from .adviser_retrieval_query_v01 import merge_balanced_hits

    primary = search_adviser_literature(
        primary_query,
        limit=limit,
        position_state=position_state,
        intent=intent,
        explicit_management_question=explicit_management_question,
        query_tag=query_tag,
    )
    if not supplement_query:
        return primary
    supplement = search_adviser_literature(
        supplement_query,
        limit=max(4, limit // 2),
        position_state=position_state,
        intent=intent,
        explicit_management_question=explicit_management_question,
        query_tag=query_tag,
    )
    return merge_balanced_hits(primary, supplement, limit=limit)
