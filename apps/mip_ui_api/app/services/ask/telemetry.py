from __future__ import annotations

import json

from app.db import get_connection
from app.services.ask.models import AskResolution, AskContext


def log_resolution_event(ctx: AskContext, resolution: AskResolution) -> None:
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.AGENT_OUT.ASK_QUERY_EVENT (
              QUERY_TS,
              QUESTION,
              ROUTE,
              INTENT,
              NORMALIZED_TERMS,
              MATCHED_SOURCE_TYPES,
              DOCS_CONFIDENCE,
              GLOSSARY_CONFIDENCE,
              WEB_CONFIDENCE,
              OVERALL_CONFIDENCE,
              WEB_FALLBACK_USED,
              ANSWER_FAILED,
              SUGGESTED_TERMS,
              UNKNOWN_TERMS
            )
            SELECT
              CURRENT_TIMESTAMP(),
              %s,
              %s,
              %s,
              PARSE_JSON(%s),
              PARSE_JSON(%s),
              %s, %s, %s, %s,
              %s,
              %s,
              PARSE_JSON(%s),
              PARSE_JSON(%s)
            """,
            (
                ctx.question,
                ctx.route,
                ctx.intent,
                json.dumps(ctx.normalized_tokens),
                json.dumps(sorted({source.source_type for source in resolution.sources})),
                resolution.confidence.docs_confidence,
                resolution.confidence.glossary_confidence,
                resolution.confidence.web_confidence,
                resolution.confidence.overall,
                resolution.fallback_used,
                not bool(resolution.answer.strip()),
                json.dumps(resolution.did_you_mean),
                json.dumps(resolution.unknown_terms),
            ),
        )
        if resolution.unknown_terms:
            for term in resolution.unknown_terms[:10]:
                cur.execute(
                    """
                    INSERT INTO MIP.AGENT_OUT.GLOSSARY_CANDIDATE_TERM (
                      TERM_TEXT, CATEGORY, SOURCE_TYPE, SOURCE_REF, RECOMMENDED_DEFINITION, REVIEW_STATUS
                    )
                    SELECT %s, 'unknown', 'ASK_QUERY', 'ask/v2', '', 'pending'
                    """,
                    (term,),
                )
    except Exception:
        # Keep Ask MIP resilient even if telemetry write fails.
        return
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()
