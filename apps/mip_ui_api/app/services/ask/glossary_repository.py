from __future__ import annotations

import json
from difflib import SequenceMatcher

from app.db import fetch_all, get_connection
from app.services.ask.normalize import normalize_text, tokenize


def _build_aliases(term: str, aliases_json: str | None) -> list[str]:
    aliases: list[str] = [term]
    if aliases_json:
        try:
            parsed = json.loads(aliases_json)
            if isinstance(parsed, list):
                aliases.extend([str(item) for item in parsed])
        except Exception:
            pass
    return [a for a in aliases if a]


def find_glossary_matches(question: str) -> tuple[list[dict], float]:
    q_norm = normalize_text(question)
    q_tokens = set(tokenize(question))
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              TERM_KEY,
              DISPLAY_TERM,
              ALIASES,
              CATEGORY,
              DEFINITION_SHORT,
              DEFINITION_LONG,
              MIP_SPECIFIC_MEANING,
              GENERAL_MARKET_MEANING,
              EXAMPLE_IN_MIP,
              IS_APPROVED
            FROM MIP.APP.GLOSSARY_TERM
            WHERE IS_APPROVED = TRUE
            """
        )
        rows = fetch_all(cur)
    except Exception:
        rows = []
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()

    scored: list[tuple[float, dict]] = []
    for row in rows:
        display = str(row.get("DISPLAY_TERM") or row.get("TERM_KEY") or "")
        aliases = _build_aliases(display, row.get("ALIASES"))
        best = 0.0
        for alias in aliases:
            a_norm = normalize_text(alias)
            if not a_norm:
                continue
            exact = 1.0 if q_norm == a_norm or f" {a_norm} " in f" {q_norm} " else 0.0
            overlap = len(set(tokenize(alias)).intersection(q_tokens)) / max(1.0, float(len(set(tokenize(alias)))))
            fuzzy = SequenceMatcher(None, q_norm, a_norm).ratio()
            best = max(best, exact, overlap * 0.85, fuzzy * 0.75)
        if best >= 0.45:
            scored.append((best, row))
    scored.sort(key=lambda x: x[0], reverse=True)
    if not scored:
        return [], 0.0
    matches = [item[1] for item in scored[:5]]
    return matches, scored[0][0]


def search_glossary(term: str) -> list[dict]:
    matches, _ = find_glossary_matches(term)
    return matches


def list_glossary(limit: int = 200) -> list[dict]:
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY, DEFINITION_SHORT, IS_APPROVED, REVIEW_STATUS, UPDATED_AT
            FROM MIP.APP.GLOSSARY_TERM
            ORDER BY UPDATED_AT DESC
            LIMIT %s
            """,
            (limit,),
        )
        return fetch_all(cur)
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()


def upsert_glossary_entry(payload: dict) -> None:
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            MERGE INTO MIP.APP.GLOSSARY_TERM t
            USING (
              SELECT
                %s AS TERM_KEY,
                %s AS DISPLAY_TERM,
                %s AS ALIASES,
                %s AS CATEGORY,
                %s AS DEFINITION_SHORT,
                %s AS DEFINITION_LONG,
                %s AS MIP_SPECIFIC_MEANING,
                %s AS GENERAL_MARKET_MEANING,
                %s AS EXAMPLE_IN_MIP,
                %s AS RELATED_TERMS,
                %s AS SOURCE_TYPE,
                %s AS SOURCE_REF,
                %s AS IS_APPROVED,
                %s AS REVIEW_STATUS
            ) s
            ON t.TERM_KEY = s.TERM_KEY
            WHEN MATCHED THEN UPDATE SET
              DISPLAY_TERM = s.DISPLAY_TERM,
              ALIASES = s.ALIASES,
              CATEGORY = s.CATEGORY,
              DEFINITION_SHORT = s.DEFINITION_SHORT,
              DEFINITION_LONG = s.DEFINITION_LONG,
              MIP_SPECIFIC_MEANING = s.MIP_SPECIFIC_MEANING,
              GENERAL_MARKET_MEANING = s.GENERAL_MARKET_MEANING,
              EXAMPLE_IN_MIP = s.EXAMPLE_IN_MIP,
              RELATED_TERMS = s.RELATED_TERMS,
              SOURCE_TYPE = s.SOURCE_TYPE,
              SOURCE_REF = s.SOURCE_REF,
              IS_APPROVED = s.IS_APPROVED,
              REVIEW_STATUS = s.REVIEW_STATUS,
              UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
              TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY, DEFINITION_SHORT, DEFINITION_LONG,
              MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING, EXAMPLE_IN_MIP, RELATED_TERMS,
              SOURCE_TYPE, SOURCE_REF, IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
            )
            VALUES (
              s.TERM_KEY, s.DISPLAY_TERM, s.ALIASES, s.CATEGORY, s.DEFINITION_SHORT, s.DEFINITION_LONG,
              s.MIP_SPECIFIC_MEANING, s.GENERAL_MARKET_MEANING, s.EXAMPLE_IN_MIP, s.RELATED_TERMS,
              s.SOURCE_TYPE, s.SOURCE_REF, s.IS_APPROVED, s.REVIEW_STATUS, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
            """,
            (
                payload.get("term_key"),
                payload.get("display_term"),
                payload.get("aliases", "[]"),
                payload.get("category", "ui"),
                payload.get("definition_short", ""),
                payload.get("definition_long", ""),
                payload.get("mip_specific_meaning", ""),
                payload.get("general_market_meaning", ""),
                payload.get("example_in_mip", ""),
                payload.get("related_terms", "[]"),
                payload.get("source_type", "MANUAL"),
                payload.get("source_ref", "ask_admin"),
                bool(payload.get("is_approved", False)),
                payload.get("review_status", "pending"),
            ),
        )
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()
