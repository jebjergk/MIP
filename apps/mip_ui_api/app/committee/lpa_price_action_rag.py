"""Cost-bounded advisory literature support for LONG LPA shadow sessions.

This module owns one retrieval at the Stage 0/1 boundary. It never retries,
falls back to another corpus, invokes an agent, or writes trading authority.
"""
from __future__ import annotations

import json
import logging
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from app.db import fetch_all, get_connection

logger = logging.getLogger(__name__)

PERSONA_LABEL = "Price Action & Trading Methodologist"
SCOPE = "LONG_ONLY_LPA_REVALIDATION"

MAX_RAG_CALLS_PER_LPA_ACTION = 1
MAX_RAG_RETRIES = 0
MAX_RAG_CARDS = 5
MAX_TOTAL_LITERATURE_SUPPORT_CHARS = 3000
MAX_SINGLE_CARD_CHARS = 900
TIMEOUT_SECONDS = 8

_CONFIG_KEYS = (
    "LPA_PRICE_ACTION_RAG_ENABLED",
    "LPA_PRICE_ACTION_RAG_LONG_ONLY",
    "LPA_PRICE_ACTION_RAG_MAX_CARDS",
    "LPA_PRICE_ACTION_RAG_MAX_QUERY_RETRIES",
    "LPA_PRICE_ACTION_RAG_MAX_SNIPPET_CHARS_PER_CARD",
    "LPA_PRICE_ACTION_RAG_MAX_TOTAL_CHARS",
    "LPA_PRICE_ACTION_RAG_FAIL_OPEN",
    "LPA_PRICE_ACTION_RAG_AUDIT_ENABLED",
)
_FORBIDDEN_TEXT = re.compile(
    r"(?is)(?:\n|\r)?source\s*:.*$|(?:\n|\r)?pp\.\s*\d+.*$|al\s+brooks|"
    r"failed breakout short|shorting below|use failed breakout short|"
    r"\bshort entry\b|\benter short\b|\bsell short\b"
)
_ALLOWED_SUPPORTS = {
    "APPROVE", "APPROVE_REDUCED", "WAIT_RECLAIM", "WAIT_PULLBACK",
    "DEFER", "REJECT", "MIXED",
}


@dataclass(frozen=True)
class RagConfig:
    enabled: bool = False
    long_only: bool = True
    max_cards: int = 3
    max_snippet_chars: int = MAX_SINGLE_CARD_CHARS
    max_total_chars: int = MAX_TOTAL_LITERATURE_SUPPORT_CHARS
    fail_open: bool = True
    audit_enabled: bool = True
    config_available: bool = True


def _bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _bounded_int(value: Any, default: int, low: int, high: int) -> int:
    try:
        return max(low, min(int(value), high))
    except (TypeError, ValueError):
        return default


def read_rag_config() -> RagConfig:
    """Read APP_CONFIG once. Any read error disables RAG, never UNAVAILABLE."""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        placeholders = ",".join(["%s"] * len(_CONFIG_KEYS))
        cur.execute(
            f"SELECT CONFIG_KEY, CONFIG_VALUE FROM MIP.APP.APP_CONFIG "
            f"WHERE CONFIG_KEY IN ({placeholders})",
            _CONFIG_KEYS,
        )
        raw = {str(r["CONFIG_KEY"]): r.get("CONFIG_VALUE") for r in fetch_all(cur)}
        # A missing master key is treated as an unavailable config read.
        if "LPA_PRICE_ACTION_RAG_ENABLED" not in raw:
            return RagConfig(enabled=False, config_available=False)
        return RagConfig(
            enabled=_bool(raw.get("LPA_PRICE_ACTION_RAG_ENABLED"), False),
            long_only=_bool(raw.get("LPA_PRICE_ACTION_RAG_LONG_ONLY"), True),
            max_cards=_bounded_int(raw.get("LPA_PRICE_ACTION_RAG_MAX_CARDS"), 3, 1, MAX_RAG_CARDS),
            max_snippet_chars=_bounded_int(
                raw.get("LPA_PRICE_ACTION_RAG_MAX_SNIPPET_CHARS_PER_CARD"),
                MAX_SINGLE_CARD_CHARS, 1, MAX_SINGLE_CARD_CHARS,
            ),
            max_total_chars=_bounded_int(
                raw.get("LPA_PRICE_ACTION_RAG_MAX_TOTAL_CHARS"),
                MAX_TOTAL_LITERATURE_SUPPORT_CHARS, 1, MAX_TOTAL_LITERATURE_SUPPORT_CHARS,
            ),
            fail_open=_bool(raw.get("LPA_PRICE_ACTION_RAG_FAIL_OPEN"), True),
            audit_enabled=_bool(raw.get("LPA_PRICE_ACTION_RAG_AUDIT_ENABLED"), True),
        )
    except Exception as exc:
        logger.warning("LPA_RAG config_read_failed=%s treating_as_disabled=true", type(exc).__name__)
        return RagConfig(enabled=False, config_available=False)
    finally:
        if conn is not None:
            conn.close()


def disabled_slice() -> Dict[str, Any]:
    return {"enabled": False, "status": "DISABLED"}


def _base_slice(status: str) -> Dict[str, Any]:
    return {
        "enabled": True,
        "status": status,
        "persona_label": PERSONA_LABEL,
        "scope": SCOPE,
        "retrieval_count": 0,
        "cards": [],
        "methodologist_effect": "NO_MATERIAL_EFFECT",
        "cost_guard": {
            "retrievals_attempted": 0,
            "retries": 0,
            "max_cards": 3,
            "max_total_chars": MAX_TOTAL_LITERATURE_SUPPORT_CHARS,
        },
    }


def derive_intraday_15m_status(picture: Optional[Dict[str, Any]]) -> str:
    p = picture or {}
    if not p.get("session_available"):
        reason = str(p.get("reason") or "").upper()
        return "ERROR_FALLBACK_TO_PRIOR" if "ERROR" in reason else "UNAVAILABLE"
    bucket = str(p.get("verdict_bucket") or "").upper()
    if bucket == "SUPPORTS":
        return "USED_SUPPORTIVE"
    if bucket == "MIXED":
        return "MIXED_NOISE"
    return "NOT_SUPPORTIVE_FALLBACK_TO_PRIOR"


def derive_entry_zone_state(entry: Dict[str, Any]) -> str:
    low, high, price = entry.get("zone_low"), entry.get("zone_high"), entry.get("latest_price")
    try:
        low_f, high_f, price_f = float(low), float(high), float(price)
    except (TypeError, ValueError):
        return "ENTRY_ZONE_MISSED"
    if low_f <= price_f <= high_f:
        return "INSIDE_ENTRY_ZONE"
    width = max(high_f - low_f, high_f * 0.005, 0.01)
    if price_f < low_f:
        return "NEAR_ENTRY_ZONE" if low_f - price_f <= width else "BELOW_ENTRY_ZONE"
    extension = (price_f - high_f) / max(high_f, 0.01)
    if extension <= 0.02:
        return "ABOVE_ENTRY_ZONE_NOT_EXTENDED"
    if extension <= 0.05:
        return "ABOVE_ENTRY_ZONE_EXTENDED"
    return "ENTRY_ZONE_MISSED"


def build_query(pack_slices: Dict[str, Any]) -> str:
    meta = pack_slices.get("proposal_meta") or {}
    thesis = pack_slices.get("phase4_thesis_verdict") or {}
    summary = pack_slices.get("thesis_summary") or {}
    zone = pack_slices.get("entry_zone") or {}
    invalidation = pack_slices.get("invalidation") or {}
    dossier = pack_slices.get("phase4_dossier_context") or {}
    intraday = pack_slices.get("intraday_session_picture") or {}
    fields = [
        f"Symbol: {meta.get('symbol') or ''}",
        "Direction: LONG",
        f"Setup: {meta.get('setup_family') or summary.get('setup_family') or ''}",
        f"Proposal thesis: {thesis.get('final_thesis') or summary.get('proposal_summary') or ''}",
        f"Entry zone: {zone.get('zone_low')} to {zone.get('zone_high')}",
        f"Current price: {zone.get('latest_price')}",
        f"Entry-zone state: {derive_entry_zone_state(zone)}",
        f"Invalidation: {invalidation.get('invalidation_level')}; breached={invalidation.get('invalidation_breached')}",
        f"Support: {dossier.get('nearest_support')}; resistance: {dossier.get('nearest_resistance')}",
        f"15-minute RTH status: {derive_intraday_15m_status(intraday)}",
        f"Intraday summary: {intraday.get('operator_line') or ''}",
        "Question: Given this LONG setup, is the long opportunity still pursuable now without chasing?",
    ]
    return "\n".join(fields)[:4000]


def _clean_text(value: Any) -> str:
    text = _FORBIDDEN_TEXT.sub("", str(value or ""))
    return re.sub(r"\s+", " ", text).strip()


def _compact_cards(raw_cards: Any, config: RagConfig) -> tuple[list[Dict[str, Any]], int]:
    cards: list[Dict[str, Any]] = []
    remaining = config.max_total_chars
    for raw in list(raw_cards or [])[: config.max_cards]:
        if not isinstance(raw, dict) or remaining <= 0:
            break
        raw_text = " ".join(
            str(raw.get(k) or "")
            for k in ("title", "methodologist_view", "relevance_reason", "snippet")
        )
        if re.search(
            r"(?i)failed breakout short|shorting below|use failed breakout short|"
            r"\bshort entry\b|\benter short\b|\bsell short\b",
            raw_text,
        ):
            continue
        card_id = _clean_text(raw.get("card_id") or raw.get("CARD_ID"))
        title = _clean_text(raw.get("title") or raw.get("CONCEPT_NAME"))
        view = _clean_text(raw.get("methodologist_view"))
        reason = _clean_text(raw.get("relevance_reason"))
        snippet = _clean_text(raw.get("snippet"))[: config.max_snippet_chars]
        if not card_id:
            continue
        supports = str(raw.get("supports") or "MIXED").upper()
        if supports not in _ALLOWED_SUPPORTS:
            supports = "MIXED"
        # Allocate the remaining global text budget without fabricating detail.
        title = title[: min(180, remaining)]
        remaining -= len(title)
        view = view[: min(450, max(remaining, 0))]
        remaining -= len(view)
        reason = reason[: min(240, max(remaining, 0))]
        remaining -= len(reason)
        snippet = snippet[: max(remaining, 0)]
        remaining -= len(snippet)
        cards.append({
            "card_id": card_id,
            "title": title,
            "methodologist_view": view,
            "relevance_reason": reason,
            "supports": supports,
            "snippet": snippet,
        })
    return cards, config.max_total_chars - remaining


def _json_variant(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return None
    return value


def _load_replay(session_id: str) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT LITERATURE_SUPPORT_JSON FROM MIP.KNOWLEDGE.LPA_PRICE_ACTION_RAG_AUDIT "
            "WHERE SHADOW_SESSION_ID = %s LIMIT 1",
            (session_id,),
        )
        rows = fetch_all(cur)
        return _json_variant(rows[0].get("LITERATURE_SUPPORT_JSON")) if rows else None
    finally:
        conn.close()


def _acquire_guard(
    *,
    session_id: str,
    action_id: Optional[str],
    hearing_id: str,
    proposal_id: int,
    symbol: str,
    evidence_pack_hash: Optional[str],
    query_text: str,
    config: RagConfig,
) -> bool:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.KNOWLEDGE.LPA_PRICE_ACTION_RAG_AUDIT
                (AUDIT_ID, ACTION_ID, SYMBOL, SIDE, PROPOSAL_ID, HEARING_ID,
                 SHADOW_SESSION_ID, EVIDENCE_PACK_HASH, RAG_ENABLED, RAG_STATUS,
                 QUERY_TEXT, TOP_K_REQUESTED, RETRY_COUNT, METHODOLOGIST_EFFECT)
            SELECT %s, %s, %s, 'LONG', %s, %s, %s, %s, TRUE, 'PENDING',
                   %s, %s, 0, 'NO_MATERIAL_EFFECT'
            WHERE NOT EXISTS (
                SELECT 1 FROM MIP.KNOWLEDGE.LPA_PRICE_ACTION_RAG_AUDIT
                 WHERE SHADOW_SESSION_ID = %s
            )
            """,
            (
                str(uuid.uuid4()), action_id, symbol, proposal_id, hearing_id,
                session_id, evidence_pack_hash,
                query_text if config.audit_enabled else None,
                config.max_cards, session_id,
            ),
        )
        return int(getattr(cur, "rowcount", 0) or 0) > 0
    finally:
        conn.close()


def _execute_search(query_text: str, max_cards: int) -> Dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {TIMEOUT_SECONDS}")
        cur.execute(
            "CALL MIP.KNOWLEDGE.SP_SEARCH_REVALIDATION_LITERATURE(%s, %s, %s)",
            (query_text, max_cards, "LONG"),
        )
        row = cur.fetchone()
        payload = _json_variant(row[0]) if row else None
        return payload if isinstance(payload, dict) else {
            "status": "UNAVAILABLE", "error": "empty_or_invalid_response", "results": [],
        }
    finally:
        conn.close()


def _finish_audit(session_id: str, support: Dict[str, Any], latency_ms: int, retrieval_id: Optional[str]) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cards = support.get("cards") or []
        cur.execute(
            """
            UPDATE MIP.KNOWLEDGE.LPA_PRICE_ACTION_RAG_AUDIT
               SET COMPLETED_AT = CURRENT_TIMESTAMP(),
                   RAG_STATUS = %s,
                   CARD_IDS_RETURNED = PARSE_JSON(%s),
                   CARD_COUNT = %s,
                   TOTAL_SNIPPET_CHARS = %s,
                   APPROX_PROMPT_CHARS_ADDED = %s,
                   RETRIEVAL_LATENCY_MS = %s,
                   RETRY_COUNT = 0,
                   ERROR_CLASS = %s,
                   ERROR_MESSAGE_TRUNC = %s,
                   RETRIEVAL_ID = %s,
                   LITERATURE_SUPPORT_JSON = PARSE_JSON(%s),
                   METHODOLOGIST_EFFECT = 'NO_MATERIAL_EFFECT'
             WHERE SHADOW_SESSION_ID = %s
            """,
            (
                support.get("status"),
                json.dumps([c.get("card_id") for c in cards]),
                len(cards),
                int(support.get("cost_guard", {}).get("total_chars") or 0),
                len(json.dumps(support, default=str)),
                latency_ms,
                support.get("error_class"),
                str(support.get("error") or "")[:500] or None,
                retrieval_id,
                json.dumps(support, default=str),
                session_id,
            ),
        )
    finally:
        conn.close()


def get_literature_support(
    *,
    session_id: str,
    hearing_id: str,
    proposal_id: int,
    symbol: str,
    side: str,
    pack_slices: Dict[str, Any],
    action_id: Optional[str] = None,
    evidence_pack_hash: Optional[str] = None,
    config: Optional[RagConfig] = None,
    search_fn: Optional[Callable[[str, int], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Return a frozen literature_support slice. Never raises to the committee."""
    cfg = config or read_rag_config()
    if not cfg.config_available or not cfg.enabled:
        return disabled_slice()
    if cfg.long_only and str(side or "").upper() != "LONG":
        support = _base_slice("SKIPPED_SIDE_NOT_LONG")
        support["cost_guard"]["max_cards"] = cfg.max_cards
        return support
    try:
        replay = _load_replay(session_id)
        if isinstance(replay, dict):
            replay["replayed_from_audit"] = True
            logger.info(
                "LPA_RAG action_id=%s symbol=%s side=LONG status=%s replay=true",
                action_id, symbol, replay.get("status"),
            )
            return replay

        query_text = build_query(pack_slices)
        acquired = _acquire_guard(
            session_id=session_id,
            action_id=action_id,
            hearing_id=hearing_id,
            proposal_id=proposal_id,
            symbol=symbol,
            evidence_pack_hash=evidence_pack_hash,
            query_text=query_text,
            config=cfg,
        )
        if not acquired:
            replay = _load_replay(session_id)
            if isinstance(replay, dict):
                replay["replayed_from_audit"] = True
                return replay
            support = _base_slice("UNAVAILABLE")
            support["error_class"] = "RetrievalAlreadyInProgress"
            return support

        started = time.monotonic()
        payload = (search_fn or _execute_search)(query_text, cfg.max_cards)
        latency_ms = int((time.monotonic() - started) * 1000)
        status = str(payload.get("status") or "").upper()
        if payload.get("error") or status == "UNAVAILABLE":
            support = _base_slice("UNAVAILABLE")
            support["error_class"] = "LiteratureRetrievalError"
            support["error"] = str(payload.get("error") or "retrieval unavailable")[:500]
            retrieval_id = payload.get("retrieval_id")
        else:
            cards, total_chars = _compact_cards(payload.get("results"), cfg)
            support = _base_slice("USED" if cards else "NO_RELEVANT_LONG_CARDS")
            support["retrieval_count"] = len(cards)
            support["cards"] = cards
            support["intraday_15m_status"] = derive_intraday_15m_status(
                pack_slices.get("intraday_session_picture")
            )
            support["entry_zone_state"] = derive_entry_zone_state(pack_slices.get("entry_zone") or {})
            support["cost_guard"].update({
                "retrievals_attempted": 1,
                "max_cards": cfg.max_cards,
                "max_total_chars": cfg.max_total_chars,
                "total_chars": total_chars,
            })
            retrieval_id = payload.get("retrieval_id")
        _finish_audit(session_id, support, latency_ms, retrieval_id)
        logger.info(
            "LPA_RAG action_id=%s symbol=%s side=LONG status=%s card_count=%s chars=%s latency_ms=%s",
            action_id, symbol, support.get("status"), len(support.get("cards") or []),
            support.get("cost_guard", {}).get("total_chars", 0), latency_ms,
        )
        return support
    except Exception as exc:
        support = _base_slice("UNAVAILABLE")
        support["error_class"] = type(exc).__name__
        support["error"] = str(exc)[:500]
        logger.warning(
            "LPA_RAG action_id=%s status=UNAVAILABLE error_class=%s retries=0 "
            "continuing_without_rag=true",
            action_id, type(exc).__name__,
        )
        # Best effort only. A failure to update audit must not mask fail-open.
        try:
            _finish_audit(session_id, support, 0, None)
        except Exception:
            pass
        return support


def update_methodologist_effect(
    session_id: str,
    literature_support: Optional[Dict[str, Any]],
    effect: Dict[str, Any],
    final_verdict: str,
    confidence: float,
) -> Dict[str, Any]:
    """Persist advisory attribution only; never updates authority tables."""
    support_status = str((literature_support or {}).get("status") or "").upper()
    normalized = dict(effect or {})
    if support_status != "USED":
        normalized = {
            "used": False,
            "effect": "NO_MATERIAL_EFFECT",
            "summary": "",
        }
    normalized["effect"] = str(
        normalized.get("effect") or "NO_MATERIAL_EFFECT"
    ).upper()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.KNOWLEDGE.LPA_PRICE_ACTION_RAG_AUDIT
               SET METHODOLOGIST_EFFECT = %s,
                   CHAIR_FINAL_VERDICT = %s,
                   CHAIR_CONFIDENCE = %s
             WHERE SHADOW_SESSION_ID = %s
            """,
            (
                normalized["effect"][:40],
                str(final_verdict or "")[:40],
                float(confidence or 0.0),
                session_id,
            ),
        )
    finally:
        conn.close()
    return normalized
