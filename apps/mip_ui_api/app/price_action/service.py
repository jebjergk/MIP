from __future__ import annotations

import json
import logging
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from app.db import fetch_all, get_connection

from .geometry import Bar, analyse_geometry
from .models import (
    AnalyseRequest,
    AnalyseResponse,
    Annotation,
    DailyBar,
    ExpertRead,
    LongVerdictModel,
    MethodologistOutput,
    PlainExplanation,
)

logger = logging.getLogger(__name__)

MAX_RAG_CALLS = 3
MAX_RAG_RETRIES = 0
MAX_RAG_CARDS = 8
MAX_RAG_CARDS_PER_TOPIC = 2
MAX_RAG_CARD_CHARS = 900
MAX_RAG_TOTAL_CHARS = 8000
MAX_RAG_TIMEOUT_SECONDS = 10
MAX_LLM_CALLS = 1
LLM_TIMEOUT_SECONDS = 60
DEFAULT_LLM_MODEL = "claude-4-sonnet"
VERDICT_ALLOWLIST = {
    "LONG_APPROVE",
    "LONG_APPROVE_REDUCED",
    "WAIT_PULLBACK",
    "WAIT_RECLAIM",
    "DEFER",
    "REJECT",
    "NO_CLEAR_LONG",
}
RAG_TOPICS = ("MARKET_STRUCTURE", "PATTERN_RISK", "LONG_DECISION")
_CONFIG_KEYS = (
    "PRICE_ACTION_ANALYSER_ENABLED",
    "PRICE_ACTION_ANALYSER_PIVOT_LEFT",
    "PRICE_ACTION_ANALYSER_PIVOT_RIGHT",
    "PRICE_ACTION_ANALYSER_MAX_LOOKBACK_BARS",
    "PRICE_ACTION_ANALYSER_RAG_ENABLED",
    "PRICE_ACTION_ANALYSER_RAG_MAX_CALLS",
    "PRICE_ACTION_ANALYSER_RAG_MAX_RETRIES",
    "PRICE_ACTION_ANALYSER_RAG_MAX_TOTAL_CARDS",
    "PRICE_ACTION_ANALYSER_RAG_MAX_CARDS_PER_TOPIC",
    "PRICE_ACTION_ANALYSER_RAG_MAX_CARD_CHARS",
    "PRICE_ACTION_ANALYSER_RAG_MAX_TOTAL_CHARS",
    "PRICE_ACTION_ANALYSER_RAG_TIMEOUT_SECONDS",
    "PRICE_ACTION_ANALYSER_LLM_ENABLED",
    "PRICE_ACTION_ANALYSER_AUDIT_ENABLED",
)
_RAG_CONFIG_KEYS = tuple(key for key in _CONFIG_KEYS if "_RAG_" in key)
_FORBIDDEN_TEXT = re.compile(
    r"(?i)\b(?:author|book|chapter|page\s+\d+|raw\s+quote|source\s*:)\b|"
    r"\b(?:short|sell)\s+(?:the\s+)?(?:stock|asset|position|setup|trade)\b|"
    r"\b(?:enter|open|take|recommend)\s+(?:a\s+)?short\b"
)


def _methodologist_response_format() -> dict[str, Any]:
    string = {"type": "string"}
    string_array = {"type": "array", "items": string}
    expert_properties = {
        "market_context": string,
        "trend_state": string,
        "current_pattern": string,
        "support_resistance_read": string,
        "long_location_quality": string,
        "continuation_vs_failure_risk": string,
        "what_supports_the_long": string_array,
        "what_weakens_the_long": string_array,
        "confirmation_needed": string,
        "invalidation_logic": string,
    }
    plain_properties = {
        "summary": string,
        "what_the_chart_is_doing": string,
        "why_it_matters": string,
        "what_to_wait_for": string,
        "simple_risk_warning": string,
    }
    verdict_properties = {
        "decision": string,
        "confidence": {"type": "number"},
        "reason_summary": string,
        "not_a_short_recommendation": {"type": "boolean"},
    }
    annotation_properties = {
        "type": string,
        "label": string,
        "date": string,
        "price": {"type": "number"},
    }
    return {
        "type": "json",
        "schema": {
            "type": "object",
            "properties": {
                "symbol": string,
                "timeframe": string,
                "side": string,
                "analysis_status": string,
                "expert_read": {
                    "type": "object",
                    "properties": expert_properties,
                    "required": list(expert_properties),
                    "additionalProperties": False,
                },
                "plain_explanation": {
                    "type": "object",
                    "properties": plain_properties,
                    "required": list(plain_properties),
                    "additionalProperties": False,
                },
                "verdict": {
                    "type": "object",
                    "properties": verdict_properties,
                    "required": list(verdict_properties),
                    "additionalProperties": False,
                },
                "annotations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": annotation_properties,
                        "required": list(annotation_properties),
                        "additionalProperties": False,
                    },
                },
            },
            "required": [
                "symbol",
                "timeframe",
                "side",
                "analysis_status",
                "expert_read",
                "plain_explanation",
                "verdict",
                "annotations",
            ],
            "additionalProperties": False,
        },
    }


class PriceActionError(Exception):
    def __init__(self, message: str, status_code: int = 500):
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class RuntimeConfig:
    enabled: bool = False
    pivot_left: int = 2
    pivot_right: int = 2
    max_lookback_bars: int = 250
    rag_enabled: bool = False
    rag_max_calls: int = MAX_RAG_CALLS
    rag_max_retries: int = MAX_RAG_RETRIES
    rag_max_total_cards: int = MAX_RAG_CARDS
    rag_max_cards_per_topic: int = MAX_RAG_CARDS_PER_TOPIC
    rag_max_card_chars: int = MAX_RAG_CARD_CHARS
    rag_max_total_chars: int = MAX_RAG_TOTAL_CHARS
    rag_timeout_seconds: int = MAX_RAG_TIMEOUT_SECONDS
    llm_enabled: bool = False
    audit_enabled: bool = False
    llm_model: str = DEFAULT_LLM_MODEL


def _as_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _bounded_int(value: Any, default: int, low: int, hard_max: int) -> int:
    try:
        return max(low, min(int(value), hard_max))
    except (TypeError, ValueError):
        return default


def read_runtime_config() -> RuntimeConfig:
    """Read every feature flag/cap; absent controls disable the affected path."""
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
        values = {str(row["CONFIG_KEY"]): row.get("CONFIG_VALUE") for row in fetch_all(cur)}
        feature_enabled = (
            "PRICE_ACTION_ANALYSER_ENABLED" in values
            and _as_bool(values["PRICE_ACTION_ANALYSER_ENABLED"])
        )
        rag_config_complete = all(key in values for key in _RAG_CONFIG_KEYS)
        return RuntimeConfig(
            enabled=feature_enabled,
            pivot_left=_bounded_int(values.get("PRICE_ACTION_ANALYSER_PIVOT_LEFT"), 2, 1, 10),
            pivot_right=_bounded_int(values.get("PRICE_ACTION_ANALYSER_PIVOT_RIGHT"), 2, 1, 10),
            max_lookback_bars=_bounded_int(
                values.get("PRICE_ACTION_ANALYSER_MAX_LOOKBACK_BARS"), 250, 90, 250
            ),
            rag_enabled=(
                feature_enabled
                and rag_config_complete
                and _as_bool(values.get("PRICE_ACTION_ANALYSER_RAG_ENABLED"))
            ),
            rag_max_calls=_bounded_int(
                values.get("PRICE_ACTION_ANALYSER_RAG_MAX_CALLS"), MAX_RAG_CALLS, 0, MAX_RAG_CALLS
            ),
            rag_max_retries=MAX_RAG_RETRIES,
            rag_max_total_cards=_bounded_int(
                values.get("PRICE_ACTION_ANALYSER_RAG_MAX_TOTAL_CARDS"),
                MAX_RAG_CARDS,
                0,
                MAX_RAG_CARDS,
            ),
            rag_max_cards_per_topic=_bounded_int(
                values.get("PRICE_ACTION_ANALYSER_RAG_MAX_CARDS_PER_TOPIC"),
                MAX_RAG_CARDS_PER_TOPIC,
                0,
                MAX_RAG_CARDS_PER_TOPIC,
            ),
            rag_max_card_chars=_bounded_int(
                values.get("PRICE_ACTION_ANALYSER_RAG_MAX_CARD_CHARS"),
                MAX_RAG_CARD_CHARS,
                1,
                MAX_RAG_CARD_CHARS,
            ),
            rag_max_total_chars=_bounded_int(
                values.get("PRICE_ACTION_ANALYSER_RAG_MAX_TOTAL_CHARS"),
                MAX_RAG_TOTAL_CHARS,
                1,
                MAX_RAG_TOTAL_CHARS,
            ),
            rag_timeout_seconds=_bounded_int(
                values.get("PRICE_ACTION_ANALYSER_RAG_TIMEOUT_SECONDS"),
                MAX_RAG_TIMEOUT_SECONDS,
                1,
                MAX_RAG_TIMEOUT_SECONDS,
            ),
            llm_enabled=(
                feature_enabled
                and "PRICE_ACTION_ANALYSER_LLM_ENABLED" in values
                and _as_bool(values["PRICE_ACTION_ANALYSER_LLM_ENABLED"])
            ),
            audit_enabled=(
                feature_enabled
                and "PRICE_ACTION_ANALYSER_AUDIT_ENABLED" in values
                and _as_bool(values["PRICE_ACTION_ANALYSER_AUDIT_ENABLED"])
            ),
        )
    except Exception as exc:
        logger.warning(
            "price_action event=config_unavailable error_class=%s feature_disabled=true",
            type(exc).__name__,
        )
        return RuntimeConfig()
    finally:
        if conn is not None:
            conn.close()


def load_daily_bars(request: AnalyseRequest) -> list[Bar]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TS, OPEN, HIGH, LOW, CLOSE, VOLUME
              FROM MIP.MART.MARKET_BARS
             WHERE SYMBOL = %s
               AND INTERVAL_MINUTES = 1440
               AND CAST(TS AS DATE) <= COALESCE(%s, CURRENT_DATE())
             QUALIFY ROW_NUMBER() OVER (
                 PARTITION BY CAST(TS AS DATE)
                 ORDER BY TS DESC, MARKET_TYPE ASC
             ) = 1
             ORDER BY TS DESC
             LIMIT %s
            """,
            (request.symbol, request.as_of_date, request.lookback_bars),
        )
        rows = fetch_all(cur)
    except Exception as exc:
        logger.exception(
            "price_action event=bar_load_failed symbol=%s error_class=%s",
            request.symbol,
            type(exc).__name__,
        )
        raise PriceActionError("Unable to load canonical daily market bars.", 503) from exc
    finally:
        conn.close()
    bars = [Bar.from_row(row) for row in reversed(rows)]
    if not bars:
        raise PriceActionError(f"No daily market bars found for {request.symbol}.", 404)
    if len(bars) < 30:
        raise PriceActionError(f"Insufficient daily history for {request.symbol}; 30 bars required.", 422)
    return bars


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


def _rag_queries(geometry: dict[str, Any], situation: dict[str, Any]) -> list[tuple[str, str]]:
    structure = geometry["structure"]
    return [
        (
            "MARKET_STRUCTURE",
            f"LONG daily {structure['trend']} {structure['highs']} {structure['lows']} "
            f"range {situation['market_cycle']}",
        ),
        (
            "PATTERN_RISK",
            f"LONG daily pattern risk breakout {situation['breakout_followthrough']} "
            f"patterns {json.dumps(geometry['patterns'], separators=(',', ':'))}",
        ),
        (
            "LONG_DECISION",
            f"LONG daily decision location {situation['current_location']} "
            f"entry {situation['long_entry_quality']} breakout {situation['breakout_followthrough']}",
        ),
    ]


def _retrieve_topic(
    query: str,
    topic: str,
    top_k: int,
    side: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout_seconds}")
        cur.execute(
            "CALL MIP.KNOWLEDGE.SP_SEARCH_PRICE_ACTION_KNOWLEDGE(%s, %s, %s, %s)",
            (query[:2000], topic, top_k, side),
        )
        row = cur.fetchone()
        payload = _json_value(row[0]) if row else None
        if not isinstance(payload, dict):
            raise ValueError("Knowledge procedure returned invalid JSON")
        return payload
    finally:
        conn.close()


def _compact_card(raw: dict[str, Any], topic: str, config: RuntimeConfig) -> dict[str, Any] | None:
    card_id = str(raw.get("card_id") or raw.get("CARD_ID") or "").strip()
    view = str(raw.get("methodologist_view") or "")[: config.rag_max_card_chars].strip()
    if not card_id or _FORBIDDEN_TEXT.search(" ".join(str(value) for value in raw.values())):
        return None
    return {
        "card_id": card_id[:100],
        "topic": topic,
        "concept": str(raw.get("concept") or "Price-action concept")[:160],
        "methodologist_view": view,
        "relevance_reason": str(raw.get("relevance_reason") or "")[:300],
        "applies_to": "LONG_EVALUATION",
    }


def retrieve_literature(
    geometry: dict[str, Any],
    situation: dict[str, Any],
    config: RuntimeConfig,
    retrieve_fn: Callable[[str, str, int, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cards: list[dict[str, Any]] = []
    calls = 0
    errors = 0
    seen: set[str] = set()
    started = time.monotonic()
    if (
        config.rag_max_calls == 0
        or config.rag_max_total_cards == 0
        or config.rag_max_cards_per_topic == 0
    ):
        return {
            "rag_status": "NO_RELEVANT_CARDS",
            "retrieval_count": 0,
            "card_count": 0,
            "total_chars": 0,
            "cards": [],
            "retries": 0,
            "latency_ms": 0,
        }
    for topic, query in _rag_queries(geometry, situation):
        if calls >= config.rag_max_calls or len(cards) >= config.rag_max_total_cards:
            break
        calls += 1
        try:
            if retrieve_fn:
                payload = retrieve_fn(query, topic, config.rag_max_cards_per_topic, "LONG")
            else:
                payload = _retrieve_topic(
                    query,
                    topic,
                    config.rag_max_cards_per_topic,
                    "LONG",
                    config.rag_timeout_seconds,
                )
            if str(payload.get("status") or "").upper() == "UNAVAILABLE":
                errors += 1
                continue
            raw_cards = payload.get("results") or []
        except Exception as exc:
            errors += 1
            logger.warning(
                "price_action event=rag_call_failed topic=%s call=%s retries=0 error_class=%s",
                topic,
                calls,
                type(exc).__name__,
            )
            continue
        for raw in list(raw_cards)[: config.rag_max_cards_per_topic]:
            if not isinstance(raw, dict):
                continue
            card = _compact_card(raw, topic, config)
            if not card or card["card_id"] in seen:
                continue
            candidate_chars = len(json.dumps(cards + [card], separators=(",", ":")))
            if candidate_chars > config.rag_max_total_chars:
                break
            cards.append(card)
            seen.add(card["card_id"])
            if len(cards) >= config.rag_max_total_cards:
                break
    total_chars = len(json.dumps(cards, separators=(",", ":")))
    return {
        "rag_status": "USED" if cards else ("UNAVAILABLE" if errors else "NO_RELEVANT_CARDS"),
        "retrieval_count": calls,
        "card_count": len(cards),
        "total_chars": total_chars,
        "cards": cards,
        "retries": 0,
        "latency_ms": int((time.monotonic() - started) * 1000),
    }


def _extract_cortex_text(raw: Any) -> str:
    parsed = _json_value(raw)
    if isinstance(parsed, dict):
        choices = parsed.get("choices") or []
        if choices:
            message = choices[0].get("messages") or choices[0].get("message") or choices[0].get("text")
            if isinstance(message, dict):
                return str(message.get("content") or "")
            return str(message or "")
        return json.dumps(parsed, separators=(",", ":"))
    return str(raw or "")


def _grounded_annotation(item: dict[str, Any], geometry: dict[str, Any]) -> Annotation:
    allowed = {
        "type", "label", "date", "start_date", "end_date", "price",
        "y_min", "y_max", "confidence", "metadata",
    }
    if set(item) - allowed or item.get("type") != "TEXT_NOTE":
        raise ValueError("Methodologist annotation shape/type is not allowed")
    dates = {str(point["date"]) for point in geometry["ema20_series"]}
    if item.get("date") not in dates:
        raise ValueError("Methodologist annotation date is not grounded")
    grounded_prices = {
        float(geometry["latest"]["close"]),
        float(geometry["ema20"]["value"]),
        *(float(point["price"]) for point in geometry["swings"]),
        *(
            float(value)
            for zone in geometry["support_resistance_zones"]
            for value in (zone["low"], zone["high"])
        ),
    }
    prices = [
        float(item[key])
        for key in ("price", "y_min", "y_max")
        if item.get(key) is not None
    ]
    if not prices or any(not any(abs(price - known) <= 0.0001 for known in grounded_prices) for price in prices):
        raise ValueError("Methodologist annotation price is not grounded")
    return Annotation(source="methodologist_interpretation", **item)


def _validate_methodologist(payload: Any, geometry: dict[str, Any]) -> MethodologistOutput:
    exact = {
        "symbol",
        "timeframe",
        "side",
        "analysis_status",
        "expert_read",
        "plain_explanation",
        "verdict",
        "annotations",
    }
    if not isinstance(payload, dict) or set(payload) != exact:
        raise ValueError("LLM JSON shape is not exact")
    if payload["analysis_status"] != "OK":
        raise ValueError("LLM analysis_status must be OK")
    if payload["symbol"] != geometry["symbol"] or payload["timeframe"] != "1D" or payload["side"] != "LONG":
        raise ValueError("LLM identity fields do not match")
    verdict = payload["verdict"]
    if not isinstance(verdict, dict) or set(verdict) != {
        "decision", "confidence", "reason_summary", "not_a_short_recommendation"
    }:
        raise ValueError("LLM verdict shape is not exact")
    decision = str(verdict["decision"]).upper()
    if decision not in VERDICT_ALLOWLIST or verdict["not_a_short_recommendation"] is not True:
        raise ValueError("LLM long-only verdict contract failed")
    if not isinstance(payload["expert_read"], dict) or set(payload["expert_read"]) != {
        "market_context",
        "trend_state",
        "current_pattern",
        "support_resistance_read",
        "long_location_quality",
        "continuation_vs_failure_risk",
        "what_supports_the_long",
        "what_weakens_the_long",
        "confirmation_needed",
        "invalidation_logic",
    }:
        raise ValueError("LLM expert_read shape is not exact")
    if not isinstance(payload["plain_explanation"], dict) or set(payload["plain_explanation"]) != {
        "summary",
        "what_the_chart_is_doing",
        "why_it_matters",
        "what_to_wait_for",
        "simple_risk_warning",
    }:
        raise ValueError("LLM plain_explanation shape is not exact")
    text = json.dumps(payload, separators=(",", ":"))
    if _FORBIDDEN_TEXT.search(text):
        raise ValueError("LLM output failed source/long-only guard")
    raw_annotations = payload["annotations"]
    if not isinstance(raw_annotations, list):
        raise ValueError("LLM annotations must be an array")
    annotations = [_grounded_annotation(item, geometry) for item in raw_annotations[:6]]
    normalized = dict(payload)
    normalized["verdict"] = {**verdict, "decision": decision}
    normalized["annotations"] = annotations
    return MethodologistOutput(**normalized)


def deterministic_methodologist(
    geometry: dict[str, Any],
    situation: dict[str, Any],
    analysis_status: str = "GEOMETRY_ONLY",
) -> MethodologistOutput:
    trend = geometry["structure"]["trend"]
    location = geometry["long_location"]
    breakout = situation["breakout_followthrough"]
    if breakout == "FAILED":
        verdict = "WAIT_RECLAIM"
        explanation = "The upside break failed; a long case needs a reclaim before it becomes actionable."
    elif location == "EXTENDED_NEAR_HIGHS":
        verdict = "WAIT_PULLBACK"
        explanation = "Price is above trend support but extended near recent highs; wait for a better long location."
    elif trend == "UPTREND" and geometry["ema20"]["price_above"]:
        verdict = "LONG_APPROVE_REDUCED" if location == "NEUTRAL" else "LONG_APPROVE"
        explanation = "The daily structure and EMA20 alignment support a long-only setup at the detected location."
    elif trend == "DOWNTREND":
        verdict = "NO_CLEAR_LONG"
        explanation = "Daily swing structure does not currently provide a clear long setup."
    elif situation["market_cycle"] == "TRADING_RANGE":
        verdict = "DEFER"
        explanation = "Price is range-bound; defer until support holds clearly or resistance breaks."
    else:
        verdict = "NO_CLEAR_LONG"
        explanation = "The deterministic geometry is mixed, so there is no clear long setup."
    return MethodologistOutput(
        symbol=geometry["symbol"],
        timeframe="1D",
        side="LONG",
        analysis_status=analysis_status,
        expert_read=ExpertRead(
            market_context=(
                f"{trend}: {geometry['structure']['highs']} and "
                f"{geometry['structure']['lows']}."
            ),
            trend_state=trend,
            current_pattern=(
                f"Breakout state is {breakout}; market cycle is {situation['market_cycle']}."
            ),
            support_resistance_read=(
                f"{len(geometry['support_zones'])} support zones and "
                f"{len(geometry['resistance_zones'])} resistance zones were detected."
            ),
            long_location_quality=geometry["entry_location"]["long_location_quality"],
            continuation_vs_failure_risk=explanation,
            what_supports_the_long=[
                f"EMA20 state: {'ABOVE' if geometry['ema20']['price_above'] else 'BELOW'}",
                f"Long location: {location}",
            ],
            what_weakens_the_long=[
                f"Breakout state: {breakout}",
                f"Swing structure: {geometry['structure']['highs']}/"
                f"{geometry['structure']['lows']}",
            ],
            confirmation_needed=(
                "A supportive pullback, resistance breakout, or failed-breakout reclaim."
            ),
            invalidation_logic=(
                "The long thesis weakens if price loses the nearest detected support "
                "and remains below EMA20."
            ),
        ),
        plain_explanation=PlainExplanation(
            summary=explanation,
            what_the_chart_is_doing=(
                f"The daily chart is in a {trend.lower()} with price "
                f"{'above' if geometry['ema20']['price_above'] else 'below'} its 20-day average."
            ),
            why_it_matters=(
                "That combination determines whether a long is supported now or needs confirmation."
            ),
            what_to_wait_for=(
                "Wait for support to hold, a pullback to improve location, or a clean reclaim "
                "when the verdict is not an approval."
            ),
            simple_risk_warning=(
                "This is chart analysis only; a detected level can fail and is not a trade instruction."
            ),
        ),
        verdict=LongVerdictModel(
            decision=verdict,
            confidence=0.65 if verdict not in {"NO_CLEAR_LONG", "DEFER"} else 0.55,
            reason_summary=explanation,
            not_a_short_recommendation=True,
        ),
        annotations=[],
    )


def call_methodologist(
    model: str,
    geometry: dict[str, Any],
    situation: dict[str, Any],
    knowledge: list[dict[str, Any]],
    call_fn: Callable[[str, str], Any] | None = None,
) -> MethodologistOutput:
    compact_geometry = {
        key: geometry.get(key)
        for key in (
            "symbol",
            "timeframe",
            "latest",
            "structure",
            "range",
            "breakout",
            "breakout_failure",
            "patterns",
            "pullback_context",
            "entry_location",
            "market_cycle_context",
        )
    }
    compact_geometry["swing_points"] = geometry.get("swing_points", [])[-8:]
    compact_geometry["support_zones"] = geometry.get("support_zones", [])[:3]
    compact_geometry["resistance_zones"] = geometry.get("resistance_zones", [])[:3]
    prompt_payload = {
        "direction": "LONG",
        "detected_geometry": compact_geometry,
        "situation_model": situation,
        "methodologist_knowledge": knowledge,
    }
    prompt = (
        "You are the Price Action METHODOLOGIST. Interpret only supplied deterministic daily "
        "geometry for a LONG opportunity. Never name an author or book, reproduce a quote, or "
        "recommend a short position. Return strict JSON only with exactly: symbol, timeframe, side, "
        "analysis_status, expert_read, plain_explanation, verdict, annotations. analysis_status must "
        "be OK. expert_read must contain exactly market_context, trend_state, current_pattern, "
        "support_resistance_read, long_location_quality, continuation_vs_failure_risk, "
        "what_supports_the_long, what_weakens_the_long, confirmation_needed, invalidation_logic. "
        "plain_explanation must "
        "contain summary, what_the_chart_is_doing, why_it_matters, what_to_wait_for, "
        "simple_risk_warning. verdict must contain decision, confidence, reason_summary and "
        f"not_a_short_recommendation; decision must be one of {sorted(VERDICT_ALLOWLIST)} and "
        "not_a_short_recommendation must be true. Each annotation must have type TEXT_NOTE and use "
        "an exact date and price already "
        "present in detected_geometry; omit annotations unless grounded. Do not include source; "
        "the server assigns methodologist_interpretation.\n"
        + json.dumps(prompt_payload, separators=(",", ":"), default=str)
    )
    try:
        if call_fn:
            raw = call_fn(model, prompt)
        else:
            conn = get_connection()
            try:
                cur = conn.cursor()
                cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {LLM_TIMEOUT_SECONDS}")
                cur.execute(
                    """
                    SELECT AI_COMPLETE(
                        model => %(model)s,
                        prompt => %(prompt)s,
                        model_parameters => OBJECT_CONSTRUCT(
                            'temperature', 0,
                            'max_tokens', 2400
                        ),
                        response_format => PARSE_JSON(%(response_format)s)
                    )
                    """,
                    {
                        "model": model,
                        "prompt": prompt,
                        "response_format": json.dumps(_methodologist_response_format()),
                    },
                )
                row = cur.fetchone()
                raw = row[0] if row else None
            finally:
                conn.close()
        text = _extract_cortex_text(raw).strip()
        if not text.startswith("{") or not text.endswith("}"):
            raise ValueError("Cortex response was not strict JSON")
        return _validate_methodologist(json.loads(text), geometry)
    except Exception as exc:
        logger.warning(
            "price_action event=methodologist_unavailable calls=1 retries=0 error_class=%s",
            type(exc).__name__,
        )
        return deterministic_methodologist(geometry, situation, "METHODOLOGIST_UNAVAILABLE")


def _write_audit(
    response: AnalyseResponse,
    request: AnalyseRequest,
    rag_enabled: bool,
    duration_ms: int,
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.KNOWLEDGE.PRICE_ACTION_ANALYSER_AUDIT (
                ANALYSIS_ID, SYMBOL, SIDE, TIMEFRAME, LOOKBACK_BARS, AS_OF_DATE,
                RAG_ENABLED, RAG_STATUS, RAG_CALL_COUNT, CARD_COUNT, CARD_IDS,
                TOTAL_RAG_CHARS, GEOMETRY_SUMMARY_JSON, SITUATION_MODEL_JSON,
                METHODOLOGIST_OUTPUT_JSON, VERDICT, CONFIDENCE, ERROR_CLASS,
                ERROR_MESSAGE_TRUNC, LATENCY_MS
            )
            SELECT %s, %s, 'LONG', 'DAILY', %s, %s, %s, %s, %s, %s,
                   PARSE_JSON(%s), %s, PARSE_JSON(%s), PARSE_JSON(%s),
                   PARSE_JSON(%s), %s, %s, NULL, NULL, %s
            """,
            (
                response.analysis_id,
                response.symbol,
                response.lookback_bars,
                response.as_of_date,
                rag_enabled,
                response.rag_status,
                response.retrieval_count,
                response.card_count,
                json.dumps([card.get("card_id") for card in response.methodologist_knowledge]),
                response.total_chars,
                json.dumps(response.detected_geometry, default=str),
                json.dumps(response.situation_model, default=str),
                response.methodologist.model_dump_json(),
                response.methodologist.verdict.decision,
                response.methodologist.verdict.confidence,
                duration_ms,
            ),
        )
    finally:
        conn.close()


def analyse(
    request: AnalyseRequest,
    *,
    config: RuntimeConfig | None = None,
    bars: list[Bar] | None = None,
    retrieve_fn: Callable[[str, str, int, str], dict[str, Any]] | None = None,
    llm_fn: Callable[[str, str], Any] | None = None,
) -> AnalyseResponse:
    started = time.monotonic()
    runtime = config or read_runtime_config()
    if request.side != "LONG":
        raise PriceActionError("Price Action Analyser v1 supports LONG analysis only.", 400)
    if not runtime.enabled:
        raise PriceActionError("Price Action Analyser is disabled.", 503)
    if request.lookback_bars > runtime.max_lookback_bars:
        raise PriceActionError("Requested lookback exceeds the configured maximum.", 422)
    loaded_bars = bars or load_daily_bars(request)
    geometry, situation, detected_annotations = analyse_geometry(
        loaded_bars,
        runtime.pivot_left,
        runtime.pivot_right,
    )
    support_zones = [
        zone for zone in geometry["support_resistance_zones"] if zone["type"] == "SUPPORT"
    ]
    resistance_zones = [
        zone for zone in geometry["support_resistance_zones"] if zone["type"] == "RESISTANCE"
    ]
    pullback_pct = geometry["pullback"]["from_20_bar_high_pct"]
    pullback_quality = (
        "ORDERLY" if pullback_pct <= 8 else ("DAMAGING" if pullback_pct > 15 else "SHARP")
    )
    location_map = {
        "CONSTRUCTIVE_PULLBACK": ("GOOD", "LOW"),
        "NEUTRAL": ("ACCEPTABLE", "MODERATE"),
        "EXTENDED_NEAR_HIGHS": ("CHASING", "HIGH"),
        "UNFAVOURABLE": ("POOR", "HIGH"),
    }
    location_quality, chasing_risk = location_map.get(
        geometry["long_location"], ("UNCLEAR", "MODERATE")
    )
    structure = geometry["structure"]
    swing_structure = (
        "HH_HL" if structure["trend"] == "UPTREND"
        else ("LH_LL" if structure["trend"] == "DOWNTREND" else "MIXED")
    )
    market_cycle = (
        "FAILED_BREAKOUT" if geometry["breakout_failure"]
        else ("BREAKOUT" if geometry["breakout"] else (
            "TRADING_RANGE" if geometry["range"]["detected"] else (
                "TREND_PULLBACK" if structure["trend"] != "MIXED" else "UNCLEAR"
            )
        ))
    )
    recent_high = max(bar.high for bar in loaded_bars[-20:])
    bars_since_high = len(loaded_bars) - 1 - max(
        index for index, bar in enumerate(loaded_bars) if bar.high == recent_high
    )
    geometry.update({
        "symbol": request.symbol,
        "timeframe": "1D",
        "lookback_bars": request.lookback_bars,
        "as_of_date": loaded_bars[-1].day.isoformat(),
        "latest_close": loaded_bars[-1].close,
        "swing_points": geometry["swings"],
        "support_zones": support_zones,
        "resistance_zones": resistance_zones,
        "trendlines": [],
        "channels": [],
        "trading_ranges": geometry["range_boxes"],
        "breakout_events": ([{
            "direction": "UP",
            "breakout_date": geometry["breakout"]["date"],
            "level": geometry["breakout"]["level"],
            "follow_through_bars": 0 if geometry["breakout_failure"] else 1,
            "status": "FAILED" if geometry["breakout_failure"] else "HELD",
        }] if geometry["breakout"] else []),
        "failed_breakout_candidates": (
            [geometry["breakout_failure"]] if geometry["breakout_failure"] else []
        ),
        "wedge_candidates": (
            [geometry["patterns"]["wedge_candidate"]]
            if geometry["patterns"]["wedge_candidate"] else []
        ),
        "double_top_bottom_candidates": geometry["patterns"]["double_candidates"],
        "pullback_context": {
            "pullback_from_recent_high_pct": pullback_pct,
            "bars_since_recent_high": bars_since_high,
            "pullback_quality": pullback_quality,
            "held_above_recent_support": geometry["pullback"]["near_support"],
            "current_location": geometry["long_location"],
        },
        "entry_location": {
            "long_location_quality": location_quality,
            "distance_to_nearest_support_pct": (
                abs(support_zones[0]["distance_pct_from_close"]) if support_zones else None
            ),
            "distance_to_nearest_resistance_pct": (
                abs(resistance_zones[0]["distance_pct_from_close"]) if resistance_zones else None
            ),
            "risk_of_chasing": chasing_risk,
        },
        "market_cycle_context": {
            "swing_structure": swing_structure,
            "trend_state": (
                "TRADING_RANGE" if geometry["range"]["detected"] else structure["trend"]
            ),
            "latest_structure_event": (
                "BREAKOUT" if geometry["breakout"] else (
                    "PULLBACK" if pullback_pct > 0 else "NONE"
                )
            ),
        },
    })
    situation = {
        "side": "LONG",
        "timeframe": "1D",
        "trend_context": (
            f"{structure['trend']}_WITH_PULLBACK"
            if structure["trend"] != "MIXED" and pullback_pct > 0
            else structure["trend"]
        ),
        "swing_structure": swing_structure,
        "market_cycle": market_cycle,
        "current_location": geometry["long_location"],
        "support_context": (
            "recent swing support below" if support_zones else "no nearby support cluster"
        ),
        "resistance_context": (
            "recent swing resistance overhead"
            if resistance_zones else "no nearby resistance cluster"
        ),
        "breakout_followthrough": (
            "FAILED" if geometry["breakout_failure"] else (
                "HELD" if geometry["breakout"] else "NONE"
            )
        ),
        "pullback_quality": pullback_quality,
        "wedge_risk": "POSSIBLE" if geometry["wedge_candidates"] else "NONE",
        "long_entry_quality": location_quality,
        "primary_question": (
            "Is this a pursuable long now, or should it wait for pullback/reclaim?"
        ),
    }
    rag = (
        retrieve_literature(geometry, situation, runtime, retrieve_fn)
        if runtime.rag_enabled
        else {
            "rag_status": "DISABLED",
            "retrieval_count": 0,
            "card_count": 0,
            "total_chars": 0,
            "cards": [],
            "retries": 0,
        }
    )
    if runtime.llm_enabled:
        methodologist = call_methodologist(
            runtime.llm_model,
            geometry,
            situation,
            rag["cards"],
            llm_fn,
        )
    else:
        methodologist = deterministic_methodologist(geometry, situation)
    ema_by_date = {item["date"]: item["value"] for item in geometry["ema20_series"]}
    rendered_bars = [
        DailyBar(
            date=bar.day,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            ema20=ema_by_date[bar.day.isoformat()],
        )
        for bar in loaded_bars
    ]
    response = AnalyseResponse(
        analysis_id=str(uuid.uuid4()),
        symbol=request.symbol,
        lookback_bars=request.lookback_bars,
        as_of_date=loaded_bars[-1].day,
        bar_count=len(loaded_bars),
        bars=rendered_bars,
        current_price=loaded_bars[-1].close,
        detected_geometry=geometry,
        situation_model=situation,
        annotations=detected_annotations + methodologist.annotations,
        rag_status=rag["rag_status"],
        retrieval_count=rag["retrieval_count"],
        card_count=rag["card_count"],
        total_chars=rag["total_chars"],
        methodologist_knowledge=rag["cards"],
        methodologist=methodologist,
    )
    duration_ms = int((time.monotonic() - started) * 1000)
    if runtime.audit_enabled:
        try:
            _write_audit(response, request, runtime.rag_enabled, duration_ms)
        except Exception as exc:
            logger.warning(
                "price_action event=audit_failed analysis_id=%s error_class=%s",
                response.analysis_id,
                type(exc).__name__,
            )
    logger.info(
        "price_action event=analysis_complete analysis_id=%s symbol=%s bars=%s "
        "rag_status=%s rag_calls=%s methodologist_status=%s llm_calls=%s audit_enabled=%s "
        "duration_ms=%s",
        response.analysis_id,
        request.symbol,
        len(loaded_bars),
        response.rag_status,
        response.retrieval_count,
        response.methodologist.analysis_status,
        1 if runtime.llm_enabled else 0,
        runtime.audit_enabled,
        duration_ms,
    )
    return response
