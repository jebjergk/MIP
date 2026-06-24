"""
Phase 4 Proposal Board — bounded single-pass Snowflake AI_COMPLETE client.

Replaces Cortex Agent REST calls (uncapped tool loops) with one-shot COMPLETE
calls that have hard max_tokens, statement timeout, and optional JSON schema.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_STATEMENT_TIMEOUT_SEC = 120
DEFAULT_MAX_RETRIES = 1


@dataclass
class CompleteCallResult:
    parsed: Optional[Dict[str, Any]]
    raw_text: str
    usage: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


def _extract_complete_text(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, str):
        s = raw.strip()
        if s.startswith("{"):
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    if "structured_output" in obj and isinstance(obj["structured_output"], list):
                        parts = []
                        for block in obj["structured_output"]:
                            if isinstance(block, dict) and block.get("raw_message"):
                                parts.append(str(block["raw_message"]))
                        if parts:
                            return "\n".join(parts).strip()
                    if "choices" in obj:
                        choices = obj.get("choices") or []
                        if choices:
                            msg = choices[0].get("message") or choices[0].get("messages") or {}
                            if isinstance(msg, dict):
                                content = msg.get("content")
                                if isinstance(content, str):
                                    return content.strip()
                    for key in ("text", "response", "content", "message"):
                        if key in obj and isinstance(obj[key], str):
                            return obj[key].strip()
            except json.JSONDecodeError:
                pass
        return s
    if isinstance(raw, dict):
        for key in ("text", "response", "content"):
            val = raw.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        return json.dumps(raw)
    return str(raw)


def _parse_json_object(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    s = text.strip()
    # AI_COMPLETE often returns a JSON string literal whose value is JSON text.
    for _ in range(3):
        try:
            obj = json.loads(s)
            if isinstance(obj, dict):
                return obj
            if isinstance(obj, str):
                s = obj.strip()
                continue
            return None
        except json.JSONDecodeError:
            break
    first = s.find("{")
    last = s.rfind("}")
    if first >= 0 and last > first:
        try:
            obj = json.loads(s[first:last + 1])
            return obj if isinstance(obj, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def run_complete_json_sync(
    conn_factory: Callable[[], Any],
    *,
    model: str,
    system_prompt: str,
    user_message: str,
    response_format: Optional[Dict[str, Any]],
    max_tokens: int,
    statement_timeout_sec: int = DEFAULT_STATEMENT_TIMEOUT_SEC,
    label: str = "complete",
) -> CompleteCallResult:
    """Execute one AI_COMPLETE call (sync — run via asyncio.to_thread)."""
    prompt = (
        f"{system_prompt.strip()}\n\n"
        f"---\n\n"
        f"{user_message.strip()}\n\n"
        "Return ONLY the JSON object. No prose. No markdown fences."
    )
    conn = conn_factory()
    cur = conn.cursor()
    try:
        cur.execute(
            "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = %s",
            (int(statement_timeout_sec),),
        )
        if response_format:
            sql = """
                SELECT AI_COMPLETE(
                    model => %(model)s,
                    prompt => %(prompt)s,
                    model_parameters => OBJECT_CONSTRUCT(
                        'temperature', 0,
                        'max_tokens', %(max_tokens)s::INTEGER
                    ),
                    response_format => PARSE_JSON(%(response_format)s)
                ) AS result
            """
            params = {
                "model": model,
                "prompt": prompt,
                "max_tokens": int(max_tokens),
                "response_format": json.dumps(response_format),
            }
        else:
            sql = """
                SELECT AI_COMPLETE(
                    model => %(model)s,
                    prompt => %(prompt)s,
                    model_parameters => OBJECT_CONSTRUCT(
                        'temperature', 0,
                        'max_tokens', %(max_tokens)s::INTEGER
                    )
                ) AS result
            """
            params = {
                "model": model,
                "prompt": prompt,
                "max_tokens": int(max_tokens),
            }
        cur.execute(sql, params)
        row = cur.fetchone()
        raw = row[0] if row else None
        text = _extract_complete_text(raw)
        parsed = _parse_json_object(text)
        if parsed is None and isinstance(raw, str):
            parsed = _parse_json_object(raw)
        usage: Dict[str, Any] = {}
        if isinstance(raw, str):
            try:
                wrapper = json.loads(raw)
                if isinstance(wrapper, dict) and isinstance(wrapper.get("usage"), dict):
                    usage = wrapper["usage"]
            except json.JSONDecodeError:
                pass
        elif isinstance(raw, dict) and isinstance(raw.get("usage"), dict):
            usage = raw["usage"]
        if parsed is None and text:
            return CompleteCallResult(
                parsed=None,
                raw_text=text,
                usage=usage,
                error=f"{label}: non_object_or_unparseable_json",
            )
        return CompleteCallResult(parsed=parsed, raw_text=text, usage=usage)
    except Exception as exc:  # noqa: BLE001
        logger.warning("phase4_complete[%s] failed model=%s: %s", label, model, exc)
        return CompleteCallResult(
            parsed=None,
            raw_text="",
            error=f"{type(exc).__name__}: {exc}",
        )
    finally:
        try:
            cur.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


async def run_complete_json(
    conn_factory: Callable[[], Any],
    *,
    model: str,
    system_prompt: str,
    user_message: str,
    response_format: Optional[Dict[str, Any]],
    max_tokens: int,
    statement_timeout_sec: int = DEFAULT_STATEMENT_TIMEOUT_SEC,
    max_retries: int = DEFAULT_MAX_RETRIES,
    label: str = "complete",
) -> CompleteCallResult:
    """Bounded AI_COMPLETE with schema fallback and parse retry."""
    last: CompleteCallResult = CompleteCallResult(parsed=None, raw_text="", error="no_attempt")
    attempts = max(1, int(max_retries) + 1)
    use_schema = response_format is not None
    for attempt in range(1, attempts + 1):
        last = await asyncio.to_thread(
            run_complete_json_sync,
            conn_factory,
            model=model,
            system_prompt=system_prompt,
            user_message=user_message,
            response_format=response_format if use_schema else None,
            max_tokens=max_tokens,
            statement_timeout_sec=statement_timeout_sec,
            label=f"{label}:attempt{attempt}",
        )
        if last.parsed is not None:
            return last
        if use_schema and not last.raw_text:
            logger.warning("phase4_complete[%s] response_format empty — plain fallback", label)
            use_schema = False
            continue
        if attempt < attempts:
            logger.warning(
                "phase4_complete[%s] retrying parse failure attempt=%d/%d err=%s",
                label, attempt, attempts, last.error,
            )
    return last
