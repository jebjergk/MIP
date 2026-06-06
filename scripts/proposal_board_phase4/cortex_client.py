"""
Phase 4 Cortex Agentic Proposal Board — Snowflake Cortex Agents REST API client.

Standalone equivalent of MIP/apps/mip_ui_api/app/committee/shadow_cortex_client.py
that reads connection details from the .env.agent file (same auth used by
cursorfiles/query_snowflake.py).

Provides:
  - generate_jwt(...)            JWT from the CURSOR_AGENT keypair
  - run_agent_object(...)        invoke a CREATE AGENT object (specialists, chair)
  - run_agent_objectless(...)    invoke ad-hoc AGENT_RUN (challenge, revision)
  - extract_agent_text(...)      pull final text from the response

The Cortex Agents REST schema is the post-2025-09-01 shape:
  - messages[].content is an array of typed content blocks
  - the objectless endpoint takes models.orchestration + instructions.system
  - non-streaming responses are { "role": "assistant", "content": [...] }
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import jwt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PublicFormat,
    load_pem_private_key,
)

logger = logging.getLogger(__name__)

_AGENT_OBJECT_URL = (
    "https://{account}.snowflakecomputing.com"
    "/api/v2/databases/MIP/schemas/APP/agents/{name}:run"
)
_AGENT_OBJECTLESS_URL = (
    "https://{account}.snowflakecomputing.com"
    "/api/v2/cortex/agent:run"
)

_JWT_LIFETIME_SEC = 59 * 60  # Snowflake max is 60


# ---------------------------------------------------------------------------
# JWT generation (RSA keypair auth)
# ---------------------------------------------------------------------------

def _load_private_key(path: str):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Phase 4 board private key not found: {path}")
    with p.open("rb") as f:
        return load_pem_private_key(f.read(), password=None, backend=default_backend())


def _public_key_fingerprint(private_key) -> str:
    pub_der = private_key.public_key().public_bytes(
        Encoding.DER, PublicFormat.SubjectPublicKeyInfo
    )
    digest = hashlib.sha256(pub_der).digest()
    return "SHA256:" + base64.b64encode(digest).decode("utf-8")


def generate_jwt(account: str, user: str, private_key_path: str) -> str:
    account_upper = account.upper()
    user_upper = user.upper()
    private_key = _load_private_key(private_key_path)
    fingerprint = _public_key_fingerprint(private_key)

    now = int(time.time())
    qualified_name = f"{account_upper}.{user_upper}"

    payload = {
        "iss": f"{qualified_name}.{fingerprint}",
        "sub": qualified_name,
        "iat": now,
        "exp": now + _JWT_LIFETIME_SEC,
    }
    return jwt.encode(payload, private_key, algorithm="RS256")


def _auth_headers(account: str, user: str, private_key_path: str) -> Dict[str, str]:
    token = generate_jwt(account, user, private_key_path)
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    }


# ---------------------------------------------------------------------------
# Message normalization
# ---------------------------------------------------------------------------

def _normalize_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Coerce each message's `content` to an array of typed content blocks.
    Drop any 'system' role entries — system instructions belong in
    instructions.system, never in messages.
    """
    out: List[Dict[str, Any]] = []
    for msg in messages:
        role = msg.get("role")
        if role == "system":
            logger.warning("phase4_cortex_client: dropping disallowed 'system' message")
            continue
        content = msg.get("content")
        if isinstance(content, str):
            content_blocks: List[Dict[str, Any]] = [{"type": "text", "text": content}]
        elif isinstance(content, list):
            content_blocks = content
        else:
            content_blocks = [{"type": "text", "text": "" if content is None else str(content)}]
        out.append({"role": role, "content": content_blocks})
    return out


# ---------------------------------------------------------------------------
# Transient-failure retry (timeouts / dropped connections / 5xx / 429)
# ---------------------------------------------------------------------------
#
# The dominant Phase 4 board failure was NOT bad model output — it was the HTTP
# call to the Cortex Agents endpoint timing out or the connection dropping
# mid-stream, which previously surfaced as an empty `{"error":""}` and silently
# discarded the whole symbol. These hiccups are almost always transient: a fresh
# attempt over a new connection typically returns quickly. We retry only on
# transport-level failures and retryable status codes, with exponential backoff
# and jitter, and we ALWAYS raise a non-empty, typed error when attempts are
# exhausted so the failure reason is visible in PROPOSAL_BOARD_OUTPUT_ERROR.

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_DEFAULT_MAX_ATTEMPTS = 3
_RETRY_BASE_DELAY_SEC = 2.0
_RETRY_MAX_DELAY_SEC = 20.0


def _is_retryable_exception(exc: Exception) -> bool:
    # httpx.TimeoutException covers Connect/Read/Write/Pool timeouts; the broader
    # httpx.TransportError covers connection resets, remote-protocol errors, etc.
    if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_STATUS
    return False


def _format_exc(exc: Exception) -> str:
    """A non-empty, typed description — httpx timeouts often have str(exc) == ''."""
    name = type(exc).__name__
    msg = str(exc).strip()
    if isinstance(exc, httpx.HTTPStatusError):
        return f"{name}: HTTP {exc.response.status_code} {msg}".strip()
    return f"{name}: {msg}" if msg else name


async def _post_agent_with_retries(
    *,
    url: str,
    body: Dict[str, Any],
    headers: Dict[str, str],
    timeout: float,
    label: str,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
) -> Dict[str, Any]:
    # Split timeout so a stalled connect fails fast (and is retried) while the
    # model still gets the full budget to generate its answer (read).
    timeout_cfg = httpx.Timeout(timeout, connect=min(15.0, timeout))
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            async with httpx.AsyncClient(timeout=timeout_cfg) as client:
                resp = await client.post(url, json=body, headers=headers)
                if resp.status_code >= 400:
                    logger.warning(
                        "phase4_agent[%s]: HTTP %s body=%s",
                        label, resp.status_code, resp.text[:1000],
                    )
                resp.raise_for_status()
                return resp.json()
        except Exception as e:  # noqa: BLE001
            last_exc = e
            retryable = _is_retryable_exception(e)
            logger.warning(
                "phase4_agent[%s]: attempt %d/%d failed (retryable=%s): %s",
                label, attempt, max_attempts, retryable, _format_exc(e),
            )
            if not retryable or attempt >= max_attempts:
                break
            delay = min(_RETRY_MAX_DELAY_SEC, _RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1)))
            delay += random.uniform(0.0, delay * 0.25)
            await asyncio.sleep(delay)

    detail = _format_exc(last_exc) if last_exc is not None else "unknown error"
    raise RuntimeError(
        f"Cortex agent call failed after {max_attempts} attempt(s) [{label}]: {detail}"
    ) from last_exc


# ---------------------------------------------------------------------------
# Persistent agent (CREATE AGENT) runner
# ---------------------------------------------------------------------------

async def run_agent_object(
    account: str,
    user: str,
    private_key_path: str,
    agent_name: str,
    messages: List[Dict[str, Any]],
    timeout: float = 180.0,
) -> Dict[str, Any]:
    url = _AGENT_OBJECT_URL.format(
        account=account.lower().replace("_", "-"),
        name=agent_name,
    )
    headers = _auth_headers(account, user, private_key_path)
    body = {
        "messages": _normalize_messages(messages),
        "stream": False,
    }

    logger.debug("phase4_agent_object: POST %s (agent=%s)", url, agent_name)
    return await _post_agent_with_retries(
        url=url, body=body, headers=headers, timeout=timeout,
        label=f"object:{agent_name}",
    )


# ---------------------------------------------------------------------------
# Objectless agent runner (ad-hoc instructions, used for challenge/revision)
# ---------------------------------------------------------------------------

async def run_agent_objectless(
    account: str,
    user: str,
    private_key_path: str,
    model: str,
    system_prompt: str,
    user_message: str,
    tools: Optional[List[Dict[str, Any]]] = None,
    tool_resources: Optional[Dict[str, Any]] = None,
    timeout: float = 120.0,
) -> Dict[str, Any]:
    url = _AGENT_OBJECTLESS_URL.format(
        account=account.lower().replace("_", "-")
    )
    headers = _auth_headers(account, user, private_key_path)

    body: Dict[str, Any] = {
        "models": {"orchestration": model},
        "instructions": {
            "system": system_prompt,
            "response": "Return ONLY the JSON object specified in the system prompt. No prose. No markdown fences.",
        },
        "messages": _normalize_messages([{"role": "user", "content": user_message}]),
        "stream": False,
    }
    if tools:
        body["tools"] = tools
    if tool_resources:
        body["tool_resources"] = tool_resources

    logger.debug("phase4_agent_objectless: POST %s (model=%s)", url, model)
    return await _post_agent_with_retries(
        url=url, body=body, headers=headers, timeout=timeout,
        label=f"objectless:{model}",
    )


# ---------------------------------------------------------------------------
# Response text extraction
# ---------------------------------------------------------------------------

def _extract_text_from_content_blocks(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, list):
        return ""
    texts: List[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            t = block.get("text")
            if isinstance(t, str) and t:
                texts.append(t)
            elif isinstance(t, dict):
                tt = t.get("text")
                if isinstance(tt, str) and tt:
                    texts.append(tt)
    return "\n".join(texts).strip()


def extract_agent_text(response: Dict[str, Any]) -> str:
    """
    Pull the final text content from a Cortex Agent response.

    Non-streaming response shape:
      { "role": "assistant", "content": [ {"type":"text","text":"..."}, ... ] }

    Legacy shapes (defensive):
      { "messages": [ { "role":"assistant", "content":[...] } ] }
      { "choices":  [ { "message": { "content":"..." } } ] }
    """
    if response.get("role") == "assistant":
        text = _extract_text_from_content_blocks(response.get("content"))
        if text:
            return text

    messages = response.get("messages") or []
    for msg in reversed(messages):
        if msg.get("role") == "assistant":
            text = _extract_text_from_content_blocks(msg.get("content"))
            if text:
                return text

    choices = response.get("choices") or []
    for choice in choices:
        msg = choice.get("message") or {}
        content = msg.get("content") or ""
        if isinstance(content, str) and content:
            return content.strip()
        if isinstance(content, list):
            text = _extract_text_from_content_blocks(content)
            if text:
                return text

    import json
    return json.dumps(response)
