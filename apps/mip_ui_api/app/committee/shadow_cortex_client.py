"""
Shadow Board Phase 1 — Snowflake Cortex Agents REST API client.

Provides:
  - JWT generation from RSA private key (keypair auth)
  - run_agent_object()      — persistent CREATE AGENT objects (specialists, chair)
  - run_agent_objectless()  — objectless AGENT_RUN (challenge, revision)
  - extract_agent_text()    — pull final text from agent response content blocks

Cortex Agents REST API contract (post-2025-09-01 schema):
  - messages[].content is an ARRAY of typed content blocks, e.g.
        {"role": "user", "content": [{"type": "text", "text": "..."}]}
    Plain string content is rejected with 400 Bad Request.
  - The objectless endpoint does NOT accept "system" as a message role.
    System instructions go into the top-level `instructions.system` field.
  - The model field for the objectless endpoint is `models.orchestration`,
    not a flat `model` string.
  - With `stream=false` the response body is NOT wrapped in a `messages`
    array. It is a single object: {"role": "assistant", "content": [...]}.
"""
from __future__ import annotations

import hashlib
import base64
import logging
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

# ---------------------------------------------------------------------------
# Cortex Agents REST API endpoints
# ---------------------------------------------------------------------------
_AGENT_OBJECT_URL = (
    "https://{account}.snowflakecomputing.com"
    "/api/v2/databases/MIP/schemas/APP/agents/{name}:run"
)
_AGENT_OBJECTLESS_URL = (
    "https://{account}.snowflakecomputing.com"
    "/api/v2/cortex/agent:run"
)

_JWT_LIFETIME_SEC = 59 * 60  # 59 minutes (Snowflake max is 60)


# ---------------------------------------------------------------------------
# JWT generation
# ---------------------------------------------------------------------------

def _load_private_key(path: str):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Shadow board private key not found: {path}")
    with p.open("rb") as f:
        return load_pem_private_key(f.read(), password=None, backend=default_backend())


def _public_key_fingerprint(private_key) -> str:
    pub_der = private_key.public_key().public_bytes(
        Encoding.DER, PublicFormat.SubjectPublicKeyInfo
    )
    digest = hashlib.sha256(pub_der).digest()
    return "SHA256:" + base64.b64encode(digest).decode("utf-8")


def generate_jwt(account: str, user: str, private_key_path: str) -> str:
    """
    Generate a short-lived JWT for Snowflake Cortex Agents REST API.
    account — e.g. 'PIDHQBT-PM44629' (no .snowflakecomputing.com suffix)
    user    — e.g. 'CURSOR_AGENT'
    """
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
    Normalize messages so each message's `content` is an array of typed
    content blocks (the format Cortex Agents REST requires). String content
    is wrapped as a single text block. Lists are passed through unchanged.

    Also drops any 'system' role entries — system prompts are NOT permitted
    in the messages array on Cortex Agents and produce a 400.
    """
    out: List[Dict[str, Any]] = []
    for msg in messages:
        role = msg.get("role")
        if role == "system":
            # Defensive: callers should never put system messages here.
            logger.warning("shadow_cortex_client: dropping disallowed 'system' message")
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
# Persistent agent (CREATE AGENT object) runner
# ---------------------------------------------------------------------------

async def run_agent_object(
    account: str,
    user: str,
    private_key_path: str,
    agent_name: str,
    messages: List[Dict[str, Any]],
    timeout: float = 120.0,
) -> Dict[str, Any]:
    """
    Call a persistent Cortex Agent (CREATE AGENT) via REST.
    Returns the raw response dict.
    Raises httpx.HTTPStatusError on non-2xx, httpx.TimeoutException on timeout.
    """
    url = _AGENT_OBJECT_URL.format(
        account=account.lower().replace("_", "-"),
        name=agent_name,
    )
    headers = _auth_headers(account, user, private_key_path)
    body = {
        "messages": _normalize_messages(messages),
        "stream": False,
    }

    logger.debug("shadow_agent_object: POST %s (agent=%s)", url, agent_name)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=body, headers=headers)
        if resp.status_code >= 400:
            # Surface Snowflake's error body so the caller logs explain *why* 4xx.
            logger.warning(
                "shadow_agent_object: %s -> HTTP %s body=%s",
                agent_name, resp.status_code, resp.text[:1000],
            )
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Objectless agent runner (AGENT_RUN with dynamic instructions)
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
    timeout: float = 90.0,
) -> Dict[str, Any]:
    """
    Run an objectless Cortex agent (inline config, no CREATE AGENT object required).
    Used for challenge and revision turns where instructions are dynamically assembled.

    Per the post-2025-09-01 Cortex Agents schema:
      - The orchestration model goes under `models.orchestration`.
      - The system prompt MUST go under `instructions.system`, NOT as a system
        message in the messages array.
    """
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

    logger.debug("shadow_agent_objectless: POST %s (model=%s)", url, model)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=body, headers=headers)
        if resp.status_code >= 400:
            logger.warning(
                "shadow_agent_objectless: model=%s -> HTTP %s body=%s",
                model, resp.status_code, resp.text[:1000],
            )
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Response text extraction
# ---------------------------------------------------------------------------

def _extract_text_from_content_blocks(content: Any) -> str:
    """
    Walk a Cortex Agents `content` array and return the concatenated text
    from any `{"type": "text", "text": "..."}` blocks. Tool-use, tool-result,
    and thinking blocks are ignored — only the final user-visible text matters
    for downstream JSON parsing.
    """
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, list):
        return ""
    texts: List[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        # New schema: top-level "text" field on type=="text" blocks.
        if block.get("type") == "text":
            t = block.get("text")
            if isinstance(t, str) and t:
                texts.append(t)
            # Some variants nest under "text": {"text": "..."}.
            elif isinstance(t, dict):
                tt = t.get("text")
                if isinstance(tt, str) and tt:
                    texts.append(tt)
    return "\n".join(texts).strip()


def extract_agent_text(response: Dict[str, Any]) -> str:
    """
    Pull the final text content from a Cortex Agent response.

    Non-streaming Cortex Agents REST response format (current schema):
      { "role": "assistant", "content": [ {"type":"text", "text":"..."}, ... ] }

    Legacy / fallback formats also handled defensively:
      { "messages": [ { "role":"assistant", "content":[...] } ] }
      { "choices": [ { "message": { "content":"..." } } ] }
    """
    # 1) Primary: top-level role+content (current non-streaming shape)
    if response.get("role") == "assistant":
        text = _extract_text_from_content_blocks(response.get("content"))
        if text:
            return text

    # 2) Legacy: messages array of role+content
    messages = response.get("messages") or []
    for msg in reversed(messages):
        if msg.get("role") == "assistant":
            text = _extract_text_from_content_blocks(msg.get("content"))
            if text:
                return text

    # 3) Fallback: choices format (some Cortex endpoints)
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

    # 4) Last resort: dump the whole response as string
    import json
    return json.dumps(response)
