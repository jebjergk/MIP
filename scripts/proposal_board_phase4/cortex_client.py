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

import base64
import hashlib
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
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=body, headers=headers)
        if resp.status_code >= 400:
            logger.warning(
                "phase4_agent_object: %s -> HTTP %s body=%s",
                agent_name, resp.status_code, resp.text[:1000],
            )
        resp.raise_for_status()
        return resp.json()


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
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=body, headers=headers)
        if resp.status_code >= 400:
            logger.warning(
                "phase4_agent_objectless: model=%s -> HTTP %s body=%s",
                model, resp.status_code, resp.text[:1000],
            )
        resp.raise_for_status()
        return resp.json()


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
