"""
Shadow Board Phase 1 — Snowflake Cortex Agents REST API client.

Provides:
  - JWT generation from RSA private key (keypair auth)
  - run_agent_object()      — persistent CREATE AGENT objects (specialists, chair)
  - run_agent_objectless()  — objectless AGENT_RUN (challenge, revision)
  - extract_agent_text()    — pull final text from agent response content blocks
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
    body = {"messages": messages, "stream": False}

    logger.debug("shadow_agent_object: POST %s (agent=%s)", url, agent_name)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=body, headers=headers)
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
    """
    url = _AGENT_OBJECTLESS_URL.format(
        account=account.lower().replace("_", "-")
    )
    headers = _auth_headers(account, user, private_key_path)

    body: Dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "stream": False,
    }
    if tools:
        body["tools"] = tools
    if tool_resources:
        body["tool_resources"] = tool_resources

    logger.debug("shadow_agent_objectless: POST %s (model=%s)", url, model)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=body, headers=headers)
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Response text extraction
# ---------------------------------------------------------------------------

def extract_agent_text(response: Dict[str, Any]) -> str:
    """
    Pull the final text content from a Cortex Agent response.

    Cortex Agents REST response format:
      { "messages": [ { "role": "assistant", "content": [ {"type": "text", "text": "..."} ] } ] }
    or a simple:
      { "choices": [ { "message": { "content": "..." } } ] }
    """
    # Primary format: messages array
    messages = response.get("messages") or []
    for msg in reversed(messages):
        if msg.get("role") == "assistant":
            content = msg.get("content") or []
            if isinstance(content, str):
                return content.strip()
            if isinstance(content, list):
                texts = [
                    block.get("text", "")
                    for block in content
                    if isinstance(block, dict) and block.get("type") == "text"
                ]
                combined = " ".join(t for t in texts if t).strip()
                if combined:
                    return combined

    # Fallback: choices format (some Cortex endpoints)
    choices = response.get("choices") or []
    for choice in choices:
        msg = choice.get("message") or {}
        content = msg.get("content") or ""
        if content:
            return str(content).strip()

    # Last resort: dump the whole response as string
    import json
    return json.dumps(response)
