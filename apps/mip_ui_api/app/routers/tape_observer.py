"""Proxy to mip_market_observer Tape snapshot (optional)."""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request

from fastapi import APIRouter, HTTPException, Query

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/observation/tape", tags=["tape"])


def _tape_base_url() -> str | None:
    raw = (os.getenv("TAPE_OBSERVER_BASE_URL") or "").strip().rstrip("/")
    return raw or None


@router.get("/v1/snapshot")
def tape_snapshot_proxy(symbol: str = Query(..., min_length=1, max_length=32)):
    base = _tape_base_url()
    if not base:
        raise HTTPException(
            status_code=503,
            detail="Tape observer not configured (set TAPE_OBSERVER_BASE_URL, e.g. http://127.0.0.1:8095).",
        )
    q = urllib.parse.urlencode({"symbol": symbol.strip().upper()})
    url = f"{base}/tape/v1/snapshot?{q}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=6) as resp:
            body = resp.read().decode()
            return json.loads(body)
    except urllib.error.HTTPError as e:
        _log.warning("Tape observer HTTP %s: %s", e.code, e.reason)
        raise HTTPException(status_code=502, detail=f"Tape observer HTTP {e.code}") from e
    except Exception as exc:
        _log.warning("Tape observer proxy failed: %s", exc)
        raise HTTPException(status_code=502, detail="Tape observer unreachable") from exc


@router.get("/v1/snapshot/debug")
def tape_snapshot_debug_proxy(symbol: str = Query(..., min_length=1, max_length=32)):
    base = _tape_base_url()
    if not base:
        raise HTTPException(
            status_code=503,
            detail="Tape observer not configured (set TAPE_OBSERVER_BASE_URL).",
        )
    q = urllib.parse.urlencode({"symbol": symbol.strip().upper()})
    url = f"{base}/tape/v1/snapshot/debug?{q}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = resp.read().decode()
            return json.loads(body)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise HTTPException(status_code=404, detail="Tape debug disabled on observer") from e
        raise HTTPException(status_code=502, detail=f"Tape observer HTTP {e.code}") from e
    except Exception as exc:
        _log.warning("Tape debug proxy failed: %s", exc)
        raise HTTPException(status_code=502, detail="Tape observer unreachable") from exc
