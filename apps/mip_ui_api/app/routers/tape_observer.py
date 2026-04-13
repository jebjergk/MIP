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


def _tape_observer_health_diagnostics(base: str) -> dict | None:
    try:
        url = f"{base.rstrip('/')}/health"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=4) as resp:
            data = json.loads(resp.read().decode())
            diag = data.get("ib_host_diagnostics")
            return diag if isinstance(diag, dict) else None
    except Exception:
        return None


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
            body = json.loads(resp.read().decode())
        diag = _tape_observer_health_diagnostics(base)
        if diag is None:
            try:
                from app.integrations.ibkr_read_host import diagnostics_template, merge_diagnostics

                diag = merge_diagnostics(
                    diagnostics_template("tape_observer"),
                    surface_freshness="unknown",
                    surface_freshness_reason="tape_health_unreachable",
                    tape_transport_state="unknown",
                )
            except Exception:
                diag = None
        if diag is not None:
            body = dict(body)
            body["ib_host_diagnostics"] = diag
        return body
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
