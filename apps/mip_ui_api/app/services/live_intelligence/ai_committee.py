"""Event-driven committee: rule-based JSON by default; optional Ollama (local, free)."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any

AGENT_NAMES = [
    "TapeReader",
    "RiskOfficer",
    "ProfitDefender",
    "PatternHistorian",
    "NewsAnalyst",
    "Contrarian",
    "ExecutionSpecialist",
    "DevilsAdvocate",
]


def _agent_stub(
    name: str,
    tile: dict[str, Any],
    intel: dict[str, Any],
) -> dict[str, Any]:
    sym = intel.get("symbol") or tile.get("symbol")
    urg = intel.get("exit_urgency", "HOLD")
    thesis = intel.get("thesis_fracture", "THESIS_INTACT")
    feats = intel.get("derived_features") or {}
    stance = "NEUTRAL"
    action = "HOLD_WITH_MONITORING"
    if name == "RiskOfficer":
        stance = "ELEVATED" if urg in {"PREPARE", "EXIT_NOW"} else "CONTAINED"
        action = "ADD_PROTECTION" if urg == "EXIT_NOW" else "MONITOR"
    elif name == "ProfitDefender":
        stance = "DEFEND" if urg in {"PREPARE", "EXIT_NOW"} else "LET_RUN"
        action = "TRIM" if urg == "PREPARE" else "HOLD"
    elif name == "TapeReader":
        stance = str(feats.get("pattern_label") or "MIXED")
        action = "WATCH" if feats.get("pattern_label") == "VOLATILITY_SPIKE" else "HOLD"
    elif name == "PatternHistorian":
        stance = "ALIGNED" if thesis == "THESIS_INTACT" else "DIVERGENT"
        action = "REVIEW_HISTORICAL_ANALOG" if thesis != "THESIS_INTACT" else "HOLD"
    elif name == "NewsAnalyst":
        hot = any(str((e.get("meta") or {}).get("badge") or "").upper() in {"HOT", "RISK"} for e in (tile.get("events") or []))
        stance = "CATALYST_HOT" if hot else "LOW_SIGNAL"
        action = "WATCH" if hot else "NO_ACTION"
    elif name == "Contrarian":
        stance = "FADE_STRETCH" if intel.get("novelty_state") == "STRETCHED" else "NEUTRAL"
        action = "WAIT_FOR_MEAN_REVERSION" if stance == "FADE_STRETCH" else "HOLD"
    elif name == "ExecutionSpecialist":
        pref = (intel.get("action_simulation") or {}).get("preferred_ranking") or ["hold"]
        stance = f"PREFERS_{pref[0].upper()}"
        action = pref[0].upper()
    elif name == "DevilsAdvocate":
        stance = "CHALLENGE_THESIS" if thesis in {"THESIS_DAMAGED", "THESIS_BROKEN"} else "ACCEPT_SETUP"
        action = "FORCE_REVIEW" if stance == "CHALLENGE_THESIS" else "HOLD"

    evidence = {
        "exit_urgency": urg,
        "thesis_fracture": thesis,
        "pattern": feats.get("pattern_label"),
        "novelty": intel.get("novelty_state"),
        "symbol": sym,
    }
    return {
        "agent": name,
        "stance": stance,
        "confidence": "HIGH" if urg in {"PREPARE", "EXIT_NOW"} else "MEDIUM",
        "rationale": f"{name} synthesized from session evidence: {json.dumps(evidence, sort_keys=True)[:400]}",
        "supporting_evidence": [evidence],
        "risk_flags": [f for f in [urg if urg != "HOLD" else None, thesis if thesis != "THESIS_INTACT" else None] if f],
        "recommended_action": action,
        "what_changes_my_mind": "Material improvement in tape + thesis repair + vol normalization.",
    }


def _build_playbook(intel: dict[str, Any], tile: dict[str, Any]) -> list[dict[str, Any]]:
    sl = (tile.get("overlays") or {}).get("stop_loss")
    tp = (tile.get("overlays") or {}).get("take_profit")
    rules = []
    if sl is not None:
        rules.append(
            {
                "if_condition": f"Next bar closes below stop {sl}",
                "action": "reduce_50_pct",
                "observables": ["close", "stop_loss"],
            }
        )
    if tp is not None:
        rules.append(
            {
                "if_condition": f"Price tags take-profit {tp}",
                "action": "lock_gains_or_trail",
                "observables": ["high", "take_profit"],
            }
        )
    rules.append(
        {
            "if_condition": "Cone breach persists for 3 live cycles",
            "action": "prepare_exit",
            "observables": ["inside_cone", "cycles"],
        }
    )
    rules.append(
        {
            "if_condition": "Novelty OUTSIDE_COMFORT_ZONE and exit_urgency>=MONITOR",
            "action": "tighten_stop",
            "observables": ["novelty_state", "exit_urgency"],
        }
    )
    return rules


def _ollama_complete(prompt: str) -> str | None:
    base = (os.environ.get("OLLAMA_BASE_URL") or "").rstrip("/")
    model = os.environ.get("OLLAMA_MODEL") or "llama3.2"
    if not base:
        return None
    url = f"{base}/api/generate"
    payload = json.dumps({"model": model, "prompt": prompt, "stream": False}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
            return str(data.get("response") or "").strip() or None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return None


def run_ai_enrichment(
    body: dict[str, Any],
    *,
    now_ts: float | None = None,
    last_ai_by_symbol: dict[str, float] | None = None,
) -> dict[str, Any]:
    """
    Gating is caller's responsibility; this function enriches when invoked.
    last_ai_by_symbol: unix ts per symbol for cooldown (optional).
    """
    sym = str(body.get("symbol") or "").upper()
    intel = dict(body.get("intelligence") or {})
    tile = dict(body.get("deterministic_snapshot") or body.get("tile") or {})
    force = bool(body.get("force"))
    now = now_ts if now_ts is not None else time.time()
    cooldown = float(os.environ.get("LIVE_INTEL_AI_COOLDOWN_SEC") or "120")
    if last_ai_by_symbol and sym in last_ai_by_symbol and not force:
        if now - last_ai_by_symbol[sym] < cooldown:
            return {
                "agents": [],
                "playbook": [],
                "committee_headline": "AI cooldown active; use deterministic panel.",
                "used_ollama": False,
                "skipped": True,
            }

    agents = [_agent_stub(n, tile, intel) for n in AGENT_NAMES]
    playbook = _build_playbook(intel, tile)
    used_ollama = False
    prose = _ollama_complete(
        "One sentence committee headline given JSON: " + json.dumps({"symbol": sym, "urgency": intel.get("exit_urgency")})[:800]
    )
    headline = prose if prose else f"{sym}: committee review complete ({len(agents)} agents)."
    if prose:
        used_ollama = True

    return {
        "agents": agents,
        "playbook": playbook,
        "committee_headline": headline[:500],
        "used_ollama": used_ollama,
        "skipped": False,
    }
