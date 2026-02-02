"""
Deterministic audit interpreter: turns pipeline step rows (MIP_AUDIT_LOG DETAILS JSON)
into phases, "what happened and why" sections, and narrative. No LLM; pure if/else and lookup.
DETAILS keys from 145_sp_run_daily_pipeline: step_name, scope, ingest_status, reason (NO_NEW_BARS), etc.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

# Pipeline phase order (align with SP_RUN_DAILY_PIPELINE)
PHASE_ORDER = [
    "ingestion",
    "returns_refresh",
    "recommendations",
    "evaluation",
    "trusted_signal_refresh",
    "portfolio_simulation",
    "morning_brief",
    "proposer",
    "executor",
    "agent_run_all",
]

PHASE_LABELS = {
    "ingestion": "Ingest",
    "returns_refresh": "Returns",
    "recommendations": "Recommendations",
    "evaluation": "Evaluation",
    "trusted_signal_refresh": "Trust gating",
    "portfolio_simulation": "Portfolio simulation",
    "morning_brief": "Morning brief",
    "proposer": "Proposer",
    "executor": "Executor",
    "agent_run_all": "Agent run",
}


def _parse_ts(v: Any) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _duration_seconds(started_at: Any, completed_at: Any) -> float | None:
    s = _parse_ts(started_at)
    c = _parse_ts(completed_at)
    if s and c:
        return (c - s).total_seconds()
    return None


def _get(d: dict | None, key: str, default: Any = None) -> Any:
    if d is None or not isinstance(d, dict):
        return default
    return d.get(key, default)


def _details(row: dict) -> dict:
    details = row.get("DETAILS")
    if isinstance(details, str):
        try:
            return json.loads(details) if details else {}
        except json.JSONDecodeError:
            return {}
    return details if isinstance(details, dict) else {}


def _step_name(row: dict) -> str:
    d = _details(row)
    return (_get(d, "step_name") or row.get("EVENT_NAME") or "unknown").strip().lower().replace(" ", "_")


def step_to_summary_card(row: dict) -> dict:
    """Build one summary card from an audit row."""
    details = _details(row)
    step_name = _step_name(row)
    status = row.get("STATUS") or _get(details, "status") or "UNKNOWN"
    started_at = _get(details, "started_at")
    completed_at = _get(details, "completed_at")
    duration_sec = _duration_seconds(started_at, completed_at)
    rows_affected = row.get("ROWS_AFFECTED")
    portfolio_count = _get(details, "portfolio_count")

    return {
        "step_name": step_name,
        "event_name": row.get("EVENT_NAME"),
        "status": status,
        "started_at": started_at.isoformat() if hasattr(started_at, "isoformat") else started_at,
        "completed_at": completed_at.isoformat() if hasattr(completed_at, "isoformat") else completed_at,
        "duration_seconds": duration_sec,
        "rows_affected": rows_affected,
        "portfolio_count": portfolio_count,
        "error_message": row.get("ERROR_MESSAGE"),
    }


STEP_NARRATIVE = {
    "ingestion": "Ingestion completed.",
    "returns_refresh": "Returns refresh completed.",
    "recommendations": "Recommendations step completed.",
    "evaluation": "Evaluation step completed.",
    "portfolio_simulation": "Portfolio simulation completed.",
    "trusted_signal_refresh": "Trusted signal refresh completed.",
    "morning_brief": "Morning brief step completed.",
    "proposer": "Proposer step completed.",
    "executor": "Executor step completed.",
    "agent_run_all": "Agent run all completed.",
    "SP_RUN_DAILY_PIPELINE": "Pipeline run completed.",
}


def step_to_narrative_bullet(row: dict) -> str:
    """One short narrative bullet for this step."""
    details = _details(row)
    step_name = _step_name(row)
    status = (row.get("STATUS") or "").upper()
    err = row.get("ERROR_MESSAGE")
    portfolio_count = _get(details, "portfolio_count")

    if status == "FAIL" and err:
        return f"Step {step_name or row.get('EVENT_NAME', 'unknown')} failed: {err}"

    if status == "SKIPPED_NO_NEW_BARS":
        return f"{PHASE_LABELS.get(step_name, step_name)} was skipped (no new market data)."
    if status in ("SKIP_RATE_LIMIT", "SUCCESS_WITH_SKIPS"):
        if step_name == "ingestion":
            return "Ingestion completed with skips or rate limit; downstream may be skipped if no new bars."

    template = STEP_NARRATIVE.get(step_name) or STEP_NARRATIVE.get((row.get("EVENT_NAME") or "").strip())
    if template:
        if portfolio_count is not None and "portfolio" in step_name:
            return f"Portfolio simulation completed: {portfolio_count} portfolio(s)."
        return template
    if portfolio_count is not None:
        return f"Step {step_name or row.get('EVENT_NAME')}: {portfolio_count} portfolio(s)."
    return f"Step {step_name or row.get('EVENT_NAME', 'unknown')} completed."


# --- What happened and why (structured sections) ---

def _is_skip_or_noop(row: dict) -> bool:
    status = (row.get("STATUS") or "").upper()
    return status in ("SKIPPED_NO_NEW_BARS", "SKIP_RATE_LIMIT") or "SKIP" in status


def _section_no_new_bars(phase_key: str, phase_label: str) -> dict:
    return {
        "headline": f"{phase_label} was skipped",
        "what_happened": f"The pipeline did not run the {phase_label.lower()} step.",
        "why": "No new market bars were available after ingestion, so downstream steps were skipped to avoid stale work.",
        "impact": "No new recommendations, evaluations, or portfolio updates from this run.",
        "next_check": "Run again after new market data is available, or check ingestion and data source.",
    }


def _section_rate_limit(phase_label: str) -> dict:
    return {
        "headline": f"{phase_label} hit rate limit or partial success",
        "what_happened": "Ingestion completed with skips or a rate limit.",
        "why": "The data provider may have throttled requests or some symbols were skipped.",
        "impact": "Fewer bars than expected; downstream may have been skipped if no new bars.",
        "next_check": "Check ingestion logs and provider limits; consider spacing runs or reducing scope.",
    }


def _section_fail(phase_label: str, error_message: str | None) -> dict:
    return {
        "headline": f"{phase_label} failed",
        "what_happened": f"The {phase_label.lower()} step failed.",
        "why": error_message or "An error occurred during this step.",
        "impact": "Pipeline did not complete; later steps may not have run.",
        "next_check": "Fix the error and re-run the pipeline; check ERROR_MESSAGE in the timeline.",
    }


def _section_success(phase_label: str, details: dict, row: dict) -> dict | None:
    """Optional success section (e.g. one-line clarity). Return None to omit."""
    portfolio_count = _get(details, "portfolio_count")
    if portfolio_count is not None and "portfolio" in phase_label.lower():
        return {
            "headline": f"{phase_label} completed",
            "what_happened": f"Portfolio simulation ran for {portfolio_count} portfolio(s).",
            "why": "Step completed successfully.",
            "impact": "Portfolios were updated.",
            "next_check": None,
        }
    return None


def _row_to_section(row: dict, phase_key: str, phase_label: str) -> dict | None:
    """Produce one structured section for a step row if it's a skip/no-op/fail or notable success."""
    details = _details(row)
    status = (row.get("STATUS") or "").upper()
    reason = _get(details, "reason") or ""
    err = row.get("ERROR_MESSAGE")

    if status == "SKIPPED_NO_NEW_BARS" or reason == "NO_NEW_BARS":
        return _section_no_new_bars(phase_key, phase_label)
    if status == "SKIP_RATE_LIMIT" and phase_key == "ingestion":
        return _section_rate_limit(phase_label)
    if status == "FAIL":
        return _section_fail(phase_label, err)
    if status == "SUCCESS" and phase_key == "portfolio_simulation":
        return _section_success(phase_label, details, row)
    return None


def _group_rows_by_phase(rows: list[dict]) -> dict[str, list[dict]]:
    """Group audit rows by step_name into phases (only PIPELINE_STEP / rows with DETAILS)."""
    groups: dict[str, list[dict]] = {p: [] for p in PHASE_ORDER}
    others: list[dict] = []
    for r in rows:
        if r.get("EVENT_TYPE") not in ("PIPELINE", "PIPELINE_STEP") and not r.get("DETAILS"):
            others.append(r)
            continue
        step = _step_name(r)
        if step in groups:
            groups[step].append(r)
        else:
            others.append(r)
    return groups


def build_phases_and_sections(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Group rows into phases (each phase has label + events) and build "what happened and why" sections.
    Returns (phases, sections). phases = [{ phase_key, phase_label, events: [summary_cards] }].
    sections = [{ headline, what_happened, why, impact, next_check }].
    """
    groups = _group_rows_by_phase(rows)
    phases: list[dict] = []
    sections: list[dict] = []

    for phase_key in PHASE_ORDER:
        phase_rows = groups.get(phase_key, [])
        if not phase_rows:
            continue
        phase_label = PHASE_LABELS.get(phase_key, phase_key.replace("_", " ").title())
        events = [step_to_summary_card(r) for r in phase_rows]
        phases.append({
            "phase_key": phase_key,
            "phase_label": phase_label,
            "events": events,
        })
        for r in phase_rows:
            sec = _row_to_section(r, phase_key, phase_label)
            if sec:
                sec["phase_key"] = phase_key
                sec["phase_label"] = phase_label
                sections.append(sec)

    return phases, sections


def build_interpreted_narrative(rows: list[dict], sections: list[dict], narrative_bullets: list[str]) -> str:
    """
    Build a short paragraph for "What happened" that states clearly when there were no new bars.
    """
    has_no_new_bars = any(
        (r.get("STATUS") or "").upper() == "SKIPPED_NO_NEW_BARS"
        or _get(_details(r), "reason") == "NO_NEW_BARS"
        for r in rows
    )
    if has_no_new_bars:
        return (
            "This run had no new market bars. The pipeline did not run recommendations, evaluation, "
            "or portfolio steps because there was no new market data after ingestion. Run again after new data is available."
        )
    if sections:
        headlines = [s.get("headline", "") for s in sections if s.get("headline")]
        if headlines:
            return " ".join(headlines) + "."
    if narrative_bullets:
        return " ".join(narrative_bullets[:5])  # first few bullets as paragraph
    return "Pipeline run completed. See timeline for details."


def interpret_timeline(rows: list[dict]) -> dict:
    """
    Input: list of audit rows (EVENT_TS, EVENT_TYPE, EVENT_NAME, STATUS, ROWS_AFFECTED, DETAILS, ERROR_MESSAGE).
    Output: structured JSON for UI:
      - timeline: raw rows (serializable)
      - summary_cards: legacy flat list
      - narrative_bullets: legacy flat list
      - phases: [{ phase_key, phase_label, events }]
      - sections: [{ headline, what_happened, why, impact, next_check, phase_key?, phase_label? }]
      - interpreted_narrative: short paragraph (no new bars stated clearly when applicable)
    """
    summary_cards = []
    narrative_bullets = []
    for r in rows:
        if r.get("EVENT_TYPE") in ("PIPELINE", "PIPELINE_STEP") or r.get("DETAILS"):
            summary_cards.append(step_to_summary_card(r))
            narrative_bullets.append(step_to_narrative_bullet(r))

    phases, sections = build_phases_and_sections(rows)
    interpreted_narrative = build_interpreted_narrative(rows, sections, narrative_bullets)

    return {
        "timeline": rows,
        "summary_cards": summary_cards,
        "narrative_bullets": narrative_bullets,
        "phases": phases,
        "sections": sections,
        "interpreted_narrative": interpreted_narrative,
    }
