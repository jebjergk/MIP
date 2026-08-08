"""Brooks Intraday Phase 3 service wiring."""

from __future__ import annotations

from fastapi import HTTPException

from . import store
from .errors import BrooksIntradayError
from .models import RunDetail, SimpleStatusResponse
from .observation_repository import load_observations


def _http_error(exc: BrooksIntradayError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.to_payload())


def start_run(run_id: str) -> SimpleStatusResponse:
    try:
        updated = store.run_replay_start(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc
    except BrooksIntradayError as exc:
        raise _http_error(exc) from exc
    return SimpleStatusResponse(
        run_id=run_id,
        status=updated.status,
        message="Replay started (manual mode pauses for Next Bar).",
    )


def pause_run(run_id: str) -> SimpleStatusResponse:
    try:
        updated = store.run_replay_pause(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc
    return SimpleStatusResponse(run_id=run_id, status=updated.status)


def resume_run(run_id: str) -> SimpleStatusResponse:
    try:
        updated = store.run_replay_resume(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc
    except BrooksIntradayError as exc:
        raise _http_error(exc) from exc
    return SimpleStatusResponse(run_id=run_id, status=updated.status)


def stop_run(run_id: str) -> SimpleStatusResponse:
    try:
        updated = store.run_replay_stop(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc
    return SimpleStatusResponse(run_id=run_id, status=updated.status)


def reset_run(run_id: str) -> SimpleStatusResponse:
    try:
        updated = store.reset_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc
    return SimpleStatusResponse(
        run_id=run_id,
        status=updated.status,
        message="Replay reset — new analysis attempt; prior observations retained.",
    )


def next_bar(run_id: str, *, request_token: str | None = None) -> dict:
    try:
        return store.run_next_bar(run_id, request_token=request_token)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc
    except BrooksIntradayError as exc:
        raise _http_error(exc) from exc


def get_replay_state(run_id: str) -> dict:
    try:
        return store.get_replay_state(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc


def get_visible_bars(run_id: str, symbol: str | None = None) -> dict:
    try:
        return store.get_visible_bars(run_id, symbol)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc
    except BrooksIntradayError as exc:
        raise _http_error(exc) from exc


def get_observations_for_symbol(run_id: str, symbol: str, *, review_filter: str | None = None) -> dict:
    store.get_run(run_id)
    with store._lock:
        state = store._runs.get(run_id) or {}
    attempt_id = (
        state.get("phase4_review_baseline_attempt_id")
        or state.get("configuration", {}).get("phase4_review_baseline_attempt_id")
        or state.get("active_replay_attempt_id")
        or state.get("configuration", {}).get("active_replay_attempt_id")
    )
    if not attempt_id:
        from .replay_attempt_repository import get_review_baseline_attempt_id

        attempt_id = get_review_baseline_attempt_id(run_id)
    rows = load_observations(
        run_id,
        symbol,
        replay_attempt_id=attempt_id,
        limit=500,
        review_filter=review_filter if review_filter and not str(review_filter).startswith("pattern:") else None,
    )
    pattern_attempt = state.get("phase5_pattern_attempt_id") or state.get("configuration", {}).get(
        "phase5_pattern_attempt_id"
    )
    if not pattern_attempt:
        from .replay_attempt_repository import list_replay_attempts as _list_atts

        for att in _list_atts(run_id):
            if att.get("ruleset_version") == "BROOKS_PATTERN_RULESET_V0_1" and att.get("status") == "COMPLETED":
                pattern_attempt = att.get("replay_attempt_id")
                break
    if pattern_attempt:
        from .pattern_repository import load_bar_pattern_links

        links_by_ts: dict[str, dict] = {}
        for link in load_bar_pattern_links(
            run_id, replay_attempt_id=str(pattern_attempt), symbol=symbol, limit=500
        ):
            links_by_ts[str(link.get("bar_ts"))[:19]] = link
        for row in rows:
            ts_key = str(row.get("bar_ts"))[:19]
            link = links_by_ts.get(ts_key)
            if link:
                row["pattern_snapshot_json"] = link.get("pattern_snapshot_json")
                row["active_pattern_ids"] = link.get("active_pattern_ids")
                row["pattern_action"] = link.get("action")
                row["pattern_explanation"] = link.get("explanation")
    if review_filter and str(review_filter).startswith("pattern:"):
        pf = str(review_filter).split(":", 1)[1].lower()
        rows = [
            r
            for r in rows
            if any(pf in str(p.get("pattern_family", "")).lower() for p in (r.get("pattern_snapshot_json") or []))
        ]
    return {
        "run_id": run_id,
        "symbol": symbol.upper(),
        "observations": rows,
        "total": len(rows),
        "replay_attempt_id": attempt_id,
        "pattern_attempt_id": pattern_attempt,
    }


def reset_pattern_run(run_id: str, *, allow_diagnostic_legacy: bool = False) -> SimpleStatusResponse:
    try:
        updated = store.reset_pattern_run(run_id, allow_diagnostic_legacy=allow_diagnostic_legacy)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc
    return SimpleStatusResponse(
        run_id=run_id,
        status=updated.status,
        message="Pattern replay reset — new Phase 5 attempt; Phase 4 baseline unchanged.",
    )


def list_patterns(run_id: str, symbol: str | None = None, pattern_filter: str | None = None) -> dict:
    store.get_run(run_id)
    with store._lock:
        state = store._runs.get(run_id) or {}
    pattern_attempt = state.get("phase5_pattern_attempt_id") or state.get("configuration", {}).get(
        "phase5_pattern_attempt_id"
    )
    if not pattern_attempt:
        from .replay_attempt_repository import list_replay_attempts as _list_atts

        for att in _list_atts(run_id):
            if att.get("ruleset_version") == "BROOKS_PATTERN_RULESET_V0_1" and att.get("status") == "COMPLETED":
                pattern_attempt = att.get("replay_attempt_id")
                break
    if not pattern_attempt:
        return {"run_id": run_id, "patterns": [], "total": 0, "pattern_attempt_id": None}
    from .pattern_repository import load_patterns

    patterns = load_patterns(
        run_id, replay_attempt_id=str(pattern_attempt), symbol=symbol, pattern_filter=pattern_filter, limit=5000
    )
    return {"run_id": run_id, "pattern_attempt_id": pattern_attempt, "patterns": patterns, "total": len(patterns)}


def pattern_review_summary(run_id: str) -> dict:
    payload = list_patterns(run_id)
    counts: dict[str, int] = {}
    by_symbol: dict[str, dict[str, int]] = {}
    for p in payload.get("patterns") or []:
        fam = p.get("pattern_family") or "?"
        sym = p.get("symbol") or "?"
        counts[fam] = counts.get(fam, 0) + 1
        by_symbol.setdefault(sym, {})
        by_symbol[sym][fam] = by_symbol[sym].get(fam, 0) + 1
    return {
        "run_id": run_id,
        "pattern_attempt_id": payload.get("pattern_attempt_id"),
        "pattern_instance_count": payload.get("total", 0),
        "by_family": counts,
        "by_symbol": by_symbol,
    }


def list_replay_attempts(run_id: str) -> dict:
    store.get_run(run_id)
    from .replay_attempt_repository import list_replay_attempts as _list

    return {"run_id": run_id, "attempts": _list(run_id)}


def observation_review_summary(run_id: str) -> dict:
    store.get_run(run_id)
    from .replay_attempt_repository import get_review_baseline_attempt_id

    attempt_id = get_review_baseline_attempt_id(run_id)
    if not attempt_id:
        from .errors import BrooksIntradayError

        raise BrooksIntradayError("REPLAY_NOT_READY", "No Phase 4 baseline attempt.", run_id=run_id)
    from .observation_repository import review_summary

    return review_summary(run_id, replay_attempt_id=attempt_id)
