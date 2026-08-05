"""Lightweight Phase 9 stage progress / heartbeat helpers (no heavy queries)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from .experiment_execution_repository import get_execution, update_execution

PROGRESS_PHASE_COMPUTE = "compute"
PROGRESS_PHASE_PERSIST = "persist"
PROGRESS_PHASE_INTEGRITY = "integrity"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")


def build_stage_progress(
    *,
    stage: str,
    completed: int,
    total: int,
    unit: str = "observations",
    phase: str | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "stage": stage,
        "completed": int(completed),
        "total": int(total),
        "unit": unit,
        "updated_at_utc": _utc_now_iso(),
    }
    if phase:
        out["phase"] = phase
    return out


def patch_stage_progress(
    execution_id: str,
    *,
    stage: str,
    completed: int,
    total: int,
    unit: str = "observations",
    phase: str | None = None,
    clear: bool = False,
) -> dict[str, Any]:
    """Merge stage_progress into EXECUTION.PROGRESS_JSON and bump heartbeat."""
    ex = get_execution(execution_id) or {}
    progress = dict(ex.get("progress_json") or {})
    if clear:
        progress.pop("stage_progress", None)
    else:
        progress["stage_progress"] = build_stage_progress(
            stage=stage, completed=completed, total=total, unit=unit, phase=phase
        )
    update_execution(
        execution_id,
        progress_json=progress,
        heartbeat_at=datetime.now(timezone.utc).replace(tzinfo=None),
    )
    return progress.get("stage_progress") or {}


def make_progress_callback(
    execution_id: str | None,
    *,
    stage: str,
    total: int,
    unit: str = "observations",
    every: int = 100,
    phase: str = PROGRESS_PHASE_COMPUTE,
) -> Callable[..., None] | None:
    """Return a callback(completed, *, phase=..., total=..., unit=...) that patches progress."""
    if not execution_id:
        return None
    state = {"n": -1, "phase": None}
    default_total = total
    default_unit = unit
    default_phase = phase

    def _cb(
        completed: int,
        *,
        phase: str | None = None,
        total: int | None = None,
        unit: str | None = None,
    ) -> None:
        ph = phase or default_phase
        tot = int(total if total is not None else default_total)
        un = unit or default_unit
        if (
            completed == tot
            or completed - state["n"] >= every
            or completed == 0
            or ph != state["phase"]
        ):
            state["n"] = completed
            state["phase"] = ph
            try:
                patch_stage_progress(
                    execution_id,
                    stage=stage,
                    completed=completed,
                    total=tot,
                    unit=un,
                    phase=ph,
                )
            except Exception:
                pass

    return _cb


def emit_progress(
    on_progress: Callable[..., None] | None,
    completed: int,
    *,
    phase: str,
    total: int,
    unit: str = "observations",
) -> None:
    if not on_progress:
        return
    try:
        on_progress(completed, phase=phase, total=total, unit=unit)
    except TypeError:
        on_progress(completed)
