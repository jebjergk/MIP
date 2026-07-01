from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import date, datetime
from typing import Any, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field

from app.cursorfiles_paths import mip_workspace_root, resolve_subprocess_python
from app.db import get_connection, fetch_all

log = logging.getLogger(__name__)


# ── COCKPIT_RUN_LOG helpers ──────────────────────────────────────────────────
# Best-effort audit writes: they log a warning on failure but never raise,
# so they cannot break the existing pipeline/board flow.

def _cockpit_run_start(
    run_id: str,
    run_type: str,
    target_date_str: Optional[str] = None,
    operator: Optional[str] = None,
) -> None:
    """Insert a RUNNING row into COCKPIT_RUN_LOG."""
    try:
        conn = get_connection()
        try:
            td_expr = f"'{target_date_str}'::DATE" if target_date_str else "NULL"
            op_val = f"'{operator}'" if operator else "NULL"
            conn.cursor().execute(
                f"INSERT INTO MIP.APP.COCKPIT_RUN_LOG "
                f"(RUN_ID, RUN_TYPE, TARGET_DATE, STARTED_AT, STATUS, OPERATOR) "
                f"VALUES ('{run_id}', '{run_type}', {td_expr}, "
                f"CURRENT_TIMESTAMP(), 'RUNNING', {op_val})"
            )
        finally:
            conn.close()
    except Exception:
        log.warning("COCKPIT_RUN_LOG start insert failed (non-fatal)", exc_info=True)


def _cockpit_run_complete(
    run_id: str,
    status: str,
    summary: Any = None,
    error_detail: Optional[str] = None,
) -> None:
    """Update COCKPIT_RUN_LOG row to SUCCESS or FAILED."""
    try:
        conn = get_connection()
        try:
            summary_json = json.dumps(summary, default=str) if summary is not None else None
            sq = summary_json.replace("'", "''") if summary_json else None
            sq_expr = f"PARSE_JSON('{sq}')" if sq else "NULL"
            err = (error_detail or "").replace("'", "''")[:3900] if error_detail else None
            err_expr = f"'{err}'" if err else "NULL"
            conn.cursor().execute(
                f"UPDATE MIP.APP.COCKPIT_RUN_LOG "
                f"SET STATUS='{status}', COMPLETED_AT=CURRENT_TIMESTAMP(), "
                f"SUMMARY_JSON={sq_expr}, ERROR_DETAIL={err_expr} "
                f"WHERE RUN_ID='{run_id}'"
            )
        finally:
            conn.close()
    except Exception:
        log.warning("COCKPIT_RUN_LOG complete update failed (non-fatal)", exc_info=True)


def _summarize_ib_daily_job_failure(payload: Any, stderr: str, stdout: str) -> str:
    """Turn subprocess JSON / logs into a short line for the Cockpit error banner."""
    lines: list[str] = []

    def ingest_symbols_blob(blob: Any) -> None:
        if not isinstance(blob, dict):
            return
        syms = blob.get("symbols")
        if not isinstance(syms, list):
            return
        failed = [s for s in syms if isinstance(s, dict) and s.get("status") == "FAILED"]
        for s in failed[:6]:
            sym = s.get("symbol") or "?"
            mt = s.get("market_type") or "?"
            err = (s.get("error") or "?").replace("\n", " ")[:240]
            lines.append(f"{sym} ({mt}): {err}")
        if len(failed) > 6:
            lines.append(f"... and {len(failed) - 6} more symbol failures")

    if isinstance(payload, dict):
        step = payload.get("step")
        if step == "ingest_ibkr_daily":
            ih = payload.get("ingest_human")
            if ih:
                lines.append(str(ih)[:900])
            else:
                lines.append("Step: IBKR ingest.")
            inner = payload.get("payload")
            if inner is None and isinstance(payload.get("stdout"), str):
                inner = _try_parse_json_blob(payload.get("stdout"))
            if isinstance(inner, dict) and inner.get("error") and not ih:
                lines.append(str(inner["error"])[:500])
            ingest_symbols_blob(inner)
        elif step == "sp_run_ib_daily_catchup":
            lines.append("Step: Snowflake SP_RUN_IB_DAILY_CATCHUP.")
            if payload.get("sql"):
                lines.append(str(payload["sql"])[:300])
            inner = payload.get("payload")
            if isinstance(inner, (dict, list)):
                lines.append(str(inner)[:500])
        else:
            ingest_symbols_blob(payload)
            if payload.get("error"):
                lines.append(str(payload["error"])[:500])

    if not lines:
        tail = (stderr or stdout or "").strip().replace("\n", " ")
        if tail:
            lines.append(tail[-700:])

    out = " ".join(lines) if lines else ""
    blob = (out + " " + (stderr or "") + " " + (stdout or "")).lower()
    if "numpy" in blob and ("__config__" in blob or "source directory" in blob):
        out += (
            " — Tip: this often means the IB subprocess used a different Python than "
            "cursorfiles/.venv (Conda + venv wheels mixed); use that venv's python.exe "
            "(default) or match minor versions in MIP_SUBPROCESS_PYTHON. "
            "If the venv install is corrupt: pip install --force-reinstall \"numpy>=2.0,<3\" there."
        )
    return out.strip()

router = APIRouter(prefix="/manage", tags=["management"])

DEPRECATION_MESSAGE = (
    "Legacy sim management endpoints are retired. "
    "Use /live endpoints for portfolio configuration and activity."
)


def _retired(path: str):
    raise HTTPException(
        status_code=410,
        detail={
            "status": "DEPRECATED",
            "path": path,
            "message": DEPRECATION_MESSAGE,
        },
    )


def _try_parse_json_blob(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return raw
    return value


def _extract_first_json(stream: Any) -> Any:
    """Parse the first JSON object/array in a string, ignoring any trailing
    non-JSON text.

    run_board.py prints its machine-readable JSON status payload AND then a
    human-readable "PHASE 4 COST / ATTRITION SUMMARY" block to stdout. A plain
    json.loads(stream[idx:]) raises "Extra data" on that trailing text, which
    previously made the wrapper treat a fully successful board run as FAILED.
    Using raw_decode parses only the leading JSON value and discards the rest.
    """
    if not isinstance(stream, str) or not stream:
        return None
    idx_arr = stream.find("[")
    idx_obj = stream.find("{")
    if idx_arr < 0 and idx_obj < 0:
        return None
    if idx_arr < 0:
        idx = idx_obj
    elif idx_obj < 0:
        idx = idx_arr
    else:
        idx = min(idx_arr, idx_obj)
    try:
        obj, _ = json.JSONDecoder().raw_decode(stream[idx:])
        return obj
    except Exception:
        return None


def _aggregate_lpa_import_skip_summary(per_portfolio_results: dict[str, Any]) -> dict[str, Any]:
    """Roll up import skip counters across portfolios for Cockpit display."""
    totals = {
        "candidate_count": 0,
        "imported_count": 0,
        "skipped_existing_count": 0,
        "skipped_long_only_count": 0,
        "skipped_stale_count": 0,
        "skipped_live_position_count": 0,
        "skipped_duplicate_symbol_count": 0,
        "skipped_contract_violations_count": 0,
    }
    contract_details: list[dict[str, Any]] = []
    for _pid, res in (per_portfolio_results or {}).items():
        if not isinstance(res, dict) or res.get("error"):
            continue
        for key in totals:
            totals[key] += int(res.get(key) or 0)
        for detail in res.get("skipped_contract_violation_details") or []:
            if isinstance(detail, dict):
                contract_details.append(detail)
    totals["contract_violation_details"] = contract_details[:10]
    return totals


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _compute_ib_duration_str(start_date_iso: str, end_date_iso: str) -> str:
    try:
        start_dt = date.fromisoformat(start_date_iso)
        end_dt = date.fromisoformat(end_date_iso)
    except ValueError:
        return "2 Y"
    days = max((end_dt - start_dt).days + 10, 30)
    years = (days + 364) // 365
    if years <= 1:
        return "1 Y"
    return f"{years} Y"


class IBOnboardingRunRequest(BaseModel):
    symbols: list[str] = Field(..., min_length=1)
    market_type: str = Field(default="STOCK")
    start_date: str = Field(default="2025-08-01")
    end_date: str = Field(default_factory=lambda: date.today().isoformat())
    auto_activate_if_trusted: bool = True
    priority: int = 50
    symbol_cohort: str | None = None
    duration_str: str | None = None
    use_rth: bool = True


@router.post("/ib/daily-job/run")
def run_ib_manual_daily_job(
    target_date: str = Query(
        "to_date(convert_timezone('America/New_York', current_timestamp()))",
        description="Snowflake date literal, e.g. New York market date expression or '2026-03-13'",
    ),
    dry_run: bool = Query(False),
    skip_ingest: bool = Query(False),
    run_pipeline: bool = Query(True, description="After successful IB job, run SP_RUN_DAILY_PIPELINE."),
    run_proposal_board: bool = Query(
        False,
        description=(
            "After SP_RUN_DAILY_PIPELINE succeeds, run the Phase 4 Cortex agentic "
            "proposal board (MIP/scripts/proposal_board_phase4/run_board.py). "
            "Default FALSE — agentic search is a separate operator action "
            "(POST /manage/proposal-board/run). "
            "Skipped automatically when dry_run=true or run_pipeline=false."
        ),
    ),
    proposal_board_portfolio: int = Query(
        1, description="portfolio_id passed to run_board.py (default 1 = configured IBKR PAPER test portfolio)."
    ),
    proposal_board_max_proposals: int = Query(8, ge=1, le=20),
    proposal_board_max_rounds: int = Query(1, ge=1, le=3),
    proposal_board_max_candidates: int = Query(
        35,
        ge=1,
        le=50,
        description=(
            "Pre-screen cap: top-N STOCK candidates by structural appeal score "
            "sent to the AI_COMPLETE panel (~6 LLM calls each). Default 35 "
            "(~1.9 Snowflake credits/run at current rates)."
        ),
    ),
    proposal_board_inter_concurrency: int = Query(
        2, ge=1, le=8,
        description="Parallel dossiers sent to agents (--inter-concurrency).",
    ),
    proposal_board_market_types: str = Query(
        "STOCK",
        description=(
            "Comma-separated MARKET_TYPE filter passed to run_board.py. "
            "Default 'STOCK' since MIP does not currently trade ETF or FX. "
            "Use 'STOCK,ETF,FX' to include everything."
        ),
    ),
    import_proposals_to_lpa: bool = Query(
        True,
        description=(
            "After the capped board publishes, automatically import genuine "
            "EXECUTABLE structural proposals into LIVE_ACTIONS so they appear "
            "in LPA without any manual step. Non-STOCK / research-only / "
            "policy-blocked proposals are never imported."
        ),
    ),
    synth_intraday_daily: bool = Query(
        False,
        description="If true, ingest builds 1440m rows from 1m bars (STOCK/ETF RTH TRADES, FX MIDPOINT) on today's NY calendar date, then catch-up and optional pipeline.",
    ),
):
    dmu_run_id = str(uuid4())
    _cockpit_run_start(dmu_run_id, "DAILY_MARKET_UPDATE", operator="cockpit")

    project_root = mip_workspace_root()
    py = resolve_subprocess_python(project_root)
    runner = project_root / "cursorfiles" / "run_ib_manual_daily_job.py"
    if not runner.is_file():
        _cockpit_run_complete(dmu_run_id, "FAILED", error_detail=f"Runner script missing: {runner}")
        raise HTTPException(
            status_code=500,
            detail=(
                f"Manual IB daily runner script missing: {runner}. "
                f"Workspace root resolved to {project_root}."
            ),
        )
    if not py.is_file():
        _cockpit_run_complete(dmu_run_id, "FAILED", error_detail=f"Python interpreter not found: {py}")
        raise HTTPException(
            status_code=500,
            detail=(
                f"Python interpreter not found at {py}. "
                "Create cursorfiles/.venv (see cursorfiles/requirements.txt) or set "
                "MIP_SUBPROCESS_PYTHON to a full path to python.exe (same minor version as "
                "that venv if you use its packages)."
            ),
        )

    cmd = [str(py), str(runner), "--target-date", target_date]
    if dry_run:
        cmd.append("--dry-run")
    if skip_ingest:
        cmd.append("--skip-ingest")
    if synth_intraday_daily:
        cmd.append("--synth-daily-from-intraday")

    child_env = dict(os.environ)
    for key in list(child_env.keys()):
        if key.startswith("SNOWFLAKE_"):
            child_env.pop(key, None)

    log.info(
        "IB daily job: workspace=%s python=%s synth_intraday_daily=%s",
        project_root,
        py,
        synth_intraday_daily,
    )

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(project_root / "cursorfiles"),
            env=child_env,
            capture_output=True,
            text=True,
            timeout=900,
        )
    except subprocess.TimeoutExpired:
        log.error("IB daily job subprocess timed out after 900s")
        raise HTTPException(
            status_code=504,
            detail=jsonable_encoder(
                {
                    "message": "Manual IB daily job timed out (ingest + catch-up exceeded 15 minutes).",
                    "python": str(py),
                    "workspace": str(project_root),
                }
            ),
        )

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()

    payload: Any = None
    for stream in (stdout, stderr):
        if not stream:
            continue
        idx_arr = stream.find("[")
        idx_obj = stream.find("{")
        idx = idx_arr if idx_arr >= 0 and (idx_obj < 0 or idx_arr < idx_obj) else idx_obj
        if idx < 0:
            continue
        try:
            payload = json.loads(stream[idx:])
            break
        except Exception:
            continue

    if proc.returncode != 0:
        log.warning(
            "IB daily job failed rc=%s stderr_tail=%s",
            proc.returncode,
            (stderr or stdout)[-500:],
        )
        summary = _summarize_ib_daily_job_failure(payload, stderr, stdout)
        base_msg = "Manual IB daily job failed (ingest or SP_RUN_IB_DAILY_CATCHUP)."
        message = f"{base_msg} {summary}".strip() if summary else base_msg
        _cockpit_run_complete(dmu_run_id, "FAILED", error_detail=message[:3900])
        raise HTTPException(
            status_code=500,
            detail=jsonable_encoder(
                {
                    "message": message,
                    "failure_summary": summary or None,
                    "python": str(py),
                    "workspace": str(project_root),
                    "payload": payload,
                    "stdout": stdout[-4000:],
                    "stderr": stderr[-4000:],
                }
            ),
        )

    response = {
        "status": "SUCCESS",
        "run_type": "DAILY_MARKET_UPDATE",
        "cockpit_run_id": dmu_run_id,
        "payload": payload,
        "pipeline_triggered": False,
        "proposal_board_triggered": False,
        "agentic_opportunity_search_not_run": True,
        "synth_intraday_daily": bool(synth_intraday_daily),
        "python_used": str(py),
        "ingest_partial_failure": bool(
            isinstance(payload, dict) and payload.get("status") == "PARTIAL_FAILURE"
        ),
    }

    if not dry_run and run_pipeline:
        snow_script = project_root / "cursorfiles" / "query_snowflake.py"
        pipeline_cmd = [str(py), str(snow_script), "-q", "call MIP.APP.SP_RUN_DAILY_PIPELINE()", "--json"]
        try:
            pipeline_proc = subprocess.run(
                pipeline_cmd,
                cwd=str(project_root),
                env=child_env,
                capture_output=True,
                text=True,
                timeout=1800,
            )
        except subprocess.TimeoutExpired:
            log.error("SP_RUN_DAILY_PIPELINE subprocess timed out after 1800s")
            raise HTTPException(
                status_code=504,
                detail=jsonable_encoder(
                    {
                        "message": "IB ingest/catch-up succeeded, but daily pipeline timed out (30 min).",
                        "python": str(py),
                        "ingest_payload": payload,
                    }
                ),
            )
        pipeline_stdout = (pipeline_proc.stdout or "").strip()
        pipeline_stderr = (pipeline_proc.stderr or "").strip()
        pipeline_payload: Any = None
        for stream in (pipeline_stdout, pipeline_stderr):
            if not stream:
                continue
            idx_arr = stream.find("[")
            idx_obj = stream.find("{")
            idx = idx_arr if idx_arr >= 0 and (idx_obj < 0 or idx_arr < idx_obj) else idx_obj
            if idx < 0:
                continue
            try:
                pipeline_payload = json.loads(stream[idx:])
                break
            except Exception:
                continue
        if pipeline_proc.returncode != 0:
            log.warning(
                "SP_RUN_DAILY_PIPELINE failed rc=%s stderr_tail=%s",
                pipeline_proc.returncode,
                (pipeline_stderr or pipeline_stdout)[-500:],
            )
            err_msg = "IB daily job succeeded, but SP_RUN_DAILY_PIPELINE failed."
            _cockpit_run_complete(dmu_run_id, "FAILED", error_detail=err_msg)
            raise HTTPException(
                status_code=500,
                detail=jsonable_encoder(
                    {
                        "message": err_msg,
                        "python": str(py),
                        "payload": payload,
                        "pipeline_payload": pipeline_payload,
                        "pipeline_stdout": pipeline_stdout[-4000:],
                        "pipeline_stderr": pipeline_stderr[-4000:],
                    }
                ),
            )
        response["pipeline_triggered"] = True
        response["pipeline_result"] = pipeline_payload

    if not dry_run and run_pipeline and run_proposal_board:
        normalized_market_types = ",".join(
            sorted({m.strip().upper() for m in (proposal_board_market_types or "STOCK").split(",") if m.strip()})
        ) or "STOCK"
        board_cmd = [
            str(py),
            "-m",
            "MIP.scripts.proposal_board_phase4.run_board",
            "--portfolio",
            str(int(proposal_board_portfolio)),
            "--max-proposals",
            str(int(proposal_board_max_proposals)),
            "--max-rounds",
            str(int(proposal_board_max_rounds)),
            "--max-candidates",
            str(int(proposal_board_max_candidates)),
            "--inter-concurrency",
            str(int(proposal_board_inter_concurrency)),
            "--market-types",
            normalized_market_types,
        ]
        log.info(
            "Phase 4 agentic board (CAPPED): workspace=%s portfolio=%s max_proposals=%s "
            "max_rounds=%s max_candidates=%s inter_concurrency=%s market_types=%s",
            project_root,
            proposal_board_portfolio,
            proposal_board_max_proposals,
            proposal_board_max_rounds,
            proposal_board_max_candidates,
            proposal_board_inter_concurrency,
            normalized_market_types,
        )
        try:
            board_proc = subprocess.run(
                board_cmd,
                cwd=str(project_root),
                env=child_env,
                capture_output=True,
                text=True,
                timeout=3000,
            )
        except subprocess.TimeoutExpired:
            log.error("Phase 4 agentic board subprocess timed out after 3000s")
            raise HTTPException(
                status_code=504,
                detail=jsonable_encoder(
                    {
                        "message": (
                            "IB ingest/catch-up + daily pipeline succeeded, but Phase 4 "
                            "agentic proposal board timed out (50 min). Inspect "
                            "MIP.APP.PROPOSAL_BOARD_RUN for the in-flight RUN_ID."
                        ),
                        "python": str(py),
                        "ingest_payload": payload,
                    }
                ),
            )
        board_stdout = (board_proc.stdout or "").strip()
        board_stderr = (board_proc.stderr or "").strip()
        board_payload: Any = None
        for stream in (board_stdout, board_stderr):
            board_payload = _extract_first_json(stream)
            if isinstance(board_payload, dict):
                break
        board_status = (
            str(board_payload.get("status") or "").upper()
            if isinstance(board_payload, dict)
            else ""
        )
        board_ok_statuses = {"COMPLETE", "COMPLETE_NO_DOSSIERS", "PARTIAL_FAILURE"}
        board_partial = board_status == "PARTIAL_FAILURE"
        if board_proc.returncode != 0 or board_status not in board_ok_statuses:
            log.warning(
                "Phase 4 agentic board failed rc=%s status=%s stderr_tail=%s",
                board_proc.returncode,
                board_status or "?",
                (board_stderr or board_stdout)[-500:],
            )
            raise HTTPException(
                status_code=500,
                detail=jsonable_encoder(
                    {
                        "message": (
                            "IB daily job + SP_RUN_DAILY_PIPELINE succeeded, but the "
                            "Phase 4 agentic proposal board failed. Cockpit/Timeline "
                            "will continue to show the previous authoritative run."
                        ),
                        "python": str(py),
                        "payload": payload,
                        "pipeline_payload": response.get("pipeline_result"),
                        "board_payload": board_payload,
                        "board_stdout": board_stdout[-4000:],
                        "board_stderr": board_stderr[-4000:],
                    }
                ),
            )
        response["proposal_board_triggered"] = True
        response["proposal_board_result"] = board_payload
        response["proposal_board_partial"] = board_partial
        if board_partial:
            response["status"] = "PARTIAL_SUCCESS"

        # ── Guaranteed LPA import — genuine proposals must appear in LPA ──
        # Historically the only path that materialised structural proposals
        # into LIVE_ACTIONS was a best-effort bridge fired when the LPA
        # activity overview happened to be loaded. That left genuine
        # EXECUTABLE proposals (e.g. CSCO) stranded in
        # STRUCTURAL_TRADE_PROPOSALS and invisible/un-tradeable in LPA. We
        # now import as an explicit, wired step of the daily button so the
        # operator never has to run anything by hand. The importer only
        # promotes STATUS='PROPOSED' + EXECUTION_POLICY_STATUS='EXECUTABLE'
        # + STOCK proposals into a PENDING (non-executed) LIVE_ACTIONS state;
        # it never submits to a broker and never touches real-money gates.
        response["lpa_import_triggered"] = False
        if import_proposals_to_lpa:
            try:
                from .live import (  # lazy import avoids router import cycle
                    ImportStructuralProposalsRequest,
                    import_structural_proposals,
                )

                # Proposals are PORTFOLIO-AGNOSTIC: the agentic board publishes a
                # single shared set of STRUCTURAL_TRADE_PROPOSALS (PORTFOLIO_ID is
                # NULL). The same proposal is used across real-money and paper.
                # The daily run must therefore make EVERY active live portfolio's
                # LPA see the same proposals INDEPENDENTLY — it must never write
                # LPA for only the one account that happened to be connected when
                # the job ran. We import into each active LIVE_PORTFOLIO_CONFIG
                # portfolio; each gets its own (PORTFOLIO_ID, PROPOSAL_ID)
                # LIVE_ACTIONS row in a PENDING (non-executed) state. Importing
                # into a REAL portfolio's LPA never auto-trades — real-money
                # execution still requires the separate arming/confirmation gates.
                conn = get_connection()
                try:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT PORTFOLIO_ID
                        FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                        WHERE COALESCE(IS_ACTIVE, TRUE) = TRUE
                        ORDER BY PORTFOLIO_ID
                        """
                    )
                    active_portfolios = [int(r[0]) for r in (cur.fetchall() or [])]
                finally:
                    try:
                        conn.close()
                    except Exception:  # noqa: BLE001
                        pass

                if not active_portfolios:
                    # Fall back to the board's context portfolio so the daily run
                    # never silently imports nothing when config is unexpected.
                    active_portfolios = [int(proposal_board_portfolio)]

                per_portfolio_results: dict[str, Any] = {}
                total_imported = 0
                for pid in active_portfolios:
                    try:
                        import_result = import_structural_proposals(
                            ImportStructuralProposalsRequest(
                                live_portfolio_id=pid,
                                limit=max(1, min(int(proposal_board_max_proposals), 50)),
                                max_proposal_age_days=7,
                                dedupe_by_symbol=True,
                                skip_stale=True,
                            )
                        )
                        res = (
                            import_result
                            if isinstance(import_result, dict)
                            else {"result": import_result}
                        )
                        per_portfolio_results[str(pid)] = res
                        total_imported += int(res.get("imported_count") or 0)
                    except HTTPException as hex_exc:
                        # Per-portfolio failure must not abort the others or the
                        # whole daily job — the board already published.
                        log.warning(
                            "LPA import portfolio %s failed (http): %s",
                            pid, hex_exc.detail,
                        )
                        per_portfolio_results[str(pid)] = {
                            "error": jsonable_encoder(hex_exc.detail)
                        }
                    except Exception as exc:  # noqa: BLE001
                        log.exception("LPA import portfolio %s failed", pid)
                        per_portfolio_results[str(pid)] = {"error": str(exc)}

                response["lpa_import_triggered"] = True
                response["lpa_import_portfolios"] = active_portfolios
                response["lpa_import_total_imported"] = total_imported
                response["lpa_import_result"] = per_portfolio_results
                response["lpa_import_skip_summary"] = _aggregate_lpa_import_skip_summary(
                    per_portfolio_results
                )
                log.info(
                    "LPA import after capped board: portfolios=%s total_imported=%s",
                    active_portfolios, total_imported,
                )
            except Exception as exc:  # noqa: BLE001
                log.exception("LPA import after board failed")
                response["lpa_import_triggered"] = False
                response["lpa_import_error"] = str(exc)

    _cockpit_run_complete(
        dmu_run_id,
        "SUCCESS",
        summary={
            "run_type": "DAILY_MARKET_UPDATE",
            "pipeline_triggered": response.get("pipeline_triggered"),
            "proposal_board_triggered": False,
            "agentic_opportunity_search_not_run": True,
            "ingest_partial_failure": response.get("ingest_partial_failure"),
        },
    )
    return jsonable_encoder(response)


# ── Phase 4 agentic opportunity search ───────────────────────────────────────

_STALENESS_SQL = """
WITH last_trading AS (
    -- Most recent completed US equity trading session (Mon-Fri, not a holiday, before today)
    SELECT MAX(d) AS expected_date
    FROM (
        SELECT DATEADD(DAY, -seq4()::INT, CURRENT_DATE())::DATE AS d
        FROM TABLE(GENERATOR(ROWCOUNT => 30))
    )
    WHERE DAYOFWEEK(d) NOT IN (0, 6)
      AND d NOT IN (
          SELECT HOLIDAY_DATE FROM MIP.APP.US_EQUITY_HOLIDAYS
          WHERE EXCHANGE = 'NYSE' AND FULL_DAY_CLOSE = TRUE
      )
      AND d < CURRENT_DATE()
),
last_pipeline AS (
    SELECT MAX(DETAILS:effective_to_ts::TIMESTAMP_NTZ)::DATE AS pipeline_date
    FROM MIP.APP.MIP_AUDIT_LOG
    WHERE EVENT_TYPE = 'PIPELINE'
      AND EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
      AND STATUS IN ('SUCCESS', 'SUCCESS_WITH_SKIPS')
),
latest_bar AS (
    SELECT MAX(TS::DATE) AS bar_date
    FROM MIP.MART.MARKET_BARS
    WHERE MARKET_TYPE IN ('STOCK', 'ETF') AND INTERVAL_MINUTES = 1440
)
SELECT
    lt.expected_date,
    lp.pipeline_date,
    lb.bar_date,
    DATEDIFF('day', lp.pipeline_date, CURRENT_DATE()) AS pipeline_lag_days,
    DATEDIFF('day', lb.bar_date, CURRENT_DATE()) AS bar_lag_days
FROM last_trading lt, last_pipeline lp, latest_bar lb
"""


@router.post("/proposal-board/run")
def run_proposal_board(
    portfolio: int = Query(
        1,
        description="portfolio_id passed to run_board.py (default 1 = configured IBKR PAPER portfolio).",
    ),
    max_proposals: int = Query(8, ge=1, le=20),
    max_rounds: int = Query(1, ge=1, le=3),
    max_candidates: int = Query(
        35,
        ge=1,
        le=50,
        description=(
            "Pre-screen cap: top-N candidates by structural appeal score "
            "sent to AI_COMPLETE (~6 calls each). Default 35."
        ),
    ),
    inter_concurrency: int = Query(2, ge=1, le=8, description="Parallel dossiers sent to agents."),
    market_types: str = Query(
        "STOCK",
        description=(
            "Comma-separated MARKET_TYPE filter. Default 'STOCK' — MIP does not trade ETF/FX. "
            "Changing to include FX or ETF requires explicit operator intent."
        ),
    ),
    import_proposals_to_lpa: bool = Query(
        True,
        description=(
            "Import genuine EXECUTABLE STOCK proposals into LIVE_ACTIONS after the board runs. "
            "Non-STOCK, research-only, and policy-blocked proposals are never imported. "
            "Importing into LPA never auto-executes a trade."
        ),
    ),
    staleness_max_trading_days_lag: int = Query(
        1,
        ge=0,
        le=5,
        description=(
            "Maximum acceptable pipeline lag (trading days). "
            "Refuse to run if pipeline_date < expected_date - lag. Default 1."
        ),
    ),
):
    """
    Run the Phase 4 Cortex agentic proposal board (costly AI operation).

    Pre-flight checks that market data and pipeline are current before spending
    Cortex credits. Use /manage/ib/daily-job/run (Run Daily Market Update) first
    to ensure fresh data.

    Does NOT automatically execute any trade. Proposals appear in LPA as PENDING
    review items only.
    """
    board_run_id = str(uuid4())
    _cockpit_run_start(board_run_id, "AGENTIC_OPPORTUNITY_SEARCH", operator="cockpit")

    # ── Staleness preflight ──────────────────────────────────────────────────
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(_STALENESS_SQL)
            row = fetch_all(cur)
        finally:
            conn.close()
    except Exception as exc:
        _cockpit_run_complete(board_run_id, "FAILED", error_detail=f"Staleness check failed: {exc}")
        raise HTTPException(status_code=500, detail=f"Staleness check query failed: {exc}") from exc

    if not row:
        _cockpit_run_complete(board_run_id, "FAILED", error_detail="Staleness check returned no rows")
        raise HTTPException(status_code=500, detail="Staleness check returned no rows.")

    staleness = row[0]
    expected_date = staleness.get("EXPECTED_DATE")
    pipeline_date = staleness.get("PIPELINE_DATE")
    bar_date = staleness.get("BAR_DATE")
    _pl = staleness.get("PIPELINE_LAG_DAYS")
    _bl = staleness.get("BAR_LAG_DAYS")
    pipeline_lag = int(_pl) if _pl is not None else 999
    bar_lag = int(_bl) if _bl is not None else 999

    if pipeline_lag > staleness_max_trading_days_lag or bar_lag > staleness_max_trading_days_lag:
        detail = {
            "message": (
                "Market data or pipeline is stale. Run 'Run Daily Market Update' first "
                "to refresh data before running the agentic opportunity search."
            ),
            "expected_trading_date": str(expected_date) if expected_date else None,
            "pipeline_date": str(pipeline_date) if pipeline_date else None,
            "bar_date": str(bar_date) if bar_date else None,
            "pipeline_lag_days": pipeline_lag,
            "bar_lag_days": bar_lag,
            "staleness_max_trading_days_lag": staleness_max_trading_days_lag,
        }
        _cockpit_run_complete(board_run_id, "FAILED", error_detail=detail["message"])
        raise HTTPException(status_code=409, detail=jsonable_encoder(detail))

    # ── Phase 4 board execution ──────────────────────────────────────────────
    project_root = mip_workspace_root()
    py = resolve_subprocess_python(project_root)

    child_env = dict(os.environ)
    for key in list(child_env.keys()):
        if key.startswith("SNOWFLAKE_"):
            child_env.pop(key, None)

    normalized_market_types = ",".join(
        sorted({m.strip().upper() for m in (market_types or "STOCK").split(",") if m.strip()})
    ) or "STOCK"

    board_cmd = [
        str(py),
        "-m",
        "MIP.scripts.proposal_board_phase4.run_board",
        "--portfolio",
        str(int(portfolio)),
        "--max-proposals",
        str(int(max_proposals)),
        "--max-rounds",
        str(int(max_rounds)),
        "--max-candidates",
        str(int(max_candidates)),
        "--inter-concurrency",
        str(int(inter_concurrency)),
        "--market-types",
        normalized_market_types,
    ]
    log.info(
        "Phase 4 agentic board (CAPPED): workspace=%s portfolio=%s max_proposals=%s "
        "max_rounds=%s max_candidates=%s inter_concurrency=%s market_types=%s",
        project_root,
        portfolio,
        max_proposals,
        max_rounds,
        max_candidates,
        inter_concurrency,
        normalized_market_types,
    )

    try:
        board_proc = subprocess.run(
            board_cmd,
            cwd=str(project_root),
            env=child_env,
            capture_output=True,
            text=True,
            timeout=3000,
        )
    except subprocess.TimeoutExpired:
        log.error("Phase 4 agentic board subprocess timed out after 3000s")
        err_msg = (
            "Phase 4 agentic proposal board timed out (50 min). "
            "Inspect MIP.APP.PROPOSAL_BOARD_RUN for the in-flight RUN_ID."
        )
        _cockpit_run_complete(board_run_id, "FAILED", error_detail=err_msg)
        raise HTTPException(
            status_code=504,
            detail=jsonable_encoder({"message": err_msg, "python": str(py)}),
        )

    board_stdout = (board_proc.stdout or "").strip()
    board_stderr = (board_proc.stderr or "").strip()
    board_payload: Any = None
    for stream in (board_stdout, board_stderr):
        board_payload = _extract_first_json(stream)
        if isinstance(board_payload, dict):
            break

    board_status = (
        str(board_payload.get("status") or "").upper()
        if isinstance(board_payload, dict)
        else ""
    )
    board_ok_statuses = {"COMPLETE", "COMPLETE_NO_DOSSIERS", "PARTIAL_FAILURE"}
    board_partial = board_status == "PARTIAL_FAILURE"
    if board_proc.returncode != 0 or board_status not in board_ok_statuses:
        log.warning(
            "Phase 4 agentic board failed rc=%s status=%s stderr_tail=%s",
            board_proc.returncode,
            board_status or "?",
            (board_stderr or board_stdout)[-500:],
        )
        err_msg = (
            "Phase 4 agentic proposal board failed. "
            "Cockpit/Timeline will continue to show the previous authoritative run."
        )
        _cockpit_run_complete(board_run_id, "FAILED", error_detail=err_msg)
        raise HTTPException(
            status_code=500,
            detail=jsonable_encoder(
                {
                    "message": err_msg,
                    "python": str(py),
                    "board_payload": board_payload,
                    "board_stdout": board_stdout[-4000:],
                    "board_stderr": board_stderr[-4000:],
                }
            ),
        )

    response = {
        "status": "PARTIAL_SUCCESS" if board_partial else "SUCCESS",
        "run_type": "AGENTIC_OPPORTUNITY_SEARCH",
        "cockpit_run_id": board_run_id,
        "proposal_board_result": board_payload,
        "proposal_board_partial": board_partial,
        "trade_auto_executed": False,
        "staleness_check": {
            "expected_trading_date": str(expected_date) if expected_date else None,
            "pipeline_date": str(pipeline_date) if pipeline_date else None,
            "bar_date": str(bar_date) if bar_date else None,
            "pipeline_lag_days": pipeline_lag,
            "bar_lag_days": bar_lag,
        },
    }

    # ── LPA import ───────────────────────────────────────────────────────────
    response["lpa_import_triggered"] = False
    if import_proposals_to_lpa:
        try:
            from .live import (
                ImportStructuralProposalsRequest,
                import_structural_proposals,
            )

            conn = get_connection()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT PORTFOLIO_ID
                    FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                    WHERE COALESCE(IS_ACTIVE, TRUE) = TRUE
                    ORDER BY PORTFOLIO_ID
                    """
                )
                active_portfolios = [int(r[0]) for r in (cur.fetchall() or [])]
            finally:
                conn.close()

            if not active_portfolios:
                active_portfolios = [int(portfolio)]

            per_portfolio_results: dict[str, Any] = {}
            total_imported = 0
            for pid in active_portfolios:
                try:
                    import_result = import_structural_proposals(
                        ImportStructuralProposalsRequest(
                            live_portfolio_id=pid,
                            limit=max(1, min(int(max_proposals), 50)),
                            max_proposal_age_days=7,
                            dedupe_by_symbol=True,
                            skip_stale=True,
                        )
                    )
                    res = (
                        import_result
                        if isinstance(import_result, dict)
                        else {"result": import_result}
                    )
                    per_portfolio_results[str(pid)] = res
                    total_imported += int(res.get("imported_count") or 0)
                except HTTPException as hex_exc:
                    log.warning("LPA import portfolio %s failed (http): %s", pid, hex_exc.detail)
                    per_portfolio_results[str(pid)] = {"error": jsonable_encoder(hex_exc.detail)}
                except Exception as exc:
                    log.exception("LPA import portfolio %s failed", pid)
                    per_portfolio_results[str(pid)] = {"error": str(exc)}

            response["lpa_import_triggered"] = True
            response["lpa_import_portfolios"] = active_portfolios
            response["lpa_import_total_imported"] = total_imported
            response["lpa_import_result"] = per_portfolio_results
            response["lpa_import_skip_summary"] = _aggregate_lpa_import_skip_summary(
                per_portfolio_results
            )
            log.info(
                "LPA import after capped board: portfolios=%s total_imported=%s",
                active_portfolios,
                total_imported,
            )
        except Exception as exc:
            log.exception("LPA import after board failed")
            response["lpa_import_triggered"] = False
            response["lpa_import_error"] = str(exc)

    _cockpit_run_complete(
        board_run_id,
        "SUCCESS",
        summary={
            "run_type": "AGENTIC_OPPORTUNITY_SEARCH",
            "board_status": board_status,
            "board_partial": board_partial,
            "market_types": normalized_market_types,
            "max_candidates": max_candidates,
            "lpa_import_triggered": response.get("lpa_import_triggered"),
            "lpa_import_total_imported": response.get("lpa_import_total_imported"),
            "trade_auto_executed": False,
        },
    )
    return jsonable_encoder(response)


@router.get("/cockpit-ops/status")
def cockpit_ops_status():
    """
    Poll active Cockpit operator jobs (Daily Market Update, Agentic Search)
    and in-flight Phase 4 board progress for UI status display.
    """
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT RUN_ID, RUN_TYPE, STATUS, STARTED_AT, COMPLETED_AT, ERROR_DETAIL
                  FROM MIP.APP.COCKPIT_RUN_LOG
                 WHERE STATUS = 'RUNNING'
                    OR STARTED_AT >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
                 ORDER BY STARTED_AT DESC
                 LIMIT 8
                """
            )
            cockpit_rows = fetch_all(cur)

            cur.execute(
                """
                SELECT r.RUN_ID, r.RUN_STATUS, r.CANDIDATE_COUNT, r.FINAL_PROPOSAL_COUNT,
                       r.PROMPT_VERSION, r.STARTED_AT, r.FINISHED_AT,
                       (SELECT COUNT(*) FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2 ao
                         WHERE ao.RUN_ID = r.RUN_ID) AS OUTCOMES,
                       (SELECT COUNT(*) FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT tv
                         WHERE tv.RUN_ID = r.RUN_ID) AS VERDICTS
                  FROM MIP.APP.PROPOSAL_BOARD_RUN r
                 WHERE r.STARTED_AT >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
                    OR (
                        r.RUN_STATUS = 'RUNNING'
                        AND r.STARTED_AT >= DATEADD('minute', -90, CURRENT_TIMESTAMP())
                    )
                 ORDER BY r.STARTED_AT DESC
                 LIMIT 3
                """
            )
            board_rows = fetch_all(cur)
        finally:
            conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"cockpit-ops status query failed: {exc}") from exc

    active_runs = [r for r in cockpit_rows if r.get("STATUS") == "RUNNING"]
    board_running = next((b for b in board_rows if b.get("RUN_STATUS") == "RUNNING"), None)

    return jsonable_encoder(
        {
            "ops_running": bool(active_runs),
            "active_runs": active_runs,
            "recent_runs": cockpit_rows,
            "board_running": board_running,
            "recent_board_runs": board_rows,
        }
    )


@router.post("/ib/onboarding/run")
def run_ib_symbol_onboarding(payload: IBOnboardingRunRequest):
    project_root = mip_workspace_root()
    py = resolve_subprocess_python(project_root)
    ingest_script = project_root / "cursorfiles" / "ingest_ibkr_bars.py"
    snow_script = project_root / "cursorfiles" / "query_snowflake.py"
    if not py.is_file() or not ingest_script.is_file() or not snow_script.is_file():
        raise HTTPException(
            status_code=500,
            detail=(
                "IB onboarding runtime not found. "
                f"Python={py}, ingest={ingest_script}, query_snowflake={snow_script}."
            ),
        )

    symbols = []
    seen = set()
    for raw in payload.symbols:
        normalized = (raw or "").strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        symbols.append(normalized)
    if not symbols:
        raise HTTPException(status_code=400, detail="symbols must include at least one non-empty value.")

    market_type = (payload.market_type or "STOCK").strip().upper()
    run_id = str(uuid4())
    cohort = payload.symbol_cohort or f"IB_ONBOARD_{run_id.replace('-', '')[:8].upper()}"
    duration_str = payload.duration_str or _compute_ib_duration_str(payload.start_date, payload.end_date)
    symbols_csv = ",".join(symbols)

    child_env = dict(os.environ)
    for key in list(child_env.keys()):
        if key.startswith("SNOWFLAKE_"):
            child_env.pop(key, None)

    values_sql = ",".join([f"({_sql_literal(symbol)})" for symbol in symbols])
    upsert_sql = f"""
merge into MIP.APP.INGEST_UNIVERSE t
using (
    select
        v.column1::string as SYMBOL,
        {_sql_literal(market_type)} as MARKET_TYPE,
        1440 as INTERVAL_MINUTES,
        {int(payload.priority)} as PRIORITY,
        {_sql_literal(cohort)} as SYMBOL_COHORT,
        {_sql_literal(f'IB onboarding pre-ingest {run_id}')} as NOTES
    from values {values_sql} v
) s
on upper(t.SYMBOL) = upper(s.SYMBOL)
and upper(t.MARKET_TYPE) = upper(s.MARKET_TYPE)
and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
when matched then update set
    t.IS_ENABLED = true,
    t.PRIORITY = greatest(coalesce(t.PRIORITY, 0), s.PRIORITY),
    t.SYMBOL_COHORT = s.SYMBOL_COHORT,
    t.NOTES = coalesce(t.NOTES, s.NOTES)
when not matched then insert (
    SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, IS_ENABLED, PRIORITY, SYMBOL_COHORT, NOTES
) values (
    s.SYMBOL, s.MARKET_TYPE, s.INTERVAL_MINUTES, true, s.PRIORITY, s.SYMBOL_COHORT, s.NOTES
)
"""

    pre_cmd = [str(py), str(snow_script), "-q", upsert_sql]
    pre_proc = subprocess.run(
        pre_cmd,
        cwd=str(project_root),
        env=child_env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    if pre_proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Failed to upsert ingest universe rows for onboarding.",
                "stdout": (pre_proc.stdout or "")[-4000:],
                "stderr": (pre_proc.stderr or "")[-4000:],
            },
        )

    ingest_cmd = [
        str(py),
        str(ingest_script),
        "--interval-minutes",
        "1440",
        "--duration-str",
        duration_str,
        "--symbols",
        symbols_csv,
    ]
    if payload.use_rth:
        ingest_cmd.append("--use-rth")

    ingest_proc = subprocess.run(
        ingest_cmd,
        cwd=str(project_root),
        env=child_env,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    ingest_stdout = (ingest_proc.stdout or "").strip()
    ingest_stderr = (ingest_proc.stderr or "").strip()
    ingest_payload = _try_parse_json_blob(ingest_stdout) or _try_parse_json_blob(ingest_stderr)
    if ingest_proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "IB daily ingest failed for onboarding symbols.",
                "ingest_payload": ingest_payload,
                "stdout": ingest_stdout[-4000:],
                "stderr": ingest_stderr[-4000:],
            },
        )

    symbols_json = json.dumps(symbols).replace("'", "''")
    onboarding_sql = (
        "call MIP.APP.SP_RUN_IB_SYMBOL_ONBOARDING("
        f"parse_json('{symbols_json}'), "
        f"{_sql_literal(market_type)}, "
        f"{_sql_literal(payload.start_date)}::date, "
        f"{_sql_literal(payload.end_date)}::date, "
        f"{'true' if payload.auto_activate_if_trusted else 'false'}, "
        f"{_sql_literal(run_id)}, "
        f"{_sql_literal(cohort)}, "
        f"{int(payload.priority)}"
        ")"
    )
    onboarding_cmd = [str(py), str(snow_script), "-q", onboarding_sql, "--json"]
    onboard_proc = subprocess.run(
        onboarding_cmd,
        cwd=str(project_root),
        env=child_env,
        capture_output=True,
        text=True,
        timeout=7200,
    )
    onboard_stdout = (onboard_proc.stdout or "").strip()
    onboard_stderr = (onboard_proc.stderr or "").strip()
    onboard_payload = _try_parse_json_blob(onboard_stdout) or _try_parse_json_blob(onboard_stderr)
    if onboard_proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "IB onboarding procedure failed.",
                "sql": onboarding_sql,
                "ingest_payload": ingest_payload,
                "onboarding_payload": onboard_payload,
                "stdout": onboard_stdout[-4000:],
                "stderr": onboard_stderr[-4000:],
            },
        )

    return {
        "status": "SUCCESS",
        "run_id": run_id,
        "symbol_cohort": cohort,
        "market_type": market_type,
        "symbols_requested": symbols,
        "duration_str": duration_str,
        "ingest_result": ingest_payload,
        "onboarding_result": onboard_payload,
    }


@router.get("/ib/daily-job/health")
def get_ib_manual_daily_health():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            with u as (
                select distinct upper(replace(SYMBOL, '/', '')) as SYMBOL_N, upper(MARKET_TYPE) as MARKET_TYPE
                from MIP.APP.INGEST_UNIVERSE
                where coalesce(IS_ENABLED, true)
                  and INTERVAL_MINUTES = 1440
            ),
            universe_count as (
                select count(*) as universe_n from u
            ),
            per_day_coverage as (
                select
                    d,
                    count(*) as bar_symbols_on_day
                from (
                    select distinct
                        TS::date as d,
                        upper(replace(SYMBOL, '/', '')) as SYMBOL_N,
                        upper(MARKET_TYPE) as MARKET_TYPE
                    from MIP.MART.MARKET_BARS
                    where INTERVAL_MINUTES = 1440
                ) x
                group by d
            ),
            latest_date_pick as (
                -- Prefer the newest day where *all* universe symbols have a bar. Raw max(TS::date) alone
                -- misleads when one symbol lands on the next calendar day (e.g. FX/UTC vs NY, IB bar date).
                select coalesce(
                    (
                        select max(p.d)
                        from per_day_coverage p
                        cross join universe_count uc
                        where p.bar_symbols_on_day = uc.universe_n
                    ),
                    (select max(TS::date) from MIP.MART.MARKET_BARS where INTERVAL_MINUTES = 1440)
                ) as LATEST_DATE
            ),
            latest_ts_pick as (
                select max(m.TS) as LATEST_TS
                from MIP.MART.MARKET_BARS m
                cross join latest_date_pick l
                where m.INTERVAL_MINUTES = 1440
                  and m.TS::date = l.LATEST_DATE
            ),
            b as (
                select distinct
                    upper(replace(SYMBOL, '/', '')) as SYMBOL_N,
                    upper(MARKET_TYPE) as MARKET_TYPE
                from MIP.MART.MARKET_BARS
                where INTERVAL_MINUTES = 1440
                  and TS::date = (select LATEST_DATE from latest_date_pick)
            )
            select
                (select LATEST_DATE from latest_date_pick) as LATEST_DAILY_BAR_DATE,
                (select LATEST_TS from latest_ts_pick) as LATEST_DAILY_BAR_TS,
                (select universe_n from universe_count) as UNIVERSE_SYMBOLS,
                (select count(*) from b) as BAR_SYMBOLS_ON_LATEST_DATE,
                (
                    select count(*)
                    from u
                    left join b
                      on b.SYMBOL_N = u.SYMBOL_N
                     and b.MARKET_TYPE = u.MARKET_TYPE
                    where b.SYMBOL_N is null
                ) as MISSING_SYMBOLS_ON_LATEST_DATE
            """
        )
        coverage_row = fetch_all(cur)[0]

        cur.execute(
            """
            select
                max(EVENT_TS) as LATEST_PIPELINE_EVENT_TS,
                max(DETAILS:effective_to_ts::timestamp_ntz)::date as LATEST_EFFECTIVE_TO_DATE
            from MIP.APP.MIP_AUDIT_LOG
            where EVENT_TYPE = 'PIPELINE'
              and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
              and STATUS in ('SUCCESS', 'SUCCESS_WITH_SKIPS')
            """
        )
        pipeline_row = fetch_all(cur)[0]

        catchup = None
        try:
            cur.execute("call MIP.APP.SP_RUN_IB_DAILY_CATCHUP(current_date(), true)")
            row = cur.fetchone()
            catchup_cell = row[0] if row else None
            catchup = _try_parse_json_blob(catchup_cell)
            if isinstance(catchup, str):
                catchup = _try_parse_json_blob(catchup)
        except Exception as catchup_error:
            catchup = {
                "status": "UNAVAILABLE",
                "error": str(catchup_error),
                "missing_days": None,
            }

        latest_date = coverage_row.get("LATEST_DAILY_BAR_DATE")
        if latest_date is not None and hasattr(latest_date, "isoformat"):
            latest_date = latest_date.isoformat()
        latest_ts = coverage_row.get("LATEST_DAILY_BAR_TS")
        if latest_ts is not None and hasattr(latest_ts, "isoformat"):
            latest_ts = latest_ts.isoformat()
        latest_event_ts = pipeline_row.get("LATEST_PIPELINE_EVENT_TS")
        if latest_event_ts is not None and hasattr(latest_event_ts, "isoformat"):
            latest_event_ts = latest_event_ts.isoformat()
        latest_effective = pipeline_row.get("LATEST_EFFECTIVE_TO_DATE")
        if latest_effective is not None and hasattr(latest_effective, "isoformat"):
            latest_effective = latest_effective.isoformat()

        missing = int(coverage_row.get("MISSING_SYMBOLS_ON_LATEST_DATE") or 0)
        bar_symbols = int(coverage_row.get("BAR_SYMBOLS_ON_LATEST_DATE") or 0)
        universe = int(coverage_row.get("UNIVERSE_SYMBOLS") or 0)
        up_to_date = bool(universe > 0 and missing == 0 and bar_symbols == universe)
        bars_lag_days = None
        raw_latest_date = coverage_row.get("LATEST_DAILY_BAR_DATE")
        if raw_latest_date is not None and hasattr(raw_latest_date, "toordinal"):
            bars_lag_days = (datetime.utcnow().date() - raw_latest_date).days

        return {
            "status": "SUCCESS",
            "up_to_date": up_to_date,
            "coverage": {
                "latest_daily_bar_date": latest_date,
                "latest_daily_bar_ts": latest_ts,
                "universe_symbols": universe,
                "bar_symbols_on_latest_date": bar_symbols,
                "missing_symbols_on_latest_date": missing,
                "bars_lag_days": bars_lag_days,
            },
            "pipeline": {
                "latest_pipeline_event_ts": latest_event_ts,
                "latest_effective_to_date": latest_effective,
            },
            "catchup_dry_run": catchup,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load IB daily job health: {e}")
    finally:
        conn.close()


@router.api_route("/{subpath:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def retired_management_subpaths(subpath: str, request: Request):
    _retired(f"/manage/{subpath} [{request.method}]")
