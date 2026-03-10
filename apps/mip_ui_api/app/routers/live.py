"""
GET /live/metrics — lightweight live metrics for header and Suggestions.
Read-only. Returns api_ok, snowflake_ok, updated_at, last_run, last_brief, outcomes.
"""
import json
import os
import subprocess
import uuid
from pathlib import Path
from datetime import datetime, timezone, timedelta
from queue import Empty, Queue
from threading import Event, Thread
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query, HTTPException, Body
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.config import get_snowflake_config
from app.db import get_connection, fetch_all, serialize_row, serialize_rows, SnowflakeAuthError
from app.training_status import score_training_status_row, DEFAULT_MIN_SIGNALS

router = APIRouter(prefix="/live", tags=["live"])


class PmAcceptRequest(BaseModel):
    actor: str


class RejectStaleActionRequest(BaseModel):
    actor: str = "portfolio_manager"
    notes: str | None = None


class ComplianceDecisionRequest(BaseModel):
    actor: str
    decision: str = Field(pattern="^(APPROVE|DENY)$")
    notes: str | None = None
    reference_id: str | None = None


class LivePortfolioConfigUpsertRequest(BaseModel):
    sim_portfolio_id: int | None = None
    ibkr_account_id: str | None = None
    adapter_mode: str | None = Field(default=None, pattern="^(PAPER|LIVE)$")
    base_currency: str | None = None
    max_positions: int | None = None
    max_position_pct: float | None = None
    cash_buffer_pct: float | None = None
    max_slippage_pct: float | None = None
    validity_window_sec: int | None = None
    quote_freshness_threshold_sec: int | None = None
    snapshot_freshness_threshold_sec: int | None = None
    drawdown_stop_pct: float | None = None
    bust_pct: float | None = None
    cooldown_bars: int | None = None
    is_active: bool | None = None


class ImportLiveActionsFromProposalsRequest(BaseModel):
    live_portfolio_id: int
    source_portfolio_id: int | None = None
    run_id: str | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    latest_batch_only: bool = True
    dedupe_by_symbol: bool = True
    max_proposal_age_days: int = Field(default=7, ge=1, le=180)


class ExecuteLiveActionRequest(BaseModel):
    actor: str
    attempt_n: int = 1


class ApproveAndSubmitLiveDecisionRequest(BaseModel):
    pm_actor: str = "portfolio_manager"
    compliance_actor: str = "compliance_user"
    intent_submit_actor: str = "intent_submitter"
    intent_approve_actor: str = "intent_approver"
    execution_actor: str = "execution_operator"
    committee_actor: str = "committee_orchestrator"
    committee_model: str = "claude-3-5-sonnet"
    attempt_n: int = 1
    force_refresh_1m: bool = True
    committee_recheck_before_submit: bool = True


class RevalidateLiveActionRequest(BaseModel):
    force_refresh_1m: bool = False


class CommitteeRunRequest(BaseModel):
    actor: str = "committee_orchestrator"
    model: str = "claude-3-5-sonnet"
    force_rerun: bool = False


class OpeningValidationRequest(BaseModel):
    force_refresh_1m: bool = False
    now_utc_iso: str | None = None


class IntentSubmitRequest(BaseModel):
    actor: str
    reference_id: str | None = None


class IntentApproveRequest(BaseModel):
    actor: str


class UpdateLiveOrderStatusRequest(BaseModel):
    actor: str
    status: str = Field(pattern="^(PARTIAL_FILL|FILLED|CANCELED|REJECTED)$")
    qty_filled: float | None = None
    avg_fill_price: float | None = None
    broker_order_id: str | None = None
    notes: str | None = None


class SimulatePaperWorkflowRequest(BaseModel):
    live_portfolio_id: int
    source_portfolio_id: int | None = None
    run_id: str | None = None
    limit: int = Field(default=50, ge=1, le=500)
    scenario: str = Field(default="PARTIAL_THEN_FILL", pattern="^(PARTIAL_THEN_FILL|CANCEL|REJECT)$")


class RebuildLiveStateRequest(BaseModel):
    portfolio_id: int | None = None
    dry_run: bool = True
    actor: str = "system_rebuild"


class SetLiveActivationRequest(BaseModel):
    portfolio_id: int
    actor: str
    force: bool = False


class DisableLiveActivationRequest(BaseModel):
    portfolio_id: int
    actor: str
    reason: str | None = None


class SmokeGateRequest(BaseModel):
    phase: str = Field(default="phase6_7", pattern="^(phase4|phase5|phase6_7|full)$")
    include_db_checks: bool = True
    include_write_checks: bool = False


_ALLOWED_TRANSITIONS = {
    "RESEARCH_IMPORTED": {"PENDING_OPEN_VALIDATION"},
    "PROPOSED": {"PENDING_OPEN_VALIDATION"},
    "PENDING_OPEN_VALIDATION": {"OPEN_BLOCKED", "OPEN_CAUTION", "OPEN_ELIGIBLE"},
    "OPEN_ELIGIBLE": {"PENDING_OPEN_STABILITY_REVIEW", "READY_FOR_APPROVAL_FLOW", "OPEN_BLOCKED"},
    "OPEN_CAUTION": {"PENDING_OPEN_STABILITY_REVIEW", "READY_FOR_APPROVAL_FLOW", "OPEN_BLOCKED"},
    "PENDING_OPEN_STABILITY_REVIEW": {"READY_FOR_APPROVAL_FLOW", "OPEN_BLOCKED"},
    "COMMITTEE_REVIEWED": {"READY_FOR_APPROVAL_FLOW"},
    "READY_FOR_APPROVAL_FLOW": {"PM_ACCEPTED"},
    "PM_ACCEPTED": {"COMPLIANCE_APPROVED", "COMPLIANCE_DENIED"},
    "COMPLIANCE_APPROVED": {"INTENT_SUBMITTED"},
    "INTENT_SUBMITTED": {"INTENT_APPROVED"},
    "INTENT_APPROVED": {"REVALIDATED_PASS", "REVALIDATED_FAIL"},
    "REVALIDATED_FAIL": {"REVALIDATED_PASS", "REVALIDATED_FAIL"},
    "REVALIDATED_PASS": {"REVALIDATED_PASS", "EXECUTION_REQUESTED"},
}

LIVE_POLICY_VERSION = "phase2_session_realism_v1"
EXECUTION_CLICK_MAX_REVALIDATION_SEC = 300
LIVE_ACTIVATION_POLICY_VERSION = "phase7_controlled_live_v1"
TRAINING_QUALIFICATION_POLICY_VERSION = "phase5_training_qualification_v1"
NEWS_CONTEXT_POLICY_VERSION = "phaseA_news_context_v1"
OPENING_VALIDATION_POLICY_VERSION = "phase_opening_validation_v1"
NY_TZ = ZoneInfo("America/New_York")
COMMITTEE_ROLES = [
    "PROPOSER",
    "TRADER_EXECUTION_REVIEWER",
    "RISK_MANAGER",
    "CHALLENGER",
    "PORTFOLIO_MANAGER",
    "POST_TRADE_REVIEWER",
]


def _fetch_live_action(cur, action_id: str) -> dict | None:
    cur.execute(
        """
        select
          ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, SIDE, PROPOSED_QTY, PROPOSED_PRICE, ASSET_CLASS,
          STATUS, VALIDITY_WINDOW_END, COMPLIANCE_STATUS, REVALIDATION_TS, REVALIDATION_PRICE,
          PRICE_DEVIATION_PCT, PRICE_GUARD_RESULT, REASON_CODES, EXECUTION_PRICE_SOURCE,
          PARAM_SNAPSHOT, ONE_MIN_BAR_TS,
          INTENT_SUBMITTED_BY, INTENT_SUBMITTED_TS, INTENT_APPROVED_BY, INTENT_APPROVED_TS, INTENT_REFERENCE_ID,
          COMMITTEE_REQUIRED, COMMITTEE_STATUS, COMMITTEE_RUN_ID, COMMITTEE_COMPLETED_TS, COMMITTEE_VERDICT,
          TRAINING_QUALIFICATION_SNAPSHOT, TRAINING_LIVE_ELIGIBLE, TRAINING_RANK_IMPACT, TRAINING_SIZE_CAP_FACTOR,
          TARGET_EXPECTATION_SNAPSHOT, TARGET_OPEN_CONDITION_FACTOR, TARGET_EXPECTATION_POLICY_VERSION,
          NEWS_CONTEXT_SNAPSHOT, NEWS_CONTEXT_STATE, NEWS_EVENT_SHOCK_FLAG, NEWS_FRESHNESS_BUCKET, NEWS_CONTEXT_POLICY_VERSION,
          REVALIDATION_OUTCOME, REVALIDATION_POLICY_VERSION, REVALIDATION_DATA_SOURCE
        from MIP.LIVE.LIVE_ACTIONS
        where ACTION_ID = %s
        """,
        (action_id,),
    )
    rows = fetch_all(cur)
    return rows[0] if rows else None


def _assert_transition_allowed(current_status: str | None, target_status: str) -> None:
    allowed = _ALLOWED_TRANSITIONS.get((current_status or "").upper(), set())
    if target_status not in allowed:
        raise HTTPException(
            status_code=409,
            detail=f"Invalid status transition: {current_status} -> {target_status}",
        )


def _write_reason_codes(cur, action_id: str, reason_codes: list[str]) -> None:
    cur.execute(
        """
        update MIP.LIVE.LIVE_ACTIONS
           set REASON_CODES = parse_json(%s),
               UPDATED_AT = current_timestamp()
         where ACTION_ID = %s
        """,
        (json.dumps(reason_codes), action_id),
    )


def _fetch_live_action_state(action_id: str) -> dict | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select ACTION_ID, STATUS, COMPLIANCE_STATUS, REASON_CODES, PORTFOLIO_ID, SYMBOL, SIDE
            from MIP.LIVE.LIVE_ACTIONS
            where ACTION_ID = %s
            """,
            (action_id,),
        )
        rows = fetch_all(cur)
        return rows[0] if rows else None
    finally:
        conn.close()


def _compute_snapshot_freshness_state(snapshot_age_sec: int | None, threshold_sec: int | None) -> str:
    if snapshot_age_sec is None:
        return "BLOCKED"
    threshold = int(threshold_sec or 300)
    if snapshot_age_sec <= threshold:
        return "FRESH"
    if snapshot_age_sec <= threshold * 2:
        return "AGING"
    if snapshot_age_sec <= threshold * 4:
        return "STALE"
    return "BLOCKED"


def _compute_drift_state(drift_status: str | None, unresolved_count: int) -> str:
    status = (drift_status or "").upper()
    if unresolved_count > 0:
        return "BLOCKED"
    if status in ("", "OK", "CLEAR", "HEALTHY"):
        return "CLEAR"
    if status in ("WARN", "WARNING", "CAUTION"):
        return "WARNING"
    return "BLOCKED"


def _required_next_step_for_status(status: str) -> str:
    status_upper = (status or "").upper()
    mapping = {
        "RESEARCH_IMPORTED": "Run committee review",
        "PROPOSED": "Run committee review",
        "PENDING_OPEN_VALIDATION": "Pass opening validation",
        "OPEN_ELIGIBLE": "Run committee review",
        "OPEN_CAUTION": "Run committee review (caution)",
        "PENDING_OPEN_STABILITY_REVIEW": "Wait for stabilization window",
        "READY_FOR_APPROVAL_FLOW": "PM accept",
        "PM_ACCEPTED": "Compliance decision",
        "COMPLIANCE_APPROVED": "Submit intent",
        "INTENT_SUBMITTED": "Approve intent",
        "INTENT_APPROVED": "Revalidate price and gates",
        "REVALIDATED_FAIL": "Revalidate again",
        "REVALIDATED_PASS": "Ready to submit order",
        "EXECUTION_REQUESTED": "Await broker/order lifecycle update",
    }
    return mapping.get(status_upper, "Review action details")


def _append_learning_ledger_event(
    cur,
    *,
    event_name: str,
    status: str,
    action_before: dict | None,
    action_after: dict | None,
    influence_delta: dict | None = None,
    outcome_state: dict | None = None,
    policy_version: str | None = None,
) -> None:
    """
    Best-effort append to canonical learning ledger.
    Never raise to caller.
    """
    try:
        after = action_after or {}
        before = action_before or {}
        after_snapshot = _parse_variant(after.get("PARAM_SNAPSHOT"))
        before_snapshot = _parse_variant(before.get("PARAM_SNAPSHOT"))
        run_id = (
            after.get("RUN_ID_VARCHAR")
            or before.get("RUN_ID_VARCHAR")
            or after_snapshot.get("run_id")
            or before_snapshot.get("run_id")
            or None
        )
        portfolio_id = after.get("PORTFOLIO_ID") or before.get("PORTFOLIO_ID")
        proposal_id = after.get("PROPOSAL_ID") or before.get("PROPOSAL_ID")
        symbol = after.get("SYMBOL") or before.get("SYMBOL")
        market_type = after.get("ASSET_CLASS") or before.get("ASSET_CLASS")
        live_action_id = after.get("ACTION_ID") or before.get("ACTION_ID")

        cur.execute(
            """
            call MIP.APP.SP_LEDGER_APPEND_EVENT(
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                try_parse_json(%s), try_parse_json(%s), try_parse_json(%s), try_parse_json(%s), try_parse_json(%s), %s
            )
            """,
            (
                "LIVE_EVENT",
                event_name,
                status,
                run_id,
                run_id,
                portfolio_id,
                proposal_id,
                live_action_id,
                None,  # live_order_id
                symbol,
                market_type,
                None,  # training_version
                policy_version,
                None,  # source facts hash
                json.dumps({"source": "live_router"}),
                json.dumps(before),
                json.dumps(after),
                json.dumps(influence_delta or {}),
                json.dumps({
                    "action_id": live_action_id,
                    "proposal_id": proposal_id,
                    "run_id": run_id,
                }),
                json.dumps(outcome_state or {}),
            ),
        )
    except Exception:
        # Non-fatal by design.
        return


def _parse_variant(v):
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return {}
    return {}


def _parse_list_variant(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _parse_iso_utc(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _market_session_bounds_utc(now_utc: datetime) -> tuple[datetime, datetime]:
    now_ny = now_utc.astimezone(NY_TZ)
    open_ny = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    close_ny = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
    return open_ny.astimezone(timezone.utc), close_ny.astimezone(timezone.utc)


def _is_market_open_ny(now_utc: datetime) -> bool:
    now_ny = now_utc.astimezone(NY_TZ)
    if now_ny.weekday() >= 5:
        return False
    open_utc, close_utc = _market_session_bounds_utc(now_utc)
    return open_utc <= now_utc <= close_utc


def _read_app_config(cur, keys: list[str]) -> dict[str, str]:
    if not keys:
        return {}
    placeholders = ",".join(["%s"] * len(keys))
    cur.execute(
        f"""
        select CONFIG_KEY, CONFIG_VALUE
        from MIP.APP.APP_CONFIG
        where CONFIG_KEY in ({placeholders})
        """,
        tuple(keys),
    )
    rows = fetch_all(cur)
    return {str(r.get("CONFIG_KEY")): str(r.get("CONFIG_VALUE")) for r in rows if r.get("CONFIG_KEY") is not None}


def _opening_policy(cur, cfg: dict, action: dict) -> dict:
    cfg_map = _read_app_config(
        cur,
        [
            "LIVE_OPENING_POLICY_MODE",
            "LIVE_OPENING_STABILIZATION_MINUTES",
            "LIVE_OPENING_FIRST_HOUR_CONFIRM_MINUTES",
            "LIVE_OPENING_SNAPSHOT_MAX_AGE_SEC",
            "LIVE_OPENING_GAP_CAUTION_PCT",
            "LIVE_OPENING_GAP_BLOCK_PCT",
        ],
    )
    mode = str(cfg_map.get("LIVE_OPENING_POLICY_MODE", "SHORT_STABILIZATION_REQUIRED")).upper()
    if mode not in ("IMMEDIATE_ELIGIBLE", "SHORT_STABILIZATION_REQUIRED", "FIRST_HOUR_CONFIRM_REQUIRED"):
        mode = "SHORT_STABILIZATION_REQUIRED"
    stabilization_minutes = int(float(cfg_map.get("LIVE_OPENING_STABILIZATION_MINUTES", "5")))
    first_hour_minutes = int(float(cfg_map.get("LIVE_OPENING_FIRST_HOUR_CONFIRM_MINUTES", "60")))
    snapshot_max_age_sec = int(
        float(cfg_map.get("LIVE_OPENING_SNAPSHOT_MAX_AGE_SEC", str(cfg.get("QUOTE_FRESHNESS_THRESHOLD_SEC") or 60)))
    )
    gap_caution_pct = float(cfg_map.get("LIVE_OPENING_GAP_CAUTION_PCT", "0.02"))
    gap_block_pct = float(cfg_map.get("LIVE_OPENING_GAP_BLOCK_PCT", "0.04"))

    training_snapshot = _parse_variant(action.get("TRAINING_QUALIFICATION_SNAPSHOT"))
    trusted_level = str(training_snapshot.get("trusted_level") or "").upper()
    live_eligible = bool(training_snapshot.get("live_eligible"))
    mode_effective = mode
    if mode == "IMMEDIATE_ELIGIBLE" and (not live_eligible or trusted_level not in ("TRUSTED", "HIGH")):
        mode_effective = "SHORT_STABILIZATION_REQUIRED"

    return {
        "mode_requested": mode,
        "mode_effective": mode_effective,
        "stabilization_minutes": max(stabilization_minutes, 0),
        "first_hour_confirm_minutes": max(first_hour_minutes, 5),
        "snapshot_max_age_sec": max(snapshot_max_age_sec, 15),
        "gap_caution_pct": max(gap_caution_pct, 0.0),
        "gap_block_pct": max(gap_block_pct, gap_caution_pct),
        "policy_version": OPENING_VALIDATION_POLICY_VERSION,
    }


def _expected_entry_reference(cur, action: dict) -> float | None:
    if action.get("PROPOSED_PRICE") is not None:
        try:
            return float(action.get("PROPOSED_PRICE"))
        except Exception:
            pass
    symbol = action.get("SYMBOL")
    if not symbol:
        return None
    cur.execute(
        """
        select CLOSE
        from MIP.MART.MARKET_BARS
        where SYMBOL = %s
          and INTERVAL_MINUTES = 1440
        order by TS desc
        limit 1
        """,
        (symbol,),
    )
    row = cur.fetchone()
    if not row:
        return None
    try:
        return float(row[0]) if row[0] is not None else None
    except Exception:
        return None


def _persist_opening_snapshot(cur, action: dict, new_status: str, reason_codes: list[str], opening_payload: dict):
    current_snapshot = _parse_variant(action.get("PARAM_SNAPSHOT"))
    current_snapshot["opening_validation"] = opening_payload
    cur.execute(
        """
        update MIP.LIVE.LIVE_ACTIONS
           set STATUS = %s,
               REASON_CODES = parse_json(%s),
               PARAM_SNAPSHOT = parse_json(%s),
               UPDATED_AT = current_timestamp()
         where ACTION_ID = %s
        """,
        (new_status, json.dumps(reason_codes), json.dumps(current_snapshot), action.get("ACTION_ID")),
    )


def _run_opening_sanity_gate(cur, action: dict, *, force_refresh_1m: bool = False, now_utc: datetime | None = None) -> dict:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    portfolio_id = action.get("PORTFOLIO_ID")
    cur.execute(
        """
        select
          IBKR_ACCOUNT_ID, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC, DRIFT_STATUS, IS_ACTIVE
        from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
        where PORTFOLIO_ID = %s
        """,
        (portfolio_id,),
    )
    cfg_rows = fetch_all(cur)
    if not cfg_rows:
        raise HTTPException(status_code=400, detail="Live portfolio config missing for opening validation.")
    cfg = cfg_rows[0]
    policy = _opening_policy(cur, cfg, action)
    refresh_result = {"attempted": False}
    if force_refresh_1m:
        refresh_result = _force_refresh_latest_one_minute_bars(cur)

    symbol = action.get("SYMBOL")
    cur.execute(
        """
        select TS, CLOSE
        from MIP.MART.MARKET_BARS
        where SYMBOL = %s
          and INTERVAL_MINUTES = 1
        order by TS desc
        limit 1
        """,
        (symbol,),
    )
    bar = cur.fetchone()
    bar_ts = bar[0] if bar else None
    bar_px = float(bar[1]) if bar and bar[1] is not None else None
    bar_age_sec = None
    if bar_ts is not None:
        bar_age_sec = (now_utc - bar_ts.replace(tzinfo=timezone.utc)).total_seconds()

    expected_entry_px = _expected_entry_reference(cur, action)
    gap_pct = None
    if expected_entry_px and bar_px:
        try:
            gap_pct = abs(bar_px - expected_entry_px) / max(abs(expected_entry_px), 1e-9)
        except Exception:
            gap_pct = None

    open_utc, _ = _market_session_bounds_utc(now_utc)
    wait_minutes = 0
    mode_effective = policy["mode_effective"]
    if mode_effective == "SHORT_STABILIZATION_REQUIRED":
        wait_minutes = int(policy["stabilization_minutes"])
    elif mode_effective == "FIRST_HOUR_CONFIRM_REQUIRED":
        wait_minutes = int(policy["first_hour_confirm_minutes"])
    ready_after_utc = open_utc + timedelta(minutes=wait_minutes)
    stability_ready = now_utc >= ready_after_utc

    hard_reasons: list[str] = []
    caution_reasons: list[str] = []
    if not _is_market_open_ny(now_utc):
        hard_reasons.append("OPEN_MARKET_CLOSED")
    if not symbol:
        hard_reasons.append("OPEN_MISSING_SYMBOL")
    if bar_ts is None:
        hard_reasons.append("OPEN_SNAPSHOT_MISSING")
    elif bar_age_sec is not None and bar_age_sec > float(policy["snapshot_max_age_sec"]):
        hard_reasons.append("OPEN_SNAPSHOT_STALE")
    if gap_pct is not None:
        if gap_pct > float(policy["gap_block_pct"]):
            hard_reasons.append("OPEN_GAP_BLOCK")
        elif gap_pct > float(policy["gap_caution_pct"]):
            caution_reasons.append("OPEN_GAP_CAUTION")
    else:
        caution_reasons.append("OPEN_GAP_UNAVAILABLE")

    guard = _compute_live_activation_guard(cur, int(portfolio_id))
    if not guard.get("eligible", False):
        hard_reasons.append("OPEN_LIVE_GUARD_FAILED")

    news_snapshot = _parse_variant(action.get("NEWS_CONTEXT_SNAPSHOT")) or _parse_variant(_parse_variant(action.get("PARAM_SNAPSHOT")).get("news_context"))
    news_shock = bool(news_snapshot.get("event_shock_flag"))
    news_freshness = str(news_snapshot.get("freshness_bucket") or "").upper()
    if news_shock and news_freshness in ("FRESH", "OVERNIGHT"):
        caution_reasons.append("OPEN_NEWS_SHOCK_CAUTION")

    if hard_reasons:
        result = "OPEN_BLOCKED"
    elif caution_reasons:
        result = "OPEN_CAUTION"
    else:
        result = "OPEN_ELIGIBLE"
    reason_codes = hard_reasons + caution_reasons
    opening_payload = {
        "result": result,
        "checked_at_utc": now_utc.isoformat(),
        "session_date_ny": now_utc.astimezone(NY_TZ).date().isoformat(),
        "market_open_ts_utc": open_utc.isoformat(),
        "symbol": symbol,
        "opening_snapshot_ts": bar_ts.isoformat() if bar_ts is not None else None,
        "opening_reference_price": bar_px,
        "opening_snapshot_age_sec": bar_age_sec,
        "expected_entry_reference_price": expected_entry_px,
        "gap_vs_expected_entry_pct": gap_pct,
        "stabilization": {
            "mode_requested": policy["mode_requested"],
            "mode_effective": mode_effective,
            "ready_after_utc": ready_after_utc.isoformat(),
            "is_ready": bool(stability_ready),
            "wait_minutes": wait_minutes,
        },
        "policy": policy,
        "reasons": reason_codes,
        "live_guard": {
            "eligible": bool(guard.get("eligible", False)),
            "reasons": guard.get("reasons") or [],
            "checks": guard.get("checks") or {},
        },
        "news_context": {
            "context_state": news_snapshot.get("context_state"),
            "event_shock_flag": news_shock,
            "freshness_bucket": news_snapshot.get("freshness_bucket"),
        },
        "refresh": refresh_result,
    }
    _persist_opening_snapshot(cur, action, result, reason_codes, opening_payload)
    action_after = _fetch_live_action(cur, action.get("ACTION_ID"))
    _append_learning_ledger_event(
        cur,
        event_name="LIVE_OPENING_SANITY_GATE",
        status=result,
        action_before=action,
        action_after=action_after,
        policy_version=OPENING_VALIDATION_POLICY_VERSION,
        influence_delta={
            "opening_result": result,
            "gap_vs_expected_entry_pct": gap_pct,
            "opening_snapshot_age_sec": bar_age_sec,
            "stability_mode_effective": mode_effective,
            "stability_wait_minutes": wait_minutes,
            "stability_ready": bool(stability_ready),
        },
        outcome_state={"opening_validation": opening_payload},
    )
    return {"result": result, "reason_codes": reason_codes, "opening_validation": opening_payload}

def _first_session_realism_checks(cur, action: dict, cfg: dict) -> tuple[list[str], dict]:
    """
    Fail-closed realism checks:
    require 1m-bar-sourced revalidation and fresh 1m market data.
    """
    reason_codes: list[str] = []
    symbol = action.get("SYMBOL")
    if not symbol:
        reason_codes.append("FIRST_SESSION_REALISM_MISSING_SYMBOL")
        return reason_codes, {"has_symbol": False}

    source = (action.get("EXECUTION_PRICE_SOURCE") or "").upper()
    if source != "ONE_MINUTE_BAR":
        reason_codes.append("FIRST_SESSION_REALISM_SOURCE_REQUIRED")

    one_min_bar_ts = action.get("ONE_MIN_BAR_TS")
    if not one_min_bar_ts:
        reason_codes.append("FIRST_SESSION_REALISM_MISSING_1M_REFERENCE")

    cur.execute(
        """
        select TS, CLOSE
        from MIP.MART.MARKET_BARS
        where SYMBOL = %s
          and INTERVAL_MINUTES = 1
        order by TS desc
        limit 1
        """,
        (symbol,),
    )
    latest_bar = cur.fetchone()
    latest_ts = latest_bar[0] if latest_bar else None
    latest_close = latest_bar[1] if latest_bar else None
    if not latest_ts:
        reason_codes.append("FIRST_SESSION_REALISM_NO_1M_BAR")
    else:
        now_utc = datetime.now(timezone.utc)
        bar_ts_utc = latest_ts.replace(tzinfo=timezone.utc)
        bar_age_sec = (now_utc - bar_ts_utc).total_seconds()
        max_age_sec = int(cfg.get("QUOTE_FRESHNESS_THRESHOLD_SEC") or 60)
        if bar_age_sec > max_age_sec:
            reason_codes.append("FIRST_SESSION_REALISM_1M_STALE")
        if one_min_bar_ts and latest_ts and one_min_bar_ts != latest_ts:
            reason_codes.append("FIRST_SESSION_REALISM_REVALIDATION_NOT_LATEST")

    details = {
        "symbol": symbol,
        "execution_price_source": source or None,
        "one_min_bar_ts": one_min_bar_ts,
        "latest_one_min_bar_ts": latest_ts,
        "latest_one_min_close": latest_close,
        "quote_freshness_threshold_sec": cfg.get("QUOTE_FRESHNESS_THRESHOLD_SEC"),
    }
    return reason_codes, details


def _safe_early_exit_details(details: dict) -> dict:
    steps = details.get("steps") if isinstance(details, dict) else {}
    ingestion = steps.get("ingestion") if isinstance(steps, dict) else {}
    early_exit = steps.get("early_exit") if isinstance(steps, dict) else {}
    return {
        "run_id": details.get("run_id"),
        "started_at": details.get("started_at"),
        "completed_at": details.get("completed_at"),
        "interval_minutes": details.get("interval_minutes"),
        "bars_ingested": details.get("bars_ingested"),
        "symbols_processed": details.get("symbols_processed"),
        "positions_evaluated": details.get("positions_evaluated"),
        "exit_signals": details.get("exit_signals"),
        "exits_executed": details.get("exits_executed"),
        "steps": {
            "ingestion_status": ingestion.get("status") if isinstance(ingestion, dict) else None,
            "early_exit_status": early_exit.get("status") if isinstance(early_exit, dict) else None,
        },
    }


def _compute_live_activation_guard(cur, portfolio_id: int) -> dict:
    cur.execute(
        """
        select
          PORTFOLIO_ID,
          IBKR_ACCOUNT_ID,
          ADAPTER_MODE,
          DRIFT_STATUS,
          IS_ACTIVE,
          SNAPSHOT_FRESHNESS_THRESHOLD_SEC
        from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
        where PORTFOLIO_ID = %s
        """,
        (portfolio_id,),
    )
    cfg_rows = fetch_all(cur)
    if not cfg_rows:
        return {"eligible": False, "reasons": ["LIVE_CONFIG_NOT_FOUND"], "config": None, "checks": {}}

    cfg = cfg_rows[0]
    reasons: list[str] = []
    checks: dict = {
        "is_active": bool(cfg.get("IS_ACTIVE")),
        "drift_status": cfg.get("DRIFT_STATUS"),
        "adapter_mode": cfg.get("ADAPTER_MODE"),
    }
    if cfg.get("IS_ACTIVE") is False:
        reasons.append("LIVE_CONFIG_INACTIVE")

    drift_status = (cfg.get("DRIFT_STATUS") or "").upper()

    account_id = cfg.get("IBKR_ACCOUNT_ID")
    cur.execute(
        """
        select SNAPSHOT_TS
        from MIP.LIVE.BROKER_SNAPSHOTS
        where SNAPSHOT_TYPE = 'NAV'
          and IBKR_ACCOUNT_ID = %s
        order by SNAPSHOT_TS desc
        limit 1
        """,
        (account_id,),
    )
    nav_rows = fetch_all(cur)
    latest_nav = nav_rows[0] if nav_rows else None
    if not latest_nav or not latest_nav.get("SNAPSHOT_TS"):
        reasons.append("MISSING_NAV_SNAPSHOT")
        checks["snapshot_age_sec"] = None
    else:
        snap_ts = latest_nav.get("SNAPSHOT_TS")
        snap_age_sec = int((datetime.now(timezone.utc) - snap_ts.replace(tzinfo=timezone.utc)).total_seconds())
        max_snap_age = int(cfg.get("SNAPSHOT_FRESHNESS_THRESHOLD_SEC") or 300)
        checks["snapshot_age_sec"] = snap_age_sec
        checks["snapshot_max_age_sec"] = max_snap_age
        if snap_age_sec > max_snap_age:
            reasons.append("NAV_SNAPSHOT_STALE")

    # Drift reconciliation is optional in environments that do not deploy
    # MIP.LIVE.DRIFT_LOG. Guard should not fail hard when missing.
    unresolved_drift_count = 0
    try:
        cur.execute(
            """
            select count(*) as CNT
            from MIP.LIVE.DRIFT_LOG
            where PORTFOLIO_ID = %s
              and coalesce(DRIFT_DETECTED, false) = true
              and RESOLUTION_TS is null
            """,
            (portfolio_id,),
        )
        drift_rows = fetch_all(cur)
        unresolved_drift_count = int((drift_rows[0] or {}).get("CNT") or 0)
    except Exception:
        checks["drift_table_available"] = False
    checks["unresolved_drift_count"] = unresolved_drift_count

    cur.execute(
        """
        select count(*) as CNT
        from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
        where coalesce(IS_ACTIVE, true) = true
          and upper(coalesce(ADAPTER_MODE, 'PAPER')) = 'LIVE'
          and PORTFOLIO_ID <> %s
        """,
        (portfolio_id,),
    )
    live_rows = fetch_all(cur)
    other_live_count = int((live_rows[0] or {}).get("CNT") or 0)
    checks["other_live_portfolios"] = other_live_count
    if other_live_count > 0:
        reasons.append("OTHER_LIVE_PORTFOLIO_ACTIVE")

    return {
        "eligible": len(reasons) == 0,
        "reasons": reasons,
        "config": serialize_row(cfg),
        "checks": checks,
    }


def _summary_hint_from_status_and_details(status: str | None, details: dict | None) -> str | None:
    """Derive a short summary hint for the run (same as runs router)."""
    if not status:
        return None
    s = (status or "").upper()
    if s == "SKIPPED_NO_NEW_BARS":
        return "No new bars"
    if s == "SKIP_RATE_LIMIT":
        return "Rate limit"
    if s == "SUCCESS_WITH_SKIPS":
        return "Success with skips"
    if s == "FAIL":
        return "Failed"
    if s == "SUCCESS":
        return None
    return None


@router.get("/metrics")
def get_live_metrics(portfolio_id: int = Query(1, description="Portfolio ID for latest brief")):
    """
    Single cheap request for UI to poll every 30–60s.
    Returns: api_ok, snowflake_ok, updated_at, last_run, last_brief, outcomes.
    last_brief uses found: false when no brief exists for portfolio_id.
    outcomes.since_last_run = count of outcomes with CALCULATED_AT > last_run.completed_at (or null/0 when no run).
    """
    updated_at = datetime.now(timezone.utc).isoformat()
    api_ok = True
    snowflake_ok = False
    last_run = None
    last_intraday_run = None
    last_brief = {"found": False}
    outcomes = {"total": 0, "last_calculated_at": None, "since_last_run": None}

    try:
        conn = get_connection()
        snowflake_ok = True
    except SnowflakeAuthError:
        pass
    except Exception:
        pass

    if not snowflake_ok:
        return {
            "api_ok": api_ok,
            "snowflake_ok": snowflake_ok,
            "updated_at": updated_at,
            "last_run": last_run,
            "last_intraday_run": last_intraday_run,
            "last_brief": last_brief,
            "outcomes": outcomes,
        }

    try:
        cur = conn.cursor()

        # --- Last run: same logic as /runs (MIP_AUDIT_LOG, PIPELINE, SP_RUN_DAILY_PIPELINE), most recent by completion
        runs_sql = """
        select EVENT_TS, RUN_ID, STATUS, DETAILS
        from MIP.APP.MIP_AUDIT_LOG
        where EVENT_TYPE = 'PIPELINE' and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
        order by EVENT_TS desc
        limit 200
        """
        cur.execute(runs_sql)
        run_rows = fetch_all(cur)
        runs_by_id = {}
        for r in run_rows:
            run_id = r.get("RUN_ID")
            if not run_id:
                continue
            ts = r.get("EVENT_TS")
            status = r.get("STATUS") or ""
            details = r.get("DETAILS")
            if isinstance(details, str):
                try:
                    details = json.loads(details) if details else {}
                except Exception:
                    details = {}
            if run_id not in runs_by_id:
                runs_by_id[run_id] = {"started_at": ts, "completed_at": ts, "status": status, "details": details}
            else:
                run = runs_by_id[run_id]
                if ts:
                    if run["started_at"] is None or (ts < run["started_at"]):
                        run["started_at"] = ts
                    if run["completed_at"] is None or (ts > run["completed_at"]):
                        run["completed_at"] = ts
                        run["status"] = status
                        run["details"] = details
        run_list = [
            {
                "run_id": rid,
                "started_at": r["started_at"].isoformat() if hasattr(r["started_at"], "isoformat") else r["started_at"],
                "completed_at": r["completed_at"].isoformat() if hasattr(r["completed_at"], "isoformat") else r["completed_at"],
                "status": r["status"] if r["status"] != "START" else "RUNNING",
                "summary_hint": _summary_hint_from_status_and_details(r["status"], r.get("details")),
            }
            for rid, r in runs_by_id.items()
        ]
        run_list.sort(key=lambda x: (x["completed_at"] or x["started_at"] or ""), reverse=True)
        if run_list:
            last_run = serialize_row(run_list[0])

        # --- Last intraday run: most recent from INTRADAY_PIPELINE_RUN_LOG
        intraday_sql = """
        select RUN_ID, STARTED_AT, COMPLETED_AT, STATUS,
               BARS_INGESTED, SIGNALS_GENERATED, SYMBOLS_PROCESSED
        from MIP.APP.INTRADAY_PIPELINE_RUN_LOG
        order by STARTED_AT desc
        limit 1
        """
        try:
            cur.execute(intraday_sql)
            irow = cur.fetchone()
            if irow:
                icols = [d[0] for d in cur.description]
                ir = dict(zip(icols, irow))
                ir_status = ir.get("STATUS") or ""
                last_intraday_run = serialize_row({
                    "run_id": ir.get("RUN_ID"),
                    "started_at": ir["STARTED_AT"].isoformat() if hasattr(ir.get("STARTED_AT"), "isoformat") else ir.get("STARTED_AT"),
                    "completed_at": ir["COMPLETED_AT"].isoformat() if hasattr(ir.get("COMPLETED_AT"), "isoformat") else ir.get("COMPLETED_AT"),
                    "status": ir_status if ir_status != "START" else "RUNNING",
                    "bars_ingested": ir.get("BARS_INGESTED"),
                    "signals_generated": ir.get("SIGNALS_GENERATED"),
                    "symbols_processed": ir.get("SYMBOLS_PROCESSED"),
                })
        except Exception:
            pass

        # --- Last brief: same as /briefs/latest
        brief_sql = """
        select
          mb.PORTFOLIO_ID as portfolio_id,
          coalesce(
            try_cast(mb.BRIEF:as_of_ts::varchar as timestamp_ntz),
            try_cast(get_path(mb.BRIEF, 'attribution.as_of_ts')::varchar as timestamp_ntz),
            mb.AS_OF_TS
          ) as as_of_ts,
          coalesce(
            get_path(mb.BRIEF, 'attribution.pipeline_run_id')::varchar,
            mb.PIPELINE_RUN_ID
          ) as pipeline_run_id,
          mb.AGENT_NAME as agent_name
        from MIP.AGENT_OUT.MORNING_BRIEF mb
        where mb.PORTFOLIO_ID = %s and coalesce(mb.AGENT_NAME, '') = 'MORNING_BRIEF'
        order by mb.AS_OF_TS desc
        limit 1
        """
        cur.execute(brief_sql, (portfolio_id,))
        brief_row = cur.fetchone()
        if brief_row:
            cols = [d[0] for d in cur.description]
            last_brief = serialize_row(dict(zip(cols, brief_row)))
            last_brief["found"] = True
        else:
            last_brief = {"found": False}

        # --- Outcomes: total, max(CALCULATED_AT), and since_last_run
        outcomes_sql = """
        select count(*) as total, max(CALCULATED_AT) as last_calculated_at
        from MIP.APP.RECOMMENDATION_OUTCOMES
        """
        cur.execute(outcomes_sql)
        out_row = cur.fetchone()
        if out_row:
            outcomes["total"] = int(out_row[0]) if out_row[0] is not None else 0
            lca = out_row[1]
            outcomes["last_calculated_at"] = lca.isoformat() if hasattr(lca, "isoformat") else (str(lca) if lca else None)

        last_run_completed_at = None
        if last_run and last_run.get("completed_at"):
            last_run_completed_at = last_run["completed_at"]

        if last_run_completed_at is not None:
            # Count outcomes where CALCULATED_AT > last_run.completed_at
            # last_run_completed_at is already ISO string from serialize_row
            since_sql = """
            select count(*) as cnt from MIP.APP.RECOMMENDATION_OUTCOMES
            where CALCULATED_AT > %s
            """
            cur.execute(since_sql, (last_run_completed_at,))
            since_row = cur.fetchone()
            outcomes["since_last_run"] = int(since_row[0]) if since_row and since_row[0] is not None else 0
        else:
            outcomes["since_last_run"] = 0

        conn.close()
    except Exception:
        snowflake_ok = False
        try:
            conn.close()
        except Exception:
            pass

    return {
        "api_ok": api_ok,
        "snowflake_ok": snowflake_ok,
        "updated_at": updated_at,
        "last_run": last_run,
        "last_intraday_run": last_intraday_run,
        "last_brief": last_brief,
        "outcomes": outcomes,
    }


def _project_root() -> Path:
    # .../MIP/apps/mip_ui_api/app/routers/live.py -> repo root
    return Path(__file__).resolve().parent.parent.parent.parent.parent.parent


def _run_on_demand_snapshot_sync(
    host: str,
    port: int,
    client_id: int,
    account: str | None,
    portfolio_id: int | None,
) -> dict:
    root = _project_root()
    py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "sync_ibkr_paper_snapshot.py"
    if not py.exists() or not script.exists():
        raise HTTPException(
            status_code=500,
            detail="Snapshot sync runtime not found (cursorfiles venv or sync script missing).",
        )

    cmd = [str(py), str(script), "--once", "--host", host, "--port", str(port), "--client-id", str(client_id)]
    if account:
        cmd.extend(["--account", account])
    if portfolio_id is not None:
        cmd.extend(["--portfolio-id", str(portfolio_id)])

    # Ensure the snapshot script resolves credentials from .env.agent instead of
    # inheriting API process Snowflake env (read-only role).
    child_env = dict(os.environ)
    for key in list(child_env.keys()):
        if key.startswith("SNOWFLAKE_"):
            child_env.pop(key, None)

    proc = subprocess.run(
        cmd,
        cwd=str(root),
        env=child_env,
        capture_output=True,
        text=True,
        timeout=90,
    )
    if proc.returncode != 0:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "On-demand snapshot sync failed.",
                "stderr": proc.stderr[-4000:],
                "stdout": proc.stdout[-4000:],
            },
        )
    out = (proc.stdout or "").strip()
    # script prints JSON payload on success
    try:
        json_start = out.rfind("{")
        payload = json.loads(out[json_start:]) if json_start >= 0 else {}
    except Exception:
        payload = {"raw_output": out}
    return payload


def _run_agent_snowflake_query(query: str, timeout_sec: int = 120) -> list | dict:
    """
    Execute a Snowflake query via agent runtime (.env.agent / CURSOR_AGENT).
    """
    root = _project_root()
    py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "query_snowflake.py"
    if not py.exists() or not script.exists():
        raise HTTPException(
            status_code=500,
            detail="Agent Snowflake runtime not found (cursorfiles venv or query script missing).",
        )

    cmd = [str(py), str(script), "-q", query, "--json"]
    child_env = dict(os.environ)
    for key in list(child_env.keys()):
        if key.startswith("SNOWFLAKE_"):
            child_env.pop(key, None)

    proc = subprocess.run(
        cmd,
        cwd=str(root),
        env=child_env,
        capture_output=True,
        text=True,
        timeout=timeout_sec,
    )
    if proc.returncode != 0:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Agent Snowflake query failed.",
                "stderr": proc.stderr[-4000:],
                "stdout": proc.stdout[-4000:],
            },
        )

    out = (proc.stdout or "").strip()
    json_start = out.find("[")
    if json_start < 0:
        json_start = out.find("{")
    if json_start < 0:
        return []
    try:
        return json.loads(out[json_start:])
    except Exception:
        return []


def _force_refresh_latest_one_minute_bars(cur) -> dict:
    """
    Best-effort 1-minute bar refresh before revalidation.
    Non-fatal; caller still proceeds with currently available data.
    """
    try:
        cur.execute("call MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS(1)")
        row = cur.fetchone()
        payload = row[0] if row else None
        return {"attempted": True, "status": "SUCCESS", "payload": payload}
    except Exception as exc:
        return {"attempted": True, "status": "FAIL", "error": str(exc)}


def _fetch_training_min_signals(cur) -> int:
    try:
        cur.execute(
            """
            select MIN_SIGNALS
            from MIP.APP.TRAINING_GATE_PARAMS
            where IS_ACTIVE
            qualify row_number() over (order by PARAM_SET) = 1
            """
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        pass
    return int(DEFAULT_MIN_SIGNALS)


def _fetch_training_version(cur) -> str | None:
    try:
        cur.execute(
            """
            select TRAINING_VERSION
            from MIP.APP.V_TRAINING_VERSION_CURRENT
            where POLICY_NAME = 'DAILY_POLICY'
            limit 1
            """
        )
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _build_training_qualification_snapshot(
    cur,
    *,
    symbol: str | None,
    market_type: str | None,
    pattern_id,
    interval_minutes: int = 1440,
    target_weight=None,
) -> dict:
    symbol_norm = (symbol or "").upper().strip()
    market_type_norm = (market_type or "").upper().strip()
    if not symbol_norm or not market_type_norm:
        return {
            "available": False,
            "reason": "MISSING_SYMBOL_OR_MARKET_TYPE",
            "policy_version": TRAINING_QUALIFICATION_POLICY_VERSION,
        }
    if pattern_id is None:
        return {
            "available": False,
            "reason": "MISSING_PATTERN_ID",
            "symbol": symbol_norm,
            "market_type": market_type_norm,
            "policy_version": TRAINING_QUALIFICATION_POLICY_VERSION,
        }

    min_signals = _fetch_training_min_signals(cur)
    training_version = _fetch_training_version(cur)
    cur.execute(
        """
        with recs as (
            select RECOMMENDATION_ID
            from MIP.APP.RECOMMENDATION_LOG
            where upper(SYMBOL) = %s
              and upper(MARKET_TYPE) = %s
              and PATTERN_ID = %s
              and INTERVAL_MINUTES = %s
        )
        select
            count(*) as RECS_TOTAL,
            count_if(o.EVAL_STATUS = 'SUCCESS') as OUTCOMES_TOTAL,
            count(distinct iff(o.EVAL_STATUS = 'SUCCESS', o.HORIZON_BARS, null)) as HORIZONS_COVERED
        from recs r
        left join MIP.APP.RECOMMENDATION_OUTCOMES o
          on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
        """,
        (symbol_norm, market_type_norm, pattern_id, interval_minutes),
    )
    rows = fetch_all(cur)
    metric = rows[0] if rows else {}
    recs_total = int(metric.get("RECS_TOTAL") or 0)
    outcomes_total = int(metric.get("OUTCOMES_TOTAL") or 0)
    horizons_covered = int(metric.get("HORIZONS_COVERED") or 0)
    score = score_training_status_row(recs_total, outcomes_total, horizons_covered, min_signals=min_signals)

    cur.execute(
        """
        select TRUSTED_LEVEL, READY_FLAG, REASON
        from MIP.MART.V_SYMBOL_TRAINING_READINESS
        where upper(SYMBOL) = %s
        limit 1
        """,
        (symbol_norm,),
    )
    readiness_rows = fetch_all(cur)
    readiness = readiness_rows[0] if readiness_rows else {}

    trusted_level = str(readiness.get("TRUSTED_LEVEL") or "UNTRUSTED")
    ready_flag = bool(readiness.get("READY_FLAG")) if readiness.get("READY_FLAG") is not None else False
    readiness_reason = str(readiness.get("REASON") or "READINESS_UNKNOWN")

    live_eligible = bool(ready_flag and score.maturity_stage in ("LEARNING", "CONFIDENT"))
    if score.maturity_stage == "CONFIDENT" and trusted_level == "TRUSTED":
        size_cap_factor = 1.0
        rank_impact = "PROMOTE"
    elif score.maturity_stage in ("LEARNING", "CONFIDENT") and trusted_level in ("TRUSTED", "WATCH"):
        size_cap_factor = 0.75
        rank_impact = "NEUTRAL"
    elif score.maturity_stage == "WARMING_UP":
        size_cap_factor = 0.5
        rank_impact = "DEMOTE"
    else:
        size_cap_factor = 0.0
        rank_impact = "DEMOTE"

    proposed_target_weight = float(target_weight) if target_weight is not None else None
    capped_target_weight = (proposed_target_weight * size_cap_factor) if proposed_target_weight is not None else None
    reason_codes: list[str] = []
    if not live_eligible:
        reason_codes.append("TRAINING_NOT_LIVE_ELIGIBLE")
    if size_cap_factor < 1.0:
        reason_codes.append("TRAINING_SIZE_CAP_APPLIED")
    if trusted_level != "TRUSTED":
        reason_codes.append(f"TRAINING_TRUST_{trusted_level}")
    reason_codes.append(f"TRAINING_MATURITY_{score.maturity_stage}")

    return {
        "available": True,
        "policy_version": TRAINING_QUALIFICATION_POLICY_VERSION,
        "training_version": training_version,
        "as_of_ts": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol_norm,
        "market_type": market_type_norm,
        "pattern_id": int(pattern_id),
        "interval_minutes": int(interval_minutes),
        "recs_total": recs_total,
        "outcomes_total": outcomes_total,
        "horizons_covered": horizons_covered,
        "min_signals": int(min_signals),
        "maturity_score": float(score.maturity_score),
        "maturity_stage": score.maturity_stage,
        "trusted_level": trusted_level,
        "ready_flag": ready_flag,
        "readiness_reason": readiness_reason,
        "live_eligible": live_eligible,
        "rank_impact": rank_impact,
        "size_cap_factor": float(size_cap_factor),
        "proposed_target_weight": proposed_target_weight,
        "capped_target_weight": capped_target_weight,
        "reason_codes": reason_codes,
    }


def _clamp_float(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))


def _build_target_expectation_snapshot(
    cur,
    *,
    symbol: str | None,
    market_type: str | None,
    pattern_id,
    interval_minutes: int = 1440,
    open_condition_factor: float = 1.0,
) -> dict:
    symbol_norm = (symbol or "").upper().strip()
    market_type_norm = (market_type or "").upper().strip()
    reasons: list[str] = []

    base_return = None
    if symbol_norm and market_type_norm and pattern_id is not None:
        cur.execute(
            """
            select
              avg(case when o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null then o.REALIZED_RETURN end) as AVG_RETURN,
              count_if(o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null) as N_RET
            from MIP.APP.RECOMMENDATION_LOG r
            join MIP.APP.RECOMMENDATION_OUTCOMES o
              on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
            where upper(r.SYMBOL) = %s
              and upper(r.MARKET_TYPE) = %s
              and r.PATTERN_ID = %s
              and r.INTERVAL_MINUTES = %s
              and o.HORIZON_BARS = 5
            """,
            (symbol_norm, market_type_norm, pattern_id, interval_minutes),
        )
        rows = fetch_all(cur)
        m = rows[0] if rows else {}
        n_ret = int(m.get("N_RET") or 0)
        if n_ret >= 10 and m.get("AVG_RETURN") is not None:
            base_return = float(m.get("AVG_RETURN"))
            reasons.append("BASE_FROM_SYMBOL_PATTERN_H5")

    if base_return is None and market_type_norm and pattern_id is not None:
        cur.execute(
            """
            select
              avg(case when o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null then o.REALIZED_RETURN end) as AVG_RETURN,
              count_if(o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null) as N_RET
            from MIP.APP.RECOMMENDATION_LOG r
            join MIP.APP.RECOMMENDATION_OUTCOMES o
              on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
            where upper(r.MARKET_TYPE) = %s
              and r.PATTERN_ID = %s
              and r.INTERVAL_MINUTES = %s
              and o.HORIZON_BARS = 5
            """,
            (market_type_norm, pattern_id, interval_minutes),
        )
        rows = fetch_all(cur)
        m = rows[0] if rows else {}
        n_ret = int(m.get("N_RET") or 0)
        if n_ret >= 30 and m.get("AVG_RETURN") is not None:
            base_return = float(m.get("AVG_RETURN"))
            reasons.append("BASE_FROM_MARKET_PATTERN_H5")

    if base_return is None:
        base_return = 0.02
        reasons.append("BASE_DEFAULT_FALLBACK")

    # Keep expected move bands realistic and bounded.
    base_return = _clamp_float(base_return, 0.005, 0.12)
    open_factor = _clamp_float(float(open_condition_factor or 1.0), 0.5, 1.0)
    conservative = _clamp_float(base_return * 0.7 * open_factor, 0.003, 0.12)
    base = _clamp_float(base_return * open_factor, 0.004, 0.14)
    strong = _clamp_float(base_return * 1.3 * open_factor, 0.005, 0.18)

    if open_factor < 1.0:
        reasons.append("OPEN_CONDITION_COMPRESSION_APPLIED")

    return {
        "policy_version": "phase6_target_bands_v1",
        "as_of_ts": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol_norm or None,
        "market_type": market_type_norm or None,
        "pattern_id": int(pattern_id) if pattern_id is not None else None,
        "interval_minutes": int(interval_minutes),
        "open_condition_factor": open_factor,
        "bands": {
            "conservative": round(conservative, 6),
            "base": round(base, 6),
            "strong": round(strong, 6),
        },
        "reason_codes": reasons,
    }


def _to_dt_utc(v):
    if v is None:
        return None
    if hasattr(v, "replace"):
        try:
            return v.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None


def _news_freshness_bucket(snapshot_age_minutes: float | None) -> str:
    if snapshot_age_minutes is None:
        return "UNKNOWN"
    if snapshot_age_minutes <= 90:
        return "FRESH"
    if snapshot_age_minutes <= 16 * 60:
        return "OVERNIGHT"
    if snapshot_age_minutes <= 48 * 60:
        return "WARM"
    return "STALE"


def _normalize_news_context_snapshot(source_signals_raw, rationale_raw, proposal_ts=None) -> dict:
    source_signals = _parse_variant(source_signals_raw)
    rationale = _parse_variant(rationale_raw)
    news_context = _parse_variant(source_signals.get("news_context"))
    news_agg = _parse_variant(source_signals.get("news_agg"))
    news_features = _parse_variant(source_signals.get("news_features"))

    snapshot_age_minutes = source_signals.get("news_snapshot_age_minutes")
    try:
        snapshot_age_minutes = float(snapshot_age_minutes) if snapshot_age_minutes is not None else None
    except Exception:
        snapshot_age_minutes = None
    freshness_bucket = _news_freshness_bucket(snapshot_age_minutes)

    badge = str(news_context.get("news_context_badge") or news_agg.get("badge") or "NEUTRAL").upper()
    conflict = bool(source_signals.get("news_conflict_high")) or bool(news_agg.get("conflict"))
    uncertainty = bool(source_signals.get("news_uncertainty_high")) or bool(news_context.get("uncertainty_flag"))
    event_risk = bool(source_signals.get("news_event_risk_high")) or bool(source_signals.get("news_block_new_entry"))
    info_pressure = float(news_agg.get("info_pressure") or news_features.get("news_pressure") or 0.0)
    is_stale = bool(source_signals.get("news_is_stale")) or freshness_bucket == "STALE"

    if event_risk and freshness_bucket in ("FRESH", "OVERNIGHT"):
        context_state = "DESTABILIZING"
    elif conflict or uncertainty or badge in ("HOT", "ALERT"):
        context_state = "CAUTIONARY"
    elif badge in ("COOL", "LOW") and not event_risk:
        context_state = "SUPPORTIVE"
    else:
        context_state = "NEUTRAL"

    if info_pressure >= 0.75:
        intensity_level = "HIGH"
    elif info_pressure >= 0.35:
        intensity_level = "MEDIUM"
    else:
        intensity_level = "LOW"

    proposal_dt = _to_dt_utc(proposal_ts)
    last_pub_dt = _to_dt_utc(news_context.get("last_news_published_at") or news_agg.get("last_published_at"))
    timing_model = "OLDER_BACKGROUND"
    if proposal_dt and last_pub_dt:
        delta_min = (proposal_dt - last_pub_dt).total_seconds() / 60.0
        if 0 <= delta_min <= 90:
            timing_model = "SAME_SESSION_FRESH"
        elif 0 <= delta_min <= 16 * 60:
            timing_model = "OVERNIGHT"

    top_clusters = news_agg.get("top_clusters") if isinstance(news_agg.get("top_clusters"), list) else []
    top_events = news_features.get("top_events") if isinstance(news_features.get("top_events"), list) else []
    theme_labels = []
    for x in (top_clusters + top_events):
        if isinstance(x, dict):
            lbl = x.get("label") or x.get("name") or x.get("event_type")
            if lbl:
                theme_labels.append(str(lbl))
        elif x:
            theme_labels.append(str(x))
    theme_labels = list(dict.fromkeys(theme_labels))[:8]

    interpretation_confidence = 0.85
    if is_stale:
        interpretation_confidence = 0.45
    elif context_state in ("CAUTIONARY", "DESTABILIZING"):
        interpretation_confidence = 0.75

    reason_codes = list(source_signals.get("news_reasons") or rationale.get("news_reasons") or [])
    if not isinstance(reason_codes, list):
        reason_codes = []
    if is_stale:
        reason_codes.append("NEWS_CONTEXT_STALE")
    if context_state == "DESTABILIZING":
        reason_codes.append("NEWS_CONTEXT_DESTABILIZING")
    reason_codes = list(dict.fromkeys([str(r) for r in reason_codes]))[:20]

    return {
        "policy_version": NEWS_CONTEXT_POLICY_VERSION,
        "as_of_ts": datetime.now(timezone.utc).isoformat(),
        "freshness_bucket": freshness_bucket,
        "timing_model": timing_model,
        "intensity_level": intensity_level,
        "context_state": context_state,
        "event_shock_flag": bool(event_risk and freshness_bucket in ("FRESH", "OVERNIGHT")),
        "relevance": "SYMBOL_LINKED" if (news_context or news_agg or news_features) else "LOW",
        "theme_labels": theme_labels,
        "interpretation_confidence": interpretation_confidence,
        "snapshot_age_minutes": snapshot_age_minutes,
        "is_stale": is_stale,
        "badge": badge,
        "news_count": news_context.get("news_count"),
        "news_reasons": reason_codes,
        "raw": {
            "news_context": news_context,
            "news_agg": news_agg,
            "news_features": news_features,
        },
    }


def _fetch_latest_symbol_news_context(cur, symbol: str | None, market_type: str | None) -> dict:
    symbol_norm = (symbol or "").upper().strip()
    market_type_norm = (market_type or "").upper().strip()
    if not symbol_norm or not market_type_norm:
        return {"available": False, "reason": "MISSING_SYMBOL_OR_MARKET_TYPE"}
    try:
        cur.execute(
            """
            select
              AS_OF_TS_BUCKET, SYMBOL, MARKET_TYPE, INFO_PRESSURE, NOVELTY, CONFLICT, BADGE,
              LAST_PUBLISHED_AT, LAST_INGESTED_AT, SNAPSHOT_TS, TOP_CLUSTERS
            from MIP.MART.V_NEWS_AGG_LATEST
            where SYMBOL = %s
              and MARKET_TYPE = %s
            limit 1
            """,
            (symbol_norm, market_type_norm),
        )
        rows = fetch_all(cur)
        if not rows:
            return {"available": False, "reason": "NO_NEWS_CONTEXT_ROW"}
        row = rows[0]
        now_utc = datetime.now(timezone.utc)
        snap_dt = _to_dt_utc(row.get("SNAPSHOT_TS"))
        age_minutes = None
        if snap_dt:
            age_minutes = max(0.0, (now_utc - snap_dt).total_seconds() / 60.0)
        normalized = {
            "policy_version": NEWS_CONTEXT_POLICY_VERSION,
            "as_of_ts": now_utc.isoformat(),
            "freshness_bucket": _news_freshness_bucket(age_minutes),
            "timing_model": "SAME_SESSION_FRESH" if (age_minutes is not None and age_minutes <= 90) else ("OVERNIGHT" if (age_minutes is not None and age_minutes <= 16 * 60) else "OLDER_BACKGROUND"),
            "intensity_level": "HIGH" if float(row.get("INFO_PRESSURE") or 0.0) >= 0.75 else ("MEDIUM" if float(row.get("INFO_PRESSURE") or 0.0) >= 0.35 else "LOW"),
            "context_state": "DESTABILIZING" if str(row.get("BADGE") or "").upper() in ("HOT", "ALERT") and bool(row.get("CONFLICT")) else ("CAUTIONARY" if bool(row.get("CONFLICT")) else "NEUTRAL"),
            "event_shock_flag": str(row.get("BADGE") or "").upper() in ("HOT", "ALERT") and (_news_freshness_bucket(age_minutes) in ("FRESH", "OVERNIGHT")),
            "relevance": "SYMBOL_LINKED",
            "theme_labels": row.get("TOP_CLUSTERS") if isinstance(row.get("TOP_CLUSTERS"), list) else [],
            "interpretation_confidence": 0.8 if age_minutes is not None and age_minutes <= 16 * 60 else 0.55,
            "snapshot_age_minutes": age_minutes,
            "is_stale": _news_freshness_bucket(age_minutes) == "STALE",
            "badge": str(row.get("BADGE") or "NEUTRAL").upper(),
            "news_reasons": [],
            "raw": serialize_row(row),
        }
        return {"available": True, **normalized}
    except Exception as exc:
        return {"available": False, "reason": f"NEWS_QUERY_FAILED: {exc}"}


def _collect_news_monitoring_escalations(cur) -> dict:
    try:
        cur.execute(
            """
            with latest_positions as (
                select s.SYMBOL
                from MIP.LIVE.BROKER_SNAPSHOTS s
                where s.SNAPSHOT_TYPE = 'POSITION'
                  and s.SNAPSHOT_TS = (
                    select max(SNAPSHOT_TS)
                    from MIP.LIVE.BROKER_SNAPSHOTS
                    where SNAPSHOT_TYPE = 'POSITION'
                  )
                  and coalesce(s.POSITION_QTY, 0) <> 0
            )
            select
                p.SYMBOL,
                n.BADGE,
                n.CONFLICT,
                n.INFO_PRESSURE,
                n.LAST_PUBLISHED_AT,
                n.SNAPSHOT_TS
            from latest_positions p
            left join MIP.MART.V_NEWS_AGG_LATEST n
              on n.SYMBOL = p.SYMBOL
             and n.MARKET_TYPE = 'STOCK'
            """
        )
        rows = fetch_all(cur)
        escalations = []
        for r in rows:
            snap = _fetch_latest_symbol_news_context(cur, r.get("SYMBOL"), "STOCK")
            if not snap.get("available"):
                continue
            if snap.get("context_state") in ("CAUTIONARY", "DESTABILIZING"):
                escalations.append(
                    {
                        "symbol": r.get("SYMBOL"),
                        "context_state": snap.get("context_state"),
                        "event_shock_flag": bool(snap.get("event_shock_flag")),
                        "freshness_bucket": snap.get("freshness_bucket"),
                        "intensity_level": snap.get("intensity_level"),
                    }
                )
        return {
            "positions_with_news_escalation": len(escalations),
            "escalation_symbols": escalations[:20],
        }
    except Exception as exc:
        return {
            "positions_with_news_escalation": 0,
            "escalation_symbols": [],
            "error": str(exc),
        }


def _fetch_committee_pw_evidence(cur, action_id: str, portfolio_id: int | None) -> dict:
    if portfolio_id is None:
        return {"available": False, "reason": "MISSING_PORTFOLIO_ID"}
    try:
        cur.execute(
            """
            select
              ACTION_ID,
              PORTFOLIO_ID,
              PW_AS_OF_TS,
              TOP_OUTPERFORMERS,
              TOP_RECOMMENDATIONS,
              EVIDENCE_SUMMARY
            from MIP.APP.V_LIVE_ACTION_PARALLEL_WORLDS_EVIDENCE
            where ACTION_ID = %s
            limit 1
            """,
            (action_id,),
        )
        rows = fetch_all(cur)
        if not rows:
            return {"available": False, "reason": "NO_EVIDENCE_ROW"}
        row = rows[0]
        return {
            "available": True,
            "as_of_ts": row.get("PW_AS_OF_TS"),
            "top_outperformers": _parse_variant(row.get("TOP_OUTPERFORMERS")),
            "top_recommendations": _parse_variant(row.get("TOP_RECOMMENDATIONS")),
            "summary": _parse_variant(row.get("EVIDENCE_SUMMARY")),
        }
    except Exception as exc:
        return {"available": False, "reason": f"QUERY_FAILED: {exc}"}


def _extract_cortex_text(raw) -> str:
    if raw is None:
        return ""
    if isinstance(raw, dict):
        choices = raw.get("choices", [])
        if choices:
            msg = choices[0].get("messages", "") or choices[0].get("message", "")
            if isinstance(msg, dict):
                return str(msg.get("content", "") or "")
            return str(msg or "")
        return json.dumps(raw)
    if isinstance(raw, str):
        return raw
    return str(raw)


def _extract_first_json_object(text: str) -> str | None:
    """
    Extract first balanced JSON object from mixed text.
    Handles braces inside quoted strings.
    """
    if not text:
        return None
    start = text.find("{")
    while start >= 0:
        depth = 0
        in_str = False
        esc = False
        for i in range(start, len(text)):
            ch = text[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == "\"":
                    in_str = False
                continue
            if ch == "\"":
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start : i + 1]
        start = text.find("{", start + 1)
    return None


def _parse_cortex_json_text(text: str) -> dict:
    if not text:
        raise ValueError("Empty model response.")
    cleaned = text.strip()
    # Normalize fenced blocks if present.
    if "```" in cleaned:
        cleaned = cleaned.replace("```json", "```").replace("```JSON", "```")
        parts = cleaned.split("```")
        fenced = [p.strip() for p in parts if p.strip()]
        if fenced:
            # Prefer first fenced body.
            cleaned = fenced[0]
    # 1) direct parse
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    # 2) parse first object from mixed prose+json
    candidate = _extract_first_json_object(cleaned)
    if candidate:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("Cortex response was not parseable JSON object.")


def _call_cortex_json(cur, model: str, prompt: str) -> dict:
    cur.execute("select snowflake.cortex.complete(%s, %s) as response", (model, prompt))
    row = cur.fetchone()
    raw = row[0] if row else None
    text = _extract_cortex_text(raw).strip()
    return _parse_cortex_json_text(text)


def _committee_fallback(role: str, error_hint: str | None = None) -> dict:
    err = (error_hint or "").strip()
    if len(err) > 240:
        err = err[:240] + "..."
    return {
        "role": role,
        "stance": "CONDITIONAL",
        "confidence": 0.45,
        "summary": "Fallback committee output due to model/unparseable response.",
        "size_factor": 0.5,
        "reasons": ["MODEL_FALLBACK"] + ([f"MODEL_ERROR:{err}"] if err else []),
        "assumptions": ["Use deterministic risk gates before execution."],
    }


def _committee_prompt(role: str, context: dict, round_n: int = 1, prior_messages: list[dict] | None = None) -> str:
    prior_messages = prior_messages or []
    role_focus = {
        "PROPOSER": "Build the strongest symbol-specific case with concrete target/hold assumptions.",
        "TRADER_EXECUTION_REVIEWER": "Focus on execution realism, opening behavior, slippage, and timing constraints.",
        "RISK_MANAGER": "Focus on size, drawdown/correlation, and explicit risk controls.",
        "CHALLENGER": "Provide the strongest falsification and what could break this setup.",
        "PORTFOLIO_MANAGER": "Focus on cross-candidate capital allocation priority and portfolio fit.",
        "POST_TRADE_REVIEWER": "Focus on ex-ante evaluability and what would validate/invalidate this call later.",
    }.get(role, "Provide role-specific analysis.")
    return (
        "You are one role in an institutional multi-agent trade committee.\n"
        "Return ONLY a JSON object with keys:\n"
        "{\"stance\":\"SUPPORT|CONDITIONAL|BLOCK\",\"confidence\":0.0-1.0,"
        "\"summary\":\"...\",\"size_factor\":0.0-1.0,"
        "\"should_enter\":true|false,"
        "\"target_return\":number,"
        "\"hold_bars\":integer,"
        "\"early_exit_target_return\":number,"
        "\"reasons\":[\"...\"],\"assumptions\":[\"...\"]}\n"
        "Rules:\n"
        "- Be strict and risk-aware.\n"
        "- If data freshness is weak, prefer CONDITIONAL or BLOCK.\n"
        "- Contribute to joint decision dimensions: enter/size/target/hold/early-exit.\n"
        "- Return ONE valid JSON object only, no preface/suffix.\n"
        "- Do not include markdown or code fences.\n"
        "- If uncertain, still return schema with null fields where needed.\n"
        "- Include at least one symbol-specific reason with concrete values from context.\n"
        "- Do not repeat prior messages verbatim; add a unique angle for this role.\n"
        f"Round: {round_n}\n"
        f"Role: {role}\n"
        f"Role focus: {role_focus}\n"
        f"Prior agent messages JSON: {json.dumps(prior_messages, default=str)}\n"
        f"Context JSON: {json.dumps(context, default=str)}\n"
    )


def _normalize_role_output(role: str, out: dict, symbol: str | None = None) -> dict:
    stance = str(out.get("stance", "CONDITIONAL")).upper()
    if stance not in ("SUPPORT", "CONDITIONAL", "BLOCK"):
        stance = "CONDITIONAL"
    try:
        confidence = float(out.get("confidence", 0.5))
    except Exception:
        confidence = 0.5
    confidence = max(0.0, min(1.0, confidence))
    try:
        size_factor = float(out.get("size_factor", 1.0))
    except Exception:
        size_factor = 1.0
    size_factor = max(0.0, min(1.0, size_factor))
    should_enter = bool(out.get("should_enter", stance != "BLOCK"))
    try:
        target_return = float(out.get("target_return")) if out.get("target_return") is not None else None
    except Exception:
        target_return = None
    try:
        hold_bars = int(out.get("hold_bars")) if out.get("hold_bars") is not None else None
    except Exception:
        hold_bars = None
    try:
        early_exit_target_return = (
            float(out.get("early_exit_target_return")) if out.get("early_exit_target_return") is not None else None
        )
    except Exception:
        early_exit_target_return = None
    summary = str(out.get("summary", "")).strip()[:4000]
    symbol_norm = (symbol or "").upper().strip()
    if symbol_norm and symbol_norm not in summary.upper():
        summary = f"{symbol_norm}: {summary}" if summary else f"{symbol_norm}: no summary provided."
    reasons = out.get("reasons") if isinstance(out.get("reasons"), list) else []
    if not reasons:
        reasons = ["MISSING_EXPLICIT_REASONS"]
    return {
        "role": role,
        "stance": stance,
        "confidence": confidence,
        "summary": summary,
        "size_factor": size_factor,
        "should_enter": should_enter,
        "target_return": target_return,
        "hold_bars": hold_bars,
        "early_exit_target_return": early_exit_target_return,
        "reasons": reasons,
        "assumptions": out.get("assumptions") if isinstance(out.get("assumptions"), list) else [],
    }


def _aggregate_committee(outputs: list[dict]) -> dict:
    stances = [o.get("stance", "CONDITIONAL") for o in outputs]
    size_factors = [float(o.get("size_factor", 1.0)) for o in outputs if o.get("size_factor") is not None]
    if any(s == "BLOCK" for s in stances):
        recommendation = "BLOCK"
    elif any(s == "CONDITIONAL" for s in stances):
        recommendation = "PROCEED_REDUCED"
    else:
        recommendation = "PROCEED"
    size_factor = min(size_factors) if size_factors else 1.0
    size_factor = max(0.0, min(1.0, size_factor))
    should_enter_votes = [bool(o.get("should_enter")) for o in outputs]
    should_enter = bool(sum(1 for v in should_enter_votes if v) >= max(1, (len(should_enter_votes) + 1) // 2))
    target_returns = [float(o["target_return"]) for o in outputs if o.get("target_return") is not None]
    hold_bars_vals = [int(o["hold_bars"]) for o in outputs if o.get("hold_bars") is not None]
    early_exit_targets = [float(o["early_exit_target_return"]) for o in outputs if o.get("early_exit_target_return") is not None]
    confidence = 0.0
    if outputs:
        confidence = sum(float(o.get("confidence", 0.0)) for o in outputs) / len(outputs)
    target_return = (sum(target_returns) / len(target_returns)) if target_returns else None
    hold_bars = int(round(sum(hold_bars_vals) / len(hold_bars_vals))) if hold_bars_vals else None
    early_exit_target_return = (sum(early_exit_targets) / len(early_exit_targets)) if early_exit_targets else None
    early_exit_target_return = _normalize_early_exit_target(target_return, early_exit_target_return)
    return {
        "recommendation": recommendation,
        "size_factor": size_factor,
        "confidence": round(confidence, 4),
        "blocked": recommendation == "BLOCK",
        "joint_decision": {
            "should_enter": should_enter and recommendation != "BLOCK",
            "position_size_factor": round(size_factor, 4),
            "realistic_target_return": target_return,
            "hold_bars": hold_bars,
            "acceptable_early_exit_target_return": early_exit_target_return,
        },
    }


def _normalize_early_exit_target(target_return, early_exit_target_return):
    """
    Enforce policy: acceptable early-exit target must be above the base target.
    """
    try:
        t = float(target_return) if target_return is not None else None
    except Exception:
        t = None
    try:
        e = float(early_exit_target_return) if early_exit_target_return is not None else None
    except Exception:
        e = None

    if t is None:
        return e

    # Require at least +0.15% absolute OR +10% relative above target.
    floor = max(t + 0.0015, t * 1.10)
    if e is None or e <= t:
        return floor
    if e < floor:
        return floor
    return e


def _backfill_joint_decision_from_policy(verdict: dict, context: dict) -> dict:
    out = dict(verdict or {})
    jd = dict((out.get("joint_decision") or {}))
    target_snapshot = _parse_variant(context.get("target_expectation_snapshot"))
    bands = _parse_variant(target_snapshot.get("bands"))
    if jd.get("realistic_target_return") is None:
        base = bands.get("base")
        if base is not None:
            try:
                jd["realistic_target_return"] = float(base)
            except Exception:
                pass
    if jd.get("acceptable_early_exit_target_return") is None:
        conservative = bands.get("conservative")
        if conservative is not None:
            try:
                jd["acceptable_early_exit_target_return"] = float(conservative)
            except Exception:
                pass
    if jd.get("hold_bars") is None:
        jd["hold_bars"] = 5
    jd["acceptable_early_exit_target_return"] = _normalize_early_exit_target(
        jd.get("realistic_target_return"),
        jd.get("acceptable_early_exit_target_return"),
    )
    out["joint_decision"] = jd
    out["quality_backfilled"] = True
    return out


def _sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _run_multiagent_dialogue(
    cur,
    *,
    model: str,
    context: dict,
    persist_run_id: str | None = None,
    emit=None,
) -> tuple[list[dict], dict]:
    """Run two dialogue rounds across committee roles."""
    round1: list[dict] = []
    prior_messages: list[dict] = []
    for role in COMMITTEE_ROLES:
        try:
            role_out_raw = _call_cortex_json(cur, model, _committee_prompt(role, context, round_n=1, prior_messages=[]))
        except Exception as exc:
            role_out_raw = _committee_fallback(role, str(exc))
        role_out = _normalize_role_output(role, role_out_raw, context.get("symbol"))
        round1.append(role_out)
        prior_messages.append({"role": role, "summary": role_out.get("summary"), "stance": role_out.get("stance")})
        if emit:
            emit("agent_turn", {"round": 1, "role": role, "output": role_out})
        if persist_run_id:
            cur.execute(
                """
                insert into MIP.LIVE.COMMITTEE_ROLE_OUTPUT (
                  RUN_ID, ROLE_NAME, STANCE, CONFIDENCE, SUMMARY, OUTPUT_JSON, CREATED_AT
                )
                select %s, %s, %s, %s, %s, parse_json(%s), current_timestamp()
                """,
                (
                    persist_run_id,
                    f"R1_{role}",
                    role_out["stance"],
                    role_out["confidence"],
                    role_out["summary"],
                    json.dumps(role_out),
                ),
            )

    round2: list[dict] = []
    for role in COMMITTEE_ROLES:
        try:
            role_out_raw = _call_cortex_json(cur, model, _committee_prompt(role, context, round_n=2, prior_messages=prior_messages))
        except Exception as exc:
            role_out_raw = _committee_fallback(role, str(exc))
        role_out = _normalize_role_output(role, role_out_raw, context.get("symbol"))
        round2.append(role_out)
        if emit:
            emit("agent_turn", {"round": 2, "role": role, "output": role_out})
        if persist_run_id:
            cur.execute(
                """
                insert into MIP.LIVE.COMMITTEE_ROLE_OUTPUT (
                  RUN_ID, ROLE_NAME, STANCE, CONFIDENCE, SUMMARY, OUTPUT_JSON, CREATED_AT
                )
                select %s, %s, %s, %s, %s, parse_json(%s), current_timestamp()
                """,
                (
                    persist_run_id,
                    role,
                    role_out["stance"],
                    role_out["confidence"],
                    role_out["summary"],
                    json.dumps(role_out),
                ),
            )

    final_outputs = round2 if round2 else round1
    verdict = _aggregate_committee(final_outputs)
    if emit:
        emit("joint_decision", {"verdict": verdict})
    return final_outputs, verdict


def _build_action_decision_context(cur, action: dict) -> dict:
    symbol = action.get("SYMBOL")
    latest_bar = None
    if symbol:
        cur.execute(
            """
            select TS, CLOSE
            from MIP.MART.MARKET_BARS
            where SYMBOL = %s and INTERVAL_MINUTES = 1
            order by TS desc
            limit 1
            """,
            (symbol,),
        )
        latest_bar = cur.fetchone()
    pw_evidence = _fetch_committee_pw_evidence(cur, action.get("ACTION_ID"), action.get("PORTFOLIO_ID"))
    action_training_snapshot = _parse_variant(action.get("TRAINING_QUALIFICATION_SNAPSHOT"))
    if not action_training_snapshot:
        action_training_snapshot = _parse_variant(action.get("PARAM_SNAPSHOT")).get("training_qualification")
    action_target_snapshot = _parse_variant(action.get("TARGET_EXPECTATION_SNAPSHOT"))
    if not action_target_snapshot:
        action_target_snapshot = _parse_variant(action.get("PARAM_SNAPSHOT")).get("target_expectation")
    action_news_snapshot = _parse_variant(action.get("NEWS_CONTEXT_SNAPSHOT"))
    if not action_news_snapshot:
        action_news_snapshot = _parse_variant(action.get("PARAM_SNAPSHOT")).get("news_context")
    latest_news_snapshot = _fetch_latest_symbol_news_context(cur, symbol, action.get("ASSET_CLASS"))
    context = {
        "action_id": action.get("ACTION_ID"),
        "portfolio_id": action.get("PORTFOLIO_ID"),
        "symbol": symbol,
        "side": action.get("SIDE"),
        "proposed_qty": action.get("PROPOSED_QTY"),
        "proposed_price": action.get("PROPOSED_PRICE"),
        "status": action.get("STATUS"),
        "latest_one_minute_bar": {
            "ts": latest_bar[0].isoformat() if latest_bar and hasattr(latest_bar[0], "isoformat") else (latest_bar[0] if latest_bar else None),
            "close": float(latest_bar[1]) if latest_bar and latest_bar[1] is not None else None,
        },
        "training_qualification_snapshot": action_training_snapshot,
        "target_expectation_snapshot": action_target_snapshot,
        "news_context_snapshot": action_news_snapshot,
        "latest_symbol_news_context": latest_news_snapshot,
        "parallel_worlds_evidence": pw_evidence,
    }
    return context


@router.post("/snapshot/refresh")
def refresh_live_snapshot(
    portfolio_id: int | None = Query(None, description="Optional LIVE portfolio ID to stamp snapshot rows"),
    account: str | None = Query(None, description="IBKR account code (optional if only one managed account)"),
    host: str = Query("127.0.0.1", description="IB Gateway/TWS host"),
    port: int = Query(4002, description="IB paper port (4002 for Gateway paper, 7497 for TWS paper)"),
    client_id: int = Query(9402, description="IB client id"),
):
    """
    On-demand snapshot refresh.
    Triggers a single IBKR read-only pull and stores results in MIP.LIVE.BROKER_SNAPSHOTS.
    Intended for:
      - opening Live Portfolio page
      - opening Live Trade/Approval page
      - pre-trade and post-trade refresh
    """
    result = _run_on_demand_snapshot_sync(
        host=host,
        port=port,
        client_id=client_id,
        account=account,
        portfolio_id=portfolio_id,
    )
    return {
        "ok": True,
        "mode": "on_demand",
        "result": result,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/snapshot/latest")
def get_latest_live_snapshot(
    portfolio_id: int | None = Query(None, description="Optional portfolio filter"),
    account: str | None = Query(None, description="Optional IBKR account filter"),
):
    """
    Latest snapshot records from MIP.LIVE for display.
    Returns latest NAV, cash rows, positions, and open orders (trimmed).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        where = []
        params: list = []
        if portfolio_id is not None:
            where.append("PORTFOLIO_ID = %s")
            params.append(portfolio_id)
        if account:
            where.append("IBKR_ACCOUNT_ID = %s")
            params.append(account)
        where_sql = (" where " + " and ".join(where)) if where else ""

        nav_sql = f"""
        select SNAPSHOT_TS, IBKR_ACCOUNT_ID, PORTFOLIO_ID, CURRENCY,
               NET_LIQUIDATION_EUR, TOTAL_CASH_EUR, GROSS_POSITION_VALUE_EUR
        from MIP.LIVE.BROKER_SNAPSHOTS
        {where_sql}
          and SNAPSHOT_TYPE = 'NAV'
        order by SNAPSHOT_TS desc
        limit 1
        """ if where else """
        select SNAPSHOT_TS, IBKR_ACCOUNT_ID, PORTFOLIO_ID, CURRENCY,
               NET_LIQUIDATION_EUR, TOTAL_CASH_EUR, GROSS_POSITION_VALUE_EUR
        from MIP.LIVE.BROKER_SNAPSHOTS
        where SNAPSHOT_TYPE = 'NAV'
        order by SNAPSHOT_TS desc
        limit 1
        """
        if params:
            cur.execute(nav_sql, tuple(params))
        else:
            cur.execute(nav_sql)
        nav_row = cur.fetchone()
        nav_cols = [d[0] for d in cur.description] if cur.description else []
        nav = serialize_row(dict(zip(nav_cols, nav_row))) if nav_row else None

        # Determine timestamp/account from latest nav when possible
        latest_ts = nav.get("SNAPSHOT_TS") if nav else None
        latest_account = nav.get("IBKR_ACCOUNT_ID") if nav else account

        cash = []
        positions = []
        open_orders = []
        if latest_ts and latest_account:
            cur.execute(
                """
                select SNAPSHOT_TS, IBKR_ACCOUNT_ID, CURRENCY, CASH_BALANCE, SETTLED_CASH
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'CASH'
                  and SNAPSHOT_TS = %s
                  and IBKR_ACCOUNT_ID = %s
                order by CURRENCY
                """,
                (latest_ts, latest_account),
            )
            cash = fetch_all(cur)

            cur.execute(
                """
                select SNAPSHOT_TS, IBKR_ACCOUNT_ID, SYMBOL, SECURITY_TYPE, EXCHANGE, CURRENCY,
                       POSITION_QTY, AVG_COST
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'POSITION'
                  and SNAPSHOT_TS = %s
                  and IBKR_ACCOUNT_ID = %s
                order by SYMBOL
                limit 200
                """,
                (latest_ts, latest_account),
            )
            positions = fetch_all(cur)

            cur.execute(
                """
                select SNAPSHOT_TS, IBKR_ACCOUNT_ID, OPEN_ORDER_ID, OPEN_ORDER_STATUS,
                       SYMBOL, OPEN_ORDER_QTY, OPEN_ORDER_FILLED, OPEN_ORDER_REMAINING,
                       OPEN_ORDER_LIMIT_PRICE
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'OPEN_ORDER'
                  and SNAPSHOT_TS = %s
                  and IBKR_ACCOUNT_ID = %s
                order by OPEN_ORDER_ID
                limit 200
                """,
                (latest_ts, latest_account),
            )
            open_orders = fetch_all(cur)

        return {
            "ok": True,
            "mode": "on_demand",
            "latest_nav": nav,
            "cash": serialize_rows(cash),
            "positions": serialize_rows(positions),
            "open_orders": serialize_rows(open_orders),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        conn.close()


@router.get("/early-exit/status")
def get_early_exit_status(
    limit: int = Query(25, ge=1, le=200),
    include_raw: bool = Query(False, description="Include raw DETAILS payload from audit log"),
):
    """
    Hourly early-exit monitor status and recent run history.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select
              max(case when CONFIG_KEY = 'EARLY_EXIT_ENABLED' then CONFIG_VALUE end) as EARLY_EXIT_ENABLED,
              max(case when CONFIG_KEY = 'EARLY_EXIT_INTERVAL_MINUTES' then CONFIG_VALUE end) as EARLY_EXIT_INTERVAL_MINUTES
            from MIP.APP.APP_CONFIG
            where CONFIG_KEY in ('EARLY_EXIT_ENABLED', 'EARLY_EXIT_INTERVAL_MINUTES')
            """
        )
        cfg_rows = fetch_all(cur)
        cfg = cfg_rows[0] if cfg_rows else {}
        enabled = str(cfg.get("EARLY_EXIT_ENABLED") or "").strip().lower() in ("1", "true", "yes", "on")
        interval_minutes = int(cfg.get("EARLY_EXIT_INTERVAL_MINUTES") or 60)

        cur.execute(
            """
            select
              EVENT_TS,
              RUN_ID,
              STATUS,
              ROWS_AFFECTED,
              DETAILS
            from MIP.APP.MIP_AUDIT_LOG
            where EVENT_TYPE = 'EARLY_EXIT_PIPELINE'
              and EVENT_NAME = 'SP_RUN_HOURLY_EARLY_EXIT_MONITOR'
            order by EVENT_TS desc
            limit %s
            """,
            (limit,),
        )
        rows = fetch_all(cur)
        runs = []
        for r in rows:
            details = _parse_variant(r.get("DETAILS"))
            safe_details = _safe_early_exit_details(details)
            runs.append({
                "event_ts": r.get("EVENT_TS"),
                "run_id": r.get("RUN_ID"),
                "status": r.get("STATUS"),
                "rows_affected": r.get("ROWS_AFFECTED"),
                "interval_minutes": safe_details.get("interval_minutes"),
                "bars_ingested": safe_details.get("bars_ingested"),
                "positions_evaluated": safe_details.get("positions_evaluated"),
                "exit_signals": safe_details.get("exit_signals"),
                "exits_executed": safe_details.get("exits_executed"),
                "details": safe_details,
                "details_raw": details if include_raw else None,
            })

        return {
            "enabled": enabled,
            "interval_minutes": interval_minutes,
            "latest": serialize_row(runs[0]) if runs else None,
            "runs": serialize_rows(runs),
            "count": len(runs),
        }
    finally:
        conn.close()


@router.post("/early-exit/run")
def run_early_exit_monitor():
    """
    Trigger one on-demand hourly early-exit monitor run.
    """
    raw_results = _run_agent_snowflake_query("call MIP.APP.SP_RUN_HOURLY_EARLY_EXIT_MONITOR()", timeout_sec=300)
    raw = raw_results[0] if isinstance(raw_results, list) and raw_results else (raw_results if isinstance(raw_results, dict) else {})
    payload = raw
    if isinstance(raw, dict) and len(raw) == 1:
        payload = next(iter(raw.values()))
    result = _parse_variant(payload) if not isinstance(payload, dict) else payload
    status = str(result.get("status") or "UNKNOWN").upper()

    # Best-effort local ledger append through API connection.
    conn = get_connection()
    try:
        cur = conn.cursor()
        news_monitoring = _collect_news_monitoring_escalations(cur)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_EARLY_EXIT_MONITOR_RUN",
            status=status,
            action_before=None,
            action_after={
                "RUN_ID_VARCHAR": result.get("run_id"),
                "PARAM_SNAPSHOT": {
                    "interval_minutes": result.get("interval_minutes"),
                },
            },
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "positions_evaluated": result.get("positions_evaluated"),
                "exit_signals": result.get("exit_signals"),
                "exits_executed": result.get("exits_executed"),
                "news_monitoring_escalation": news_monitoring.get("positions_with_news_escalation"),
            },
            outcome_state={**result, "news_monitoring": news_monitoring},
        )
    finally:
        conn.close()

    return {"ok": True, "result": serialize_row(result), "status": status, "news_monitoring": news_monitoring}


@router.get("/drift/status")
def get_broker_drift_status(portfolio_id: int | None = Query(None, description="Live portfolio ID (optional)")):
    """
    Broker-truth drift status for a live portfolio.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        if portfolio_id is None:
            cur.execute(
                """
                select
                  PORTFOLIO_ID,
                  IBKR_ACCOUNT_ID,
                  DRIFT_STATUS,
                  SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
                  QUOTE_FRESHNESS_THRESHOLD_SEC,
                  UPDATED_AT
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where coalesce(IS_ACTIVE, true) = true
                order by PORTFOLIO_ID
                limit 1
                """
            )
        else:
            cur.execute(
                """
                select
                  PORTFOLIO_ID,
                  IBKR_ACCOUNT_ID,
                  DRIFT_STATUS,
                  SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
                  QUOTE_FRESHNESS_THRESHOLD_SEC,
                  UPDATED_AT
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where PORTFOLIO_ID = %s
                """,
                (portfolio_id,),
            )
        cfg_rows = fetch_all(cur)
        if not cfg_rows:
            return {
                "portfolio_id": portfolio_id,
                "ibkr_account_id": None,
                "drift_status": "NO_CONFIG",
                "snapshot_freshness_threshold_sec": None,
                "latest_snapshot": None,
                "snapshot_age_sec": None,
                "unresolved_drift_count": 0,
                "latest_unresolved_drift": None,
            }
        cfg = cfg_rows[0]
        resolved_portfolio_id = cfg.get("PORTFOLIO_ID")
        account_id = cfg.get("IBKR_ACCOUNT_ID")

        cur.execute(
            """
            select SNAPSHOT_TS, NET_LIQUIDATION_EUR, TOTAL_CASH_EUR
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'NAV'
              and IBKR_ACCOUNT_ID = %s
            order by SNAPSHOT_TS desc
            limit 1
            """,
            (account_id,),
        )
        nav_rows = fetch_all(cur)
        nav = nav_rows[0] if nav_rows else {}
        snapshot_ts = nav.get("SNAPSHOT_TS")
        snapshot_age_sec = None
        if snapshot_ts and hasattr(snapshot_ts, "replace"):
            snapshot_age_sec = int((datetime.now(timezone.utc) - snapshot_ts.replace(tzinfo=timezone.utc)).total_seconds())

        drift_rows = []
        drift_table_available = True
        try:
            cur.execute(
                """
                select
                  DRIFT_ID, RECONCILIATION_TS, NAV_DRIFT_PCT, CASH_DRIFT_EUR, POSITION_DRIFT_COUNT, DRIFT_DETECTED,
                  RESOLUTION_TS, RESOLUTION_METHOD, DETAILS
                from MIP.LIVE.DRIFT_LOG
                where PORTFOLIO_ID = %s
                  and coalesce(DRIFT_DETECTED, false) = true
                order by RECONCILIATION_TS desc
                limit 20
                """,
                (resolved_portfolio_id,),
            )
            drift_rows = fetch_all(cur)
        except Exception:
            drift_table_available = False
        unresolved = [r for r in drift_rows if not r.get("RESOLUTION_TS")]
        latest_unresolved = unresolved[0] if unresolved else None

        return {
            "portfolio_id": resolved_portfolio_id,
            "ibkr_account_id": account_id,
            "drift_status": cfg.get("DRIFT_STATUS"),
            "snapshot_freshness_threshold_sec": cfg.get("SNAPSHOT_FRESHNESS_THRESHOLD_SEC"),
            "latest_snapshot": serialize_row(nav) if nav else None,
            "snapshot_age_sec": snapshot_age_sec,
            "drift_table_available": drift_table_available,
            "unresolved_drift_count": len(unresolved),
            "latest_unresolved_drift": serialize_row(latest_unresolved) if latest_unresolved else None,
        }
    finally:
        conn.close()


@router.get("/activation/guard")
def get_live_activation_guard(portfolio_id: int = Query(..., description="Live portfolio ID")):
    conn = get_connection()
    try:
        cur = conn.cursor()
        guard = _compute_live_activation_guard(cur, portfolio_id)
        return {"portfolio_id": portfolio_id, **guard}
    finally:
        conn.close()


@router.post("/activation/enable")
def enable_live_activation(req: SetLiveActivationRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        guard = _compute_live_activation_guard(cur, req.portfolio_id)
        if (not guard.get("eligible")) and (not req.force):
            raise HTTPException(
                status_code=409,
                detail={"message": "Live activation blocked by safety guards.", "reasons": guard.get("reasons")},
            )

        cur.execute(
            """
            update MIP.LIVE.LIVE_PORTFOLIO_CONFIG
               set ADAPTER_MODE = 'LIVE',
                   IS_ACTIVE = true,
                   UPDATED_AT = current_timestamp()
             where PORTFOLIO_ID = %s
            """,
            (req.portfolio_id,),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Live portfolio config not found.")

        try:
            cur.execute(
                """
                insert into MIP.LIVE.BROKER_EVENT_LEDGER (
                  EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, PAYLOAD
                )
                values (
                  %s, current_timestamp(), 'LIVE_ACTIVATION_ENABLED', %s, parse_json(%s)
                )
                """,
                (
                    str(uuid.uuid4()),
                    req.portfolio_id,
                    json.dumps({"actor": req.actor, "force": req.force, "reasons": guard.get("reasons", [])}),
                ),
            )
        except Exception:
            # Non-fatal telemetry write; activation update must still complete.
            pass

        _append_learning_ledger_event(
            cur,
            event_name="LIVE_ACTIVATION_ENABLE",
            status="SUCCESS" if guard.get("eligible") else "FORCED",
            action_before=None,
            action_after={"PORTFOLIO_ID": req.portfolio_id},
            policy_version=LIVE_ACTIVATION_POLICY_VERSION,
            influence_delta={
                "eligible": guard.get("eligible"),
                "forced": req.force,
            },
            outcome_state={"actor": req.actor, "reasons": guard.get("reasons", [])},
        )

        cur.execute(
            """
            select ADAPTER_MODE
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            """,
            (req.portfolio_id,),
        )
        mode_rows = fetch_all(cur)
        mode_after = (mode_rows[0] or {}).get("ADAPTER_MODE") if mode_rows else None
        if (mode_after or "").upper() != "LIVE":
            raise HTTPException(
                status_code=409,
                detail={"message": "Activation update did not persist LIVE mode.", "mode_after": mode_after},
            )
        return {
            "ok": True,
            "portfolio_id": req.portfolio_id,
            "adapter_mode": mode_after,
            "forced": req.force,
            "guard": guard,
        }
    finally:
        conn.close()


@router.post("/activation/disable")
def disable_live_activation(req: DisableLiveActivationRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            update MIP.LIVE.LIVE_PORTFOLIO_CONFIG
               set ADAPTER_MODE = 'PAPER',
                   UPDATED_AT = current_timestamp()
             where PORTFOLIO_ID = %s
            """,
            (req.portfolio_id,),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Live portfolio config not found.")

        try:
            cur.execute(
                """
                insert into MIP.LIVE.BROKER_EVENT_LEDGER (
                  EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, PAYLOAD
                )
                values (
                  %s, current_timestamp(), 'LIVE_ACTIVATION_DISABLED', %s, parse_json(%s)
                )
                """,
                (
                    str(uuid.uuid4()),
                    req.portfolio_id,
                    json.dumps({"actor": req.actor, "reason": req.reason}),
                ),
            )
        except Exception:
            # Non-fatal telemetry write; disable update must still complete.
            pass

        _append_learning_ledger_event(
            cur,
            event_name="LIVE_ACTIVATION_DISABLE",
            status="SUCCESS",
            action_before=None,
            action_after={"PORTFOLIO_ID": req.portfolio_id},
            policy_version=LIVE_ACTIVATION_POLICY_VERSION,
            influence_delta={"adapter_mode_after": "PAPER"},
            outcome_state={"actor": req.actor, "reason": req.reason},
        )

        cur.execute(
            """
            select ADAPTER_MODE
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            """,
            (req.portfolio_id,),
        )
        mode_rows = fetch_all(cur)
        mode_after = (mode_rows[0] or {}).get("ADAPTER_MODE") if mode_rows else None
        if (mode_after or "").upper() != "PAPER":
            raise HTTPException(
                status_code=409,
                detail={"message": "Disable update did not persist PAPER mode.", "mode_after": mode_after},
            )
        return {
            "ok": True,
            "portfolio_id": req.portfolio_id,
            "adapter_mode": mode_after,
            "reason": req.reason,
        }
    finally:
        conn.close()


@router.post("/smoke/phase-gate")
def run_phase_gate_smoke(req: SmokeGateRequest):
    """
    Phase-gated smoke/non-regression checks for live path continuity.
    """
    checks: list[dict] = []

    def _record(name: str, ok: bool, detail=None):
        checks.append({"name": name, "ok": ok, "detail": detail})

    # Read-only HTTP checks against current API runtime.
    try:
        conn = get_connection()
        _record("api_connection", True)
    except Exception as exc:
        _record("api_connection", False, str(exc))
        return {"ok": False, "phase": req.phase, "checks": checks}
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # Endpoint-level checks
    for name, fn in [
        ("early_exit_status", lambda: get_early_exit_status(limit=1, include_raw=False)),
        ("drift_status", lambda: get_broker_drift_status(portfolio_id=None)),
        ("orders_list", lambda: list_live_orders(portfolio_id=None, action_id=None, limit=5)),
        ("actions_list", lambda: list_live_trade_actions(portfolio_id=None, pending_only=True, limit=5)),
    ]:
        try:
            payload = fn()
            _record(name, True, {"keys": list(payload.keys()) if isinstance(payload, dict) else None})
        except Exception as exc:
            _record(name, False, str(exc))

    # Optional DB checks (read-only)
    if req.include_db_checks:
        db_queries = {
            "db_live_actions_exists": "select count(*) as CNT from MIP.LIVE.LIVE_ACTIONS",
            "db_live_orders_exists": "select count(*) as CNT from MIP.LIVE.LIVE_ORDERS",
            "db_ledger_exists": "select count(*) as CNT from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER",
            "db_audit_recent": "select count(*) as CNT from MIP.APP.MIP_AUDIT_LOG where EVENT_TS >= dateadd(day,-7,current_timestamp())",
        }
        for name, q in db_queries.items():
            try:
                rows = _run_agent_snowflake_query(q, timeout_sec=60)
                cnt = None
                if isinstance(rows, list) and rows:
                    cnt = rows[0].get("CNT")
                _record(name, True, {"count": cnt})
            except Exception as exc:
                _record(name, False, str(exc))

    # Optional write checks kept guarded by explicit flag.
    if req.include_write_checks:
        try:
            result = rebuild_live_action_state(RebuildLiveStateRequest(portfolio_id=None, dry_run=True, actor="phase_gate_smoke"))
            _record("write_path_rebuild_dry_run", True, {"changed_actions": result.get("changed_actions")})
        except Exception as exc:
            _record("write_path_rebuild_dry_run", False, str(exc))

    ok = all(c["ok"] for c in checks)
    return {
        "ok": ok,
        "phase": req.phase,
        "include_db_checks": req.include_db_checks,
        "include_write_checks": req.include_write_checks,
        "checks": checks,
        "failed": [c for c in checks if not c["ok"]],
    }


@router.get("/trades/actions")
def list_live_trade_actions(
    portfolio_id: int | None = Query(None),
    pending_only: bool = Query(True),
    limit: int = Query(200, ge=1, le=1000),
):
    conn = get_connection()
    try:
        cur = conn.cursor()
        wheres = ["1=1"]
        params = []
        if portfolio_id is not None:
            wheres.append("PORTFOLIO_ID = %s")
            params.append(portfolio_id)
        if pending_only:
            wheres.append(
                "STATUS in ('RESEARCH_IMPORTED','PROPOSED','PENDING_OPEN_VALIDATION','OPEN_ELIGIBLE','OPEN_CAUTION','PENDING_OPEN_STABILITY_REVIEW','READY_FOR_APPROVAL_FLOW','PM_ACCEPTED','COMPLIANCE_APPROVED','INTENT_SUBMITTED','INTENT_APPROVED','REVALIDATED_PASS','REVALIDATED_FAIL','EXECUTION_REQUESTED')"
            )
        wheres_aliased = [w.replace("PORTFOLIO_ID", "la.PORTFOLIO_ID").replace("STATUS in", "la.STATUS in") for w in wheres]
        params.append(limit)
        sql = f"""
        with proposer_summary as (
          select RUN_ID, SUMMARY
          from MIP.LIVE.COMMITTEE_ROLE_OUTPUT
          where ROLE_NAME = 'PROPOSER'
          qualify row_number() over (partition by RUN_ID order by CREATED_AT desc) = 1
        )
        select
          la.ACTION_ID, la.PROPOSAL_ID, la.PORTFOLIO_ID, la.SYMBOL, la.SIDE, la.PROPOSED_QTY, la.PROPOSED_PRICE, la.ASSET_CLASS,
          la.STATUS, la.VALIDITY_WINDOW_END,
          la.COMMITTEE_REQUIRED, la.COMMITTEE_STATUS, la.COMMITTEE_RUN_ID, la.COMMITTEE_COMPLETED_TS, la.COMMITTEE_VERDICT,
          cv.VERDICT_JSON:verdict:joint_decision as COMMITTEE_JOINT_DECISION,
          ps.SUMMARY as COMMITTEE_SUMMARY,
          la.TRAINING_QUALIFICATION_SNAPSHOT, la.TRAINING_LIVE_ELIGIBLE, la.TRAINING_RANK_IMPACT, la.TRAINING_SIZE_CAP_FACTOR,
          la.TARGET_EXPECTATION_SNAPSHOT, la.TARGET_OPEN_CONDITION_FACTOR, la.TARGET_EXPECTATION_POLICY_VERSION,
          la.NEWS_CONTEXT_SNAPSHOT, la.NEWS_CONTEXT_STATE, la.NEWS_EVENT_SHOCK_FLAG, la.NEWS_FRESHNESS_BUCKET, la.NEWS_CONTEXT_POLICY_VERSION,
          la.PM_APPROVED_BY, la.PM_APPROVED_TS,
          la.COMPLIANCE_STATUS, la.COMPLIANCE_APPROVED_BY, la.COMPLIANCE_DECISION_TS, la.COMPLIANCE_NOTES, la.COMPLIANCE_REFERENCE_ID,
          la.INTENT_SUBMITTED_BY, la.INTENT_SUBMITTED_TS, la.INTENT_APPROVED_BY, la.INTENT_APPROVED_TS, la.INTENT_REFERENCE_ID,
          la.REVALIDATION_TS, la.REVALIDATION_PRICE, la.PRICE_DEVIATION_PCT, la.PRICE_GUARD_RESULT,
          la.REVALIDATION_OUTCOME, la.REVALIDATION_POLICY_VERSION, la.REVALIDATION_DATA_SOURCE,
          la.REASON_CODES,
          la.ONE_MIN_BAR_TS, la.ONE_MIN_BAR_CLOSE, la.EXECUTION_PRICE_SOURCE,
          la.CREATED_AT, la.UPDATED_AT
        from MIP.LIVE.LIVE_ACTIONS la
        left join MIP.LIVE.COMMITTEE_VERDICT cv
          on cv.RUN_ID = la.COMMITTEE_RUN_ID
        left join proposer_summary ps
          on ps.RUN_ID = la.COMMITTEE_RUN_ID
        where {' and '.join(wheres_aliased)}
        order by coalesce(la.COMPLIANCE_DECISION_TS, la.PM_APPROVED_TS, la.CREATED_AT) desc
        limit %s
        """
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"actions": serialize_rows(rows), "count": len(rows)}
    finally:
        conn.close()


@router.get("/activity/overview")
def get_live_activity_overview(limit: int = Query(200, ge=50, le=1000)):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select
              PORTFOLIO_ID, IBKR_ACCOUNT_ID, DRIFT_STATUS, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              MAX_POSITIONS, MAX_POSITION_PCT, BUST_PCT, IS_ACTIVE, UPDATED_AT
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where coalesce(IS_ACTIVE, true) = true
            order by PORTFOLIO_ID
            limit 1
            """
        )
        cfg_rows = fetch_all(cur)
        if not cfg_rows:
            return {
                "ok": True,
                "portfolio": None,
                "readiness": {"snapshot_state": "BLOCKED", "drift_state": "BLOCKED", "actionable": False},
                "account_kpis": {},
                "open_positions": [],
                "open_orders": [],
                "orders": [],
                "executions": [],
                "pending_decisions": [],
                "counts": {},
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        cfg = cfg_rows[0]
        portfolio_id = cfg.get("PORTFOLIO_ID")
        account_id = cfg.get("IBKR_ACCOUNT_ID")

        cur.execute(
            """
            select SNAPSHOT_TS, NET_LIQUIDATION_EUR, TOTAL_CASH_EUR, GROSS_POSITION_VALUE_EUR
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'NAV'
              and IBKR_ACCOUNT_ID = %s
            order by SNAPSHOT_TS desc
            limit 1
            """,
            (account_id,),
        )
        nav_rows = fetch_all(cur)
        nav = nav_rows[0] if nav_rows else {}
        latest_snapshot_ts = nav.get("SNAPSHOT_TS")
        snapshot_age_sec = None
        if latest_snapshot_ts and hasattr(latest_snapshot_ts, "replace"):
            snapshot_age_sec = int((datetime.now(timezone.utc) - latest_snapshot_ts.replace(tzinfo=timezone.utc)).total_seconds())

        open_positions = []
        open_orders = []
        if latest_snapshot_ts:
            cur.execute(
                """
                select
                  SYMBOL, SECURITY_TYPE, EXCHANGE, CURRENCY, POSITION_QTY, AVG_COST,
                  MARKET_VALUE, UNREALIZED_PNL, REALIZED_PNL
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'POSITION'
                  and IBKR_ACCOUNT_ID = %s
                  and SNAPSHOT_TS = %s
                  and coalesce(POSITION_QTY, 0) <> 0
                order by abs(POSITION_QTY) desc, SYMBOL
                limit %s
                """,
                (account_id, latest_snapshot_ts, limit),
            )
            open_positions = fetch_all(cur)

            cur.execute(
                """
                select
                  OPEN_ORDER_ID, OPEN_ORDER_STATUS, SYMBOL, OPEN_ORDER_QTY, OPEN_ORDER_FILLED,
                  OPEN_ORDER_REMAINING, OPEN_ORDER_LIMIT_PRICE
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'OPEN_ORDER'
                  and IBKR_ACCOUNT_ID = %s
                  and SNAPSHOT_TS = %s
                order by OPEN_ORDER_ID
                limit %s
                """,
                (account_id, latest_snapshot_ts, limit),
            )
            open_orders = fetch_all(cur)

        cur.execute(
            """
            select
              ORDER_ID, ACTION_ID, BROKER_ORDER_ID, STATUS, SYMBOL, SIDE, ORDER_TYPE,
              QTY_ORDERED, LIMIT_PRICE, QTY_FILLED, AVG_FILL_PRICE,
              SUBMITTED_AT, ACKNOWLEDGED_AT, FILLED_AT, LAST_UPDATED_AT, CREATED_AT
            from MIP.LIVE.LIVE_ORDERS
            where PORTFOLIO_ID = %s
            order by LAST_UPDATED_AT desc, CREATED_AT desc
            limit %s
            """,
            (portfolio_id, limit),
        )
        orders = fetch_all(cur)
        order_groups: dict[str, list[dict]] = {}
        for order in orders:
            key = str(order.get("ACTION_ID") or "")
            if not key:
                continue
            order_groups.setdefault(key, []).append(order)

        protection_by_action: dict[str, dict] = {}
        for action_id, action_orders in order_groups.items():
            parent_leg = None
            take_profit_leg = None
            stop_loss_leg = None
            for ord_row in action_orders:
                order_type = str(ord_row.get("ORDER_TYPE") or "").upper()
                status = str(ord_row.get("STATUS") or "").upper()
                leg = {
                    "order_id": ord_row.get("ORDER_ID"),
                    "broker_order_id": ord_row.get("BROKER_ORDER_ID"),
                    "status": status,
                    "order_type": order_type,
                }
                if any(token in order_type for token in ("STOP", "STP", "SL")):
                    if stop_loss_leg is None:
                        stop_loss_leg = leg
                elif any(token in order_type for token in ("TP", "TAKE_PROFIT", "LIMIT_TP")):
                    if take_profit_leg is None:
                        take_profit_leg = leg
                else:
                    if parent_leg is None:
                        parent_leg = leg

            if take_profit_leg and stop_loss_leg:
                protection_state = "FULL"
            elif take_profit_leg or stop_loss_leg:
                protection_state = "PARTIAL"
            else:
                protection_state = "NONE"
            protection_by_action[action_id] = {
                "state": protection_state,
                "parent": parent_leg,
                "take_profit": take_profit_leg,
                "stop_loss": stop_loss_leg,
            }

        cur.execute(
            """
            select
              la.ACTION_ID, la.PROPOSAL_ID, la.SYMBOL, la.SIDE, la.STATUS, la.COMPLIANCE_STATUS,
              la.COMMITTEE_VERDICT, la.COMMITTEE_STATUS, la.COMMITTEE_REQUIRED, la.REASON_CODES,
              la.COMMITTEE_RUN_ID, la.COMMITTEE_COMPLETED_TS,
              cv.SIZE_FACTOR as COMMITTEE_SIZE_FACTOR,
              cv.VERDICT_JSON:verdict:joint_decision as COMMITTEE_JOINT_DECISION,
              la.PROPOSED_QTY, la.PROPOSED_PRICE, la.TARGET_OPEN_CONDITION_FACTOR, la.TRAINING_SIZE_CAP_FACTOR,
              la.TARGET_EXPECTATION_SNAPSHOT, la.CREATED_AT, la.UPDATED_AT
            from MIP.LIVE.LIVE_ACTIONS la
            left join MIP.LIVE.COMMITTEE_VERDICT cv
              on cv.RUN_ID = la.COMMITTEE_RUN_ID
            where la.PORTFOLIO_ID = %s
              and la.STATUS in (
                'RESEARCH_IMPORTED','PROPOSED','PENDING_OPEN_VALIDATION','OPEN_ELIGIBLE','OPEN_CAUTION',
                'PENDING_OPEN_STABILITY_REVIEW','READY_FOR_APPROVAL_FLOW','PM_ACCEPTED','COMPLIANCE_APPROVED',
                'INTENT_SUBMITTED','INTENT_APPROVED','REVALIDATED_PASS','REVALIDATED_FAIL','EXECUTION_REQUESTED'
              )
            order by la.CREATED_AT desc
            limit %s
            """,
            (portfolio_id, limit),
        )
        action_rows = fetch_all(cur)

        unresolved_drift_count = 0
        try:
            cur.execute(
                """
                select count(*) as CNT
                from MIP.LIVE.DRIFT_LOG
                where PORTFOLIO_ID = %s
                  and coalesce(DRIFT_DETECTED, false) = true
                  and RESOLUTION_TS is null
                """,
                (portfolio_id,),
            )
            unresolved_rows = fetch_all(cur)
            unresolved_drift_count = int((unresolved_rows[0] or {}).get("CNT") or 0)
        except Exception:
            unresolved_drift_count = 0

        now_utc = datetime.now(timezone.utc)
        market_open = _is_market_open_ny(now_utc)
        open_utc, close_utc = _market_session_bounds_utc(now_utc)
        snapshot_state = _compute_snapshot_freshness_state(snapshot_age_sec, cfg.get("SNAPSHOT_FRESHNESS_THRESHOLD_SEC"))
        drift_state = _compute_drift_state(cfg.get("DRIFT_STATUS"), unresolved_drift_count)
        page_actionable = snapshot_state in ("FRESH", "AGING") and drift_state != "BLOCKED" and market_open
        held_symbols = {str((r.get("SYMBOL") or "")).upper() for r in open_positions}
        nav_eur = float(nav.get("NET_LIQUIDATION_EUR") or 0.0) if nav else 0.0

        pending_decisions = []
        for row in action_rows:
            symbol = str(row.get("SYMBOL") or "").upper()
            joint_decision = _parse_variant(row.get("COMMITTEE_JOINT_DECISION"))
            proposed_qty = float(row.get("PROPOSED_QTY")) if row.get("PROPOSED_QTY") is not None else None
            proposed_price = float(row.get("PROPOSED_PRICE")) if row.get("PROPOSED_PRICE") is not None else None
            estimated_notional = (abs(proposed_qty) * abs(proposed_price)) if (proposed_qty is not None and proposed_price is not None) else None
            position_pct = (estimated_notional / nav_eur) if (estimated_notional is not None and nav_eur > 0) else None
            committee_size_factor = float(row.get("COMMITTEE_SIZE_FACTOR")) if row.get("COMMITTEE_SIZE_FACTOR") is not None else None
            size_cap_factor = float(row.get("TRAINING_SIZE_CAP_FACTOR")) if row.get("TRAINING_SIZE_CAP_FACTOR") is not None else None
            target_open_factor = (
                float(row.get("TARGET_OPEN_CONDITION_FACTOR"))
                if row.get("TARGET_OPEN_CONDITION_FACTOR") is not None
                else 1.0
            )
            final_qty_preview = proposed_qty
            if final_qty_preview is not None and committee_size_factor is not None:
                final_qty_preview = max(final_qty_preview * committee_size_factor, 1.0)
            if final_qty_preview is not None and size_cap_factor is not None:
                final_qty_preview = max(final_qty_preview * size_cap_factor, 1.0)
            if final_qty_preview is not None and target_open_factor is not None:
                final_qty_preview = max(final_qty_preview * target_open_factor, 1.0)
            sizing_reason = None
            if proposed_qty is None and proposed_price is None:
                sizing_reason = "Quantity/price not finalized yet (revalidation will price from latest bar)."
            elif proposed_qty is None:
                sizing_reason = "Quantity not finalized yet."
            elif proposed_price is None:
                sizing_reason = "Price not finalized yet (revalidation pending)."

            status = (row.get("STATUS") or "").upper()
            action_reason_codes = _parse_list_variant(row.get("REASON_CODES"))
            blocked = status == "OPEN_BLOCKED" or bool(action_reason_codes and any("BLOCK" in str(x).upper() for x in action_reason_codes))
            submit_allowed = status in ("INTENT_APPROVED", "REVALIDATED_FAIL", "REVALIDATED_PASS", "COMPLIANCE_APPROVED", "INTENT_SUBMITTED", "PM_ACCEPTED", "READY_FOR_APPROVAL_FLOW")
            submit_allowed = submit_allowed and page_actionable and (not blocked)
            in_position = symbol in held_symbols
            action_id = str(row.get("ACTION_ID") or "")
            protection_details = protection_by_action.get(action_id) or {"state": "NONE", "parent": None, "take_profit": None, "stop_loss": None}
            protection_planned = bool(
                joint_decision.get("realistic_target_return") is not None
                or joint_decision.get("acceptable_early_exit_target_return") is not None
            )

            if status != "EXECUTION_REQUESTED" and not in_position:
                pending_decisions.append(
                    {
                        "action_id": row.get("ACTION_ID"),
                        "proposal_id": row.get("PROPOSAL_ID"),
                        "symbol": row.get("SYMBOL"),
                        "side": row.get("SIDE"),
                        "status": row.get("STATUS"),
                        "compliance_status": row.get("COMPLIANCE_STATUS"),
                        "committee_verdict": row.get("COMMITTEE_VERDICT"),
                        "committee_status": row.get("COMMITTEE_STATUS"),
                        "committee_run_id": row.get("COMMITTEE_RUN_ID"),
                        "committee_completed_ts": row.get("COMMITTEE_COMPLETED_TS"),
                        "committee_required": bool(row.get("COMMITTEE_REQUIRED")) if row.get("COMMITTEE_REQUIRED") is not None else True,
                        "reason_codes": action_reason_codes,
                        "required_next_step": _required_next_step_for_status(status),
                        "submission_allowed": bool(submit_allowed),
                        "is_blocked": bool(blocked),
                        "held_in_broker_position": bool(in_position),
                        "sizing": {
                            "proposed_qty": proposed_qty,
                            "proposed_price": proposed_price,
                            "account_basis_nav_eur": nav_eur if nav_eur > 0 else None,
                            "estimated_notional_eur": estimated_notional,
                            "estimated_position_pct": position_pct,
                            "committee_size_factor": committee_size_factor,
                            "training_size_cap_factor": size_cap_factor,
                            "target_open_condition_factor": target_open_factor,
                            "final_qty_preview": final_qty_preview,
                            "availability_reason": sizing_reason,
                            "max_position_pct_limit": float(cfg.get("MAX_POSITION_PCT")) if cfg.get("MAX_POSITION_PCT") is not None else None,
                        },
                        "protection": {"planned": protection_planned, **protection_details},
                        "timestamps": {
                            "created_at": row.get("CREATED_AT"),
                            "updated_at": row.get("UPDATED_AT"),
                        },
                    }
                )

        executions = []
        for order in orders:
            status = (order.get("STATUS") or "").upper()
            qty_filled = order.get("QTY_FILLED")
            is_exec = status in ("PARTIAL_FILL", "FILLED") or (qty_filled is not None and float(qty_filled) > 0)
            if not is_exec:
                continue
            executions.append(
                {
                    "order_id": order.get("ORDER_ID"),
                    "action_id": order.get("ACTION_ID"),
                    "broker_order_id": order.get("BROKER_ORDER_ID"),
                    "symbol": order.get("SYMBOL"),
                    "side": order.get("SIDE"),
                    "qty_filled": float(qty_filled) if qty_filled is not None else None,
                    "avg_fill_price": float(order.get("AVG_FILL_PRICE")) if order.get("AVG_FILL_PRICE") is not None else None,
                    "status": order.get("STATUS"),
                    "execution_ts": order.get("FILLED_AT") or order.get("LAST_UPDATED_AT"),
                    "source": "MIP_BROKER_LEDGER",
                }
            )

        orders_enriched = []
        for ord_row in orders:
            action_key = str(ord_row.get("ACTION_ID") or "")
            orders_enriched.append(
                {
                    **ord_row,
                    "PROTECTION": protection_by_action.get(action_key)
                    or {"state": "NONE", "parent": None, "take_profit": None, "stop_loss": None},
                }
            )

        return {
            "ok": True,
            "portfolio": {
                "portfolio_id": portfolio_id,
                "ibkr_account_id": account_id,
                "is_active": bool(cfg.get("IS_ACTIVE")) if cfg.get("IS_ACTIVE") is not None else True,
                "config_updated_at": cfg.get("UPDATED_AT"),
            },
            "readiness": {
                "snapshot_state": snapshot_state,
                "drift_state": drift_state,
                "actionable": page_actionable,
                "blocking_reasons": [
                    reason
                    for reason, active in [
                        ("SNAPSHOT_STALE_OR_MISSING", snapshot_state in ("STALE", "BLOCKED")),
                        ("DRIFT_UNRESOLVED", drift_state == "BLOCKED"),
                        ("OUTSIDE_OPERATING_HOURS", not market_open),
                    ]
                    if active
                ],
                "market_open": market_open,
                "market_window_open_utc": open_utc,
                "market_window_close_utc": close_utc,
            },
            "account_kpis": {
                "equity_nav_eur": float(nav.get("NET_LIQUIDATION_EUR")) if nav.get("NET_LIQUIDATION_EUR") is not None else None,
                "cash_eur": float(nav.get("TOTAL_CASH_EUR")) if nav.get("TOTAL_CASH_EUR") is not None else None,
                "gross_exposure_eur": float(nav.get("GROSS_POSITION_VALUE_EUR")) if nav.get("GROSS_POSITION_VALUE_EUR") is not None else None,
                "open_positions_count": len(open_positions),
                "open_orders_count": len(open_orders),
                "snapshot_ts": latest_snapshot_ts,
                "snapshot_age_sec": snapshot_age_sec,
                "snapshot_freshness_threshold_sec": cfg.get("SNAPSHOT_FRESHNESS_THRESHOLD_SEC"),
                "drift_status": cfg.get("DRIFT_STATUS"),
                "unresolved_drift_count": unresolved_drift_count,
                "max_positions": cfg.get("MAX_POSITIONS"),
                "max_position_pct": cfg.get("MAX_POSITION_PCT"),
                "bust_pct": cfg.get("BUST_PCT"),
            },
            "open_positions": serialize_rows(open_positions),
            "open_orders": serialize_rows(open_orders),
            "orders": serialize_rows(orders_enriched),
            "executions": serialize_rows(executions),
            "pending_decisions": serialize_rows(pending_decisions),
            "counts": {
                "pending_decisions": len(pending_decisions),
                "orders": len(orders),
                "executions": len(executions),
                "open_positions": len(open_positions),
                "open_orders": len(open_orders),
            },
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        conn.close()


@router.post("/decisions/{action_id}/approve-and-submit")
def approve_and_submit_live_decision(action_id: str, req: ApproveAndSubmitLiveDecisionRequest):
    steps: list[str] = []
    action = _fetch_live_action_state(action_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found.")

    def refresh_state() -> dict:
        latest = _fetch_live_action_state(action_id)
        if not latest:
            raise HTTPException(status_code=404, detail="Action disappeared during workflow.")
        return latest

    action = refresh_state()
    status = (action.get("STATUS") or "").upper()

    if status in ("EXECUTION_REQUESTED", "EXECUTED"):
        return {
            "ok": True,
            "action_id": action_id,
            "status": status,
            "idempotent_replay": True,
            "steps": steps,
        }

    if status in ("RESEARCH_IMPORTED", "PROPOSED", "PENDING_OPEN_VALIDATION"):
        validation_resp = run_opening_validation(
            action_id,
            OpeningValidationRequest(force_refresh_1m=req.force_refresh_1m),
        )
        steps.append("opening_validate")
        action = refresh_state()
        status = (action.get("STATUS") or "").upper()
        if status == "OPEN_BLOCKED":
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Opening validation blocked submit.",
                    "reason_codes": validation_resp.get("reason_codes") or _parse_list_variant(action.get("REASON_CODES")),
                },
            )

    if status in ("OPEN_ELIGIBLE", "OPEN_CAUTION", "PENDING_OPEN_STABILITY_REVIEW", "READY_FOR_APPROVAL_FLOW"):
        run_live_trade_committee(
            action_id,
            CommitteeRunRequest(
                actor=req.committee_actor,
                model=req.committee_model,
                force_rerun=req.committee_recheck_before_submit,
            ),
        )
        steps.append("committee_run")
        action = refresh_state()
        status = (action.get("STATUS") or "").upper()
        if status == "OPEN_BLOCKED":
            raise HTTPException(
                status_code=409,
                detail={"message": "Decision blocked in committee/opening stage.", "reason_codes": _parse_list_variant(action.get("REASON_CODES"))},
            )

    if status == "READY_FOR_APPROVAL_FLOW":
        pm_accept_live_action(action_id, PmAcceptRequest(actor=req.pm_actor))
        steps.append("pm_accept")
        action = refresh_state()
        status = (action.get("STATUS") or "").upper()

    if status == "PM_ACCEPTED":
        compliance_decide_live_action(
            action_id,
            ComplianceDecisionRequest(actor=req.compliance_actor, decision="APPROVE"),
        )
        steps.append("compliance_approve")
        action = refresh_state()
        status = (action.get("STATUS") or "").upper()

    if status == "COMPLIANCE_APPROVED":
        submit_live_trade_intent(
            action_id,
            IntentSubmitRequest(
                actor=req.intent_submit_actor,
                reference_id=f"LIVE_ACTIVITY_{int(datetime.now(timezone.utc).timestamp())}",
            ),
        )
        steps.append("intent_submit")
        action = refresh_state()
        status = (action.get("STATUS") or "").upper()

    if status == "INTENT_SUBMITTED":
        approve_live_trade_intent(action_id, IntentApproveRequest(actor=req.intent_approve_actor))
        steps.append("intent_approve")
        action = refresh_state()
        status = (action.get("STATUS") or "").upper()

    if status in ("INTENT_APPROVED", "REVALIDATED_FAIL", "REVALIDATED_PASS"):
        revalidate_live_action(
            action_id,
            RevalidateLiveActionRequest(force_refresh_1m=req.force_refresh_1m),
        )
        steps.append("revalidate")
        action = refresh_state()
        status = (action.get("STATUS") or "").upper()

    if status == "REVALIDATED_PASS":
        execute_resp = execute_live_action(
            action_id,
            ExecuteLiveActionRequest(actor=req.execution_actor, attempt_n=req.attempt_n),
        )
        steps.append("execute")
        action = refresh_state()
        return {
            "ok": True,
            "action_id": action_id,
            "status": action.get("STATUS"),
            "steps": steps,
            "execute_result": execute_resp,
            "reason_codes": _parse_list_variant(action.get("REASON_CODES")),
        }

    raise HTTPException(
        status_code=409,
        detail={
            "message": "Decision did not reach executable state.",
            "status": status,
            "reason_codes": _parse_list_variant(action.get("REASON_CODES")),
            "steps": steps,
        },
    )


@router.get("/trades/actions/{action_id}/committee")
def get_live_trade_committee(action_id: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select RUN_ID, ACTION_ID, PORTFOLIO_ID, STATUS, MODEL_NAME, STARTED_AT, COMPLETED_AT, DETAILS
            from MIP.LIVE.COMMITTEE_RUN
            where ACTION_ID = %s
            order by STARTED_AT desc
            limit 1
            """,
            (action_id,),
        )
        runs = fetch_all(cur)
        if not runs:
            return {"ok": True, "run": None, "role_outputs": [], "verdict": None}
        run = runs[0]
        run_id = run.get("RUN_ID")
        cur.execute(
            """
            select ROLE_NAME, STANCE, CONFIDENCE, SUMMARY, OUTPUT_JSON, CREATED_AT
            from MIP.LIVE.COMMITTEE_ROLE_OUTPUT
            where RUN_ID = %s
            order by ROLE_NAME
            """,
            (run_id,),
        )
        role_outputs = fetch_all(cur)
        cur.execute(
            """
            select RECOMMENDATION, SIZE_FACTOR, CONFIDENCE, IS_BLOCKED, REASON_CODES, VERDICT_JSON, CREATED_AT
            from MIP.LIVE.COMMITTEE_VERDICT
            where RUN_ID = %s
            limit 1
            """,
            (run_id,),
        )
        verdict_rows = fetch_all(cur)
        return {
            "ok": True,
            "run": serialize_row(run),
            "role_outputs": serialize_rows(role_outputs),
            "verdict": serialize_row(verdict_rows[0]) if verdict_rows else None,
        }
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/opening/validate")
def run_opening_validation(action_id: str, req: OpeningValidationRequest = Body(default_factory=OpeningValidationRequest)):
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")
        now_utc = _parse_iso_utc(req.now_utc_iso) if req.now_utc_iso else datetime.now(timezone.utc)
        if now_utc is None:
            raise HTTPException(status_code=400, detail="Invalid now_utc_iso format. Use ISO-8601.")
        gate = _run_opening_sanity_gate(cur, action, force_refresh_1m=req.force_refresh_1m, now_utc=now_utc)
        action_after = _fetch_live_action(cur, action_id)
        return {
            "ok": True,
            "action_id": action_id,
            "status": action_after.get("STATUS") if action_after else gate.get("result"),
            "opening_result": gate.get("result"),
            "reason_codes": gate.get("reason_codes") or [],
            "opening_validation": gate.get("opening_validation") or {},
        }
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/committee/run")
def run_live_trade_committee(action_id: str, req: CommitteeRunRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")
        status_upper = (action.get("STATUS") or "").upper()
        if status_upper in ("RESEARCH_IMPORTED", "PROPOSED", "PENDING_OPEN_VALIDATION"):
            opening_gate = _run_opening_sanity_gate(cur, action, force_refresh_1m=False, now_utc=datetime.now(timezone.utc))
            action = _fetch_live_action(cur, action_id)
            status_upper = (action.get("STATUS") or "").upper()
            if status_upper == "OPEN_BLOCKED":
                return {
                    "ok": False,
                    "action_id": action_id,
                    "status": "OPEN_BLOCKED",
                    "blocked_stage": "OPENING_SANITY_GATE",
                    "reason_codes": opening_gate.get("reason_codes") or [],
                    "opening_validation": opening_gate.get("opening_validation") or {},
                }

        if status_upper not in ("OPEN_ELIGIBLE", "OPEN_CAUTION", "PENDING_OPEN_STABILITY_REVIEW", "READY_FOR_APPROVAL_FLOW"):
            raise HTTPException(
                status_code=409,
                detail=f"Committee run blocked until opening validation passes (current: {status_upper}).",
            )

        opening_validation = _parse_variant(_parse_variant(action.get("PARAM_SNAPSHOT")).get("opening_validation"))
        stabilization = _parse_variant(opening_validation.get("stabilization"))
        ready_after = stabilization.get("ready_after_utc")
        ready_after_dt = _parse_iso_utc(ready_after)
        is_ready = bool(stabilization.get("is_ready"))
        if ready_after_dt is not None:
            is_ready = datetime.now(timezone.utc) >= ready_after_dt
            stabilization["is_ready"] = bool(is_ready)
            opening_validation["stabilization"] = stabilization
        if status_upper != "READY_FOR_APPROVAL_FLOW" and not is_ready:
            _persist_opening_snapshot(
                cur,
                action,
                "PENDING_OPEN_STABILITY_REVIEW",
                _parse_list_variant(action.get("REASON_CODES")) + ["OPEN_STABILITY_WAIT_REQUIRED"],
                opening_validation,
            )
            return {
                "ok": False,
                "action_id": action_id,
                "status": "PENDING_OPEN_STABILITY_REVIEW",
                "blocked_stage": "OPENING_STABILITY_REVIEW",
                "ready_after_utc": ready_after,
                "opening_validation": opening_validation,
            }

        if not req.force_rerun:
            cur.execute(
                """
                select RUN_ID, STATUS
                from MIP.LIVE.COMMITTEE_RUN
                where ACTION_ID = %s
                order by STARTED_AT desc
                limit 1
                """,
                (action_id,),
            )
            existing = fetch_all(cur)
            if existing and (existing[0].get("STATUS") or "").upper() == "COMPLETED":
                return {"ok": True, "action_id": action_id, "run_id": existing[0].get("RUN_ID"), "status": "COMPLETED", "idempotent_replay": True}

        run_id = str(uuid.uuid4())
        cur.execute(
            """
            insert into MIP.LIVE.COMMITTEE_RUN (
              RUN_ID, ACTION_ID, PORTFOLIO_ID, STATUS, MODEL_NAME, STARTED_AT, DETAILS
            )
            select
              %s, %s, %s, 'RUNNING', %s, current_timestamp(), try_parse_json(%s)
            """,
            (
                run_id,
                action_id,
                action.get("PORTFOLIO_ID"),
                req.model,
                json.dumps({"actor": req.actor}),
            ),
        )

        context = _build_action_decision_context(cur, action)
        action_training_snapshot = context.get("training_qualification_snapshot") or {}
        action_target_snapshot = context.get("target_expectation_snapshot") or {}
        action_news_snapshot = context.get("news_context_snapshot") or {}
        latest_news_snapshot = context.get("latest_symbol_news_context") or {}
        pw_evidence = context.get("parallel_worlds_evidence")

        outputs, verdict = _run_multiagent_dialogue(
            cur,
            model=req.model,
            context=context,
            persist_run_id=run_id,
            emit=None,
        )
        jd = _parse_variant(verdict.get("joint_decision"))
        if not verdict.get("blocked") and (
            jd.get("realistic_target_return") is None
            or jd.get("hold_bars") is None
            or jd.get("acceptable_early_exit_target_return") is None
        ):
            verdict = _backfill_joint_decision_from_policy(verdict, context)
        reason_codes = []
        if verdict["blocked"]:
            reason_codes.append("COMMITTEE_BLOCKED")
        elif verdict["recommendation"] == "PROCEED_REDUCED":
            reason_codes.append("COMMITTEE_REDUCED_SIZE")
        if verdict.get("quality_backfilled"):
            reason_codes.append("COMMITTEE_POLICY_BACKFILL_APPLIED")
        reason_codes.append("COMMITTEE_REVIEWED")
        next_status = "OPEN_BLOCKED" if verdict["blocked"] else "READY_FOR_APPROVAL_FLOW"
        proposed_price_derived = None
        proposed_qty_derived = None
        try:
            cur.execute(
                """
                select CLOSE
                from MIP.MART.MARKET_BARS
                where SYMBOL = %s
                  and INTERVAL_MINUTES = 1
                order by TS desc
                limit 1
                """,
                (action.get("SYMBOL"),),
            )
            bar_rows = fetch_all(cur)
            if bar_rows and bar_rows[0].get("CLOSE") is not None:
                proposed_price_derived = float(bar_rows[0].get("CLOSE"))
            else:
                cur.execute(
                    """
                    select CLOSE
                    from MIP.MART.MARKET_BARS
                    where SYMBOL = %s
                      and INTERVAL_MINUTES in (15, 60, 1440)
                    order by TS desc
                    limit 1
                    """,
                    (action.get("SYMBOL"),),
                )
                fallback_rows = fetch_all(cur)
                if fallback_rows and fallback_rows[0].get("CLOSE") is not None:
                    proposed_price_derived = float(fallback_rows[0].get("CLOSE"))
        except Exception:
            proposed_price_derived = None

        try:
            param_snapshot = _parse_variant(action.get("PARAM_SNAPSHOT"))
            target_weight = param_snapshot.get("target_weight")
            target_weight_abs = abs(float(target_weight)) if target_weight is not None else None
            committee_size_factor = float(verdict.get("size_factor") or 1.0)
            training_size_cap = float(action.get("TRAINING_SIZE_CAP_FACTOR") or 1.0)
            open_factor = float(action.get("TARGET_OPEN_CONDITION_FACTOR") or 1.0)
            effective_weight = None
            if target_weight_abs is not None:
                effective_weight = target_weight_abs * committee_size_factor * training_size_cap * open_factor

            if proposed_price_derived is not None and effective_weight is not None and effective_weight > 0:
                cur.execute(
                    """
                    select
                      c.IBKR_ACCOUNT_ID,
                      s.NET_LIQUIDATION_EUR
                    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG c
                    left join MIP.LIVE.BROKER_SNAPSHOTS s
                      on s.IBKR_ACCOUNT_ID = c.IBKR_ACCOUNT_ID
                     and s.SNAPSHOT_TYPE = 'NAV'
                    where c.PORTFOLIO_ID = %s
                    qualify row_number() over (
                        partition by c.PORTFOLIO_ID
                        order by s.SNAPSHOT_TS desc nulls last
                    ) = 1
                    """,
                    (action.get("PORTFOLIO_ID"),),
                )
                nav_rows = fetch_all(cur)
                nav_eur = float((nav_rows[0] or {}).get("NET_LIQUIDATION_EUR") or 0.0) if nav_rows else 0.0
                if nav_eur > 0:
                    est_notional = nav_eur * effective_weight
                    proposed_qty_derived = max(int(est_notional / max(proposed_price_derived, 1e-9)), 1)
        except Exception:
            proposed_qty_derived = None

        cur.execute(
            """
            insert into MIP.LIVE.COMMITTEE_VERDICT (
              RUN_ID, ACTION_ID, PORTFOLIO_ID, RECOMMENDATION, SIZE_FACTOR, CONFIDENCE, IS_BLOCKED,
              REASON_CODES, VERDICT_JSON, CREATED_AT
            )
            select
              %s, %s, %s, %s, %s, %s, %s, try_parse_json(%s), try_parse_json(%s), current_timestamp()
            """,
            (
                run_id,
                action_id,
                action.get("PORTFOLIO_ID"),
                verdict["recommendation"],
                verdict["size_factor"],
                verdict["confidence"],
                verdict["blocked"],
                json.dumps(reason_codes),
                json.dumps({"verdict": verdict, "outputs": outputs, "joint_decision": verdict.get("joint_decision")}),
            ),
        )
        cur.execute(
            """
            update MIP.LIVE.COMMITTEE_RUN
               set STATUS = 'COMPLETED',
                   COMPLETED_AT = current_timestamp(),
                   DETAILS = parse_json(%s)
             where RUN_ID = %s
            """,
            (
                json.dumps(
                    {
                        "actor": req.actor,
                        "role_count": len(outputs),
                        "recommendation": verdict["recommendation"],
                        "joint_decision": verdict.get("joint_decision"),
                    }
                ),
                run_id,
            ),
        )
        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set COMMITTEE_STATUS = 'COMPLETED',
                   COMMITTEE_RUN_ID = %s,
                   COMMITTEE_COMPLETED_TS = current_timestamp(),
                   COMMITTEE_VERDICT = %s,
                   STATUS = %s,
                   PROPOSED_PRICE = coalesce(%s, PROPOSED_PRICE),
                   PROPOSED_QTY = coalesce(%s, PROPOSED_QTY),
                   REASON_CODES = parse_json(%s),
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (
                run_id,
                verdict["recommendation"],
                next_status,
                proposed_price_derived,
                proposed_qty_derived,
                json.dumps(reason_codes),
                action_id,
            ),
        )
        action_after = _fetch_live_action(cur, action_id)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_COMMITTEE_COMPLETED",
            status="COMPLETED",
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "committee_run_id": run_id,
                "recommendation": verdict["recommendation"],
                "size_factor": verdict["size_factor"],
                "blocked": verdict["blocked"],
                "news_context_state": action_news_snapshot.get("context_state"),
                "news_event_shock_flag": bool(action_news_snapshot.get("event_shock_flag")),
            },
            outcome_state={
                "roles": COMMITTEE_ROLES,
                "training_qualification_snapshot": action_training_snapshot,
                "target_expectation_snapshot": action_target_snapshot,
                "news_context_snapshot": action_news_snapshot,
                "latest_symbol_news_context": latest_news_snapshot,
                "parallel_worlds_evidence": pw_evidence,
                "joint_decision": verdict.get("joint_decision"),
            },
        )
        return {
            "ok": True,
            "action_id": action_id,
            "run_id": run_id,
            "status": "COMPLETED",
            "action_status": next_status,
            "verdict": verdict,
            "joint_decision": verdict.get("joint_decision"),
            "derived_sizing": {
                "proposed_price": proposed_price_derived,
                "proposed_qty": proposed_qty_derived,
            },
        }
    finally:
        conn.close()


@router.get("/trades/actions/{action_id}/committee/live-prompt")
def stream_live_trade_committee_prompt(
    action_id: str,
    actor: str = Query(default="committee_orchestrator"),
    model: str = Query(default="claude-3-5-sonnet"),
):
    def event_stream():
        out_queue: Queue = Queue()
        done = Event()
        result: dict = {"outputs": [], "verdict": None, "error": None}

        def worker():
            conn = get_connection()
            try:
                cur = conn.cursor()
                action = _fetch_live_action(cur, action_id)
                if not action:
                    result["error"] = "Action not found."
                    return
                context = _build_action_decision_context(cur, action)

                def cb(ev_name: str, payload: dict):
                    out_queue.put((ev_name, {"action_id": action_id, **payload}))

                outputs, verdict = _run_multiagent_dialogue(
                    cur,
                    model=model,
                    context=context,
                    persist_run_id=None,
                    emit=cb,
                )
                result["outputs"] = outputs or []
                result["verdict"] = verdict or {}
            except Exception as exc:
                result["error"] = str(exc)
            finally:
                conn.close()
                done.set()

        Thread(target=worker, daemon=True).start()
        yield _sse_event("start", {"action_id": action_id, "actor": actor, "model": model})
        heartbeat_n = 0

        while not done.is_set() or not out_queue.empty():
            try:
                ev_name, payload = out_queue.get(timeout=0.4)
                yield _sse_event(ev_name, payload)
            except Empty:
                heartbeat_n += 1
                if heartbeat_n % 3 == 0:
                    yield _sse_event("heartbeat", {"action_id": action_id, "status": "running"})
                continue

        if result.get("error"):
            yield _sse_event("error", {"action_id": action_id, "message": result.get("error")})
            return

        for out in result.get("outputs") or []:
            yield _sse_event(
                "role_summary",
                {
                    "action_id": action_id,
                    "role": out.get("role"),
                    "stance": out.get("stance"),
                    "confidence": out.get("confidence"),
                    "summary": out.get("summary"),
                },
            )
        verdict = result.get("verdict") or {}
        yield _sse_event("final", {"action_id": action_id, "joint_decision": verdict.get("joint_decision"), "verdict": verdict})

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.get("/trades/actions/{action_id}/revalidate/live-prompt")
def stream_revalidate_prompt(
    action_id: str,
    model: str = Query(default="claude-3-5-sonnet"),
):
    def event_stream():
        conn = get_connection()
        try:
            cur = conn.cursor()
            action = _fetch_live_action(cur, action_id)
            if not action:
                yield _sse_event("error", {"message": "Action not found."})
                return
            context = _build_action_decision_context(cur, action)
            context["stage"] = "REVALIDATION_PREVIEW"
            context["revalidation"] = {
                "last_outcome": action.get("REVALIDATION_OUTCOME"),
                "last_ts": str(action.get("REVALIDATION_TS")) if action.get("REVALIDATION_TS") is not None else None,
            }
            yield _sse_event("start", {"action_id": action_id, "stage": "revalidation", "model": model})
            outputs, verdict = _run_multiagent_dialogue(cur, model=model, context=context, persist_run_id=None, emit=None)
            for out in outputs:
                yield _sse_event(
                    "agent_turn",
                    {
                        "action_id": action_id,
                        "role": out.get("role"),
                        "stance": out.get("stance"),
                        "confidence": out.get("confidence"),
                        "summary": out.get("summary"),
                    },
                )
            yield _sse_event("final", {"action_id": action_id, "joint_decision": verdict.get("joint_decision"), "verdict": verdict})
        except Exception as exc:
            yield _sse_event("error", {"action_id": action_id, "message": str(exc)})
        finally:
            conn.close()

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/trades/actions/{action_id}/pm-accept")
def pm_accept_live_action(action_id: str, req: PmAcceptRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")
        committee_required = bool(action.get("COMMITTEE_REQUIRED")) if action.get("COMMITTEE_REQUIRED") is not None else True
        committee_status = (action.get("COMMITTEE_STATUS") or "").upper()
        if committee_required and committee_status != "COMPLETED":
            raise HTTPException(
                status_code=409,
                detail={"message": "Committee output is required before PM accept.", "reason_codes": ["COMMITTEE_REQUIRED_BEFORE_PM_ACCEPT"]},
            )
        _assert_transition_allowed(action.get("STATUS"), "PM_ACCEPTED")
        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set STATUS = 'PM_ACCEPTED',
                   PM_APPROVED_BY = %s,
                   PM_APPROVED_TS = current_timestamp(),
                   COMPLIANCE_STATUS = 'PENDING',
                   REASON_CODES = null,
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (req.actor, action_id),
        )
        action_after = _fetch_live_action(cur, action_id)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_PM_ACCEPT",
            status="PM_ACCEPTED",
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "approval_transition": f"{action.get('STATUS')}->PM_ACCEPTED",
                "actor": req.actor,
            },
        )
        return {"ok": True, "action_id": action_id, "status": "PM_ACCEPTED"}
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/reject-stale")
def reject_stale_live_action(action_id: str, req: RejectStaleActionRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")

        current_status = (action.get("STATUS") or "").upper()
        allowed_statuses = {
            "RESEARCH_IMPORTED",
            "PROPOSED",
            "PENDING_OPEN_VALIDATION",
            "OPEN_ELIGIBLE",
            "OPEN_CAUTION",
            "PENDING_OPEN_STABILITY_REVIEW",
            "READY_FOR_APPROVAL_FLOW",
            "PM_ACCEPTED",
            "COMPLIANCE_APPROVED",
            "INTENT_SUBMITTED",
            "INTENT_APPROVED",
            "REVALIDATED_FAIL",
            "REVALIDATED_PASS",
        }
        if current_status not in allowed_statuses:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": f"Cannot stale-reject from status {current_status}.",
                    "reason_codes": ["STALE_REJECT_NOT_ALLOWED_STATUS"],
                },
            )

        existing_reason_codes = _parse_list_variant(action.get("REASON_CODES"))
        merged_reasons = sorted(set(existing_reason_codes + ["STALE_REJECTED_MANUAL"]))
        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set STATUS = 'OPEN_BLOCKED',
                   REASON_CODES = parse_json(%s),
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (json.dumps(merged_reasons), action_id),
        )
        action_after = _fetch_live_action(cur, action_id)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_STALE_REJECTED",
            status="OPEN_BLOCKED",
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "actor": req.actor,
                "reason_codes": merged_reasons,
            },
            outcome_state={"notes": req.notes},
        )
        return {"ok": True, "action_id": action_id, "status": "OPEN_BLOCKED", "reason_codes": merged_reasons}
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/compliance")
def compliance_decide_live_action(action_id: str, req: ComplianceDecisionRequest):
    status = "COMPLIANCE_APPROVED" if req.decision == "APPROVE" else "COMPLIANCE_DENIED"
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")
        _assert_transition_allowed(action.get("STATUS"), status)
        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set STATUS = %s,
                   COMPLIANCE_STATUS = %s,
                   COMPLIANCE_APPROVED_BY = %s,
                   COMPLIANCE_DECISION_TS = current_timestamp(),
                   COMPLIANCE_NOTES = %s,
                   COMPLIANCE_REFERENCE_ID = %s,
                   REASON_CODES = null,
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (status, req.decision, req.actor, req.notes, req.reference_id, action_id),
        )
        action_after = _fetch_live_action(cur, action_id)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_COMPLIANCE_DECISION",
            status=status,
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "approval_transition": f"{action.get('STATUS')}->{status}",
                "decision": req.decision,
                "actor": req.actor,
            },
            outcome_state={
                "compliance_notes": req.notes,
                "compliance_reference_id": req.reference_id,
            },
        )
        return {"ok": True, "action_id": action_id, "status": status}
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/intent-submit")
def submit_live_trade_intent(action_id: str, req: IntentSubmitRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")
        _assert_transition_allowed(action.get("STATUS"), "INTENT_SUBMITTED")
        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set STATUS = 'INTENT_SUBMITTED',
                   INTENT_SUBMITTED_BY = %s,
                   INTENT_SUBMITTED_TS = current_timestamp(),
                   INTENT_REFERENCE_ID = coalesce(%s, INTENT_REFERENCE_ID),
                   REASON_CODES = null,
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (req.actor, req.reference_id, action_id),
        )
        action_after = _fetch_live_action(cur, action_id)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_INTENT_SUBMITTED",
            status="INTENT_SUBMITTED",
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "intent_transition": f"{action.get('STATUS')}->INTENT_SUBMITTED",
                "actor": req.actor,
            },
            outcome_state={"intent_reference_id": req.reference_id},
        )
        return {"ok": True, "action_id": action_id, "status": "INTENT_SUBMITTED"}
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/intent-approve")
def approve_live_trade_intent(action_id: str, req: IntentApproveRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")
        _assert_transition_allowed(action.get("STATUS"), "INTENT_APPROVED")
        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set STATUS = 'INTENT_APPROVED',
                   INTENT_APPROVED_BY = %s,
                   INTENT_APPROVED_TS = current_timestamp(),
                   REASON_CODES = null,
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (req.actor, action_id),
        )
        action_after = _fetch_live_action(cur, action_id)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_INTENT_APPROVED",
            status="INTENT_APPROVED",
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "intent_transition": f"{action.get('STATUS')}->INTENT_APPROVED",
                "actor": req.actor,
            },
        )
        return {"ok": True, "action_id": action_id, "status": "INTENT_APPROVED"}
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/revalidate")
def revalidate_live_action(
    action_id: str,
    req: RevalidateLiveActionRequest = Body(default_factory=RevalidateLiveActionRequest),
):
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")
        current_status = action.get("STATUS")
        if current_status == "INTENT_APPROVED":
            _assert_transition_allowed(current_status, "REVALIDATED_PASS")
        elif current_status == "REVALIDATED_FAIL":
            _assert_transition_allowed(current_status, "REVALIDATED_PASS")
        elif current_status == "REVALIDATED_PASS":
            _assert_transition_allowed(current_status, "REVALIDATED_PASS")
        else:
            raise HTTPException(
                status_code=409,
                detail=f"Revalidation allowed only from INTENT_APPROVED/REVALIDATED_FAIL/REVALIDATED_PASS (current: {current_status})",
            )
        symbol = action.get("SYMBOL")
        proposed_price = action.get("PROPOSED_PRICE")
        refresh_info = {"attempted": False}
        if req.force_refresh_1m:
            refresh_info = _force_refresh_latest_one_minute_bars(cur)

        cur.execute(
            """
            select TS, CLOSE
            from MIP.MART.MARKET_BARS
            where SYMBOL = %s
              and INTERVAL_MINUTES = 1
            order by TS desc
            limit 1
            """,
            (symbol,),
        )
        bar = cur.fetchone()
        source = "ONE_MINUTE_BAR"
        if bar:
            ref_ts, ref_price = bar
        else:
            cur.execute(
                """
                select TS, CLOSE
                from MIP.MART.MARKET_BARS
                where SYMBOL = %s
                  and INTERVAL_MINUTES in (15, 60, 1440)
                order by TS desc
                limit 1
                """,
                (symbol,),
            )
            fallback = cur.fetchone()
            if not fallback:
                raise HTTPException(status_code=400, detail="No market bar found for symbol, revalidation blocked.")
            ref_ts, ref_price = fallback
            source = "BAR_FALLBACK"

        deviation = None
        if proposed_price and ref_price:
            try:
                deviation = abs(float(ref_price) - float(proposed_price)) / max(float(proposed_price), 1e-9)
            except Exception:
                deviation = None
        revalidation_outcome = "FAIL"
        status = "REVALIDATED_FAIL"
        reason_codes: list[str] = []
        reduced_size_factor = None
        target_open_condition_factor = 1.0

        if deviation is None or deviation <= 0.02:
            revalidation_outcome = "PASS"
            status = "REVALIDATED_PASS"
            target_open_condition_factor = 1.0
        elif deviation <= 0.04:
            # Latency-aware compromise: keep candidate valid but force a reduced-size execution envelope.
            revalidation_outcome = "PASS_WITH_REDUCED_SIZE"
            status = "REVALIDATED_PASS"
            reduced_size_factor = 0.5
            target_open_condition_factor = 0.8
            reason_codes.append("REDUCED_SIZE_DUE_TO_PRICE_DEVIATION")
        else:
            revalidation_outcome = "FAIL"
            status = "REVALIDATED_FAIL"
            target_open_condition_factor = 0.65
            reason_codes.append("PRICE_GUARD_FAIL")

        source_effective = "FORCED_REFRESH_1M" if req.force_refresh_1m else source
        latest_news_snapshot = _fetch_latest_symbol_news_context(cur, action.get("SYMBOL"), action.get("ASSET_CLASS"))
        news_for_policy = latest_news_snapshot if latest_news_snapshot.get("available") else _parse_variant(action.get("NEWS_CONTEXT_SNAPSHOT"))
        news_context_state = str(news_for_policy.get("context_state") or "NEUTRAL").upper()
        news_event_shock = bool(news_for_policy.get("event_shock_flag"))
        news_freshness = str(news_for_policy.get("freshness_bucket") or "UNKNOWN").upper()
        news_causes_caution = news_context_state in ("CAUTIONARY", "DESTABILIZING")
        if news_event_shock and news_freshness in ("FRESH", "OVERNIGHT"):
            if revalidation_outcome == "PASS":
                revalidation_outcome = "PASS_WITH_REDUCED_SIZE"
                status = "REVALIDATED_PASS"
                reduced_size_factor = min(reduced_size_factor or 1.0, 0.5)
                reason_codes.append("NEWS_REVALIDATION_CAUTION")
            elif revalidation_outcome == "FAIL":
                reason_codes.append("NEWS_EVENT_SHOCK_BLOCK")
        elif news_causes_caution and revalidation_outcome == "PASS":
            reason_codes.append("NEWS_REVALIDATION_CAUTION")

        target_snapshot_before = _parse_variant(action.get("TARGET_EXPECTATION_SNAPSHOT"))
        target_snapshot_after = _build_target_expectation_snapshot(
            cur,
            symbol=action.get("SYMBOL"),
            market_type=action.get("ASSET_CLASS"),
            pattern_id=_parse_variant(action.get("PARAM_SNAPSHOT")).get("signal_pattern_id"),
            interval_minutes=1440,
            open_condition_factor=target_open_condition_factor,
        )
        guard_result = "PASS" if status == "REVALIDATED_PASS" else "FAIL"

        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set REVALIDATION_TS = current_timestamp(),
                   REVALIDATION_PRICE = %s,
                   PRICE_DEVIATION_PCT = %s,
                   PRICE_GUARD_RESULT = %s,
                   ONE_MIN_BAR_TS = %s,
                   ONE_MIN_BAR_CLOSE = %s,
                   EXECUTION_PRICE_SOURCE = %s,
                   REVALIDATION_OUTCOME = %s,
                   REVALIDATION_POLICY_VERSION = %s,
                   REVALIDATION_DATA_SOURCE = %s,
                   TARGET_EXPECTATION_SNAPSHOT = parse_json(%s),
                   TARGET_OPEN_CONDITION_FACTOR = %s,
                   TARGET_EXPECTATION_POLICY_VERSION = %s,
                   NEWS_CONTEXT_SNAPSHOT = parse_json(%s),
                   NEWS_CONTEXT_STATE = %s,
                   NEWS_EVENT_SHOCK_FLAG = %s,
                   NEWS_FRESHNESS_BUCKET = %s,
                   NEWS_CONTEXT_POLICY_VERSION = %s,
                   STATUS = %s,
                   PROPOSED_QTY = case
                       when %s is not null and PROPOSED_QTY is not null then greatest(PROPOSED_QTY * %s, 1)
                       else PROPOSED_QTY
                   end,
                   REASON_CODES = parse_json(%s),
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (
                ref_price,
                deviation,
                guard_result,
                ref_ts if source == "ONE_MINUTE_BAR" else None,
                ref_price if source == "ONE_MINUTE_BAR" else None,
                source,
                revalidation_outcome,
                LIVE_POLICY_VERSION,
                source_effective,
                json.dumps(target_snapshot_after),
                float(target_open_condition_factor),
                target_snapshot_after.get("policy_version"),
                json.dumps(news_for_policy),
                news_context_state,
                bool(news_event_shock),
                news_freshness,
                news_for_policy.get("policy_version") or NEWS_CONTEXT_POLICY_VERSION,
                status,
                reduced_size_factor,
                reduced_size_factor,
                json.dumps(reason_codes),
                action_id,
            ),
        )
        action_after = _fetch_live_action(cur, action_id)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_REVALIDATION",
            status=status,
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "price_deviation_pct": float(deviation) if deviation is not None else None,
                "price_guard_result": revalidation_outcome,
                "price_source": source_effective,
                "reduced_size_factor": reduced_size_factor,
                "target_open_condition_factor_before": target_snapshot_before.get("open_condition_factor"),
                "target_open_condition_factor_after": target_snapshot_after.get("open_condition_factor"),
                "target_bands_after": _parse_variant(target_snapshot_after).get("bands"),
                "news_context_state": news_context_state,
                "news_event_shock_flag": bool(news_event_shock),
                "news_freshness_bucket": news_freshness,
                "force_refresh_1m": bool(req.force_refresh_1m),
            },
            outcome_state={
                "target_expectation_snapshot": target_snapshot_after,
                "news_context_snapshot": news_for_policy,
            },
        )
        return {
            "ok": True,
            "action_id": action_id,
            "status": status,
            "revalidation_outcome": revalidation_outcome,
            "policy_version": LIVE_POLICY_VERSION,
            "price_source": source_effective,
            "revalidation_price": float(ref_price) if ref_price is not None else None,
            "price_deviation_pct": float(deviation) if deviation is not None else None,
            "reduced_size_factor": reduced_size_factor,
            "target_open_condition_factor": float(target_open_condition_factor),
            "target_expectation_snapshot": target_snapshot_after,
            "force_refresh_1m": bool(req.force_refresh_1m),
            "refresh": refresh_info,
        }
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/execute")
def execute_live_action(action_id: str, req: ExecuteLiveActionRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")

        reason_codes: list[str] = []
        now_utc = datetime.now(timezone.utc)
        current_status = (action.get("STATUS") or "").upper()
        compliance_status = (action.get("COMPLIANCE_STATUS") or "").upper()
        if current_status != "REVALIDATED_PASS":
            reason_codes.append("EXECUTION_REQUIRES_REVALIDATED_PASS")
        if compliance_status != "APPROVE":
            reason_codes.append("COMPLIANCE_NOT_APPROVED")

        compliance_decision_ts = action.get("COMPLIANCE_DECISION_TS")
        revalidation_ts = action.get("REVALIDATION_TS")
        if compliance_decision_ts and revalidation_ts:
            cd_ts = compliance_decision_ts.replace(tzinfo=timezone.utc)
            rv_ts = revalidation_ts.replace(tzinfo=timezone.utc)
            if rv_ts <= cd_ts:
                reason_codes.append("REVALIDATION_REQUIRED_AFTER_COMPLIANCE")

        cur.execute(
            """
            select
              IBKR_ACCOUNT_ID, MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC, DRIFT_STATUS, IS_ACTIVE
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            """,
            (action.get("PORTFOLIO_ID"),),
        )
        cfg_rows = fetch_all(cur)
        if not cfg_rows:
            raise HTTPException(status_code=400, detail="Live portfolio config missing.")
        cfg = cfg_rows[0]
        if cfg.get("IS_ACTIVE") is False:
            raise HTTPException(status_code=400, detail="Live portfolio config is inactive.")
        drift_status = (cfg.get("DRIFT_STATUS") or "").upper()
        if drift_status and drift_status not in ("OK", "CLEAR", "HEALTHY"):
            reason_codes.append("BROKER_TRUTH_DRIFT_UNRESOLVED")

        unresolved_drift_row = None
        try:
            cur.execute(
                """
                select
                  DRIFT_ID, RECONCILIATION_TS, NAV_DRIFT_PCT, CASH_DRIFT_EUR, POSITION_DRIFT_COUNT, DETAILS
                from MIP.LIVE.DRIFT_LOG
                where PORTFOLIO_ID = %s
                  and coalesce(DRIFT_DETECTED, false) = true
                  and RESOLUTION_TS is null
                order by RECONCILIATION_TS desc
                limit 1
                """,
                (action.get("PORTFOLIO_ID"),),
            )
            unresolved_drift_rows = fetch_all(cur)
            if unresolved_drift_rows:
                unresolved_drift_row = unresolved_drift_rows[0]
                reason_codes.append("UNRESOLVED_DRIFT_LOG_PRESENT")
        except Exception:
            unresolved_drift_row = None

        validity_window_end = action.get("VALIDITY_WINDOW_END")
        if validity_window_end and hasattr(validity_window_end, "replace"):
            vw = validity_window_end.replace(tzinfo=timezone.utc)
            if vw < now_utc:
                reason_codes.append("ACTION_EXPIRED")

        if not revalidation_ts:
            reason_codes.append("MISSING_REVALIDATION")
        else:
            rv_ts = revalidation_ts.replace(tzinfo=timezone.utc)
            rv_age_sec = (now_utc - rv_ts).total_seconds()
            validity_window_sec = int(cfg.get("VALIDITY_WINDOW_SEC") or 14400)
            max_reval_age = min(validity_window_sec, EXECUTION_CLICK_MAX_REVALIDATION_SEC)
            if rv_age_sec > max_reval_age:
                reason_codes.append("EXECUTION_CLICK_REVALIDATION_STALE")

        if (action.get("PRICE_GUARD_RESULT") or "").upper() != "PASS":
            reason_codes.append("PRICE_GUARD_FAIL")
        realism_reason_codes, realism_details = _first_session_realism_checks(cur, action, cfg)
        reason_codes.extend(realism_reason_codes)
        news_snapshot = _parse_variant(action.get("NEWS_CONTEXT_SNAPSHOT"))
        if not news_snapshot:
            news_snapshot = _parse_variant(action.get("PARAM_SNAPSHOT")).get("news_context")
        news_context_state = str(news_snapshot.get("context_state") or "").upper()
        news_event_shock_flag = bool(news_snapshot.get("event_shock_flag"))
        news_freshness_bucket = str(news_snapshot.get("freshness_bucket") or "").upper()
        if news_event_shock_flag and news_freshness_bucket in ("FRESH", "OVERNIGHT"):
            reason_codes.append("NEWS_EXECUTION_BLOCKED_EVENT_SHOCK")
        elif news_context_state in ("CAUTIONARY", "DESTABILIZING"):
            reason_codes.append("NEWS_EXECUTION_CAUTION")

        account_id = cfg.get("IBKR_ACCOUNT_ID")
        cur.execute(
            """
            select SNAPSHOT_TS, NET_LIQUIDATION_EUR, TOTAL_CASH_EUR
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'NAV'
              and IBKR_ACCOUNT_ID = %s
            order by SNAPSHOT_TS desc
            limit 1
            """,
            (account_id,),
        )
        nav_rows = fetch_all(cur)
        if not nav_rows:
            reason_codes.append("MISSING_SNAPSHOT")
            nav_eur = None
            cash_eur = None
            snapshot_ts = None
        else:
            nav_row = nav_rows[0]
            snapshot_ts = nav_row.get("SNAPSHOT_TS")
            nav_eur = float(nav_row.get("NET_LIQUIDATION_EUR") or 0.0)
            cash_eur = float(nav_row.get("TOTAL_CASH_EUR") or 0.0)
            if snapshot_ts:
                snap_age_sec = (now_utc - snapshot_ts.replace(tzinfo=timezone.utc)).total_seconds()
                max_snap_age = cfg.get("SNAPSHOT_FRESHNESS_THRESHOLD_SEC") or 300
                if snap_age_sec > max_snap_age:
                    reason_codes.append("SNAPSHOT_STALE")

        cur.execute(
            """
            select count(distinct SYMBOL) as OPEN_POSITIONS
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'POSITION'
              and IBKR_ACCOUNT_ID = %s
              and SNAPSHOT_TS = (
                select max(SNAPSHOT_TS)
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'POSITION'
                  and IBKR_ACCOUNT_ID = %s
              )
              and coalesce(POSITION_QTY, 0) <> 0
            """,
            (account_id, account_id),
        )
        pos_rows = fetch_all(cur)
        open_positions = int((pos_rows[0] or {}).get("OPEN_POSITIONS") or 0)

        max_positions = cfg.get("MAX_POSITIONS")
        if max_positions is not None and open_positions >= int(max_positions):
            reason_codes.append("MAX_POSITIONS_EXCEEDED")

        proposed_qty = action.get("PROPOSED_QTY")
        px = action.get("REVALIDATION_PRICE") or action.get("PROPOSED_PRICE")
        if proposed_qty is None or px is None:
            reason_codes.append("MISSING_NOTIONAL_INPUT")
            est_notional = None
        else:
            est_notional = abs(float(proposed_qty) * float(px))

        max_position_pct = cfg.get("MAX_POSITION_PCT")
        if est_notional is not None and nav_eur and max_position_pct is not None and nav_eur > 0:
            if (est_notional / nav_eur) > float(max_position_pct):
                reason_codes.append("MAX_POSITION_PCT_EXCEEDED")

        if est_notional is not None and nav_eur and (action.get("SIDE") or "").upper() == "BUY":
            cash_buffer_pct = float(cfg.get("CASH_BUFFER_PCT") or 0.0)
            min_cash_after = nav_eur * cash_buffer_pct
            if (cash_eur - est_notional) < min_cash_after:
                reason_codes.append("CASH_BUFFER_BREACH")

        proposal_or_action = action.get("PROPOSAL_ID") if action.get("PROPOSAL_ID") is not None else action_id
        idempotency_key = f"{action.get('PORTFOLIO_ID')}:{proposal_or_action}:{req.attempt_n}"
        cur.execute(
            """
            select ORDER_ID, STATUS
            from MIP.LIVE.LIVE_ORDERS
            where IDEMPOTENCY_KEY = %s
            limit 1
            """,
            (idempotency_key,),
        )
        existing_order_rows = fetch_all(cur)
        if existing_order_rows:
            existing_order = existing_order_rows[0]
            return {
                "ok": True,
                "action_id": action_id,
                "status": "EXECUTION_REQUESTED",
                "order_id": existing_order.get("ORDER_ID"),
                "idempotency_key": idempotency_key,
                "mode": "PAPER_PLACEHOLDER",
                "idempotent_replay": True,
            }

        cur.execute(
            """
            select ORDER_ID, STATUS, IDEMPOTENCY_KEY
            from MIP.LIVE.LIVE_ORDERS
            where ACTION_ID = %s
              and STATUS in ('SUBMITTED','ACKNOWLEDGED','PARTIAL_FILL','FILLED')
            limit 1
            """,
            (action_id,),
        )
        existing_action_order = fetch_all(cur)
        if existing_action_order:
            status_existing = (existing_action_order[0].get("STATUS") or "").upper()
            if status_existing in ("SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL"):
                reason_codes.append("ACTIVE_ORDER_EXISTS_DIFFERENT_ATTEMPT")
            else:
                reason_codes.append("ACTION_ALREADY_EXECUTED")

        if reason_codes:
            final_reason_codes = sorted(set(reason_codes))
            _write_reason_codes(cur, action_id, final_reason_codes)
            _append_learning_ledger_event(
                cur,
                event_name="LIVE_EXECUTION_BLOCKED",
                status="BLOCKED",
                action_before=action,
                action_after=_fetch_live_action(cur, action_id),
                policy_version=LIVE_POLICY_VERSION,
                influence_delta={
                    "safety_gates_passed": False,
                    "reason_codes": final_reason_codes,
                },
                outcome_state={
                    "validator": "FIRST_SESSION_REALISM",
                    "realism_details": realism_details,
                    "actor": req.actor,
                    "required_status": "REVALIDATED_PASS",
                    "required_compliance": "APPROVE",
                    "drift_status": drift_status,
                    "latest_unresolved_drift": serialize_row(unresolved_drift_row) if unresolved_drift_row else None,
                    "news_context_snapshot": news_snapshot,
                },
            )
            raise HTTPException(
                status_code=409,
                detail={"message": "Execution blocked by safety gates.", "reason_codes": final_reason_codes},
            )

        order_id = str(uuid.uuid4())
        entry_price = float(action.get("REVALIDATION_PRICE") or action.get("PROPOSED_PRICE"))
        qty_ordered = float(proposed_qty)
        side = str(action.get("SIDE") or "").upper()
        exit_side = "SELL" if side == "BUY" else "BUY"

        cur.execute(
            """
            select
              VERDICT_JSON:verdict:joint_decision:realistic_target_return::float as TARGET_RETURN,
              VERDICT_JSON:verdict:joint_decision:acceptable_early_exit_target_return::float as EARLY_EXIT_TARGET_RETURN
            from MIP.LIVE.COMMITTEE_VERDICT
            where RUN_ID = %s
            limit 1
            """,
            (action.get("COMMITTEE_RUN_ID"),),
        )
        verdict_rows = fetch_all(cur)
        verdict = verdict_rows[0] if verdict_rows else {}
        realistic_target_return = verdict.get("TARGET_RETURN")
        early_exit_target_return = verdict.get("EARLY_EXIT_TARGET_RETURN")
        target_return = (
            float(early_exit_target_return)
            if early_exit_target_return is not None
            else (float(realistic_target_return) if realistic_target_return is not None else None)
        )
        stop_loss_pct = float(cfg.get("BUST_PCT")) if cfg.get("BUST_PCT") is not None else None

        tp_price = None
        sl_price = None
        if target_return is not None:
            if side == "BUY":
                tp_price = entry_price * (1 + target_return)
            elif side == "SELL":
                tp_price = max(entry_price * (1 - target_return), 0.0001)
        if stop_loss_pct is not None:
            if side == "BUY":
                sl_price = max(entry_price * (1 - stop_loss_pct), 0.0001)
            elif side == "SELL":
                sl_price = entry_price * (1 + stop_loss_pct)

        order_legs = [
            {
                "order_id": order_id,
                "idempotency_key": idempotency_key,
                "side": side,
                "order_type": "MKT_PAPER",
                "limit_price": entry_price,
                "role": "PARENT",
            }
        ]
        if tp_price is not None:
            order_legs.append(
                {
                    "order_id": str(uuid.uuid4()),
                    "idempotency_key": f"{idempotency_key}:TP",
                    "side": exit_side,
                    "order_type": "LMT_TP_PAPER",
                    "limit_price": float(tp_price),
                    "role": "TAKE_PROFIT",
                }
            )
        if sl_price is not None:
            order_legs.append(
                {
                    "order_id": str(uuid.uuid4()),
                    "idempotency_key": f"{idempotency_key}:SL",
                    "side": exit_side,
                    "order_type": "STP_SL_PAPER",
                    "limit_price": float(sl_price),
                    "role": "STOP_LOSS",
                }
            )

        for leg in order_legs:
            cur.execute(
                """
                insert into MIP.LIVE.LIVE_ORDERS (
                  ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID, IDEMPOTENCY_KEY, STATUS,
                  SYMBOL, SIDE, ORDER_TYPE, QTY_ORDERED, LIMIT_PRICE,
                  SUBMITTED_AT, ACKNOWLEDGED_AT, LAST_UPDATED_AT, CREATED_AT
                )
                values (
                  %s, %s, %s, %s, %s, 'ACKNOWLEDGED',
                  %s, %s, %s, %s, %s,
                  current_timestamp(), current_timestamp(), current_timestamp(), current_timestamp()
                )
                """,
                (
                    leg["order_id"],
                    action_id,
                    action.get("PORTFOLIO_ID"),
                    account_id,
                    leg["idempotency_key"],
                    action.get("SYMBOL"),
                    leg["side"],
                    leg["order_type"],
                    qty_ordered,
                    leg["limit_price"],
                ),
            )

        cur.execute(
            """
            insert into MIP.LIVE.BROKER_EVENT_LEDGER (
              EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, PROPOSAL_ID, ACTION_ID,
              IDEMPOTENCY_KEY, BROKER_ORDER_ID, SYMBOL, SIDE, QTY, PRICE, PAYLOAD
            )
            values (
              %s, current_timestamp(), 'EXECUTION_REQUESTED', %s, %s, %s,
              %s, %s, %s, %s, %s, %s, parse_json(%s)
            )
            """,
            (
                str(uuid.uuid4()),
                action.get("PORTFOLIO_ID"),
                action.get("PROPOSAL_ID"),
                action_id,
                idempotency_key,
                order_id,
                action.get("SYMBOL"),
                side,
                qty_ordered,
                entry_price,
                json.dumps(
                    {
                        "actor": req.actor,
                        "mode": "PAPER_PLACEHOLDER",
                        "protection_state": (
                            "FULL"
                            if (tp_price is not None and sl_price is not None)
                            else ("PARTIAL" if (tp_price is not None or sl_price is not None) else "NONE")
                        ),
                        "legs": order_legs,
                    }
                ),
            ),
        )

        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set STATUS = 'EXECUTION_REQUESTED',
                   REASON_CODES = parse_json(%s),
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (json.dumps(["EXECUTION_REQUESTED"]), action_id),
        )
        action_after = _fetch_live_action(cur, action_id)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_EXECUTION_REQUESTED",
            status="EXECUTION_REQUESTED",
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "safety_gates_passed": True,
                "idempotency_key": idempotency_key,
                "actor": req.actor,
                "target_open_condition_factor": action.get("TARGET_OPEN_CONDITION_FACTOR"),
                "target_bands": _parse_variant(action.get("TARGET_EXPECTATION_SNAPSHOT")).get("bands"),
                "news_context_state": news_context_state,
                "news_event_shock_flag": news_event_shock_flag,
                "news_freshness_bucket": news_freshness_bucket,
            },
            outcome_state={
                "order_id": order_id,
                "mode": "PAPER_PLACEHOLDER",
                "order_legs": order_legs,
                "target_expectation_snapshot": _parse_variant(action.get("TARGET_EXPECTATION_SNAPSHOT")),
                "news_context_snapshot": news_snapshot,
            },
        )

        return {
            "ok": True,
            "action_id": action_id,
            "status": "EXECUTION_REQUESTED",
            "order_id": order_id,
            "order_ids": [leg["order_id"] for leg in order_legs],
            "order_legs": order_legs,
            "protection_state": (
                "FULL"
                if (tp_price is not None and sl_price is not None)
                else ("PARTIAL" if (tp_price is not None or sl_price is not None) else "NONE")
            ),
            "idempotency_key": idempotency_key,
            "mode": "PAPER_PLACEHOLDER",
        }
    finally:
        conn.close()


@router.get("/trades/orders")
def list_live_orders(
    portfolio_id: int | None = Query(None),
    action_id: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
):
    conn = get_connection()
    try:
        cur = conn.cursor()
        wheres = ["1=1"]
        params: list = []
        if portfolio_id is not None:
            wheres.append("PORTFOLIO_ID = %s")
            params.append(portfolio_id)
        if action_id:
            wheres.append("ACTION_ID = %s")
            params.append(action_id)
        params.append(limit)
        cur.execute(
            f"""
            select
              ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID, IDEMPOTENCY_KEY, BROKER_ORDER_ID,
              STATUS, SYMBOL, SIDE, ORDER_TYPE, QTY_ORDERED, LIMIT_PRICE,
              QTY_FILLED, AVG_FILL_PRICE,
              SUBMITTED_AT, ACKNOWLEDGED_AT, FILLED_AT, LAST_UPDATED_AT, CREATED_AT
            from MIP.LIVE.LIVE_ORDERS
            where {' and '.join(wheres)}
            order by LAST_UPDATED_AT desc, CREATED_AT desc
            limit %s
            """,
            tuple(params),
        )
        rows = fetch_all(cur)
        return {"orders": serialize_rows(rows), "count": len(rows)}
    finally:
        conn.close()


@router.post("/trades/orders/{order_id}/status")
def update_live_order_status(order_id: str, req: UpdateLiveOrderStatusRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select
              ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID, IDEMPOTENCY_KEY, BROKER_ORDER_ID,
              STATUS, SYMBOL, SIDE, QTY_ORDERED, QTY_FILLED, AVG_FILL_PRICE
            from MIP.LIVE.LIVE_ORDERS
            where ORDER_ID = %s
            """,
            (order_id,),
        )
        order_rows = fetch_all(cur)
        if not order_rows:
            raise HTTPException(status_code=404, detail="Order not found.")
        order = order_rows[0]
        target_status = req.status.upper()
        current_status = (order.get("STATUS") or "").upper()
        if current_status == target_status:
            return {"ok": True, "order_id": order_id, "status": target_status, "idempotent_replay": True}

        qty_ordered = float(order.get("QTY_ORDERED") or 0.0)
        existing_qty_filled = float(order.get("QTY_FILLED") or 0.0)
        new_qty_filled = req.qty_filled if req.qty_filled is not None else existing_qty_filled
        if target_status == "PARTIAL_FILL":
            if new_qty_filled <= 0 or (qty_ordered > 0 and new_qty_filled >= qty_ordered):
                raise HTTPException(status_code=400, detail="PARTIAL_FILL requires qty_filled between 0 and qty_ordered.")
        if target_status == "FILLED":
            new_qty_filled = qty_ordered if qty_ordered > 0 else (req.qty_filled or existing_qty_filled)

        cur.execute(
            """
            update MIP.LIVE.LIVE_ORDERS
               set STATUS = %s,
                   BROKER_ORDER_ID = coalesce(%s, BROKER_ORDER_ID),
                   QTY_FILLED = %s,
                   AVG_FILL_PRICE = coalesce(%s, AVG_FILL_PRICE),
                   FILLED_AT = case when %s = 'FILLED' then current_timestamp() else FILLED_AT end,
                   LAST_UPDATED_AT = current_timestamp()
             where ORDER_ID = %s
            """,
            (
                target_status,
                req.broker_order_id,
                new_qty_filled,
                req.avg_fill_price,
                target_status,
                order_id,
            ),
        )

        action_id = order.get("ACTION_ID")
        if action_id:
            action_status = None
            action_reason_codes: list[str] | None = None
            if target_status == "FILLED":
                action_status = "EXECUTED"
                action_reason_codes = ["ORDER_FILLED"]
            elif target_status == "PARTIAL_FILL":
                action_status = "EXECUTION_PARTIAL"
                action_reason_codes = ["ORDER_PARTIAL_FILL"]
            elif target_status == "CANCELED":
                action_status = "EXECUTION_CANCELED"
                action_reason_codes = ["ORDER_CANCELED"]
            elif target_status == "REJECTED":
                action_status = "EXECUTION_REJECTED"
                action_reason_codes = ["ORDER_REJECTED"]

            if action_status:
                cur.execute(
                    """
                    update MIP.LIVE.LIVE_ACTIONS
                       set STATUS = %s,
                           REASON_CODES = parse_json(%s),
                           UPDATED_AT = current_timestamp()
                     where ACTION_ID = %s
                    """,
                    (action_status, json.dumps(action_reason_codes), action_id),
                )

        cur.execute(
            """
            insert into MIP.LIVE.BROKER_EVENT_LEDGER (
              EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, ACTION_ID,
              IDEMPOTENCY_KEY, BROKER_ORDER_ID, SYMBOL, SIDE, QTY, PRICE, PAYLOAD
            )
            values (
              %s, current_timestamp(), %s, %s, %s,
              %s, %s, %s, %s, %s, %s, parse_json(%s)
            )
            """,
            (
                str(uuid.uuid4()),
                f"ORDER_{target_status}",
                order.get("PORTFOLIO_ID"),
                action_id,
                order.get("IDEMPOTENCY_KEY"),
                req.broker_order_id or order.get("BROKER_ORDER_ID"),
                order.get("SYMBOL"),
                order.get("SIDE"),
                new_qty_filled if target_status in ("PARTIAL_FILL", "FILLED") else (order.get("QTY_ORDERED") or 0.0),
                req.avg_fill_price if req.avg_fill_price is not None else order.get("AVG_FILL_PRICE"),
                json.dumps({"actor": req.actor, "notes": req.notes}),
            ),
        )

        if action_id:
            action_after = _fetch_live_action(cur, action_id)
            _append_learning_ledger_event(
                cur,
                event_name="LIVE_ORDER_STATUS_UPDATE",
                status=target_status,
                action_before=None,
                action_after=action_after,
                policy_version=LIVE_POLICY_VERSION,
                influence_delta={
                    "order_id": order_id,
                    "from_status": current_status,
                    "to_status": target_status,
                    "qty_filled": new_qty_filled,
                    "news_context_state": _parse_variant(action_after.get("NEWS_CONTEXT_SNAPSHOT")).get("context_state"),
                    "news_influenced_block": "NEWS_EXECUTION_BLOCKED_EVENT_SHOCK" in (_parse_list_variant(action_after.get("REASON_CODES")) if action_after else []),
                },
                outcome_state={
                    "actor": req.actor,
                    "broker_order_id": req.broker_order_id or order.get("BROKER_ORDER_ID"),
                    "notes": req.notes,
                    "news_context_snapshot": _parse_variant(action_after.get("NEWS_CONTEXT_SNAPSHOT")) if action_after else {},
                },
            )

        return {
            "ok": True,
            "order_id": order_id,
            "status": target_status,
            "qty_filled": new_qty_filled,
            "avg_fill_price": req.avg_fill_price if req.avg_fill_price is not None else order.get("AVG_FILL_PRICE"),
        }
    finally:
        conn.close()


@router.post("/trades/smoke/paper-workflow")
def run_paper_workflow_smoke(req: SimulatePaperWorkflowRequest):
    """
    One-click paper workflow simulation:
    import -> PM accept -> compliance approve -> revalidate -> execute -> order status progression.
    """
    steps: list[dict] = []

    source_portfolio_id = req.source_portfolio_id
    if source_portfolio_id is None:
        conn = get_connection()
        try:
            cur = conn.cursor()
            source_wheres = [
                "STATUS in ('PROPOSED', 'APPROVED')",
                "SYMBOL is not null",
                "SIDE in ('BUY', 'SELL')",
            ]
            source_params = []
            if req.run_id:
                source_wheres.append("RUN_ID_VARCHAR = %s")
                source_params.append(req.run_id)
            source_params.append(req.limit)
            cur.execute(
                f"""
                select PORTFOLIO_ID
                from MIP.AGENT_OUT.ORDER_PROPOSALS
                where {' and '.join(source_wheres)}
                order by PROPOSED_AT desc
                limit %s
                """,
                tuple(source_params),
            )
            proposal_rows = fetch_all(cur)
            if proposal_rows:
                source_portfolio_id = int((proposal_rows[0] or {}).get("PORTFOLIO_ID"))
        finally:
            conn.close()

    if source_portfolio_id is None:
        raise HTTPException(
            status_code=409,
            detail="No source portfolio could be inferred for paper workflow smoke. Provide source_portfolio_id.",
        )

    import_result = import_live_actions_from_proposals(
        ImportLiveActionsFromProposalsRequest(
            live_portfolio_id=req.live_portfolio_id,
            source_portfolio_id=source_portfolio_id,
            run_id=req.run_id,
            limit=req.limit,
        )
    )
    steps.append({"step": "import_proposals", "result": import_result})

    action_id = None
    imported_action_ids = import_result.get("imported_action_ids") or []
    if imported_action_ids:
        action_id = imported_action_ids[0]
    else:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                select ACTION_ID
                from MIP.LIVE.LIVE_ACTIONS
                where PORTFOLIO_ID = %s
                  and STATUS in ('RESEARCH_IMPORTED', 'PROPOSED')
                order by CREATED_AT desc
                limit 1
                """,
                (req.live_portfolio_id,),
            )
            rows = fetch_all(cur)
            action_id = rows[0].get("ACTION_ID") if rows else None
        finally:
            conn.close()
    if not action_id:
        raise HTTPException(
            status_code=409,
            detail="No importable or pending action found for workflow smoke.",
        )

    committee_result = run_live_trade_committee(action_id, CommitteeRunRequest(actor="smoke_committee"))
    steps.append({"step": "committee_run", "result": committee_result})

    pm_result = pm_accept_live_action(action_id, PmAcceptRequest(actor="smoke_pm"))
    steps.append({"step": "pm_accept", "result": pm_result})

    compliance_result = compliance_decide_live_action(
        action_id,
        ComplianceDecisionRequest(actor="smoke_compliance", decision="APPROVE"),
    )
    steps.append({"step": "compliance_approve", "result": compliance_result})

    intent_submit_result = submit_live_trade_intent(
        action_id,
        IntentSubmitRequest(actor="smoke_intent_submit", reference_id="SMOKE_INTENT"),
    )
    steps.append({"step": "intent_submit", "result": intent_submit_result})

    intent_approve_result = approve_live_trade_intent(
        action_id,
        IntentApproveRequest(actor="smoke_intent_approver"),
    )
    steps.append({"step": "intent_approve", "result": intent_approve_result})

    revalidate_result = revalidate_live_action(action_id)
    steps.append({"step": "revalidate", "result": revalidate_result})

    execute_result = execute_live_action(
        action_id,
        ExecuteLiveActionRequest(actor="smoke_execution", attempt_n=1),
    )
    steps.append({"step": "execute", "result": execute_result})

    order_id = execute_result.get("order_id")
    if order_id:
        if req.scenario == "PARTIAL_THEN_FILL":
            partial_result = update_live_order_status(
                order_id,
                UpdateLiveOrderStatusRequest(
                    actor="smoke_broker",
                    status="PARTIAL_FILL",
                    qty_filled=1,
                    notes="smoke partial fill",
                ),
            )
            steps.append({"step": "order_partial_fill", "result": partial_result})
            filled_result = update_live_order_status(
                order_id,
                UpdateLiveOrderStatusRequest(
                    actor="smoke_broker",
                    status="FILLED",
                    notes="smoke fill",
                ),
            )
            steps.append({"step": "order_filled", "result": filled_result})
        elif req.scenario == "CANCEL":
            canceled_result = update_live_order_status(
                order_id,
                UpdateLiveOrderStatusRequest(
                    actor="smoke_broker",
                    status="CANCELED",
                    notes="smoke cancel",
                ),
            )
            steps.append({"step": "order_canceled", "result": canceled_result})
        elif req.scenario == "REJECT":
            rejected_result = update_live_order_status(
                order_id,
                UpdateLiveOrderStatusRequest(
                    actor="smoke_broker",
                    status="REJECTED",
                    notes="smoke reject",
                ),
            )
            steps.append({"step": "order_rejected", "result": rejected_result})

    return {
        "ok": True,
        "scenario": req.scenario,
        "live_portfolio_id": req.live_portfolio_id,
        "action_id": action_id,
        "order_id": order_id,
        "steps": steps,
    }


@router.post("/trades/rebuild-state")
def rebuild_live_action_state(req: RebuildLiveStateRequest):
    """
    Restart-safe rebuild of LIVE_ACTIONS status from persisted LIVE_ORDERS.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        wheres = ["1=1"]
        params: list = []
        if req.portfolio_id is not None:
            wheres.append("a.PORTFOLIO_ID = %s")
            params.append(req.portfolio_id)

        cur.execute(
            f"""
            select
              a.ACTION_ID,
              a.PORTFOLIO_ID,
              a.STATUS as ACTION_STATUS,
              count(o.ORDER_ID) as ORDER_COUNT,
              count_if(o.STATUS in ('SUBMITTED', 'ACKNOWLEDGED')) as OPEN_COUNT,
              count_if(o.STATUS = 'PARTIAL_FILL') as PARTIAL_COUNT,
              count_if(o.STATUS = 'FILLED') as FILLED_COUNT,
              count_if(o.STATUS = 'CANCELED') as CANCELED_COUNT,
              count_if(o.STATUS = 'REJECTED') as REJECTED_COUNT,
              max(o.LAST_UPDATED_AT) as LAST_ORDER_UPDATED_AT
            from MIP.LIVE.LIVE_ACTIONS a
            left join MIP.LIVE.LIVE_ORDERS o
              on o.ACTION_ID = a.ACTION_ID
            where {' and '.join(wheres)}
            group by a.ACTION_ID, a.PORTFOLIO_ID, a.STATUS
            having count(o.ORDER_ID) > 0
            order by LAST_ORDER_UPDATED_AT desc nulls last
            """,
            tuple(params),
        )
        rows = fetch_all(cur)

        changes: list[dict] = []
        inspected = 0
        for r in rows:
            inspected += 1
            current_status = (r.get("ACTION_STATUS") or "").upper()
            target_status = None
            reason_codes = None
            if (r.get("FILLED_COUNT") or 0) > 0:
                target_status = "EXECUTED"
                reason_codes = ["ORDER_FILLED_REBUILT"]
            elif (r.get("PARTIAL_COUNT") or 0) > 0:
                target_status = "EXECUTION_PARTIAL"
                reason_codes = ["ORDER_PARTIAL_FILL_REBUILT"]
            elif (r.get("OPEN_COUNT") or 0) > 0:
                target_status = "EXECUTION_REQUESTED"
                reason_codes = ["ORDER_OPEN_REBUILT"]
            elif (r.get("CANCELED_COUNT") or 0) > 0:
                target_status = "EXECUTION_CANCELED"
                reason_codes = ["ORDER_CANCELED_REBUILT"]
            elif (r.get("REJECTED_COUNT") or 0) > 0:
                target_status = "EXECUTION_REJECTED"
                reason_codes = ["ORDER_REJECTED_REBUILT"]

            if target_status and target_status != current_status:
                changes.append(
                    {
                        "action_id": r.get("ACTION_ID"),
                        "portfolio_id": r.get("PORTFOLIO_ID"),
                        "from_status": current_status,
                        "to_status": target_status,
                        "reason_codes": reason_codes,
                    }
                )
                if not req.dry_run:
                    cur.execute(
                        """
                        update MIP.LIVE.LIVE_ACTIONS
                           set STATUS = %s,
                               REASON_CODES = parse_json(%s),
                               UPDATED_AT = current_timestamp()
                         where ACTION_ID = %s
                        """,
                        (target_status, json.dumps(reason_codes), r.get("ACTION_ID")),
                    )

        cur.execute(
            """
            select
              a.ACTION_ID,
              a.PORTFOLIO_ID,
              a.STATUS as ACTION_STATUS
            from MIP.LIVE.LIVE_ACTIONS a
            where (%s is null or a.PORTFOLIO_ID = %s)
              and a.STATUS in ('EXECUTION_REQUESTED', 'EXECUTION_PARTIAL')
              and not exists (
                select 1
                from MIP.LIVE.LIVE_ORDERS o
                where o.ACTION_ID = a.ACTION_ID
              )
            order by a.UPDATED_AT desc
            limit 500
            """,
            (req.portfolio_id, req.portfolio_id),
        )
        orphan_rows = fetch_all(cur)
        for r in orphan_rows:
            inspected += 1
            target_status = "REVALIDATED_PASS"
            reason_codes = ["REBUILD_MISSING_ORDER_RESET"]
            changes.append(
                {
                    "action_id": r.get("ACTION_ID"),
                    "portfolio_id": r.get("PORTFOLIO_ID"),
                    "from_status": (r.get("ACTION_STATUS") or "").upper(),
                    "to_status": target_status,
                    "reason_codes": reason_codes,
                }
            )
            if not req.dry_run:
                cur.execute(
                    """
                    update MIP.LIVE.LIVE_ACTIONS
                       set STATUS = %s,
                           REASON_CODES = parse_json(%s),
                           UPDATED_AT = current_timestamp()
                     where ACTION_ID = %s
                    """,
                    (target_status, json.dumps(reason_codes), r.get("ACTION_ID")),
                )

        _append_learning_ledger_event(
            cur,
            event_name="LIVE_REBUILD_STATE",
            status="DRY_RUN" if req.dry_run else "SUCCESS",
            action_before=None,
            action_after={"PORTFOLIO_ID": req.portfolio_id},
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "inspected_actions": inspected,
                "changed_actions": len(changes),
                "dry_run": req.dry_run,
            },
            outcome_state={
                "actor": req.actor,
                "sample_changes": changes[:25],
            },
        )

        return {
            "ok": True,
            "portfolio_id_filter": req.portfolio_id,
            "dry_run": req.dry_run,
            "inspected_actions": inspected,
            "changed_actions": len(changes),
            "changes": changes[:200],
        }
    finally:
        conn.close()


@router.get("/portfolio-config")
def list_live_portfolio_configs():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select
              PORTFOLIO_ID, SIM_PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS,
              DRIFT_STATUS, CONFIG_VERSION, IS_ACTIVE, CREATED_AT, UPDATED_AT
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            order by PORTFOLIO_ID
            """
        )
        rows = fetch_all(cur)
        return {"configs": serialize_rows(rows), "count": len(rows)}
    finally:
        conn.close()


@router.get("/portfolio-config/{portfolio_id}")
def get_live_portfolio_config(portfolio_id: int):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select
              PORTFOLIO_ID, SIM_PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS,
              DRIFT_STATUS, CONFIG_VERSION, IS_ACTIVE, CREATED_AT, UPDATED_AT
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            """,
            (portfolio_id,),
        )
        rows = fetch_all(cur)
        if not rows:
            raise HTTPException(status_code=404, detail="Live portfolio config not found.")
        return {"config": serialize_row(rows[0])}
    finally:
        conn.close()


@router.put("/portfolio-config/{portfolio_id}")
def upsert_live_portfolio_config(portfolio_id: int, req: LivePortfolioConfigUpsertRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "select PORTFOLIO_ID, IBKR_ACCOUNT_ID, CONFIG_VERSION from MIP.LIVE.LIVE_PORTFOLIO_CONFIG where PORTFOLIO_ID = %s",
            (portfolio_id,),
        )
        existing = cur.fetchone()

        if existing:
            cur.execute(
                """
                update MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                   set SIM_PORTFOLIO_ID = coalesce(%s, SIM_PORTFOLIO_ID),
                       IBKR_ACCOUNT_ID = coalesce(%s, IBKR_ACCOUNT_ID),
                       ADAPTER_MODE = coalesce(%s, ADAPTER_MODE),
                       BASE_CURRENCY = coalesce(%s, BASE_CURRENCY),
                       MAX_POSITIONS = coalesce(%s, MAX_POSITIONS),
                       MAX_POSITION_PCT = coalesce(%s, MAX_POSITION_PCT),
                       CASH_BUFFER_PCT = coalesce(%s, CASH_BUFFER_PCT),
                       MAX_SLIPPAGE_PCT = coalesce(%s, MAX_SLIPPAGE_PCT),
                       VALIDITY_WINDOW_SEC = coalesce(%s, VALIDITY_WINDOW_SEC),
                       QUOTE_FRESHNESS_THRESHOLD_SEC = coalesce(%s, QUOTE_FRESHNESS_THRESHOLD_SEC),
                       SNAPSHOT_FRESHNESS_THRESHOLD_SEC = coalesce(%s, SNAPSHOT_FRESHNESS_THRESHOLD_SEC),
                       DRAWDOWN_STOP_PCT = coalesce(%s, DRAWDOWN_STOP_PCT),
                       BUST_PCT = coalesce(%s, BUST_PCT),
                       COOLDOWN_BARS = coalesce(%s, COOLDOWN_BARS),
                       IS_ACTIVE = coalesce(%s, IS_ACTIVE),
                       CONFIG_VERSION = coalesce(CONFIG_VERSION, 1) + 1,
                       UPDATED_AT = current_timestamp()
                 where PORTFOLIO_ID = %s
                """,
                (
                    req.sim_portfolio_id,
                    req.ibkr_account_id,
                    req.adapter_mode,
                    req.base_currency.upper() if req.base_currency else None,
                    req.max_positions,
                    req.max_position_pct,
                    req.cash_buffer_pct,
                    req.max_slippage_pct,
                    req.validity_window_sec,
                    req.quote_freshness_threshold_sec,
                    req.snapshot_freshness_threshold_sec,
                    req.drawdown_stop_pct,
                    req.bust_pct,
                    req.cooldown_bars,
                    req.is_active,
                    portfolio_id,
                ),
            )
        else:
            if not req.ibkr_account_id:
                raise HTTPException(status_code=400, detail="ibkr_account_id is required when creating config.")
            cur.execute(
                """
                insert into MIP.LIVE.LIVE_PORTFOLIO_CONFIG (
                  PORTFOLIO_ID, SIM_PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
                  MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
                  VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
                  DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS, IS_ACTIVE, CONFIG_VERSION,
                  CREATED_AT, UPDATED_AT
                )
                values (
                  %s, %s, %s, coalesce(%s, 'PAPER'), coalesce(%s, 'EUR'),
                  %s, %s, %s, %s,
                  coalesce(%s, 14400), coalesce(%s, 60), coalesce(%s, 300),
                  %s, %s, coalesce(%s, 3), coalesce(%s, true), 1,
                  current_timestamp(), current_timestamp()
                )
                """,
                (
                    portfolio_id,
                    req.sim_portfolio_id,
                    req.ibkr_account_id,
                    req.adapter_mode,
                    req.base_currency.upper() if req.base_currency else None,
                    req.max_positions,
                    req.max_position_pct,
                    req.cash_buffer_pct,
                    req.max_slippage_pct,
                    req.validity_window_sec,
                    req.quote_freshness_threshold_sec,
                    req.snapshot_freshness_threshold_sec,
                    req.drawdown_stop_pct,
                    req.bust_pct,
                    req.cooldown_bars,
                    req.is_active,
                ),
            )

        cur.execute(
            """
            select
              PORTFOLIO_ID, SIM_PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS,
              DRIFT_STATUS, CONFIG_VERSION, IS_ACTIVE, CREATED_AT, UPDATED_AT
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            """,
            (portfolio_id,),
        )
        rows = fetch_all(cur)
        return {"ok": True, "config": serialize_row(rows[0]) if rows else None}
    finally:
        conn.close()


@router.post("/portfolio-config")
def create_live_portfolio_config(req: LivePortfolioConfigUpsertRequest):
    """
    Create a new live config with server-owned portfolio_id allocation.
    """
    if not req.ibkr_account_id:
        raise HTTPException(status_code=400, detail="ibkr_account_id is required when creating config.")

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("select coalesce(max(PORTFOLIO_ID), 0) + 1 as NEXT_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG")
        next_id_row = fetch_all(cur)
        next_id = int((next_id_row[0] or {}).get("NEXT_ID") or 1)

        cur.execute(
            """
            insert into MIP.LIVE.LIVE_PORTFOLIO_CONFIG (
              PORTFOLIO_ID, SIM_PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS, IS_ACTIVE, CONFIG_VERSION,
              CREATED_AT, UPDATED_AT
            )
            values (
              %s, %s, %s, coalesce(%s, 'PAPER'), coalesce(%s, 'EUR'),
              %s, %s, %s, %s,
              coalesce(%s, 14400), coalesce(%s, 60), coalesce(%s, 300),
              %s, %s, coalesce(%s, 3), coalesce(%s, true), 1,
              current_timestamp(), current_timestamp()
            )
            """,
            (
                next_id,
                req.sim_portfolio_id,
                req.ibkr_account_id,
                req.adapter_mode,
                req.base_currency.upper() if req.base_currency else None,
                req.max_positions,
                req.max_position_pct,
                req.cash_buffer_pct,
                req.max_slippage_pct,
                req.validity_window_sec,
                req.quote_freshness_threshold_sec,
                req.snapshot_freshness_threshold_sec,
                req.drawdown_stop_pct,
                req.bust_pct,
                req.cooldown_bars,
                req.is_active,
            ),
        )

        cur.execute(
            """
            select
              PORTFOLIO_ID, SIM_PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS,
              DRIFT_STATUS, CONFIG_VERSION, IS_ACTIVE, CREATED_AT, UPDATED_AT
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            """,
            (next_id,),
        )
        rows = fetch_all(cur)
        return {"ok": True, "portfolio_id": next_id, "config": serialize_row(rows[0]) if rows else None}
    finally:
        conn.close()


@router.delete("/portfolio-config/{portfolio_id}")
def delete_live_portfolio_config(
    portfolio_id: int,
    force: bool = Query(False, description="Force delete even when dependent live rows exist."),
):
    # Use agent runtime role (CURSOR_AGENT / MIP_ADMIN_ROLE) because API runtime
    # role can be read-only for LIVE schema writes.
    existing_rows = _run_agent_snowflake_query(
        f"select PORTFOLIO_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG where PORTFOLIO_ID = {int(portfolio_id)}"
    )
    if not isinstance(existing_rows, list) or not existing_rows:
        raise HTTPException(status_code=404, detail="Live portfolio config not found.")

    action_rows = _run_agent_snowflake_query(
        f"select count(*) as CNT from MIP.LIVE.LIVE_ACTIONS where PORTFOLIO_ID = {int(portfolio_id)}"
    )
    actions_count = int(((action_rows[0] if isinstance(action_rows, list) and action_rows else {}) or {}).get("CNT") or 0)

    order_rows = _run_agent_snowflake_query(
        f"select count(*) as CNT from MIP.LIVE.LIVE_ORDERS where PORTFOLIO_ID = {int(portfolio_id)}"
    )
    orders_count = int(((order_rows[0] if isinstance(order_rows, list) and order_rows else {}) or {}).get("CNT") or 0)

    if not force and (actions_count > 0 or orders_count > 0):
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Delete blocked: linked live actions/orders exist for this portfolio.",
                "portfolio_id": portfolio_id,
                "actions_count": actions_count,
                "orders_count": orders_count,
                "hint": "Use force=true only if you intentionally want to remove config despite historical linkage.",
            },
        )

    _run_agent_snowflake_query(
        f"delete from MIP.LIVE.LIVE_PORTFOLIO_CONFIG where PORTFOLIO_ID = {int(portfolio_id)}"
    )
    return {
        "ok": True,
        "deleted_portfolio_id": portfolio_id,
        "forced": force,
        "actions_count": actions_count,
        "orders_count": orders_count,
    }


@router.post("/trades/actions/import-proposals")
def import_live_actions_from_proposals(req: ImportLiveActionsFromProposalsRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            select coalesce(VALIDITY_WINDOW_SEC, 14400) as VALIDITY_WINDOW_SEC
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
              and coalesce(IS_ACTIVE, true) = true
            """,
            (req.live_portfolio_id,),
        )
        cfg = cur.fetchone()
        if not cfg:
            raise HTTPException(
                status_code=400,
                detail="Live portfolio config not found or inactive.",
            )
        (validity_window_sec,) = cfg
        source_portfolio_id = int(req.source_portfolio_id) if req.source_portfolio_id is not None else None
        source_origin = "request" if source_portfolio_id is not None else "all_portfolios"

        wheres = [
            "STATUS in ('PROPOSED', 'APPROVED')",
            "SYMBOL is not null",
            "SIDE in ('BUY', 'SELL')",
        ]
        params = []
        if source_portfolio_id is not None:
            wheres.append("PORTFOLIO_ID = %s")
            params.append(source_portfolio_id)
        scope = "all_active"
        latest_batch_date = None
        if req.run_id:
            wheres.append("RUN_ID_VARCHAR = %s")
            params.append(req.run_id)
            scope = "run_id"
        elif req.latest_batch_only:
            cur.execute(
                f"""
                select max(to_date(PROPOSED_AT)) as LATEST_DAY
                from MIP.AGENT_OUT.ORDER_PROPOSALS
                where {' and '.join(wheres)}
                """,
                tuple(params),
            )
            row = cur.fetchone()
            latest_batch_date = row[0] if row else None
            if latest_batch_date is not None:
                wheres.append("to_date(PROPOSED_AT) = %s")
                params.append(latest_batch_date)
                scope = "latest_batch_day"
        params.append(req.limit)

        cur.execute(
            f"""
            select
              PROPOSAL_ID, PORTFOLIO_ID, RUN_ID_VARCHAR, SYMBOL, MARKET_TYPE, SIDE, TARGET_WEIGHT,
              STATUS, SIGNAL_PATTERN_ID, RECOMMENDATION_ID, PROPOSED_AT, SOURCE_SIGNALS, RATIONALE
            from MIP.AGENT_OUT.ORDER_PROPOSALS
            where {' and '.join(wheres)}
            order by PROPOSED_AT desc
            limit %s
            """,
            tuple(params),
        )
        proposals = fetch_all(cur)
        if req.max_proposal_age_days:
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(req.max_proposal_age_days))
            proposals = [
                p
                for p in proposals
                if (p.get("PROPOSED_AT") is not None and p.get("PROPOSED_AT").replace(tzinfo=timezone.utc) >= cutoff)
            ]
        skipped_duplicate_symbol = 0
        if req.dedupe_by_symbol:
            seen_symbols: set[str] = set()
            deduped: list[dict] = []
            for p in proposals:
                sym = (p.get("SYMBOL") or "").upper().strip()
                if not sym:
                    continue
                if sym in seen_symbols:
                    skipped_duplicate_symbol += 1
                    continue
                seen_symbols.add(sym)
                deduped.append(p)
            proposals = deduped

        imported = 0
        skipped_existing = 0
        skipped_invalid = 0
        skipped_symbol_already_queued = 0
        imported_action_ids: list[str] = []
        source_portfolios: set[int] = set()
        distinct_symbols: set[str] = set()

        for p in proposals:
            proposal_id = p.get("PROPOSAL_ID")
            if proposal_id is None:
                skipped_invalid += 1
                continue
            if p.get("PORTFOLIO_ID") is not None:
                source_portfolios.add(int(p.get("PORTFOLIO_ID")))
            if p.get("SYMBOL"):
                distinct_symbols.add((p.get("SYMBOL") or "").upper())

            cur.execute(
                """
                select ACTION_ID
                from MIP.LIVE.LIVE_ACTIONS
                where PORTFOLIO_ID = %s
                  and PROPOSAL_ID = %s
                limit 1
                """,
                (req.live_portfolio_id, proposal_id),
            )
            if cur.fetchone():
                skipped_existing += 1
                continue
            cur.execute(
                """
                select ACTION_ID
                from MIP.LIVE.LIVE_ACTIONS
                where PORTFOLIO_ID = %s
                  and upper(coalesce(SYMBOL, '')) = %s
                  and STATUS in (
                    'PENDING_OPEN_VALIDATION','OPEN_CAUTION','OPEN_ELIGIBLE','PENDING_OPEN_STABILITY_REVIEW',
                    'READY_FOR_APPROVAL_FLOW','PM_ACCEPTED','COMPLIANCE_APPROVED','INTENT_SUBMITTED',
                    'INTENT_APPROVED','REVALIDATED_PASS','REVALIDATED_FAIL','EXECUTION_REQUESTED','EXECUTION_PARTIAL'
                  )
                limit 1
                """,
                (req.live_portfolio_id, (p.get("SYMBOL") or "").upper()),
            )
            if cur.fetchone():
                skipped_symbol_already_queued += 1
                continue

            action_id = str(uuid.uuid4())
            training_snapshot = _build_training_qualification_snapshot(
                cur,
                symbol=(p.get("SYMBOL") or "").upper(),
                market_type=p.get("MARKET_TYPE"),
                pattern_id=p.get("SIGNAL_PATTERN_ID"),
                interval_minutes=1440,
                target_weight=p.get("TARGET_WEIGHT"),
            )
            target_snapshot = _build_target_expectation_snapshot(
                cur,
                symbol=(p.get("SYMBOL") or "").upper(),
                market_type=p.get("MARKET_TYPE"),
                pattern_id=p.get("SIGNAL_PATTERN_ID"),
                interval_minutes=1440,
                open_condition_factor=1.0,
            )
            news_snapshot = _normalize_news_context_snapshot(
                p.get("SOURCE_SIGNALS"),
                p.get("RATIONALE"),
                proposal_ts=p.get("PROPOSED_AT"),
            )
            snapshot_payload = json.dumps(
                {
                    "source": "ORDER_PROPOSALS",
                    "source_portfolio_id": source_portfolio_id,
                    "source_origin": source_origin,
                    "run_id": p.get("RUN_ID_VARCHAR"),
                    "proposal_status": p.get("STATUS"),
                    "signal_pattern_id": p.get("SIGNAL_PATTERN_ID"),
                    "recommendation_id": p.get("RECOMMENDATION_ID"),
                    "target_weight": p.get("TARGET_WEIGHT"),
                    "proposed_at": str(p.get("PROPOSED_AT")) if p.get("PROPOSED_AT") is not None else None,
                    "training_qualification": training_snapshot,
                    "target_expectation": target_snapshot,
                    "news_context": news_snapshot,
                }
            )

            import_reason_codes = [
                "RESEARCH_IMPORTED",
                "PENDING_OPENING_VALIDATION",
                "NON_EXECUTABLE_UNTIL_OPENING_SANITY_STABILITY_COMMITTEE_PM_COMPLIANCE_INTENT_REVALIDATION",
            ]
            cur.execute(
                """
                insert into MIP.LIVE.LIVE_ACTIONS (
                  ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, SIDE, PROPOSED_QTY, ASSET_CLASS,
                  STATUS, VALIDITY_WINDOW_END, COMPLIANCE_STATUS, PARAM_SNAPSHOT, REASON_CODES,
                  TRAINING_QUALIFICATION_SNAPSHOT, TRAINING_LIVE_ELIGIBLE, TRAINING_RANK_IMPACT, TRAINING_SIZE_CAP_FACTOR,
                  TARGET_EXPECTATION_SNAPSHOT, TARGET_OPEN_CONDITION_FACTOR, TARGET_EXPECTATION_POLICY_VERSION,
                  NEWS_CONTEXT_SNAPSHOT, NEWS_CONTEXT_STATE, NEWS_EVENT_SHOCK_FLAG, NEWS_FRESHNESS_BUCKET, NEWS_CONTEXT_POLICY_VERSION,
                  COMMITTEE_REQUIRED, COMMITTEE_STATUS,
                  CREATED_AT, UPDATED_AT
                )
                select
                  %s, %s, %s, %s, %s, %s, %s,
                  'PENDING_OPEN_VALIDATION', dateadd(second, %s, current_timestamp()), 'PENDING', parse_json(%s), parse_json(%s),
                  parse_json(%s), %s, %s, %s,
                  parse_json(%s), %s, %s,
                  parse_json(%s), %s, %s, %s, %s,
                  true, 'PENDING',
                  current_timestamp(), current_timestamp()
                """,
                (
                    action_id,
                    proposal_id,
                    req.live_portfolio_id,
                    (p.get("SYMBOL") or "").upper(),
                    (p.get("SIDE") or "").upper(),
                    None,
                    p.get("MARKET_TYPE"),
                    int(validity_window_sec) if validity_window_sec is not None else 14400,
                    snapshot_payload,
                    json.dumps(import_reason_codes),
                    json.dumps(training_snapshot),
                    bool(training_snapshot.get("live_eligible")),
                    training_snapshot.get("rank_impact"),
                    float(training_snapshot.get("size_cap_factor") or 0.0),
                    json.dumps(target_snapshot),
                    float(target_snapshot.get("open_condition_factor") or 1.0),
                    target_snapshot.get("policy_version"),
                    json.dumps(news_snapshot),
                    news_snapshot.get("context_state"),
                    bool(news_snapshot.get("event_shock_flag")),
                    news_snapshot.get("freshness_bucket"),
                    news_snapshot.get("policy_version"),
                ),
            )
            _append_learning_ledger_event(
                cur,
                event_name="LIVE_RESEARCH_IMPORT",
                status="PENDING_OPEN_VALIDATION",
                action_before=None,
                action_after={
                    "ACTION_ID": action_id,
                    "PROPOSAL_ID": proposal_id,
                    "PORTFOLIO_ID": req.live_portfolio_id,
                    "SYMBOL": (p.get("SYMBOL") or "").upper(),
                    "SIDE": (p.get("SIDE") or "").upper(),
                    "ASSET_CLASS": p.get("MARKET_TYPE"),
                    "RUN_ID_VARCHAR": p.get("RUN_ID_VARCHAR"),
                },
                influence_delta={
                    "default_executable": False,
                    "required_sequence": [
                        "OPENING_SANITY_GATE",
                        "OPENING_STABILITY_REVIEW",
                        "COMMITTEE_COMPLETED",
                        "PM_ACCEPTED",
                        "COMPLIANCE_APPROVED",
                        "INTENT_SUBMITTED",
                        "INTENT_APPROVED",
                        "REVALIDATED_PASS",
                        "EXECUTION_REQUESTED",
                    ],
                    "proposal_status_source": p.get("STATUS"),
                    "training_live_eligible": training_snapshot.get("live_eligible"),
                    "training_rank_impact": training_snapshot.get("rank_impact"),
                    "training_size_cap_factor": training_snapshot.get("size_cap_factor"),
                    "training_maturity_stage": training_snapshot.get("maturity_stage"),
                    "training_trusted_level": training_snapshot.get("trusted_level"),
                    "target_bands": _parse_variant(target_snapshot).get("bands"),
                    "target_open_condition_factor": target_snapshot.get("open_condition_factor"),
                    "news_context_state": news_snapshot.get("context_state"),
                    "news_event_shock_flag": bool(news_snapshot.get("event_shock_flag")),
                    "news_freshness_bucket": news_snapshot.get("freshness_bucket"),
                },
                policy_version=LIVE_POLICY_VERSION,
                outcome_state={
                    "import_source": "ORDER_PROPOSALS",
                    "reason_codes": import_reason_codes,
                    "training_qualification_snapshot": training_snapshot,
                    "target_expectation_snapshot": target_snapshot,
                    "news_context_snapshot": news_snapshot,
                },
            )
            imported += 1
            imported_action_ids.append(action_id)

        return {
            "ok": True,
            "live_portfolio_id": req.live_portfolio_id,
            "source_portfolio_id": source_portfolio_id,
            "source_origin": source_origin,
            "source_scope": scope,
            "latest_batch_date": str(latest_batch_date) if latest_batch_date is not None else None,
            "source_portfolio_ids": sorted(list(source_portfolios)),
            "distinct_symbol_count": len(distinct_symbols),
            "run_id_filter": req.run_id,
            "candidate_count": len(proposals),
            "imported_count": imported,
            "skipped_existing_count": skipped_existing,
            "skipped_invalid_count": skipped_invalid,
            "skipped_duplicate_symbol_count": skipped_duplicate_symbol,
            "skipped_symbol_already_queued_count": skipped_symbol_already_queued,
            "imported_action_ids": imported_action_ids[:50],
        }
    finally:
        conn.close()


@router.get("/trades/proposal-candidates")
def list_live_proposal_candidates(
    live_portfolio_id: int | None = Query(default=None),
    source_portfolio_id: int | None = Query(default=None),
    run_id: str | None = Query(default=None),
    latest_batch_only: bool = Query(default=True),
    dedupe_by_symbol: bool = Query(default=True),
    max_proposal_age_days: int = Query(default=7, ge=1, le=180),
    limit: int = Query(default=100, ge=1, le=1000),
):
    conn = get_connection()
    try:
        cur = conn.cursor()
        wheres = [
            "op.STATUS in ('PROPOSED', 'APPROVED')",
            "op.SYMBOL is not null",
            "op.SIDE in ('BUY', 'SELL')",
        ]
        params: list[object] = []
        if source_portfolio_id is not None:
            wheres.append("op.PORTFOLIO_ID = %s")
            params.append(int(source_portfolio_id))

        scope = "all_active"
        latest_batch_date = None
        if run_id:
            wheres.append("op.RUN_ID_VARCHAR = %s")
            params.append(run_id)
            scope = "run_id"
        elif latest_batch_only:
            cur.execute(
                f"""
                select max(to_date(op.PROPOSED_AT)) as LATEST_DAY
                from MIP.AGENT_OUT.ORDER_PROPOSALS op
                where {' and '.join(wheres)}
                """,
                tuple(params),
            )
            row = cur.fetchone()
            latest_batch_date = row[0] if row else None
            if latest_batch_date is not None:
                wheres.append("to_date(op.PROPOSED_AT) = %s")
                params.append(latest_batch_date)
                scope = "latest_batch_day"

        queued_filter_sql = ""
        query_params: list[object] = []
        if live_portfolio_id is not None:
            queued_filter_sql = "and la.PORTFOLIO_ID = %s"
            query_params.append(int(live_portfolio_id))
        query_params.extend(params)
        query_params.append(limit)
        cur.execute(
            f"""
            select
              op.PROPOSAL_ID,
              op.PORTFOLIO_ID,
              op.RUN_ID_VARCHAR,
              op.SYMBOL,
              op.MARKET_TYPE,
              op.SIDE,
              op.TARGET_WEIGHT,
              op.STATUS,
              op.PROPOSED_AT,
              exists(
                select 1
                from MIP.LIVE.LIVE_ACTIONS la
                where la.PROPOSAL_ID = op.PROPOSAL_ID
                  {queued_filter_sql}
              ) as ALREADY_QUEUED
            from MIP.AGENT_OUT.ORDER_PROPOSALS op
            where {' and '.join(wheres)}
            order by op.PROPOSED_AT desc
            limit %s
            """,
            tuple(query_params),
        )
        rows = fetch_all(cur)
        if max_proposal_age_days:
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(max_proposal_age_days))
            rows = [
                r
                for r in rows
                if (r.get("PROPOSED_AT") is not None and r.get("PROPOSED_AT").replace(tzinfo=timezone.utc) >= cutoff)
            ]
        deduped_out = rows
        if dedupe_by_symbol:
            seen_symbols: set[str] = set()
            deduped_out = []
            for r in rows:
                sym = (r.get("SYMBOL") or "").upper().strip()
                if not sym or sym in seen_symbols:
                    continue
                seen_symbols.add(sym)
                deduped_out.append(r)
        return {
            "ok": True,
            "scope": scope,
            "latest_batch_date": str(latest_batch_date) if latest_batch_date is not None else None,
            "source_portfolio_id_filter": source_portfolio_id,
            "run_id_filter": run_id,
            "live_portfolio_id_filter": live_portfolio_id,
            "count": len(deduped_out),
            "dedupe_by_symbol": dedupe_by_symbol,
            "max_proposal_age_days": max_proposal_age_days,
            "candidates": serialize_rows(deduped_out),
        }
    finally:
        conn.close()
