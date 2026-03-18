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
    allow_stale_import: bool = False
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
    committee_refresh_ibkr_news: bool = True
    committee_ibkr_news_max_symbols: int = 20
    committee_ibkr_news_max_headlines_per_symbol: int = 5
    committee_ibkr_news_min_symbols_covered: int = 1
    committee_ibkr_news_max_age_minutes: int = 120


class ApproveLiveDecisionRequest(BaseModel):
    pm_actor: str = "portfolio_manager"
    compliance_actor: str = "compliance_user"
    intent_submit_actor: str = "intent_submitter"
    intent_approve_actor: str = "intent_approver"


class SubmitLiveDecisionRequest(BaseModel):
    execution_actor: str = "execution_operator"
    attempt_n: int = 1


class RevalidateLiveActionRequest(BaseModel):
    force_refresh_1m: bool = False


class CancelPendingOrdersRequest(BaseModel):
    portfolio_id: int | None = None
    ibkr_account_id: str | None = None
    symbol: str | None = None
    actor: str = "portfolio_manager"
    dry_run: bool = False
    include_local_sync: bool = True


class CancelSingleOrderRequest(BaseModel):
    actor: str = "portfolio_manager"
    dry_run: bool = False
    include_local_sync: bool = True


class CommitteeRunRequest(BaseModel):
    actor: str = "committee_orchestrator"
    model: str = "claude-3-5-sonnet"
    force_rerun: bool = False
    refresh_ibkr_news: bool = True
    ibkr_news_max_symbols: int = 20
    ibkr_news_max_headlines_per_symbol: int = 5
    ibkr_news_min_symbols_covered: int = 1
    ibkr_news_max_age_minutes: int = 120


class ApplyCommitteeVerdictRequest(BaseModel):
    actor: str = "committee_orchestrator"
    model: str = "claude-3-5-sonnet"
    verdict: dict = Field(default_factory=dict)


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


class CreateExitActionRequest(BaseModel):
    portfolio_id: int
    symbol: str
    qty: float | None = None
    actor: str = "portfolio_manager"
    reason: str | None = None
    auto_submit: bool = False
    force_refresh_1m: bool = True


_ALLOWED_TRANSITIONS = {
    "RESEARCH_IMPORTED": {"PENDING_OPEN_VALIDATION"},
    "PROPOSED": {"PENDING_OPEN_VALIDATION"},
    "PENDING_OPEN_VALIDATION": {"OPEN_BLOCKED", "OPEN_CAUTION", "OPEN_ELIGIBLE"},
    "OPEN_BLOCKED": {"PENDING_OPEN_STABILITY_REVIEW", "OPEN_CAUTION", "OPEN_ELIGIBLE", "READY_FOR_APPROVAL_FLOW"},
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
LIVE_ORDER_ACTIVE_STATUSES = {
    "SUBMITTED",
    "ACKNOWLEDGED",
    "PENDINGSUBMIT",
    "PRESUBMITTED",
    "PARTIAL_FILL",
    "PARTIALLYFILLED",
}
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
          ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, SIDE, ACTION_INTENT, EXIT_TYPE, EXIT_REASON, PROPOSED_QTY, PROPOSED_PRICE, ASSET_CLASS,
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
            select ACTION_ID, STATUS, COMPLIANCE_STATUS, REASON_CODES, PORTFOLIO_ID, SYMBOL, SIDE, ACTION_INTENT, EXIT_TYPE
            from MIP.LIVE.LIVE_ACTIONS
            where ACTION_ID = %s
            """,
            (action_id,),
        )
        rows = fetch_all(cur)
        return rows[0] if rows else None
    finally:
        conn.close()


def _normalize_broker_order_id(value) -> str:
    if value is None:
        return ""
    norm = str(value).strip()
    if norm in ("", "0", "0.0", "None", "none", "NULL", "null"):
        return ""
    return norm


def _normalize_broker_price(value):
    if value is None:
        return None
    try:
        px = float(value)
    except Exception:
        return None
    # IBKR sometimes emits max-double sentinel when a field is not set.
    if abs(px) >= 1e100:
        return None
    return px


def _normalize_action_intent(side: str | None, action_intent: str | None = None) -> str:
    intent = str(action_intent or "").upper().strip()
    if intent in ("ENTRY", "EXIT"):
        return intent
    side_upper = str(side or "").upper().strip()
    return "EXIT" if side_upper == "SELL" else "ENTRY"


def _fetch_live_symbol_position_qty(cur, portfolio_id: int | None, symbol: str | None) -> float:
    if portfolio_id is None or not symbol:
        return 0.0
    cur.execute(
        """
        select IBKR_ACCOUNT_ID
        from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
        where PORTFOLIO_ID = %s
          and coalesce(IS_ACTIVE, true) = true
        limit 1
        """,
        (portfolio_id,),
    )
    cfg_rows = fetch_all(cur)
    account_id = str((cfg_rows[0] or {}).get("IBKR_ACCOUNT_ID") or "").strip() if cfg_rows else ""
    if not account_id:
        return 0.0
    broker_truth = _fetch_latest_broker_truth(cur, account_id, str(symbol).upper())
    return float(broker_truth.get("symbol_position_qty") or 0.0)


def _fetch_latest_broker_truth(cur, account_id: str, symbol: str | None = None) -> dict:
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
    latest_snapshot_ts = (nav_rows[0] or {}).get("SNAPSHOT_TS") if nav_rows else None
    if not latest_snapshot_ts:
        return {
            "snapshot_ts": None,
            "open_order_ids": set(),
            "open_orders": [],
            "symbol_position_qty": 0.0,
            "has_symbol_position": False,
        }

    cur.execute(
        """
        select OPEN_ORDER_ID, OPEN_ORDER_STATUS, SYMBOL, OPEN_ORDER_QTY, OPEN_ORDER_FILLED, OPEN_ORDER_REMAINING
        from MIP.LIVE.BROKER_SNAPSHOTS
        where SNAPSHOT_TYPE = 'OPEN_ORDER'
          and IBKR_ACCOUNT_ID = %s
          and SNAPSHOT_TS = %s
        """,
        (account_id, latest_snapshot_ts),
    )
    open_orders = fetch_all(cur)
    open_order_ids = {
        _normalize_broker_order_id(r.get("OPEN_ORDER_ID"))
        for r in open_orders
        if _normalize_broker_order_id(r.get("OPEN_ORDER_ID"))
    }

    symbol_position_qty = 0.0
    has_symbol_position = False
    if symbol:
        cur.execute(
            """
            select coalesce(sum(POSITION_QTY), 0) as POSITION_QTY
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'POSITION'
              and IBKR_ACCOUNT_ID = %s
              and SNAPSHOT_TS = %s
              and upper(SYMBOL) = upper(%s)
            """,
            (account_id, latest_snapshot_ts, symbol),
        )
        pos_rows = fetch_all(cur)
        symbol_position_qty = float((pos_rows[0] or {}).get("POSITION_QTY") or 0.0)
        has_symbol_position = abs(symbol_position_qty) > 0

    return {
        "snapshot_ts": latest_snapshot_ts,
        "open_order_ids": open_order_ids,
        "open_orders": open_orders,
        "symbol_position_qty": symbol_position_qty,
        "has_symbol_position": has_symbol_position,
    }


def _is_order_active_in_broker_truth(order_row: dict, broker_open_order_ids: set[str]) -> bool:
    status = str(order_row.get("STATUS") or "").upper()
    if status not in LIVE_ORDER_ACTIVE_STATUSES:
        return False
    broker_order_id = _normalize_broker_order_id(order_row.get("BROKER_ORDER_ID"))
    return bool(broker_order_id and broker_order_id in broker_open_order_ids)


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
        "OPEN_BLOCKED": "Wait for market open and fresh opening snapshot, then run committee revalidation",
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


def _auto_import_latest_proposals_for_live_portfolio(
    live_portfolio_id: int,
    *,
    source_portfolio_id: int | None = None,
    limit: int = 200,
) -> dict:
    """
    Best-effort bridge from research proposals -> live actions so
    /live/activity/overview reflects latest proposal output.
    """
    safe_limit = max(1, min(int(limit or 200), 1000))
    try:
        result = import_live_actions_from_proposals(
            ImportLiveActionsFromProposalsRequest(
                live_portfolio_id=int(live_portfolio_id),
                source_portfolio_id=int(source_portfolio_id)
                if source_portfolio_id is not None
                else None,
                limit=safe_limit,
                latest_batch_only=True,
                allow_stale_import=False,
                dedupe_by_symbol=True,
                max_proposal_age_days=7,
            )
        )
        return {
            "attempted": True,
            "ok": bool(result.get("ok")),
            "candidate_count": int(result.get("candidate_count") or 0),
            "imported_count": int(result.get("imported_count") or 0),
            "skipped_existing_count": int(result.get("skipped_existing_count") or 0),
            "skipped_symbol_live_position_count": int(
                result.get("skipped_symbol_live_position_count") or 0
            ),
            "source_scope": result.get("source_scope"),
            "latest_batch_date": result.get("latest_batch_date"),
        }
    except Exception as exc:
        return {
            "attempted": True,
            "ok": False,
            "error": str(exc),
            "candidate_count": 0,
            "imported_count": 0,
            "skipped_existing_count": 0,
            "skipped_symbol_live_position_count": 0,
            "source_scope": None,
            "latest_batch_date": None,
        }


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


def _market_bar_ts_to_utc(ts: datetime | str | None) -> datetime | None:
    if ts is None:
        return None
    try:
        if isinstance(ts, str):
            parsed = _parse_iso_utc(ts)
            if parsed is not None:
                return parsed
            return None
        if ts.tzinfo is None:
            # MARKET_BARS.TS is stored as NY session clock time (NTZ).
            return ts.replace(tzinfo=NY_TZ).astimezone(timezone.utc)
        return ts.astimezone(timezone.utc)
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


def _extended_trading_bounds_utc(now_utc: datetime) -> tuple[datetime, datetime]:
    now_ny = now_utc.astimezone(NY_TZ)
    # ET window for actionable revalidation/submit controls:
    # pre-market (04:00 ET) through after-hours close (20:00 ET).
    open_ny = now_ny.replace(hour=4, minute=0, second=0, microsecond=0)
    close_ny = now_ny.replace(hour=20, minute=0, second=0, microsecond=0)
    return open_ny.astimezone(timezone.utc), close_ny.astimezone(timezone.utc)


def _is_extended_trading_open_ny(now_utc: datetime) -> bool:
    now_ny = now_utc.astimezone(NY_TZ)
    if now_ny.weekday() >= 5:
        return False
    open_utc, close_utc = _extended_trading_bounds_utc(now_utc)
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


def _parse_bool_config(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    raw = str(value).strip().lower()
    if raw in ("1", "true", "yes", "on", "y"):
        return True
    if raw in ("0", "false", "no", "off", "n"):
        return False
    return default


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
    snapshot_refresh = {"attempted": False}
    if force_refresh_1m:
        refresh_result = _force_refresh_latest_one_minute_bars(cur, action.get("SYMBOL"))
        account_id = cfg.get("IBKR_ACCOUNT_ID")
        if account_id:
            try:
                snapshot_payload = _run_on_demand_snapshot_sync(
                    **_default_snapshot_sync_params(),
                    account=str(account_id),
                    portfolio_id=int(portfolio_id) if portfolio_id is not None else None,
                )
                snapshot_refresh = {"attempted": True, "status": "SUCCESS", "payload": snapshot_payload}
            except Exception as exc:
                snapshot_refresh = {"attempted": True, "status": "FAIL", "error": str(exc)}

    symbol = action.get("SYMBOL")
    cur.execute(
        """
        select TS, CLOSE
        from MIP.MART.MARKET_BARS
        where SYMBOL = %s
          and INTERVAL_MINUTES = 1
        order by case when upper(coalesce(SOURCE, '')) = 'IBKR' then 0 else 1 end, TS desc
        limit 1
        """,
        (symbol,),
    )
    bar = cur.fetchone()
    bar_ts = bar[0] if bar else None
    bar_px = float(bar[1]) if bar and bar[1] is not None else None
    bar_age_sec = None
    if bar_ts is not None:
        bar_ts_utc = _market_bar_ts_to_utc(bar_ts)
        if bar_ts_utc is not None:
            bar_age_sec = (now_utc - bar_ts_utc).total_seconds()

    expected_entry_px = _expected_entry_reference(cur, action)
    gap_pct = None
    if expected_entry_px and bar_px:
        try:
            gap_pct = abs(bar_px - expected_entry_px) / max(abs(expected_entry_px), 1e-9)
        except Exception:
            gap_pct = None

    open_utc, _ = _extended_trading_bounds_utc(now_utc)
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
    if not _is_extended_trading_open_ny(now_utc):
        hard_reasons.append("OPEN_MARKET_CLOSED")
    if not symbol:
        hard_reasons.append("OPEN_MISSING_SYMBOL")
    if bar_ts is None:
        hard_reasons.append("OPEN_SNAPSHOT_MISSING")
    effective_snapshot_max_age_sec = float(policy["snapshot_max_age_sec"])
    if _is_extended_trading_open_ny(now_utc) and not _is_market_open_ny(now_utc):
        # Extended-hours prints can be sparse; keep a wider tolerance than regular session.
        effective_snapshot_max_age_sec = max(effective_snapshot_max_age_sec, 1800.0)
    if bar_ts is not None and bar_age_sec is not None and bar_age_sec > effective_snapshot_max_age_sec:
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
        "effective_snapshot_max_age_sec": effective_snapshot_max_age_sec,
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
        "refresh": {
            "bars": refresh_result,
            "broker_snapshot": snapshot_refresh,
        },
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
    For EXIT actions, bypass freshness/recency hard blocks to avoid trapping
    risk-reduction orders when quote timestamps lag.
    """
    reason_codes: list[str] = []
    symbol = action.get("SYMBOL")
    if not symbol:
        reason_codes.append("FIRST_SESSION_REALISM_MISSING_SYMBOL")
        return reason_codes, {"has_symbol": False}
    action_intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
    is_exit = action_intent == "EXIT"

    source = (action.get("EXECUTION_PRICE_SOURCE") or "").upper()
    allowed_sources = {"ONE_MINUTE_BAR", "IBKR_DIRECT_1M"}
    if source not in allowed_sources:
        reason_codes.append("FIRST_SESSION_REALISM_SOURCE_REQUIRED")

    one_min_bar_ts = action.get("ONE_MIN_BAR_TS")
    if not one_min_bar_ts:
        reason_codes.append("FIRST_SESSION_REALISM_MISSING_1M_REFERENCE")
    one_min_bar_ts_utc = _market_bar_ts_to_utc(one_min_bar_ts)

    cur.execute(
        """
        select TS, CLOSE
        from MIP.MART.MARKET_BARS
        where SYMBOL = %s
          and INTERVAL_MINUTES = 1
          and upper(coalesce(SOURCE, '')) = 'IBKR'
        order by TS desc
        limit 1
        """,
        (symbol,),
    )
    latest_bar = cur.fetchone()
    latest_ts = latest_bar[0] if latest_bar else None
    latest_close = latest_bar[1] if latest_bar else None
    latest_ts_utc = _market_bar_ts_to_utc(latest_ts)
    using_revalidation_bar_as_latest = False
    if source == "IBKR_DIRECT_1M" and one_min_bar_ts_utc is not None:
        # IB-direct revalidation can be fresher than MART.MARKET_BARS (or MART
        # may lag writes). Prefer the direct bar whenever it is newer.
        use_direct_bar = (
            latest_ts_utc is None
            or (one_min_bar_ts_utc - latest_ts_utc).total_seconds() > 30
        )
        if use_direct_bar:
            latest_ts = one_min_bar_ts
            latest_ts_utc = one_min_bar_ts_utc
            latest_close = action.get("ONE_MIN_BAR_CLOSE")
            using_revalidation_bar_as_latest = True
    now_utc = datetime.now(timezone.utc)
    market_open = _is_extended_trading_open_ny(now_utc)
    open_utc, close_utc = _extended_trading_bounds_utc(now_utc)
    if not is_exit:
        if latest_ts_utc is None:
            reason_codes.append("FIRST_SESSION_REALISM_NO_1M_BAR")
        else:
            bar_age_sec = (now_utc - latest_ts_utc).total_seconds()
            # 60s proved too strict for committee runtime; enforce practical lower bound.
            max_age_sec = max(int(cfg.get("QUOTE_FRESHNESS_THRESHOLD_SEC") or 60), 300)
            # Outside extended-hours: allow submit with latest regular-session bar.
            if market_open and bar_age_sec is not None and bar_age_sec > max_age_sec:
                reason_codes.append("FIRST_SESSION_REALISM_1M_STALE")
            if one_min_bar_ts_utc is not None and latest_ts_utc is not None:
                ts_diff_sec = abs((one_min_bar_ts_utc - latest_ts_utc).total_seconds())
                if ts_diff_sec > 90:
                    reason_codes.append("FIRST_SESSION_REALISM_REVALIDATION_NOT_LATEST")
            elif one_min_bar_ts and latest_ts and one_min_bar_ts != latest_ts:
                reason_codes.append("FIRST_SESSION_REALISM_REVALIDATION_NOT_LATEST")

    details = {
        "symbol": symbol,
        "action_intent": action_intent,
        "is_exit": is_exit,
        "market_open_ny": market_open,
        "market_open_ts_utc": open_utc.isoformat(),
        "market_close_ts_utc": close_utc.isoformat(),
        "execution_price_source": source or None,
        "one_min_bar_ts": one_min_bar_ts,
        "latest_one_min_bar_ts": latest_ts,
        "latest_one_min_close": latest_close,
        "latest_one_min_from_revalidation": using_revalidation_bar_as_latest,
        "quote_freshness_threshold_sec": max(int(cfg.get("QUOTE_FRESHNESS_THRESHOLD_SEC") or 60), 300),
    }
    if is_exit:
        details["exit_reality_bypass"] = True
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
        if _is_extended_trading_open_ny(datetime.now(timezone.utc)) and not _is_market_open_ny(datetime.now(timezone.utc)):
            max_snap_age = max(max_snap_age, 900)
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
def get_live_metrics(portfolio_id: int | None = Query(None, description="Live portfolio ID for latest brief (auto-resolved when omitted)")):
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
    resolved_portfolio_id = portfolio_id
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

        if resolved_portfolio_id is None:
            cur.execute(
                """
                select PORTFOLIO_ID
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where coalesce(IS_ACTIVE, true)
                order by UPDATED_AT desc, PORTFOLIO_ID asc
                limit 1
                """
            )
            cfg_row = cur.fetchone()
            if cfg_row:
                resolved_portfolio_id = int(cfg_row[0])

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
        if resolved_portfolio_id is not None:
            cur.execute(brief_sql, (resolved_portfolio_id,))
            brief_row = cur.fetchone()
            if brief_row:
                cols = [d[0] for d in cur.description]
                last_brief = serialize_row(dict(zip(cols, brief_row)))
                last_brief["found"] = True
            else:
                last_brief = {"found": False}
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
        "portfolio_id": resolved_portfolio_id,
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


def _default_snapshot_sync_params() -> dict:
    return {
        "host": os.getenv("IBKR_SNAPSHOT_HOST", os.getenv("IBKR_EXEC_HOST", "127.0.0.1")),
        "port": int(os.getenv("IBKR_SNAPSHOT_PORT", os.getenv("IBKR_EXEC_PORT", "4002"))),
        "client_id": int(os.getenv("IBKR_SNAPSHOT_CLIENT_ID", "9402")),
    }


def _submit_ibkr_order_bundle(
    *,
    account: str,
    symbol: str,
    side: str,
    qty: float,
    entry_price: float | None,
    tp_price: float | None,
    sl_price: float | None,
    tif: str = "DAY",
    child_tif: str | None = None,
) -> dict:
    """
    Submit parent + optional TP/SL bundle to IBKR through cursorfiles runtime.
    """
    root = _project_root()
    py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "place_ibkr_order.py"
    if not py.exists() or not script.exists():
        raise HTTPException(
            status_code=500,
            detail="IBKR order runtime not found (cursorfiles venv or place script missing).",
        )

    host = os.getenv("IBKR_EXEC_HOST", "127.0.0.1")
    port = int(os.getenv("IBKR_EXEC_PORT", "4002"))
    client_id = int(os.getenv("IBKR_EXEC_CLIENT_ID", "9410"))
    connect_timeout_sec = int(os.getenv("IBKR_EXEC_CONNECT_TIMEOUT_SEC", "12"))
    exchange = os.getenv("IBKR_EXEC_EXCHANGE", "SMART")
    currency = os.getenv("IBKR_EXEC_CURRENCY", "USD")
    outside_rth = os.getenv("IBKR_EXEC_OUTSIDE_RTH", "1").strip().lower() in ("1", "true", "yes", "on")

    cmd = [
        str(py),
        str(script),
        "--host",
        host,
        "--port",
        str(port),
        "--client-id",
        str(client_id),
        "--connect-timeout-sec",
        str(connect_timeout_sec),
        "--account",
        str(account),
        "--symbol",
        str(symbol).upper(),
        "--side",
        str(side).upper(),
        "--qty",
        str(float(qty)),
        "--tif",
        str(tif or "DAY").upper(),
        "--exchange",
        exchange,
        "--currency",
        currency,
    ]
    if child_tif:
        cmd.extend(["--child-tif", str(child_tif).upper()])
    if entry_price is not None:
        cmd.extend(["--entry-price", str(float(entry_price))])
    if tp_price is not None:
        cmd.extend(["--tp-price", str(float(tp_price))])
    if sl_price is not None:
        cmd.extend(["--sl-price", str(float(sl_price))])
    if outside_rth:
        cmd.append("--outside-rth")

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
        timeout=60,
    )

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if proc.returncode != 0:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "IBKR order submission failed.",
                "reason_codes": ["IBKR_SUBMIT_FAILED"],
                "stderr": stderr[-4000:],
                "stdout": stdout[-4000:],
            },
        )

    json_blob = None
    for stream in (stdout, stderr):
        if not stream:
            continue
        chunks = [s.strip() for s in stream.splitlines() if s.strip()]
        if stream.strip():
            chunks.append(stream.strip())
        for maybe in reversed(chunks):
            try:
                parsed = json.loads(maybe)
                if isinstance(parsed, dict):
                    json_blob = parsed
                    break
            except Exception:
                continue
        if isinstance(json_blob, dict):
            break
    if not isinstance(json_blob, dict) or not json_blob.get("ok"):
        raise HTTPException(
            status_code=409,
            detail={
                "message": "IBKR order submission returned unexpected payload.",
                "reason_codes": ["IBKR_SUBMIT_BAD_PAYLOAD"],
                "stdout": stdout[-4000:],
                "stderr": stderr[-4000:],
            },
        )
    return json_blob


def _cancel_ibkr_open_orders(
    *,
    account: str,
    symbol: str | None = None,
    broker_order_id: str | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Cancel open/pending IBKR orders for an account (optionally symbol-scoped).
    """
    root = _project_root()
    py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "cancel_ibkr_open_orders.py"
    if not py.exists() or not script.exists():
        raise HTTPException(
            status_code=500,
            detail="IBKR cancel runtime not found (cursorfiles venv or cancel script missing).",
        )

    host = os.getenv("IBKR_EXEC_HOST", "127.0.0.1")
    port = int(os.getenv("IBKR_EXEC_PORT", "4002"))
    client_id = int(os.getenv("IBKR_EXEC_CLIENT_ID", "9410"))
    connect_timeout_sec = int(os.getenv("IBKR_EXEC_CONNECT_TIMEOUT_SEC", "12"))

    cmd = [
        str(py),
        str(script),
        "--host",
        host,
        "--port",
        str(port),
        "--client-id",
        str(client_id),
        "--connect-timeout-sec",
        str(connect_timeout_sec),
        "--account",
        str(account),
    ]
    if symbol:
        cmd.extend(["--symbol", str(symbol).upper()])
    if broker_order_id:
        cmd.extend(["--broker-order-id", str(broker_order_id)])
    if dry_run:
        cmd.append("--dry-run")

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

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if proc.returncode != 0:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "IBKR cancel open orders failed.",
                "reason_codes": ["IBKR_CANCEL_FAILED"],
                "stderr": stderr[-4000:],
                "stdout": stdout[-4000:],
            },
        )

    json_blob = None
    for stream in (stdout, stderr):
        if not stream:
            continue
        chunks = [s.strip() for s in stream.splitlines() if s.strip()]
        if stream.strip():
            chunks.append(stream.strip())
        for maybe in reversed(chunks):
            try:
                parsed = json.loads(maybe)
                if isinstance(parsed, dict):
                    json_blob = parsed
                    break
            except Exception:
                continue
        if isinstance(json_blob, dict):
            break
    if not isinstance(json_blob, dict) or not json_blob.get("ok"):
        raise HTTPException(
            status_code=409,
            detail={
                "message": "IBKR cancel open orders returned unexpected payload.",
                "reason_codes": ["IBKR_CANCEL_BAD_PAYLOAD"],
                "stdout": stdout[-4000:],
                "stderr": stderr[-4000:],
            },
        )
    return json_blob


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


def _run_agent_ibkr_bar_refresh(symbol: str | None, timeout_sec: int = 120) -> dict:
    """
    Fetch latest 1-minute bar directly from IB Gateway via agent runtime.
    No Snowflake writes in this path.
    """
    if not symbol:
        return {"attempted": False, "status": "SKIPPED", "reason": "MISSING_SYMBOL"}

    root = _project_root()
    py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "fetch_ibkr_live_bars.py"
    if not py.exists() or not script.exists():
        return {
            "attempted": False,
            "status": "SKIPPED",
            "reason": "IBKR_LIVE_FETCH_RUNTIME_NOT_FOUND",
        }

    cmd = [
        str(py),
        str(script),
        "--symbols",
        str(symbol).upper(),
        "--market-types",
        "FX" if "/" in str(symbol) else "STOCK",
        "--interval-minutes",
        "1",
        "--window-bars",
        "1",
    ]
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

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    payload = {}
    for stream in (stdout, stderr):
        if not stream:
            continue
        start_idx = stream.find("{")
        if start_idx < 0:
            continue
        try:
            parsed = json.loads(stream[start_idx:])
            if isinstance(parsed, dict):
                payload = parsed
                break
        except Exception:
            continue

    if proc.returncode != 0:
        return {
            "attempted": True,
            "status": "FAIL",
            "executor": "agent_runtime_ibkr",
            "payload": payload or None,
            "stderr": stderr[-2000:],
            "stdout": stdout[-2000:],
        }

    return {
        "attempted": True,
        "status": "SUCCESS",
        "executor": "agent_runtime_ibkr",
        "payload": payload,
    }


def _extract_latest_one_min_bar_from_refresh(refresh_info: dict | None, symbol: str | None) -> tuple[datetime, float] | None:
    if not isinstance(refresh_info, dict):
        return None
    if str(refresh_info.get("status") or "").upper() != "SUCCESS":
        return None
    payload = refresh_info.get("payload")
    if not isinstance(payload, dict):
        return None
    target_symbol = str(symbol or "").strip().upper().replace(" ", "")
    for sym_payload in payload.get("symbols") or []:
        if not isinstance(sym_payload, dict):
            continue
        sym_value = str(sym_payload.get("symbol") or "").strip().upper().replace(" ", "")
        if target_symbol and sym_value and sym_value != target_symbol:
            continue
        bars = sym_payload.get("bars")
        if not isinstance(bars, list) or not bars:
            continue
        latest = bars[-1]
        if not isinstance(latest, dict):
            continue
        ts_val = latest.get("ts")
        close_val = latest.get("close")
        if ts_val is None or close_val is None:
            continue
        ts_utc = _market_bar_ts_to_utc(ts_val)
        if ts_utc is None:
            continue
        try:
            return ts_utc, float(close_val)
        except Exception:
            continue
    return None


def _run_agent_ibkr_news_refresh(
    *,
    max_symbols: int = 20,
    max_headlines_per_symbol: int = 5,
    timeout_sec: int = 180,
) -> dict:
    """
    Execute IBKR news ingest via agent runtime and return structured result.
    """
    root = _project_root()
    py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "ingest_ibkr_news.py"
    if not py.exists() or not script.exists():
        return {
            "attempted": False,
            "status": "SKIPPED",
            "reason": "IBKR_NEWS_RUNTIME_NOT_FOUND",
        }

    cmd = [
        str(py),
        str(script),
        "--max-symbols",
        str(max(1, int(max_symbols))),
        "--max-headlines-per-symbol",
        str(max(1, int(max_headlines_per_symbol))),
    ]
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

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    payload = {}
    for stream in (stdout, stderr):
        if not stream:
            continue
        start_idx = stream.find("{")
        if start_idx < 0:
            continue
        try:
            parsed = json.loads(stream[start_idx:])
            if isinstance(parsed, dict):
                payload = parsed
                break
        except Exception:
            continue

    if proc.returncode != 0:
        return {
            "attempted": True,
            "status": "FAIL",
            "executor": "agent_runtime_ibkr_news",
            "payload": payload or None,
            "stderr": stderr[-2000:],
            "stdout": stdout[-2000:],
        }

    return {
        "attempted": True,
        "status": "SUCCESS",
        "executor": "agent_runtime_ibkr_news",
        "payload": payload or None,
    }


def _refresh_news_context_chain(cur) -> dict:
    """
    Recompute NEWS layers after direct NEWS_RAW ingest.
    """
    cur.execute("call MIP.NEWS.SP_MAP_NEWS_SYMBOLS(null)")
    map_rows = fetch_all(cur)
    cur.execute("call MIP.NEWS.SP_COMPUTE_INFO_STATE_DAILY(current_timestamp(), null)")
    compute_rows = fetch_all(cur)
    cur.execute("call MIP.NEWS.SP_AGGREGATE_NEWS_EVENTS(current_timestamp(), null)")
    agg_rows = fetch_all(cur)
    return {
        "map": serialize_row(map_rows[0]) if map_rows else None,
        "compute": serialize_row(compute_rows[0]) if compute_rows else None,
        "aggregate": serialize_row(agg_rows[0]) if agg_rows else None,
    }


def _evaluate_ibkr_news_readiness(
    cur,
    *,
    symbol: str | None,
    min_symbols_covered: int = 1,
    max_age_minutes: int = 120,
) -> dict:
    """
    Determine if IBKR_NEWS_API is fresh enough for committee usage.
    """
    reasons: list[str] = []
    symbol_norm = (symbol or "").upper().strip()

    cur.execute(
        """
        select
          SOURCE_ID,
          HEALTH_STATUS,
          ENTRIES_TODAY,
          SYMBOLS_COVERED_TODAY,
          LAST_INGESTED_AT_ET,
          LAST_INGEST_AGE_MINUTES,
          MISSING_ROUNDS,
          IS_STALE
        from MIP.MART.V_NEWS_FEED_HEALTH
        where SOURCE_ID = 'IBKR_NEWS_API'
        limit 1
        """
    )
    feed_rows = fetch_all(cur)
    feed_row = serialize_row(feed_rows[0]) if feed_rows else None
    if not feed_row:
        reasons.append("IBKR_NEWS_SOURCE_NOT_REGISTERED")
        return {
            "ready": False,
            "reason_codes": reasons,
            "feed_health": None,
            "symbol_coverage": None,
        }

    entries_today = int(feed_row.get("ENTRIES_TODAY") or 0)
    symbols_covered_today = int(feed_row.get("SYMBOLS_COVERED_TODAY") or 0)
    age_minutes_raw = feed_row.get("LAST_INGEST_AGE_MINUTES")
    age_minutes = float(age_minutes_raw) if age_minutes_raw is not None else None

    if entries_today <= 0:
        reasons.append("IBKR_NEWS_NO_TODAY_ROWS")
    if symbols_covered_today < max(1, int(min_symbols_covered)):
        reasons.append("IBKR_NEWS_MIN_COVERAGE_NOT_MET")
    if age_minutes is None:
        reasons.append("IBKR_NEWS_AGE_UNKNOWN")
    elif age_minutes > float(max_age_minutes):
        reasons.append("IBKR_NEWS_STALE")

    symbol_coverage = None
    if symbol_norm:
        cur.execute(
            """
            select
              count(*) as SYMBOL_ROWS_TODAY,
              max(INGESTED_AT) as LAST_INGESTED_AT
            from MIP.NEWS.NEWS_RAW
            where SOURCE_ID = 'IBKR_NEWS_API'
              and upper(SYMBOL_HINT) = %s
              and cast(convert_timezone('UTC', 'America/New_York', INGESTED_AT) as date)
                  = cast(convert_timezone('America/New_York', current_timestamp()) as date)
            """,
            (symbol_norm,),
        )
        symbol_rows = fetch_all(cur)
        symbol_coverage = serialize_row(symbol_rows[0]) if symbol_rows else {}
        if int((symbol_coverage or {}).get("SYMBOL_ROWS_TODAY") or 0) <= 0:
            reasons.append("IBKR_NEWS_SYMBOL_NOT_COVERED")

    return {
        "ready": len(reasons) == 0,
        "reason_codes": reasons,
        "feed_health": feed_row,
        "symbol_coverage": symbol_coverage,
    }


def _force_refresh_latest_one_minute_bars(cur, symbol: str | None = None) -> dict:
    """
    Direct IBKR-only 1-minute refresh before revalidation.
    """
    return _run_agent_ibkr_bar_refresh(symbol)


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
    optimal_horizon_bars = None

    def _pick_horizon(rows: list[dict], min_obs: int) -> tuple[float | None, int | None, bool]:
        candidates = []
        for r in rows:
            try:
                h = int(r.get("HORIZON_BARS"))
                avg_ret = float(r.get("AVG_RETURN")) if r.get("AVG_RETURN") is not None else None
                n_ret = int(r.get("N_RET") or 0)
            except Exception:
                continue
            if avg_ret is None:
                continue
            candidates.append({"h": h, "avg": avg_ret, "n": n_ret})
        if not candidates:
            return None, None, False

        strong = [c for c in candidates if c["n"] >= min_obs]
        low_sample = False
        universe = strong
        if not universe:
            universe = sorted(candidates, key=lambda x: (x["n"], x["avg"]), reverse=True)[:3]
            low_sample = True
        best = sorted(universe, key=lambda x: (x["avg"], x["n"]), reverse=True)[0]
        return float(best["avg"]), int(best["h"]), low_sample

    if symbol_norm and market_type_norm and pattern_id is not None:
        cur.execute(
            """
            select
              o.HORIZON_BARS,
              avg(case when o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null then o.REALIZED_RETURN end) as AVG_RETURN,
              count_if(o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null) as N_RET
            from MIP.APP.RECOMMENDATION_LOG r
            join MIP.APP.RECOMMENDATION_OUTCOMES o
              on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
            where upper(r.SYMBOL) = %s
              and upper(r.MARKET_TYPE) = %s
              and r.PATTERN_ID = %s
              and r.INTERVAL_MINUTES = %s
            group by o.HORIZON_BARS
            """,
            (symbol_norm, market_type_norm, pattern_id, interval_minutes),
        )
        rows = fetch_all(cur)
        sel_ret, sel_h, low_sample = _pick_horizon(rows, min_obs=10)
        if sel_ret is not None:
            base_return = sel_ret
            optimal_horizon_bars = sel_h
            reasons.append(f"BASE_FROM_SYMBOL_PATTERN_OPTIMAL_H{sel_h}")
            if low_sample:
                reasons.append("OPTIMAL_HORIZON_LOW_SAMPLE")

    if base_return is None and market_type_norm and pattern_id is not None:
        cur.execute(
            """
            select
              o.HORIZON_BARS,
              avg(case when o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null then o.REALIZED_RETURN end) as AVG_RETURN,
              count_if(o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null) as N_RET
            from MIP.APP.RECOMMENDATION_LOG r
            join MIP.APP.RECOMMENDATION_OUTCOMES o
              on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
            where upper(r.MARKET_TYPE) = %s
              and r.PATTERN_ID = %s
              and r.INTERVAL_MINUTES = %s
            group by o.HORIZON_BARS
            """,
            (market_type_norm, pattern_id, interval_minutes),
        )
        rows = fetch_all(cur)
        sel_ret, sel_h, low_sample = _pick_horizon(rows, min_obs=30)
        if sel_ret is not None:
            base_return = sel_ret
            optimal_horizon_bars = sel_h
            reasons.append(f"BASE_FROM_MARKET_PATTERN_OPTIMAL_H{sel_h}")
            if low_sample:
                reasons.append("OPTIMAL_HORIZON_LOW_SAMPLE")

    if base_return is None:
        base_return = 0.02
        optimal_horizon_bars = 5
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
        "optimal_horizon_bars": int(optimal_horizon_bars) if optimal_horizon_bars is not None else 5,
        "max_hold_trading_days": int(max(1, round((optimal_horizon_bars or 5) * (interval_minutes / 1440.0)))),
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
    action_intent = str(context.get("action_intent") or "ENTRY").upper()
    is_exit = action_intent == "EXIT"
    role_focus = {
        "PROPOSER": "Build the strongest symbol-specific case with concrete target/hold assumptions.",
        "TRADER_EXECUTION_REVIEWER": "Focus on execution realism, opening behavior, slippage, and timing constraints.",
        "RISK_MANAGER": "Focus on size, drawdown/correlation, and explicit risk controls.",
        "CHALLENGER": "Provide the strongest falsification and what could break this setup.",
        "PORTFOLIO_MANAGER": "Focus on cross-candidate capital allocation priority and portfolio fit.",
        "POST_TRADE_REVIEWER": "Focus on ex-ante evaluability and what would validate/invalidate this call later.",
    }.get(role, "Provide role-specific analysis.")
    objective_line = (
        "- This is an EXIT decision (position close/reduce), not a new entry. Interpret should_enter as should_execute_exit.\n"
        "- For EXIT, reserve BLOCK only for hard safety/compliance blockers (e.g., missing position, halted symbol, or severe execution integrity risk).\n"
        if is_exit
        else "- This is an ENTRY decision. should_enter means open/increase position.\n"
    )
    schema_line = (
        "{\"stance\":\"SUPPORT|CONDITIONAL|BLOCK\",\"confidence\":0.0-1.0,"
        "\"summary\":\"...\",\"exit_size_factor\":0.0-1.0,"
        "\"should_execute_exit\":true|false,"
        "\"exit_horizon_bars\":integer,"
        "\"max_giveback_pct\":number,"
        "\"reasons\":[\"...\"],\"assumptions\":[\"...\"]}\n"
        if is_exit
        else
        "{\"stance\":\"SUPPORT|CONDITIONAL|BLOCK\",\"confidence\":0.0-1.0,"
        "\"summary\":\"...\",\"size_factor\":0.0-1.0,"
        "\"should_enter\":true|false,"
        "\"target_return\":number,"
        "\"stop_loss_pct\":number,"
        "\"hold_bars\":integer,"
        "\"early_exit_target_return\":number,"
        "\"reasons\":[\"...\"],\"assumptions\":[\"...\"]}\n"
    )
    return (
        "You are one role in an institutional multi-agent trade committee.\n"
        "Return ONLY a JSON object with keys:\n"
        f"{schema_line}"
        "Rules:\n"
        "- Be strict and risk-aware.\n"
        "- If data freshness is weak, prefer CONDITIONAL or BLOCK.\n"
        "- When discussing news freshness, explicitly cite source as ACTION_NEWS (news_context_snapshot) or LATEST_NEWS (latest_symbol_news_context).\n"
        "- If ACTION_NEWS and LATEST_NEWS conflict, state the conflict explicitly and prioritize LATEST_NEWS recency for risk gating.\n"
        f"{objective_line}"
        "- Contribute to joint decision dimensions: enter/size/target/stop/hold/early-exit.\n"
        "- stop_loss_pct must be positive and risk-aware versus expected edge after fees.\n"
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


def _normalize_role_output(role: str, out: dict, symbol: str | None = None, action_intent: str = "ENTRY") -> dict:
    intent = str(action_intent or "ENTRY").upper()
    is_exit = intent == "EXIT"
    stance = str(out.get("stance", "CONDITIONAL")).upper()
    if stance not in ("SUPPORT", "CONDITIONAL", "BLOCK"):
        stance = "CONDITIONAL"
    try:
        confidence = float(out.get("confidence", 0.5))
    except Exception:
        confidence = 0.5
    confidence = max(0.0, min(1.0, confidence))
    size_key_val = out.get("exit_size_factor") if is_exit else out.get("size_factor")
    if size_key_val is None:
        size_key_val = out.get("size_factor")
    try:
        size_factor = float(size_key_val if size_key_val is not None else 1.0)
    except Exception:
        size_factor = 1.0
    size_factor = max(0.0, min(1.0, size_factor))
    should_execute_exit = out.get("should_execute_exit")
    should_enter_raw = out.get("should_enter")
    decision_flag = should_execute_exit if is_exit and should_execute_exit is not None else should_enter_raw
    should_enter = bool(decision_flag if decision_flag is not None else (stance != "BLOCK"))
    target_key_val = out.get("target_return")
    if target_key_val is None and is_exit:
        target_key_val = out.get("exit_target_return")
    try:
        target_return = float(target_key_val) if target_key_val is not None else None
    except Exception:
        target_return = None
    stop_key_val = out.get("stop_loss_pct")
    if stop_key_val is None and is_exit:
        stop_key_val = out.get("max_giveback_pct")
    try:
        stop_loss_pct = float(stop_key_val) if stop_key_val is not None else None
    except Exception:
        stop_loss_pct = None
    if stop_loss_pct is not None:
        stop_loss_pct = max(0.0005, min(stop_loss_pct, 0.95))
    hold_key_val = out.get("exit_horizon_bars") if is_exit else out.get("hold_bars")
    if hold_key_val is None:
        hold_key_val = out.get("hold_bars")
    try:
        hold_bars = int(hold_key_val) if hold_key_val is not None else None
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
        "should_execute_exit": should_enter if is_exit else None,
        "exit_size_factor": size_factor if is_exit else None,
        "exit_horizon_bars": hold_bars if is_exit else None,
        "target_return": target_return,
        "stop_loss_pct": stop_loss_pct,
        "hold_bars": hold_bars,
        "early_exit_target_return": early_exit_target_return,
        "reasons": reasons,
        "assumptions": out.get("assumptions") if isinstance(out.get("assumptions"), list) else [],
    }


def _aggregate_committee(outputs: list[dict], action_intent: str = "ENTRY") -> dict:
    intent = str(action_intent or "ENTRY").upper()
    stances = [o.get("stance", "CONDITIONAL") for o in outputs]
    size_factors = [float(o.get("size_factor", 1.0)) for o in outputs if o.get("size_factor") is not None]
    if intent == "EXIT":
        # Exit committee is advisory-first: block only on broad consensus.
        block_count = sum(1 for s in stances if s == "BLOCK")
        recommendation = "BLOCK" if (outputs and block_count == len(outputs)) else "PROCEED_REDUCED"
    else:
        if any(s == "BLOCK" for s in stances):
            recommendation = "BLOCK"
        elif any(s == "CONDITIONAL" for s in stances):
            recommendation = "PROCEED_REDUCED"
        else:
            recommendation = "PROCEED"
    size_factor = (
        (sum(size_factors) / len(size_factors))
        if (intent == "EXIT" and size_factors)
        else (min(size_factors) if size_factors else 1.0)
    )
    size_factor = max(0.0, min(1.0, size_factor))
    should_enter_votes = [bool(o.get("should_enter")) for o in outputs]
    if intent == "EXIT":
        # For exits, default to executable unless unanimous hard block.
        should_enter = not (outputs and all(str(o.get("stance", "")).upper() == "BLOCK" for o in outputs))
    else:
        should_enter = bool(sum(1 for v in should_enter_votes if v) >= max(1, (len(should_enter_votes) + 1) // 2))
    target_returns = [float(o["target_return"]) for o in outputs if o.get("target_return") is not None]
    stop_losses = [float(o["stop_loss_pct"]) for o in outputs if o.get("stop_loss_pct") is not None]
    hold_bars_vals = [int(o["hold_bars"]) for o in outputs if o.get("hold_bars") is not None]
    early_exit_targets = [float(o["early_exit_target_return"]) for o in outputs if o.get("early_exit_target_return") is not None]
    confidence = 0.0
    if outputs:
        confidence = sum(float(o.get("confidence", 0.0)) for o in outputs) / len(outputs)
    target_return = (sum(target_returns) / len(target_returns)) if target_returns else None
    stop_loss_pct = (sum(stop_losses) / len(stop_losses)) if stop_losses else None
    hold_bars = int(round(sum(hold_bars_vals) / len(hold_bars_vals))) if hold_bars_vals else None
    early_exit_target_return = (sum(early_exit_targets) / len(early_exit_targets)) if early_exit_targets else None
    early_exit_target_return = _normalize_early_exit_target(target_return, early_exit_target_return)
    joint_decision = {
        "should_enter": should_enter and recommendation != "BLOCK",
        "position_size_factor": round(size_factor, 4),
        "realistic_target_return": target_return,
        "stop_loss_pct": stop_loss_pct,
        "hold_bars": hold_bars,
        "acceptable_early_exit_target_return": early_exit_target_return,
    }
    if intent == "EXIT":
        joint_decision.update(
            {
                "should_execute_exit": should_enter and recommendation != "BLOCK",
                "exit_size_factor": round(size_factor, 4),
                "exit_horizon_bars": hold_bars,
            }
        )
    return {
        "recommendation": recommendation,
        "size_factor": size_factor,
        "confidence": round(confidence, 4),
        "blocked": recommendation == "BLOCK",
        "joint_decision": joint_decision,
    }


def _extract_committee_quality_reason_codes(outputs: list[dict]) -> list[str]:
    codes: list[str] = []
    fallback_count = 0
    model_error_count = 0
    missing_reasons_count = 0
    for out in outputs or []:
        reasons = out.get("reasons") if isinstance(out.get("reasons"), list) else []
        reason_set = {str(r).upper() for r in reasons if r is not None}
        if "MODEL_FALLBACK" in reason_set:
            fallback_count += 1
        if any(str(r).upper().startswith("MODEL_ERROR:") for r in reasons):
            model_error_count += 1
        if "MISSING_EXPLICIT_REASONS" in reason_set:
            missing_reasons_count += 1
    if fallback_count > 0:
        codes.append("COMMITTEE_MODEL_FALLBACK")
        if fallback_count == len(outputs or []):
            codes.append("COMMITTEE_MODEL_FALLBACK_ALL_ROLES")
    if model_error_count > 0:
        codes.append("COMMITTEE_MODEL_ERROR_PRESENT")
    if missing_reasons_count > 0:
        codes.append("COMMITTEE_OUTPUT_REASONS_MISSING")
    return codes


def _suppress_block_on_degraded_entry_quality(verdict: dict, action_intent: str, risk_cfg: dict | None = None) -> dict:
    out = dict(verdict or {})
    intent = str(action_intent or "ENTRY").upper()
    if intent == "EXIT":
        return out
    recommendation = str(out.get("recommendation") or "").upper()
    degraded_quality = bool(out.get("degraded_quality")) or bool(out.get("quality_backfilled"))
    jd = _parse_variant(out.get("joint_decision"))
    target_return = jd.get("realistic_target_return")
    stop_loss_pct = jd.get("stop_loss_pct")
    min_rr = float(os.getenv("LIVE_MIN_R_MULTIPLE", "1.10"))
    bust_pct = None
    if isinstance(risk_cfg, dict) and risk_cfg.get("bust_pct") is not None:
        try:
            bust_pct = float(risk_cfg.get("bust_pct"))
        except Exception:
            bust_pct = None
    quality_risk_normalized = False
    try:
        target_return_num = float(target_return) if target_return is not None else None
    except Exception:
        target_return_num = None
    try:
        stop_loss_num = float(stop_loss_pct) if stop_loss_pct is not None else None
    except Exception:
        stop_loss_num = None
    if degraded_quality and target_return_num is not None and target_return_num > 0 and min_rr > 0:
        # Degraded-quality outputs often carry unrealistic stop values; normalize to
        # an execution-viable stop cap so submit doesn't fail later with RR gate.
        stop_cap_from_rr = target_return_num / min_rr
        if bust_pct is not None and bust_pct > 0:
            stop_cap_from_rr = min(stop_cap_from_rr, bust_pct)
        stop_cap_from_rr = max(stop_cap_from_rr, 0.005)
        if stop_loss_num is None or stop_loss_num <= 0 or stop_loss_num > stop_cap_from_rr:
            jd["stop_loss_pct"] = float(stop_cap_from_rr)
            out["joint_decision"] = jd
            quality_risk_normalized = True
    if recommendation != "BLOCK" or not degraded_quality:
        if quality_risk_normalized:
            out["quality_risk_normalized"] = True
        return out
    try:
        size_factor = float(out.get("size_factor", 1.0))
    except Exception:
        size_factor = 1.0
    size_factor = max(0.0, min(0.35, size_factor))
    out["recommendation"] = "PROCEED_REDUCED"
    out["blocked"] = False
    out["size_factor"] = size_factor
    jd["should_enter"] = True
    jd["position_size_factor"] = size_factor
    out["joint_decision"] = jd
    out["quality_block_override_applied"] = True
    if quality_risk_normalized:
        out["quality_risk_normalized"] = True
    return out


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


def _decision_allows_execution(jd: dict, action_intent: str) -> bool:
    intent = str(action_intent or "ENTRY").upper()
    if intent == "EXIT":
        if jd.get("should_execute_exit") is not None:
            return bool(jd.get("should_execute_exit"))
        return bool(jd.get("should_enter", True))
    if jd.get("should_enter") is not None:
        return bool(jd.get("should_enter"))
    return bool(jd.get("should_execute_exit", True))


def _backfill_joint_decision_from_policy(verdict: dict, context: dict) -> dict:
    out = dict(verdict or {})
    jd = dict((out.get("joint_decision") or {}))
    target_snapshot = _parse_variant(context.get("target_expectation_snapshot"))
    bands = _parse_variant(target_snapshot.get("bands"))
    risk_cfg = _parse_variant(context.get("execution_risk_config"))
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
        horizon_bars = target_snapshot.get("optimal_horizon_bars")
        try:
            jd["hold_bars"] = int(horizon_bars) if horizon_bars is not None else 5
        except Exception:
            jd["hold_bars"] = 5
    if jd.get("max_hold_trading_days") is None:
        max_hold_days = target_snapshot.get("max_hold_trading_days")
        try:
            jd["max_hold_trading_days"] = int(max_hold_days) if max_hold_days is not None else max(1, int(jd.get("hold_bars") or 5))
        except Exception:
            jd["max_hold_trading_days"] = max(1, int(jd.get("hold_bars") or 5))
    if jd.get("stop_loss_pct") is None:
        bust = risk_cfg.get("bust_pct")
        if bust is not None:
            try:
                jd["stop_loss_pct"] = float(bust)
            except Exception:
                pass
    jd["acceptable_early_exit_target_return"] = _normalize_early_exit_target(
        jd.get("realistic_target_return"),
        jd.get("acceptable_early_exit_target_return"),
    )
    if str(context.get("action_intent") or "ENTRY").upper() == "EXIT":
        jd["should_execute_exit"] = bool(jd.get("should_enter", True))
        jd["exit_size_factor"] = jd.get("position_size_factor")
        jd["exit_horizon_bars"] = jd.get("hold_bars")
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
        role_out = _normalize_role_output(role, role_out_raw, context.get("symbol"), action_intent=context.get("action_intent"))
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
        role_out = _normalize_role_output(role, role_out_raw, context.get("symbol"), action_intent=context.get("action_intent"))
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
    verdict = _aggregate_committee(final_outputs, action_intent=context.get("action_intent"))
    quality_reason_codes = _extract_committee_quality_reason_codes(final_outputs)
    verdict["quality_reason_codes"] = quality_reason_codes
    verdict["degraded_quality"] = len(quality_reason_codes) > 0
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
    risk_cfg = {}
    if action.get("PORTFOLIO_ID") is not None:
        try:
            cur.execute(
                """
                select BUST_PCT, MAX_SLIPPAGE_PCT, MAX_POSITION_PCT
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where PORTFOLIO_ID = %s
                limit 1
                """,
                (action.get("PORTFOLIO_ID"),),
            )
            cfg_rows = fetch_all(cur)
            if cfg_rows:
                risk_cfg = {
                    "bust_pct": float(cfg_rows[0].get("BUST_PCT")) if cfg_rows[0].get("BUST_PCT") is not None else None,
                    "max_slippage_pct": float(cfg_rows[0].get("MAX_SLIPPAGE_PCT")) if cfg_rows[0].get("MAX_SLIPPAGE_PCT") is not None else None,
                    "max_position_pct": float(cfg_rows[0].get("MAX_POSITION_PCT")) if cfg_rows[0].get("MAX_POSITION_PCT") is not None else None,
                }
        except Exception:
            risk_cfg = {}
    context = {
        "action_id": action.get("ACTION_ID"),
        "portfolio_id": action.get("PORTFOLIO_ID"),
        "symbol": symbol,
        "side": action.get("SIDE"),
        "action_intent": _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT")),
        "exit_type": action.get("EXIT_TYPE"),
        "exit_reason": action.get("EXIT_REASON"),
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
        "execution_risk_config": risk_cfg,
    }
    return context


def _extract_pw_bias(pw_evidence: dict) -> str | None:
    """
    Map top PW recommendation into a directional bias:
    - REDUCE: conservative/risk-off
    - EXPAND: aggressive/risk-on
    """
    if not isinstance(pw_evidence, dict) or not pw_evidence.get("available"):
        return None
    recs = pw_evidence.get("top_recommendations")
    if not isinstance(recs, list) or not recs:
        return None
    first = recs[0] if isinstance(recs[0], dict) else {}
    rec_type = str(first.get("recommendation_type") or "").upper()
    confidence = str(first.get("confidence_class") or "").upper()
    if confidence not in ("STRONG", "EMERGING"):
        return None
    if rec_type == "CONSERVATIVE":
        return "REDUCE"
    if rec_type == "AGGRESSIVE":
        return "EXPAND"
    return None


def _extract_committee_bias(verdict: dict) -> str | None:
    if not isinstance(verdict, dict):
        return None
    rec = str(verdict.get("recommendation") or "").upper()
    if rec in ("BLOCK", "PROCEED_REDUCED"):
        return "REDUCE"
    if rec == "PROCEED":
        return "EXPAND"
    return None


def _has_tier_c_conflict(verdict: dict, pw_evidence: dict, news_snapshot: dict) -> bool:
    """
    Tier C conflict: committee direction opposes high-confidence PW direction
    while market/news context is cautionary.
    """
    pw_bias = _extract_pw_bias(pw_evidence)
    committee_bias = _extract_committee_bias(verdict)
    if pw_bias is None or committee_bias is None or pw_bias == committee_bias:
        return False

    ns = news_snapshot if isinstance(news_snapshot, dict) else {}
    context_state = str(ns.get("context_state") or "").upper()
    event_shock = bool(ns.get("event_shock_flag"))
    risk_high = event_shock or context_state in ("CAUTIONARY", "DESTABILIZING")
    return risk_high


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


@router.post("/orders/cancel-pending")
def cancel_pending_live_orders(req: CancelPendingOrdersRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        account_id = str(req.ibkr_account_id or "").strip()
        portfolio_id = req.portfolio_id
        if not account_id:
            if portfolio_id is not None:
                cur.execute(
                    """
                    select IBKR_ACCOUNT_ID
                    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                    where PORTFOLIO_ID = %s
                    limit 1
                    """,
                    (portfolio_id,),
                )
            else:
                cur.execute(
                    """
                    select PORTFOLIO_ID, IBKR_ACCOUNT_ID
                    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                    where coalesce(IS_ACTIVE, true)
                    order by UPDATED_AT desc, PORTFOLIO_ID
                    limit 1
                    """
                )
            cfg_rows = fetch_all(cur)
            cfg_row = cfg_rows[0] if cfg_rows else {}
            if portfolio_id is None and cfg_row.get("PORTFOLIO_ID") is not None:
                portfolio_id = int(cfg_row.get("PORTFOLIO_ID"))
            account_id = str(cfg_row.get("IBKR_ACCOUNT_ID") or "").strip()
        if not account_id:
            raise HTTPException(
                status_code=400,
                detail={"message": "Cannot cancel pending orders: IBKR account is missing.", "reason_codes": ["MISSING_IBKR_ACCOUNT"]},
            )
    finally:
        conn.close()

    cancel_result = _cancel_ibkr_open_orders(
        account=account_id,
        symbol=req.symbol,
        dry_run=bool(req.dry_run),
    )

    local_rows_updated = 0
    snapshot_refresh = {"attempted": False}
    if not req.dry_run:
        try:
            snapshot_refresh = _run_on_demand_snapshot_sync(
                **_default_snapshot_sync_params(),
                account=account_id,
                portfolio_id=portfolio_id,
            )
            snapshot_refresh["attempted"] = True
        except Exception as exc:
            snapshot_refresh = {"attempted": True, "status": "FAIL", "error": str(exc)}

        if req.include_local_sync:
            canceled_ids = [
                _normalize_broker_order_id(x)
                for x in (cancel_result.get("canceled_order_ids") or [])
                if _normalize_broker_order_id(x)
            ]
            if canceled_ids:
                conn2 = get_connection()
                try:
                    cur2 = conn2.cursor()
                    placeholders = ",".join(["%s"] * len(canceled_ids))
                    cur2.execute(
                        f"""
                        update MIP.LIVE.LIVE_ORDERS
                           set STATUS = 'CANCELED',
                               LAST_UPDATED_AT = current_timestamp()
                         where IBKR_ACCOUNT_ID = %s
                           and STATUS in ('SUBMITTED','ACKNOWLEDGED','PENDINGSUBMIT','PRESUBMITTED','PARTIAL_FILL','PARTIALLYFILLED')
                           and BROKER_ORDER_ID in ({placeholders})
                        """,
                        tuple([account_id] + canceled_ids),
                    )
                    local_rows_updated = int(cur2.rowcount or 0)
                finally:
                    conn2.close()

    return {
        "ok": True,
        "account_id": account_id,
        "portfolio_id": portfolio_id,
        "symbol": (req.symbol or "").upper() or None,
        "dry_run": bool(req.dry_run),
        "actor": req.actor,
        "cancel_result": cancel_result,
        "snapshot_refresh": snapshot_refresh,
        "local_rows_updated": local_rows_updated,
    }


@router.post("/orders/{order_id}/cancel")
def cancel_single_live_order(order_id: str, req: CancelSingleOrderRequest = Body(default_factory=CancelSingleOrderRequest)):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID, BROKER_ORDER_ID, STATUS, SYMBOL
            from MIP.LIVE.LIVE_ORDERS
            where ORDER_ID = %s
            """,
            (order_id,),
        )
        rows = fetch_all(cur)
        if not rows:
            raise HTTPException(status_code=404, detail="Order not found.")
        order = rows[0]
    finally:
        conn.close()

    current_status = str(order.get("STATUS") or "").upper()
    active_statuses = {"SUBMITTED", "ACKNOWLEDGED", "PENDINGSUBMIT", "PRESUBMITTED", "PARTIAL_FILL", "PARTIALLYFILLED"}
    if current_status not in active_statuses:
        return {
            "ok": True,
            "order_id": order_id,
            "status": current_status,
            "idempotent_replay": True,
            "message": "Order is not in an active pending state.",
        }

    account_id = str(order.get("IBKR_ACCOUNT_ID") or "").strip()
    if not account_id:
        conn2 = get_connection()
        try:
            cur2 = conn2.cursor()
            cur2.execute(
                """
                select IBKR_ACCOUNT_ID
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where PORTFOLIO_ID = %s
                limit 1
                """,
                (order.get("PORTFOLIO_ID"),),
            )
            cfg_rows = fetch_all(cur2)
            account_id = str((cfg_rows[0] or {}).get("IBKR_ACCOUNT_ID") or "").strip() if cfg_rows else ""
        finally:
            conn2.close()
    if not account_id:
        raise HTTPException(
            status_code=400,
            detail={"message": "Cannot cancel order: IBKR account is missing.", "reason_codes": ["MISSING_IBKR_ACCOUNT"]},
        )

    cancel_result = _cancel_ibkr_open_orders(
        account=account_id,
        symbol=str(order.get("SYMBOL") or "").upper() or None,
        broker_order_id=_normalize_broker_order_id(order.get("BROKER_ORDER_ID")) or None,
        dry_run=bool(req.dry_run),
    )

    local_sync = None
    if not req.dry_run and req.include_local_sync:
        try:
            local_sync = update_live_order_status(
                order_id,
                UpdateLiveOrderStatusRequest(
                    actor=req.actor,
                    status="CANCELED",
                    broker_order_id=_normalize_broker_order_id(order.get("BROKER_ORDER_ID")) or None,
                    notes="single-order cancel via live orders panel",
                ),
            )
        except Exception as exc:
            local_sync = {"ok": False, "error": str(exc)}

    return {
        "ok": True,
        "order_id": order_id,
        "actor": req.actor,
        "dry_run": bool(req.dry_run),
        "account_id": account_id,
        "symbol": str(order.get("SYMBOL") or "").upper() or None,
        "broker_order_id": _normalize_broker_order_id(order.get("BROKER_ORDER_ID")) or None,
        "cancel_result": cancel_result,
        "local_sync": local_sync,
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
        if pending_only:
            # Expired pending actions are stale and must not stay in the actionable queue.
            wheres_aliased.append("coalesce(la.VALIDITY_WINDOW_END, la.CREATED_AT) >= current_timestamp()")
        params.append(limit)
        sql = f"""
        with proposer_summary as (
          select RUN_ID, SUMMARY
          from MIP.LIVE.COMMITTEE_ROLE_OUTPUT
          where ROLE_NAME = 'PROPOSER'
          qualify row_number() over (partition by RUN_ID order by CREATED_AT desc) = 1
        )
        select
          la.ACTION_ID, la.PROPOSAL_ID, la.PORTFOLIO_ID, la.SYMBOL, la.SIDE, la.ACTION_INTENT, la.EXIT_TYPE, la.EXIT_REASON, la.PROPOSED_QTY, la.PROPOSED_PRICE, la.ASSET_CLASS,
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
def get_live_activity_overview(
    limit: int = Query(200, ge=50, le=1000),
    order_limit: int = Query(120, ge=20, le=1000),
    execution_limit: int = Query(60, ge=10, le=500),
    order_lookback_days: int = Query(30, ge=1, le=365),
    snapshot_lookback_days: int = Query(14, ge=1, le=365),
):
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
                "activity_trends": {"nav": [], "positions": []},
                "ui_hints": {},
                "counts": {},
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        cfg = cfg_rows[0]
        portfolio_id = cfg.get("PORTFOLIO_ID")
        account_id = cfg.get("IBKR_ACCOUNT_ID")
        app_cfg = _read_app_config(
            cur,
            ["LIVE_AUTO_IMPORT_PROPOSALS_ON_OVERVIEW", "LIVE_AUTO_IMPORT_PROPOSAL_LIMIT"],
        )
        auto_import_enabled = str(
            app_cfg.get("LIVE_AUTO_IMPORT_PROPOSALS_ON_OVERVIEW", "true")
        ).strip().lower() not in {"0", "false", "no", "off"}
        auto_import_limit_raw = app_cfg.get("LIVE_AUTO_IMPORT_PROPOSAL_LIMIT", "200")
        try:
            auto_import_limit = int(auto_import_limit_raw)
        except Exception:
            auto_import_limit = 200
        auto_import_summary = {"attempted": False, "ok": None}
        if auto_import_enabled and portfolio_id is not None:
            auto_import_summary = _auto_import_latest_proposals_for_live_portfolio(
                int(portfolio_id),
                source_portfolio_id=int(portfolio_id),
                limit=auto_import_limit,
            )
        auto_import_summary["enabled"] = auto_import_enabled

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

        cur.execute(
            """
            select SNAPSHOT_TS, NET_LIQUIDATION_EUR, TOTAL_CASH_EUR, GROSS_POSITION_VALUE_EUR
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'NAV'
              and IBKR_ACCOUNT_ID = %s
              and SNAPSHOT_TS >= dateadd(day, -%s, current_timestamp())
            order by SNAPSHOT_TS asc
            """,
            (account_id, snapshot_lookback_days),
        )
        nav_trend_rows = fetch_all(cur)
        nav_trend = [
            {
                "snapshot_ts": r.get("SNAPSHOT_TS"),
                "nav_eur": float(r.get("NET_LIQUIDATION_EUR")) if r.get("NET_LIQUIDATION_EUR") is not None else None,
                "cash_eur": float(r.get("TOTAL_CASH_EUR")) if r.get("TOTAL_CASH_EUR") is not None else None,
                "gross_exposure_eur": float(r.get("GROSS_POSITION_VALUE_EUR")) if r.get("GROSS_POSITION_VALUE_EUR") is not None else None,
            }
            for r in nav_trend_rows
        ]

        cur.execute(
            """
            select
              SNAPSHOT_TS,
              sum(coalesce(UNREALIZED_PNL, 0)) as TOTAL_UNREALIZED_PNL,
              sum(abs(coalesce(MARKET_VALUE, 0))) as TOTAL_MARKET_VALUE,
              count_if(coalesce(POSITION_QTY, 0) <> 0) as OPEN_POSITION_COUNT
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'POSITION'
              and IBKR_ACCOUNT_ID = %s
              and SNAPSHOT_TS >= dateadd(day, -%s, current_timestamp())
            group by SNAPSHOT_TS
            order by SNAPSHOT_TS asc
            """,
            (account_id, snapshot_lookback_days),
        )
        position_trend_rows = fetch_all(cur)
        position_trend = [
            {
                "snapshot_ts": r.get("SNAPSHOT_TS"),
                "total_unrealized_pnl": float(r.get("TOTAL_UNREALIZED_PNL")) if r.get("TOTAL_UNREALIZED_PNL") is not None else None,
                "total_market_value": float(r.get("TOTAL_MARKET_VALUE")) if r.get("TOTAL_MARKET_VALUE") is not None else None,
                "open_position_count": int(r.get("OPEN_POSITION_COUNT") or 0),
            }
            for r in position_trend_rows
        ]

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
              and coalesce(LAST_UPDATED_AT, CREATED_AT) >= dateadd(day, -%s, current_timestamp())
            order by LAST_UPDATED_AT desc, CREATED_AT desc
            limit %s
            """,
            (portfolio_id, order_lookback_days, order_limit),
        )
        orders = fetch_all(cur)
        broker_open_order_ids = {_normalize_broker_order_id(r.get("OPEN_ORDER_ID")) for r in open_orders if _normalize_broker_order_id(r.get("OPEN_ORDER_ID"))}
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
                    "side": ord_row.get("SIDE"),
                    "limit_price": float(ord_row.get("LIMIT_PRICE")) if ord_row.get("LIMIT_PRICE") is not None else None,
                    "avg_fill_price": float(ord_row.get("AVG_FILL_PRICE")) if ord_row.get("AVG_FILL_PRICE") is not None else None,
                    "qty_ordered": float(ord_row.get("QTY_ORDERED")) if ord_row.get("QTY_ORDERED") is not None else None,
                    "qty_filled": float(ord_row.get("QTY_FILLED")) if ord_row.get("QTY_FILLED") is not None else None,
                    "broker_truth_active": _is_order_active_in_broker_truth(ord_row, broker_open_order_ids),
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
              la.ACTION_ID, la.PROPOSAL_ID, la.SYMBOL, la.SIDE, la.ACTION_INTENT, la.EXIT_TYPE, la.STATUS, la.COMPLIANCE_STATUS,
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
                'OPEN_BLOCKED','PENDING_OPEN_STABILITY_REVIEW','READY_FOR_APPROVAL_FLOW','PM_ACCEPTED','COMPLIANCE_APPROVED',
                'INTENT_SUBMITTED','INTENT_APPROVED','REVALIDATED_PASS','REVALIDATED_FAIL','EXECUTION_REQUESTED'
              )
            order by la.CREATED_AT desc
            limit %s
            """,
            (portfolio_id, limit),
        )
        action_rows = fetch_all(cur)
        action_meta_by_id: dict[str, dict] = {}
        for a in action_rows:
            action_id_key = str(a.get("ACTION_ID") or "")
            if not action_id_key:
                continue
            action_meta_by_id[action_id_key] = {
                "action_intent": _normalize_action_intent(a.get("SIDE"), a.get("ACTION_INTENT")),
                "side": str(a.get("SIDE") or "").upper(),
            }

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
        market_open = _is_extended_trading_open_ny(now_utc)
        open_utc, close_utc = _extended_trading_bounds_utc(now_utc)
        snapshot_state = _compute_snapshot_freshness_state(snapshot_age_sec, cfg.get("SNAPSHOT_FRESHNESS_THRESHOLD_SEC"))
        drift_state = _compute_drift_state(cfg.get("DRIFT_STATUS"), unresolved_drift_count)
        page_actionable = snapshot_state in ("FRESH", "AGING") and drift_state != "BLOCKED" and market_open
        held_symbols = {str((r.get("SYMBOL") or "")).upper() for r in open_positions}
        nav_eur = float(nav.get("NET_LIQUIDATION_EUR") or 0.0) if nav else 0.0

        pending_decisions = []
        suppress_pending_symbols: set[str] = set()
        for row in action_rows:
            symbol = str(row.get("SYMBOL") or "").upper()
            if not symbol:
                continue
            status = (row.get("STATUS") or "").upper()
            action_intent = _normalize_action_intent(row.get("SIDE"), row.get("ACTION_INTENT"))
            is_exit = action_intent == "EXIT"
            action_id = str(row.get("ACTION_ID") or "")
            action_orders = order_groups.get(action_id) or []
            has_active_order = any(_is_order_active_in_broker_truth(o, broker_open_order_ids) for o in action_orders)
            if has_active_order or ((symbol in held_symbols) and not is_exit):
                suppress_pending_symbols.add(symbol)

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
            committee_should_enter = joint_decision.get("should_enter")
            committee_blocks_entry = (action_intent != "EXIT") and (committee_should_enter is False)
            hard_block_codes = {
                "MAX_POSITIONS_EXCEEDED",
                "MAX_POSITION_PCT_EXCEEDED",
                "CASH_BUFFER_BREACH",
                "MISSING_NOTIONAL_INPUT",
                "EXIT_POSITION_MISSING",
                "LIVE_TP_REQUIRED_MISSING",
                "LIVE_SL_REQUIRED_MISSING",
                "LIVE_BRACKET_REQUIRED",
                "LIVE_TP_NET_EDGE_TOO_LOW",
                "LIVE_RISK_REWARD_TOO_LOW",
                "BROKER_SHORT_POSITION_OUT_OF_POLICY",
                "SYMBOL_SHORT_POSITION_OUT_OF_POLICY",
                "ENTRY_SIDE_NOT_ALLOWED_LONG_ONLY",
                "FIRST_SESSION_REALISM_SOURCE_REQUIRED",
                "FIRST_SESSION_REALISM_NO_1M_BAR",
                "FIRST_SESSION_REALISM_MISSING_1M_REFERENCE",
                "FIRST_SESSION_REALISM_1M_STALE",
                "FIRST_SESSION_REALISM_REVALIDATION_NOT_LATEST",
                "NEWS_EXECUTION_BLOCKED_EVENT_SHOCK",
                "NEWS_EXECUTION_CAUTION",
            }
            if action_intent == "EXIT":
                hard_block_codes -= {
                    "FIRST_SESSION_REALISM_SOURCE_REQUIRED",
                    "FIRST_SESSION_REALISM_NO_1M_BAR",
                    "FIRST_SESSION_REALISM_MISSING_1M_REFERENCE",
                    "FIRST_SESSION_REALISM_1M_STALE",
                    "FIRST_SESSION_REALISM_REVALIDATION_NOT_LATEST",
                }
            execution_hard_blocked = bool(
                action_reason_codes
                and any(
                    str(x).upper() in hard_block_codes
                    for x in action_reason_codes
                )
            )
            submit_allowed = status in ("INTENT_APPROVED", "REVALIDATED_FAIL", "REVALIDATED_PASS", "COMPLIANCE_APPROVED", "INTENT_SUBMITTED", "PM_ACCEPTED", "READY_FOR_APPROVAL_FLOW")
            submit_allowed = submit_allowed and page_actionable and (not blocked) and (not execution_hard_blocked) and (not committee_blocks_entry)
            in_position = symbol in held_symbols
            action_intent = _normalize_action_intent(row.get("SIDE"), row.get("ACTION_INTENT"))
            is_exit = action_intent == "EXIT"
            action_id = str(row.get("ACTION_ID") or "")
            action_orders = order_groups.get(action_id) or []
            has_active_order = any(_is_order_active_in_broker_truth(o, broker_open_order_ids) for o in action_orders)
            protection_details = protection_by_action.get(action_id) or {"state": "NONE", "parent": None, "take_profit": None, "stop_loss": None}
            protection_planned = bool(
                joint_decision.get("realistic_target_return") is not None
                or joint_decision.get("acceptable_early_exit_target_return") is not None
            )

            if (
                status != "EXECUTION_REQUESTED"
                and (not has_active_order)
                and (is_exit or not in_position)
                and (is_exit or symbol not in suppress_pending_symbols)
            ):
                pending_decisions.append(
                    {
                        "action_id": row.get("ACTION_ID"),
                        "proposal_id": row.get("PROPOSAL_ID"),
                        "symbol": row.get("SYMBOL"),
                        "side": row.get("SIDE"),
                        "action_intent": action_intent,
                        "exit_type": row.get("EXIT_TYPE"),
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
                        "execution_hard_blocked": bool(execution_hard_blocked),
                        "committee_should_enter": committee_should_enter,
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

        # Keep one pending row per symbol (most relevant/latest) to avoid queue bloat in UI.
        pending_by_symbol: dict[str, dict] = {}
        for row in pending_decisions:
            symbol_key = str(row.get("symbol") or "").upper().strip()
            if not symbol_key:
                continue
            existing = pending_by_symbol.get(symbol_key)
            if existing is None:
                pending_by_symbol[symbol_key] = row
                continue

            row_allowed = bool(row.get("submission_allowed"))
            existing_allowed = bool(existing.get("submission_allowed"))
            if row_allowed and not existing_allowed:
                pending_by_symbol[symbol_key] = row
                continue
            if existing_allowed and not row_allowed:
                continue

            row_ts = (row.get("timestamps") or {}).get("updated_at") or (row.get("timestamps") or {}).get("created_at")
            existing_ts = (existing.get("timestamps") or {}).get("updated_at") or (existing.get("timestamps") or {}).get("created_at")
            if row_ts and existing_ts:
                try:
                    if row_ts > existing_ts:
                        pending_by_symbol[symbol_key] = row
                except Exception:
                    pass
            elif row_ts and not existing_ts:
                pending_by_symbol[symbol_key] = row

        pending_decisions = list(pending_by_symbol.values())

        execution_rows_ib = []
        try:
            cur.execute(
                """
                select
                  SNAPSHOT_TS, SYMBOL, SECURITY_TYPE, OPEN_ORDER_ID, OPEN_ORDER_FILLED,
                  OPEN_ORDER_LIMIT_PRICE, AVG_COST, REALIZED_PNL, PAYLOAD
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'EXECUTION'
                  and IBKR_ACCOUNT_ID = %s
                  and SNAPSHOT_TS >= dateadd(day, -%s, current_timestamp())
                order by SNAPSHOT_TS desc
                limit %s
                """,
                (account_id, order_lookback_days, max(int(execution_limit) * 20, 200)),
            )
            execution_rows_ib = fetch_all(cur)
        except Exception:
            execution_rows_ib = []

        broker_order_to_action: dict[str, str] = {}
        for order in orders:
            broker_id = _normalize_broker_order_id(order.get("BROKER_ORDER_ID"))
            if broker_id:
                broker_order_to_action.setdefault(broker_id, str(order.get("ACTION_ID") or ""))
        position_basis_by_symbol: dict[str, dict] = {}
        try:
            cur.execute(
                """
                with ranked as (
                    select
                      upper(SYMBOL) as SYMBOL,
                      POSITION_QTY,
                      AVG_COST,
                      SNAPSHOT_TS,
                      row_number() over (
                        partition by upper(SYMBOL)
                        order by SNAPSHOT_TS desc
                      ) as RN
                    from MIP.LIVE.BROKER_SNAPSHOTS
                    where SNAPSHOT_TYPE = 'POSITION'
                      and IBKR_ACCOUNT_ID = %s
                      and coalesce(POSITION_QTY, 0) <> 0
                      and SNAPSHOT_TS >= dateadd(day, -%s, current_timestamp())
                )
                select SYMBOL, POSITION_QTY, AVG_COST, SNAPSHOT_TS
                from ranked
                where RN = 1
                """,
                (account_id, order_lookback_days),
            )
            for b in fetch_all(cur):
                symbol_key = str(b.get("SYMBOL") or "").upper()
                if not symbol_key:
                    continue
                position_basis_by_symbol[symbol_key] = b
        except Exception:
            position_basis_by_symbol = {}

        executions_ib = []
        seen_ib_exec_keys: set[str] = set()
        for row in execution_rows_ib:
            payload = _parse_variant(row.get("PAYLOAD"))
            exec_id = str(payload.get("exec_id") or "").strip()
            symbol = str(row.get("SYMBOL") or payload.get("symbol") or "").upper()
            if not symbol:
                continue
            broker_order_id = (
                _normalize_broker_order_id(row.get("OPEN_ORDER_ID"))
                or _normalize_broker_order_id(payload.get("perm_id"))
                or _normalize_broker_order_id(payload.get("order_id"))
            )
            qty_filled_raw = payload.get("shares")
            if qty_filled_raw is None:
                qty_filled_raw = row.get("OPEN_ORDER_FILLED")
            if qty_filled_raw is None:
                qty_filled_raw = row.get("POSITION_QTY")
            qty_filled = abs(float(qty_filled_raw)) if qty_filled_raw is not None else None
            avg_fill_price = payload.get("price")
            if avg_fill_price is None:
                avg_fill_price = row.get("OPEN_ORDER_LIMIT_PRICE")
            if avg_fill_price is None:
                avg_fill_price = row.get("AVG_COST")
            side_raw = str(payload.get("side") or "").upper().strip()
            if side_raw in ("BUY", "BOT", "B"):
                side = "BUY"
            elif side_raw in ("SELL", "SLD", "S"):
                side = "SELL"
            else:
                side = "BUY" if (qty_filled_raw is not None and float(qty_filled_raw) >= 0) else "SELL"
            execution_ts = payload.get("time") or row.get("SNAPSHOT_TS")
            realized_pnl = payload.get("realized_pnl")
            if realized_pnl is None:
                realized_pnl = row.get("REALIZED_PNL")
            realized_pnl_is_estimate = False
            action_id = broker_order_to_action.get(broker_order_id) if broker_order_id else None
            action_meta = action_meta_by_id.get(str(action_id or "")) or {}
            basis = position_basis_by_symbol.get(symbol) or {}
            basis_qty = float(basis.get("POSITION_QTY") or 0.0) if basis.get("POSITION_QTY") is not None else 0.0
            execution_context = None
            if side == "BUY" and basis_qty < 0:
                execution_context = "CLOSE_SHORT"
            elif side == "SELL" and basis_qty > 0:
                execution_context = "CLOSE_LONG"
            elif side == "BUY":
                execution_context = "OPEN_OR_ADD_LONG"
            elif side == "SELL":
                execution_context = "OPEN_OR_ADD_SHORT"
            if (
                (realized_pnl is None or abs(float(realized_pnl)) < 1e-12)
                and qty_filled is not None
                and avg_fill_price is not None
            ):
                try:
                    basis_cost = float(basis.get("AVG_COST")) if basis.get("AVG_COST") is not None else None
                    est_pnl = None
                    if basis_cost is not None:
                        if side == "BUY" and basis_qty < 0:
                            # Buy-to-cover short.
                            est_pnl = (basis_cost - float(avg_fill_price)) * float(qty_filled)
                        elif side == "SELL" and basis_qty > 0:
                            # Sell-to-close long.
                            est_pnl = (float(avg_fill_price) - basis_cost) * float(qty_filled)
                    if est_pnl is not None:
                        realized_pnl = float(est_pnl)
                        realized_pnl_is_estimate = True
                except Exception:
                    pass
            dedupe_key = exec_id or f"{broker_order_id}:{symbol}:{execution_ts}:{qty_filled}:{avg_fill_price}"
            if dedupe_key in seen_ib_exec_keys:
                continue
            seen_ib_exec_keys.add(dedupe_key)
            market_type = "FX" if "/" in symbol else str(row.get("SECURITY_TYPE") or "").upper()
            executions_ib.append(
                {
                    "order_id": exec_id or broker_order_id or f"IB_EXEC_{len(executions_ib)+1}",
                    "action_id": action_id,
                    "broker_order_id": broker_order_id,
                    "symbol": symbol,
                    "market_type": market_type or None,
                    "side": side,
                    "action_intent": action_meta.get("action_intent"),
                    "execution_context": execution_context,
                    "qty_filled": qty_filled,
                    "avg_fill_price": float(avg_fill_price) if avg_fill_price is not None else None,
                    "realized_pnl": float(realized_pnl) if realized_pnl is not None else None,
                    "realized_pnl_is_estimate": bool(realized_pnl_is_estimate),
                    "status": "FILLED",
                    "execution_ts": execution_ts,
                    "source": "IBKR_SNAPSHOT_EXECUTION",
                }
            )

        executions_local = []
        for order in orders:
            status = (order.get("STATUS") or "").upper()
            qty_filled = order.get("QTY_FILLED")
            is_exec = status in ("PARTIAL_FILL", "FILLED") or (qty_filled is not None and float(qty_filled) > 0)
            if not is_exec:
                continue
            executions_local.append(
                {
                    "order_id": order.get("ORDER_ID"),
                    "action_id": order.get("ACTION_ID"),
                    "broker_order_id": order.get("BROKER_ORDER_ID"),
                    "symbol": order.get("SYMBOL"),
                    "side": order.get("SIDE"),
                    "action_intent": _normalize_action_intent(order.get("SIDE"), order.get("ACTION_INTENT")),
                    "execution_context": None,
                    "qty_filled": float(qty_filled) if qty_filled is not None else None,
                    "avg_fill_price": float(order.get("AVG_FILL_PRICE")) if order.get("AVG_FILL_PRICE") is not None else None,
                    "realized_pnl": None,
                    "realized_pnl_is_estimate": False,
                    "status": order.get("STATUS"),
                    "execution_ts": order.get("FILLED_AT") or order.get("LAST_UPDATED_AT"),
                    "source": "MIP_BROKER_LEDGER",
                }
            )

        executions = list(executions_ib)
        seen_combined = {
            f"{str(e.get('broker_order_id') or '')}:{str(e.get('symbol') or '').upper()}:{str(e.get('execution_ts') or '')}:{str(e.get('qty_filled') or '')}"
            for e in executions
        }
        for local_exec in executions_local:
            local_key = (
                f"{str(local_exec.get('broker_order_id') or '')}:{str(local_exec.get('symbol') or '').upper()}:"
                f"{str(local_exec.get('execution_ts') or '')}:{str(local_exec.get('qty_filled') or '')}"
            )
            if local_key in seen_combined:
                continue
            executions.append(local_exec)
        executions = executions[:execution_limit]

        orders_enriched = []
        for ord_row in orders:
            action_key = str(ord_row.get("ACTION_ID") or "")
            status_upper = str(ord_row.get("STATUS") or "").upper()
            broker_truth_active = _is_order_active_in_broker_truth(ord_row, broker_open_order_ids)
            symbol_upper = str(ord_row.get("SYMBOL") or "").upper()
            broker_truth_in_position = symbol_upper in held_symbols
            status_for_display = ord_row.get("STATUS")
            if status_upper in LIVE_ORDER_ACTIVE_STATUSES and not broker_truth_active and not broker_truth_in_position:
                # Enforce IBKR truth in display: local active states must be broker-confirmed.
                status_for_display = "NOT_ACTIVE_AT_BROKER"
            orders_enriched.append(
                {
                    **ord_row,
                    "STATUS": status_for_display,
                    "BROKER_TRUTH_ACTIVE": broker_truth_active,
                    "BROKER_TRUTH_IN_POSITION": broker_truth_in_position,
                    "PROTECTION": protection_by_action.get(action_key)
                    or {"state": "NONE", "parent": None, "take_profit": None, "stop_loss": None},
                }
            )

        nav_change_abs = None
        nav_change_pct = None
        if nav_trend and nav_trend[0].get("nav_eur") is not None and nav_trend[-1].get("nav_eur") is not None:
            start_nav = float(nav_trend[0]["nav_eur"])
            end_nav = float(nav_trend[-1]["nav_eur"])
            nav_change_abs = end_nav - start_nav
            nav_change_pct = (nav_change_abs / start_nav) if start_nav else None

        pnl_change_abs = None
        if position_trend and position_trend[0].get("total_unrealized_pnl") is not None and position_trend[-1].get("total_unrealized_pnl") is not None:
            pnl_change_abs = float(position_trend[-1]["total_unrealized_pnl"]) - float(position_trend[0]["total_unrealized_pnl"])

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
                "trend_nav_change_abs": nav_change_abs,
                "trend_nav_change_pct": nav_change_pct,
                "trend_unrealized_change_abs": pnl_change_abs,
                "trend_window_days": snapshot_lookback_days,
                "trend_points": len(nav_trend),
            },
            "activity_trends": {
                "nav": serialize_rows(nav_trend),
                "positions": serialize_rows(position_trend),
            },
            "ui_hints": {
                "order_lookback_days": order_lookback_days,
                "order_limit": order_limit,
                "execution_limit": execution_limit,
                "snapshot_lookback_days": snapshot_lookback_days,
                "auto_import_latest_proposals": auto_import_summary,
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


@router.post("/positions/exit-action")
def create_exit_action_from_position(req: CreateExitActionRequest):
    symbol = str(req.symbol or "").upper().strip()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required.")

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select PORTFOLIO_ID, IBKR_ACCOUNT_ID, VALIDITY_WINDOW_SEC
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
              and coalesce(IS_ACTIVE, true) = true
            limit 1
            """,
            (int(req.portfolio_id),),
        )
        cfg_rows = fetch_all(cur)
        if not cfg_rows:
            raise HTTPException(status_code=404, detail="Active live portfolio config not found.")
        cfg = cfg_rows[0]
        account_id = str(cfg.get("IBKR_ACCOUNT_ID") or "").strip()
        validity_window_sec = int(cfg.get("VALIDITY_WINDOW_SEC") or 14400)
        if not account_id:
            raise HTTPException(
                status_code=409,
                detail={"message": "IBKR account missing for portfolio.", "reason_codes": ["MISSING_IBKR_ACCOUNT"]},
            )

        cur.execute(
            """
            select ACTION_ID, STATUS
            from MIP.LIVE.LIVE_ACTIONS
            where PORTFOLIO_ID = %s
              and upper(coalesce(SYMBOL, '')) = %s
              and upper(coalesce(ACTION_INTENT, iff(upper(coalesce(SIDE,''))='SELL','EXIT','ENTRY'))) = 'EXIT'
              and STATUS in (
                'PENDING_OPEN_VALIDATION','OPEN_CAUTION','OPEN_ELIGIBLE','PENDING_OPEN_STABILITY_REVIEW',
                'READY_FOR_APPROVAL_FLOW','PM_ACCEPTED','COMPLIANCE_APPROVED','INTENT_SUBMITTED',
                'INTENT_APPROVED','REVALIDATED_PASS','REVALIDATED_FAIL','EXECUTION_REQUESTED','EXECUTION_PARTIAL'
              )
            order by CREATED_AT desc
            limit 1
            """,
            (int(req.portfolio_id), symbol),
        )
        existing_rows = fetch_all(cur)
        if existing_rows:
            existing = existing_rows[0]
            return {
                "ok": True,
                "idempotent_replay": True,
                "action_id": existing.get("ACTION_ID"),
                "status": existing.get("STATUS"),
                "symbol": symbol,
                "message": "Existing active exit action reused.",
            }

        cur.execute(
            """
            select POSITION_QTY, AVG_COST, SNAPSHOT_TS
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'POSITION'
              and IBKR_ACCOUNT_ID = %s
              and SNAPSHOT_TS = (
                select max(SNAPSHOT_TS)
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'POSITION'
                  and IBKR_ACCOUNT_ID = %s
              )
              and upper(SYMBOL) = upper(%s)
            limit 1
            """,
            (account_id, account_id, symbol),
        )
        pos_rows = fetch_all(cur)
        if not pos_rows:
            raise HTTPException(
                status_code=404,
                detail={"message": "No broker position found for symbol.", "reason_codes": ["EXIT_POSITION_MISSING"]},
            )
        pos = pos_rows[0]
        position_qty = float(pos.get("POSITION_QTY") or 0.0)
        if abs(position_qty) <= 0:
            raise HTTPException(
                status_code=409,
                detail={"message": "Position quantity is zero.", "reason_codes": ["EXIT_POSITION_MISSING"]},
            )
        side = "SELL" if position_qty > 0 else "BUY"
        max_qty = abs(position_qty)
        qty = float(req.qty) if req.qty is not None else max_qty
        qty = max(min(abs(qty), max_qty), 0.0)
        if qty <= 0:
            raise HTTPException(status_code=400, detail="qty must be > 0.")

        proposed_price = None
        cur.execute(
            """
            select CLOSE
            from MIP.MART.MARKET_BARS
            where SYMBOL = %s
              and SOURCE = 'IBKR'
              and INTERVAL_MINUTES = 1
            order by TS desc
            limit 1
            """,
            (symbol,),
        )
        bar_rows = fetch_all(cur)
        if bar_rows and bar_rows[0].get("CLOSE") is not None:
            proposed_price = float(bar_rows[0].get("CLOSE"))
        else:
            cur.execute(
                """
                select CLOSE
                from MIP.MART.MARKET_BARS
                where SYMBOL = %s
                  and SOURCE = 'IBKR'
                  and INTERVAL_MINUTES in (15, 60, 1440)
                order by TS desc
                limit 1
                """,
                (symbol,),
            )
            fallback_rows = fetch_all(cur)
            if fallback_rows and fallback_rows[0].get("CLOSE") is not None:
                proposed_price = float(fallback_rows[0].get("CLOSE"))
        if proposed_price is None:
            proposed_price = float(pos.get("AVG_COST") or 0.0) or None

        action_id = str(uuid.uuid4())
        reason_codes = ["MANUAL_EXIT_REQUESTED"]
        reason_text = (req.reason or "Manual exit requested from open position").strip()
        param_snapshot = {
            "source": "BROKER_POSITION_EXIT",
            "actor": req.actor,
            "snapshot_ts": str(pos.get("SNAPSHOT_TS")) if pos.get("SNAPSHOT_TS") is not None else None,
            "position_qty": position_qty,
            "max_exit_qty": max_qty,
            "requested_qty": qty,
            "reason": reason_text,
        }
        cur.execute(
            """
            insert into MIP.LIVE.LIVE_ACTIONS (
              ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, SIDE, ACTION_INTENT, EXIT_TYPE, EXIT_REASON,
              PROPOSED_QTY, PROPOSED_PRICE, ASSET_CLASS, STATUS, VALIDITY_WINDOW_END, COMPLIANCE_STATUS,
              PARAM_SNAPSHOT, REASON_CODES, COMMITTEE_REQUIRED, COMMITTEE_STATUS, CREATED_AT, UPDATED_AT
            )
            select
              %s, null, %s, %s, %s, 'EXIT', 'MANUAL', %s,
              %s, %s, null, 'READY_FOR_APPROVAL_FLOW', dateadd(second, %s, current_timestamp()), 'PENDING',
              parse_json(%s), parse_json(%s), true, 'PENDING', current_timestamp(), current_timestamp()
            """,
            (
                action_id,
                int(req.portfolio_id),
                symbol,
                side,
                reason_text,
                qty,
                proposed_price,
                validity_window_sec,
                json.dumps(param_snapshot),
                json.dumps(reason_codes),
            ),
        )

        result = {
            "ok": True,
            "action_id": action_id,
            "status": "READY_FOR_APPROVAL_FLOW",
            "symbol": symbol,
            "side": side,
            "action_intent": "EXIT",
            "exit_type": "MANUAL",
            "qty": qty,
            "proposed_price": proposed_price,
            "message": "Manual exit action created.",
        }

        if req.auto_submit:
            submit_result = approve_and_submit_live_decision(
                action_id,
                ApproveAndSubmitLiveDecisionRequest(force_refresh_1m=req.force_refresh_1m),
            )
            result["auto_submit"] = submit_result

        return result
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

    committee_result = None
    if status in ("OPEN_ELIGIBLE", "OPEN_CAUTION", "PENDING_OPEN_STABILITY_REVIEW", "READY_FOR_APPROVAL_FLOW"):
        committee_result = run_live_trade_committee(
            action_id,
            CommitteeRunRequest(
                actor=req.committee_actor,
                model=req.committee_model,
                force_rerun=req.committee_recheck_before_submit,
                refresh_ibkr_news=req.committee_refresh_ibkr_news,
                ibkr_news_max_symbols=req.committee_ibkr_news_max_symbols,
                ibkr_news_max_headlines_per_symbol=req.committee_ibkr_news_max_headlines_per_symbol,
                ibkr_news_min_symbols_covered=req.committee_ibkr_news_min_symbols_covered,
                ibkr_news_max_age_minutes=req.committee_ibkr_news_max_age_minutes,
            ),
        )
        steps.append("committee_run")
        action = refresh_state()
        status = (action.get("STATUS") or "").upper()
        if status == "OPEN_BLOCKED":
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Decision blocked in committee/opening stage.",
                    "reason_codes": _parse_list_variant(action.get("REASON_CODES")),
                    "committee_news": (committee_result or {}).get("news_runtime"),
                },
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
        # Deterministic pre-execution hardening: refresh broker snapshot only when stale.
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                select IBKR_ACCOUNT_ID, VALIDITY_WINDOW_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where PORTFOLIO_ID = %s
                """,
                (action.get("PORTFOLIO_ID"),),
            )
            cfg_rows = fetch_all(cur)
            cfg = cfg_rows[0] if cfg_rows else {}
            account_id = cfg.get("IBKR_ACCOUNT_ID")
            validity_sec = int(cfg.get("VALIDITY_WINDOW_SEC") or 14400)
            snapshot_freshness_threshold_sec = int(cfg.get("SNAPSHOT_FRESHNESS_THRESHOLD_SEC") or 300)

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
            latest_snapshot_ts = (nav_rows[0] or {}).get("SNAPSHOT_TS") if nav_rows else None
        finally:
            conn.close()

        if not account_id:
            raise HTTPException(
                status_code=409,
                detail={"message": "Cannot refresh broker snapshot: IBKR account is missing.", "reason_codes": ["MISSING_IBKR_ACCOUNT"]},
            )

        snapshot_is_fresh = False
        if latest_snapshot_ts and hasattr(latest_snapshot_ts, "replace"):
            snapshot_age_sec = int((datetime.now(timezone.utc) - latest_snapshot_ts.replace(tzinfo=timezone.utc)).total_seconds())
            snapshot_is_fresh = snapshot_age_sec <= snapshot_freshness_threshold_sec

        if snapshot_is_fresh:
            steps.append("snapshot_already_fresh")
        else:
            try:
                _run_on_demand_snapshot_sync(
                    **_default_snapshot_sync_params(),
                    account=account_id,
                    portfolio_id=action.get("PORTFOLIO_ID"),
                )
                steps.append("snapshot_refresh")
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(
                    status_code=409,
                    detail={"message": "Snapshot refresh failed before execution.", "reason_codes": ["SNAPSHOT_REFRESH_FAILED"], "error": str(exc)},
                )

        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                update MIP.LIVE.LIVE_ACTIONS
                   set VALIDITY_WINDOW_END = dateadd(second, %s, current_timestamp()),
                       UPDATED_AT = current_timestamp()
                 where ACTION_ID = %s
                """,
                (validity_sec, action_id),
            )
        finally:
            conn.close()
        steps.append("validity_renew")

        # Re-run revalidation right before execution so 1m realism checks use freshest bar reference.
        revalidate_live_action(
            action_id,
            RevalidateLiveActionRequest(force_refresh_1m=True),
        )
        steps.append("pre_execute_revalidate")
        action = refresh_state()
        status = (action.get("STATUS") or "").upper()
        if status != "REVALIDATED_PASS":
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Pre-execution revalidation failed.",
                    "status": status,
                    "reason_codes": _parse_list_variant(action.get("REASON_CODES")),
                    "steps": steps,
                },
            )

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
            "committee_result": committee_result,
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


@router.post("/decisions/{action_id}/approve-flow")
def approve_live_decision_flow(action_id: str, req: ApproveLiveDecisionRequest):
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

    if status == "INTENT_APPROVED":
        return {
            "ok": True,
            "action_id": action_id,
            "status": status,
            "steps": steps,
            "reason_codes": _parse_list_variant(action.get("REASON_CODES")),
            "next_required_step": "Run committee revalidation before submit",
        }

    raise HTTPException(
        status_code=409,
        detail={
            "message": "Decision is not in approval-flow stage.",
            "status": status,
            "reason_codes": _parse_list_variant(action.get("REASON_CODES")),
            "steps": steps,
            "next_required_step": _required_next_step_for_status(status),
        },
    )


@router.post("/decisions/{action_id}/submit-only")
def submit_live_decision_only(action_id: str, req: SubmitLiveDecisionRequest):
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

    # Streamlined UX: if user presses Submit from pre-intent states, auto-run approval chain.
    if status in ("READY_FOR_APPROVAL_FLOW", "PM_ACCEPTED", "COMPLIANCE_APPROVED", "INTENT_SUBMITTED"):
        approve_live_decision_flow(
            action_id,
            ApproveLiveDecisionRequest(),
        )
        steps.append("auto_approve_flow")
        action = refresh_state()
        status = (action.get("STATUS") or "").upper()

    if status != "REVALIDATED_PASS":
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Submit requires manual revalidation pass.",
                "status": status,
                "reason_codes": _parse_list_variant(action.get("REASON_CODES")),
                "next_required_step": _required_next_step_for_status(status),
            },
        )

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select IBKR_ACCOUNT_ID, VALIDITY_WINDOW_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            """,
            (action.get("PORTFOLIO_ID"),),
        )
        cfg_rows = fetch_all(cur)
        cfg = cfg_rows[0] if cfg_rows else {}
        account_id = cfg.get("IBKR_ACCOUNT_ID")
        validity_sec = int(cfg.get("VALIDITY_WINDOW_SEC") or 14400)
        snapshot_freshness_threshold_sec = int(cfg.get("SNAPSHOT_FRESHNESS_THRESHOLD_SEC") or 300)

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
        latest_snapshot_ts = (nav_rows[0] or {}).get("SNAPSHOT_TS") if nav_rows else None
    finally:
        conn.close()

    if not account_id:
        raise HTTPException(
            status_code=409,
            detail={"message": "Cannot refresh broker snapshot: IBKR account is missing.", "reason_codes": ["MISSING_IBKR_ACCOUNT"]},
        )

    snapshot_is_fresh = False
    if latest_snapshot_ts and hasattr(latest_snapshot_ts, "replace"):
        snapshot_age_sec = int((datetime.now(timezone.utc) - latest_snapshot_ts.replace(tzinfo=timezone.utc)).total_seconds())
        snapshot_is_fresh = snapshot_age_sec <= snapshot_freshness_threshold_sec

    if snapshot_is_fresh:
        steps.append("snapshot_already_fresh")
    else:
        try:
            _run_on_demand_snapshot_sync(
                **_default_snapshot_sync_params(),
                account=account_id,
                portfolio_id=action.get("PORTFOLIO_ID"),
            )
            steps.append("snapshot_refresh")
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=409,
                detail={"message": "Snapshot refresh failed before execution.", "reason_codes": ["SNAPSHOT_REFRESH_FAILED"], "error": str(exc)},
            )

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set VALIDITY_WINDOW_END = dateadd(second, %s, current_timestamp()),
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (validity_sec, action_id),
        )
    finally:
        conn.close()
    steps.append("validity_renew")

    # Safety-critical: stale realism must fail submission; user must revalidate manually again.
    action = refresh_state()
    status = (action.get("STATUS") or "").upper()
    if status != "REVALIDATED_PASS":
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Submit requires latest manual revalidation pass.",
                "status": status,
                "reason_codes": _parse_list_variant(action.get("REASON_CODES")),
                "steps": steps,
            },
        )

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
        # Opening-gate block means committee should be treated as skipped-at-gate, not still pending.
        if (
            action_after
            and str(action_after.get("STATUS") or "").upper() == "OPEN_BLOCKED"
            and str(action_after.get("COMMITTEE_STATUS") or "").upper() != "COMPLETED"
        ):
            cur.execute(
                """
                update MIP.LIVE.LIVE_ACTIONS
                   set COMMITTEE_STATUS = 'SKIPPED',
                       COMMITTEE_VERDICT = 'BLOCK_OPENING_GUARD',
                       COMMITTEE_RUN_ID = null,
                       COMMITTEE_COMPLETED_TS = current_timestamp(),
                       UPDATED_AT = current_timestamp()
                 where ACTION_ID = %s
                """,
                (action_id,),
            )
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
        news_ingest = {"attempted": False, "status": "SKIPPED", "reason": "DISABLED"}
        news_recompute = None
        news_readiness = {
            "ready": False,
            "reason_codes": ["IBKR_NEWS_NOT_CHECKED"],
            "feed_health": None,
            "symbol_coverage": None,
        }
        news_fallback_active = True
        if req.refresh_ibkr_news:
            news_ingest = _run_agent_ibkr_news_refresh(
                max_symbols=req.ibkr_news_max_symbols,
                max_headlines_per_symbol=req.ibkr_news_max_headlines_per_symbol,
            )
            if str(news_ingest.get("status") or "").upper() == "SUCCESS":
                try:
                    news_recompute = _refresh_news_context_chain(cur)
                except Exception as exc:
                    news_recompute = {"status": "FAIL", "error": str(exc)}
        news_readiness = _evaluate_ibkr_news_readiness(
            cur,
            symbol=action.get("SYMBOL"),
            min_symbols_covered=req.ibkr_news_min_symbols_covered,
            max_age_minutes=req.ibkr_news_max_age_minutes,
        )
        news_fallback_active = not bool(news_readiness.get("ready"))
        status_upper = (action.get("STATUS") or "").upper()
        if status_upper in ("RESEARCH_IMPORTED", "PROPOSED", "PENDING_OPEN_VALIDATION"):
            opening_gate = _run_opening_sanity_gate(cur, action, force_refresh_1m=True, now_utc=datetime.now(timezone.utc))
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
                    "news_runtime": {
                        "refresh_ibkr_news": bool(req.refresh_ibkr_news),
                        "ibkr_ingest": news_ingest,
                        "news_recompute": news_recompute,
                        "ibkr_readiness": news_readiness,
                        "fallback_mode": "RSS_FALLBACK" if news_fallback_active else "IBKR_PRIMARY",
                    },
                }

        if status_upper not in ("OPEN_BLOCKED", "OPEN_ELIGIBLE", "OPEN_CAUTION", "PENDING_OPEN_STABILITY_REVIEW", "READY_FOR_APPROVAL_FLOW"):
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
                "news_runtime": {
                    "refresh_ibkr_news": bool(req.refresh_ibkr_news),
                    "ibkr_ingest": news_ingest,
                    "news_recompute": news_recompute,
                    "ibkr_readiness": news_readiness,
                    "fallback_mode": "RSS_FALLBACK" if news_fallback_active else "IBKR_PRIMARY",
                },
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
                json.dumps(
                    {
                        "actor": req.actor,
                        "news_runtime": {
                            "refresh_ibkr_news": bool(req.refresh_ibkr_news),
                            "ibkr_ingest": news_ingest,
                            "news_recompute": news_recompute,
                            "ibkr_readiness": news_readiness,
                            "fallback_mode": "RSS_FALLBACK" if news_fallback_active else "IBKR_PRIMARY",
                        },
                    }
                ),
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
            or jd.get("stop_loss_pct") is None
            or jd.get("hold_bars") is None
            or jd.get("acceptable_early_exit_target_return") is None
        ):
            verdict = _backfill_joint_decision_from_policy(verdict, context)
        action_intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
        is_exit = action_intent == "EXIT"
        exit_position_qty = None
        exit_override_applied = False
        if is_exit:
            exit_position_qty = _fetch_live_symbol_position_qty(cur, action.get("PORTFOLIO_ID"), action.get("SYMBOL"))
            jd_exit = _parse_variant(verdict.get("joint_decision"))
            if abs(float(exit_position_qty)) <= 0:
                verdict["recommendation"] = "BLOCK"
                verdict["blocked"] = True
                jd_exit["should_enter"] = False
                jd_exit["should_execute_exit"] = False
                verdict["joint_decision"] = jd_exit
            elif verdict.get("blocked"):
                verdict["blocked"] = False
                if str(verdict.get("recommendation") or "").upper() == "BLOCK":
                    verdict["recommendation"] = "PROCEED_REDUCED"
                jd_exit["should_enter"] = True
                jd_exit["should_execute_exit"] = True
                verdict["joint_decision"] = jd_exit
                exit_override_applied = True
        verdict = _suppress_block_on_degraded_entry_quality(
            verdict,
            action_intent,
            context.get("execution_risk_config"),
        )
        reason_codes = []
        if is_exit and abs(float(exit_position_qty or 0.0)) <= 0:
            reason_codes.append("EXIT_POSITION_MISSING")
        if exit_override_applied:
            reason_codes.append("EXIT_INTENT_OVERRIDE_APPLIED")
        if news_fallback_active:
            reason_codes.append("NEWS_FALLBACK_RSS_ONLY")
        for rc in (news_readiness.get("reason_codes") or []):
            if rc and rc not in reason_codes:
                reason_codes.append(str(rc))
        for rc in (verdict.get("quality_reason_codes") or []):
            if rc and rc not in reason_codes:
                reason_codes.append(str(rc))
        if verdict["blocked"]:
            reason_codes.append("COMMITTEE_BLOCKED")
        elif verdict["recommendation"] == "PROCEED_REDUCED":
            reason_codes.append("COMMITTEE_REDUCED_SIZE")
        if verdict.get("quality_block_override_applied"):
            reason_codes.append("COMMITTEE_BLOCK_SUPPRESSED_QUALITY_DEGRADED")
        if verdict.get("quality_risk_normalized"):
            reason_codes.append("COMMITTEE_RISK_NORMALIZED_FOR_EXECUTION")
        tier_c_conflict = _has_tier_c_conflict(verdict, pw_evidence, action_news_snapshot)
        if tier_c_conflict:
            reason_codes.append("TIER_C_CONFLICT_ALERT")
        if verdict.get("quality_backfilled"):
            reason_codes.append("COMMITTEE_POLICY_BACKFILL_APPLIED")
        reason_codes.append("COMMITTEE_REVIEWED")
        verdict["tier_c_conflict"] = tier_c_conflict
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
                        "news_runtime": {
                            "refresh_ibkr_news": bool(req.refresh_ibkr_news),
                            "ibkr_ingest": news_ingest,
                            "news_recompute": news_recompute,
                            "ibkr_readiness": news_readiness,
                            "fallback_mode": "RSS_FALLBACK" if news_fallback_active else "IBKR_PRIMARY",
                        },
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
                "news_fallback_mode": "RSS_FALLBACK" if news_fallback_active else "IBKR_PRIMARY",
                "ibkr_news_reason_codes": news_readiness.get("reason_codes") or [],
            },
            outcome_state={
                "roles": COMMITTEE_ROLES,
                "training_qualification_snapshot": action_training_snapshot,
                "target_expectation_snapshot": action_target_snapshot,
                "news_context_snapshot": action_news_snapshot,
                "latest_symbol_news_context": latest_news_snapshot,
                "news_runtime": {
                    "refresh_ibkr_news": bool(req.refresh_ibkr_news),
                    "ibkr_ingest": news_ingest,
                    "news_recompute": news_recompute,
                    "ibkr_readiness": news_readiness,
                    "fallback_mode": "RSS_FALLBACK" if news_fallback_active else "IBKR_PRIMARY",
                },
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
            "news_runtime": {
                "refresh_ibkr_news": bool(req.refresh_ibkr_news),
                "ibkr_ingest": news_ingest,
                "news_recompute": news_recompute,
                "ibkr_readiness": news_readiness,
                "fallback_mode": "RSS_FALLBACK" if news_fallback_active else "IBKR_PRIMARY",
            },
        }
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/committee/apply")
def apply_live_trade_committee(action_id: str, req: ApplyCommitteeVerdictRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")

        status_upper = (action.get("STATUS") or "").upper()
        if status_upper not in (
            "OPEN_BLOCKED",
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
        ):
            raise HTTPException(
                status_code=409,
                detail=f"Committee apply blocked for current status: {status_upper}.",
            )

        verdict_in = dict(req.verdict or {})
        jd = _parse_variant(verdict_in.get("joint_decision"))
        action_intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
        allows_execution = _decision_allows_execution(jd, action_intent)
        recommendation = str(verdict_in.get("recommendation") or ("BLOCK" if not allows_execution else "PROCEED_REDUCED")).upper()
        size_factor = float(verdict_in.get("size_factor") or jd.get("position_size_factor") or 1.0)
        confidence = float(verdict_in.get("confidence") or 0.5)
        blocked = bool(verdict_in.get("blocked")) or recommendation == "BLOCK" or (not allows_execution)
        context = _build_action_decision_context(cur, action)
        pw_evidence = context.get("parallel_worlds_evidence") or {}
        action_news_snapshot = context.get("news_context_snapshot") or {}

        verdict = {
            "recommendation": recommendation,
            "size_factor": max(0.0, min(1.0, size_factor)),
            "confidence": max(0.0, min(1.0, confidence)),
            "blocked": blocked,
            "joint_decision": jd,
        }
        verdict = _backfill_joint_decision_from_policy(verdict, context)
        jd = _parse_variant(verdict.get("joint_decision"))
        action_intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
        is_exit = action_intent == "EXIT"
        exit_position_qty = None
        exit_override_applied = False
        if is_exit:
            exit_position_qty = _fetch_live_symbol_position_qty(cur, action.get("PORTFOLIO_ID"), action.get("SYMBOL"))
            if abs(float(exit_position_qty)) <= 0:
                verdict["recommendation"] = "BLOCK"
                verdict["blocked"] = True
                jd["should_enter"] = False
                jd["should_execute_exit"] = False
                verdict["joint_decision"] = jd
            elif verdict.get("blocked"):
                verdict["blocked"] = False
                if str(verdict.get("recommendation") or "").upper() == "BLOCK":
                    verdict["recommendation"] = "PROCEED_REDUCED"
                jd["should_enter"] = True
                jd["should_execute_exit"] = True
                verdict["joint_decision"] = jd
                exit_override_applied = True

        verdict = _suppress_block_on_degraded_entry_quality(
            verdict,
            action_intent,
            context.get("execution_risk_config"),
        )
        reason_codes = []
        if is_exit and abs(float(exit_position_qty or 0.0)) <= 0:
            reason_codes.append("EXIT_POSITION_MISSING")
        if exit_override_applied:
            reason_codes.append("EXIT_INTENT_OVERRIDE_APPLIED")
        if verdict["blocked"]:
            reason_codes.append("COMMITTEE_BLOCKED")
        elif verdict["recommendation"] == "PROCEED_REDUCED":
            reason_codes.append("COMMITTEE_REDUCED_SIZE")
        if verdict.get("quality_block_override_applied"):
            reason_codes.append("COMMITTEE_BLOCK_SUPPRESSED_QUALITY_DEGRADED")
        if verdict.get("quality_risk_normalized"):
            reason_codes.append("COMMITTEE_RISK_NORMALIZED_FOR_EXECUTION")
        tier_c_conflict = _has_tier_c_conflict(verdict, pw_evidence, action_news_snapshot)
        if tier_c_conflict:
            reason_codes.append("TIER_C_CONFLICT_ALERT")
        if verdict.get("quality_backfilled"):
            reason_codes.append("COMMITTEE_POLICY_BACKFILL_APPLIED")
        reason_codes.append("COMMITTEE_REVIEWED")
        verdict["tier_c_conflict"] = tier_c_conflict
        next_status = "OPEN_BLOCKED" if verdict["blocked"] else "READY_FOR_APPROVAL_FLOW"

        # Derive proposed price/qty so row no longer remains fully pending.
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

        run_id = str(uuid.uuid4())
        cur.execute(
            """
            insert into MIP.LIVE.COMMITTEE_RUN (
              RUN_ID, ACTION_ID, PORTFOLIO_ID, STATUS, MODEL_NAME, STARTED_AT, COMPLETED_AT, DETAILS
            )
            select
              %s, %s, %s, 'COMPLETED', %s, current_timestamp(), current_timestamp(), try_parse_json(%s)
            """,
            (
                run_id,
                action_id,
                action.get("PORTFOLIO_ID"),
                req.model,
                json.dumps({"actor": req.actor, "source": "STREAM_APPLY"}),
            ),
        )
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
                json.dumps({"verdict": verdict, "joint_decision": verdict.get("joint_decision")}),
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
        return {
            "ok": True,
            "action_id": action_id,
            "run_id": run_id,
            "status": "COMPLETED",
            "action_status": next_status,
            "verdict": verdict,
            "joint_decision": verdict.get("joint_decision"),
            "derived_sizing": {"proposed_price": proposed_price_derived, "proposed_qty": proposed_qty_derived},
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
                status_upper = str(action.get("STATUS") or "").upper()
                if status_upper in ("RESEARCH_IMPORTED", "PROPOSED", "PENDING_OPEN_VALIDATION"):
                    opening_gate = _run_opening_sanity_gate(
                        cur,
                        action,
                        force_refresh_1m=True,
                        now_utc=datetime.now(timezone.utc),
                    )
                    action = _fetch_live_action(cur, action_id)
                    status_upper = str((action or {}).get("STATUS") or "").upper()
                    if status_upper == "OPEN_BLOCKED":
                        reason_codes = opening_gate.get("reason_codes") or []
                        reason_text = ", ".join(reason_codes) if reason_codes else "OPEN_BLOCKED"
                        result["error"] = f"Committee stream blocked by opening validation ({reason_text})."
                        return
                allowed_for_stream = {
                    "OPEN_BLOCKED",
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
                if status_upper not in allowed_for_stream:
                    result["error"] = f"Committee stream blocked until opening validation passes (current: {status_upper})."
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
            "OPEN_BLOCKED",
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
            "EXECUTION_REQUESTED",
            "EXECUTION_PARTIAL",
        }
        if current_status not in allowed_statuses:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": f"Cannot stale-reject from status {current_status}.",
                    "reason_codes": ["STALE_REJECT_NOT_ALLOWED_STATUS"],
                },
            )

        execution_cleanup = {}
        if current_status in {"EXECUTION_REQUESTED", "EXECUTION_PARTIAL"}:
            cur.execute(
                """
                select IBKR_ACCOUNT_ID
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where PORTFOLIO_ID = %s
                limit 1
                """,
                (action.get("PORTFOLIO_ID"),),
            )
            cfg_rows = fetch_all(cur)
            account_id = (cfg_rows[0] or {}).get("IBKR_ACCOUNT_ID") if cfg_rows else None
            if account_id:
                broker_truth = _fetch_latest_broker_truth(cur, str(account_id), str(action.get("SYMBOL") or ""))
                broker_open_ids = broker_truth.get("open_order_ids") or set()
                cur.execute(
                    """
                    select ORDER_ID, BROKER_ORDER_ID, STATUS
                    from MIP.LIVE.LIVE_ORDERS
                    where ACTION_ID = %s
                    """,
                    (action_id,),
                )
                action_orders = fetch_all(cur)
                active_order_ids = []
                for ord_row in action_orders:
                    if _is_order_active_in_broker_truth(ord_row, broker_open_ids):
                        active_order_ids.append(ord_row.get("ORDER_ID"))
                if active_order_ids or broker_truth.get("has_symbol_position"):
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": "Cannot stale-reject execution while IB broker truth still shows active order/position.",
                            "reason_codes": ["STALE_REJECT_BLOCKED_BROKER_ACTIVE"],
                            "active_order_ids": active_order_ids,
                            "has_symbol_position": bool(broker_truth.get("has_symbol_position")),
                        },
                    )
                cur.execute(
                    """
                    update MIP.LIVE.LIVE_ORDERS
                       set STATUS = case
                                      when upper(coalesce(STATUS, '')) in ('SUBMITTED','ACKNOWLEDGED','PENDINGSUBMIT','PRESUBMITTED')
                                        then 'CANCELED'
                                      else STATUS
                                    end,
                           LAST_UPDATED_AT = current_timestamp()
                     where ACTION_ID = %s
                    """,
                    (action_id,),
                )
                execution_cleanup = {
                    "account_id": account_id,
                    "snapshot_ts": broker_truth.get("snapshot_ts"),
                    "orders_marked_canceled": True,
                }

        existing_reason_codes = _parse_list_variant(action.get("REASON_CODES"))
        merged_reasons = sorted(set(existing_reason_codes + ["STALE_REJECTED_MANUAL"]))
        if current_status in {"EXECUTION_REQUESTED", "EXECUTION_PARTIAL"}:
            merged_reasons = sorted(set(merged_reasons + ["EXECUTION_NOT_ACTIVE_AT_BROKER"]))
        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set STATUS = 'REJECTED',
                   COMPLIANCE_STATUS = 'REJECTED',
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
            status="REJECTED",
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "actor": req.actor,
                "reason_codes": merged_reasons,
            },
            outcome_state={"notes": req.notes, "execution_cleanup": execution_cleanup},
        )
        return {"ok": True, "action_id": action_id, "status": "REJECTED", "reason_codes": merged_reasons}
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
        if current_status == "OPEN_BLOCKED":
            # Allow iterative reassessment for committee/opening blocks without forcing stale reject.
            committee_resp = run_live_trade_committee(
                action_id,
                CommitteeRunRequest(
                    actor="revalidate_recheck",
                    force_rerun=True,
                ),
            )
            action_after = _fetch_live_action(cur, action_id) or {}
            status_after = (action_after.get("STATUS") or "").upper()
            if status_after == "OPEN_BLOCKED":
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "Action remains blocked after committee recheck.",
                        "reason_codes": _parse_list_variant(action_after.get("REASON_CODES")),
                        "blocked_stage": (committee_resp or {}).get("blocked_stage") if isinstance(committee_resp, dict) else None,
                    },
                )
            return {
                "ok": True,
                "action_id": action_id,
                "status": status_after,
                "revalidation_outcome": "RECHECKED",
                "price_deviation_pct": None,
                "reason_codes": _parse_list_variant(action_after.get("REASON_CODES")),
                "source": "OPEN_BLOCKED_COMMITTEE_RERUN",
            }
        if current_status == "INTENT_APPROVED":
            _assert_transition_allowed(current_status, "REVALIDATED_PASS")
        elif current_status == "REVALIDATED_FAIL":
            _assert_transition_allowed(current_status, "REVALIDATED_PASS")
        elif current_status == "REVALIDATED_PASS":
            _assert_transition_allowed(current_status, "REVALIDATED_PASS")
        else:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Revalidation allowed only from OPEN_BLOCKED "
                    "or INTENT_APPROVED/REVALIDATED_FAIL/REVALIDATED_PASS "
                    f"(current: {current_status})"
                ),
            )
        now_utc = datetime.now(timezone.utc)
        if not _is_extended_trading_open_ny(now_utc):
            ext_open_utc, ext_close_utc = _extended_trading_bounds_utc(now_utc)
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Revalidation is only enabled during ET 04:00-20:00.",
                    "reason_codes": ["OUTSIDE_EXTENDED_TRADING_WINDOW"],
                    "window_open_utc": ext_open_utc.isoformat(),
                    "window_close_utc": ext_close_utc.isoformat(),
                },
            )
        symbol = action.get("SYMBOL")
        proposed_price = action.get("PROPOSED_PRICE")
        portfolio_id = action.get("PORTFOLIO_ID")
        action_intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
        is_exit = action_intent == "EXIT"
        freshness_threshold_sec = 900
        try:
            cur.execute(
                """
                select coalesce(QUOTE_FRESHNESS_THRESHOLD_SEC, 900)
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where PORTFOLIO_ID = %s
                limit 1
                """,
                (portfolio_id,),
            )
            cfg_row = cur.fetchone()
            if cfg_row and cfg_row[0] is not None:
                freshness_threshold_sec = int(cfg_row[0])
        except Exception:
            freshness_threshold_sec = 900
        refresh_info = {"attempted": False}
        if req.force_refresh_1m:
            refresh_info = _force_refresh_latest_one_minute_bars(cur, symbol)
            if str(refresh_info.get("status") or "").upper() != "SUCCESS":
                raise HTTPException(
                    status_code=400,
                    detail={
                        "message": "IBKR direct 1m refresh failed, revalidation blocked.",
                        "reason_codes": ["IBKR_DIRECT_REFRESH_FAILED"],
                        "refresh": refresh_info,
                    },
                )

        direct_ibkr_one_min = _extract_latest_one_min_bar_from_refresh(refresh_info, symbol) if req.force_refresh_1m else None
        source = "IBKR_DIRECT_1M" if direct_ibkr_one_min else "ONE_MINUTE_BAR"
        if direct_ibkr_one_min:
            ref_ts, ref_price = direct_ibkr_one_min
        else:
            cur.execute(
                """
                select TS, CLOSE
                from MIP.MART.MARKET_BARS
                where SYMBOL = %s
                  and INTERVAL_MINUTES = 1
                  and upper(coalesce(SOURCE, '')) = 'IBKR'
                order by case when upper(coalesce(SOURCE, '')) = 'IBKR' then 0 else 1 end, TS desc
                limit 1
                """,
                (symbol,),
            )
            bar = cur.fetchone()
            if bar:
                ref_ts, ref_price = bar
            else:
                cur.execute(
                    """
                    select TS, CLOSE
                    from MIP.MART.MARKET_BARS
                    where SYMBOL = %s
                      and INTERVAL_MINUTES in (15, 60, 1440)
                      and upper(coalesce(SOURCE, '')) = 'IBKR'
                    order by TS desc
                    limit 1
                    """,
                    (symbol,),
                )
                fallback = cur.fetchone()
                if not fallback:
                    if is_exit and proposed_price is not None:
                        ref_ts = datetime.now(timezone.utc)
                        ref_price = float(proposed_price)
                        source = "EXIT_PRICE_FALLBACK"
                    else:
                        raise HTTPException(status_code=400, detail="No market bar found for symbol, revalidation blocked.")
                else:
                    ref_ts, ref_price = fallback
                    source = "BAR_FALLBACK"

        ref_ts_utc = _market_bar_ts_to_utc(ref_ts)
        if ref_ts_utc is None:
            if is_exit and proposed_price is not None:
                ref_ts_utc = datetime.now(timezone.utc)
                ref_price = float(proposed_price)
                source = "EXIT_PRICE_FALLBACK"
            else:
                raise HTTPException(status_code=400, detail="Unable to parse market bar timestamp, revalidation blocked.")
        now_utc = datetime.now(timezone.utc)
        bar_age_sec = (now_utc - ref_ts_utc).total_seconds()
        market_open_now = _is_extended_trading_open_ny(now_utc)
        if bar_age_sec > freshness_threshold_sec and not is_exit and market_open_now:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"IBKR bar is stale ({int(bar_age_sec)}s old > {int(freshness_threshold_sec)}s threshold), "
                    "revalidation blocked."
                ),
            )

        deviation = None
        if proposed_price and ref_price:
            try:
                deviation = abs(float(ref_price) - float(proposed_price)) / max(float(proposed_price), 1e-9)
            except Exception:
                deviation = None
        revalidation_outcome = "FAIL"
        status = "REVALIDATED_FAIL"
        existing_reason_codes = _parse_list_variant(action.get("REASON_CODES"))
        reason_codes: list[str] = []
        reduced_size_factor = None
        target_open_condition_factor = 1.0
        if is_exit and bar_age_sec > freshness_threshold_sec:
            reason_codes.append("EXIT_REVALIDATION_STALE_BAR_BYPASS")
        if (not is_exit) and (not market_open_now) and bar_age_sec > freshness_threshold_sec:
            reason_codes.append("REVALIDATION_STALE_BAR_OUTSIDE_SESSION_ALLOWED")
        if source == "IBKR_DIRECT_1M":
            reason_codes.append("REVALIDATION_PRICE_FROM_IBKR_DIRECT")

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
        merged_reason_codes: list[str] = []
        for rc in existing_reason_codes + reason_codes:
            rc_text = str(rc)
            if rc_text and rc_text not in merged_reason_codes:
                merged_reason_codes.append(rc_text)

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
                ref_ts if source in ("ONE_MINUTE_BAR", "IBKR_DIRECT_1M") else None,
                ref_price if source in ("ONE_MINUTE_BAR", "IBKR_DIRECT_1M") else None,
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
                json.dumps(merged_reason_codes),
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
            "reason_codes": merged_reason_codes,
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
              IBKR_ACCOUNT_ID, ADAPTER_MODE, MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT,
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
        param_snapshot = _parse_variant(action.get("PARAM_SNAPSHOT"))
        if not isinstance(param_snapshot, dict):
            param_snapshot = {}
        news_snapshot = _parse_variant(action.get("NEWS_CONTEXT_SNAPSHOT"))
        if not isinstance(news_snapshot, dict):
            news_snapshot = _parse_variant(param_snapshot.get("news_context"))
        if not isinstance(news_snapshot, dict):
            news_snapshot = {}
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

        side = str(action.get("SIDE") or "").upper()
        action_intent = _normalize_action_intent(side, action.get("ACTION_INTENT"))
        exit_type = str(action.get("EXIT_TYPE") or "").upper() or None
        is_exit = action_intent == "EXIT"

        long_only_cfg = _read_app_config(cur, ["LIVE_ENFORCE_LONG_ONLY", "LIVE_BLOCK_ON_BROKER_SHORT"])
        enforce_long_only = _parse_bool_config(long_only_cfg.get("LIVE_ENFORCE_LONG_ONLY"), True)
        block_on_broker_short = _parse_bool_config(long_only_cfg.get("LIVE_BLOCK_ON_BROKER_SHORT"), True)
        long_only_guard = {
            "enabled": bool(enforce_long_only and block_on_broker_short),
            "entry_side": side,
            "short_symbols": [],
            "symbol_position_qty": None,
            "snapshot_ts": None,
        }
        if not is_exit and enforce_long_only and block_on_broker_short and account_id:
            cur.execute(
                """
                with latest_pos as (
                    select max(SNAPSHOT_TS) as SNAPSHOT_TS
                    from MIP.LIVE.BROKER_SNAPSHOTS
                    where SNAPSHOT_TYPE = 'POSITION'
                      and IBKR_ACCOUNT_ID = %s
                )
                select upper(SYMBOL) as SYMBOL, coalesce(sum(POSITION_QTY), 0) as POSITION_QTY, max(s.SNAPSHOT_TS) as SNAPSHOT_TS
                from MIP.LIVE.BROKER_SNAPSHOTS s
                join latest_pos lp on s.SNAPSHOT_TS = lp.SNAPSHOT_TS
                where s.SNAPSHOT_TYPE = 'POSITION'
                  and s.IBKR_ACCOUNT_ID = %s
                  and coalesce(s.POSITION_QTY, 0) <> 0
                group by upper(SYMBOL)
                """,
                (account_id, account_id),
            )
            broker_pos_rows = fetch_all(cur)
            action_symbol = str(action.get("SYMBOL") or "").upper()
            short_symbols: list[str] = []
            symbol_position_qty: float | None = None
            snapshot_ts = None
            for row in broker_pos_rows:
                sym = str(row.get("SYMBOL") or "").upper()
                qty = float(row.get("POSITION_QTY") or 0.0)
                if snapshot_ts is None:
                    snapshot_ts = row.get("SNAPSHOT_TS")
                if sym == action_symbol:
                    symbol_position_qty = qty
                if qty < 0:
                    short_symbols.append(sym)
            long_only_guard["short_symbols"] = short_symbols
            long_only_guard["symbol_position_qty"] = symbol_position_qty
            long_only_guard["snapshot_ts"] = (
                snapshot_ts.isoformat() if hasattr(snapshot_ts, "isoformat") else (str(snapshot_ts) if snapshot_ts is not None else None)
            )
            if side != "BUY":
                reason_codes.append("ENTRY_SIDE_NOT_ALLOWED_LONG_ONLY")
            if short_symbols:
                reason_codes.append("BROKER_SHORT_POSITION_OUT_OF_POLICY")
            if symbol_position_qty is not None and symbol_position_qty < 0:
                reason_codes.append("SYMBOL_SHORT_POSITION_OUT_OF_POLICY")

        max_positions = cfg.get("MAX_POSITIONS")
        if not is_exit and max_positions is not None and open_positions >= int(max_positions):
            reason_codes.append("MAX_POSITIONS_EXCEEDED")

        proposed_qty = action.get("PROPOSED_QTY")
        if is_exit and (proposed_qty is None or float(proposed_qty) <= 0):
            broker_truth_for_exit = _fetch_latest_broker_truth(cur, str(account_id), str(action.get("SYMBOL")))
            symbol_position_qty = float(broker_truth_for_exit.get("symbol_position_qty") or 0.0)
            if abs(symbol_position_qty) <= 0:
                reason_codes.append("EXIT_POSITION_MISSING")
            else:
                proposed_qty = abs(symbol_position_qty)
        px = action.get("REVALIDATION_PRICE") or action.get("PROPOSED_PRICE")
        if proposed_qty is None or px is None:
            reason_codes.append("MISSING_NOTIONAL_INPUT")
            est_notional = None
        else:
            est_notional = abs(float(proposed_qty) * float(px))

        max_position_pct = cfg.get("MAX_POSITION_PCT")
        if not is_exit and est_notional is not None and nav_eur and max_position_pct is not None and nav_eur > 0:
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
            realism_details["long_only_guard"] = long_only_guard
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
        exit_side = "SELL" if side == "BUY" else "BUY"
        adapter_mode = str(cfg.get("ADAPTER_MODE") or "PAPER").upper()
        execution_mode = str(os.getenv("LIVE_EXECUTION_MODE", "AUTO")).upper()
        use_ibkr_submit = adapter_mode == "LIVE"
        if execution_mode == "IBKR":
            use_ibkr_submit = True
        elif execution_mode == "PLACEHOLDER":
            use_ibkr_submit = False

        cur.execute(
            """
            select
              VERDICT_JSON:verdict:joint_decision:realistic_target_return::float as TARGET_RETURN,
              VERDICT_JSON:verdict:joint_decision:acceptable_early_exit_target_return::float as EARLY_EXIT_TARGET_RETURN,
              VERDICT_JSON:verdict:joint_decision:stop_loss_pct::float as STOP_LOSS_PCT
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
        committee_stop_loss_pct = verdict.get("STOP_LOSS_PCT")
        target_return = (
            float(early_exit_target_return)
            if early_exit_target_return is not None
            else (float(realistic_target_return) if realistic_target_return is not None else None)
        )
        stop_loss_pct_default = float(cfg.get("BUST_PCT")) if cfg.get("BUST_PCT") is not None else None
        stop_loss_pct = float(committee_stop_loss_pct) if committee_stop_loss_pct is not None else stop_loss_pct_default
        if stop_loss_pct is not None and stop_loss_pct_default is not None:
            # Never allow committee stop wider than configured portfolio bust guard.
            stop_loss_pct = min(float(stop_loss_pct), float(stop_loss_pct_default))

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
        if is_exit:
            # Exit intent should submit a close order without opening a new bracket.
            tp_price = None
            sl_price = None
        if use_ibkr_submit and not is_exit:
            risk_reason_codes = []
            if target_return is None or target_return <= 0:
                risk_reason_codes.append("LIVE_TP_REQUIRED_MISSING")
            if stop_loss_pct is None or stop_loss_pct <= 0:
                risk_reason_codes.append("LIVE_SL_REQUIRED_MISSING")
            if tp_price is None or sl_price is None:
                risk_reason_codes.append("LIVE_BRACKET_REQUIRED")
            slippage_bps = 2.0
            fee_bps = 1.0
            spread_bps = 0.0
            try:
                cur.execute(
                    """
                    select
                      coalesce(max(case when CONFIG_KEY = 'SLIPPAGE_BPS' then try_to_number(CONFIG_VALUE) end), 2) as SLIPPAGE_BPS,
                      coalesce(max(case when CONFIG_KEY = 'FEE_BPS' then try_to_number(CONFIG_VALUE) end), 1) as FEE_BPS,
                      coalesce(max(case when CONFIG_KEY = 'SPREAD_BPS' then try_to_number(CONFIG_VALUE) end), 0) as SPREAD_BPS
                    from MIP.APP.APP_CONFIG
                    where CONFIG_KEY in ('SLIPPAGE_BPS','FEE_BPS','SPREAD_BPS')
                    """
                )
                fee_rows = fetch_all(cur)
                if fee_rows:
                    slippage_bps = float(fee_rows[0].get("SLIPPAGE_BPS") or slippage_bps)
                    fee_bps = float(fee_rows[0].get("FEE_BPS") or fee_bps)
                    spread_bps = float(fee_rows[0].get("SPREAD_BPS") or spread_bps)
            except Exception:
                pass
            fee_return_floor = (slippage_bps + fee_bps + (spread_bps / 2.0)) / 10000.0
            min_net_tp_bps = float(os.getenv("LIVE_MIN_NET_TP_BPS", "5"))
            min_rr = float(os.getenv("LIVE_MIN_R_MULTIPLE", "1.10"))
            min_tp_required = fee_return_floor + (min_net_tp_bps / 10000.0)
            if target_return is not None and target_return <= min_tp_required:
                risk_reason_codes.append("LIVE_TP_NET_EDGE_TOO_LOW")
            if target_return is not None and stop_loss_pct is not None and stop_loss_pct > 0:
                rr_multiple = float(target_return) / float(stop_loss_pct)
                if rr_multiple < min_rr:
                    risk_reason_codes.append("LIVE_RISK_REWARD_TOO_LOW")
            if risk_reason_codes:
                final_reason_codes = sorted(set(reason_codes + risk_reason_codes))
                _write_reason_codes(cur, action_id, final_reason_codes)
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "Execution blocked: live IB trades require valid TP/SL and minimum risk-reward edge.",
                        "reason_codes": final_reason_codes,
                    },
                )
        order_legs = []
        broker_submit_payload = None
        broker_truth_check = None
        if use_ibkr_submit:
            submit_attempt_payload = {
                "account": str(account_id),
                "symbol": str(action.get("SYMBOL")),
                "side": side,
                "action_intent": action_intent,
                "exit_type": exit_type,
                "qty": qty_ordered,
                "entry_price": float(entry_price) if entry_price is not None else None,
                "tp_price": float(tp_price) if tp_price is not None else None,
                "sl_price": float(sl_price) if sl_price is not None else None,
                "tif": "DAY",
                "runtime": {
                    "host": os.getenv("IBKR_EXEC_HOST", "127.0.0.1"),
                    "port": int(os.getenv("IBKR_EXEC_PORT", "4002")),
                    "client_id": int(os.getenv("IBKR_EXEC_CLIENT_ID", "9410")),
                    "connect_timeout_sec": int(os.getenv("IBKR_EXEC_CONNECT_TIMEOUT_SEC", "12")),
                    "exchange": os.getenv("IBKR_EXEC_EXCHANGE", "SMART"),
                    "currency": os.getenv("IBKR_EXEC_CURRENCY", "USD"),
                    "outside_rth": os.getenv("IBKR_EXEC_OUTSIDE_RTH", "1").strip().lower() in ("1", "true", "yes", "on"),
                    "child_tif": os.getenv("IBKR_EXEC_CHILD_TIF", "GTC").upper(),
                },
            }
            cur.execute(
                """
                insert into MIP.LIVE.BROKER_EVENT_LEDGER (
                  EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, PROPOSAL_ID, ACTION_ID,
                  IDEMPOTENCY_KEY, SYMBOL, SIDE, QTY, PRICE, PAYLOAD
                )
                select
                  %s, current_timestamp(), 'EXECUTION_SUBMIT_ATTEMPT', %s, %s, %s,
                  %s, %s, %s, %s, %s, try_parse_json(%s)
                """,
                (
                    str(uuid.uuid4()),
                    action.get("PORTFOLIO_ID"),
                    action.get("PROPOSAL_ID"),
                    action_id,
                    idempotency_key,
                    action.get("SYMBOL"),
                    side,
                    qty_ordered,
                    entry_price,
                    json.dumps(submit_attempt_payload),
                ),
            )
            try:
                broker_submit_payload = _submit_ibkr_order_bundle(
                    account=str(account_id),
                    symbol=str(action.get("SYMBOL")),
                    side=side,
                    qty=qty_ordered,
                    entry_price=float(entry_price) if entry_price is not None else None,
                    tp_price=float(tp_price) if tp_price is not None else None,
                    sl_price=float(sl_price) if sl_price is not None else None,
                    tif="DAY",
                    child_tif=os.getenv("IBKR_EXEC_CHILD_TIF", "GTC"),
                )
            except HTTPException as exc:
                cur.execute(
                    """
                    insert into MIP.LIVE.BROKER_EVENT_LEDGER (
                      EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, PROPOSAL_ID, ACTION_ID,
                      IDEMPOTENCY_KEY, SYMBOL, SIDE, QTY, PRICE, PAYLOAD
                    )
                    select
                      %s, current_timestamp(), 'EXECUTION_SUBMIT_FAILED', %s, %s, %s,
                      %s, %s, %s, %s, %s, try_parse_json(%s)
                    """,
                    (
                        str(uuid.uuid4()),
                        action.get("PORTFOLIO_ID"),
                        action.get("PROPOSAL_ID"),
                        action_id,
                        idempotency_key,
                        action.get("SYMBOL"),
                        side,
                        qty_ordered,
                        entry_price,
                        json.dumps(
                            {
                                "message": "IBKR submission raised HTTPException",
                                "detail": exc.detail,
                                "attempt": submit_attempt_payload,
                            }
                        ),
                    ),
                )
                raise
            cur.execute(
                """
                insert into MIP.LIVE.BROKER_EVENT_LEDGER (
                  EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, PROPOSAL_ID, ACTION_ID,
                  IDEMPOTENCY_KEY, SYMBOL, SIDE, QTY, PRICE, PAYLOAD
                )
                select
                  %s, current_timestamp(), 'EXECUTION_SUBMIT_RESULT', %s, %s, %s,
                  %s, %s, %s, %s, %s, try_parse_json(%s)
                """,
                (
                    str(uuid.uuid4()),
                    action.get("PORTFOLIO_ID"),
                    action.get("PROPOSAL_ID"),
                    action_id,
                    idempotency_key,
                    action.get("SYMBOL"),
                    side,
                    qty_ordered,
                    entry_price,
                    json.dumps(broker_submit_payload or {}),
                ),
            )
            ib_orders = broker_submit_payload.get("orders") or []
            ib_orders_after_wait = broker_submit_payload.get("orders_after_wait") or []
            # Prefer broker IDs observed after wait/poll, because initial order bundle
            # can report perm_id=0 before IB acknowledges final identifiers.
            role_to_confirmed_broker_id = {}
            for ow in ib_orders_after_wait:
                ow_role = str(ow.get("role") or "").upper()
                ow_id = _normalize_broker_order_id(ow.get("perm_id") or ow.get("order_id"))
                if ow_role and ow_id:
                    role_to_confirmed_broker_id[ow_role] = ow_id
            for ib_leg in ib_orders:
                role = str(ib_leg.get("role") or "").upper()
                if role == "TAKE_PROFIT":
                    idem = f"{idempotency_key}:TP"
                elif role == "STOP_LOSS":
                    idem = f"{idempotency_key}:SL"
                else:
                    idem = idempotency_key
                broker_order_id = role_to_confirmed_broker_id.get(
                    role,
                    _normalize_broker_order_id(ib_leg.get("perm_id") or ib_leg.get("order_id")),
                )
                ib_lmt_price = _normalize_broker_price(ib_leg.get("lmt_price"))
                ib_aux_price = _normalize_broker_price(ib_leg.get("aux_price"))
                if role == "STOP_LOSS":
                    normalized_limit_price = (
                        ib_aux_price
                        if ib_aux_price is not None
                        else (
                            float(sl_price)
                            if sl_price is not None
                            else (ib_lmt_price if ib_lmt_price is not None else float(entry_price))
                        )
                    )
                elif role == "TAKE_PROFIT":
                    normalized_limit_price = (
                        ib_lmt_price
                        if ib_lmt_price is not None
                        else (
                            float(tp_price)
                            if tp_price is not None
                            else (ib_aux_price if ib_aux_price is not None else float(entry_price))
                        )
                    )
                else:
                    normalized_limit_price = (
                        ib_lmt_price
                        if ib_lmt_price is not None
                        else (
                            float(entry_price)
                            if entry_price is not None
                            else (ib_aux_price if ib_aux_price is not None else None)
                        )
                    )
                order_legs.append(
                    {
                        "order_id": str(uuid.uuid4()),
                        "broker_order_id": broker_order_id,
                        "idempotency_key": idem,
                        "side": side if role == "PARENT" else exit_side,
                        "order_type": str(ib_leg.get("order_type") or ("MKT" if role == "PARENT" else "LMT")),
                        "limit_price": normalized_limit_price,
                        "role": role or "PARENT",
                        "status": str(ib_leg.get("status") or "SUBMITTED").upper(),
                    }
                )
            if not order_legs:
                raise HTTPException(
                    status_code=409,
                    detail={"message": "IBKR submission returned no orders.", "reason_codes": ["IBKR_EMPTY_ORDER_BUNDLE"]},
                )
            # Fail closed: do not accept local execution state unless broker-truth snapshot confirms it.
            _run_on_demand_snapshot_sync(
                **_default_snapshot_sync_params(),
                account=str(account_id),
                portfolio_id=action.get("PORTFOLIO_ID"),
            )
            broker_truth_raw = _fetch_latest_broker_truth(cur, str(account_id), str(action.get("SYMBOL") or ""))
            broker_order_ids = {
                _normalize_broker_order_id(leg.get("broker_order_id"))
                for leg in order_legs
                if _normalize_broker_order_id(leg.get("broker_order_id"))
            }
            broker_open_ids = broker_truth_raw.get("open_order_ids") or set()
            has_open_order = bool(broker_order_ids and broker_order_ids.intersection(broker_open_ids))
            has_position = bool(broker_truth_raw.get("has_symbol_position"))
            broker_truth_check = {
                **broker_truth_raw,
                "open_order_ids": sorted(broker_open_ids),
            }
            if not has_open_order and not has_position:
                final_reason_codes = ["IBKR_TRUTH_MISSING_ORDER_ACK"]
                _write_reason_codes(cur, action_id, final_reason_codes)
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "IBKR did not confirm open-order or position after submit; execution blocked to prevent drift.",
                        "reason_codes": final_reason_codes,
                        "broker_order_ids": sorted(broker_order_ids),
                        "snapshot_ts": (
                            broker_truth_raw.get("snapshot_ts").isoformat()
                            if hasattr(broker_truth_raw.get("snapshot_ts"), "isoformat")
                            else broker_truth_raw.get("snapshot_ts")
                        ),
                    },
                )
        else:
            order_legs = [
                {
                    "order_id": order_id,
                    "broker_order_id": None,
                    "idempotency_key": idempotency_key,
                    "side": side,
                    "order_type": "MKT_PAPER",
                    "limit_price": entry_price,
                    "role": "PARENT",
                    "status": "ACKNOWLEDGED",
                }
            ]
            if tp_price is not None:
                order_legs.append(
                    {
                        "order_id": str(uuid.uuid4()),
                        "broker_order_id": None,
                        "idempotency_key": f"{idempotency_key}:TP",
                        "side": exit_side,
                        "order_type": "LMT_TP_PAPER",
                        "limit_price": float(tp_price),
                        "role": "TAKE_PROFIT",
                        "status": "ACKNOWLEDGED",
                    }
                )
            if sl_price is not None:
                order_legs.append(
                    {
                        "order_id": str(uuid.uuid4()),
                        "broker_order_id": None,
                        "idempotency_key": f"{idempotency_key}:SL",
                        "side": exit_side,
                        "order_type": "STP_SL_PAPER",
                        "limit_price": float(sl_price),
                        "role": "STOP_LOSS",
                        "status": "ACKNOWLEDGED",
                    }
                )

        # Keep API response anchored to the parent leg order id.
        order_id = str((order_legs[0] or {}).get("order_id") or order_id)

        for leg in order_legs:
            cur.execute(
                """
                insert into MIP.LIVE.LIVE_ORDERS (
                  ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID, IDEMPOTENCY_KEY, BROKER_ORDER_ID, STATUS,
                  SYMBOL, SIDE, ACTION_INTENT, EXIT_TYPE, ORDER_TYPE, QTY_ORDERED, LIMIT_PRICE,
                  SUBMITTED_AT, ACKNOWLEDGED_AT, LAST_UPDATED_AT, CREATED_AT
                )
                values (
                  %(order_id)s, %(action_id)s, %(portfolio_id)s, %(account_id)s, %(idempotency_key)s, %(broker_order_id)s, %(status)s,
                  %(symbol)s, %(side)s, %(action_intent)s, %(exit_type)s, %(order_type)s, %(qty_ordered)s, %(limit_price)s,
                  current_timestamp(), current_timestamp(), current_timestamp(), current_timestamp()
                )
                """,
                {
                    "order_id": leg["order_id"],
                    "action_id": action_id,
                    "portfolio_id": action.get("PORTFOLIO_ID"),
                    "account_id": account_id,
                    "idempotency_key": leg["idempotency_key"],
                    "broker_order_id": leg.get("broker_order_id"),
                    "status": leg.get("status") or "ACKNOWLEDGED",
                    "symbol": action.get("SYMBOL"),
                    "side": leg["side"],
                    "action_intent": action_intent,
                    "exit_type": exit_type,
                    "order_type": leg["order_type"],
                    "qty_ordered": qty_ordered,
                    "limit_price": leg["limit_price"],
                },
            )

        # Mark execution requested before non-critical telemetry writes so retries
        # cannot duplicate intent when downstream insert fails.
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

        cur.execute(
            """
            insert into MIP.LIVE.BROKER_EVENT_LEDGER (
              EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, PROPOSAL_ID, ACTION_ID,
              IDEMPOTENCY_KEY, BROKER_ORDER_ID, SYMBOL, SIDE, QTY, PRICE, PAYLOAD
            )
            select
              %s, current_timestamp(), 'EXECUTION_REQUESTED', %s, %s, %s,
              %s, %s, %s, %s, %s, %s, try_parse_json(%s)
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
                        "mode": "IBKR_GATEWAY" if use_ibkr_submit else "PAPER_PLACEHOLDER",
                        "adapter_mode": adapter_mode,
                        "action_intent": action_intent,
                        "exit_type": exit_type,
                        "broker_truth_check": broker_truth_check,
                        "protection_state": (
                            "FULL"
                            if (tp_price is not None and sl_price is not None)
                            else ("PARTIAL" if (tp_price is not None or sl_price is not None) else "NONE")
                        ),
                        "legs": order_legs,
                        "broker_submit_payload": broker_submit_payload,
                    },
                    default=str,
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
                "mode": "IBKR_GATEWAY" if use_ibkr_submit else "PAPER_PLACEHOLDER",
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
            "action_intent": action_intent,
            "exit_type": exit_type,
            "protection_state": (
                "FULL"
                if (tp_price is not None and sl_price is not None)
                else ("PARTIAL" if (tp_price is not None or sl_price is not None) else "NONE")
            ),
            "idempotency_key": idempotency_key,
            "mode": "IBKR_GATEWAY" if use_ibkr_submit else "PAPER_PLACEHOLDER",
            "broker_submit_payload": broker_submit_payload,
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
              STATUS, SYMBOL, SIDE, ACTION_INTENT, EXIT_TYPE, ORDER_TYPE, QTY_ORDERED, LIMIT_PRICE,
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
            select
              %s, current_timestamp(), %s, %s, %s,
              %s, %s, %s, %s, %s, %s, try_parse_json(%s)
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
              PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
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
              PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
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
                   set IBKR_ACCOUNT_ID = coalesce(%s, IBKR_ACCOUNT_ID),
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
                  PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
                  MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
                  VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
                  DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS, IS_ACTIVE, CONFIG_VERSION,
                  CREATED_AT, UPDATED_AT
                )
                values (
                  %s, %s, coalesce(%s, 'PAPER'), coalesce(%s, 'EUR'),
                  %s, %s, %s, %s,
                  coalesce(%s, 14400), coalesce(%s, 60), coalesce(%s, 300),
                  %s, %s, coalesce(%s, 3), coalesce(%s, true), 1,
                  current_timestamp(), current_timestamp()
                )
                """,
                (
                    portfolio_id,
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
              PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
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
              PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS, IS_ACTIVE, CONFIG_VERSION,
              CREATED_AT, UPDATED_AT
            )
            values (
              %s, %s, coalesce(%s, 'PAPER'), coalesce(%s, 'EUR'),
              %s, %s, %s, %s,
              coalesce(%s, 14400), coalesce(%s, 60), coalesce(%s, 300),
              %s, %s, coalesce(%s, 3), coalesce(%s, true), 1,
              current_timestamp(), current_timestamp()
            )
            """,
            (
                next_id,
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
              PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
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
            select
              coalesce(VALIDITY_WINDOW_SEC, 14400) as VALIDITY_WINDOW_SEC,
              IBKR_ACCOUNT_ID
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
        validity_window_sec, ibkr_account_id = cfg
        ibkr_account_id = str(ibkr_account_id or "").strip()
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
        else:
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
            if latest_batch_date is not None and (req.latest_batch_only or not req.allow_stale_import):
                wheres.append("to_date(PROPOSED_AT) = %s")
                params.append(latest_batch_date)
                scope = "latest_batch_day"
            elif req.allow_stale_import and not req.latest_batch_only:
                scope = "all_active_with_stale"
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
        if latest_batch_date is not None and req.run_id is None and not req.allow_stale_import:
            proposals = [
                p
                for p in proposals
                if (p.get("PROPOSED_AT") is not None and p.get("PROPOSED_AT").date() == latest_batch_date)
            ]
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
        skipped_symbol_live_position_count = 0
        imported_action_ids: list[str] = []
        source_portfolios: set[int] = set()
        distinct_symbols: set[str] = set()
        live_position_cache: dict[str, bool] = {}

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
            symbol_upper = (p.get("SYMBOL") or "").upper()
            if ibkr_account_id and symbol_upper:
                has_live_position = live_position_cache.get(symbol_upper)
                if has_live_position is None:
                    broker_truth = _fetch_latest_broker_truth(cur, ibkr_account_id, symbol_upper)
                    has_live_position = bool(broker_truth.get("has_symbol_position"))
                    live_position_cache[symbol_upper] = has_live_position
                if has_live_position:
                    skipped_symbol_live_position_count += 1
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
                    "source_scope": scope,
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
            action_intent = _normalize_action_intent((p.get("SIDE") or "").upper(), None)
            exit_type = "MANUAL" if action_intent == "EXIT" else None
            cur.execute(
                """
                insert into MIP.LIVE.LIVE_ACTIONS (
                  ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, SIDE, ACTION_INTENT, EXIT_TYPE, EXIT_REASON, PROPOSED_QTY, ASSET_CLASS,
                  STATUS, VALIDITY_WINDOW_END, COMPLIANCE_STATUS, PARAM_SNAPSHOT, REASON_CODES,
                  TRAINING_QUALIFICATION_SNAPSHOT, TRAINING_LIVE_ELIGIBLE, TRAINING_RANK_IMPACT, TRAINING_SIZE_CAP_FACTOR,
                  TARGET_EXPECTATION_SNAPSHOT, TARGET_OPEN_CONDITION_FACTOR, TARGET_EXPECTATION_POLICY_VERSION,
                  NEWS_CONTEXT_SNAPSHOT, NEWS_CONTEXT_STATE, NEWS_EVENT_SHOCK_FLAG, NEWS_FRESHNESS_BUCKET, NEWS_CONTEXT_POLICY_VERSION,
                  COMMITTEE_REQUIRED, COMMITTEE_STATUS,
                  CREATED_AT, UPDATED_AT
                )
                select
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
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
                    action_intent,
                    exit_type,
                    None,
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
            "skipped_symbol_live_position_count": skipped_symbol_live_position_count,
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
    allow_stale: bool = Query(default=False),
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
        else:
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
            if latest_batch_date is not None and (latest_batch_only or not allow_stale):
                wheres.append("to_date(op.PROPOSED_AT) = %s")
                params.append(latest_batch_date)
                scope = "latest_batch_day"
            elif allow_stale and not latest_batch_only:
                scope = "all_active_with_stale"

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
        if latest_batch_date is not None and run_id is None and not allow_stale:
            rows = [
                r
                for r in rows
                if (r.get("PROPOSED_AT") is not None and r.get("PROPOSED_AT").date() == latest_batch_date)
            ]
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
            "allow_stale": allow_stale,
            "dedupe_by_symbol": dedupe_by_symbol,
            "max_proposal_age_days": max_proposal_age_days,
            "candidates": serialize_rows(deduped_out),
        }
    finally:
        conn.close()
