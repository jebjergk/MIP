"""
GET /live/metrics — lightweight live metrics for header and Suggestions.
Read-only. Returns api_ok, snowflake_ok, updated_at, last_run, last_brief, outcomes.
"""
from __future__ import annotations

import json
import logging
import math
import os
import subprocess
import time
import uuid
from decimal import Decimal
from pathlib import Path
from datetime import date, datetime, timezone, timedelta
from queue import Empty, Queue
from threading import Event, Lock, Thread
from zoneinfo import ZoneInfo


def _json_default_decimal_safe(o):
    """JSON encoder default for Snowflake Decimal/date/datetime values.

    fetch_all() returns rows with native Snowflake types (Decimal, datetime,
    date, etc.). When those values flow through dicts that are later
    json.dumps'd back into the database (e.g. PARAM_SNAPSHOT merges that
    embed parts of the LIVE_ACTIONS row), the default encoder raises
    "Object of type Decimal is not JSON serializable". This default
    coerces Decimals to int/float (preserving int-valuedness so 20 stays
    20 and not "20") and datetime/date to ISO strings; everything else
    falls back to str(o) so we never crash a write path on a stray type.
    """
    if isinstance(o, Decimal):
        if o == o.to_integral_value():
            return int(o)
        return float(o)
    if hasattr(o, "isoformat"):
        return o.isoformat()
    return str(o)


def _json_dumps_safe(obj) -> str:
    """json.dumps wrapper that tolerates Decimal/datetime values.

    Use for any payload that may transitively contain a Snowflake row
    field (e.g. PARAM_SNAPSHOT contract / joint_decision merges). Plain
    json.dumps would crash on Decimal; this stays write-safe and
    round-trips numerically through Snowflake VARIANT.
    """
    return json.dumps(obj, default=_json_default_decimal_safe)

from fastapi import APIRouter, Query, HTTPException, Body
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.config import get_snowflake_config
from app.db import get_connection, fetch_all, serialize_row, serialize_rows, SnowflakeAuthError
from app.live_execution_utils import to_dt_utc, is_close_like_execution
from app.training_status import score_training_status_row, DEFAULT_MIN_SIGNALS
from app.entry_intel_hooks import (
    build_closeout_api_intel,
    build_closeout_summary_for_api,
    ensure_entry_intel_for_proposal,
    fetch_latest_snapshot_id_for_proposal,
    insert_entry_intel_action_link,
    is_protective_leg_order,
    maybe_write_trade_closeout_on_exit_filled,
    maybe_write_trade_closeout_on_protective_leg_filled,
)
from app.routers.live_bracket_calibration import (
    _calibrate_live_entry_bracket_to_min_viable,
    classify_blocked_bracket_for_diagnostics,
)
from app.services.broker_execution_reconcile import (
    insert_reconcile_audit,
    run_reconcile_dry_run,
    verify_apply_item,
)
from app.committee.agentic_authority import (
    evaluate_authority_gate,
    evaluate_authority_gate_bulk,
    is_agentic_primary_materialization_enabled,
    should_block_deterministic_materialization,
)
from app.committee.committee2_live_bridge import (
    fetch_committee2_final_decision_for_action,
    structural_entry_verdict_from_committee2_final,
)
from app.routers.committee import (
    HearingCommitRequest,
    committee_final_decision_commit_for_action,
    _require_enabled,
    _fetch_proposal,
    _fetch_snapshot,
    _fetch_hearing_by_proposal,
    _run_refresh,
    _run_evidence_only_refresh,
    _underlying_sf_conn,
)
from app.services.live_intelligence.structural_committee import (
    build_structural_entry_joint_decision,
    build_structural_exit_execution_only_verdict,
)
from app.services.live_intelligence.structural_routing import (
    STRUCTURAL_COMMITTEE_LOGIC_VERSION,
    build_structural_execution_contract_v1,
    build_structural_verdict_envelope_v1,
    contract_executable_bracket,
    is_structural_live_action,
)
from app.services.live_intelligence.live_intent_policy import (
    assert_legacy_execute_forbidden,
    assert_legacy_order_proposals_import_allowed,
    assert_live_committee_policy,
    live_intent_kind_from_row,
    live_structural_only_enabled_cur,
    overview_excluded_intent_kinds,
    structural_proposal_minimum_contract_violations,
)
from app.services.live_intelligence import exit_policy as exit_policy_service

router = APIRouter(prefix="/live", tags=["live"])
_log = logging.getLogger(__name__)

# Structural committee is deterministic (not multi-LLM). Pause briefly between SSE role lines
# so the UI shows each specialist in turn instead of one instantaneous burst.
_STRUCTURAL_COMMITTEE_SSE_ROLE_DELAY_SEC = float(os.environ.get("MIP_STRUCTURAL_COMMITTEE_SSE_DELAY_SEC", "0.38"))


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
    broker_name: str | None = None
    adapter_mode: str | None = Field(default=None, pattern="^(PAPER|LIVE)$")
    base_currency: str | None = None
    max_positions: int | None = None
    max_position_pct: float | None = None
    cash_buffer_pct: float | None = None
    max_slippage_pct: float | None = None
    validity_window_sec: int | None = None
    quote_freshness_threshold_sec: int | None = None
    snapshot_freshness_threshold_sec: int | None = None
    max_bar_end_lag_sec: int | None = None
    drawdown_stop_pct: float | None = None
    bust_pct: float | None = None
    cooldown_bars: int | None = None
    is_active: bool | None = None
    is_execution_enabled: bool | None = None
    real_money_enabled: bool | None = None


class ImportLiveActionsFromProposalsRequest(BaseModel):
    live_portfolio_id: int
    source_portfolio_id: int | None = None
    run_id: str | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    latest_batch_only: bool = True
    allow_stale_import: bool = False
    dedupe_by_symbol: bool = True
    max_proposal_age_days: int = Field(default=7, ge=1, le=180)


class ImportStructuralProposalsRequest(BaseModel):
    live_portfolio_id: int
    limit: int = Field(default=10, ge=1, le=50)
    max_proposal_age_days: int = Field(default=7, ge=1, le=30)
    dedupe_by_symbol: bool = True
    skip_stale: bool = True
    entry_zone_tolerance_pct: float = Field(default=2.0, ge=0, le=20.0)


class ExecuteLiveActionRequest(BaseModel):
    actor: str
    attempt_n: int = Field(default=1, ge=1)
    # Optional client-side context assertions. Backend validates these against
    # the frozen LIVE_ACTIONS fields and LIVE_PORTFOLIO_CONFIG independently
    # (Phase 1 gates are authoritative). Mismatches are rejected before broker submit.
    portfolio_id: int | None = None
    ibkr_account_id: str | None = None
    broker_name: str | None = None
    broker_universe_type: str | None = None


class ApproveAndSubmitLiveDecisionRequest(BaseModel):
    pm_actor: str = "portfolio_manager"
    compliance_actor: str = "compliance_user"
    intent_submit_actor: str = "intent_submitter"
    intent_approve_actor: str = "intent_approver"
    execution_actor: str = "execution_operator"
    committee_actor: str = "committee_orchestrator"
    committee_model: str = "claude-4-sonnet"
    attempt_n: int = Field(default=1, ge=1)
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
    attempt_n: int = Field(default=1, ge=1)
    # Client-side context assertions forwarded to execute_live_action.
    portfolio_id: int | None = None
    ibkr_account_id: str | None = None
    broker_name: str | None = None
    broker_universe_type: str | None = None


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
    portfolio_id: int | None = None  # required for account assertion (Phase 3A)


class CommitteeRunRequest(BaseModel):
    actor: str = "committee_orchestrator"
    model: str = "claude-4-sonnet"
    force_rerun: bool = False
    refresh_ibkr_news: bool = True
    ibkr_news_max_symbols: int = 20
    ibkr_news_max_headlines_per_symbol: int = 5
    ibkr_news_min_symbols_covered: int = 1
    ibkr_news_max_age_minutes: int = 120


class ApplyCommitteeVerdictRequest(BaseModel):
    actor: str = "committee_orchestrator"
    model: str = "claude-4-sonnet"
    verdict: dict = Field(default_factory=dict)


class Committee2OrchestrateRequest(BaseModel):
    """LPA-first Committee 2.0: optional flags only — action_id and proposal_id come from LIVE_ACTIONS."""

    force_rebuild_hearing: bool = False
    force_fresh_shadow: bool = False


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
    total_commission: float | None = None
    notes: str | None = None
    # Optional broker-truth fill time. When set on a FILLED transition, the
    # update preserves the broker's actual execution timestamp instead of
    # stamping current_timestamp(). Reconcile/backfill paths should always
    # pass this; live submit acks can omit it (current_timestamp is correct
    # for fills that just happened).
    filled_at: datetime | None = None


class ReconcileApplyItem(BaseModel):
    exec_key: str
    order_id: str


class ReconcileExecutionsDryRunRequest(BaseModel):
    portfolio_id: int
    lookback_days: int = Field(default=14, ge=1, le=90)
    actor: str = "reconcile_operator"


class ReconcileExecutionsApplyRequest(BaseModel):
    portfolio_id: int
    lookback_days: int = Field(default=14, ge=1, le=90)
    actor: str
    confirm_apply: bool = Field(
        ...,
        description="Must be true after reviewing dry-run; prevents accidental writes.",
    )
    items: list[ReconcileApplyItem] = Field(min_length=1)


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
          PARAM_SNAPSHOT, ONE_MIN_BAR_TS, ONE_MIN_BAR_CLOSE,
          INTENT_SUBMITTED_BY, INTENT_SUBMITTED_TS, INTENT_APPROVED_BY, INTENT_APPROVED_TS, INTENT_REFERENCE_ID,
          COMMITTEE_REQUIRED, COMMITTEE_STATUS, COMMITTEE_RUN_ID, COMMITTEE_COMPLETED_TS, COMMITTEE_VERDICT,
          TRAINING_QUALIFICATION_SNAPSHOT, TRAINING_LIVE_ELIGIBLE, TRAINING_RANK_IMPACT, TRAINING_SIZE_CAP_FACTOR,
          TARGET_EXPECTATION_SNAPSHOT, TARGET_OPEN_CONDITION_FACTOR, TARGET_EXPECTATION_POLICY_VERSION,
          NEWS_CONTEXT_SNAPSHOT, NEWS_CONTEXT_STATE, NEWS_EVENT_SHOCK_FLAG, NEWS_FRESHNESS_BUCKET, NEWS_CONTEXT_POLICY_VERSION,
          REVALIDATION_OUTCOME, REVALIDATION_POLICY_VERSION, REVALIDATION_DATA_SOURCE,
          SETUP_EVENT_ID, SETUP_FAMILY, DIRECTION, ENTRY_ZONE_LOW, ENTRY_ZONE_HIGH,
          INVALIDATION_LEVEL, INVALIDATION_RULE, STRUCTURE_CONFIDENCE, LEVEL_SIGNIFICANCE,
          STRUCTURAL_STATE, REGIME_TAGS, REGIME_COMPAT, TRUST_LABEL,
          MEANINGFUL_HIT_RATE, PATH_SURVIVAL_RATE, MFE_MAE_RATIO, AVG_BARS_TO_THRESHOLD,
          DOMINANT_FAILURE_MODE, BEST_WINDOW, RISK_CLASS, EXIT_STYLE,
          TRAIL_STYLE, TRAIL_PARAMS, TRAIL_ACTIVATION_TYPE, TRAIL_ACTIVATION_PARAM,
          EXIT_POLICY, TRAIL_STATUS, EXIT_POLICY_REASON,
          EXPECTED_HOLD_CHARACTER, MAX_HOLD_BARS,
          SETUP_NARRATIVE, PROPOSAL_RATIONALE,
          CURRENT_PRICE, DISTANCE_TO_ENTRY_ZONE, SETUP_STILL_VALID, PRICE_MOVED_TOO_FAR, FRESHNESS_ASSESSMENT,
          MARKET_TYPE, LIVE_INTENT_KIND,
          BROKER_NAME, IBKR_ACCOUNT_ID, BROKER_UNIVERSE_TYPE
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
            select
              ACTION_ID, STATUS, COMPLIANCE_STATUS, REASON_CODES,
              PORTFOLIO_ID, SYMBOL, SIDE, ACTION_INTENT, EXIT_TYPE,
              LIVE_INTENT_KIND, PROPOSAL_ID, SETUP_EVENT_ID
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
    """Canonical string for permId/orderId matching across IB snapshot vs DB (avoid 2123724407.0 vs 2123724407)."""
    if value is None or isinstance(value, bool):
        return ""
    try:
        from decimal import Decimal

        if isinstance(value, Decimal):
            if value == value.to_integral_value():
                return str(int(value))
            norm = str(value).strip()
            if norm in ("", "0", "0.0", "None", "none", "NULL", "null"):
                return ""
            return norm
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float) and value == int(value):
            return str(int(value))
    except Exception:
        pass
    norm = str(value).strip()
    if norm in ("", "0", "0.0", "None", "none", "NULL", "null"):
        return ""
    if len(norm) > 2 and norm.endswith(".0") and norm[:-2].lstrip("-").isdigit():
        return norm[:-2]
    try:
        f = float(norm)
        if f == int(f):
            return str(int(f))
    except Exception:
        pass
    return norm


def _broker_open_order_ids_from_snapshot_rows(open_orders: list[dict] | None) -> set[str]:
    """Union of OPEN_ORDER_ID and payload perm/order ids so LIVE_ORDERS rows match IB truth."""
    out: set[str] = set()
    for r in open_orders or []:
        oid = _normalize_broker_order_id(r.get("OPEN_ORDER_ID"))
        if oid:
            out.add(oid)
        payload = _parse_variant(r.get("PAYLOAD"))
        if not isinstance(payload, dict):
            continue
        for key in ("permId", "perm_id", "orderId", "order_id"):
            nid = _normalize_broker_order_id(payload.get(key))
            if nid:
                out.add(nid)
    return out


def _broker_placement_order_ids_from_ibkr_submit_payload(payload: dict | None) -> set[str]:
    """
    Perm/order ids from this submit bundle only (orders / orders_after_wait).
    Do not union all account open trades — that caused false post-submit ack when any
    unrelated working order appeared in a stale snapshot while the new legs did not.
    """
    if not isinstance(payload, dict):
        return set()
    out: set[str] = set()
    for key in ("orders_after_wait", "orders"):
        for ow in payload.get(key) or []:
            if not isinstance(ow, dict):
                continue
            for fld in ("perm_id", "order_id"):
                nid = _normalize_broker_order_id(ow.get(fld))
                if nid:
                    out.add(nid)
    return out


def _broker_order_ids_from_ibkr_submit_payload(payload: dict | None) -> set[str]:
    """Union of placement ids plus open_trade_ids_account (diagnostics / legacy callers)."""
    out = _broker_placement_order_ids_from_ibkr_submit_payload(payload)
    if not isinstance(payload, dict):
        return out
    for x in payload.get("open_trade_ids_account") or []:
        nid = _normalize_broker_order_id(x)
        if nid:
            out.add(nid)
    return out


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


def _live_parse_fx_pair_for_broker(symbol: str | None) -> tuple[str, str] | None:
    """
    Map proposal symbols (AUD/USD, AUDUSD) to IB portfolio keys: SYMBOL=base, CURRENCY=quote.
    """
    if not symbol:
        return None
    s = str(symbol).strip().upper().replace(" ", "").replace("-", "/")
    if "/" in s:
        a, b = s.split("/", 1)
        a, b = a.strip(), b.strip()
        if len(a) == 3 and len(b) == 3 and a.isalpha() and b.isalpha():
            return a, b
    if len(s) == 6 and s.isalpha():
        return s[:3], s[3:]
    return None


def _broker_cash_is_currency_balance_only(row_sym: str, row_ccy: str | None, row_sec: str | None) -> bool:
    """IB 'USD' with currency USD is a cash balance line; negative qty is common and not an FX pair short."""
    sec = (row_sec or "").upper().strip()
    s = (row_sym or "").upper().strip()
    c = (row_ccy or "").upper().strip()
    if sec != "CASH":
        return False
    if len(s) != 3 or not s.isalpha():
        return False
    return s == c


def _broker_position_matches_live_action(
    row_sym: str,
    row_ccy: str | None,
    row_sec: str | None,
    action_symbol: str,
    asset_class: str | None,
) -> bool:
    a_sym = (action_symbol or "").upper().strip()
    r_sym = (row_sym or "").upper().strip()
    r_ccy = (row_ccy or "").upper().strip()
    r_sec = (row_sec or "").upper().strip()
    ac = (asset_class or "").upper().strip()
    pq = _live_parse_fx_pair_for_broker(action_symbol)
    treat_as_fx = ac == "FX" or pq is not None
    if treat_as_fx and pq:
        base, quote = pq
        if r_sec == "CASH" and r_sym == base and r_ccy == quote:
            return True
    if treat_as_fx and not pq:
        return r_sym == a_sym
    return r_sym == a_sym


def _negative_broker_line_blocks_long_only_entry(
    row_sym: str,
    row_ccy: str | None,
    row_sec: str | None,
    qty: float,
    action_symbol: str,
    asset_class: str | None,
) -> bool:
    """
    Decide whether a negative POSITION_QTY row should add BROKER_SHORT for this submit.

    IB reports FX / multi-currency exposure as CASH rows (often base EUR, currency USD).
    Those are not equity shorts; blocking a PFE BUY because EUR.USD is negative was a false positive.

    - Stock / ETF / default entries: only negative **non-CASH** lines (STK, OPT, …) block.
    - FX entries: still block negative CASH **pair** lines (symbol != currency) and any security short.
    """
    if qty >= 0:
        return False
    sec = (row_sec or "").upper().strip()
    s = (row_sym or "").upper().strip()
    c = (row_ccy or "").upper().strip()

    if _broker_cash_is_currency_balance_only(s, row_ccy, sec):
        return False
    if sec == "CASH" and len(s) == 3 and s.isalpha() and not c:
        return False

    ac = (asset_class or "").upper().strip()
    is_fx_entry = ac == "FX" or _live_parse_fx_pair_for_broker(action_symbol) is not None

    if not is_fx_entry:
        return sec != "CASH"

    # FX entry: equity/option shorts still violate long-only for the whole account.
    if sec != "CASH":
        return True
    # Negative CASH *pair* line (base != quote): only block this BUY if it is the **same**
    # pair as the proposal. Otherwise unrelated FX exposure (e.g. short EUR/USD) falsely
    # blocked AUD/JPY BUY with "short / long-only" messaging.
    if not c or s == c:
        return False
    return _broker_position_matches_live_action(s, c, sec, action_symbol, asset_class)


def _backfill_local_order_id_to_perm_id(
    cur,
    portfolio_id: int | None = None,
    account_id: str | None = None,
    lookback_days: int = 14,
    action_id: str | None = None,
) -> dict:
    """
    Best-effort backfill: when LIVE_ORDERS.BROKER_ORDER_ID holds a TWS local order_id
    (because IB returned perm_id=0 in the initial ack), look up matching EXECUTION
    snapshots that carry both payload.order_id and payload.perm_id, and rewrite
    LIVE_ORDERS.BROKER_ORDER_ID = perm_id.

    Idempotent: rows whose BROKER_ORDER_ID already equals the perm_id are skipped.
    Returns {scanned, mapped, updated, samples}.
    """
    out = {"scanned": 0, "mapped": 0, "updated": 0, "samples": []}
    try:
        lookback_days = int(lookback_days)
        scopes: list[str] = []
        params: list[object] = []
        if portfolio_id is not None:
            scopes.append("la.PORTFOLIO_ID = %s")
            params.append(int(portfolio_id))
        if account_id:
            scopes.append("la.IBKR_ACCOUNT_ID = %s")
            params.append(str(account_id))
        if action_id:
            scopes.append("la.ACTION_ID = %s")
            params.append(str(action_id))
        scope_sql = (" and " + " and ".join(scopes)) if scopes else ""

        # Pull LIVE_ORDERS rows whose BROKER_ORDER_ID looks like a TWS local order id
        # (small integer) and was touched recently.
        cur.execute(
            f"""
            select la.ORDER_ID, la.BROKER_ORDER_ID, la.SYMBOL, la.IBKR_ACCOUNT_ID,
                   la.PORTFOLIO_ID, la.ACTION_ID
              from MIP.LIVE.LIVE_ORDERS la
             where la.BROKER_ORDER_ID is not null
               and try_to_number(la.BROKER_ORDER_ID) is not null
               and try_to_number(la.BROKER_ORDER_ID) between 1 and 9999999
               and coalesce(la.LAST_UPDATED_AT, la.CREATED_AT) >= dateadd(day, -%s, current_timestamp())
               {scope_sql}
            """,
            tuple([lookback_days] + params),
        )
        candidate_rows = fetch_all(cur)
        out["scanned"] = len(candidate_rows)
        if not candidate_rows:
            return out

        # Build (account, local_id) -> [order rows] index.
        idx: dict[tuple[str, str], list[dict]] = {}
        for r in candidate_rows:
            acct = str(r.get("IBKR_ACCOUNT_ID") or "").strip()
            lid = str(r.get("BROKER_ORDER_ID") or "").strip()
            if not acct or not lid:
                continue
            try:
                lid_norm = str(int(float(lid)))
            except Exception:
                continue
            idx.setdefault((acct, lid_norm), []).append(r)

        if not idx:
            return out

        # Look up matching EXECUTION snapshots that expose both order_id and perm_id.
        accounts = sorted({a for (a, _) in idx.keys()})
        acct_placeholders = ",".join(["%s"] * len(accounts))
        cur.execute(
            f"""
            select distinct
                   IBKR_ACCOUNT_ID,
                   try_to_number(payload:order_id::string)::string as LOCAL_ID,
                   payload:perm_id::string as PERM_ID
              from MIP.LIVE.BROKER_SNAPSHOTS
             where SNAPSHOT_TYPE = 'EXECUTION'
               and IBKR_ACCOUNT_ID in ({acct_placeholders})
               and SNAPSHOT_TS >= dateadd(day, -%s, current_timestamp())
               and payload:order_id is not null
               and payload:perm_id is not null
               and try_to_number(payload:perm_id::string) > 0
            """,
            tuple(accounts + [lookback_days]),
        )
        mapping_rows = fetch_all(cur)

        # (account, local_id) -> perm_id
        mapping: dict[tuple[str, str], str] = {}
        for m in mapping_rows:
            acct = str(m.get("IBKR_ACCOUNT_ID") or "").strip()
            lid = str(m.get("LOCAL_ID") or "").strip()
            pid = str(m.get("PERM_ID") or "").strip()
            if not acct or not lid or not pid:
                continue
            try:
                lid = str(int(float(lid)))
                pid = str(int(float(pid)))
            except Exception:
                continue
            if pid == lid:
                continue
            mapping.setdefault((acct, lid), pid)

        out["mapped"] = len(mapping)
        if not mapping:
            return out

        # Apply the rewrites and write audit ledger entries.
        for (acct, lid), pid in mapping.items():
            for row in idx.get((acct, lid), []):
                order_id = str(row.get("ORDER_ID") or "")
                if not order_id:
                    continue
                cur.execute(
                    """
                    update MIP.LIVE.LIVE_ORDERS
                       set BROKER_ORDER_ID = %s,
                           LAST_UPDATED_AT = current_timestamp()
                     where ORDER_ID = %s
                       and BROKER_ORDER_ID = %s
                    """,
                    (pid, order_id, lid),
                )
                # Best-effort audit (no PII, no semicolons in payload values).
                try:
                    cur.execute(
                        """
                        insert into MIP.LIVE.BROKER_EVENT_LEDGER (
                          EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, ACTION_ID,
                          BROKER_ORDER_ID, SYMBOL, PAYLOAD
                        )
                        select %s, current_timestamp(), 'PERM_ID_BACKFILL', %s, %s,
                               %s, %s, parse_json(%s)
                        """,
                        (
                            str(uuid.uuid4()),
                            row.get("PORTFOLIO_ID"),
                            row.get("ACTION_ID"),
                            pid,
                            row.get("SYMBOL"),
                            json.dumps(
                                {
                                    "actor": "auto_backfill",
                                    "order_id": order_id,
                                    "before": {"BROKER_ORDER_ID": lid},
                                    "after": {"BROKER_ORDER_ID": pid},
                                }
                            ),
                        ),
                    )
                except Exception:
                    pass
                out["updated"] += 1
                if len(out["samples"]) < 10:
                    out["samples"].append(
                        {
                            "order_id": order_id,
                            "symbol": row.get("SYMBOL"),
                            "from": lid,
                            "to": pid,
                        }
                    )
        return out
    except Exception as exc:
        out["error"] = str(exc)
        return out


def _recent_unmapped_execution_summary(
    cur,
    portfolio_id: int,
    account_id: str,
    lookback_days: int = 14,
    sample_limit: int = 10,
) -> dict:
    """
    Detect broker executions that cannot be linked to known local lineage.
    Known lineage includes:
      1) LIVE_ORDERS.BROKER_ORDER_ID
      2) broker IDs captured in BROKER_EVENT_LEDGER EXECUTION_SUBMIT_RESULT payloads
    This is a strong drift signal: broker truth changed without local lineage.
    """
    try:
        lookback_days = int(lookback_days)
        sample_limit = int(max(1, sample_limit))
        portfolio_id = int(portfolio_id)
        account_id = str(account_id or "").strip()
        if not account_id:
            return {
                "count": 0,
                "latest_snapshot_ts": None,
                "symbols": [],
                "sample_broker_order_ids": [],
            }

        # 1) Recent broker executions (deduped by execution key).
        # Capture *all* candidate broker keys per row (perm_id AND TWS local order_id),
        # because LIVE_ORDERS may have been written with the local order_id when IB
        # returned perm_id=0 in the initial ack.
        cur.execute(
            """
            select
              SNAPSHOT_TS,
              upper(coalesce(SYMBOL, '')) as SYMBOL,
              coalesce(
                OPEN_ORDER_ID::string,
                PAYLOAD:perm_id::string,
                PAYLOAD:order_id::string
              ) as BROKER_ORDER_ID,
              OPEN_ORDER_ID::string         as OPEN_ORDER_ID_KEY,
              PAYLOAD:perm_id::string       as PERM_ID_KEY,
              PAYLOAD:order_id::string      as LOCAL_ORDER_ID_KEY,
              coalesce(
                PAYLOAD:exec_id::string,
                concat_ws(
                  ':',
                  coalesce(OPEN_ORDER_ID::string, PAYLOAD:perm_id::string, PAYLOAD:order_id::string, ''),
                  upper(coalesce(SYMBOL, '')),
                  coalesce(PAYLOAD:time::string, SNAPSHOT_TS::string),
                  coalesce(PAYLOAD:shares::string, ''),
                  coalesce(PAYLOAD:price::string, '')
                )
              ) as EXEC_KEY
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'EXECUTION'
              and IBKR_ACCOUNT_ID = %s
              and SNAPSHOT_TS >= dateadd(day, -%s, current_timestamp())
            qualify row_number() over (
              partition by coalesce(
                PAYLOAD:exec_id::string,
                concat_ws(
                  ':',
                  coalesce(OPEN_ORDER_ID::string, PAYLOAD:perm_id::string, PAYLOAD:order_id::string, ''),
                  upper(coalesce(SYMBOL, '')),
                  coalesce(PAYLOAD:time::string, SNAPSHOT_TS::string),
                  coalesce(PAYLOAD:shares::string, ''),
                  coalesce(PAYLOAD:price::string, '')
                )
              )
              order by SNAPSHOT_TS desc
            ) = 1
            """,
            (account_id, lookback_days),
        )
        exec_rows = fetch_all(cur)

        def _exec_candidate_keys(r: dict) -> list[str]:
            keys: list[str] = []
            for raw in (
                r.get("OPEN_ORDER_ID_KEY"),
                r.get("PERM_ID_KEY"),
                r.get("LOCAL_ORDER_ID_KEY"),
                r.get("BROKER_ORDER_ID"),
            ):
                if raw is None:
                    continue
                s = str(raw).strip()
                if not s or s in keys:
                    continue
                keys.append(s)
                # Normalize trailing ".0" coming from VARIANT casts.
                if s.endswith(".0") and s[:-2].isdigit() and s[:-2] not in keys:
                    keys.append(s[:-2])
            return keys

        dedup_execs = [
            {
                "SNAPSHOT_TS": r.get("SNAPSHOT_TS"),
                "SYMBOL": str(r.get("SYMBOL") or "").upper(),
                "BROKER_ORDER_ID": str(r.get("BROKER_ORDER_ID")) if r.get("BROKER_ORDER_ID") is not None else None,
                "EXEC_KEY": str(r.get("EXEC_KEY") or ""),
                "CANDIDATE_KEYS": _exec_candidate_keys(r),
            }
            for r in exec_rows
            if r.get("BROKER_ORDER_ID") is not None
        ]
        if not dedup_execs:
            return {
                "count": 0,
                "latest_snapshot_ts": None,
                "symbols": [],
                "sample_broker_order_ids": [],
                "unmapped_on_flat_symbols_count": 0,
                "unmapped_total_before_flat_filter": 0,
            }

        # 2) Known broker IDs from LIVE_ORDERS.
        cur.execute(
            """
            select distinct BROKER_ORDER_ID
            from MIP.LIVE.LIVE_ORDERS
            where PORTFOLIO_ID = %s
              and BROKER_ORDER_ID is not null
              and coalesce(LAST_UPDATED_AT, CREATED_AT) >= dateadd(day, -%s, current_timestamp())
            """,
            (portfolio_id, lookback_days),
        )
        known_ids = {
            str(r.get("BROKER_ORDER_ID")).strip()
            for r in fetch_all(cur)
            if r.get("BROKER_ORDER_ID") is not None and str(r.get("BROKER_ORDER_ID")).strip()
        }

        # 3) Known broker IDs from broker submit-result payloads (orders + orders_after_wait).
        cur.execute(
            """
            with ledger as (
              select BROKER_ORDER_ID::string as BROKER_ORDER_ID, PAYLOAD
              from MIP.LIVE.BROKER_EVENT_LEDGER
              where PORTFOLIO_ID = %s
                and EVENT_TYPE = 'EXECUTION_SUBMIT_RESULT'
                and EVENT_TS >= dateadd(day, -%s, current_timestamp())
            )
            select distinct coalesce(
              nullif(trim(BROKER_ORDER_ID), ''),
              nullif(trim(v1.value:perm_id::string), ''),
              nullif(trim(v1.value:order_id::string), ''),
              nullif(trim(v2.value:perm_id::string), ''),
              nullif(trim(v2.value:order_id::string), '')
            ) as BROKER_ORDER_ID
            from ledger l,
                 lateral flatten(input => l.PAYLOAD:orders_after_wait, outer => true) v1,
                 lateral flatten(input => l.PAYLOAD:orders, outer => true) v2
            """,
            (portfolio_id, lookback_days),
        )
        known_ids.update(
            {
                str(r.get("BROKER_ORDER_ID")).strip()
                for r in fetch_all(cur)
                if r.get("BROKER_ORDER_ID") is not None and str(r.get("BROKER_ORDER_ID")).strip()
            }
        )

        # 4) Compute unmapped fills against known local lineage.
        # An execution is "mapped" if ANY of its candidate keys (perm_id OR TWS local order_id)
        # matches a known local id. This handles the case where LIVE_ORDERS stored the local
        # order_id while BROKER_SNAPSHOTS later picked up the perm_id.
        unmapped_rows = []
        for r in dedup_execs:
            cand = r.get("CANDIDATE_KEYS") or []
            if not cand:
                primary = str(r.get("BROKER_ORDER_ID") or "").strip()
                if primary:
                    cand = [primary]
            mapped = any(k in known_ids for k in cand if k)
            if not mapped:
                unmapped_rows.append(r)
        if not unmapped_rows:
            return {
                "count": 0,
                "latest_snapshot_ts": None,
                "symbols": [],
                "sample_broker_order_ids": [],
                "unmapped_on_flat_symbols_count": 0,
                "unmapped_total_before_flat_filter": 0,
            }

        # 5) IB is truth for the *current* book: unmapped executions on symbols that are
        # flat at the latest NAV-linked snapshot are historical lineage gaps, not an
        # active risk that should block new submissions after a broker-side flatten.
        ib_open_symbols: set[str] = set()
        latest_nav_ts = None
        try:
            cur.execute(
                """
                with latest as (
                  select max(SNAPSHOT_TS) as SNAPSHOT_TS
                  from MIP.LIVE.BROKER_SNAPSHOTS
                  where SNAPSHOT_TYPE = 'NAV'
                    and IBKR_ACCOUNT_ID = %s
                )
                select
                  l.SNAPSHOT_TS,
                  upper(coalesce(p.SYMBOL, '')) as SYMBOL
                from latest l
                left join MIP.LIVE.BROKER_SNAPSHOTS p
                  on p.SNAPSHOT_TYPE = 'POSITION'
                 and p.IBKR_ACCOUNT_ID = %s
                 and p.SNAPSHOT_TS = l.SNAPSHOT_TS
                 and coalesce(p.POSITION_QTY, 0) <> 0
                """,
                (account_id, account_id),
            )
            pos_rows = fetch_all(cur)
            for pr in pos_rows:
                if pr.get("SNAPSHOT_TS") is not None and latest_nav_ts is None:
                    latest_nav_ts = pr.get("SNAPSHOT_TS")
                sym_p = str(pr.get("SYMBOL") or "").strip().upper()
                if sym_p:
                    ib_open_symbols.add(sym_p)
        except Exception:
            ib_open_symbols = set()
            latest_nav_ts = None

        blocking_unmapped: list[dict] = []
        flat_unmapped: list[dict] = []
        if latest_nav_ts is None:
            # Cannot align to a book snapshot — stay conservative.
            blocking_unmapped = list(unmapped_rows)
        else:
            for r in unmapped_rows:
                sym = str(r.get("SYMBOL") or "").strip().upper()
                if not sym:
                    blocking_unmapped.append(r)
                elif sym in ib_open_symbols:
                    blocking_unmapped.append(r)
                else:
                    flat_unmapped.append(r)

        blocking_unmapped.sort(key=lambda x: str(x.get("SNAPSHOT_TS") or ""), reverse=True)
        sample_rows = blocking_unmapped[:sample_limit]
        symbols = sorted({str(r.get("SYMBOL") or "").upper() for r in sample_rows if str(r.get("SYMBOL") or "").strip()})
        sample_ids = [str(r.get("BROKER_ORDER_ID")) for r in sample_rows if r.get("BROKER_ORDER_ID") is not None]
        return {
            "count": len(blocking_unmapped),
            "latest_snapshot_ts": blocking_unmapped[0].get("SNAPSHOT_TS") if blocking_unmapped else None,
            "symbols": symbols,
            "sample_broker_order_ids": sample_ids,
            "unmapped_on_flat_symbols_count": len(flat_unmapped),
            "unmapped_total_before_flat_filter": len(unmapped_rows),
        }
    except Exception:
        return {
            "count": 0,
            "latest_snapshot_ts": None,
            "symbols": [],
            "sample_broker_order_ids": [],
            "unmapped_on_flat_symbols_count": 0,
            "unmapped_total_before_flat_filter": 0,
        }


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


def _fetch_latest_broker_truth(
    cur,
    account_id: str,
    symbol: str | None = None,
    asset_class: str | None = None,
) -> dict:
    # IB is the source of truth for the *current* book. Every Refresh-From-IB
    # writes a NAV snapshot; POSITION and OPEN_ORDER rows are written at the SAME
    # SNAPSHOT_TS only when the account actually holds positions / has working
    # orders. When IB is flat, a refresh writes NAV with NO POSITION/OPEN_ORDER
    # rows at all.
    #
    # The old behaviour anchored to the latest POSITION/OPEN_ORDER-typed
    # timestamp, which resurrected the last non-empty batch as phantom holdings
    # long after the broker book went flat (portfolio drift: MIP disagreeing with
    # IB — e.g. a month-old AMD/AAPL batch still reading as "held"). Anchor
    # instead to the latest NAV snapshot (the refresh anchor) and read
    # POSITION/OPEN_ORDER rows at that timestamp, so "no rows at the latest
    # refresh" correctly reads as flat. This matches the cockpit/LPA overview and
    # the unmapped-execution reconciliation, which already anchor positions to the
    # latest NAV snapshot.
    cur.execute(
        """
        select max(SNAPSHOT_TS) as SNAPSHOT_TS
        from MIP.LIVE.BROKER_SNAPSHOTS
        where IBKR_ACCOUNT_ID = %s
          and SNAPSHOT_TYPE = 'NAV'
        """,
        (account_id,),
    )
    nav_ts_rows = fetch_all(cur)
    anchor_ts = (nav_ts_rows[0] or {}).get("SNAPSHOT_TS") if nav_ts_rows else None

    if anchor_ts is not None:
        # Normal path: the latest refresh anchor is authoritative for both the
        # open-order set and the per-symbol position. Absence == flat at broker.
        latest_open_ts = anchor_ts
        latest_pos_ts = anchor_ts if symbol else None
    else:
        # Fallback only when an account has no NAV lineage at all (should not
        # happen for active live portfolios): preserve the legacy per-type max so
        # we never silently flatten a genuinely-held legacy book.
        cur.execute(
            """
            select max(SNAPSHOT_TS) as SNAPSHOT_TS
            from MIP.LIVE.BROKER_SNAPSHOTS
            where IBKR_ACCOUNT_ID = %s
              and SNAPSHOT_TYPE = 'OPEN_ORDER'
            """,
            (account_id,),
        )
        open_ts_rows = fetch_all(cur)
        latest_open_ts = (open_ts_rows[0] or {}).get("SNAPSHOT_TS") if open_ts_rows else None

        latest_pos_ts = None
        if symbol:
            cur.execute(
                """
                select max(SNAPSHOT_TS) as SNAPSHOT_TS
                from MIP.LIVE.BROKER_SNAPSHOTS
                where IBKR_ACCOUNT_ID = %s
                  and SNAPSHOT_TYPE = 'POSITION'
                """,
                (account_id,),
            )
            pos_ts_rows = fetch_all(cur)
            latest_pos_ts = (pos_ts_rows[0] or {}).get("SNAPSHOT_TS") if pos_ts_rows else None

    if not latest_open_ts and not latest_pos_ts:
        return {
            "snapshot_ts": None,
            "open_order_ids": set(),
            "open_orders": [],
            "symbol_position_qty": 0.0,
            "has_symbol_position": False,
        }

    open_orders: list[dict] = []
    if latest_open_ts:
        cur.execute(
            """
            select OPEN_ORDER_ID, OPEN_ORDER_STATUS, SYMBOL, OPEN_ORDER_QTY, OPEN_ORDER_FILLED, OPEN_ORDER_REMAINING, PAYLOAD
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'OPEN_ORDER'
              and IBKR_ACCOUNT_ID = %s
              and SNAPSHOT_TS = %s
            """,
            (account_id, latest_open_ts),
        )
        open_orders = fetch_all(cur)
    open_order_ids = _broker_open_order_ids_from_snapshot_rows(open_orders)

    symbol_position_qty = 0.0
    has_symbol_position = False
    if symbol and latest_pos_ts:
        # IBKR FX: POSITION rows use SYMBOL=base, CURRENCY=quote (e.g. AUD + JPY), not "AUD/JPY".
        cur.execute(
            """
            select
              upper(coalesce(SYMBOL, '')) as SYMBOL,
              upper(coalesce(CURRENCY, '')) as CURRENCY,
              upper(coalesce(SECURITY_TYPE, '')) as SECURITY_TYPE,
              POSITION_QTY
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'POSITION'
              and IBKR_ACCOUNT_ID = %s
              and SNAPSHOT_TS = %s
              and coalesce(POSITION_QTY, 0) <> 0
            """,
            (account_id, latest_pos_ts),
        )
        pos_rows = fetch_all(cur)
        for row in pos_rows:
            if _broker_position_matches_live_action(
                str(row.get("SYMBOL") or ""),
                row.get("CURRENCY"),
                row.get("SECURITY_TYPE"),
                symbol,
                asset_class,
            ):
                symbol_position_qty += float(row.get("POSITION_QTY") or 0.0)
        has_symbol_position = abs(symbol_position_qty) > 0

    meta_ts = latest_open_ts or latest_pos_ts
    if latest_open_ts and latest_pos_ts:
        meta_ts = max(latest_open_ts, latest_pos_ts)

    return {
        "snapshot_ts": meta_ts,
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


def _live_order_display_status(order_row: dict, broker_open_order_ids: set[str]) -> str:
    """
    Align UI status with IBKR snapshot: local working states only if this row's
    BROKER_ORDER_ID is still in the latest open-order snapshot.

    We intentionally do **not** carve out \"symbol still has a position\": stale bracket
    legs (parent/TP/SL) would otherwise stay PENDINGSUBMIT forever while the stock
    is held for an unrelated working position.
    """
    raw = str(order_row.get("STATUS") or "").upper()
    if raw not in LIVE_ORDER_ACTIVE_STATUSES:
        return str(order_row.get("STATUS") or "") or "—"
    if not _is_order_active_in_broker_truth(order_row, broker_open_order_ids):
        return "NOT_ACTIVE_AT_BROKER"
    return str(order_row.get("STATUS") or "") or "—"


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


def _required_next_step_structural_entry(
    status: str,
    *,
    agentic_verdict: dict | None = None,
    reason_codes: list | None = None,
) -> str:
    """Operator-facing next step for structural ENTRY rows (agentic-primary path)."""
    status_upper = (status or "").upper()
    av = agentic_verdict or {}
    authority_status = str(av.get("authority_status") or "").upper()
    gate_enabled = bool(av.get("gate_enabled"))
    gate_ok = bool(av.get("gate_ok")) if gate_enabled else True
    authority_mode = str(av.get("authority_mode") or "").upper()
    is_stale = bool(av.get("is_stale"))

    if authority_status == "AGENTIC_WAIT_RECLAIM":
        return "Committee says Wait/Reclaim — re-run when entry improves"
    if authority_status == "AGENTIC_DEFER":
        return "Committee deferred — re-run when conditions improve or Reject"
    if authority_status == "AGENTIC_REJECT":
        return "Committee rejected — Reject this action"
    if authority_status in ("AGENTIC_DEGRADED_NO_AUTHORITY", "AGENTIC_FAILED_NO_AUTHORITY"):
        return "Run Agentic Review"

    if is_stale and authority_mode == "OPERATOR_COMMITTED":
        return "Authority stale — re-run Agentic Review"

    if status_upper == "OPEN_BLOCKED":
        rc_list = list(reason_codes or [])
        if _reason_codes_include_bracket_contract_block(rc_list):
            return "Bracket contract incomplete — TP/SL not seeded; refresh page after fix or re-run Agentic Review"
        if (
            authority_mode == "OPERATOR_COMMITTED"
            and authority_status in ("AGENTIC_APPROVE", "AGENTIC_APPROVE_REDUCED")
        ):
            if _reason_codes_include_opening_guard_block(rc_list):
                return "Opening guard active — when market opens, click Recheck opening guard (verdict already recorded)"
            return "Action blocked after approval — check reason codes or click Recheck opening guard"
        if authority_status in ("AGENTIC_APPROVE", "AGENTIC_APPROVE_REDUCED"):
            return "Confirm committee verdict, then wait for market open"
        return "Run Agentic Review, then wait for market open"

    if gate_enabled and not gate_ok:
        if authority_status in ("AGENTIC_APPROVE", "AGENTIC_APPROVE_REDUCED"):
            if authority_mode != "OPERATOR_COMMITTED":
                return "Confirm approval"
            if status_upper in ("INTENT_APPROVED", "REVALIDATED_FAIL"):
                return "Check price for Submit"
        elif not authority_status:
            return "Run Agentic Review"

    if status_upper in (
        "RESEARCH_IMPORTED", "PROPOSED", "PENDING_OPEN_VALIDATION",
        "OPEN_ELIGIBLE", "OPEN_CAUTION", "READY_FOR_APPROVAL_FLOW",
        "PM_ACCEPTED", "COMPLIANCE_APPROVED", "INTENT_SUBMITTED",
    ):
        return "Run Agentic Review"

    if status_upper == "INTENT_APPROVED":
        if gate_enabled and gate_ok:
            return "Check price for Submit"
        return "Run Agentic Review"

    if status_upper == "REVALIDATED_FAIL":
        return "Check price for Submit"

    if status_upper == "REVALIDATED_PASS":
        if gate_enabled and not gate_ok:
            return "Review submission gates"
        return "Ready to submit"

    return _required_next_step_for_status(status)


def _auto_import_latest_proposals_for_live_portfolio(
    live_portfolio_id: int,
    *,
    source_portfolio_id: int | None = None,
    limit: int = 200,
    cur=None,
) -> dict:
    """
    Best-effort bridge from research proposals -> live actions so
    /live/activity/overview reflects latest proposal output.

    When LIVE_STRUCTURAL_ONLY is enabled, the legacy ORDER_PROPOSALS
    importer is disallowed by policy, but we still want the Structural
    Timeline proposals to materialise automatically in LPA. Delegate to
    the structural importer so AAPL/CAT/MCD etc. actually land as LIVE
    actions without the operator having to curl the admin endpoint.
    """
    if cur is not None and _live_structural_only_enabled(cur):
        try:
            safe_struct_limit = max(1, min(int(limit or 10), 50))
            struct_result = import_structural_proposals(
                ImportStructuralProposalsRequest(
                    live_portfolio_id=int(live_portfolio_id),
                    limit=safe_struct_limit,
                    max_proposal_age_days=7,
                    dedupe_by_symbol=True,
                    skip_stale=True,
                )
            )
            res = struct_result if isinstance(struct_result, dict) else {}
            return {
                "attempted": True,
                "ok": True,
                "structural_only_mode": True,
                "candidate_count": int(res.get("candidate_count") or 0),
                "imported_count": int(res.get("imported_count") or 0),
                "skipped_existing_count": int(res.get("skipped_existing_count") or 0),
                "skipped_symbol_live_position_count": int(
                    res.get("skipped_live_position_count") or 0
                ),
                "skipped_long_only_count": int(res.get("skipped_long_only_count") or 0),
                "skipped_stale_count": int(res.get("skipped_stale_count") or 0),
                "source_scope": "STRUCTURAL_TRADE_PROPOSALS",
                "latest_batch_date": None,
            }
        except HTTPException as http_exc:
            # Don't break overview load on import failure — surface diagnostics only.
            try:
                detail = http_exc.detail if isinstance(http_exc.detail, dict) else {"message": str(http_exc.detail)}
            except Exception:
                detail = {"message": "structural import failed"}
            return {
                "attempted": True,
                "ok": False,
                "structural_only_mode": True,
                "candidate_count": 0,
                "imported_count": 0,
                "skipped_existing_count": 0,
                "skipped_symbol_live_position_count": 0,
                "source_scope": "STRUCTURAL_TRADE_PROPOSALS",
                "latest_batch_date": None,
                "error": detail,
            }
        except Exception as exc:
            return {
                "attempted": True,
                "ok": False,
                "structural_only_mode": True,
                "candidate_count": 0,
                "imported_count": 0,
                "skipped_existing_count": 0,
                "skipped_symbol_live_position_count": 0,
                "source_scope": "STRUCTURAL_TRADE_PROPOSALS",
                "latest_batch_date": None,
                "error": {"message": str(exc)},
            }
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


def _fetch_entry_intel_baseline(cur, snapshot_id: str | None) -> dict | None:
    if not snapshot_id:
        return None
    try:
        cur.execute(
            """
            select WORLDS_SPEC, ALPHA_SPEC, SOURCE_VERSION, EIS_VERSION
            from MIP.LIVE.ENTRY_INTEL_SNAPSHOT
            where SNAPSHOT_ID = %s
            limit 1
            """,
            (snapshot_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "snapshot_id": snapshot_id,
            "worlds_spec": _parse_variant(row[0]),
            "alpha_spec": _parse_variant(row[1]),
            "eis_source_version": row[2],
            "eis_version": row[3],
        }
    except Exception:
        return None


def _effective_alpha_baseline_action(alpha: dict | None) -> str | None:
    """
    Phase 1 stub rows (phase=1, stub) or missing schema have no actionable baseline.
    """
    if not isinstance(alpha, dict) or not alpha:
        return None
    if alpha.get("stub") is True and alpha.get("phase") == 1:
        return None
    if not alpha.get("alpha_schema_version") and not alpha.get("recommended_action"):
        return None
    act = str(alpha.get("recommended_action") or "").upper()
    if act in ("ENTER", "REDUCE", "SKIP"):
        return act
    return None


def _alpha_committee_alignment_pair(alpha_action: str, committee_rec: str) -> tuple[str, str]:
    a = (alpha_action or "UNKNOWN").upper()
    c = (committee_rec or "UNKNOWN").upper()
    if a == "ENTER" and c == "PROCEED":
        return "ALIGNED", "alpha_ENTER_committee_PROCEED"
    if a == "ENTER" and c == "PROCEED_REDUCED":
        return "DIVERGENT", "alpha_ENTER_committee_REDUCED_SIZE"
    if a == "ENTER" and c == "BLOCK":
        return "DIVERGENT", "alpha_ENTER_committee_BLOCK"
    if a == "REDUCE" and c == "PROCEED":
        return "DIVERGENT", "alpha_REDUCE_committee_PROCEED"
    if a == "REDUCE" and c == "PROCEED_REDUCED":
        return "ALIGNED", "alpha_REDUCE_committee_PROCEED_REDUCED"
    if a == "REDUCE" and c == "BLOCK":
        return "ALIGNED", "alpha_REDUCE_committee_BLOCK"
    if a == "SKIP" and c == "BLOCK":
        return "ALIGNED", "alpha_SKIP_committee_BLOCK"
    if a == "SKIP" and c in ("PROCEED", "PROCEED_REDUCED"):
        return "DIVERGENT", "alpha_SKIP_committee_PROCEED"
    return "UNKNOWN", f"alpha_{a}_committee_{c}"


def _classify_alpha_override_class(alpha_action: str | None, committee_rec: str) -> str:
    """
    Phase 3 explicit committee vs alpha semantics (ENTRY-oriented).
    """
    if not alpha_action:
        return "NO_ALPHA_BASELINE"
    a = alpha_action.upper()
    c = (committee_rec or "UNKNOWN").upper()
    if a == "ENTER" and c == "PROCEED":
        return "ACCEPT_ALPHA"
    if a == "ENTER" and c == "PROCEED_REDUCED":
        return "REDUCE_VS_ALPHA"
    if a == "ENTER" and c == "BLOCK":
        return "BLOCK_DESPITE_ALPHA"
    if a == "REDUCE" and c == "PROCEED":
        return "INCREASE_VS_ALPHA"
    if a == "REDUCE" and c in ("PROCEED_REDUCED", "BLOCK"):
        return "ACCEPT_ALPHA"
    if a == "SKIP" and c == "BLOCK":
        return "ACCEPT_ALPHA"
    if a == "SKIP" and c in ("PROCEED", "PROCEED_REDUCED"):
        return "INCREASE_VS_ALPHA"
    return "UNKNOWN_OVERRIDE"


def _outputs_acknowledge_alpha_deviation(outputs: list[dict]) -> bool:
    """True if any role reason/summary plausibly addresses baseline deviation or alignment."""
    markers = (
        "ALPHA_BASELINE_ALIGNED",
        "ALPHA_BASELINE_DEVIATION",
        "EIS_BASELINE",
        "PRE-TRADE BASELINE",
        "PRE_TRADE",
        "WORLDS_SPEC",
        "DEVIATE FROM BASELINE",
        "AGAINST_ALPHA",
        "BASELINE_SKIP",
        "BASELINE_ENTER",
        "BASELINE_REDUCE",
    )
    for o in outputs or []:
        reasons = o.get("reasons") if isinstance(o.get("reasons"), list) else []
        for r in reasons:
            rs = str(r).upper()
            if any(m in rs for m in markers):
                return True
            if "BASELINE" in rs and ("ALIGN" in rs or "DIVERG" in rs or "DEVIAT" in rs):
                return True
        summary = str(o.get("summary") or "").upper()
        if "PRE-TRADE" in summary or "EIS" in summary or "ALPHA BASELINE" in summary:
            return True
    return False


def _alpha_override_consensus_note_v1(override_class: str) -> str:
    if override_class == "BLOCK_DESPITE_ALPHA":
        return (
            "V1: BLOCK uses existing committee supermajority rule. "
            "BLOCK_DESPITE_ALPHA is high-impact; all roles should cite explicit rationale vs alpha."
        )
    if override_class == "INCREASE_VS_ALPHA":
        return (
            "V1: INCREASE_VS_ALPHA (e.g. proceeding vs SKIP baseline) is high-impact; "
            "no extra consensus gate yet—document in reasons. Phase 4+ may add gates."
        )
    if override_class == "REDUCE_VS_ALPHA":
        return "V1: REDUCE_VS_ALPHA is expected when risk layers tighten vs deterministic alpha; document drivers."
    if override_class == "ACCEPT_ALPHA":
        return "V1: Outcome class matches deterministic alpha posture for this recommendation."
    if override_class == "NO_ALPHA_BASELINE":
        return "V1: No actionable alpha baseline (missing EIS, stub, or unknown action); committee operates without alpha binding."
    return "V1: Unknown override mapping; review alpha vs recommendation manually."


def _append_alpha_phase3_reason_codes(
    reason_codes: list[str],
    override_class: str,
    justification_status: str,
) -> None:
    """Mutates reason_codes with deterministic audit tags (idempotent append)."""
    tag = f"ALPHA_OVERRIDE_{override_class}"
    if tag not in reason_codes:
        reason_codes.append(tag)
    jst = f"ALPHA_DEVIATION_JUSTIFICATION_{justification_status}"
    if jst not in reason_codes:
        reason_codes.append(jst)


def _committee_alpha_phase3_envelope(
    context: dict,
    verdict: dict,
    outputs: list[dict] | None,
    *,
    action_intent: str,
    manual_apply: bool,
    reason_codes: list[str],
) -> dict:
    """
    Phase 3 VERDICT_JSON extension: override class, justification audit, nested entry_intel_audit_v1.
    Mutates reason_codes.
    """
    intent_u = str(action_intent or "ENTRY").upper()
    baseline = context.get("entry_intel_baseline") if isinstance(context.get("entry_intel_baseline"), dict) else {}
    alpha = baseline.get("alpha_spec") if isinstance(baseline.get("alpha_spec"), dict) else {}
    effective = _effective_alpha_baseline_action(alpha)
    rec = str(verdict.get("recommendation") or "").upper()
    override = _classify_alpha_override_class(effective, rec)

    if intent_u == "EXIT":
        justification_status = "NOT_APPLICABLE_EXIT"
    elif manual_apply:
        justification_status = "NOT_EVALUATED_MANUAL_APPLY"
    elif override in ("NO_ALPHA_BASELINE", "UNKNOWN_OVERRIDE", "ACCEPT_ALPHA"):
        justification_status = "NOT_REQUIRED"
    elif _outputs_acknowledge_alpha_deviation(outputs or []):
        justification_status = "PRESENT"
    else:
        justification_status = "MISSING"

    _append_alpha_phase3_reason_codes(reason_codes, override, justification_status)

    flat = _committee_verdict_alignment_fields(context, verdict)
    audit = _entry_intel_audit_v1(
        context,
        verdict,
        outputs,
        action_intent=intent_u,
        manual_apply=manual_apply,
        justification_status=justification_status,
    )
    return {**flat, "entry_intel_audit_v1": audit}


def _committee_verdict_alignment_fields(context: dict, verdict: dict) -> dict:
    """Flat legacy fields on VERDICT_JSON (compat). Prefer entry_intel_audit_v1 for new consumers."""
    baseline = context.get("entry_intel_baseline") if isinstance(context, dict) else None
    alpha = (baseline or {}).get("alpha_spec") if isinstance(baseline, dict) else {}
    if not isinstance(alpha, dict):
        alpha = {}
    effective = _effective_alpha_baseline_action(alpha)
    base_action = str(effective or "UNKNOWN").upper()
    rec = str(verdict.get("recommendation") or "").upper()
    align, notes = _alpha_committee_alignment_pair(base_action, rec)
    override = _classify_alpha_override_class(effective, rec)
    return {
        "alpha_baseline_action": base_action,
        "committee_recommendation_summary": rec,
        "alpha_committee_alignment": align,
        "committee_vs_alpha_notes": notes,
        "alpha_override_class": override,
    }


def _entry_intel_audit_v1(
    context: dict,
    verdict: dict,
    outputs: list[dict] | None,
    *,
    action_intent: str,
    manual_apply: bool,
    justification_status: str,
) -> dict:
    """Structured audit block for COMMITTEE_VERDICT.VERDICT_JSON (Phase 3)."""
    baseline = context.get("entry_intel_baseline") if isinstance(context.get("entry_intel_baseline"), dict) else {}
    alpha = baseline.get("alpha_spec") if isinstance(baseline.get("alpha_spec"), dict) else {}
    worlds = baseline.get("worlds_spec") if isinstance(baseline.get("worlds_spec"), dict) else {}
    effective = _effective_alpha_baseline_action(alpha)
    rec = str(verdict.get("recommendation") or "").upper()
    override = _classify_alpha_override_class(effective, rec)
    align, notes = _alpha_committee_alignment_pair(str(effective or "UNKNOWN").upper(), rec)
    dist = worlds.get("historical_distribution") if isinstance(worlds.get("historical_distribution"), dict) else {}
    return {
        "comparison_rule_version": "ALPHA_COMMITTEE_V3",
        "action_intent": str(action_intent or "ENTRY").upper(),
        "entry_intel_snapshot_id": context.get("entry_intel_snapshot_id"),
        "eis_source_version": baseline.get("eis_source_version"),
        "eis_version": baseline.get("eis_version"),
        "worlds_schema_version": worlds.get("schema_version"),
        "alpha_schema_version": alpha.get("alpha_schema_version"),
        "hod_sample_size": dist.get("sample_size"),
        "alpha_baseline_action": str(effective or "UNKNOWN").upper(),
        "alpha_baseline_size_band": alpha.get("recommended_size_band"),
        "alpha_expected_value_net": alpha.get("expected_value_net"),
        "committee_recommendation": rec,
        "alpha_override_class": override,
        "alpha_committee_alignment_legacy": align,
        "committee_vs_alpha_notes": notes,
        "alpha_deviation_justification_status": justification_status,
        "alpha_override_consensus_note_v1": _alpha_override_consensus_note_v1(override),
        "manual_stream_apply": bool(manual_apply),
        "role_output_count": len(outputs or []),
    }


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


def _live_structural_only_enabled(cur) -> bool:
    """Deployment flag: when true, legacy live proposal import and legacy committee are forbidden."""
    return live_structural_only_enabled_cur(cur)


def _parse_bool_config(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    raw = str(value).strip().lower()
    if raw in ("1", "true", "yes", "on", "y"):
        return True
    if raw in ("0", "false", "no", "off", "n"):
        return False
    return default


def _merge_unique_reason_codes(base: list[str], extra: list[str]) -> list[str]:
    """Append uppercase reason codes from extra onto base without duplicates; preserve order."""
    out: list[str] = []
    seen: set[str] = set()
    for item in (base or []) + (extra or []):
        u = str(item).strip().upper()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _live_execution_requires_ib_risk_gates(cfg_row: dict | None) -> bool:
    """Match execute_live_action use_ibkr_submit semantics for bracket/risk gates."""
    adapter_mode = str((cfg_row or {}).get("ADAPTER_MODE") or "PAPER").upper()
    execution_mode = str(os.getenv("LIVE_EXECUTION_MODE", "AUTO")).upper()
    use_ibkr_submit = adapter_mode == "LIVE"
    if execution_mode == "IBKR":
        use_ibkr_submit = True
    elif execution_mode == "PLACEHOLDER":
        use_ibkr_submit = False
    return use_ibkr_submit


def _load_live_entry_fee_params(cur) -> dict:
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
    return {
        "slippage_bps": slippage_bps,
        "fee_bps": fee_bps,
        "spread_bps": spread_bps,
        "min_net_tp_bps": float(os.getenv("LIVE_MIN_NET_TP_BPS", "5")),
        "min_rr": float(os.getenv("LIVE_MIN_R_MULTIPLE", "1.10")),
    }


def _live_rr_meets_min_floor(
    target_return: float,
    stop_loss_pct: float,
    min_rr: float,
) -> bool:
    """R/R check on 6dp-rounded pct fields (parity with structural bracket builder)."""
    try:
        tr = round(float(target_return), 6)
        sl = round(float(stop_loss_pct), 6)
    except (TypeError, ValueError):
        return False
    if sl <= 0:
        return False
    return (tr / sl) >= float(min_rr) - 1e-12


def _normalize_bust_pct_cap(raw: float | None) -> float | None:
    """BUST_PCT<=0 means unset (no portfolio stop cap). Zero must not zero out SL math."""
    if raw is None:
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _joint_decision_stop_loss_pct(
    joint_decision: dict | None,
    bust_pct_default: float | None,
) -> float | None:
    jd = joint_decision or {}
    stop_loss_pct = None
    try:
        if jd.get("stop_loss_pct") is not None:
            stop_loss_pct = float(jd.get("stop_loss_pct"))
    except (TypeError, ValueError):
        stop_loss_pct = None
    bust_cap = _normalize_bust_pct_cap(bust_pct_default)
    if stop_loss_pct is not None and bust_cap is not None:
        stop_loss_pct = min(float(stop_loss_pct), bust_cap)
    return stop_loss_pct


def _select_executable_target_from_joint_decision(
    joint_decision: dict | None,
    bust_pct_default: float | None,
    *,
    min_rr: float | None = None,
) -> tuple[float | None, float | None, str | None]:
    """Pick a live executable target_return that satisfies min R/R when possible.

    Early-exit/management targets are used only when they pass the floor; otherwise
    falls back to realistic_target_return. Returns (target_return, stop_loss_pct, source).
    """
    jd = joint_decision or {}
    stop_loss_pct = _joint_decision_stop_loss_pct(jd, bust_pct_default)
    if stop_loss_pct is None or float(stop_loss_pct) <= 0:
        return None, stop_loss_pct, None

    floor = float(min_rr if min_rr is not None else float(os.getenv("LIVE_MIN_R_MULTIPLE", "1.10")))

    candidates: list[tuple[str, float]] = []
    for source, key in (
        ("acceptable_early_exit_target_return", "acceptable_early_exit_target_return"),
        ("realistic_target_return", "realistic_target_return"),
    ):
        try:
            raw = jd.get(key)
            if raw is not None:
                val = float(raw)
                if val > 0:
                    candidates.append((source, val))
        except (TypeError, ValueError):
            continue

    for source, tr in candidates:
        if _live_rr_meets_min_floor(tr, stop_loss_pct, floor):
            return tr, stop_loss_pct, source

    if candidates:
        source, tr = candidates[-1]
        return tr, stop_loss_pct, source
    return None, stop_loss_pct, None


def _live_target_and_stop_from_joint_decision(
    joint_decision: dict | None,
    bust_pct_default: float | None,
    *,
    min_rr: float | None = None,
) -> tuple[float | None, float | None]:
    """Executable live bracket target/stop; prefers R/R-viable targets over early-exit."""
    tr, sl, _source = _select_executable_target_from_joint_decision(
        joint_decision,
        bust_pct_default,
        min_rr=min_rr,
    )
    return tr, sl


def _load_executable_entry_bracket_for_action(
    cur,
    *,
    action: dict,
    committee_run_id,
    bust_pct_default: float | None,
) -> tuple[float | None, float | None, str]:
    """
    Prefer structural_execution_contract_v1.executable_bracket, then PARAM_SNAPSHOT.executable_bracket,
    else COMMITTEE_VERDICT joint_decision.
    Returns (target_return, stop_loss_pct, source) where source is structural_contract|snapshot|verdict|none.
    """
    ps = _parse_variant(action.get("PARAM_SNAPSHOT"))
    if isinstance(ps, dict):
        sec = ps.get("structural_execution_contract_v1")
        if isinstance(sec, dict):
            eb_sc = contract_executable_bracket(sec)
            if isinstance(eb_sc, dict) and eb_sc.get("blocked"):
                return None, None, "blocked"
            if isinstance(eb_sc, dict) and eb_sc.get("target_return") is not None:
                try:
                    tr = float(eb_sc["target_return"])
                    sl_raw = eb_sc.get("stop_loss_pct")
                    sl = float(sl_raw) if sl_raw is not None else None
                except Exception:
                    tr, sl = None, None
                else:
                    bust_cap = _normalize_bust_pct_cap(bust_pct_default)
                    if sl is not None and bust_cap is not None:
                        sl = min(float(sl), bust_cap)
                    if tr is not None and tr > 0 and sl is not None and sl > 0:
                        return tr, sl, "structural_contract"
            jd_sc = sec.get("joint_decision")
            if isinstance(jd_sc, dict):
                tr2, sl2 = _live_target_and_stop_from_joint_decision(jd_sc, bust_pct_default)
                if tr2 is not None and sl2 is not None and float(tr2) > 0 and float(sl2) > 0:
                    return float(tr2), float(sl2), "structural_contract"
    if isinstance(ps, dict):
        eb = ps.get("executable_bracket")
        if isinstance(eb, dict) and eb.get("blocked"):
            return None, None, "blocked"
        if isinstance(eb, dict) and eb.get("target_return") is not None:
            try:
                tr = float(eb["target_return"])
                sl_raw = eb.get("stop_loss_pct")
                sl = float(sl_raw) if sl_raw is not None else None
            except Exception:
                tr, sl = None, None
            else:
                bust_cap = _normalize_bust_pct_cap(bust_pct_default)
                if sl is not None and bust_cap is not None:
                    sl = min(float(sl), bust_cap)
                if tr is not None and tr > 0 and sl is not None and sl > 0:
                    return tr, sl, "snapshot"
    if not committee_run_id:
        return None, None, "none"
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
        (committee_run_id,),
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
    stop_loss_pct_default = _normalize_bust_pct_cap(bust_pct_default)
    stop_loss_pct = float(committee_stop_loss_pct) if committee_stop_loss_pct is not None else stop_loss_pct_default
    if stop_loss_pct is not None and stop_loss_pct_default is not None:
        stop_loss_pct = min(float(stop_loss_pct), stop_loss_pct_default)
    if target_return is not None and stop_loss_pct is not None:
        return target_return, stop_loss_pct, "verdict"
    return None, None, "none"


def _live_ib_entry_risk_reason_codes(
    *,
    side: str,
    is_exit: bool,
    entry_price: float | None,
    target_return: float | None,
    stop_loss_pct: float | None,
    fee_params: dict,
) -> list[str]:
    """
    Live IB entry bracket viability: same rules as execute_live_action (TP/SL presence,
    net edge vs fees, R-multiple). Quantity does not affect these percentage checks.
    """
    if is_exit:
        return []
    codes: list[str] = []
    if target_return is None or target_return <= 0:
        codes.append("LIVE_TP_REQUIRED_MISSING")
    if stop_loss_pct is None or stop_loss_pct <= 0:
        codes.append("LIVE_SL_REQUIRED_MISSING")
    tp_price = None
    sl_price = None
    if entry_price is not None:
        ep = float(entry_price)
        if target_return is not None:
            trv = float(target_return)
            if side == "BUY":
                tp_price = math.fsum([ep, ep * trv])
            elif side == "SELL":
                tp_price = max(math.fsum([ep, -ep * trv]), 0.0001)
        if stop_loss_pct is not None:
            sl = float(stop_loss_pct)
            if side == "BUY":
                sl_price = max(math.fsum([ep, -ep * sl]), 0.0001)
            elif side == "SELL":
                sl_price = math.fsum([ep, ep * sl])
    if tp_price is None or sl_price is None:
        codes.append("LIVE_BRACKET_REQUIRED")
    slippage_bps = float(fee_params.get("slippage_bps") or 2.0)
    fee_bps = float(fee_params.get("fee_bps") or 1.0)
    spread_bps = float(fee_params.get("spread_bps") or 0.0)
    fee_return_floor = (slippage_bps + fee_bps + (spread_bps / 2.0)) / 10000.0
    min_net_tp_bps = float(fee_params.get("min_net_tp_bps") or 5.0)
    min_rr = float(fee_params.get("min_rr") or 1.10)
    min_tp_required = fee_return_floor + (min_net_tp_bps / 10000.0)
    if target_return is not None and target_return <= min_tp_required:
        codes.append("LIVE_TP_NET_EDGE_TOO_LOW")
    if target_return is not None and stop_loss_pct is not None and stop_loss_pct > 0:
        rr_multiple = float(target_return) / float(stop_loss_pct)
        if rr_multiple < min_rr:
            codes.append("LIVE_RISK_REWARD_TOO_LOW")
    return codes


_ENTRY_BRACKET_HARD_BLOCK_CODES = frozenset({
    "MAX_POSITIONS_EXCEEDED",
    "MAX_POSITION_PCT_EXCEEDED",
    "CASH_BUFFER_BREACH",
    "MISSING_NOTIONAL_INPUT",
    "LIVE_TP_REQUIRED_MISSING",
    "LIVE_SL_REQUIRED_MISSING",
    "LIVE_BRACKET_REQUIRED",
    "LIVE_TP_NET_EDGE_TOO_LOW",
    "LIVE_RISK_REWARD_TOO_LOW",
    "LIVE_MIN_VIABLE_SIZE_NOT_REACHED",
    "LIVE_BRACKET_NOT_VIABLE_WITHIN_GUARDRAILS",
    "LIVE_BRACKET_CALIBRATION_EXCEEDS_MAX_TP",
    "LIVE_BRACKET_CALIBRATION_EXCEEDS_MAX_SL",
    "STRUCT_SUBMIT_CONTRACT_INCOMPLETE",
    "ENTRY_SIDE_NOT_ALLOWED_LONG_ONLY",
    "BROKER_SHORT_POSITION_OUT_OF_POLICY",
    "SYMBOL_SHORT_POSITION_OUT_OF_POLICY",
})


def _is_entry_bracket_hard_block_code(code: str) -> bool:
    u = str(code or "").strip().upper()
    return u in _ENTRY_BRACKET_HARD_BLOCK_CODES or u.startswith("LIVE_BRACKET_")


def _reason_codes_include_bracket_contract_block(reason_codes: list | None) -> bool:
    return any(_is_entry_bracket_hard_block_code(rc) for rc in (reason_codes or []))


def _reason_codes_include_opening_guard_block(reason_codes: list | None) -> bool:
    opening_codes = {
        "OPEN_MARKET_CLOSED",
        "OPEN_SNAPSHOT_MISSING",
        "OPEN_SNAPSHOT_STALE",
        "OPEN_GAP_BLOCK",
        "OPEN_LIVE_GUARD_FAILED",
        "OPEN_MISSING_SYMBOL",
    }
    return any(str(rc or "").strip().upper() in opening_codes for rc in (reason_codes or []))


def _strip_recomputable_entry_bracket_codes(reason_codes: list[str]) -> list[str]:
    return [rc for rc in reason_codes if not _is_entry_bracket_hard_block_code(rc)]


def _structural_ref_price_for_bracket(action: dict) -> float | None:
    for key in ("REVALIDATION_PRICE", "PROPOSED_PRICE", "CURRENT_PRICE", "ONE_MIN_BAR_CLOSE"):
        try:
            v = action.get(key)
            if v is not None:
                px = float(v)
                if px > 0:
                    return px
        except (TypeError, ValueError):
            pass
    try:
        lo = action.get("ENTRY_ZONE_LOW")
        hi = action.get("ENTRY_ZONE_HIGH")
        if lo is not None and hi is not None:
            lo_f, hi_f = float(lo), float(hi)
            if lo_f > 0 and hi_f > 0:
                return (lo_f + hi_f) / 2.0
    except (TypeError, ValueError):
        pass
    return None


def _preflight_entry_bracket_hard_block_reason_codes(cur, action: dict) -> list[str]:
    """Recompute entry bracket viability (no DB writes) for overview/revalidate preflight."""
    intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
    if intent == "EXIT" or not is_structural_live_action(action):
        return []

    side = str(action.get("SIDE") or "").upper()
    portfolio_id = int(action.get("PORTFOLIO_ID") or 0)

    long_only_cfg = _read_app_config(cur, ["LIVE_ENFORCE_LONG_ONLY", "LIVE_BLOCK_ON_BROKER_SHORT"])
    enforce_long_only = _parse_bool_config(long_only_cfg.get("LIVE_ENFORCE_LONG_ONLY"), True)
    block_on_broker_short = _parse_bool_config(long_only_cfg.get("LIVE_BLOCK_ON_BROKER_SHORT"), True)
    if enforce_long_only and block_on_broker_short and side != "BUY":
        return ["ENTRY_SIDE_NOT_ALLOWED_LONG_ONLY"]

    cur.execute(
        """
        select IBKR_ACCOUNT_ID, ADAPTER_MODE, MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, BUST_PCT
        from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
        where PORTFOLIO_ID = %s
        """,
        (portfolio_id,),
    )
    cfg_rows = fetch_all(cur)
    live_cfg = cfg_rows[0] if cfg_rows else {}
    if not _live_execution_requires_ib_risk_gates(live_cfg):
        return []

    bust_pct_default = _normalize_bust_pct_cap(
        float(live_cfg.get("BUST_PCT")) if live_cfg.get("BUST_PCT") is not None else None
    )

    entry_price = _structural_ref_price_for_bracket(action)
    ps = _parse_variant(action.get("PARAM_SNAPSHOT"))
    eb = ps.get("executable_bracket") if isinstance(ps, dict) else None
    target_return = None
    stop_loss_pct = None
    if isinstance(eb, dict) and not eb.get("blocked"):
        try:
            if eb.get("target_return") is not None:
                target_return = float(eb["target_return"])
            if eb.get("stop_loss_pct") is not None:
                stop_loss_pct = float(eb["stop_loss_pct"])
        except (TypeError, ValueError):
            target_return = None
            stop_loss_pct = None
    if target_return is None or stop_loss_pct is None:
        jd = build_structural_entry_joint_decision(dict(action))
        target_return, stop_loss_pct = _live_target_and_stop_from_joint_decision(jd, bust_pct_default)

    fee_params = _load_live_entry_fee_params(cur)
    codes = _live_ib_entry_risk_reason_codes(
        side=side,
        is_exit=False,
        entry_price=entry_price,
        target_return=target_return,
        stop_loss_pct=stop_loss_pct,
        fee_params=fee_params,
    )

    proposed_qty = action.get("PROPOSED_QTY")
    if (
        entry_price is not None
        and float(entry_price) > 0
        and proposed_qty is not None
        and float(proposed_qty) > 0
        and target_return is not None
        and stop_loss_pct is not None
    ):
        tp_p, sl_p = _live_entry_tp_sl_prices(side, float(entry_price), target_return, stop_loss_pct)
        if tp_p is not None and sl_p is not None:
            realism_cfg = _load_bracket_realism_config(cur)
            bracket_codes = _live_bracket_realism_reason_codes(
                cur,
                live_cfg=live_cfg,
                side=side,
                entry_price=float(entry_price),
                qty=float(proposed_qty),
                tp_price=tp_p,
                sl_price=sl_p,
                target_return=target_return,
                fee_params=fee_params,
                preloaded_realism_cfg=realism_cfg,
            )
            codes = sorted(set(codes + bracket_codes))

    diag = ps.get("structural_diagnostics_v1") if isinstance(ps, dict) else {}
    contract_complete = True
    if isinstance(diag, dict):
        contract_complete = bool(diag.get("structural_contract_complete", True))
    tr_ok = target_return is not None and float(target_return) > 0
    sl_ok = stop_loss_pct is not None and float(stop_loss_pct) > 0
    eb_blocked = isinstance(eb, dict) and bool(eb.get("blocked"))
    if not contract_complete or not tr_ok or not sl_ok or eb_blocked:
        if "STRUCT_SUBMIT_CONTRACT_INCOMPLETE" not in codes:
            codes.append("STRUCT_SUBMIT_CONTRACT_INCOMPLETE")

    return sorted(set(codes))


def _load_bracket_realism_config(cur) -> dict:
    """APP_CONFIG for portfolio-relative bracket realism (Layer A + B)."""
    keys = [
        "LIVE_BRACKET_REALISM_ENABLED",
        "LIVE_BRACKET_REALISM_MODE",
        "LIVE_BRACKET_REALISM_APPLY_TO_PAPER",
        "LIVE_BRACKET_ABS_MIN_GROSS_TP_USD",
        "LIVE_BRACKET_ABS_MIN_GROSS_SL_USD",
        "LIVE_BRACKET_MIN_GROSS_TP_PCT_OF_NOTIONAL",
        "LIVE_BRACKET_MIN_NET_TP_PCT_OF_NOTIONAL",
        "LIVE_BRACKET_MIN_GROSS_SL_PCT_OF_NOTIONAL",
        "LIVE_BRACKET_MIN_GROSS_TP_BPS_OF_NAV",
        "LIVE_BRACKET_NAV_RULE_CAP_MULT",
        "LIVE_BRACKET_SMALL_POS_MAX_PCT_NAV",
        "LIVE_BRACKET_SMALL_POS_STRICT_MULT",
        "LIVE_MIN_BRACKET_WIDTH_BPS",
    ]
    raw = _read_app_config(cur, keys)

    def _f(key: str, default: float) -> float:
        try:
            v = raw.get(key)
            if v is None or str(v).strip() == "":
                return float(default)
            return float(v)
        except Exception:
            return float(default)

    enabled = _parse_bool_config(raw.get("LIVE_BRACKET_REALISM_ENABLED"), True)
    mode = str(raw.get("LIVE_BRACKET_REALISM_MODE") or os.getenv("LIVE_BRACKET_REALISM_MODE") or "BLOCK").upper()
    apply_paper = _parse_bool_config(raw.get("LIVE_BRACKET_REALISM_APPLY_TO_PAPER"), False)

    return {
        "enabled": enabled,
        "mode": mode if mode in ("OFF", "WARN", "BLOCK") else "BLOCK",
        "apply_to_paper": apply_paper,
        "abs_min_gross_tp_usd": _f("LIVE_BRACKET_ABS_MIN_GROSS_TP_USD", 1.0),
        "abs_min_gross_sl_usd": _f("LIVE_BRACKET_ABS_MIN_GROSS_SL_USD", 1.0),
        "min_gross_tp_pct_notional": _f("LIVE_BRACKET_MIN_GROSS_TP_PCT_OF_NOTIONAL", 0.015),
        "min_net_tp_pct_notional": _f("LIVE_BRACKET_MIN_NET_TP_PCT_OF_NOTIONAL", 0.01),
        "min_gross_sl_pct_notional": _f("LIVE_BRACKET_MIN_GROSS_SL_PCT_OF_NOTIONAL", 0.01),
        "min_gross_tp_bps_of_nav": _f("LIVE_BRACKET_MIN_GROSS_TP_BPS_OF_NAV", 12.0),
        "nav_rule_cap_mult": max(1.0, _f("LIVE_BRACKET_NAV_RULE_CAP_MULT", 5.0)),
        "small_pos_max_pct_nav": max(1e-9, _f("LIVE_BRACKET_SMALL_POS_MAX_PCT_NAV", 0.10)),
        "small_pos_strict_mult": max(1.0, _f("LIVE_BRACKET_SMALL_POS_STRICT_MULT", 1.2)),
        "min_bracket_width_bps": max(0.0, _f("LIVE_MIN_BRACKET_WIDTH_BPS", 25.0)),
    }


def _live_bracket_realism_codes_pure(
    *,
    nav_scale: float,
    side: str,
    entry_price: float,
    qty: float,
    tp_price: float,
    sl_price: float,
    target_return: float | None,
    fee_params: dict,
    rcfg: dict,
) -> list[str]:
    """
    Atomic bracket realism checks (no DB). Used by tests and live router.
    """
    side_u = str(side or "").upper()
    if side_u not in ("BUY", "SELL"):
        return []

    ep = float(entry_price)
    qv = float(qty)
    if ep <= 0 or qv <= 0:
        return []

    tp = float(tp_price)
    sl = float(sl_price)
    notional = abs(ep * qv)

    slippage_bps = float(fee_params.get("slippage_bps") or 2.0)
    fee_bps = float(fee_params.get("fee_bps") or 1.0)
    spread_bps = float(fee_params.get("spread_bps") or 0.0)
    fee_return_floor = (slippage_bps + fee_bps + (spread_bps / 2.0)) / 10000.0

    if side_u == "BUY":
        gross_tp_usd = max(0.0, (tp - ep) * qv)
        gross_sl_usd = max(0.0, (ep - sl) * qv)
        tp_move_bps = abs(tp / ep - 1.0) * 10000.0
        sl_move_bps = abs(ep - sl) / ep * 10000.0
    else:
        gross_tp_usd = max(0.0, (ep - tp) * qv)
        gross_sl_usd = max(0.0, (sl - ep) * qv)
        tp_move_bps = abs(ep - tp) / ep * 10000.0
        sl_move_bps = abs(sl / ep - 1.0) * 10000.0

    net_tp_usd = 0.0
    if target_return is not None:
        tr = float(target_return)
        net_tp_usd = max(0.0, (tr - fee_return_floor)) * notional

    nav = float(nav_scale)
    small_line = bool(nav > 0 and (notional / nav) < float(rcfg["small_pos_max_pct_nav"]))
    smult = float(rcfg["small_pos_strict_mult"]) if small_line else 1.0

    base_tp_pct = float(rcfg["min_gross_tp_pct_notional"])
    base_net_pct = float(rcfg["min_net_tp_pct_notional"])
    base_sl_pct = float(rcfg["min_gross_sl_pct_notional"])
    strict_tp_pct = base_tp_pct * smult
    strict_net_pct = base_net_pct * smult
    strict_sl_pct = base_sl_pct * smult

    abs_tp = float(rcfg["abs_min_gross_tp_usd"])
    abs_sl = float(rcfg["abs_min_gross_sl_usd"])
    bps_nav = float(rcfg["min_gross_tp_bps_of_nav"])
    cap_mult = float(rcfg["nav_rule_cap_mult"])
    nav_tp_floor = min(nav * (bps_nav / 10000.0), cap_mult * base_tp_pct * notional) if nav > 0 else 0.0

    req_net = strict_net_pct * notional
    min_width = float(rcfg["min_bracket_width_bps"])
    leg_min_bps = min(tp_move_bps, sl_move_bps)

    mode = str(rcfg.get("mode") or "BLOCK").upper()
    if mode == "WARN":
        return []

    codes: list[str] = []
    if gross_tp_usd < abs_tp:
        codes.append("LIVE_BRACKET_ABS_GROSS_TP_BELOW_MIN_USD")
    if gross_tp_usd < strict_tp_pct * notional:
        codes.append("LIVE_BRACKET_REL_GROSS_TP_BELOW_PCT_NOTIONAL")
    if nav > 0 and gross_tp_usd < nav_tp_floor:
        codes.append("LIVE_BRACKET_REL_GROSS_TP_BELOW_BPS_NAV")
    if net_tp_usd < req_net:
        codes.append("LIVE_BRACKET_REL_NET_TP_BELOW_PCT_NOTIONAL")
    if gross_sl_usd < abs_sl:
        codes.append("LIVE_BRACKET_ABS_GROSS_SL_BELOW_MIN_USD")
    if gross_sl_usd < strict_sl_pct * notional:
        codes.append("LIVE_BRACKET_REL_GROSS_SL_BELOW_PCT_NOTIONAL")
    if min_width > 0 and leg_min_bps < min_width:
        codes.append("LIVE_BRACKET_WIDTH_BELOW_MIN_BPS")

    return sorted(set(codes))


def _live_bracket_realism_reason_codes(
    cur,
    *,
    live_cfg: dict | None,
    side: str,
    entry_price: float | None,
    qty: float | None,
    tp_price: float | None,
    sl_price: float | None,
    target_return: float | None,
    fee_params: dict,
    preloaded_realism_cfg: dict | None = None,
) -> list[str]:
    """Layer A + B bracket realism; loads NAV from BROKER_SNAPSHOTS."""
    cfg_row = live_cfg or {}
    adapter_mode = str(cfg_row.get("ADAPTER_MODE") or "PAPER").upper()
    execution_mode = str(os.getenv("LIVE_EXECUTION_MODE", "AUTO")).upper()
    use_ibkr_submit = adapter_mode == "LIVE"
    if execution_mode == "IBKR":
        use_ibkr_submit = True
    elif execution_mode == "PLACEHOLDER":
        use_ibkr_submit = False

    rcfg = preloaded_realism_cfg if preloaded_realism_cfg is not None else _load_bracket_realism_config(cur)
    if not rcfg.get("enabled") or str(rcfg.get("mode") or "").upper() == "OFF":
        return []

    apply_paper = bool(rcfg.get("apply_to_paper"))
    if not use_ibkr_submit and not (apply_paper and adapter_mode == "PAPER"):
        return []

    ep = float(entry_price) if entry_price is not None else None
    qv = float(qty) if qty is not None else None
    if ep is None or qv is None or tp_price is None or sl_price is None:
        return []

    account_id = str((cfg_row or {}).get("IBKR_ACCOUNT_ID") or "").strip()
    nav_scale = 0.0
    if account_id:
        try:
            cur.execute(
                """
                select NET_LIQUIDATION_EUR
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'NAV'
                  and IBKR_ACCOUNT_ID = %s
                order by SNAPSHOT_TS desc
                limit 1
                """,
                (account_id,),
            )
            nav_rows = fetch_all(cur)
            if nav_rows and nav_rows[0].get("NET_LIQUIDATION_EUR") is not None:
                nav_scale = float(nav_rows[0].get("NET_LIQUIDATION_EUR") or 0.0)
        except Exception:
            nav_scale = 0.0

    return _live_bracket_realism_codes_pure(
        nav_scale=nav_scale,
        side=side,
        entry_price=ep,
        qty=qv,
        tp_price=float(tp_price),
        sl_price=float(sl_price),
        target_return=target_return,
        fee_params=fee_params,
        rcfg=rcfg,
    )


def _live_entry_tp_sl_prices(
    side: str,
    entry_price: float,
    target_return: float | None,
    stop_loss_pct: float | None,
) -> tuple[float | None, float | None]:
    """Bracket prices from return/stop; fsum avoids float slip vs notional-% gates at boundaries."""
    side_u = str(side or "").upper()
    ep = float(entry_price)
    tp_price = None
    sl_price = None
    if target_return is not None:
        tr = float(target_return)
        if side_u == "BUY":
            tp_price = math.fsum([ep, ep * tr])
        elif side_u == "SELL":
            tp_price = max(math.fsum([ep, -ep * tr]), 0.0001)
    if stop_loss_pct is not None:
        sl = float(stop_loss_pct)
        if side_u == "BUY":
            sl_price = max(math.fsum([ep, -ep * sl]), 0.0001)
        elif side_u == "SELL":
            sl_price = math.fsum([ep, ep * sl])
    return tp_price, sl_price


def _read_live_min_viable_uplift_settings(cur) -> dict:
    """APP_CONFIG overrides; env fallback. LIVE_MIN_ENTRY_NOTIONAL_EUR=0 disables notional floor only. Default 150 EUR."""
    keys = [
        "LIVE_MIN_ENTRY_NOTIONAL_EUR",
        "LIVE_MIN_VIABLE_UPLIFT_MAX_MULT",
        "LIVE_MIN_VIABLE_UPLIFT_ENABLED",
    ]
    raw = _read_app_config(cur, keys)
    enabled = _parse_bool_config(
        raw.get("LIVE_MIN_VIABLE_UPLIFT_ENABLED"),
        str(os.getenv("LIVE_MIN_VIABLE_UPLIFT_ENABLED", "true")).strip().lower() in ("1", "true", "yes", "on"),
    )
    min_notional = 0.0
    try:
        v = raw.get("LIVE_MIN_ENTRY_NOTIONAL_EUR") or os.getenv("LIVE_MIN_ENTRY_NOTIONAL_EUR") or "150"
        min_notional = float(v)
    except Exception:
        min_notional = 0.0
    max_mult = 10.0
    try:
        v = raw.get("LIVE_MIN_VIABLE_UPLIFT_MAX_MULT") or os.getenv("LIVE_MIN_VIABLE_UPLIFT_MAX_MULT") or "10"
        max_mult = float(v)
    except Exception:
        max_mult = 10.0
    if max_mult < 1.0:
        max_mult = 1.0
    return {"enabled": enabled, "min_notional_eur": max(0.0, min_notional), "max_uplift_mult": max_mult}


def _compute_min_viable_qty_from_notional_floor(
    *,
    proposed_price: float,
    committee_qty: int,
    min_notional_eur: float,
) -> int:
    """Whole-share quantity at least committee_qty and at least ceil(min_notional / price) when floor > 0."""
    px = max(float(proposed_price), 1e-9)
    q0 = max(1, int(committee_qty))
    if min_notional_eur <= 0:
        return q0
    from_floor = int(math.ceil(min_notional_eur / px))
    return max(q0, from_floor)


def _append_min_viable_uplift_param_snapshot(cur, action_id: str, uplift_meta: dict) -> None:
    action = _fetch_live_action(cur, action_id)
    if not action:
        return
    ps = _parse_variant(action.get("PARAM_SNAPSHOT"))
    if not isinstance(ps, dict):
        ps = {}
    ps["min_viable_live_uplift"] = uplift_meta
    cur.execute(
        """
        update MIP.LIVE.LIVE_ACTIONS
           set PARAM_SNAPSHOT = parse_json(%s),
               UPDATED_AT = current_timestamp()
         where ACTION_ID = %s
        """,
        (json.dumps(ps), action_id),
    )


def _read_live_bracket_calibration_settings(cur) -> dict:
    keys = [
        "LIVE_BRACKET_CALIB_ENABLED",
        "LIVE_BRACKET_CALIB_MAX_TP_MULT",
        "LIVE_BRACKET_CALIB_MAX_SL_MULT",
        "LIVE_BRACKET_CALIB_MAX_TP_ABS_ADD",
    ]
    raw = _read_app_config(cur, keys)
    enabled = _parse_bool_config(
        raw.get("LIVE_BRACKET_CALIB_ENABLED"),
        str(os.getenv("LIVE_BRACKET_CALIB_ENABLED", "true")).strip().lower() in ("1", "true", "yes", "on"),
    )
    max_tp_mult = 2.5
    try:
        v = raw.get("LIVE_BRACKET_CALIB_MAX_TP_MULT") or os.getenv("LIVE_BRACKET_CALIB_MAX_TP_MULT")
        if v is not None and str(v).strip() != "":
            max_tp_mult = float(v)
    except Exception:
        pass
    max_sl_mult = 2.0
    try:
        v = raw.get("LIVE_BRACKET_CALIB_MAX_SL_MULT") or os.getenv("LIVE_BRACKET_CALIB_MAX_SL_MULT")
        if v is not None and str(v).strip() != "":
            max_sl_mult = float(v)
    except Exception:
        pass
    max_tp_abs_add = 0.03
    try:
        v = raw.get("LIVE_BRACKET_CALIB_MAX_TP_ABS_ADD") or os.getenv("LIVE_BRACKET_CALIB_MAX_TP_ABS_ADD")
        if v is not None and str(v).strip() != "":
            max_tp_abs_add = float(v)
    except Exception:
        pass
    return {
        "enabled": enabled,
        "max_tp_mult": max(1.0, max_tp_mult),
        "max_sl_mult": max(1.0, max_sl_mult),
        "max_tp_abs_add": max(0.0, max_tp_abs_add),
    }


def _committee_bracket_baseline_snapshot(joint_decision: dict | None) -> dict:
    jd = joint_decision if isinstance(joint_decision, dict) else {}
    return {
        "realistic_target_return": jd.get("realistic_target_return"),
        "acceptable_early_exit_target_return": jd.get("acceptable_early_exit_target_return"),
        "stop_loss_pct": jd.get("stop_loss_pct"),
    }


def _seed_executable_bracket_from_joint_decision(
    cur,
    action_id: str,
    joint_decision: dict | None,
    *,
    bust_pct_default: float | None = None,
    meta: dict | None = None,
) -> bool:
    """Persist TP/SL on PARAM_SNAPSHOT from structural joint_decision.

    Trailing-stop entries still require executable TP/SL percentages for IB
    bracket construction and LPA submit gating. Qty sizing may happen later.
    """
    target_return, stop_loss_pct = _live_target_and_stop_from_joint_decision(
        joint_decision, bust_pct_default
    )
    if (
        target_return is None
        or stop_loss_pct is None
        or float(target_return) <= 0
        or float(stop_loss_pct) <= 0
    ):
        return False
    seed_meta = {"seed": "structural_joint_decision", **(meta or {})}
    _merge_live_action_param_snapshot_patch(
        cur,
        action_id,
        {
            "committee_bracket_baseline": _committee_bracket_baseline_snapshot(
                joint_decision if isinstance(joint_decision, dict) else None
            ),
            "executable_bracket": {
                "target_return": float(target_return),
                "stop_loss_pct": float(stop_loss_pct),
                "calibrated": False,
                "blocked": False,
                "meta": seed_meta,
            },
        },
    )
    return True


def _merge_live_action_param_snapshot_patch(cur, action_id: str, patch: dict) -> None:
    action = _fetch_live_action(cur, action_id)
    if not action:
        return
    ps = _parse_variant(action.get("PARAM_SNAPSHOT"))
    if not isinstance(ps, dict):
        ps = {}
    for k, v in patch.items():
        ps[k] = v
    # Use the Decimal/datetime-safe dumper because `patch` (and the existing
    # PARAM_SNAPSHOT shaped contract) transitively embed values pulled from
    # LIVE_ACTIONS via fetch_all(), which returns Snowflake numeric columns
    # as decimal.Decimal. Plain json.dumps would crash with
    # "Object of type Decimal is not JSON serializable" the moment any
    # field like TRAIL_ACTIVATION_PARAM, MAX_HOLD_BARS, or SETUP_EVENT_ID
    # is non-NULL on the action row — which is the regression that broke
    # Committee 2.0 orchestrate for newly-imported structural actions.
    cur.execute(
        """
        update MIP.LIVE.LIVE_ACTIONS
           set PARAM_SNAPSHOT = parse_json(%s),
               UPDATED_AT = current_timestamp()
         where ACTION_ID = %s
        """,
        (_json_dumps_safe(ps), action_id),
    )


def _merge_structural_contract_and_diagnostics(
    cur,
    *,
    action_id: str,
    committee_run_id: str,
    verdict: dict,
    reason_codes: list[str],
    self_heal: dict | None = None,
) -> None:
    """Persist canonical structural_execution_contract_v1 + diagnostics onto PARAM_SNAPSHOT."""
    action_row = _fetch_live_action(cur, action_id)
    if not action_row or not is_structural_live_action(action_row):
        return
    ps = _parse_variant(action_row.get("PARAM_SNAPSHOT"))
    eb = ps.get("executable_bracket") if isinstance(ps, dict) else None
    jd = _parse_variant(verdict.get("joint_decision"))
    intent = _normalize_action_intent(action_row.get("SIDE"), action_row.get("ACTION_INTENT"))
    entry_like = intent != "EXIT"
    tr_ok = sl_ok = False
    if isinstance(jd, dict):
        try:
            tr = float(jd.get("realistic_target_return")) if jd.get("realistic_target_return") is not None else None
            sl = float(jd.get("stop_loss_pct")) if jd.get("stop_loss_pct") is not None else None
            tr_ok = tr is not None and tr > 0
            sl_ok = sl is not None and sl > 0
        except (TypeError, ValueError):
            tr_ok = sl_ok = False
    has_blocked_eb = isinstance(eb, dict) and bool(eb.get("blocked"))
    eb_has_legs = (
        isinstance(eb, dict)
        and eb.get("target_return") is not None
        and eb.get("stop_loss_pct") is not None
        and not eb.get("blocked")
    )
    contract_complete = (not entry_like) or (tr_ok and sl_ok and not has_blocked_eb and eb_has_legs)
    if entry_like and tr_ok and sl_ok and eb_has_legs and not has_blocked_eb:
        contract_complete = True
    elif entry_like and has_blocked_eb:
        contract_complete = False
    diagnostics = {
        "routed_structural": True,
        "structural_committee_run_id": committee_run_id,
        "structural_contract_present": True,
        "structural_contract_complete": contract_complete,
        "legacy_path_used": False,
        "committee_logic_version": STRUCTURAL_COMMITTEE_LOGIC_VERSION,
        "reason_codes": list(reason_codes),
    }
    if self_heal:
        diagnostics["structural_self_heal"] = self_heal
    contract = build_structural_execution_contract_v1(
        action=action_row,
        verdict=verdict,
        joint_decision=jd,
        committee_run_id=committee_run_id,
        param_snapshot_executable_bracket=eb if isinstance(eb, dict) else None,
        diagnostics=diagnostics,
    )
    _merge_live_action_param_snapshot_patch(
        cur,
        action_id,
        {
            "structural_execution_contract_v1": contract,
            "structural_diagnostics_v1": diagnostics,
            "structural_source": True,
        },
    )


def _apply_post_committee_entry_viability_and_qty(
    cur,
    *,
    action_id: str,
    portfolio_id: int,
    side: str,
    is_exit: bool,
    is_committee_blocked: bool,
    proposed_price: float | None,
    committee_qty: float | None,
    joint_decision: dict | None,
    reason_codes: list[str],
) -> tuple[float | None, list[str]]:
    """
    After committee sizing: calibrate TP/SL for live bracket realism (bounded), persist executable
    bracket on PARAM_SNAPSHOT, run IB risk checks; optionally uplift qty for min notional.
    """
    if is_exit or is_committee_blocked:
        return committee_qty, reason_codes

    cur.execute(
        """
        select
          IBKR_ACCOUNT_ID, ADAPTER_MODE, MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, BUST_PCT
        from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
        where PORTFOLIO_ID = %s
        """,
        (portfolio_id,),
    )
    cfg_rows = fetch_all(cur)
    live_cfg = cfg_rows[0] if cfg_rows else {}
    bust_pct_default = _normalize_bust_pct_cap(
        float(live_cfg.get("BUST_PCT")) if live_cfg.get("BUST_PCT") is not None else None
    )
    if not _live_execution_requires_ib_risk_gates(live_cfg):
        _seed_executable_bracket_from_joint_decision(
            cur, action_id, joint_decision, bust_pct_default=bust_pct_default,
        )
        return committee_qty, reason_codes

    side_u = str(side or "").upper()

    orig_target_return, orig_stop_loss_pct = _live_target_and_stop_from_joint_decision(
        joint_decision, bust_pct_default
    )
    target_return, stop_loss_pct = orig_target_return, orig_stop_loss_pct

    fee_params = _load_live_entry_fee_params(cur)
    realism_cfg = _load_bracket_realism_config(cur)
    calib_cfg = _read_live_bracket_calibration_settings(cur)

    account_id = str(live_cfg.get("IBKR_ACCOUNT_ID") or "").strip()
    nav_eur = 0.0
    if account_id:
        cur.execute(
            """
            select NET_LIQUIDATION_EUR
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'NAV'
              and IBKR_ACCOUNT_ID = %s
            order by SNAPSHOT_TS desc
            limit 1
            """,
            (account_id,),
        )
        nav_rows = fetch_all(cur)
        nav_eur = float((nav_rows[0] or {}).get("NET_LIQUIDATION_EUR") or 0.0) if nav_rows else 0.0

    baseline_snapshot = {
        "committee_bracket_baseline": _committee_bracket_baseline_snapshot(
            joint_decision if isinstance(joint_decision, dict) else None
        ),
    }
    calib_any = False
    calib_meta_accum: dict = {}

    def _patch_executable_bracket(
        tr_v: float | None,
        sl_v: float | None,
        *,
        calibrated: bool,
        meta: dict,
        blocked: bool,
        block_codes: list[str] | None,
    ) -> None:
        if blocked:
            eb = {
                "blocked": True,
                "reason_codes": list(block_codes or []),
                "meta": meta,
                "blocked_bracket_class": (meta or {}).get("blocked_bracket_class"),
            }
            _merge_live_action_param_snapshot_patch(cur, action_id, {**baseline_snapshot, "executable_bracket": eb})
        elif tr_v is not None and sl_v is not None:
            eb = {
                "target_return": float(tr_v),
                "stop_loss_pct": float(sl_v),
                "calibrated": bool(calibrated),
                "blocked": False,
                "meta": meta,
            }
            _merge_live_action_param_snapshot_patch(cur, action_id, {**baseline_snapshot, "executable_bracket": eb})
        else:
            _merge_live_action_param_snapshot_patch(cur, action_id, baseline_snapshot)

    can_calibrate = (
        proposed_price is not None
        and float(proposed_price) > 0
        and committee_qty is not None
        and orig_target_return is not None
        and orig_stop_loss_pct is not None
        and float(orig_target_return) > 0
        and float(orig_stop_loss_pct) > 0
    )

    if can_calibrate:
        committee_int0 = max(1, int(round(float(committee_qty))))
        cres = _calibrate_live_entry_bracket_to_min_viable(
            side=side_u,
            entry_price=float(proposed_price),
            qty=float(committee_int0),
            nav_scale=nav_eur,
            baseline_target_return=float(orig_target_return),
            baseline_stop_loss_pct=float(orig_stop_loss_pct),
            bust_pct=bust_pct_default,
            fee_params=fee_params,
            rcfg=realism_cfg,
            calib_cfg=calib_cfg,
        )
        if not cres.ok:
            rc = _merge_unique_reason_codes(reason_codes, cres.reason_codes)
            lb_raw = cres.meta.get("last_bracket") or []
            lb_list = [str(x) for x in lb_raw] if isinstance(lb_raw, list) else []
            bclass, bdetail = classify_blocked_bracket_for_diagnostics(
                side=side_u,
                entry_price=float(proposed_price),
                qty=float(committee_int0),
                nav_scale=nav_eur,
                baseline_target_return=float(orig_target_return),
                baseline_stop_loss_pct=float(orig_stop_loss_pct),
                fee_params=fee_params,
                rcfg=realism_cfg,
                last_bracket=lb_list,
                calibration_reason_codes=list(cres.reason_codes),
            )
            blocked_meta = {
                **cres.meta,
                "blocked_bracket_class": bclass,
                "blocked_bracket_detail": bdetail,
            }
            _patch_executable_bracket(
                None, None, calibrated=False, meta=blocked_meta, blocked=True, block_codes=cres.reason_codes
            )
            return committee_qty, rc
        target_return = float(cres.target_return)  # type: ignore[assignment]
        stop_loss_pct = float(cres.stop_loss_pct)  # type: ignore[assignment]
        calib_any = bool(cres.calibrated)
        calib_meta_accum = dict(cres.meta)
        _patch_executable_bracket(
            target_return,
            stop_loss_pct,
            calibrated=calib_any,
            meta=calib_meta_accum,
            blocked=False,
            block_codes=None,
        )
    else:
        _merge_live_action_param_snapshot_patch(cur, action_id, baseline_snapshot)
        _seed_executable_bracket_from_joint_decision(
            cur,
            action_id,
            joint_decision,
            bust_pct_default=bust_pct_default,
            meta={"without_qty": committee_qty is None},
        )

    risk_codes = _live_ib_entry_risk_reason_codes(
        side=side_u,
        is_exit=False,
        entry_price=proposed_price,
        target_return=target_return,
        stop_loss_pct=stop_loss_pct,
        fee_params=fee_params,
    )
    if risk_codes:
        return committee_qty, _merge_unique_reason_codes(reason_codes, risk_codes)

    def _bracket_codes_for_qty(qty_val: float) -> list[str]:
        if proposed_price is None or float(proposed_price) <= 0:
            return []
        if target_return is None or stop_loss_pct is None:
            return []
        tp_p, sl_p = _live_entry_tp_sl_prices(side_u, float(proposed_price), target_return, stop_loss_pct)
        if tp_p is None or sl_p is None:
            return []
        return _live_bracket_realism_reason_codes(
            cur,
            live_cfg=live_cfg,
            side=side_u,
            entry_price=float(proposed_price),
            qty=qty_val,
            tp_price=tp_p,
            sl_price=sl_p,
            target_return=target_return,
            fee_params=fee_params,
            preloaded_realism_cfg=realism_cfg,
        )

    uplift_cfg = _read_live_min_viable_uplift_settings(cur)
    if (
        not uplift_cfg["enabled"]
        or proposed_price is None
        or committee_qty is None
        or float(proposed_price) <= 0
    ):
        if committee_qty is not None:
            q0 = max(1, int(round(float(committee_qty))))
            bc0 = _bracket_codes_for_qty(float(q0))
            if bc0:
                return committee_qty, _merge_unique_reason_codes(reason_codes, bc0)
        return committee_qty, reason_codes

    committee_int = max(1, int(round(float(committee_qty))))
    px = float(proposed_price)
    wanted_qty = _compute_min_viable_qty_from_notional_floor(
        proposed_price=px,
        committee_qty=committee_int,
        min_notional_eur=float(uplift_cfg["min_notional_eur"]),
    )
    max_allowed = int(math.ceil(float(committee_int) * float(uplift_cfg["max_uplift_mult"])))
    max_allowed = max(max_allowed, committee_int)

    if wanted_qty <= committee_int:
        bc = _bracket_codes_for_qty(float(committee_int))
        if bc:
            return float(committee_int), _merge_unique_reason_codes(reason_codes, bc)
        return float(committee_int), reason_codes

    if wanted_qty > max_allowed:
        rc = _merge_unique_reason_codes(reason_codes, ["LIVE_MIN_VIABLE_SIZE_NOT_REACHED"])
        bc = _bracket_codes_for_qty(float(committee_int))
        if bc:
            rc = _merge_unique_reason_codes(rc, bc)
        return float(committee_int), rc

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
    nav_eur2 = float((nav_rows[0] or {}).get("NET_LIQUIDATION_EUR") or 0.0) if nav_rows else 0.0
    cash_eur = float((nav_rows[0] or {}).get("TOTAL_CASH_EUR") or 0.0) if nav_rows else 0.0

    est_notional = float(wanted_qty) * px
    max_position_pct = live_cfg.get("MAX_POSITION_PCT")
    if nav_eur2 > 0 and max_position_pct is not None:
        if (est_notional / nav_eur2) > float(max_position_pct):
            rc = _merge_unique_reason_codes(reason_codes, ["LIVE_MIN_VIABLE_SIZE_NOT_REACHED"])
            bc = _bracket_codes_for_qty(float(committee_int))
            if bc:
                rc = _merge_unique_reason_codes(rc, bc)
            return float(committee_int), rc

    if side_u == "BUY" and nav_eur2 > 0:
        cash_buffer_pct = float(live_cfg.get("CASH_BUFFER_PCT") or 0.0)
        min_cash_after = nav_eur2 * cash_buffer_pct
        if (cash_eur - est_notional) < min_cash_after:
            rc = _merge_unique_reason_codes(reason_codes, ["LIVE_MIN_VIABLE_SIZE_NOT_REACHED"])
            bc = _bracket_codes_for_qty(float(committee_int))
            if bc:
                rc = _merge_unique_reason_codes(rc, bc)
            return float(committee_int), rc

    bc_up = _bracket_codes_for_qty(float(wanted_qty))
    if bc_up:
        if can_calibrate:
            cres2 = _calibrate_live_entry_bracket_to_min_viable(
                side=side_u,
                entry_price=px,
                qty=float(wanted_qty),
                nav_scale=nav_eur,
                baseline_target_return=float(orig_target_return),  # type: ignore[arg-type]
                baseline_stop_loss_pct=float(orig_stop_loss_pct),  # type: ignore[arg-type]
                bust_pct=bust_pct_default,
                fee_params=fee_params,
                rcfg=realism_cfg,
                calib_cfg=calib_cfg,
            )
            if cres2.ok:
                target_return = float(cres2.target_return)  # type: ignore[assignment]
                stop_loss_pct = float(cres2.stop_loss_pct)  # type: ignore[assignment]
                calib_any = calib_any or bool(cres2.calibrated)
                calib_meta_accum = {**calib_meta_accum, "uplift_recalibrate": cres2.meta}
                _patch_executable_bracket(
                    target_return,
                    stop_loss_pct,
                    calibrated=calib_any,
                    meta=calib_meta_accum,
                    blocked=False,
                    block_codes=None,
                )
                bc_up = _bracket_codes_for_qty(float(wanted_qty))
        if bc_up:
            return float(committee_int), _merge_unique_reason_codes(reason_codes, bc_up)

    rc2 = _merge_unique_reason_codes(reason_codes, ["LIVE_QTY_UPLIFTED_TO_MIN_VIABLE"])
    uplift_meta = {
        "committee_qty": committee_int,
        "uplifted_qty": wanted_qty,
        "reference_price": px,
        "min_notional_eur_config": float(uplift_cfg["min_notional_eur"]),
        "max_uplift_mult": float(uplift_cfg["max_uplift_mult"]),
    }
    _append_min_viable_uplift_param_snapshot(cur, action_id, uplift_meta)
    return float(wanted_qty), rc2


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
        pid = int(portfolio_id) if portfolio_id is not None else None
        refresh_result = _force_refresh_latest_one_minute_bars(
            cur, action.get("SYMBOL"), portfolio_id=pid,
        )
        account_id = cfg.get("IBKR_ACCOUNT_ID")
        if account_id:
            try:
                snapshot_payload = _run_on_demand_snapshot_sync(
                    **_snapshot_sync_params_for_portfolio(pid),
                    account=str(account_id),
                    portfolio_id=pid,
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
            # MIP.MART.MARKET_BARS.TS marks the START of the 1-minute bar, so
            # a freshly-landed bar is already 60s old the instant it commits.
            # Treat (TS + INTERVAL) as the "as of" instant when measuring age,
            # otherwise a 60s freshness threshold can never pass for any 1m
            # bar even when the IBKR ingest just ran.
            bar_age_sec = max(
                0.0,
                (now_utc - bar_ts_utc).total_seconds() - 60.0,
            )

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
    market_extended_open = _is_extended_trading_open_ny(now_utc)
    if not market_extended_open:
        hard_reasons.append("OPEN_MARKET_CLOSED")
    if not symbol:
        hard_reasons.append("OPEN_MISSING_SYMBOL")
    if bar_ts is None:
        hard_reasons.append("OPEN_SNAPSHOT_MISSING")
    effective_snapshot_max_age_sec = float(policy["snapshot_max_age_sec"])
    if market_extended_open and not _is_market_open_ny(now_utc):
        # Extended-hours prints can be sparse; keep a wider tolerance than regular session.
        effective_snapshot_max_age_sec = max(effective_snapshot_max_age_sec, 1800.0)
    if bar_ts is not None and bar_age_sec is not None and bar_age_sec > effective_snapshot_max_age_sec:
        # When the market is closed, OPEN_MARKET_CLOSED already explains why
        # the snapshot is necessarily old — duplicating with OPEN_SNAPSHOT_STALE
        # produces confusing "two redundant red errors" UX without adding any
        # gating signal. Demote to caution in that case so operators reviewing
        # actions during off-hours don't get a redundant hard block stack.
        if market_extended_open:
            hard_reasons.append("OPEN_SNAPSHOT_STALE")
        else:
            caution_reasons.append("OPEN_SNAPSHOT_STALE")
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
    try:
        from app.integrations.ibkr_read_host import get_snapshot_sync_params

        return get_snapshot_sync_params()
    except (ImportError, ModuleNotFoundError):
        return {
            "host": os.getenv("IBKR_SNAPSHOT_HOST", os.getenv("IBKR_EXEC_HOST", "127.0.0.1")),
            "port": int(os.getenv("IBKR_SNAPSHOT_PORT", os.getenv("IBKR_EXEC_PORT", "7497"))),
            "client_id": int(os.getenv("IBKR_SNAPSHOT_CLIENT_ID", "9402")),
        }


def _snapshot_sync_params_for_portfolio(portfolio_id: int | None) -> dict:
    """Resolve IB Gateway connection params for a specific portfolio.

    When portfolio_id is given, looks up IB_GATEWAY_HOST, IB_GATEWAY_PORT,
    IB_CLIENT_ID from LIVE_PORTFOLIO_CONFIG and uses non-null values.
    Falls back to env-var defaults for any column that is NULL, or when
    portfolio_id is None (preserving backward-compat for the paper portfolio
    which stores all three as NULL).
    """
    defaults = _default_snapshot_sync_params()
    if portfolio_id is None:
        return defaults
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select IB_GATEWAY_HOST, IB_GATEWAY_PORT, IB_CLIENT_ID
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            """,
            (portfolio_id,),
        )
        rows = fetch_all(cur)
    finally:
        conn.close()
    if not rows:
        return defaults
    row = rows[0]
    return {
        "host":      str(row["IB_GATEWAY_HOST"]) if row.get("IB_GATEWAY_HOST") else defaults["host"],
        "port":      int(row["IB_GATEWAY_PORT"]) if row.get("IB_GATEWAY_PORT") is not None else defaults["port"],
        "client_id": int(row["IB_CLIENT_ID"])    if row.get("IB_CLIENT_ID")    is not None else defaults["client_id"],
    }


def _probe_ibkr_session(
    host: str,
    port: int,
    client_id: int,
    expected_account: str | None,
    connect_timeout_sec: int = 8,
) -> dict:
    """
    Probe the IBKR Gateway/TWS session and return a structured compatibility result.

    Runs cursorfiles/probe_ibkr_session.py as a subprocess (because ib_insync lives
    in the cursorfiles venv, not the API venv). Strictly read-only: readonly=True,
    managedAccounts() only, no snapshot writes, no orders.

    Returns a dict with keys:
        connected         bool
        detected_accounts list[str]
        account_match     bool
        status            MATCH | ACCOUNT_MISMATCH | MULTIPLE_ACCOUNTS |
                          NOT_CONNECTED | PROBE_ERROR | CONFIG_NOT_FOUND
        message           str
    """
    root   = _project_root()
    py     = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "probe_ibkr_session.py"

    if not py.exists() or not script.exists():
        return {
            "connected": False,
            "detected_accounts": [],
            "account_match": False,
            "status": "PROBE_ERROR",
            "message": "probe_ibkr_session.py or cursorfiles venv not found.",
        }

    cmd = [
        str(py), str(script),
        "--host",      host,
        "--port",      str(port),
        "--client-id", str(client_id),
        "--timeout",   str(connect_timeout_sec),
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=connect_timeout_sec + 5,
            cwd=str(root),
        )
    except subprocess.TimeoutExpired:
        return {
            "connected": False,
            "detected_accounts": [],
            "account_match": False,
            "status": "NOT_CONNECTED",
            "message": f"IBKR probe timed out connecting to {host}:{port}.",
        }
    except Exception as exc:
        return {
            "connected": False,
            "detected_accounts": [],
            "account_match": False,
            "status": "PROBE_ERROR",
            "message": str(exc),
        }

    out = (proc.stdout or "").strip()
    payload: dict = {}
    try:
        idx = out.rfind("{")
        if idx >= 0:
            payload = json.loads(out[idx:])
    except Exception:
        payload = {}

    if not payload.get("connected"):
        return {
            "connected": False,
            "detected_accounts": [],
            "account_match": False,
            "status": "NOT_CONNECTED",
            "message": payload.get("error") or f"IBKR session on {host}:{port} is not reachable.",
        }

    detected: list[str] = list(payload.get("detected_accounts") or [])

    if not expected_account:
        return {
            "connected": True,
            "detected_accounts": detected,
            "account_match": True,
            "status": "MATCH",
            "message": "Connected (no expected account configured).",
        }

    if expected_account in detected:
        if len(detected) > 1:
            return {
                "connected": True,
                "detected_accounts": detected,
                "account_match": True,
                "status": "MULTIPLE_ACCOUNTS",
                "message": (
                    f"Expected account {expected_account} is present. "
                    f"Session also exposes: {[a for a in detected if a != expected_account]}."
                ),
            }
        return {
            "connected": True,
            "detected_accounts": detected,
            "account_match": True,
            "status": "MATCH",
            "message": f"Session matches expected account {expected_account}.",
        }

    return {
        "connected": True,
        "detected_accounts": detected,
        "account_match": False,
        "status": "ACCOUNT_MISMATCH",
        "message": (
            f"Connected IBKR session exposes {detected} "
            f"but portfolio expects {expected_account}. "
            "Start the matching TWS/Gateway session or select the matching portfolio."
        ),
    }


def _live_portfolio_ib_host_diagnostics(
    *,
    snapshot_state: str,
    latest_snapshot_ts=None,
    threshold_sec=None,
) -> dict:
    from app.integrations.ibkr_read_host import diagnostics_live_portfolio_overview

    return diagnostics_live_portfolio_overview(
        snapshot_state=snapshot_state,
        latest_snapshot_ts=latest_snapshot_ts,
        threshold_sec=threshold_sec,
    )


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
    direction: str | None = None,
    trail_amount: float | None = None,
    trail_percent: float | None = None,
    oca_group: str | None = None,
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
    port = int(os.getenv("IBKR_EXEC_PORT", "7497"))
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
    if direction:
        cmd.extend(["--direction", str(direction).upper()])
    if trail_amount is not None:
        cmd.extend(["--trail-amount", str(float(trail_amount))])
    if trail_percent is not None:
        cmd.extend(["--trail-percent", str(float(trail_percent))])
    if oca_group:
        cmd.extend(["--oca-group", str(oca_group)])

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
    port = int(os.getenv("IBKR_EXEC_PORT", "7497"))
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


LIVE_BARS_IB_CLIENT_ID = 9436


def _ibkr_connect_params_for_portfolio(portfolio_id: int | None) -> dict | None:
    """Host/port from LIVE_PORTFOLIO_CONFIG; dedicated client id for 1m bar reads."""
    if portfolio_id is None:
        return None
    snap = _snapshot_sync_params_for_portfolio(portfolio_id)
    return {
        "host": snap["host"],
        "port": snap["port"],
        "client_id": LIVE_BARS_IB_CLIENT_ID,
    }


def _run_agent_ibkr_bar_refresh(
    symbol: str | None,
    timeout_sec: int = 120,
    *,
    portfolio_id: int | None = None,
) -> dict:
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
    connect = _ibkr_connect_params_for_portfolio(portfolio_id)
    if connect:
        cmd.extend([
            "--host", str(connect["host"]),
            "--port", str(connect["port"]),
            "--client-id", str(connect["client_id"]),
        ])
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


def _run_agent_ibkr_bar_ingest(
    symbol: str | None,
    timeout_sec: int = 180,
    *,
    portfolio_id: int | None = None,
) -> dict:
    """
    Ingest latest IBKR 1-minute bars for `symbol` into MIP.MART.MARKET_BARS via
    `cursorfiles/ingest_ibkr_bars.py`. Unlike `_run_agent_ibkr_bar_refresh`
    (read-only) this one *persists* — required so downstream consumers that
    only read MARKET_BARS (committee `_live_context`, opening sanity gate)
    actually see fresh ticks instead of yesterday's daily close.
    """
    if not symbol:
        return {"attempted": False, "status": "SKIPPED", "reason": "MISSING_SYMBOL"}

    root = _project_root()
    py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "ingest_ibkr_bars.py"
    if not py.exists() or not script.exists():
        return {
            "attempted": False,
            "status": "SKIPPED",
            "reason": "IBKR_INGEST_RUNTIME_NOT_FOUND",
        }

    cmd = [
        str(py),
        str(script),
        "--symbols",
        str(symbol).upper(),
        "--interval-minutes",
        "1",
        "--duration-str",
        "1 D",
    ]
    connect = _ibkr_connect_params_for_portfolio(portfolio_id)
    if connect:
        cmd.extend([
            "--host", str(connect["host"]),
            "--port", str(connect["port"]),
            "--client-id", str(connect["client_id"]),
        ])
    child_env = dict(os.environ)
    for key in list(child_env.keys()):
        if key.startswith("SNOWFLAKE_"):
            child_env.pop(key, None)

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(root),
            env=child_env,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "attempted": True,
            "status": "FAIL",
            "executor": "agent_runtime_ibkr_ingest",
            "error": f"timeout after {timeout_sec}s: {exc}",
        }

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    payload: dict = {}
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
            "executor": "agent_runtime_ibkr_ingest",
            "payload": payload or None,
            "stderr": stderr[-2000:],
            "stdout": stdout[-2000:],
        }

    return {
        "attempted": True,
        "status": "SUCCESS",
        "executor": "agent_runtime_ibkr_ingest",
        "payload": payload,
    }


def _force_refresh_latest_one_minute_bars(
    cur,
    symbol: str | None = None,
    *,
    portfolio_id: int | None = None,
) -> dict:
    """
    Direct IBKR 1-minute refresh used before revalidation/committee re-runs.

    Returns the read-only fetch payload (consumed directly by
    `_extract_latest_one_min_bar_from_refresh` for revalidation's
    IBKR_DIRECT_1M path) AND triggers a persisting ingest into
    MIP.MART.MARKET_BARS so downstream consumers (committee `_live_context`,
    opening sanity gate, mart-only readers) see the fresh tick. The persisted
    ingest result is returned under `mart_ingest`; failures there don't fail
    the refresh — the direct payload is still authoritative for the caller.
    """
    refresh = _run_agent_ibkr_bar_refresh(symbol, portfolio_id=portfolio_id)
    if symbol and str(refresh.get("status") or "").upper() == "SUCCESS":
        try:
            ingest = _run_agent_ibkr_bar_ingest(symbol, portfolio_id=portfolio_id)
        except Exception as exc:
            ingest = {
                "attempted": True,
                "status": "FAIL",
                "executor": "agent_runtime_ibkr_ingest",
                "error": str(exc),
            }
        if isinstance(refresh, dict):
            refresh = dict(refresh)
            refresh["mart_ingest"] = ingest
    return refresh


def _fetch_ibkr_mart_reference_close(cur, symbol: str | None) -> float | None:
    """
    Latest IBKR close from MIP.MART.MARKET_BARS using the same mart path as
    revalidate_live_action (IBKR 1m, then IBKR 15/60/1440 fallback).
    Keeps PROPOSED_PRICE from committee apply aligned with revalidation reference.
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    try:
        # Pick the FRESHEST IBKR bar by timestamp across intervals rather than
        # blindly taking the latest 1m bar. A symbol whose intraday (1m) feed has
        # gone stale (e.g. only refreshed while actively traded) would otherwise
        # return a months-old 1m close, corrupting the entry reference and the
        # bracket R/R math. Ordering by TS first guarantees a current daily/15/60
        # bar beats a stale 1m; the interval tiebreak still prefers 1m precision
        # when multiple intervals share the most recent timestamp.
        cur.execute(
            """
            select CLOSE
            from MIP.MART.MARKET_BARS
            where SYMBOL = %s
              and INTERVAL_MINUTES in (1, 15, 60, 1440)
              and upper(coalesce(SOURCE, '')) = 'IBKR'
            order by TS desc,
                     case INTERVAL_MINUTES
                       when 1 then 0 when 15 then 1 when 60 then 2 else 3
                     end
            limit 1
            """,
            (sym,),
        )
        bar = cur.fetchone()
        if bar and bar[0] is not None:
            return float(bar[0])
    except Exception:
        return None
    return None


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
    return to_dt_utc(v)


def _coerce_flat_book_nav_eur(net_liq, total_cash, gross_exposure):
    """When gross exposure is ~flat, NetLiquidation can be USD/BASE while TotalCashValue is EUR.

    Snapshots then store a spurious NAV (e.g. 4115) next to correct EUR cash (3000).
    For display and trend math, trust TotalCashValue as NAV on a flat book.
    """
    try:
        cash_f = float(total_cash) if total_cash is not None else None
    except (TypeError, ValueError):
        cash_f = None
    try:
        nav_f = float(net_liq) if net_liq is not None else None
    except (TypeError, ValueError):
        nav_f = None
    try:
        gross_f = float(gross_exposure) if gross_exposure is not None else 0.0
    except (TypeError, ValueError):
        gross_f = 0.0
    flat = abs(gross_f) < 1.0
    if not flat:
        return nav_f
    if cash_f is None:
        return nav_f
    if nav_f is None:
        return cash_f
    if abs(nav_f - cash_f) <= 1.0:
        return nav_f
    return cash_f


def _is_close_like_execution(action_intent: str | None, action_side: str | None, execution_side: str | None) -> bool:
    return is_close_like_execution(action_intent, action_side, execution_side)


def _news_freshness_bucket(snapshot_age_minutes: float | None) -> str:
    if snapshot_age_minutes is None:
        return "UNKNOWN"
    if snapshot_age_minutes <= 90:
        return "FRESH"
    if snapshot_age_minutes <= 16 * 60:
        return "OVERNIGHT"
    if snapshot_age_minutes <= 48 * 60:
        return "WARM"
    # Older context should be treated as "no fresh news" for decisions.
    return "NO_FRESH_NEWS"


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
    no_fresh_news = freshness_bucket in ("STALE", "NO_FRESH_NEWS")
    is_stale = bool(source_signals.get("news_is_stale")) or no_fresh_news

    if no_fresh_news:
        # Past-context headlines are considered non-actionable for this decision.
        context_state = "NEUTRAL"
    elif event_risk and freshness_bucket in ("FRESH", "OVERNIGHT"):
        context_state = "DESTABILIZING"
    elif conflict or uncertainty or badge in ("HOT", "ALERT"):
        context_state = "CAUTIONARY"
    elif badge in ("COOL", "LOW") and not event_risk:
        context_state = "SUPPORTIVE"
    else:
        context_state = "NEUTRAL"

    if no_fresh_news:
        intensity_level = "LOW"
    elif info_pressure >= 0.75:
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
    if no_fresh_news:
        interpretation_confidence = 0.45
    elif context_state in ("CAUTIONARY", "DESTABILIZING"):
        interpretation_confidence = 0.75

    reason_codes = list(source_signals.get("news_reasons") or rationale.get("news_reasons") or [])
    if not isinstance(reason_codes, list):
        reason_codes = []
    if no_fresh_news:
        # Remove stale-style flags from decision reasons; treat as no-news consideration.
        reason_codes = [
            str(r)
            for r in reason_codes
            if str(r).upper() not in ("SNAPSHOT_STALE", "NEWS_CONTEXT_STALE", "STALE")
        ]
        reason_codes.append("NO_FRESH_NEWS_CONTEXT")
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
        "relevance": "LOW" if no_fresh_news else ("SYMBOL_LINKED" if (news_context or news_agg or news_features) else "LOW"),
        "theme_labels": theme_labels,
        "interpretation_confidence": interpretation_confidence,
        "snapshot_age_minutes": snapshot_age_minutes,
        "is_stale": is_stale,
        "badge": "NO_FRESH_NEWS" if no_fresh_news else badge,
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
        freshness_bucket = _news_freshness_bucket(age_minutes)
        no_fresh_news = freshness_bucket in ("STALE", "NO_FRESH_NEWS")
        raw_badge = str(row.get("BADGE") or "NEUTRAL").upper()
        raw_conflict = bool(row.get("CONFLICT"))
        if no_fresh_news:
            context_state = "NEUTRAL"
            event_shock_flag = False
            relevance = "LOW"
            interpretation_confidence = 0.45
            badge = "NO_FRESH_NEWS"
        else:
            context_state = "DESTABILIZING" if raw_badge in ("HOT", "ALERT") and raw_conflict else ("CAUTIONARY" if raw_conflict else "NEUTRAL")
            event_shock_flag = raw_badge in ("HOT", "ALERT") and (freshness_bucket in ("FRESH", "OVERNIGHT"))
            relevance = "SYMBOL_LINKED"
            interpretation_confidence = 0.8 if age_minutes is not None and age_minutes <= 16 * 60 else 0.55
            badge = raw_badge
        normalized = {
            "policy_version": NEWS_CONTEXT_POLICY_VERSION,
            "as_of_ts": now_utc.isoformat(),
            "freshness_bucket": freshness_bucket,
            "timing_model": "SAME_SESSION_FRESH" if (age_minutes is not None and age_minutes <= 90) else ("OVERNIGHT" if (age_minutes is not None and age_minutes <= 16 * 60) else "OLDER_BACKGROUND"),
            "intensity_level": "HIGH" if float(row.get("INFO_PRESSURE") or 0.0) >= 0.75 else ("MEDIUM" if float(row.get("INFO_PRESSURE") or 0.0) >= 0.35 else "LOW"),
            "context_state": context_state,
            "event_shock_flag": event_shock_flag,
            "relevance": relevance,
            "theme_labels": row.get("TOP_CLUSTERS") if isinstance(row.get("TOP_CLUSTERS"), list) else [],
            "interpretation_confidence": interpretation_confidence,
            "snapshot_age_minutes": age_minutes,
            "is_stale": no_fresh_news,
            "badge": badge,
            "news_reasons": (["NO_FRESH_NEWS_CONTEXT"] if no_fresh_news else []),
            "raw": serialize_row(row),
        }
        return {"available": True, **normalized}
    except Exception as exc:
        return {"available": False, "reason": f"NEWS_QUERY_FAILED: {exc}"}


def _resolve_news_for_decision(action_news_snapshot: dict | None, latest_news_snapshot: dict | None) -> dict:
    """
    Committee should use one authoritative news context:
    - Prefer latest symbol context when available.
    - Fall back to action snapshot only if latest is unavailable.
    """
    action_news = _parse_variant(action_news_snapshot)
    latest_news = _parse_variant(latest_news_snapshot)

    if latest_news.get("available"):
        return {"source": "LATEST_NEWS", "snapshot": latest_news}
    if action_news:
        return {"source": "ACTION_NEWS_FALLBACK", "snapshot": action_news}
    return {"source": "NONE", "snapshot": {"available": False, "context_state": "NEUTRAL", "freshness_bucket": "UNKNOWN"}}


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
    pattern_briefing = context.get("pattern_strategy_briefing") or {}
    strategy_line = ""
    if pattern_briefing.get("available"):
        sl_tp_line = pattern_briefing.get("sl_tp_guidance", "")
        strategy_line = (
            f"- IMPORTANT: This signal was generated by a {pattern_briefing.get('strategy_type', 'UNKNOWN')} pattern.\n"
            f"- Setup: {pattern_briefing.get('setup_description', '')}\n"
            f"- Evaluation guidance: {pattern_briefing.get('evaluation_guidance', '')}\n"
            f"- Expected hold: {pattern_briefing.get('expected_hold', '')}\n"
            f"- Risk profile: {pattern_briefing.get('risk_profile', '')}\n"
            + (f"- SL/TP guidance: {sl_tp_line}\n" if sl_tp_line else "")
            + "- IMPORTANT: You are evaluating at LIVE execution time. The market may have moved since the signal was generated. "
            "Recalculate stop_loss_pct and target_return based on the CURRENT price from latest_one_minute_bar, not the original signal price.\n"
        )
    conflict_sigs = context.get("conflict_signals") or []
    conflict_line = ""
    if conflict_sigs and not is_exit:
        conflict_types = ", ".join(set(c.get("pattern_type", "") for c in conflict_sigs))
        conflict_line = (
            f"- WARNING: {len(conflict_sigs)} conflicting bearish signal(s) detected for this symbol "
            f"({conflict_types}). These indicate downward pressure at the same timestamp. "
            "Factor this conflict into your decision — it may warrant BLOCK, reduced size, or tighter stop loss.\n"
        )
    prop_pw = context.get("proposal_pw_enrichment") if isinstance(context.get("proposal_pw_enrichment"), dict) else {}
    pw_prop_line = ""
    if prop_pw.get("pw_available"):
        pw_prop_line = (
            "- Proposal-time Parallel Worlds overlay (soft ranking signal at generation; NOT a hard gate; does not replace trust or policy): "
            f"confidence_bucket={prop_pw.get('confidence_bucket')}, "
            f"scenario_spread={prop_pw.get('scenario_spread')}, "
            f"avg_cumulative_regret={prop_pw.get('avg_cumulative_regret')}, "
            f"supporting_scenarios={prop_pw.get('analog_support_count')}, "
            f"pw_score_adjustment_applied={prop_pw.get('pw_score_adjustment_applied')}, "
            f"summary={prop_pw.get('summary_text', '')}\n"
            "- parallel_worlds_evidence (live rollup) may differ from proposal_pw_enrichment; treat both as structured context.\n"
        )
    elif isinstance(prop_pw, dict) and prop_pw.get("reason"):
        pw_prop_line = (
            f"- Proposal-time Parallel Worlds: unavailable ({prop_pw.get('reason')}). "
            "Do not infer risk from missing PW at proposal time.\n"
        )
    elif isinstance(prop_pw, dict):
        pw_prop_line = (
            "- Proposal-time Parallel Worlds: pw_available=false. Do not treat absence as a risk signal.\n"
        )
    regime = context.get("market_regime") or {}
    regime_line = ""
    if regime.get("regime"):
        regime_label = regime["regime"]
        regime_conf = regime.get("confidence", "UNKNOWN")
        regime_line = (
            f"- Market regime: {regime_label} (confidence: {regime_conf}). "
            f"In BEAR/VOLATILE_BEAR regimes, be more cautious on bullish entries; in BULL regimes, momentum trades get tailwind.\n"
        )
    eib = context.get("entry_intel_baseline") if isinstance(context.get("entry_intel_baseline"), dict) else {}
    alpha_b = eib.get("alpha_spec") if isinstance(eib.get("alpha_spec"), dict) else {}
    worlds_b = eib.get("worlds_spec") if isinstance(eib.get("worlds_spec"), dict) else {}
    dist = worlds_b.get("historical_distribution") if isinstance(worlds_b.get("historical_distribution"), dict) else {}
    effective_alpha = _effective_alpha_baseline_action(alpha_b)
    eis_line = ""
    if alpha_b.get("alpha_schema_version") or effective_alpha:
        hod_n = dist.get("sample_size")
        eis_line = (
            "- Pre-trade baseline (EIS, immutable snapshot): "
            f"recommended_action={alpha_b.get('recommended_action')}, "
            f"recommended_size_band={alpha_b.get('recommended_size_band')}, "
            f"expected_value_net={alpha_b.get('expected_value_net')}, "
            f"confidence_band={alpha_b.get('confidence_band')}, "
            f"downside_risk_band={alpha_b.get('downside_risk_band')}, "
            f"HOD_sample_size={hod_n}.\n"
            "- Full WORLDS_SPEC and ALPHA_SPEC are in context.entry_intel_baseline.worlds_spec / alpha_spec.\n"
            "- Committee aggregates a joint PROCEED / PROCEED_REDUCED / BLOCK versus this baseline; the system classifies ACCEPT_ALPHA, REDUCE_VS_ALPHA, INCREASE_VS_ALPHA, or BLOCK_DESPITE_ALPHA for audit.\n"
        )
        if effective_alpha and not is_exit:
            eis_line += (
                "- MANDATORY for each role: add to `reasons` either `ALPHA_BASELINE_ALIGNED` "
                "OR a token starting with `ALPHA_BASELINE_DEVIATION:` plus a concise explanation "
                "(e.g. ALPHA_BASELINE_DEVIATION: news shock overrides SKIP). "
                "If you materially disagree with the deterministic alpha posture, the deviation token is required.\n"
            )
    return (
        "You are one role in an institutional multi-agent trade committee.\n"
        "Return ONLY a JSON object with keys:\n"
        f"{schema_line}"
        "Rules:\n"
        "- Be strict and risk-aware.\n"
        "- If data freshness is weak, prefer CONDITIONAL or BLOCK.\n"
        "- Treat news_for_decision as the authoritative source for committee decisions.\n"
        "- action_news_context_snapshot is audit history only; do not let it override news_for_decision.\n"
        "- If news_for_decision.freshness_bucket is NO_FRESH_NEWS, treat as no active news consideration (do not frame it as stale risk).\n"
        f"{objective_line}"
        f"{strategy_line}"
        f"{conflict_line}"
        f"{pw_prop_line}"
        f"{regime_line}"
        f"{eis_line}"
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
    block_count = sum(1 for s in stances if s == "BLOCK")
    n_roles = max(len(outputs), 1)
    supermajority_threshold = max(1, int(n_roles * 2 / 3))
    if intent == "EXIT":
        recommendation = "BLOCK" if (outputs and block_count == n_roles) else "PROCEED_REDUCED"
    else:
        if block_count >= supermajority_threshold:
            recommendation = "BLOCK"
        elif block_count > 0 or any(s == "CONDITIONAL" for s in stances):
            recommendation = "PROCEED_REDUCED"
        else:
            recommendation = "PROCEED"
    _SIZE_WEIGHTS = {
        "RISK_MANAGER": 2.0,
        "PORTFOLIO_MANAGER": 2.0,
        "PROPOSER": 1.0,
        "TRADER_EXECUTION_REVIEWER": 1.0,
        "CHALLENGER": 1.0,
        "POST_TRADE_REVIEWER": 1.0,
    }
    if intent == "EXIT" and size_factors:
        size_factor = sum(size_factors) / len(size_factors)
    elif size_factors:
        weighted_sum = 0.0
        weight_total = 0.0
        for o in outputs:
            sf = o.get("size_factor")
            if sf is not None:
                w = _SIZE_WEIGHTS.get(str(o.get("role", "")).upper(), 1.0)
                weighted_sum += float(sf) * w
                weight_total += w
        size_factor = weighted_sum / weight_total if weight_total > 0 else 1.0
    else:
        size_factor = 1.0
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


def _committee_joint_decision_explicitly_allows_trade(jd: dict, action_intent: str) -> bool:
    """
    True only when the stored committee joint_decision contains an explicit proceed flag.
    Used to let human committee approval override NEWS_EXECUTION_CAUTION (not event-shock tier).
    """
    intent = str(action_intent or "ENTRY").upper()
    if not jd:
        return False
    if intent == "EXIT":
        if jd.get("should_execute_exit") is not None:
            return bool(jd.get("should_execute_exit"))
        if jd.get("should_enter") is not None:
            return bool(jd.get("should_enter"))
        return False
    if jd.get("should_enter") is not None:
        return bool(jd.get("should_enter"))
    return False


def _fetch_committee_joint_decision_for_action(cur, committee_run_id) -> dict:
    if not committee_run_id:
        return {}
    try:
        cur.execute(
            """
            select coalesce(
              VERDICT_JSON:joint_decision,
              VERDICT_JSON:verdict:joint_decision
            ) as JD
            from MIP.LIVE.COMMITTEE_VERDICT
            where RUN_ID = %s
            limit 1
            """,
            (committee_run_id,),
        )
        rows = fetch_all(cur)
        return _parse_variant((rows[0] or {}).get("JD")) if rows else {}
    except Exception:
        return {}


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
    live_action: dict | None = None,
) -> tuple[list[dict], dict]:
    """Run two dialogue rounds across committee roles."""
    if live_action is not None and is_structural_live_action(live_action):
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Legacy multi-agent committee is forbidden for structural live actions.",
                "reason_codes": ["STRUCTURAL_ROW_LEGACY_COMMITTEE_FORBIDDEN"],
            },
        )
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
    verdict["degraded_quality"] = "COMMITTEE_MODEL_FALLBACK_ALL_ROLES" in quality_reason_codes
    if emit:
        emit("joint_decision", {"verdict": verdict})
    return final_outputs, verdict


_PATTERN_STRATEGY_GUIDANCE = {
    "MOMENTUM": {
        "strategy_type": "MOMENTUM / TREND-FOLLOWING",
        "setup_description": "Price is exhibiting strong directional momentum with consecutive positive returns and new highs. The signal expects price to continue in the current direction.",
        "evaluation_guidance": "Favor trades where the trend is clear and volume confirms. Be cautious if the move looks overextended or exhaustion signals are present.",
        "expected_hold": "Medium-term (5-20 bars). Momentum trades typically need time for the trend to play out.",
        "risk_profile": "Trend reversal is the primary risk. Watch for divergence between price and momentum indicators.",
        "sl_tp_guidance": "Set stop_loss_pct at 0.03-0.05 (3-5%) to give the trend room to breathe without excessive risk. target_return based on trend continuation projection.",
    },
    "MEAN_REVERSION": {
        "strategy_type": "MEAN-REVERSION / OVERSOLD BOUNCE",
        "setup_description": "Price has deviated significantly from its recent average (VWAP proxy). The signal expects a snap-back toward the mean.",
        "evaluation_guidance": "This is a contrarian setup -- the price has DROPPED and the signal is buying the dip. Do NOT evaluate this through a trend-following lens. A declining price is the SETUP, not a reason to block. Focus on whether the deviation is large enough to warrant a bounce and whether there is a catalyst for recovery.",
        "expected_hold": "Short-term (1-3 bars). Mean-reversion trades target quick snap-backs, not sustained trends.",
        "risk_profile": "The price could continue falling (catching a falling knife). Size conservatively (size_factor 0.3-0.7) and use tight stops.",
        "sl_tp_guidance": "CRITICAL: Set tight stop_loss_pct at 0.01-0.02 (1-2%) below the deviation trough. target_return = the distance back to the VWAP proxy (deviation closing to ~0%). If the snap does not happen within 1-3 bars, the thesis is weakening. IMPORTANT: Recalculate based on CURRENT price, not the signal price -- if the deviation has already partially closed, the opportunity is smaller and SL/TP must be adjusted.",
    },
    "ORB": {
        "strategy_type": "OPENING RANGE BREAKOUT",
        "setup_description": "Price has broken out of its opening range, signaling a directional move for the session.",
        "evaluation_guidance": "Check if the breakout has sufficient volume and range size. False breakouts are common -- look for confirmation bars.",
        "expected_hold": "Intra-session to short-term (1-3 bars).",
        "risk_profile": "False breakout risk. Use the range boundary as stop-loss.",
        "sl_tp_guidance": "Set stop_loss_pct at 0.02-0.03 (2-3%). Use the opening range boundary as the natural stop level.",
    },
    "PULLBACK_CONTINUATION": {
        "strategy_type": "PULLBACK INTO TREND CONTINUATION",
        "setup_description": "After a strong impulse move, price has consolidated/pulled back and is now breaking out in the original impulse direction.",
        "evaluation_guidance": "Verify the impulse move was genuine (good volume, clear direction). The pullback should be shallow and orderly, not chaotic.",
        "expected_hold": "Medium-term (3-10 bars). Continuation trades target the next impulse leg.",
        "risk_profile": "Trend failure risk if the pullback deepens into a reversal.",
        "sl_tp_guidance": "Set stop_loss_pct at 0.02-0.04 (2-4%) below the pullback low. target_return based on impulse magnitude projection.",
    },
    "BEARISH_MOMENTUM": {
        "strategy_type": "BEARISH MOMENTUM (DEFENSIVE ONLY)",
        "setup_description": "Price is exhibiting sustained negative momentum with consecutive down days and new lows. This is a DEFENSIVE signal -- it should NOT generate buy positions.",
        "evaluation_guidance": "This signal is used to SUPPRESS bullish entries and TRIGGER early exits on existing positions for this symbol. If reviewing a bullish trade on a symbol with active bearish momentum, strongly consider BLOCKING the entry or reducing size significantly.",
        "expected_hold": "N/A -- this is a defensive context signal, not a trade entry signal.",
        "risk_profile": "The risk is in IGNORING this signal and entering bullish trades into a falling market.",
        "sl_tp_guidance": "N/A -- defensive signal only.",
    },
}


def _build_pattern_strategy_briefing(cur, training_snapshot: dict) -> dict:
    pattern_id = training_snapshot.get("pattern_id")
    if pattern_id is None:
        return {"available": False, "reason": "NO_PATTERN_ID"}
    try:
        cur.execute(
            """
            select NAME, PATTERN_TYPE,
                   LAST_HIT_RATE, LAST_AVG_RETURN, LAST_TRADE_COUNT,
                   PARAMS_JSON:market_type::string as PARAM_MARKET_TYPE
            from MIP.APP.PATTERN_DEFINITION
            where PATTERN_ID = %s
            """,
            (int(pattern_id),),
        )
        rows = fetch_all(cur)
    except Exception:
        rows = []
    if not rows:
        return {"available": False, "reason": "PATTERN_NOT_FOUND", "pattern_id": pattern_id}
    row = rows[0]
    pattern_type = str(row.get("PATTERN_TYPE") or "MOMENTUM").upper()
    guidance = _PATTERN_STRATEGY_GUIDANCE.get(pattern_type, _PATTERN_STRATEGY_GUIDANCE["MOMENTUM"])
    hit_rate = training_snapshot.get("hit_rate")
    avg_return = training_snapshot.get("avg_return")
    recs_total = training_snapshot.get("recs_total")
    return {
        "available": True,
        "pattern_id": int(pattern_id),
        "pattern_name": row.get("NAME"),
        "pattern_type": pattern_type,
        "strategy_type": guidance["strategy_type"],
        "setup_description": guidance["setup_description"],
        "evaluation_guidance": guidance["evaluation_guidance"],
        "expected_hold": guidance["expected_hold"],
        "risk_profile": guidance["risk_profile"],
        "sl_tp_guidance": guidance.get("sl_tp_guidance", ""),
        "symbol_track_record": {
            "hit_rate": float(hit_rate) if hit_rate is not None else None,
            "avg_return": float(avg_return) if avg_return is not None else None,
            "total_signals": int(recs_total) if recs_total is not None else None,
        },
    }


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
    resolved_news = _resolve_news_for_decision(action_news_snapshot, latest_news_snapshot)
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
    pattern_briefing = _build_pattern_strategy_briefing(cur, action_training_snapshot or {})
    conflict_signals = []
    try:
        signal_ts = action.get("SIGNAL_TS") or (action_training_snapshot or {}).get("signal_ts")
        market_type = action.get("ASSET_CLASS") or action.get("MARKET_TYPE")
        if symbol and signal_ts:
            cur.execute(
                """
                select
                    cpd.PATTERN_TYPE,
                    cpd.NAME as PATTERN_NAME,
                    coalesce(rl.DETAILS:direction::string, 'N/A') as DIRECTION,
                    rl.SCORE,
                    rl.DETAILS:deviation_pct::float as DEVIATION_PCT
                from MIP.APP.RECOMMENDATION_LOG rl
                join MIP.APP.PATTERN_DEFINITION cpd
                  on cpd.PATTERN_ID = rl.PATTERN_ID
                where rl.SYMBOL = %s
                  and rl.INTERVAL_MINUTES = 1440
                  and rl.TS = (select max(TS) from MIP.APP.RECOMMENDATION_LOG
                               where SYMBOL = %s and INTERVAL_MINUTES = 1440)
                  and (cpd.PATTERN_TYPE = 'BEARISH_MOMENTUM'
                       or (cpd.PATTERN_TYPE = 'MEAN_REVERSION'
                           and coalesce(rl.DETAILS:direction::string, '') = 'BEARISH'))
                order by rl.SCORE desc
                """,
                (symbol, symbol),
            )
            conflict_rows = fetch_all(cur)
            for cr in conflict_rows:
                conflict_signals.append({
                    "pattern_type": cr.get("PATTERN_TYPE"),
                    "pattern_name": cr.get("PATTERN_NAME"),
                    "direction": cr.get("DIRECTION"),
                    "score": float(cr["SCORE"]) if cr.get("SCORE") is not None else None,
                    "deviation_pct": float(cr["DEVIATION_PCT"]) if cr.get("DEVIATION_PCT") is not None else None,
                })
    except Exception:
        conflict_signals = []
    market_regime = {}
    try:
        cur.execute(
            """
            select REGIME, REGIME_CONFIDENCE, AVG_RETURN_1D, AVG_RETURN_5D,
                   AVG_RETURN_20D, AVG_VOLATILITY_20D
            from MIP.MART.V_MARKET_REGIME
            order by REGIME_DATE desc
            limit 1
            """
        )
        regime_rows = fetch_all(cur)
        if regime_rows:
            r = regime_rows[0]
            market_regime = {
                "regime": r.get("REGIME"),
                "confidence": r.get("REGIME_CONFIDENCE"),
                "avg_return_1d": float(r["AVG_RETURN_1D"]) if r.get("AVG_RETURN_1D") is not None else None,
                "avg_return_5d": float(r["AVG_RETURN_5D"]) if r.get("AVG_RETURN_5D") is not None else None,
                "avg_return_20d": float(r["AVG_RETURN_20D"]) if r.get("AVG_RETURN_20D") is not None else None,
                "volatility_20d": float(r["AVG_VOLATILITY_20D"]) if r.get("AVG_VOLATILITY_20D") is not None else None,
            }
    except Exception:
        market_regime = {}
    entry_intel_snapshot_id = None
    pid = action.get("PROPOSAL_ID")
    if pid is not None:
        try:
            entry_intel_snapshot_id = fetch_latest_snapshot_id_for_proposal(cur, int(pid))
        except Exception:
            entry_intel_snapshot_id = None
    entry_intel_baseline = None
    if entry_intel_snapshot_id:
        try:
            entry_intel_baseline = _fetch_entry_intel_baseline(cur, entry_intel_snapshot_id)
        except Exception:
            entry_intel_baseline = None
    proposal_pw_enrichment: dict = {"pw_available": False, "reason": "NO_PROPOSAL_ID"}
    proposal_diagnostics: dict = {}
    proposal_policy_version = None
    pid_ctx = action.get("PROPOSAL_ID")
    if pid_ctx is not None:
        try:
            cur.execute(
                """
                select PROPOSAL_POLICY_VERSION, PROPOSAL_DIAGNOSTICS, PW_ENRICHMENT
                from MIP.AGENT_OUT.ORDER_PROPOSALS
                where PROPOSAL_ID = %s
                limit 1
                """,
                (int(pid_ctx),),
            )
            cols = [d[0] for d in (cur.description or [])]
            row = cur.fetchone()
            if row and cols:
                prow = dict(zip(cols, row))
                proposal_policy_version = prow.get("PROPOSAL_POLICY_VERSION")
                proposal_diagnostics = _parse_variant(prow.get("PROPOSAL_DIAGNOSTICS"))
                if not isinstance(proposal_diagnostics, dict):
                    proposal_diagnostics = {}
                proposal_pw_enrichment = _parse_variant(prow.get("PW_ENRICHMENT"))
                if not isinstance(proposal_pw_enrichment, dict):
                    proposal_pw_enrichment = {"pw_available": False, "reason": "INVALID_PW_ENRICHMENT"}
        except Exception as exc:
            proposal_pw_enrichment = {"pw_available": False, "reason": f"ORDER_PROPOSALS_QUERY_FAILED:{exc}"}
            proposal_diagnostics = {}
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
        "pattern_strategy_briefing": pattern_briefing,
        "conflict_signals": conflict_signals,
        "market_regime": market_regime,
        "training_qualification_snapshot": action_training_snapshot,
        "target_expectation_snapshot": action_target_snapshot,
        "news_context_snapshot": resolved_news.get("snapshot"),
        "news_for_decision_source": resolved_news.get("source"),
        "action_news_context_snapshot": action_news_snapshot,
        "latest_symbol_news_context": latest_news_snapshot,
        "parallel_worlds_evidence": pw_evidence,
        "proposal_pw_enrichment": proposal_pw_enrichment,
        "proposal_diagnostics": proposal_diagnostics,
        "proposal_policy_version": proposal_policy_version,
        "decision_lifecycle": {
            "proposal_stage": "AUTONOMOUS_AGENT_OUT",
            "committee_stage": "EXECUTION_TIME_PRE_SUBMIT",
            "manual_submit": "POST_COMMITTEE_APPROVAL",
        },
        "entry_intel_snapshot_id": entry_intel_snapshot_id,
        "entry_intel_baseline": entry_intel_baseline,
        "execution_risk_config": risk_cfg,
    }
    if is_structural_live_action(action):
        context["structural"] = {
            "setup_event_id": action.get("SETUP_EVENT_ID"),
            "setup_family": action.get("SETUP_FAMILY"),
            "direction": action.get("DIRECTION"),
            "entry_zone_low": action.get("ENTRY_ZONE_LOW"),
            "entry_zone_high": action.get("ENTRY_ZONE_HIGH"),
            "invalidation_level": action.get("INVALIDATION_LEVEL"),
            "invalidation_rule": action.get("INVALIDATION_RULE"),
            "structure_confidence": action.get("STRUCTURE_CONFIDENCE"),
            "trust_label": action.get("TRUST_LABEL"),
            "meaningful_hit_rate": action.get("MEANINGFUL_HIT_RATE"),
            "regime_compat": action.get("REGIME_COMPAT"),
            "freshness_assessment": action.get("FRESHNESS_ASSESSMENT"),
            "expected_hold_character": action.get("EXPECTED_HOLD_CHARACTER"),
            "trail_style": action.get("TRAIL_STYLE"),
            "trail_activation_type": action.get("TRAIL_ACTIVATION_TYPE"),
            "setup_narrative": action.get("SETUP_NARRATIVE"),
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


@router.get("/ibkr/session-probe")
def get_ibkr_session_probe(
    portfolio_id: int | None = Query(None, description="LIVE portfolio ID. Resolves expected account, host, and port from config."),
):
    """
    Probe the connected IBKR TWS/Gateway session and return compatibility status.

    Connects in read-only mode, retrieves managed accounts, then disconnects immediately.
    No snapshot data is written, no Snowflake writes occur.

    Returns:
        connected         bool
        detected_accounts list[str]
        account_match     bool
        status            MATCH | ACCOUNT_MISMATCH | MULTIPLE_ACCOUNTS |
                          NOT_CONNECTED | PROBE_ERROR | CONFIG_NOT_FOUND
        message           str
    """
    if portfolio_id is None:
        return {
            "connected": False,
            "detected_accounts": [],
            "account_match": False,
            "status": "CONFIG_NOT_FOUND",
            "message": "No portfolio_id provided — cannot resolve IBKR session parameters.",
        }

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select IBKR_ACCOUNT_ID, IB_GATEWAY_HOST, IB_GATEWAY_PORT, IB_CLIENT_ID
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            """,
            (portfolio_id,),
        )
        rows = fetch_all(cur)
    finally:
        conn.close()

    if not rows:
        return {
            "connected": False,
            "detected_accounts": [],
            "account_match": False,
            "status": "CONFIG_NOT_FOUND",
            "message": f"No LIVE_PORTFOLIO_CONFIG row found for portfolio_id={portfolio_id}.",
        }

    row = rows[0]
    expected_account: str | None = row.get("IBKR_ACCOUNT_ID") or None
    params = _snapshot_sync_params_for_portfolio(portfolio_id)

    # Use a dedicated probe client ID that cannot collide with snapshot or execution clients.
    # All current allocations: 991 tape, 9402 paper-snapshot, 9403 real-snapshot (LIVE_PORTFOLIO_CONFIG),
    # 9410 probe, 9421 ingest, 9436 live-bars.
    probe_client_id = 9410

    return _probe_ibkr_session(
        host=params["host"],
        port=params["port"],
        client_id=probe_client_id,
        expected_account=expected_account,
    )


@router.post("/snapshot/refresh")
def refresh_live_snapshot(
    portfolio_id: int | None = Query(None, description="LIVE portfolio ID. Resolves host/port/account from config when provided."),
    account: str | None = Query(None, description="IBKR account code override. Resolved from config when portfolio_id is given and this is omitted."),
    host: str | None = Query(None, description="IB Gateway/TWS host override (optional; resolved from portfolio config when portfolio_id is set)."),
    port: int | None = Query(None, description="IB port override (optional; resolved from portfolio config when portfolio_id is set)."),
    client_id: int | None = Query(None, description="IB client ID override (optional; resolved from portfolio config when portfolio_id is set)."),
):
    """
    On-demand snapshot refresh.
    Triggers a single IBKR read-only pull and stores results in MIP.LIVE.BROKER_SNAPSHOTS.
    When portfolio_id is supplied, host/port/client_id are resolved from LIVE_PORTFOLIO_CONFIG
    (falling back to env-var defaults for NULL columns). Explicit query-param overrides take
    precedence over config-resolved values.
    Intended for:
      - opening Live Portfolio page
      - opening Live Trade/Approval page
      - pre-trade and post-trade refresh
    """
    # Resolve connection params: config lookup → env defaults; explicit overrides take priority.
    params = _snapshot_sync_params_for_portfolio(portfolio_id)
    effective_host      = host      if host      is not None else params["host"]
    effective_port      = port      if port      is not None else params["port"]
    effective_client_id = client_id if client_id is not None else params["client_id"]

    # Resolve account from portfolio config when not explicitly supplied.
    effective_account = account
    if not effective_account and portfolio_id is not None:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "select IBKR_ACCOUNT_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG where PORTFOLIO_ID = %s",
                (portfolio_id,),
            )
            cfg_rows = fetch_all(cur)
            if cfg_rows:
                effective_account = cfg_rows[0].get("IBKR_ACCOUNT_ID") or None
        finally:
            conn.close()

    # Session probe guard — block read if connected session does not expose the expected account.
    # Returns 409 (Conflict) instead of letting the sync attempt and surface a confusing 502.
    if portfolio_id is not None and effective_account:
        probe = _probe_ibkr_session(
            host=effective_host,
            port=effective_port,
            client_id=9410,
            expected_account=effective_account,
        )
        bad_statuses = {"ACCOUNT_MISMATCH", "NOT_CONNECTED", "PROBE_ERROR"}
        if probe.get("status") in bad_statuses:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "SESSION_MISMATCH",
                    "status": probe.get("status"),
                    "detected_accounts": probe.get("detected_accounts", []),
                    "expected_account": effective_account,
                    "message": probe.get("message", "IBKR session mismatch — start the correct TWS/Gateway."),
                },
            )

    result = _run_on_demand_snapshot_sync(
        host=effective_host,
        port=effective_port,
        client_id=effective_client_id,
        account=effective_account,
        portfolio_id=portfolio_id,
    )

    # After fresh snapshots arrive, opportunistically rewrite any LIVE_ORDERS rows
    # whose BROKER_ORDER_ID is still a TWS local order_id (because IB returned
    # perm_id=0 in the initial submit ack) to the perm_id surfaced in EXECUTION
    # snapshots. Idempotent and best-effort: never blocks the refresh.
    backfill = {"scanned": 0, "mapped": 0, "updated": 0, "samples": []}
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            backfill = _backfill_local_order_id_to_perm_id(
                cur,
                portfolio_id=portfolio_id,
                account_id=account,
            )
            try:
                conn.commit()
            except Exception:
                pass
        finally:
            conn.close()
    except Exception as exc:
        backfill = {"scanned": 0, "mapped": 0, "updated": 0, "error": str(exc)}

    return {
        "ok": True,
        "mode": "on_demand",
        "result": result,
        "perm_id_backfill": backfill,
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

    # Phase 3A — session probe before any broker cancel contact.
    # Require explicit portfolio_id (no silent fallback to most-recent-active-config).
    if portfolio_id is None:
        raise HTTPException(
            status_code=400,
            detail="portfolio_id is required for cancel-pending (Phase 3A: no implicit account fallback).",
        )
    _cancel_params = _snapshot_sync_params_for_portfolio(portfolio_id)
    _cancel_probe = _probe_ibkr_session(
        host=_cancel_params["host"],
        port=_cancel_params["port"],
        client_id=9410,
        expected_account=account_id,
    )
    if _cancel_probe.get("status") not in ("MATCH", "MULTIPLE_ACCOUNTS"):
        raise HTTPException(
            status_code=409,
            detail={
                "error": "SESSION_MISMATCH",
                "status": _cancel_probe.get("status"),
                "detected_accounts": _cancel_probe.get("detected_accounts", []),
                "expected_account": account_id,
                "message": _cancel_probe.get("message", "IBKR session mismatch — cannot cancel orders safely."),
            },
        )

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
                **_snapshot_sync_params_for_portfolio(portfolio_id),
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

    # Phase 3A — portfolio assertion and session probe before broker contact.
    _single_order_portfolio_id = order.get("PORTFOLIO_ID")
    if req.portfolio_id is not None and _single_order_portfolio_id is not None:
        if int(req.portfolio_id) != int(_single_order_portfolio_id):
            raise HTTPException(
                status_code=400,
                detail={
                    "message": f"PORTFOLIO_MISMATCH: order belongs to portfolio {_single_order_portfolio_id}, "
                               f"not the requested portfolio {req.portfolio_id}.",
                    "reason_codes": ["ORDER_PORTFOLIO_MISMATCH"],
                },
            )
    _probe_portfolio_id = _single_order_portfolio_id or (req.portfolio_id if req.portfolio_id is not None else None)
    if _probe_portfolio_id is not None:
        _single_cancel_params = _snapshot_sync_params_for_portfolio(_probe_portfolio_id)
        _single_probe = _probe_ibkr_session(
            host=_single_cancel_params["host"],
            port=_single_cancel_params["port"],
            client_id=9410,
            expected_account=account_id,
        )
        if _single_probe.get("status") not in ("MATCH", "MULTIPLE_ACCOUNTS"):
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "SESSION_MISMATCH",
                    "status": _single_probe.get("status"),
                    "detected_accounts": _single_probe.get("detected_accounts", []),
                    "expected_account": account_id,
                    "message": _single_probe.get("message", "IBKR session mismatch — cannot cancel order safely."),
                },
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
            # Global fallback (no portfolio_id/account filter). Guard against
            # multiple active portfolio configs — caller must be explicit.
            cur.execute(
                "select count(*) as CNT from MIP.LIVE.LIVE_PORTFOLIO_CONFIG where coalesce(IS_ACTIVE, true) = true"
            )
            _snap_active_count = (cur.fetchone() or [0])[0]
            if _snap_active_count > 1:
                raise HTTPException(
                    status_code=400,
                    detail="Multiple active portfolio configs exist — portfolio_id is required for /live/snapshot/latest.",
                )
            if _snap_active_count == 1:
                import logging as _logging
                _logging.getLogger("live").warning(
                    "GET /live/snapshot/latest called without portfolio_id — using single-config fallback. "
                    "Pass portfolio_id explicitly once LPA supports account selection."
                )
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
    include_legacy: bool = Query(
        False,
        description="Include LEGACY_PATTERN / UNKNOWN LIVE_INTENT_KIND rows (debug).",
    ),
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
          la.CREATED_AT, la.UPDATED_AT, la.LIVE_INTENT_KIND
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
        if not include_legacy:
            _excl = overview_excluded_intent_kinds(include_legacy=False)
            rows = [r for r in rows if live_intent_kind_from_row(r) not in _excl]
        return {"actions": serialize_rows(rows), "count": len(rows)}
    finally:
        conn.close()


def _cockpit_int_id(v) -> int | None:
    """Best-effort int for IB order ids (0 -> None)."""
    if v is None or v == "":
        return None
    try:
        if isinstance(v, bool):
            return None
        n = int(float(v))
        return None if n == 0 else n
    except (TypeError, ValueError):
        return None


def _broker_order_cockpit_member(p: dict) -> dict:
    row = p["row"]
    q = row.get("OPEN_ORDER_QTY")
    rem = row.get("OPEN_ORDER_REMAINING")
    lim = row.get("OPEN_ORDER_LIMIT_PRICE")
    return {
        "open_order_id": row.get("OPEN_ORDER_ID"),
        "status": row.get("OPEN_ORDER_STATUS"),
        "symbol": row.get("SYMBOL"),
        "qty": float(q) if q is not None else None,
        "remaining": float(rem) if rem is not None else None,
        "limit_price": float(lim) if lim is not None else None,
        "action": p.get("action"),
        "order_type": p.get("order_type"),
        "parent_order_id": p.get("parent_order_id"),
        "oca_group": p.get("oca_group"),
        "payload_order_id": p.get("order_id"),
        "payload_perm_id": p.get("perm_id"),
    }


def _parse_broker_open_order_row(row: dict) -> dict:
    payload = _parse_variant(row.get("PAYLOAD")) if row else {}
    if not isinstance(payload, dict):
        payload = {}
    oid = _cockpit_int_id(payload.get("orderId"))
    perm = _cockpit_int_id(payload.get("permId"))
    parent = _cockpit_int_id(payload.get("parentId"))
    oca = str(payload.get("ocaGroup") or "").strip() or None
    action = str(payload.get("action") or "").strip().upper() or None
    otype = str(payload.get("orderType") or "").strip().upper() or None
    open_id_norm = _normalize_broker_order_id(row.get("OPEN_ORDER_ID"))
    return {
        "row": row,
        "symbol": str(row.get("SYMBOL") or "").upper().strip(),
        "open_order_id": open_id_norm,
        "order_id": oid,
        "perm_id": perm,
        "parent_order_id": parent,
        "oca_group": oca,
        "action": action,
        "order_type": otype,
    }


def _cluster_broker_open_orders_for_cockpit(open_order_rows: list[dict]) -> list[dict]:
    """Group IBKR working orders by OCA group or parent/child (bracket) using snapshot PAYLOAD."""
    if not open_order_rows:
        return []
    parsed = [_parse_broker_open_order_row(r) for r in open_order_rows]
    by_order_id: dict[int, int] = {}
    for i, p in enumerate(parsed):
        if p["order_id"] is not None:
            by_order_id[p["order_id"]] = i

    def root_idx(i: int) -> int:
        seen: set[int] = set()
        cur_i = i
        while 0 <= cur_i < len(parsed):
            if cur_i in seen:
                return cur_i
            seen.add(cur_i)
            parent = parsed[cur_i]["parent_order_id"]
            if parent is None:
                return cur_i
            nxt = by_order_id.get(parent)
            if nxt is None:
                return cur_i
            cur_i = nxt
        return i

    used: set[int] = set()
    clusters: list[dict] = []

    oca_buckets: dict[tuple[str, str], list[int]] = {}
    for i, p in enumerate(parsed):
        if p["oca_group"] and p["symbol"]:
            oca_buckets.setdefault((p["symbol"], p["oca_group"]), []).append(i)

    for (sym, oca), idxs in sorted(oca_buckets.items()):
        used.update(idxs)
        clusters.append(
            {
                "link_type": "OCA_GROUP",
                "label": f"One-cancels-all · {sym} · group {oca}",
                "symbol": sym,
                "oca_group": oca,
                "orders": [_broker_order_cockpit_member(parsed[j]) for j in sorted(set(idxs), key=lambda x: parsed[x]["open_order_id"] or "")],
            }
        )

    root_to_idxs: dict[int, list[int]] = {}
    for i in range(len(parsed)):
        if i in used:
            continue
        r = root_idx(i)
        root_to_idxs.setdefault(r, []).append(i)

    for r, idxs in sorted(root_to_idxs.items(), key=lambda kv: (parsed[kv[0]]["symbol"], kv[0])):
        members = sorted(set(idxs), key=lambda j: parsed[j]["open_order_id"] or "")
        sym = parsed[r]["symbol"] or (parsed[members[0]]["symbol"] if members else "")
        only = members[0] if len(members) == 1 else None
        if only is not None and parsed[only]["parent_order_id"] is None:
            link_type = "STANDALONE"
            label = f"Working order · {sym or '—'}"
        else:
            link_type = "BRACKET_OR_CHILD"
            label = f"Linked orders (parent/bracket) · {sym or '—'}"
        clusters.append(
            {
                "link_type": link_type,
                "label": label,
                "symbol": sym,
                "oca_group": None,
                "orders": [_broker_order_cockpit_member(parsed[j]) for j in members],
            }
        )

    return clusters


def _cockpit_mip_family_visible(enriched: list[dict], protection: dict) -> bool:
    if len(enriched) >= 2:
        return True
    prot_state = str((protection or {}).get("state") or "NONE").upper()
    if prot_state in ("FULL", "PARTIAL"):
        return True
    if any(e.get("BROKER_TRUTH_ACTIVE") for e in enriched):
        return True
    return False


def _build_mip_order_families_for_cockpit(
    orders_enriched: list[dict],
    protection_by_action: dict[str, dict],
    order_groups: dict[str, list[dict]],
) -> list[dict]:
    """MIP LIVE_ORDERS rows grouped by ACTION_ID with bracket role hints (PROTECTION)."""
    families: list[dict] = []
    for action_id, raw_legs in order_groups.items():
        if not action_id or not raw_legs:
            continue
        enriched = [o for o in orders_enriched if str(o.get("ACTION_ID") or "") == str(action_id)]
        if not enriched:
            continue
        protection = protection_by_action.get(action_id) or {
            "state": "NONE",
            "parent": None,
            "take_profit": None,
            "stop_loss": None,
            "trailing_stop": None,
        }
        if not _cockpit_mip_family_visible(enriched, protection):
            continue
        symbol = str((enriched[0].get("SYMBOL") or "")).upper()

        def leg_summary(o: dict) -> dict:
            qo = o.get("QTY_ORDERED")
            qf = o.get("QTY_FILLED")
            lim = o.get("LIMIT_PRICE")
            return {
                "order_id": o.get("ORDER_ID"),
                "broker_order_id": o.get("BROKER_ORDER_ID"),
                "side": o.get("SIDE"),
                "order_type": o.get("ORDER_TYPE"),
                "status": o.get("STATUS"),
                "qty_ordered": float(qo) if qo is not None else None,
                "qty_filled": float(qf) if qf is not None else None,
                "limit_price": float(lim) if lim is not None else None,
                "broker_truth_active": bool(o.get("BROKER_TRUTH_ACTIVE")),
            }

        families.append(
            {
                "action_id": action_id,
                "symbol": symbol,
                "protection": protection,
                "legs": [leg_summary(o) for o in enriched],
            }
        )
    families.sort(key=lambda f: (f["symbol"], f["action_id"]))
    return families


# Structural ENTRY pending collapse: latest actionable proposal per symbol (see LPA Committee 2.0 spec).
_STRUCTURAL_PROPOSAL_TERMINAL_STATUSES = frozenset({"EXECUTED", "REJECTED", "CANCELLED", "EXPIRED"})


def _structural_proposal_status_is_actionable(status: str | None) -> bool:
    """Non-terminal proposal — consistent with open-proposal handling elsewhere (e.g. market timeline)."""
    s = (status or "PROPOSED").upper()
    return s not in _STRUCTURAL_PROPOSAL_TERMINAL_STATUSES


def _fetch_structural_proposal_status_map(cur, proposal_ids: list[int]) -> dict[int, str]:
    if not proposal_ids:
        return {}
    uniq = sorted({int(p) for p in proposal_ids})
    if not uniq:
        return {}
    placeholders = ",".join(["%s"] * len(uniq))
    cur.execute(
        f"""
        select PROPOSAL_ID, STATUS
        from MIP.APP.STRUCTURAL_TRADE_PROPOSALS
        where PROPOSAL_ID in ({placeholders})
        """,
        uniq,
    )
    out: dict[int, str] = {}
    for r in fetch_all(cur):
        try:
            out[int(r["PROPOSAL_ID"])] = str(r.get("STATUS") or "PROPOSED")
        except (TypeError, ValueError, KeyError):
            continue
    return out


def _compute_action_proposal_freshness(cur, proposal_id) -> str:
    """Single-action proposal_freshness label.

    Mirrors the overview builder's lineage join so submit/execute gates
    and the cockpit pending list agree on what counts as stale.

    Returns one of:
      - 'CURRENT': proposal is PROPOSED and its BOARD_RUN_ID is an
        authoritative run for the latest AS_OF_DATE.
      - 'SUPERSEDED_BY_NEWER_RUN': proposal is still PROPOSED but its
        parent board run is not authoritative (older AS_OF_DATE or
        cold-start fail-closed).
      - 'EXPIRED': proposal STATUS != 'PROPOSED', or the proposal row
        is gone.
      - 'NO_PROPOSAL_LINK': action carries no proposal_id at all.
    """
    if proposal_id is None:
        return "NO_PROPOSAL_LINK"
    cur.execute(
        """
        SELECT
          p.STATUS        AS PROPOSAL_STATUS_NOW,
          latest.RUN_ID   AS MATCHED_AUTH_RUN_ID
        FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
        LEFT JOIN MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN latest
          ON latest.RUN_ID = p.BOARD_RUN_ID
        WHERE p.PROPOSAL_ID = %s
        """,
        (proposal_id,),
    )
    frow = cur.fetchone()
    if not frow:
        return "EXPIRED"
    status_now = (frow[0] or "").upper() or None
    matched_auth_run_id = frow[1]
    if status_now is None or status_now != "PROPOSED":
        return "EXPIRED"
    if matched_auth_run_id is None:
        return "SUPERSEDED_BY_NEWER_RUN"
    return "CURRENT"


def _assert_proposal_freshness_for_structural_entry(action: dict | None) -> None:
    """Raise 409 when a structural ENTRY action has a non-CURRENT parent.

    LPA stale-lifecycle defense in depth: the overview already hides
    these rows from pending_decisions, but operators (or scripts) can
    still call submit/execute directly with an action_id. This guard
    ensures stale lineage cannot reach the broker even via the curl /
    replay path. EXIT actions are intentionally exempt — closing a
    live position must remain available regardless of what happened
    to the proposal that originated it.
    """
    if not action:
        return
    intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
    if intent == "EXIT":
        return
    if not is_structural_live_action(action):
        return
    conn = get_connection()
    try:
        cur = conn.cursor()
        freshness = _compute_action_proposal_freshness(cur, action.get("PROPOSAL_ID"))
    finally:
        conn.close()
    if freshness == "CURRENT":
        return
    raise HTTPException(
        status_code=409,
        detail={
            "message": (
                "Stale proposal — this action's parent proposal is no longer "
                "current. The next daily pipeline run will terminalize it; "
                "reject it explicitly if you need to clean it up sooner."
            ),
            "reason_codes": ["PROPOSAL_EXPIRED_OR_SUPERSEDED"],
            "proposal_freshness": freshness,
        },
    )


def _is_structural_entry_pending_row(row: dict) -> bool:
    if str(row.get("live_intent_kind") or "").upper() != "STRUCTURAL":
        return False
    return row.get("action_intent") != "EXIT"


def _dedupe_structural_entry_pending_rows(cur, pending_decisions: list[dict]) -> list[dict]:
    """One canonical structural ENTRY pending row per symbol.

    LPA stale-lifecycle contract (post-fix): only proposal_freshness
    == 'CURRENT' rows are eligible for the pending list. The overview
    build-loop now hard-excludes structural ENTRY rows whose parent
    proposal is no longer CURRENT, so by the time this function is
    called the input should already contain only CURRENT structural
    ENTRY rows. This function is therefore reduced to two jobs:

      1. Defense in depth — drop any non-CURRENT structural ENTRY row
         that somehow slipped through (test fixture, future code path
         change). These rows are never canonical and never shown.
      2. Pick the highest PROPOSAL_ID per symbol as the canonical row
         when multiple CURRENT siblings exist (same-day board runs
         under Phase 5D union semantics).

    The legacy STATUS-map fallback is retained for rows where the SQL
    join did not populate proposal_freshness, so older fixtures and
    unit tests still resolve to a sensible freshness label.
    """
    se_rows = [r for r in pending_decisions if _is_structural_entry_pending_row(r)]
    if not se_rows:
        return pending_decisions
    other = [r for r in pending_decisions if not _is_structural_entry_pending_row(r)]

    # Defensive backfill: if a row arrived without proposal_freshness
    # (e.g. test fixture / stale code path), recover via the existing
    # STATUS-map lookup so the CURRENT-only filter below still works.
    rows_needing_fallback = [r for r in se_rows if not r.get("proposal_freshness")]
    if rows_needing_fallback:
        prop_ids: list[int] = []
        for r in rows_needing_fallback:
            p = r.get("proposal_id")
            if p is None:
                continue
            try:
                prop_ids.append(int(p))
            except (TypeError, ValueError):
                continue
        st_map = _fetch_structural_proposal_status_map(cur, prop_ids) if prop_ids else {}
        for r in rows_needing_fallback:
            p = r.get("proposal_id")
            if p is None:
                r["proposal_freshness"] = "NO_PROPOSAL_LINK"
                continue
            try:
                pid = int(p)
            except (TypeError, ValueError):
                r["proposal_freshness"] = "NO_PROPOSAL_LINK"
                continue
            st = st_map.get(pid, "PROPOSED")
            r["proposal_freshness"] = (
                "CURRENT" if _structural_proposal_status_is_actionable(st) else "EXPIRED"
            )

    # CURRENT-only filter — drop any structural ENTRY row whose parent
    # proposal is no longer canonical. Non-CURRENT rows must never be
    # promoted to a canonical pending row; the daily pipeline cleanup
    # boundary (SP_EXPIRE_STALE_DAILY_PROPOSALS) is responsible for
    # terminalizing them in the DB.
    current_se_rows = [
        r for r in se_rows
        if str(r.get("proposal_freshness") or "").upper() == "CURRENT"
    ]
    if not current_se_rows:
        return other

    by_sym: dict[str, list[dict]] = {}
    for r in current_se_rows:
        sk = str(r.get("symbol") or "").upper().strip()
        if not sk:
            continue
        by_sym.setdefault(sk, []).append(r)

    def _pid_key(row: dict) -> int:
        p = row.get("proposal_id")
        try:
            return int(p)
        except (TypeError, ValueError):
            return -1

    deduped: list[dict] = []
    for _sym, rows_g in by_sym.items():
        # All rows here are CURRENT; pick the highest PROPOSAL_ID as
        # canonical and demote the rest into `superseded_pending` for
        # operator visibility (siblings from the same as-of board).
        canonical = max(rows_g, key=_pid_key)
        others = [x for x in rows_g if x is not canonical]
        if others:
            canon = dict(canonical)
            canon["superseded_pending"] = [
                {
                    "action_id": x.get("action_id"),
                    "proposal_id": x.get("proposal_id"),
                    "status": x.get("status"),
                    "proposal_freshness": x.get("proposal_freshness"),
                    "proposal_status_now": x.get("proposal_status_now"),
                }
                for x in others
            ]
            deduped.append(canon)
        else:
            deduped.append(canonical)
    return other + deduped


@router.get("/activity/overview")
def get_live_activity_overview(
    portfolio_id: int | None = Query(None, description="Live portfolio ID. Required when multiple active configs exist."),
    limit: int = Query(200, ge=50, le=1000),
    order_limit: int = Query(120, ge=20, le=1000),
    execution_limit: int = Query(60, ge=10, le=500),
    order_lookback_days: int = Query(30, ge=1, le=365),
    snapshot_lookback_days: int = Query(14, ge=1, le=365),
    include_legacy: bool = Query(
        False,
        description="When true, include LEGACY_PATTERN/UNKNOWN live actions in pending decisions (debug/historical).",
    ),
):
    conn = get_connection()
    try:
        cur = conn.cursor()
        if portfolio_id is not None:
            # Explicit portfolio — resolve directly, no count guard needed.
            cur.execute(
                """
                select
                  PORTFOLIO_ID, IBKR_ACCOUNT_ID, DRIFT_STATUS, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
                  MAX_POSITIONS, MAX_POSITION_PCT, BUST_PCT, IS_ACTIVE, UPDATED_AT
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where PORTFOLIO_ID = %s
                """,
                (portfolio_id,),
            )
        else:
            # Multi-config guard: if more than one active portfolio config exists and
            # no portfolio_id was provided, we cannot safely pick one. Fail closed so
            # the caller must be explicit. With a single active config the fallback
            # works normally (logs a warning for observability).
            cur.execute(
                "select count(*) as CNT from MIP.LIVE.LIVE_PORTFOLIO_CONFIG where coalesce(IS_ACTIVE, true) = true"
            )
            _active_count = (cur.fetchone() or [0])[0]
            if _active_count > 1:
                raise HTTPException(
                    status_code=400,
                    detail="Multiple active portfolio configs exist — portfolio_id is required for /live/activity/overview.",
                )
            if _active_count == 1:
                import logging as _logging
                _logging.getLogger("live").warning(
                    "GET /live/activity/overview called without portfolio_id — using single-config fallback. "
                    "Pass portfolio_id explicitly once LPA supports account selection."
                )
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
                "exit_warning_signals": [],
                "activity_trends": {"nav": [], "positions": []},
                "ui_hints": {},
                "counts": {},
                "cockpit_ibkr": {
                    "broker_order_clusters": [],
                    "mip_order_families": [],
                    "pending_decisions_count": 0,
                },
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "ib_host_diagnostics": _live_portfolio_ib_host_diagnostics(snapshot_state="BLOCKED"),
            }
        cfg = cfg_rows[0]
        portfolio_id = cfg.get("PORTFOLIO_ID")
        account_id = cfg.get("IBKR_ACCOUNT_ID")
        live_structural_only = _live_structural_only_enabled(cur)
        app_cfg = _read_app_config(
            cur,
            ["LIVE_AUTO_IMPORT_PROPOSALS_ON_OVERVIEW", "LIVE_AUTO_IMPORT_PROPOSAL_LIMIT"],
        )
        auto_import_enabled = _parse_bool_config(
            app_cfg.get("LIVE_AUTO_IMPORT_PROPOSALS_ON_OVERVIEW"), default=False
        )
        # Under LIVE_STRUCTURAL_ONLY the legacy importer is forbidden, but
        # `_auto_import_latest_proposals_for_live_portfolio` now delegates to
        # the structural importer so proposals materialise without manual
        # admin calls. Force-enable the bridge in that mode so the Structural
        # Timeline's AAPL/CAT/MCD entries become LPA actions automatically.
        if live_structural_only:
            auto_import_enabled = True
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
                cur=cur,
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
        nav = dict(nav_rows[0]) if nav_rows else {}
        if nav:
            coerced = _coerce_flat_book_nav_eur(
                nav.get("NET_LIQUIDATION_EUR"),
                nav.get("TOTAL_CASH_EUR"),
                nav.get("GROSS_POSITION_VALUE_EUR"),
            )
            if coerced is not None:
                nav["NET_LIQUIDATION_EUR"] = coerced
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
        nav_trend = []
        for r in nav_trend_rows:
            nv = _coerce_flat_book_nav_eur(
                r.get("NET_LIQUIDATION_EUR"),
                r.get("TOTAL_CASH_EUR"),
                r.get("GROSS_POSITION_VALUE_EUR"),
            )
            nav_trend.append(
                {
                    "snapshot_ts": r.get("SNAPSHOT_TS"),
                    "nav_eur": float(nv) if nv is not None else None,
                    "cash_eur": float(r.get("TOTAL_CASH_EUR")) if r.get("TOTAL_CASH_EUR") is not None else None,
                    "gross_exposure_eur": float(r.get("GROSS_POSITION_VALUE_EUR"))
                    if r.get("GROSS_POSITION_VALUE_EUR") is not None
                    else None,
                }
            )

        cur.execute(
            """
            with nav_points as (
              select SNAPSHOT_TS
              from MIP.LIVE.BROKER_SNAPSHOTS
              where SNAPSHOT_TYPE = 'NAV'
                and IBKR_ACCOUNT_ID = %s
                and SNAPSHOT_TS >= dateadd(day, -%s, current_timestamp())
            ),
            position_totals as (
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
            )
            select
              n.SNAPSHOT_TS,
              coalesce(p.TOTAL_UNREALIZED_PNL, 0) as TOTAL_UNREALIZED_PNL,
              coalesce(p.TOTAL_MARKET_VALUE, 0) as TOTAL_MARKET_VALUE,
              coalesce(p.OPEN_POSITION_COUNT, 0) as OPEN_POSITION_COUNT
            from nav_points n
            left join position_totals p
              on p.SNAPSHOT_TS = n.SNAPSHOT_TS
            order by n.SNAPSHOT_TS asc
            """,
            (account_id, snapshot_lookback_days, account_id, snapshot_lookback_days),
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

        # Load enough open-order snapshot rows to match order_limit / action limit; default 200 was too low.
        open_order_snapshot_row_cap = min(2500, max(int(order_limit), int(limit), 500))

        open_positions = []
        open_orders = []
        cockpit_broker_clusters: list[dict] = []
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
                  OPEN_ORDER_REMAINING, OPEN_ORDER_LIMIT_PRICE, PAYLOAD
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'OPEN_ORDER'
                  and IBKR_ACCOUNT_ID = %s
                  and SNAPSHOT_TS = %s
                order by OPEN_ORDER_ID
                limit %s
                """,
                (account_id, latest_snapshot_ts, open_order_snapshot_row_cap),
            )
            open_orders = fetch_all(cur)
            cockpit_broker_clusters = _cluster_broker_open_orders_for_cockpit(open_orders)

        cur.execute(
            """
            select
              ORDER_ID, ACTION_ID, BROKER_ORDER_ID, STATUS, SYMBOL, SIDE, ORDER_TYPE,
              QTY_ORDERED, LIMIT_PRICE, QTY_FILLED, AVG_FILL_PRICE,
              SUBMITTED_AT, ACKNOWLEDGED_AT, FILLED_AT, LAST_UPDATED_AT, CREATED_AT,
              ORDER_ROLE, PARENT_ORDER_ID, PROTECTION_TYPE, OCA_GROUP, STOP_PRICE,
              TRAIL_STYLE, TRAIL_ACTIVATED, TRAIL_ACTIVATED_AT,
              TRAIL_AMOUNT, TRAIL_PERCENT, BROKER_TRAIL_STATE
            from MIP.LIVE.LIVE_ORDERS
            where PORTFOLIO_ID = %s
              and coalesce(LAST_UPDATED_AT, CREATED_AT) >= dateadd(day, -%s, current_timestamp())
            order by LAST_UPDATED_AT desc, CREATED_AT desc
            limit %s
            """,
            (portfolio_id, order_lookback_days, order_limit),
        )
        orders = fetch_all(cur)
        broker_open_order_ids = _broker_open_order_ids_from_snapshot_rows(open_orders)
        held_symbols = {
            str((r.get("SYMBOL") or "")).upper()
            for r in open_positions
            if str((r.get("SYMBOL") or "")).strip()
        }
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
            trailing_stop_leg = None
            for ord_row in action_orders:
                order_type = str(ord_row.get("ORDER_TYPE") or "").upper()
                order_role = str(ord_row.get("ORDER_ROLE") or "").upper()
                protection_type = str(ord_row.get("PROTECTION_TYPE") or "").upper()
                status_display = _live_order_display_status(ord_row, broker_open_order_ids)
                leg = {
                    "order_id": ord_row.get("ORDER_ID"),
                    "broker_order_id": ord_row.get("BROKER_ORDER_ID"),
                    "status": str(status_display).upper(),
                    "order_type": order_type,
                    "order_role": order_role or None,
                    "protection_type": protection_type or None,
                    "side": ord_row.get("SIDE"),
                    "limit_price": float(ord_row.get("LIMIT_PRICE")) if ord_row.get("LIMIT_PRICE") is not None else None,
                    "stop_price": float(ord_row.get("STOP_PRICE")) if ord_row.get("STOP_PRICE") is not None else None,
                    "trail_style": (str(ord_row.get("TRAIL_STYLE")).upper() if ord_row.get("TRAIL_STYLE") is not None else None),
                    "trail_amount": float(ord_row.get("TRAIL_AMOUNT")) if ord_row.get("TRAIL_AMOUNT") is not None else None,
                    "trail_percent": float(ord_row.get("TRAIL_PERCENT")) if ord_row.get("TRAIL_PERCENT") is not None else None,
                    "avg_fill_price": float(ord_row.get("AVG_FILL_PRICE")) if ord_row.get("AVG_FILL_PRICE") is not None else None,
                    "qty_ordered": float(ord_row.get("QTY_ORDERED")) if ord_row.get("QTY_ORDERED") is not None else None,
                    "qty_filled": float(ord_row.get("QTY_FILLED")) if ord_row.get("QTY_FILLED") is not None else None,
                    "broker_truth_active": _is_order_active_in_broker_truth(ord_row, broker_open_order_ids),
                }

                # Classify by ORDER_ROLE / PROTECTION_TYPE first (authoritative,
                # populated by execute_live_action). Fall back to ORDER_TYPE
                # keyword sniffing for legacy rows that lack the role columns.
                bucket: str | None = None
                if order_role == "ENTRY":
                    bucket = "parent"
                elif order_role == "PROTECTIVE_TP" or protection_type == "TAKE_PROFIT":
                    bucket = "take_profit"
                elif order_role == "TRAILING_STOP" or protection_type == "TRAILING_STOP":
                    bucket = "trailing_stop"
                elif order_role == "PROTECTIVE_STOP" or protection_type == "FIXED_STOP":
                    bucket = "stop_loss"
                else:
                    if order_type == "TRAIL":
                        bucket = "trailing_stop"
                    elif any(token in order_type for token in ("STOP", "STP", "SL")):
                        bucket = "stop_loss"
                    elif any(token in order_type for token in ("TP", "TAKE_PROFIT", "LIMIT_TP")):
                        bucket = "take_profit"
                    else:
                        bucket = "parent"

                if bucket == "parent" and parent_leg is None:
                    parent_leg = leg
                elif bucket == "take_profit" and take_profit_leg is None:
                    take_profit_leg = leg
                elif bucket == "stop_loss" and stop_loss_leg is None:
                    stop_loss_leg = leg
                elif bucket == "trailing_stop" and trailing_stop_leg is None:
                    trailing_stop_leg = leg

            # The position is fully protected if it has a take-profit AND any
            # protective stop (fixed or trailing). Trailing counts the same as
            # a fixed stop for "armed" purposes.
            protective_stop_leg = stop_loss_leg or trailing_stop_leg
            if take_profit_leg and protective_stop_leg:
                protection_state = "FULL"
            elif take_profit_leg or protective_stop_leg:
                protection_state = "PARTIAL"
            else:
                protection_state = "NONE"
            protection_by_action[action_id] = {
                "state": protection_state,
                "parent": parent_leg,
                "take_profit": take_profit_leg,
                "stop_loss": stop_loss_leg,
                "trailing_stop": trailing_stop_leg,
            }

        # Proposal-lineage gate (operator-safety hardening, post-Phase-3):
        # join to STRUCTURAL_TRADE_PROPOSALS + the canonical
        # V_LATEST_AUTHORITATIVE_BOARD_RUN view so every action row
        # carries its parent proposal's STATUS and BOARD_RUN_ID, and the
        # currently-canonical authoritative run id. The build-loop below
        # uses these to compute `proposal_freshness` and gate
        # submission_allowed for STRUCTURAL ENTRY actions whose parent
        # proposal is no longer current.
        cur.execute(
            """
            select
              la.ACTION_ID, la.PROPOSAL_ID, la.SYMBOL, la.SIDE, la.ACTION_INTENT, la.EXIT_TYPE, la.STATUS, la.COMPLIANCE_STATUS,
              la.COMMITTEE_VERDICT, la.COMMITTEE_STATUS, la.COMMITTEE_REQUIRED, la.REASON_CODES,
              la.COMMITTEE_RUN_ID, la.COMMITTEE_COMPLETED_TS,
              cv.SIZE_FACTOR as COMMITTEE_SIZE_FACTOR,
              cv.VERDICT_JSON:verdict:joint_decision as COMMITTEE_JOINT_DECISION,
              la.PROPOSED_QTY, la.PROPOSED_PRICE, la.TARGET_OPEN_CONDITION_FACTOR, la.TRAINING_SIZE_CAP_FACTOR,
              la.TARGET_EXPECTATION_SNAPSHOT, la.PARAM_SNAPSHOT, la.CREATED_AT, la.UPDATED_AT,
              la.REVALIDATION_PRICE, la.PRICE_DEVIATION_PCT, la.REVALIDATION_TS, la.REVALIDATION_OUTCOME,
              la.SETUP_EVENT_ID, la.SETUP_FAMILY, la.DIRECTION, la.ENTRY_ZONE_LOW, la.ENTRY_ZONE_HIGH,
              la.INVALIDATION_LEVEL, la.TRAIL_STYLE, la.TRAIL_ACTIVATION_TYPE,
              la.SETUP_NARRATIVE, la.FRESHNESS_ASSESSMENT, la.EXPECTED_HOLD_CHARACTER,
              la.LIVE_INTENT_KIND,
              p.STATUS as PROPOSAL_STATUS_NOW,
              p.BOARD_RUN_ID as PROPOSAL_BOARD_RUN_ID,
              p.BOARD_DOSSIER_ID as PROPOSAL_BOARD_DOSSIER_ID,
              -- Phase 5D: V_LATEST_AUTHORITATIVE_BOARD_RUN now returns ONE
              -- row per COMPLETE run for the latest AS_OF_DATE (previously
              -- a single global "latest" row). Switching the join to match
              -- on RUN_ID converts the column to a per-row indicator: when
              -- the proposal's BOARD_RUN_ID belongs to the authoritative
              -- set the column is non-NULL and equal to the proposal's run
              -- (proposal is CURRENT); when it doesn't, the column is NULL
              -- (proposal is from an older AS_OF_DATE — SUPERSEDED). Same-
              -- day siblings are no longer adversaries; they all show up
              -- as CURRENT because every completed run for the latest
              -- AS_OF_DATE appears in the view.
              latest.RUN_ID as LATEST_AUTHORITATIVE_RUN_ID
            from MIP.LIVE.LIVE_ACTIONS la
            left join MIP.LIVE.COMMITTEE_VERDICT cv
              on cv.RUN_ID = la.COMMITTEE_RUN_ID
            left join MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
              on p.PROPOSAL_ID = la.PROPOSAL_ID
            left join MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN latest
              on latest.RUN_ID = p.BOARD_RUN_ID
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
        _excl_kinds = overview_excluded_intent_kinds(include_legacy=include_legacy)
        if _excl_kinds:
            action_rows = [
                r
                for r in action_rows
                if live_intent_kind_from_row(r) not in _excl_kinds
            ]
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

        unmapped_exec_summary = _recent_unmapped_execution_summary(
            cur,
            int(portfolio_id),
            str(account_id or ""),
            lookback_days=max(int(order_lookback_days), int(snapshot_lookback_days), 14),
            sample_limit=20,
        )
        unmapped_exec_count = int(unmapped_exec_summary.get("count") or 0)
        reconciliation_state = "REQUIRED" if unmapped_exec_count > 0 else "CLEAR"

        now_utc = datetime.now(timezone.utc)
        market_open = _is_extended_trading_open_ny(now_utc)
        open_utc, close_utc = _extended_trading_bounds_utc(now_utc)
        snapshot_state = _compute_snapshot_freshness_state(snapshot_age_sec, cfg.get("SNAPSHOT_FRESHNESS_THRESHOLD_SEC"))
        drift_state = _compute_drift_state(cfg.get("DRIFT_STATUS"), unresolved_drift_count)
        drift_state_effective = "BLOCKED" if reconciliation_state == "REQUIRED" else drift_state
        # Entries require extended-hours window; exits may submit off-hours (DAY close queues for next session).
        page_actionable_base = (
            snapshot_state in ("FRESH", "AGING")
            and drift_state_effective != "BLOCKED"
            and reconciliation_state != "REQUIRED"
        )
        page_actionable = page_actionable_base and market_open
        nav_eur = float(nav.get("NET_LIQUIDATION_EUR") or 0.0) if nav else 0.0

        pending_decisions = []
        suppress_pending_symbols: set[str] = set()
        # Stage 4d — bulk-evaluate the agentic authority gate for every visible
        # action in one query. We pass action_ids for all rows (structural and
        # non-structural alike); the per-row check below ignores the verdict
        # for non-structural / EXIT rows.
        _candidate_action_ids = [
            str(r.get("ACTION_ID")) for r in action_rows if r.get("ACTION_ID")
        ]
        agentic_authority_gate_by_action = (
            evaluate_authority_gate_bulk(conn, _candidate_action_ids)
            if _candidate_action_ids
            else {}
        )
        # Stage 4f — when agentic-primary materialization is on, the
        # committee joint-decision is diagnostic only and must not feed
        # the Submit decision. Read the flag once per request and suppress
        # `committee_blocks_entry` below for structural ENTRY rows.
        try:
            _agentic_primary_on = is_agentic_primary_materialization_enabled(conn)
        except Exception:  # noqa: BLE001
            _agentic_primary_on = False
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
            action_intent = _normalize_action_intent(row.get("SIDE"), row.get("ACTION_INTENT"))
            is_exit = action_intent == "EXIT"
            joint_decision = _parse_variant(row.get("COMMITTEE_JOINT_DECISION"))
            param_snap_row = _parse_variant(row.get("PARAM_SNAPSHOT"))
            if not isinstance(param_snap_row, dict):
                param_snap_row = {}
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
            bracket_realism_rc = bool(
                action_reason_codes
                and any(str(x).strip().upper().startswith("LIVE_BRACKET_") for x in action_reason_codes)
            )
            blocked = status == "OPEN_BLOCKED" or bool(
                action_reason_codes
                and (
                    any("BLOCK" in str(x).upper() for x in action_reason_codes)
                    or bracket_realism_rc
                )
            )
            committee_should_enter = joint_decision.get("should_enter")
            committee_blocks_entry = (action_intent != "EXIT") and (committee_should_enter is False)
            # Stage 4f — when agentic-primary is active, the deterministic
            # JD is diagnostic only. Do NOT let `should_enter=False` from a
            # stale C2 verdict participate in the Submit gate; the agentic
            # gate (below) is the single source of truth.
            is_structural_row_for_4f = (
                str(row.get("LIVE_INTENT_KIND") or "").upper() == "STRUCTURAL"
            )
            if _agentic_primary_on and is_structural_row_for_4f and not is_exit:
                committee_blocks_entry = False
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
                "LIVE_MIN_VIABLE_SIZE_NOT_REACHED",
                "LIVE_BRACKET_NOT_VIABLE_WITHIN_GUARDRAILS",
                "LIVE_BRACKET_CALIBRATION_EXCEEDS_MAX_TP",
                "LIVE_BRACKET_CALIBRATION_EXCEEDS_MAX_SL",
                "BROKER_SHORT_POSITION_OUT_OF_POLICY",
                "SYMBOL_SHORT_POSITION_OUT_OF_POLICY",
                "ENTRY_SIDE_NOT_ALLOWED_LONG_ONLY",
                "STRUCT_SUBMIT_CONTRACT_INCOMPLETE",
                "STRUCTURAL_ROW_LEGACY_COMMITTEE_FORBIDDEN",
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
                    "LIVE_TP_REQUIRED_MISSING",
                    "LIVE_SL_REQUIRED_MISSING",
                    "LIVE_BRACKET_REQUIRED",
                    "LIVE_TP_NET_EDGE_TOO_LOW",
                    "LIVE_RISK_REWARD_TOO_LOW",
                    "LIVE_MIN_VIABLE_SIZE_NOT_REACHED",
                }
            execution_hard_blocked = bool(
                action_reason_codes
                and (
                    any(str(x).upper() in hard_block_codes for x in action_reason_codes)
                    or bracket_realism_rc
                )
            )
            if (
                not is_exit
                and is_structural_live_action(row)
                and status not in ("EXECUTED", "EXECUTION_REQUESTED", "EXECUTION_PARTIAL")
            ):
                preflight_bracket_codes = _preflight_entry_bracket_hard_block_reason_codes(cur, row)
                if preflight_bracket_codes:
                    execution_hard_blocked = True
                    for pbc in preflight_bracket_codes:
                        if pbc not in action_reason_codes:
                            action_reason_codes.append(pbc)
                else:
                    action_reason_codes = _strip_recomputable_entry_bracket_codes(action_reason_codes)
                    execution_hard_blocked = bool(
                        action_reason_codes
                        and (
                            any(str(x).upper() in hard_block_codes for x in action_reason_codes)
                            or bool(
                                action_reason_codes
                                and any(
                                    str(x).strip().upper().startswith("LIVE_BRACKET_")
                                    for x in action_reason_codes
                                )
                            )
                        )
                    )
            # Proposal-lineage freshness gate (Patch Group A, post-Phase-3
            # operator-safety hardening). For STRUCTURAL ENTRY actions
            # only: a live action whose parent proposal has been EXPIRED
            # by lifecycle (Rule 0/1/2/3) or whose parent proposal is
            # still PROPOSED but belongs to a non-canonical board run is
            # no longer actionable. The action row stays in the queue
            # for visibility but loses Run Committee / Submit. EXIT
            # actions are intentionally NOT gated — closing a live
            # position is a cleanup path that must remain available
            # regardless of what happened to the originating proposal.
            #
            # `proposal_freshness` values:
            #   CURRENT      - proposal STATUS='PROPOSED' AND BOARD_RUN_ID
            #                  matches the latest authoritative run.
            #   EXPIRED      - proposal STATUS != 'PROPOSED'.
            #   SUPERSEDED_BY_NEWER_RUN
            #                - proposal still PROPOSED but BOARD_RUN_ID
            #                  is not the latest authoritative run id
            #                  (or the latest run id is unknown — a cold-
            #                  start fail-closed condition).
            #   NO_PROPOSAL_LINK
            #                - action carries no proposal_id at all.
            #                  Treated as not-fresh for STRUCTURAL ENTRY.
            proposal_status_now = (row.get("PROPOSAL_STATUS_NOW") or "").upper() or None
            proposal_board_run_id = row.get("PROPOSAL_BOARD_RUN_ID")
            # Phase 5D: with the new RUN_ID-matching LEFT JOIN above,
            # LATEST_AUTHORITATIVE_RUN_ID is either:
            #   * non-NULL and equal to the proposal's BOARD_RUN_ID, when
            #     the proposal's parent run is one of the authoritative
            #     runs for the latest AS_OF_DATE (CURRENT), OR
            #   * NULL, when no authoritative run matches the proposal's
            #     parent run — either because the proposal is from an
            #     older AS_OF_DATE (SUPERSEDED) or because no runs are
            #     currently authoritative at all (cold-start fail-closed,
            #     same SUPERSEDED treatment).
            # We therefore no longer need to compare board run ids — the
            # join itself is the test.
            latest_auth_run_id = row.get("LATEST_AUTHORITATIVE_RUN_ID")
            if row.get("PROPOSAL_ID") is None:
                proposal_freshness = "NO_PROPOSAL_LINK"
            elif proposal_status_now is None:
                proposal_freshness = "EXPIRED"
            elif proposal_status_now != "PROPOSED":
                proposal_freshness = "EXPIRED"
            elif latest_auth_run_id is None:
                proposal_freshness = "SUPERSEDED_BY_NEWER_RUN"
            else:
                proposal_freshness = "CURRENT"

            is_structural_action = str(row.get("LIVE_INTENT_KIND") or "").upper() == "STRUCTURAL"
            superseded_blocked = bool(
                is_structural_action
                and (not is_exit)
                and proposal_freshness != "CURRENT"
            )
            if superseded_blocked and "PROPOSAL_EXPIRED_OR_SUPERSEDED" not in action_reason_codes:
                action_reason_codes = list(action_reason_codes) + ["PROPOSAL_EXPIRED_OR_SUPERSEDED"]

            trade_surface_ok = page_actionable_base and (market_open or is_exit)
            submit_allowed = status in ("INTENT_APPROVED", "REVALIDATED_FAIL", "REVALIDATED_PASS", "COMPLIANCE_APPROVED", "INTENT_SUBMITTED", "PM_ACCEPTED", "READY_FOR_APPROVAL_FLOW")
            # Stage 4d — agentic authority gate. Block submit only when:
            #   * action is STRUCTURAL ENTRY (EXIT actions never use shadow board), AND
            #   * AGENTIC_AUTHORITY_ENABLED = true (gate fully off until flag flip), AND
            #   * the latest authority row does not satisfy the four gate conditions.
            _row_action_id = str(row.get("ACTION_ID") or "")
            _agentic_verdict = agentic_authority_gate_by_action.get(_row_action_id) or {}
            agentic_authority_blocks_entry = bool(
                is_structural_action
                and (not is_exit)
                and _agentic_verdict.get("gate_enabled")
                and not _agentic_verdict.get("gate_ok")
            )
            if is_structural_action and (not is_exit) and _agentic_verdict:
                from app.committee.agentic_authority import reconcile_agentic_authority_reason_codes

                action_reason_codes = reconcile_agentic_authority_reason_codes(
                    action_reason_codes,
                    authority_status=_agentic_verdict.get("authority_status"),
                    is_stale=bool(_agentic_verdict.get("is_stale")),
                    gate_ok=(
                        _agentic_verdict.get("gate_ok")
                        if _agentic_verdict.get("gate_enabled")
                        else True
                    ),
                )
            submit_allowed = (
                submit_allowed
                and trade_surface_ok
                and (not blocked)
                and (not execution_hard_blocked)
                and (not committee_blocks_entry)
                and (not superseded_blocked)
                and (not agentic_authority_blocks_entry)
            )
            in_position = symbol in held_symbols
            action_id = str(row.get("ACTION_ID") or "")
            action_orders = order_groups.get(action_id) or []
            has_active_order = any(_is_order_active_in_broker_truth(o, broker_open_order_ids) for o in action_orders)
            protection_details = protection_by_action.get(action_id) or {"state": "NONE", "parent": None, "take_profit": None, "stop_loss": None, "trailing_stop": None}
            eb_row = param_snap_row.get("executable_bracket") if isinstance(param_snap_row, dict) else None
            protection_planned = bool(
                joint_decision.get("realistic_target_return") is not None
                or joint_decision.get("acceptable_early_exit_target_return") is not None
                or (
                    isinstance(eb_row, dict)
                    and eb_row.get("blocked") is not True
                    and eb_row.get("target_return") is not None
                )
            )

            submission_gate_hints: list[str] = []
            # Surface OPEN_BLOCKED clearly for structural ENTRY rows where the
            # agentic committee already approved — operators often think they
            # still need to "commit" or "price check" while the real blocker is
            # the opening/market guard.
            if (
                status == "OPEN_BLOCKED"
                and is_structural_action
                and (not is_exit)
                and _agentic_verdict.get("authority_mode") == "OPERATOR_COMMITTED"
                and str(_agentic_verdict.get("authority_status") or "").upper()
                in ("AGENTIC_APPROVE", "AGENTIC_APPROVE_REDUCED")
            ):
                if _reason_codes_include_bracket_contract_block(action_reason_codes):
                    submission_gate_hints.append(
                        "Bracket contract incomplete (TP/SL missing) — not an opening-guard issue. "
                        "Verdict is recorded; fix bracket seeding or refresh after server update."
                    )
                elif _reason_codes_include_opening_guard_block(action_reason_codes):
                    submission_gate_hints.append(
                        "Opening guard (market closed or stale snapshot) — committee verdict is done. "
                        "Click Recheck opening guard when the market opens; no further commit needed."
                    )
                else:
                    submission_gate_hints.append(
                        "Action blocked after agentic approval — expand reason codes; "
                        "Recheck opening guard only helps for market-hours / snapshot issues."
                    )
            # Always surface the proposal-lineage gate as a top-priority
            # hint — it overrides everything else, because "the
            # underlying proposal is dead" is the most operator-relevant
            # blocker on the screen.
            if superseded_blocked:
                if proposal_freshness == "EXPIRED":
                    submission_gate_hints.append(
                        "Underlying proposal is EXPIRED — submission blocked. Reject stale / cleanup only."
                    )
                elif proposal_freshness == "SUPERSEDED_BY_NEWER_RUN":
                    submission_gate_hints.append(
                        "Underlying proposal is from a superseded board run — submission blocked. Reject stale / cleanup only."
                    )
                elif proposal_freshness == "NO_PROPOSAL_LINK":
                    submission_gate_hints.append(
                        "Action has no parent proposal — submission blocked. Reject / cleanup only."
                    )
            if status == "REVALIDATED_PASS" and not submit_allowed:
                if snapshot_state not in ("FRESH", "AGING"):
                    submission_gate_hints.append("Snapshot not fresh enough — click Refresh From IB.")
                if drift_state_effective == "BLOCKED":
                    submission_gate_hints.append("Drift / controls blocked this trading surface.")
                if reconciliation_state == "REQUIRED":
                    submission_gate_hints.append("Broker reconciliation required (unmapped executions).")
                if not market_open and not is_exit:
                    submission_gate_hints.append("Outside operating hours (new entries only; exits use other gates).")
                if blocked:
                    submission_gate_hints.append("Decision blocked — see reason codes.")
                if execution_hard_blocked:
                    submission_gate_hints.append("Execution policy blocked — see reason codes.")
                if committee_blocks_entry:
                    # Stage 4f neutralizes this gate when agentic-primary is on, so
                    # this hint only fires under the legacy/rollback path. Keep the
                    # phrasing generic ("review") rather than naming a specific board.
                    submission_gate_hints.append("Review verdict did not approve entry.")
                if agentic_authority_blocks_entry:
                    _agentic_tip = _agentic_verdict.get("tooltip")
                    if _agentic_tip:
                        submission_gate_hints.append(_agentic_tip)
                if not submission_gate_hints:
                    submission_gate_hints.append("Submission unavailable — refresh the page or verify action status.")

            dossier_raw = row.get("PROPOSAL_BOARD_DOSSIER_ID")
            try:
                structural_board_dossier_id = (
                    int(dossier_raw) if dossier_raw is not None else None
                )
            except (TypeError, ValueError):
                structural_board_dossier_id = None

            if (
                status != "EXECUTION_REQUESTED"
                and (not has_active_order)
                and (is_exit or not in_position)
                and (is_exit or symbol not in suppress_pending_symbols)
            ):
                # LPA stale-lifecycle gate. A structural ENTRY action
                # whose parent proposal is no longer CURRENT (expired
                # or from a superseded board run) is hidden from the
                # pending_decisions list entirely. The authoritative
                # cleanup boundary is the next daily pipeline run,
                # which calls SP_EXPIRE_STALE_DAILY_PROPOSALS to
                # terminalize these rows in Snowflake. Until then we
                # simply do not surface them — operators should not
                # need to manually Reject stale rows at the daily
                # boundary. EXIT actions are never gated this way:
                # closing a live position must remain available
                # regardless of what happened to the proposal that
                # originated it.
                if superseded_blocked:
                    continue

                # Defensive: the SQL whitelist already excludes terminal
                # statuses (SUPERSEDED / REJECTED / CANCELLED), but guard
                # against any future widening so terminal rows can never
                # leak into the cockpit pending list.
                if status in ("SUPERSEDED", "REJECTED", "CANCELLED"):
                    continue

                required_next_step = (
                    _required_next_step_structural_entry(
                        status,
                        agentic_verdict=_agentic_verdict if is_structural_action and (not is_exit) else None,
                        reason_codes=action_reason_codes if is_structural_action and (not is_exit) else None,
                    )
                    if is_structural_action and (not is_exit)
                    else _required_next_step_for_status(status)
                )

                pending_decisions.append(
                    {
                        "action_id": row.get("ACTION_ID"),
                        "live_intent_kind": live_intent_kind_from_row(row),
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
                        "required_next_step": required_next_step,
                        "submission_allowed": bool(submit_allowed),
                        "submission_gate_hints": submission_gate_hints,
                        "is_blocked": bool(blocked),
                        "execution_hard_blocked": bool(execution_hard_blocked),
                        "superseded_blocked": bool(superseded_blocked),
                        "agentic_authority_blocks_entry": bool(agentic_authority_blocks_entry),
                        "agentic_authority_gate": {
                            "gate_enabled": bool(_agentic_verdict.get("gate_enabled")) if _agentic_verdict else False,
                            "gate_ok": bool(_agentic_verdict.get("gate_ok")) if _agentic_verdict else None,
                            "reason_code": _agentic_verdict.get("reason_code") if _agentic_verdict else None,
                            "tooltip": _agentic_verdict.get("tooltip") if _agentic_verdict else None,
                            "authority_mode": _agentic_verdict.get("authority_mode") if _agentic_verdict else None,
                            "authority_status": _agentic_verdict.get("authority_status") if _agentic_verdict else None,
                            "is_stale": _agentic_verdict.get("is_stale") if _agentic_verdict else None,
                            "authority_id": _agentic_verdict.get("authority_id") if _agentic_verdict else None,
                            "applies": bool(is_structural_action and (not is_exit)),
                        },
                        "proposal_freshness": proposal_freshness,
                        "proposal_status_now": proposal_status_now,
                        "proposal_board_run_id": proposal_board_run_id,
                        "latest_authoritative_run_id": latest_auth_run_id,
                        "committee_should_enter": committee_should_enter,
                        "committee_decision": {
                            "should_enter": joint_decision.get("should_enter") if joint_decision else None,
                            "risk_notes": joint_decision.get("risk_notes") if joint_decision else None,
                            "realistic_target_return": joint_decision.get("realistic_target_return") if joint_decision else None,
                            "stop_loss_pct": joint_decision.get("stop_loss_pct") if joint_decision else None,
                        } if joint_decision else None,
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
                            "min_viable_uplift": param_snap_row.get("min_viable_live_uplift"),
                            "executable_bracket": param_snap_row.get("executable_bracket"),
                            "blocked_bracket_class": (
                                (eb_row or {}).get("blocked_bracket_class")
                                if isinstance(eb_row, dict) and eb_row.get("blocked")
                                else None
                            ),
                        },
                        "protection": {"planned": protection_planned, **protection_details},
                        "structural": (
                            {
                                "setup_event_id": row.get("SETUP_EVENT_ID"),
                                "setup_family": row.get("SETUP_FAMILY"),
                                "direction": row.get("DIRECTION"),
                                "entry_zone_low": float(row["ENTRY_ZONE_LOW"]) if row.get("ENTRY_ZONE_LOW") is not None else None,
                                "entry_zone_high": float(row["ENTRY_ZONE_HIGH"]) if row.get("ENTRY_ZONE_HIGH") is not None else None,
                                "invalidation_level": float(row["INVALIDATION_LEVEL"]) if row.get("INVALIDATION_LEVEL") is not None else None,
                                "trail_style": row.get("TRAIL_STYLE"),
                                "trail_activation_type": row.get("TRAIL_ACTIVATION_TYPE"),
                                "setup_narrative": row.get("SETUP_NARRATIVE"),
                                "freshness_assessment": row.get("FRESHNESS_ASSESSMENT"),
                                "hold_character": row.get("EXPECTED_HOLD_CHARACTER"),
                                "board_dossier_id": structural_board_dossier_id,
                                "committee_logic_version": STRUCTURAL_COMMITTEE_LOGIC_VERSION,
                                "structural_diagnostics_v1": param_snap_row.get("structural_diagnostics_v1"),
                                "structural_execution_contract_v1": param_snap_row.get("structural_execution_contract_v1"),
                            }
                            if is_structural_live_action(row)
                            else None
                        ),
                        "timestamps": {
                            "created_at": row.get("CREATED_AT"),
                            "updated_at": row.get("UPDATED_AT"),
                        },
                        "price_guard": {
                            "revalidation_price": float(row["REVALIDATION_PRICE"])
                            if row.get("REVALIDATION_PRICE") is not None
                            else None,
                            "price_deviation_pct": float(row["PRICE_DEVIATION_PCT"])
                            if row.get("PRICE_DEVIATION_PCT") is not None
                            else None,
                            "revalidation_ts": row.get("REVALIDATION_TS"),
                            "revalidation_outcome": row.get("REVALIDATION_OUTCOME"),
                            "pass_max_pct": 0.02,
                            "reduced_max_pct": 0.04,
                        },
                    }
                )

        pending_decisions = _dedupe_structural_entry_pending_rows(cur, pending_decisions)

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
        broker_ids_from_exec = {
            _normalize_broker_order_id(r.get("OPEN_ORDER_ID"))
            or _normalize_broker_order_id(_parse_variant(r.get("PAYLOAD")).get("perm_id"))
            or _normalize_broker_order_id(_parse_variant(r.get("PAYLOAD")).get("order_id"))
            for r in execution_rows_ib
        }
        broker_ids_from_exec = {x for x in broker_ids_from_exec if x}
        if broker_ids_from_exec:
            placeholders = ",".join(["%s"] * len(broker_ids_from_exec))
            try:
                cur.execute(
                    f"""
                    select BROKER_ORDER_ID, ACTION_ID
                    from MIP.LIVE.LIVE_ORDERS
                    where PORTFOLIO_ID = %s
                      and BROKER_ORDER_ID in ({placeholders})
                    order by coalesce(LAST_UPDATED_AT, CREATED_AT) desc
                    """,
                    tuple([portfolio_id] + sorted(broker_ids_from_exec)),
                )
                for m in fetch_all(cur):
                    broker_id = _normalize_broker_order_id(m.get("BROKER_ORDER_ID"))
                    if broker_id:
                        broker_order_to_action.setdefault(broker_id, str(m.get("ACTION_ID") or ""))
            except Exception:
                broker_order_to_action = {}
        for order in orders:
            broker_id = _normalize_broker_order_id(order.get("BROKER_ORDER_ID"))
            if broker_id and broker_id not in broker_order_to_action:
                broker_order_to_action.setdefault(broker_id, str(order.get("ACTION_ID") or ""))

        position_history_by_symbol: dict[str, list[tuple[datetime, float, float | None]]] = {}
        try:
            cur.execute(
                """
                select upper(SYMBOL) as SYMBOL, POSITION_QTY, AVG_COST, SNAPSHOT_TS
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'POSITION'
                  and IBKR_ACCOUNT_ID = %s
                  and SNAPSHOT_TS >= dateadd(day, -%s, current_timestamp())
                order by upper(SYMBOL), SNAPSHOT_TS asc
                """,
                (account_id, order_lookback_days),
            )
            for b in fetch_all(cur):
                symbol_key = str(b.get("SYMBOL") or "").upper().strip()
                if not symbol_key:
                    continue
                snap_dt = _to_dt_utc(b.get("SNAPSHOT_TS"))
                if snap_dt is None:
                    continue
                qty_val = float(b.get("POSITION_QTY") or 0.0)
                cost_val = float(b.get("AVG_COST")) if b.get("AVG_COST") is not None else None
                position_history_by_symbol.setdefault(symbol_key, []).append((snap_dt, qty_val, cost_val))
        except Exception:
            position_history_by_symbol = {}

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
            realized_raw = payload.get("realized_pnl")
            if realized_raw is None:
                realized_raw = payload.get("realizedPNL")
            if realized_raw is None:
                realized_raw = row.get("REALIZED_PNL")
            realized_pnl = None
            if realized_raw is not None:
                try:
                    realized_pnl = float(realized_raw)
                except (TypeError, ValueError):
                    realized_pnl = None
            realized_pnl_is_estimate = False
            raw_commission = payload.get("commission")
            if raw_commission is None:
                raw_commission = payload.get("commissionReport")
            exec_commission = None
            if raw_commission is not None:
                try:
                    exec_commission = float(raw_commission)
                except (TypeError, ValueError):
                    exec_commission = None
            action_id = broker_order_to_action.get(broker_order_id) if broker_order_id else None
            action_meta = action_meta_by_id.get(str(action_id or "")) or {}
            execution_ts_dt = _to_dt_utc(execution_ts)
            basis_qty = 0.0
            basis_cost = None
            symbol_hist = position_history_by_symbol.get(symbol) or []
            if symbol_hist:
                basis_tuple = None
                if execution_ts_dt is not None:
                    cutoff = execution_ts_dt - timedelta(microseconds=1)
                    for cand in symbol_hist:
                        if cand[0] <= cutoff:
                            basis_tuple = cand
                        else:
                            break
                    if basis_tuple is None:
                        for cand in symbol_hist:
                            if cand[0] <= execution_ts_dt:
                                basis_tuple = cand
                            else:
                                break
                if basis_tuple is None:
                    basis_tuple = symbol_hist[-1]
                basis_qty = float(basis_tuple[1] or 0.0)
                basis_cost = basis_tuple[2]
            # Label fills from broker position *before* this execution (not from MIP ACTION_INTENT),
            # so wrong LIVE_ORDERS→ACTION linkage cannot turn an opening buy into "cover".
            execution_context = None
            action_intent = str(action_meta.get("action_intent") or "").upper()
            if side == "BUY" and basis_qty < 0:
                execution_context = "CLOSE_SHORT"
            elif side == "SELL" and basis_qty > 0:
                execution_context = "CLOSE_LONG"
            elif side == "BUY":
                execution_context = "OPEN_OR_ADD_LONG"
            elif side == "SELL":
                execution_context = "OPEN_OR_ADD_SHORT"
            # reqExecutions often yields commissionReport with realizedPNL=0 and commission=0 as placeholders
            # before IB finalizes the fill. Treat that as "unknown" for closes so we fall back to MIP estimate.
            _pnl_eps = 1e-9
            if execution_context in ("CLOSE_LONG", "CLOSE_SHORT") and realized_pnl is not None:
                comm_abs = abs(float(exec_commission)) if exec_commission is not None else 0.0
                if abs(float(realized_pnl)) < _pnl_eps and comm_abs < _pnl_eps:
                    realized_pnl = None
            # IB often omits realizedPNL until commissionReport is populated on the fill.
            # Estimate when still unknown after the placeholder strip above.
            if (
                realized_pnl is None
                and qty_filled is not None
                and avg_fill_price is not None
                and execution_context in ("CLOSE_LONG", "CLOSE_SHORT")
            ):
                try:
                    est_basis_qty = basis_qty
                    est_basis_cost = basis_cost
                    if est_basis_cost is None and execution_ts_dt is not None:
                        cur.execute(
                            """
                            select POSITION_QTY, AVG_COST
                            from MIP.LIVE.BROKER_SNAPSHOTS
                            where SNAPSHOT_TYPE = 'POSITION'
                              and IBKR_ACCOUNT_ID = %s
                              and upper(SYMBOL) = upper(%s)
                              and SNAPSHOT_TS <= %s
                              and coalesce(POSITION_QTY, 0) <> 0
                            order by SNAPSHOT_TS desc
                            limit 1
                            """,
                            (account_id, symbol, execution_ts_dt.replace(tzinfo=None)),
                        )
                        basis_rows = fetch_all(cur)
                        if basis_rows:
                            est_basis_qty = float((basis_rows[0] or {}).get("POSITION_QTY") or est_basis_qty)
                            if (basis_rows[0] or {}).get("AVG_COST") is not None:
                                est_basis_cost = float((basis_rows[0] or {}).get("AVG_COST"))
                    est_pnl = None
                    if est_basis_cost is not None:
                        if side == "BUY" and est_basis_qty < 0:
                            est_pnl = (est_basis_cost - float(avg_fill_price)) * float(qty_filled)
                        elif side == "SELL" and est_basis_qty > 0:
                            est_pnl = (float(avg_fill_price) - est_basis_cost) * float(qty_filled)
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
            # Match UI labels: only true closes (position was opposite sign before fill), not EXIT intent alone.
            close_like = execution_context in {"CLOSE_SHORT", "CLOSE_LONG"}
            pnl_fee_source = None
            if exec_commission and exec_commission > 0:
                pnl_fee_source = "ACTUAL_BROKER"
            elif realized_pnl is not None and not realized_pnl_is_estimate:
                pnl_fee_source = "IBKR_REALIZED_ON_EXECUTION"
            executions_ib.append(
                {
                    "order_id": exec_id or broker_order_id or f"IB_EXEC_{len(executions_ib)+1}",
                    "action_id": action_id,
                    "broker_order_id": broker_order_id,
                    "symbol": symbol,
                    "market_type": market_type or None,
                    "side": side,
                    "action_intent": action_intent or None,
                    "execution_context": execution_context,
                    "close_like": close_like,
                    "qty_filled": qty_filled,
                    "avg_fill_price": float(avg_fill_price) if avg_fill_price is not None else None,
                    "realized_pnl": float(realized_pnl) if realized_pnl is not None else None,
                    "realized_pnl_is_estimate": bool(realized_pnl_is_estimate),
                    "commission": exec_commission,
                    "fee_source": pnl_fee_source,
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
                    "close_like": _is_close_like_execution(
                        _normalize_action_intent(order.get("SIDE"), order.get("ACTION_INTENT")),
                        order.get("SIDE"),
                        order.get("SIDE"),
                    ),
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
        # Also dedupe on (broker_order_id, symbol) so MIP_BROKER_LEDGER rows do
        # not surface alongside IBKR rows that already carry execution_context
        # and P&L logic. Without this, a stale FILLED_AT (e.g. from a reconcile
        # backfill stamping current_timestamp) creates a phantom ledger row
        # with the wrong date and no P&L — the 2026-05-28 incident pattern.
        seen_broker_keys = {
            (
                str(e.get("broker_order_id") or "").strip(),
                str(e.get("symbol") or "").upper(),
            )
            for e in executions
            if e.get("broker_order_id")
        }
        for local_exec in executions_local:
            local_key = (
                f"{str(local_exec.get('broker_order_id') or '')}:{str(local_exec.get('symbol') or '').upper()}:"
                f"{str(local_exec.get('execution_ts') or '')}:{str(local_exec.get('qty_filled') or '')}"
            )
            if local_key in seen_combined:
                continue
            local_broker_id = str(local_exec.get("broker_order_id") or "").strip()
            local_symbol = str(local_exec.get("symbol") or "").upper()
            if local_broker_id and (local_broker_id, local_symbol) in seen_broker_keys:
                continue
            executions.append(local_exec)

        def _execution_sort_ts(e: dict) -> datetime:
            v = e.get("execution_ts")
            d = _to_dt_utc(v) if v else None
            if d is None:
                return datetime.min.replace(tzinfo=timezone.utc)
            return d

        executions.sort(key=_execution_sort_ts, reverse=True)
        executions = executions[:execution_limit]

        orders_enriched = []
        for ord_row in orders:
            action_key = str(ord_row.get("ACTION_ID") or "")
            broker_truth_active = _is_order_active_in_broker_truth(ord_row, broker_open_order_ids)
            symbol_upper = str(ord_row.get("SYMBOL") or "").upper()
            broker_truth_in_position = symbol_upper in held_symbols
            status_for_display = _live_order_display_status(ord_row, broker_open_order_ids)
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

        cockpit_mip_families = _build_mip_order_families_for_cockpit(
            orders_enriched,
            protection_by_action,
            order_groups,
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

        exit_warning_signals = []
        held_symbols = [str(p.get("SYMBOL") or "").upper() for p in open_positions if p.get("SYMBOL")]
        if held_symbols:
            try:
                placeholders = ", ".join(["%s"] * len(held_symbols))
                cur.execute(
                    f"""
                    with latest_ts as (
                        select max(TS) as TS
                        from MIP.APP.RECOMMENDATION_LOG
                        where INTERVAL_MINUTES = 1440
                    )
                    select
                        rl.SYMBOL,
                        rl.MARKET_TYPE,
                        cpd.PATTERN_TYPE,
                        cpd.NAME as PATTERN_NAME,
                        coalesce(rl.DETAILS:direction::string, 'N/A') as DIRECTION,
                        rl.SCORE,
                        rl.DETAILS:deviation_pct::float as DEVIATION_PCT,
                        rl.DETAILS:close_price::float as CLOSE_PRICE,
                        rl.DETAILS:vwap_proxy::float as VWAP_PROXY,
                        rl.TS as SIGNAL_TS
                    from MIP.APP.RECOMMENDATION_LOG rl
                    join latest_ts lt on rl.TS = lt.TS
                    join MIP.APP.PATTERN_DEFINITION cpd
                      on cpd.PATTERN_ID = rl.PATTERN_ID
                    where rl.INTERVAL_MINUTES = 1440
                      and rl.SYMBOL in ({placeholders})
                      and (cpd.PATTERN_TYPE = 'BEARISH_MOMENTUM'
                           or (cpd.PATTERN_TYPE = 'MEAN_REVERSION'
                               and coalesce(rl.DETAILS:direction::string, '') = 'BEARISH'))
                    order by rl.SCORE desc
                    """,
                    tuple(held_symbols),
                )
                exit_warning_signals = fetch_all(cur)
            except Exception:
                exit_warning_signals = []

        recon_v2_by_symbol: dict = {}
        if portfolio_id and account_id:
            try:
                from app.services.live_intelligence.broker_mirror import build_broker_mirror
                from app.services.live_intelligence.semantic_reconciliation_v2 import run_semantic_reconciliation
                broker_mirrors = build_broker_mirror(cur, str(account_id))
                _recon_map, _recon_meta = run_semantic_reconciliation(
                    cur, int(portfolio_id), str(account_id), broker_mirrors,
                )
                for sym, detail in _recon_map.items():
                    recon_v2_by_symbol[sym] = {
                        "status": detail.get("status", "UNKNOWN"),
                        "flags": detail.get("flags", []),
                        "position_aligned": detail.get("position_aligned"),
                        "protection_aligned": detail.get("protection_aligned"),
                        "orphan_orders": detail.get("orphan_orders", []),
                        "mismatches": detail.get("mismatches", []),
                    }
            except Exception:
                pass

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
                "drift_state_effective": drift_state_effective,
                "actionable": page_actionable,
                "blocking_reasons": [
                    reason
                    for reason, active in [
                        ("SNAPSHOT_STALE_OR_MISSING", snapshot_state in ("STALE", "BLOCKED")),
                        ("DRIFT_UNRESOLVED", drift_state == "BLOCKED"),
                        ("BROKER_RECONCILIATION_REQUIRED", reconciliation_state == "REQUIRED"),
                        ("OUTSIDE_OPERATING_HOURS", not market_open),
                    ]
                    if active
                ],
                "reconciliation_state": reconciliation_state,
                "unmapped_execution_count": unmapped_exec_count,
                "unmapped_execution_latest_ts": unmapped_exec_summary.get("latest_snapshot_ts"),
                "unmapped_execution_symbols": unmapped_exec_summary.get("symbols") or [],
                "unmapped_execution_sample_broker_order_ids": unmapped_exec_summary.get("sample_broker_order_ids") or [],
                "unmapped_on_flat_symbols_count": int(
                    unmapped_exec_summary.get("unmapped_on_flat_symbols_count") or 0
                ),
                "unmapped_total_before_flat_filter": int(
                    unmapped_exec_summary.get("unmapped_total_before_flat_filter") or 0
                ),
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
                "unmapped_execution_count": unmapped_exec_count,
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
                "live_structural_only": live_structural_only,
                "include_legacy_pending": include_legacy,
                "auto_import_latest_proposals": auto_import_summary,
                "live_intent_kinds": {
                    "STRUCTURAL": "Imported from STRUCTURAL_TRADE_PROPOSALS; structural committee only.",
                    "OPERATOR_EXIT": "Manual broker exit from Live Portfolio Activity; committee waived.",
                    "LEGACY_PATTERN": "Historical pattern-era import from ORDER_PROPOSALS — hidden from default view when LIVE_STRUCTURAL_ONLY is true.",
                    "UNKNOWN": "Unclassified legacy row — treat as non-operational; use debug include_legacy to inspect.",
                    "note": "Default overview shows STRUCTURAL and OPERATOR_EXIT only unless include_legacy=true.",
                },
            },
            "open_positions": serialize_rows(open_positions),
            "open_orders": serialize_rows(
                [{k: v for k, v in row.items() if str(k).upper() != "PAYLOAD"} for row in open_orders]
            ),
            "orders": serialize_rows(orders_enriched),
            "executions": serialize_rows(executions),
            "pending_decisions": serialize_rows(pending_decisions),
            "exit_warning_signals": serialize_rows(exit_warning_signals),
            "reconciliation_v2": recon_v2_by_symbol,
            "counts": {
                "pending_decisions": len(pending_decisions),
                "orders": len(orders),
                "executions": len(executions),
                "open_positions": len(open_positions),
                "open_orders": len(open_orders),
                "exit_warning_signals": len(exit_warning_signals),
            },
            "cockpit_ibkr": {
                "broker_order_clusters": cockpit_broker_clusters,
                "mip_order_families": cockpit_mip_families,
                "pending_decisions_count": len(pending_decisions),
            },
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "ib_host_diagnostics": _live_portfolio_ib_host_diagnostics(
                snapshot_state=str(snapshot_state or "BLOCKED"),
                latest_snapshot_ts=latest_snapshot_ts,
                threshold_sec=cfg.get("SNAPSHOT_FRESHNESS_THRESHOLD_SEC"),
            ),
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
            select PORTFOLIO_ID, IBKR_ACCOUNT_ID, BROKER_NAME, IBKR_ACCOUNT_MODE, VALIDITY_WINDOW_SEC
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
        broker_name = str(cfg.get("BROKER_NAME") or "IBKR").strip()
        broker_universe_type = str(cfg.get("IBKR_ACCOUNT_MODE") or "PAPER").strip()
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
            with latest_sync_ts as (
                select max(SNAPSHOT_TS) as SNAPSHOT_TS
                from MIP.LIVE.BROKER_SNAPSHOTS
                where IBKR_ACCOUNT_ID = %s
            )
            select s.POSITION_QTY, s.AVG_COST, s.SNAPSHOT_TS
            from MIP.LIVE.BROKER_SNAPSHOTS s
            join latest_sync_ts ls on s.SNAPSHOT_TS = ls.SNAPSHOT_TS
            where s.SNAPSHOT_TYPE = 'POSITION'
              and s.IBKR_ACCOUNT_ID = %s
              and upper(s.SYMBOL) = upper(%s)
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
            "live_committee_waived": True,
            "committee_waive_rationale": "Manual exit/cover from Live Portfolio Activity; PM/compliance/revalidate/execute only.",
        }
        cur.execute(
            """
            insert into MIP.LIVE.LIVE_ACTIONS (
              ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, BROKER_NAME, IBKR_ACCOUNT_ID, BROKER_UNIVERSE_TYPE,
              SYMBOL, SIDE, ACTION_INTENT, EXIT_TYPE, EXIT_REASON,
              PROPOSED_QTY, PROPOSED_PRICE, ASSET_CLASS, STATUS, VALIDITY_WINDOW_END, COMPLIANCE_STATUS,
              PARAM_SNAPSHOT, REASON_CODES, COMMITTEE_REQUIRED, COMMITTEE_STATUS, LIVE_INTENT_KIND, CREATED_AT, UPDATED_AT
            )
            select
              %s, null, %s, %s, %s, %s, %s, %s, 'EXIT', 'MANUAL', %s,
              %s, %s, null, 'READY_FOR_APPROVAL_FLOW', dateadd(second, %s, current_timestamp()), 'PENDING',
              parse_json(%s), parse_json(%s), false, 'SKIPPED', 'OPERATOR_EXIT', current_timestamp(), current_timestamp()
            """,
            (
                action_id,
                int(req.portfolio_id),
                broker_name,
                account_id,
                broker_universe_type,
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

    committee_required_flag = bool(action.get("COMMITTEE_REQUIRED")) if action.get("COMMITTEE_REQUIRED") is not None else True
    committee_result = None
    if committee_required_flag and status in (
        "OPEN_ELIGIBLE",
        "OPEN_CAUTION",
        "PENDING_OPEN_STABILITY_REVIEW",
        "READY_FOR_APPROVAL_FLOW",
    ):
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

    _assert_proposal_freshness_for_structural_entry(action)

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
                **_snapshot_sync_params_for_portfolio(action.get("PORTFOLIO_ID")),
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
        ExecuteLiveActionRequest(
            actor=req.execution_actor,
            attempt_n=req.attempt_n,
            portfolio_id=req.portfolio_id,
            ibkr_account_id=req.ibkr_account_id,
            broker_name=req.broker_name,
            broker_universe_type=req.broker_universe_type,
        ),
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


def _try_agentic_materializer_after_opening_clear(cur, action_id: str) -> dict | None:
    """Re-run agentic materialization once the opening guard clears.

    When the operator ran Agentic Review while the market was closed, the
    authority row may be OPERATOR_COMMITTED but LIVE_ACTIONS can remain
    OPEN_BLOCKED / COMMITTEE_STATUS=PENDING with null sizing. After opening
    validation passes, materialize so the row can advance to approval flow.
    """
    try:
        from app.committee.agentic_authority import (
            POSITIVE_AUTHORITY_STATUSES,
            is_agentic_primary_materialization_enabled,
        )
        from app.routers.agentic_authority_router import agentic_materializer_status_eligible
        from app.routers.committee import _underlying_sf_conn
    except Exception as imp_exc:  # noqa: BLE001
        _log.warning(
            "opening materializer: import failed action=%s: %s",
            action_id,
            imp_exc,
        )
        return None

    action_row = _fetch_live_action(cur, action_id)
    if not action_row:
        return None
    status = str(action_row.get("STATUS") or "").upper()
    if status == "OPEN_BLOCKED":
        return None

    eligible, eligibility_reason = agentic_materializer_status_eligible(dict(action_row))
    if not eligible:
        return {"ran": False, "reason": eligibility_reason, "status": status}

    cur.execute(
        """
        select
          AUTHORITY_ID, AUTHORITY_MODE, AUTHORITY_STATUS, AUTHORITY_CONFIDENCE,
          COMMITTED_BY, IS_STALE, SHADOW_SIZE_POSTURE
        from MIP.APP.V_AGENTIC_AUTHORITY_LATEST
        where ACTION_ID = %s
        limit 1
        """,
        (action_id,),
    )
    auth_rows = fetch_all(cur)
    if not auth_rows:
        return {"ran": False, "reason": "no_authority_row", "status": status}
    authority_row = dict(auth_rows[0])
    mode = str(authority_row.get("AUTHORITY_MODE") or "").upper()
    auth_status = str(authority_row.get("AUTHORITY_STATUS") or "").upper()
    if mode != "OPERATOR_COMMITTED" or auth_status not in POSITIVE_AUTHORITY_STATUSES:
        return {"ran": False, "reason": "authority_not_committed_approve", "status": status}
    if bool(authority_row.get("IS_STALE")):
        return {"ran": False, "reason": "authority_stale", "status": status}

    conn = cur.connection
    if not is_agentic_primary_materialization_enabled(conn):
        return {"ran": False, "reason": "flag_off", "status": status}

    recovery_late_stage = (
        eligibility_reason == "recovery_incomplete_contract"
        and status in ("REVALIDATED_PASS", "REVALIDATED_FAIL")
    )
    raw = _underlying_sf_conn(conn)
    raw.autocommit(False)
    try:
        out = _materialize_structural_entry_agentic_apply(
            cur,
            action_id,
            dict(action_row),
            authority_row,
            apply_detail_source="OPENING_CLEAR_AGENTIC_MATERIALIZE",
            recovery_late_stage=recovery_late_stage,
        )
        raw.commit()
        return {"ran": True, "status": (out or {}).get("status"), "materializer": out}
    except Exception as exc:  # noqa: BLE001
        raw.rollback()
        _log.warning(
            "opening materializer: failed action=%s: %s",
            action_id,
            exc,
        )
        return {"ran": False, "reason": "materializer_error", "error": str(exc), "status": status}
    finally:
        raw.autocommit(True)


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
        materializer_result = None
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
        elif action_after and str(action_after.get("STATUS") or "").upper() != "OPEN_BLOCKED":
            materializer_result = _try_agentic_materializer_after_opening_clear(cur, action_id)
            action_after = _fetch_live_action(cur, action_id)
        next_status = str(action_after.get("STATUS") or gate.get("result") or "").upper()
        return {
            "ok": True,
            "action_id": action_id,
            "status": action_after.get("STATUS") if action_after else gate.get("result"),
            "opening_result": gate.get("result"),
            "reason_codes": gate.get("reason_codes") or [],
            "opening_validation": gate.get("opening_validation") or {},
            "agentic_materializer": materializer_result,
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

        # When the operator explicitly forces a re-run from LPA (revalidate /
        # "Re-run hearing"), pull a fresh IBKR 1-minute bar so the committee
        # sees the live tape — without this, _live_context() can only read the
        # latest persisted MARKET_BARS row, which may be minutes/hours old and
        # produces deterministic verdicts that never update across re-runs.
        if req.force_rerun and _is_extended_trading_open_ny(datetime.now(timezone.utc)):
            try:
                _force_refresh_latest_one_minute_bars(
                    cur,
                    action.get("SYMBOL"),
                    portfolio_id=int(action["PORTFOLIO_ID"]) if action.get("PORTFOLIO_ID") is not None else None,
                )
            except Exception as exc:
                logger.warning(
                    "force_rerun 1m refresh failed for %s: %s",
                    action.get("SYMBOL"),
                    exc,
                )

        _structural_only_co = _live_structural_only_enabled(cur)
        assert_live_committee_policy(
            action,
            _structural_only_co,
            is_structural_fn=is_structural_live_action,
        )

        _committee_action_intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
        _committee_is_exit = _committee_action_intent == "EXIT"
        _committee_model_run = (
            "STRUCTURAL_EXIT_EXECUTION_ONLY"
            if is_structural_live_action(action) and _committee_is_exit
            else ("STRUCTURAL_V1" if is_structural_live_action(action) else "LEGACY_MULTI_AGENT")
        )

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
                        "committee_model": _committee_model_run,
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

        is_structural = is_structural_live_action(action)
        if is_structural:
            context = {
                "entry_intel_baseline": {},
                "parallel_worlds_evidence": {},
                "news_context_snapshot": {},
                "entry_intel_snapshot_id": None,
                "training_qualification_snapshot": {},
                "target_expectation_snapshot": {},
                "action_news_context_snapshot": {},
                "latest_symbol_news_context": {},
                "news_for_decision_source": None,
            }
            action_training_snapshot = {}
            action_target_snapshot = {}
            decision_news_snapshot = {}
            action_news_snapshot = {}
            latest_news_snapshot = {}
            pw_evidence = {}
        else:
            context = _build_action_decision_context(cur, action)
            action_training_snapshot = context.get("training_qualification_snapshot") or {}
            action_target_snapshot = context.get("target_expectation_snapshot") or {}
            decision_news_snapshot = context.get("news_context_snapshot") or {}
            action_news_snapshot = context.get("action_news_context_snapshot") or {}
            latest_news_snapshot = context.get("latest_symbol_news_context") or {}
            pw_evidence = context.get("parallel_worlds_evidence")

        action_intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
        is_exit = action_intent == "EXIT"
        exit_position_qty = None
        exit_override_applied = False

        if is_structural:
            # ── Structural path: Committee 2.0 (ENTRY) or execution-only broker gate (EXIT) ──
            if is_exit:
                exit_position_qty = _fetch_live_symbol_position_qty(
                    cur, action.get("PORTFOLIO_ID"), action.get("SYMBOL")
                )
                structural_verdict = build_structural_exit_execution_only_verdict(
                    action, exit_position_qty=exit_position_qty
                )
            else:
                # Stage 4f — block structural ENTRY through this legacy route
                # when agentic-primary materialization is on. The route's
                # remaining job (writing COMMITTEE_RUN/VERDICT and updating
                # LIVE_ACTIONS.STATUS) is exactly what Stage 4f forbids.
                if _read_agentic_primary_flag_via_cursor(cur):
                    _log.error(
                        "STAGE_4F_GUARD run_live_trade_committee blocked deterministic "
                        "materialize for structural ENTRY action_id=%s.",
                        action_id,
                    )
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": (
                                "Deterministic committee run is disabled for structural "
                                "ENTRY while agentic-primary materialization is on. Use "
                                "POST /live/trades/actions/{id}/committee2/orchestrate "
                                "to refresh the hearing diagnostically, then commit "
                                "agentic authority."
                            ),
                            "reason_codes": [
                                "DETERMINISTIC_RUN_DISABLED_AGENTIC_PRIMARY",
                                "STAGE_4F_NO_C2_FALLBACK",
                            ],
                            "action_id": action_id,
                        },
                    )
                fd_row = fetch_committee2_final_decision_for_action(cur, action_id)
                if not fd_row:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": "Committee 2.0 final decision required — commit the hearing with this action_id (POST /committee/hearing/{hearing_id}/commit).",
                            "reason_codes": ["COMMITTEE2_FINAL_DECISION_REQUIRED"],
                        },
                    )
                ap = action.get("PROPOSAL_ID")
                if ap is not None and fd_row.get("PROPOSAL_ID") is not None:
                    if int(ap) != int(fd_row["PROPOSAL_ID"]):
                        raise HTTPException(
                            status_code=409,
                            detail={
                                "message": "COMMITTEE_FINAL_DECISION.PROPOSAL_ID does not match LIVE_ACTIONS.PROPOSAL_ID.",
                                "reason_codes": ["COMMITTEE2_ACTION_PROPOSAL_MISMATCH"],
                            },
                        )
                structural_verdict = structural_entry_verdict_from_committee2_final(fd_row, action)
            outputs = structural_verdict.pop("evaluations", {})
            verdict = structural_verdict
            role_outputs_list = outputs.get("freshness", {}), outputs.get("trust", {}), outputs.get("regime", {}), outputs.get("path_quality", {}), outputs.get("trail", {})

            if not is_exit:
                for eval_data in role_outputs_list:
                    role_name = {
                        id(outputs.get("freshness", {})): "StructuralValidator",
                        id(outputs.get("trust", {})): "TrustGatekeeper",
                        id(outputs.get("regime", {})): "RegimeAssessor",
                        id(outputs.get("path_quality", {})): "PathAnalyst",
                        id(outputs.get("trail", {})): "ProtectionAdvisor",
                    }.get(id(eval_data), "Unknown")
                    try:
                        cur.execute(
                            """
                            INSERT INTO MIP.LIVE.COMMITTEE_ROLE_OUTPUT (
                              RUN_ID, ROLE_NAME, STANCE, CONFIDENCE, SUMMARY, OUTPUT_JSON, CREATED_AT
                            )
                            SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s), CURRENT_TIMESTAMP()
                            """,
                            (
                                run_id,
                                role_name,
                                str(eval_data.get("passed", eval_data.get("freshness", "UNKNOWN"))).upper()[:20],
                                eval_data.get("confidence", eval_data.get("size_mult", 0.5)),
                                str(eval_data.get("reason", eval_data.get("note", "")))[:500],
                                json.dumps(eval_data, default=str),
                            ),
                        )
                    except Exception:
                        pass

            reason_codes = list(verdict.get("reason_codes") or [])
            if not is_exit:
                reason_codes.append("STRUCTURAL_COMMITTEE_REVIEWED")

            outputs = [{"structural_evaluations": outputs}]

        else:
            # ── Legacy committee path ─────────────────────────────────
            outputs, verdict = _run_multiagent_dialogue(
                cur,
                model=req.model,
                context=context,
                persist_run_id=run_id,
                emit=None,
                live_action=action,
            )
            jd = _parse_variant(verdict.get("joint_decision"))
            if not verdict.get("blocked") and (
                jd.get("realistic_target_return") is None
                or jd.get("stop_loss_pct") is None
                or jd.get("hold_bars") is None
                or jd.get("acceptable_early_exit_target_return") is None
            ):
                verdict = _backfill_joint_decision_from_policy(verdict, context)

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
        if is_structural:
            tier_c_conflict = False
        else:
            if verdict.get("quality_block_override_applied"):
                reason_codes.append("COMMITTEE_BLOCK_SUPPRESSED_QUALITY_DEGRADED")
            if verdict.get("quality_risk_normalized"):
                reason_codes.append("COMMITTEE_RISK_NORMALIZED_FOR_EXECUTION")
            tier_c_conflict = _has_tier_c_conflict(verdict, pw_evidence, decision_news_snapshot)
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
            committee_size_factor = float(verdict.get("size_factor") or 1.0)
            nav_eur = 0.0
            max_position_pct = None
            cur.execute(
                """
                SELECT c.IBKR_ACCOUNT_ID, c.MAX_POSITION_PCT, s.NET_LIQUIDATION_EUR
                FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG c
                LEFT JOIN MIP.LIVE.BROKER_SNAPSHOTS s
                  ON s.IBKR_ACCOUNT_ID = c.IBKR_ACCOUNT_ID AND s.SNAPSHOT_TYPE = 'NAV'
                WHERE c.PORTFOLIO_ID = %s
                QUALIFY ROW_NUMBER() OVER (PARTITION BY c.PORTFOLIO_ID ORDER BY s.SNAPSHOT_TS DESC NULLS LAST) = 1
                """,
                (action.get("PORTFOLIO_ID"),),
            )
            nav_rows = fetch_all(cur)
            if nav_rows:
                nav_eur = float((nav_rows[0] or {}).get("NET_LIQUIDATION_EUR") or 0.0)
                max_position_pct = (nav_rows[0] or {}).get("MAX_POSITION_PCT")

            if is_structural and proposed_price_derived and nav_eur > 0:
                pos_pct = float(max_position_pct or 0.05)
                max_notional = nav_eur * pos_pct * committee_size_factor
                proposed_qty_derived = max(int(max_notional / max(proposed_price_derived, 1e-9)), 1)
            elif proposed_price_derived and nav_eur > 0:
                param_snapshot = _parse_variant(action.get("PARAM_SNAPSHOT"))
                target_weight = param_snapshot.get("target_weight")
                target_weight_abs = abs(float(target_weight)) if target_weight is not None else None
                training_size_cap = float(action.get("TRAINING_SIZE_CAP_FACTOR") or 1.0)
                open_factor = float(action.get("TARGET_OPEN_CONDITION_FACTOR") or 1.0)
                effective_weight = None
                if target_weight_abs is not None:
                    effective_weight = target_weight_abs * committee_size_factor * training_size_cap * open_factor
                if effective_weight is not None and effective_weight > 0:
                    est_notional = nav_eur * effective_weight
                    proposed_qty_derived = max(int(est_notional / max(proposed_price_derived, 1e-9)), 1)
        except Exception:
            proposed_qty_derived = None

        proposed_qty_derived, reason_codes = _apply_post_committee_entry_viability_and_qty(
            cur,
            action_id=action_id,
            portfolio_id=int(action.get("PORTFOLIO_ID") or 0),
            side=str(action.get("SIDE") or "").upper(),
            is_exit=is_exit,
            is_committee_blocked=bool(verdict.get("blocked")),
            proposed_price=proposed_price_derived,
            committee_qty=proposed_qty_derived,
            joint_decision=_parse_variant(verdict.get("joint_decision")),
            reason_codes=reason_codes,
        )

        verdict_phase3 = _committee_alpha_phase3_envelope(
            context,
            verdict,
            outputs,
            action_intent=action_intent,
            manual_apply=False,
            reason_codes=reason_codes,
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
                json.dumps(
                    {
                        **{
                            "verdict": verdict,
                            "outputs": outputs,
                            "joint_decision": verdict.get("joint_decision"),
                            "entry_intel_snapshot_id": context.get("entry_intel_snapshot_id"),
                        },
                        **verdict_phase3,
                    }
                ),
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
        if is_structural:
            _merge_structural_contract_and_diagnostics(
                cur,
                action_id=action_id,
                committee_run_id=run_id,
                verdict=verdict,
                reason_codes=reason_codes,
            )
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
                "structural_committee": bool(is_structural),
                "news_context_state": (decision_news_snapshot or {}).get("context_state"),
                "news_event_shock_flag": bool((decision_news_snapshot or {}).get("event_shock_flag")),
                "news_for_decision_source": context.get("news_for_decision_source"),
                "news_fallback_mode": "RSS_FALLBACK" if news_fallback_active else "IBKR_PRIMARY",
                "ibkr_news_reason_codes": news_readiness.get("reason_codes") or [],
            },
            outcome_state={
                "roles": COMMITTEE_ROLES,
                "training_qualification_snapshot": action_training_snapshot,
                "target_expectation_snapshot": action_target_snapshot,
                "news_context_snapshot": decision_news_snapshot,
                "action_news_context_snapshot": action_news_snapshot,
                "latest_symbol_news_context": latest_news_snapshot,
                "news_for_decision_source": context.get("news_for_decision_source"),
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


def _read_agentic_primary_flag_via_cursor(cur) -> bool:
    """Stage 4f helper — read AGENTIC_PRIMARY_MATERIALIZATION_ENABLED using a
    cursor we already hold. Returns True (fail-closed) if the row is missing
    or unreadable. Mirrors `should_block_deterministic_materialization` but
    does not require a connection handle."""
    try:
        cur.execute(
            "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
            ("AGENTIC_PRIMARY_MATERIALIZATION_ENABLED",),
        )
        row = cur.fetchone()
        if not row:
            _log.warning(
                "Stage 4f flag row missing in APP_CONFIG — failing closed (blocking C2 materialize)."
            )
            return True
        v = row[0]
        if v is None:
            return True
        s = str(v).strip().lower()
        return s in {"true", "t", "1", "yes", "y", "on"}
    except Exception:  # noqa: BLE001
        _log.exception(
            "Stage 4f flag read failed — failing closed (blocking C2 materialize)."
        )
        return True


def _materialize_structural_entry_committee_apply(
    cur,
    action_id: str,
    action: dict,
    req: ApplyCommitteeVerdictRequest,
    *,
    apply_detail_source: str = "STREAM_APPLY",
) -> dict:
    """
    Persist COMMITTEE_RUN / COMMITTEE_VERDICT / LIVE_ACTIONS for structural ENTRY after
    COMMITTEE_FINAL_DECISION exists for action_id.

    Stage 4f — hard guard: when `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED`
    is truthy (or unreadable), this function must NEVER materialize. It
    refuses with a 409 carrying `DETERMINISTIC_MATERIALIZATION_DISABLED_AGENTIC_PRIMARY`
    so no caller — orchestrate, `/committee/apply`, streaming finalize, or
    any future call site — can silently fall back to the deterministic path.
    """
    # Stage 4f circuit breaker. Fail-CLOSED on config read errors.
    _block = _read_agentic_primary_flag_via_cursor(cur)
    if _block:
        _log.error(
            "STAGE_4F_GUARD_BLOCKED_DETERMINISTIC_MATERIALIZE action_id=%s "
            "source=%s — AGENTIC_PRIMARY_MATERIALIZATION_ENABLED is on (or "
            "unreadable). The agentic-primary materializer is the only legal "
            "path to LIVE_ACTIONS.STATUS.",
            action_id, apply_detail_source,
        )
        raise HTTPException(
            status_code=409,
            detail={
                "message": (
                    "Deterministic committee materialization is disabled while "
                    "agentic-primary materialization is the operating mode. "
                    "Run agentic review and commit the verdict via the agentic "
                    "authority endpoint to advance LIVE_ACTIONS."
                ),
                "reason_codes": [
                    "DETERMINISTIC_MATERIALIZATION_DISABLED_AGENTIC_PRIMARY",
                    "STAGE_4F_NO_C2_FALLBACK",
                ],
                "action_id": action_id,
                "apply_detail_source": apply_detail_source,
            },
        )

    fd_apply = fetch_committee2_final_decision_for_action(cur, action_id)
    if not fd_apply:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Committee 2.0 final decision required — commit the hearing with this action_id first.",
                "reason_codes": ["COMMITTEE2_FINAL_DECISION_REQUIRED"],
            },
        )
    ap = action.get("PROPOSAL_ID")
    if ap is not None and fd_apply.get("PROPOSAL_ID") is not None:
        if int(ap) != int(fd_apply["PROPOSAL_ID"]):
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "COMMITTEE_FINAL_DECISION.PROPOSAL_ID does not match LIVE_ACTIONS.PROPOSAL_ID.",
                    "reason_codes": ["COMMITTEE2_ACTION_PROPOSAL_MISMATCH"],
                },
            )
    struct_result = structural_entry_verdict_from_committee2_final(fd_apply, dict(action))
    blocked = bool(struct_result.get("blocked"))
    recommendation = str(struct_result.get("recommendation") or "BLOCK").upper()
    size_factor = float(struct_result.get("size_factor") or 0.0)
    confidence = float(struct_result.get("confidence") or 0.5)
    jd = struct_result.get("joint_decision") or {}
    reason_codes = list(struct_result.get("reason_codes") or [])
    reason_codes.append("STRUCTURAL_COMMITTEE_REVIEWED")
    verdict = {
        "recommendation": recommendation,
        "size_factor": max(0.0, min(1.0, size_factor)),
        "confidence": max(0.0, min(1.0, confidence)),
        "blocked": blocked,
        "joint_decision": jd,
        "structural_source": True,
        "tier_c_conflict": False,
    }
    next_status = "OPEN_BLOCKED" if verdict["blocked"] else "READY_FOR_APPROVAL_FLOW"
    context: dict = {"entry_intel_baseline": {}, "parallel_worlds_evidence": {}, "news_context_snapshot": {}}
    is_exit = False

    proposed_price_derived = _fetch_ibkr_mart_reference_close(cur, action.get("SYMBOL"))
    if proposed_price_derived is None and not is_exit:
        for key in ("REVALIDATION_PRICE", "PROPOSED_PRICE", "CURRENT_PRICE", "ONE_MIN_BAR_CLOSE"):
            v = action.get(key)
            if v is not None:
                try:
                    proposed_price_derived = float(v)
                    break
                except (TypeError, ValueError):
                    pass
        if proposed_price_derived is None and action.get("ENTRY_ZONE_LOW") is not None and action.get("ENTRY_ZONE_HIGH") is not None:
            try:
                proposed_price_derived = (
                    float(action["ENTRY_ZONE_LOW"]) + float(action["ENTRY_ZONE_HIGH"])
                ) / 2.0
            except (TypeError, ValueError):
                pass
    proposed_qty_derived = None

    try:
        committee_size_factor = float(verdict.get("size_factor") or 1.0)
        training_size_cap = float(action.get("TRAINING_SIZE_CAP_FACTOR") or 1.0)
        open_factor = float(action.get("TARGET_OPEN_CONDITION_FACTOR") or 1.0)
        if proposed_price_derived is None:
            raise ValueError("no reference price for sizing")

        cur.execute(
            """
            select
              c.IBKR_ACCOUNT_ID,
              c.MAX_POSITION_PCT,
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
            max_position_pct = (nav_rows[0] or {}).get("MAX_POSITION_PCT")
            pos_pct = float(max_position_pct or 0.05)
            max_notional = (
                nav_eur * pos_pct * committee_size_factor * training_size_cap * open_factor
            )
            proposed_qty_derived = max(int(max_notional / max(proposed_price_derived, 1e-9)), 1)
    except Exception:
        proposed_qty_derived = None

    proposed_qty_derived, reason_codes = _apply_post_committee_entry_viability_and_qty(
        cur,
        action_id=action_id,
        portfolio_id=int(action.get("PORTFOLIO_ID") or 0),
        side=str(action.get("SIDE") or "").upper(),
        is_exit=is_exit,
        is_committee_blocked=bool(verdict.get("blocked")),
        proposed_price=proposed_price_derived,
        committee_qty=proposed_qty_derived,
        joint_decision=_parse_variant(verdict.get("joint_decision")),
        reason_codes=reason_codes,
    )

    verdict_phase3_apply = build_structural_verdict_envelope_v1(
        action=action,
        verdict=verdict,
        reason_codes=reason_codes,
        committee_run_id="",
        outputs_wrapper=[],
    )

    run_id = str(uuid.uuid4())
    if isinstance(verdict_phase3_apply.get("structural_verdict_envelope_v1"), dict):
        verdict_phase3_apply["structural_verdict_envelope_v1"]["committee_run_id"] = run_id
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
            json.dumps({"actor": req.actor, "source": apply_detail_source}),
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
            json.dumps(
                {
                    **{
                        "verdict": verdict,
                        "joint_decision": verdict.get("joint_decision"),
                        "entry_intel_snapshot_id": context.get("entry_intel_snapshot_id"),
                    },
                    **verdict_phase3_apply,
                }
            ),
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
    _merge_structural_contract_and_diagnostics(
        cur,
        action_id=action_id,
        committee_run_id=run_id,
        verdict=verdict,
        reason_codes=reason_codes,
    )
    return {
        "ok": True,
        "action_id": action_id,
        "run_id": run_id,
        "status": "COMPLETED",
        "action_status": next_status,
        "verdict": verdict,
        "joint_decision": verdict.get("joint_decision"),
        "reason_codes": reason_codes,
        "derived_sizing": {"proposed_price": proposed_price_derived, "proposed_qty": proposed_qty_derived},
    }


# ---------------------------------------------------------------------------
# Stage 4e — Agentic-primary materializer
# ---------------------------------------------------------------------------

# Maps the canonical AUTHORITY_STATUS values to the Stage 4e materializer
# outcome. PROCEED moves the action to READY_FOR_APPROVAL_FLOW with sized
# legs. BLOCK keeps the action at OPEN_BLOCKED with no executable sizing.
_AGENTIC_AUTHORITY_TO_OUTCOME: dict[str, dict] = {
    "AGENTIC_APPROVE":              {"recommendation": "PROCEED", "size_factor": 1.0, "blocked": False},
    "AGENTIC_APPROVE_REDUCED":      {"recommendation": "PROCEED_REDUCED", "size_factor": 0.5, "blocked": False},
    "AGENTIC_WAIT_RECLAIM":         {"recommendation": "BLOCK", "size_factor": 0.0, "blocked": True},
    "AGENTIC_DEFER":                {"recommendation": "BLOCK", "size_factor": 0.0, "blocked": True},
    "AGENTIC_REJECT":               {"recommendation": "BLOCK", "size_factor": 0.0, "blocked": True},
    "AGENTIC_DEGRADED_NO_AUTHORITY":{"recommendation": "BLOCK", "size_factor": 0.0, "blocked": True},
    "AGENTIC_FAILED_NO_AUTHORITY":  {"recommendation": "BLOCK", "size_factor": 0.0, "blocked": True},
}


def _agentic_outcome_from_authority(authority_row: dict) -> dict:
    """Map an AGENTIC_REVALIDATION_AUTHORITY row to a Stage 4e materializer
    outcome. Honors IS_STALE: a stale row always blocks regardless of status.

    Returns a dict with: recommendation, size_factor, blocked, status_code.
    """
    status = str(authority_row.get("AUTHORITY_STATUS") or "").upper()
    is_stale = bool(authority_row.get("IS_STALE"))
    base = _AGENTIC_AUTHORITY_TO_OUTCOME.get(status)
    if base is None:
        return {"recommendation": "BLOCK", "size_factor": 0.0, "blocked": True, "status_code": status or "UNKNOWN"}
    if is_stale and not base["blocked"]:
        return {"recommendation": "BLOCK", "size_factor": 0.0, "blocked": True, "status_code": "STALE"}
    posture = str(authority_row.get("SHADOW_SIZE_POSTURE") or "").upper()
    size_factor = float(base["size_factor"])
    # Treat explicit FULL posture as 1.0 regardless of REDUCED status, and
    # explicit NONE / REDUCED postures as their literal meaning when the
    # status itself is APPROVE (defensive — should not happen but easy to
    # express).
    if status == "AGENTIC_APPROVE" and posture == "REDUCED":
        size_factor = 0.5
    elif status == "AGENTIC_APPROVE_REDUCED" and posture == "FULL":
        size_factor = 1.0
    return {
        "recommendation": base["recommendation"],
        "size_factor": size_factor,
        "blocked": bool(base["blocked"]),
        "status_code": status,
    }


def _materialize_structural_entry_agentic_apply(
    cur,
    action_id: str,
    action: dict,
    authority_row: dict,
    *,
    apply_detail_source: str = "AGENTIC_APPLY",
    recovery_late_stage: bool = False,
) -> dict:
    """Stage 4e agentic-primary materializer.

    Called after the operator-commit endpoint writes an OPERATOR_COMMITTED
    authority row. Replaces `_materialize_structural_entry_committee_apply`
    as the source of truth for LIVE_ACTIONS.STATUS / PROPOSED_PRICE /
    PROPOSED_QTY / REASON_CODES whenever AGENTIC_PRIMARY_MATERIALIZATION_ENABLED
    is true.

    Behavior:

    * Maps AUTHORITY_STATUS + IS_STALE to a PROCEED / BLOCK outcome via
      `_agentic_outcome_from_authority`.
    * Pulls the most recent COMMITTEE_FINAL_DECISION for the joint_decision
      structural context (TP/SL, journey baselines) — C2 still writes this
      table in Stage 4e for history/position health, so it remains a valid
      structural anchor.
    * Computes notional sizing from LIVE_PORTFOLIO_CONFIG + BROKER_SNAPSHOTS
      (NAV), gated by MAX_POSITION_PCT and the agentic size_factor.
    * Reuses `_apply_post_committee_entry_viability_and_qty` so IB risk
      gates / min-notional / bracket calibration behave identically to the
      C2 path.
    * Writes a COMMITTEE_RUN + COMMITTEE_VERDICT row tagged with
      MODEL_NAME='AGENTIC_AUTHORITY_v1' so the LPA verdict trail keeps
      rendering. Per design these writes are diagnostic — the operational
      truth lives in AGENTIC_REVALIDATION_AUTHORITY.
    * Updates LIVE_ACTIONS.STATUS to READY_FOR_APPROVAL_FLOW (PROCEED) or
      OPEN_BLOCKED (BLOCK).
    """
    outcome = _agentic_outcome_from_authority(authority_row)
    size_factor = max(0.0, min(1.0, float(outcome["size_factor"])))
    blocked = bool(outcome["blocked"])
    recommendation = outcome["recommendation"]
    confidence_raw = authority_row.get("AUTHORITY_CONFIDENCE")
    try:
        confidence = max(0.0, min(1.0, float(confidence_raw))) if confidence_raw is not None else 0.5
    except (TypeError, ValueError):
        confidence = 0.5

    # Stage 1 short submit safety: seed TP/SL from structural action fields via the
    # canonical protection builder (same helper as C2 bridge execute self-heal).
    # Agentic authority still owns thesis proceed/block; bracket math is deterministic.
    struct_jd = build_structural_entry_joint_decision(dict(action))
    struct_jd["should_enter"] = not blocked
    struct_jd["agentic_source"] = True
    struct_jd["agentic_authority_status"] = outcome["status_code"]
    if outcome["status_code"] == "AGENTIC_APPROVE_REDUCED":
        struct_jd["size_factor"] = size_factor
        struct_jd["position_size_factor"] = size_factor
    jd: dict = struct_jd

    reason_codes: list[str] = [
        "STRUCTURAL_AGENTIC_REVIEWED",
        f"AGENTIC_AUTHORITY_{outcome['status_code']}",
    ]
    if bool(authority_row.get("IS_STALE")):
        reason_codes.append("AGENTIC_AUTHORITY_STALE")
    if outcome["status_code"] == "AGENTIC_APPROVE_REDUCED":
        reason_codes.append("AGENTIC_SIZE_POSTURE_REDUCED")

    verdict = {
        "recommendation": recommendation,
        "size_factor": size_factor,
        "confidence": confidence,
        "blocked": blocked,
        "joint_decision": jd,
        "structural_source": True,
        "tier_c_conflict": False,
        "agentic_source": True,
        "authority_id": authority_row.get("AUTHORITY_ID"),
        "authority_status": outcome["status_code"],
    }
    prior_status = str(action.get("STATUS") or "").upper()
    if recovery_late_stage and prior_status in ("REVALIDATED_PASS", "REVALIDATED_FAIL"):
        next_status = prior_status
    else:
        next_status = "OPEN_BLOCKED" if blocked else "READY_FOR_APPROVAL_FLOW"
    is_exit = False

    proposed_price_derived = _fetch_ibkr_mart_reference_close(cur, action.get("SYMBOL"))
    if proposed_price_derived is None and not is_exit:
        for key in ("REVALIDATION_PRICE", "PROPOSED_PRICE", "CURRENT_PRICE", "ONE_MIN_BAR_CLOSE"):
            v = action.get(key)
            if v is not None:
                try:
                    proposed_price_derived = float(v)
                    break
                except (TypeError, ValueError):
                    pass
        if proposed_price_derived is None and action.get("ENTRY_ZONE_LOW") is not None and action.get("ENTRY_ZONE_HIGH") is not None:
            try:
                proposed_price_derived = (
                    float(action["ENTRY_ZONE_LOW"]) + float(action["ENTRY_ZONE_HIGH"])
                ) / 2.0
            except (TypeError, ValueError):
                pass
    proposed_qty_derived: float | None = None
    seed_executable_bracket = (not blocked) or recovery_late_stage

    if seed_executable_bracket and proposed_price_derived is not None:
        try:
            training_size_cap = float(action.get("TRAINING_SIZE_CAP_FACTOR") or 1.0)
            open_factor = float(action.get("TARGET_OPEN_CONDITION_FACTOR") or 1.0)
            cur.execute(
                """
                select
                  c.IBKR_ACCOUNT_ID,
                  c.MAX_POSITION_PCT,
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
            if nav_eur > 0 and not blocked:
                pos_pct = float((nav_rows[0] or {}).get("MAX_POSITION_PCT") or 0.05)
                max_notional = (
                    nav_eur * pos_pct * size_factor * training_size_cap * open_factor
                )
                proposed_qty_derived = max(int(max_notional / max(proposed_price_derived, 1e-9)), 1)
        except Exception:  # noqa: BLE001 — sizing best-effort
            proposed_qty_derived = None

    if seed_executable_bracket and proposed_qty_derived is None:
        try:
            pq = action.get("PROPOSED_QTY")
            if pq is not None and float(pq) > 0:
                proposed_qty_derived = float(pq)
        except (TypeError, ValueError):
            pass

    proposed_qty_derived, reason_codes = _apply_post_committee_entry_viability_and_qty(
        cur,
        action_id=action_id,
        portfolio_id=int(action.get("PORTFOLIO_ID") or 0),
        side=str(action.get("SIDE") or "").upper(),
        is_exit=is_exit,
        is_committee_blocked=not seed_executable_bracket,
        proposed_price=proposed_price_derived,
        committee_qty=proposed_qty_derived,
        joint_decision=jd,
        reason_codes=reason_codes,
    )

    verdict["joint_decision"] = jd
    if seed_executable_bracket:
        bracket_block = any(_is_entry_bracket_hard_block_code(rc) for rc in reason_codes)
        if not bracket_block:
            refreshed = _fetch_live_action(cur, action_id)
            ps_ref = _parse_variant((refreshed or {}).get("PARAM_SNAPSHOT"))
            eb_ref = ps_ref.get("executable_bracket") if isinstance(ps_ref, dict) else None
            try:
                tr_ok = jd.get("realistic_target_return") is not None and float(jd["realistic_target_return"]) > 0
                sl_ok = jd.get("stop_loss_pct") is not None and float(jd["stop_loss_pct"]) > 0
            except (TypeError, ValueError):
                tr_ok = sl_ok = False
            eb_ok = (
                isinstance(eb_ref, dict)
                and eb_ref.get("target_return") is not None
                and eb_ref.get("stop_loss_pct") is not None
                and not eb_ref.get("blocked")
            )
            if tr_ok and sl_ok and not eb_ok:
                _seed_executable_bracket_from_joint_decision(cur, action_id, jd)
                refreshed = _fetch_live_action(cur, action_id)
                ps_ref = _parse_variant((refreshed or {}).get("PARAM_SNAPSHOT"))
                eb_ref = ps_ref.get("executable_bracket") if isinstance(ps_ref, dict) else None
                eb_ok = (
                    isinstance(eb_ref, dict)
                    and eb_ref.get("target_return") is not None
                    and eb_ref.get("stop_loss_pct") is not None
                    and not eb_ref.get("blocked")
                )
            if not (tr_ok and sl_ok and eb_ok):
                bracket_block = True
                if "STRUCT_SUBMIT_CONTRACT_INCOMPLETE" not in reason_codes:
                    reason_codes.append("STRUCT_SUBMIT_CONTRACT_INCOMPLETE")
            elif bracket_block:
                reason_codes = _strip_recomputable_entry_bracket_codes(reason_codes)
                bracket_block = any(_is_entry_bracket_hard_block_code(rc) for rc in reason_codes)
        if bracket_block and not recovery_late_stage:
            blocked = True
            next_status = "OPEN_BLOCKED"
            verdict["blocked"] = True
            verdict["recommendation"] = "BLOCK"
        elif bracket_block and recovery_late_stage and not blocked:
            if "STRUCT_SUBMIT_CONTRACT_INCOMPLETE" not in reason_codes:
                reason_codes.append("STRUCT_SUBMIT_CONTRACT_INCOMPLETE")

    verdict_envelope = build_structural_verdict_envelope_v1(
        action=action,
        verdict=verdict,
        reason_codes=reason_codes,
        committee_run_id="",
        outputs_wrapper=[],
    )

    run_id = str(uuid.uuid4())
    if isinstance(verdict_envelope.get("structural_verdict_envelope_v1"), dict):
        verdict_envelope["structural_verdict_envelope_v1"]["committee_run_id"] = run_id
        verdict_envelope["structural_verdict_envelope_v1"]["agentic_source"] = True

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
            "AGENTIC_AUTHORITY_v1",
            json.dumps({
                "actor": authority_row.get("COMMITTED_BY") or "operator",
                "source": apply_detail_source,
                "authority_id": authority_row.get("AUTHORITY_ID"),
                "authority_status": outcome["status_code"],
                "is_stale": bool(authority_row.get("IS_STALE")),
            }),
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
            json.dumps(
                {
                    "verdict": verdict,
                    "joint_decision": jd,
                    **verdict_envelope,
                }
            ),
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
    _merge_structural_contract_and_diagnostics(
        cur,
        action_id=action_id,
        committee_run_id=run_id,
        verdict=verdict,
        reason_codes=reason_codes,
    )
    return {
        "ok": True,
        "action_id": action_id,
        "run_id": run_id,
        "status": "COMPLETED",
        "action_status": next_status,
        "verdict": verdict,
        "joint_decision": jd,
        "reason_codes": reason_codes,
        "derived_sizing": {"proposed_price": proposed_price_derived, "proposed_qty": proposed_qty_derived},
        "agentic_source": True,
        "authority_status": outcome["status_code"],
        "authority_is_stale": bool(authority_row.get("IS_STALE")),
        "recovery_late_stage": recovery_late_stage,
    }


def _inline_hearing_stale_hint(ev: dict) -> str | None:
    """Simple freshness cue from last bar embedded in hearing evidence.

    An intraday price overlay (MARKET_BARS_1M_IBKR / 15M / 60M) that is less
    than ~10 minutes old counts as "fresh tape" and suppresses the daily-bar
    staleness hint — during a live session the latest daily bar is expected
    to be yesterday's close until EOD, so firing a red badge on an otherwise
    current committee run was misleading the operator.
    """
    price_source = str(ev.get("latest_price_source") or "").upper()
    try:
        price_age_sec = (
            float(ev.get("latest_price_age_sec"))
            if ev.get("latest_price_age_sec") is not None
            else None
        )
    except (TypeError, ValueError):
        price_age_sec = None
    intraday_sources = ("MARKET_BARS_1M_IBKR", "MARKET_BARS_15M_IBKR", "MARKET_BARS_60M_IBKR")
    has_fresh_tape = (
        price_source in intraday_sources
        and price_age_sec is not None
        and price_age_sec <= 600.0
    )

    dates = ev.get("recent_bar_dates") or []
    latest = dates[0] if dates else None
    if not latest:
        if has_fresh_tape:
            return None
        return "No daily bar anchor on this hearing — refresh after the session if you need same-day evidence."
    try:
        bd = date.fromisoformat(str(latest)[:10])
    except ValueError:
        return None
    today = datetime.now(timezone.utc).date()
    age = (today - bd).days
    if age >= 2:
        if has_fresh_tape:
            return None
        return (
            f"Evidence bar {str(latest)[:10]} is {age} calendar days behind UTC today — "
            "refresh if you need fresher structure."
        )
    return None


def _build_inline_hearing_payload(
    *,
    action_id: str,
    proposal_id: int,
    hearing_id: str,
    proposal: dict,
    refresh_payload: dict,
) -> dict:
    """Structured exhibits for LPA inline renderer (Phase 1 proof surface)."""
    ev = refresh_payload.get("hearing_evidence") or {}
    chair = refresh_payload.get("chair") or {}
    operational = refresh_payload.get("operational") or {}
    posture = operational.get("posture") or {}
    snap = refresh_payload.get("snapshot_panel") or {}
    pop = refresh_payload.get("proposal") or {}
    sym = (pop.get("symbol") or proposal.get("SYMBOL") or "").strip()
    direction = (pop.get("direction") or proposal.get("DIRECTION") or "").strip()
    setup_family = pop.get("setup_family") or proposal.get("SETUP_FAMILY")
    trust_label = snap.get("TRUST_LABEL") or proposal.get("TRUST_LABEL")

    zone = snap.get("ENTRY_ZONE_JSON") or {}
    if hasattr(zone, "as_dict"):
        zone = dict(zone)
    zl = zh = None
    if isinstance(zone, dict):
        zl, zh = zone.get("low"), zone.get("high")

    inv = snap.get("INVALIDATION_JSON") or {}
    if hasattr(inv, "as_dict"):
        inv = dict(inv)
    inv_rule = inv.get("rule") if isinstance(inv, dict) else None

    trace = ev.get("recent_bar_trace") or []
    exhibit_geometry = {
        "side": direction,
        "symbol": sym,
        "zone_low": zl,
        "zone_high": zh,
        "latest_price": ev.get("latest_price"),
        "latest_price_source": ev.get("latest_price_source"),
        "latest_price_ts_utc": ev.get("latest_price_ts_utc"),
        "latest_price_age_sec": ev.get("latest_price_age_sec"),
        "zone_distance_pct": ev.get("zone_distance_pct"),
        "invalidation_level": ev.get("invalidation_level"),
        "invalidation_rule": inv_rule,
        "invalidation_breached": ev.get("invalidation_breached"),
        "post_proposal_path_trace": trace or None,
    }

    path_strip: dict = {}
    fp_art: dict = {}
    for a in refresh_payload.get("artifacts") or []:
        kind = a.get("artifact_kind")
        if kind == "PATH_STRIP":
            path_strip = a.get("payload") or {}
        elif kind == "SYMBOL_FINGERPRINT":
            fp_art = a.get("payload") or {}

    exhibit_path = {
        "pct_adverse_before_favorable": path_strip.get("pct_adverse"),
        "mhr": path_strip.get("mhr"),
        "path_quality_label": path_strip.get("label") or posture.get("path_quality"),
        "interpretation": path_strip.get("interpretation") or ev.get("path_quality_interpretation"),
    }
    exhibit_regime = {
        "proposal_regime": snap.get("REGIME_STATE"),
        "trend_now": ev.get("trend_regime_now"),
        "vol_now": ev.get("vol_regime_now"),
        "structure_now": ev.get("structural_state_now"),
        "structure_proposal": snap.get("STRUCTURAL_STATE"),
        "continuity_verdict": ev.get("regime_continuity"),
        "continuity_detail": ev.get("regime_continuity_detail"),
    }
    exhibit_protection = {
        "invalidation_level": ev.get("invalidation_level"),
        "cushion_pct": ev.get("invalidation_cushion_pct"),
        "breached": ev.get("invalidation_breached"),
        "trail_posture": posture.get("trail_posture"),
        "size_posture": posture.get("size_posture"),
    }
    exhibit_fp = {
        "one_liner": fp_art.get("one_liner"),
        "bullets": fp_art.get("bullets") or [],
        "badge": fp_art.get("badge"),
        "trust_label": fp_art.get("trust_label") or trust_label,
        "vol_regime_now": fp_art.get("vol_regime_now"),
        "path_quality": fp_art.get("path_quality"),
    }

    what_changed = chair.get("what_changed_since_proposal") or []
    roles_compact = []
    for r in refresh_payload.get("roles") or []:
        o = r.get("output") or {}
        roles_compact.append(
            {
                "role_name": r.get("role_name"),
                "stance_badge": o.get("stance_badge"),
                "one_liner": o.get("one_liner"),
            }
        )

    bar_dates = ev.get("recent_bar_dates") or []
    evidence_bar_date = bar_dates[0] if bar_dates else None

    out = {
        "action_id": action_id,
        "proposal_id": proposal_id,
        "hearing_id": hearing_id,
        "symbol": sym,
        "setup_family": setup_family,
        "direction": direction,
        "trust_label": trust_label,
        "stance": refresh_payload.get("stance"),
        "confidence": refresh_payload.get("confidence"),
        "hearing_ts": refresh_payload.get("hearing_ts"),
        "hearing_updated_at": refresh_payload.get("updated_at"),
        "evidence_bar_date": evidence_bar_date,
        "stale_hint": _inline_hearing_stale_hint(ev),
        "exhibit_geometry_hero": exhibit_geometry,
        "exhibit_path_quality": exhibit_path,
        "exhibit_regime_continuity": exhibit_regime,
        "exhibit_protection": exhibit_protection,
        "exhibit_symbol_fingerprint": exhibit_fp,
        "what_changed_strip": what_changed[:5],
        "chair_board": {
            "stance": chair.get("stance"),
            "confidence": chair.get("confidence"),
            "top_supports": chair.get("top_supports") or [],
            "top_tensions": chair.get("top_tensions") or [],
            "execution_shaping": chair.get("execution_shaping") or {},
        },
        "roles_compact": roles_compact,
        "artifacts": refresh_payload.get("artifacts") or [],
    }
    disc = refresh_payload.get("exhibit_public_disclosure_context")
    if disc is not None:
        out["exhibit_public_disclosure_context"] = disc
    live_disc = refresh_payload.get("exhibit_live_politician_disclosure_context")
    if live_disc is not None:
        out["exhibit_live_politician_disclosure_context"] = live_disc
    intra = refresh_payload.get("exhibit_intraday_substantiation_map")
    if intra is None:
        for a in refresh_payload.get("artifacts") or []:
            if (a.get("artifact_kind") or "") == "INTRADAY_SUBSTANTIATION_MAP":
                intra = a.get("payload")
                break
    if intra is not None:
        out["exhibit_intraday_substantiation_map"] = intra
    return out


_ORCHESTRATE_ALLOWED_STATUSES = frozenset(
    {
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
)


def _shadow_board_enabled_for_orchestrate(cur) -> bool:
    """Read SHADOW_BOARD_ENABLED at orchestrate time. Default off if missing."""
    try:
        cur.execute(
            "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
            ("SHADOW_BOARD_ENABLED",),
        )
        rows = fetch_all(cur)
        if not rows:
            return False
        val = (rows[0].get("CONFIG_VALUE") or "").strip().lower()
        return val in ("1", "true", "yes")
    except Exception:
        return False


def _shadow_timeout_for_orchestrate(cur, default: float = 120.0) -> float:
    """Read SHADOW_BOARD_TIMEOUT_SEC at orchestrate time. Falls back to default."""
    try:
        cur.execute(
            "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
            ("SHADOW_BOARD_TIMEOUT_SEC",),
        )
        rows = fetch_all(cur)
        if not rows:
            return default
        return float((rows[0].get("CONFIG_VALUE") or "").strip())
    except Exception:
        return default


async def _intelligence_only_shadow_kickoff(
    conn,
    cur,
    action_id: str,
    action: dict,
) -> dict | None:
    """Best-effort shadow-board kickoff when execution path is blocked by a
    safety gate (e.g. OPEN_MARKET_CLOSED outside the extended trading window).

    Performs ONLY intelligence work:
        - fetch/create hearing row
        - refresh COMMITTEE_HEARING via the agentic-primary evidence-only
          refresh (Phase 5B) when AGENTIC_PRIMARY_MATERIALIZATION_ENABLED is
          on; otherwise falls back to the legacy deterministic _run_refresh
        - compute + persist EVIDENCE_PACK_HASH on COMMITTEE_HEARING
        - kickoff_shadow_board_for_snapshot(..., action_id=action_id)

    Explicitly does NOT:
        - commit COMMITTEE_FINAL_DECISION
        - materialize LIVE_ACTIONS committee/verdict fields
        - change LIVE_ACTIONS.STATUS
        - touch /revalidate or Submit gating

    Returns a small dict on success or None on any failure / when shadow board
    is disabled. Never raises — the caller may still need to surface a 409 for
    the execution-side block.
    """
    proposal_id_raw = action.get("PROPOSAL_ID")
    if proposal_id_raw is None:
        return None
    try:
        proposal_id_int = int(proposal_id_raw)
    except (TypeError, ValueError):
        return None
    try:
        proposal = _fetch_proposal(cur, proposal_id_int)
        if not proposal:
            return None
        snapshot = _fetch_snapshot(cur, proposal_id_int)
        if not snapshot:
            return None
    except Exception as exc:  # noqa: BLE001
        _log.warning(
            "intelligence_only_kickoff: proposal/snapshot fetch failed action=%s: %s",
            action_id, exc,
        )
        return None

    if not _shadow_board_enabled_for_orchestrate(cur):
        return None

    try:
        from app.committee.shadow_board import (
            compute_evidence_pack_hash,
            kickoff_shadow_board_for_snapshot,
        )
    except Exception as imp_exc:  # noqa: BLE001
        _log.warning("intelligence_only_kickoff: shadow_board import failed: %s", imp_exc)
        return None

    existing = _fetch_hearing_by_proposal(cur, proposal_id_int)
    hearing_id = str(existing["HEARING_ID"]) if existing else str(uuid.uuid4())

    raw = _underlying_sf_conn(conn)
    prior_autocommit = True
    refresh_payload: dict = {}
    try:
        raw.autocommit(False)
        prior_autocommit = False
        # Phase 5B: prefer the agentic-only evidence-dossier refresh when
        # AGENTIC_PRIMARY_MATERIALIZATION_ENABLED is on. Falls back to the
        # legacy deterministic refresh for rollback mode. Either way only
        # writes to COMMITTEE_HEARING / COMMITTEE_ROLE_OUTPUT /
        # COMMITTEE_EVIDENCE_ARTIFACT — never to LIVE_ACTIONS, never to
        # COMMITTEE_FINAL_DECISION.
        _agentic_primary_for_kickoff = False
        try:
            _agentic_primary_for_kickoff = is_agentic_primary_materialization_enabled(raw)
        except Exception:  # noqa: BLE001 — fail closed to legacy refresh
            _agentic_primary_for_kickoff = False
        if _agentic_primary_for_kickoff:
            refresh_payload = _run_evidence_only_refresh(
                conn, hearing_id, proposal_id_int, snapshot, proposal,
            )
        else:
            refresh_payload = _run_refresh(conn, hearing_id, proposal_id_int, snapshot, proposal)
        raw.commit()
    except Exception as exc:  # noqa: BLE001
        try:
            raw.rollback()
        except Exception:  # noqa: BLE001
            pass
        _log.warning(
            "intelligence_only_kickoff: hearing refresh failed action=%s hearing=%s: %s",
            action_id, hearing_id, exc,
        )
        # Refresh failure is not fatal: fall through and attempt shadow kickoff
        # against whatever evidence is already in COMMITTEE_HEARING.
    finally:
        try:
            raw.autocommit(True)
            prior_autocommit = True
        except Exception:  # noqa: BLE001
            pass

    # Compute + persist evidence_pack_hash so the shadow session binds to the
    # same snapshot identity as the deterministic hearing.
    evidence_pack_hash: str | None = None
    try:
        cur.execute(
            "SELECT * FROM MIP.APP.COMMITTEE_HEARING WHERE HEARING_ID = %s",
            (hearing_id,),
        )
        hearing_rows = fetch_all(cur)
        hearing_row = hearing_rows[0] if hearing_rows else None
        evidence_pack_hash = compute_evidence_pack_hash(
            snapshot_id=int(snapshot["SNAPSHOT_ID"]),
            snapshot_row=dict(snapshot),
            proposal_row=dict(proposal),
            hearing_row=hearing_row,
        )
        cur.execute(
            """
            UPDATE MIP.APP.COMMITTEE_HEARING
               SET EVIDENCE_PACK_HASH = %s
             WHERE HEARING_ID = %s
            """,
            (evidence_pack_hash, hearing_id),
        )
    except Exception as exc:  # noqa: BLE001
        _log.warning(
            "intelligence_only_kickoff: evidence_pack_hash compute/persist failed action=%s hearing=%s: %s",
            action_id, hearing_id, exc,
        )

    if not evidence_pack_hash:
        # Shadow kickoff requires the hash for snapshot binding. Without it we
        # cannot guarantee idempotency — bail rather than create an orphan
        # shadow session.
        return None

    try:
                shadow_kickoff = await kickoff_shadow_board_for_snapshot(
                    hearing_id=hearing_id,
                    proposal_id=proposal_id_int,
                    snapshot_id=int(snapshot["SNAPSHOT_ID"]),
                    evidence_pack_hash=evidence_pack_hash,
                    timeout_sec=_shadow_timeout_for_orchestrate(cur),
                    force=bool(req.force_fresh_shadow),
                    action_id=action_id,
                )
    except Exception as exc:  # noqa: BLE001
        _log.warning(
            "intelligence_only_kickoff: shadow kickoff failed action=%s hearing=%s: %s",
            action_id, hearing_id, exc,
        )
        return None

    return {
        "intelligence_only": True,
        "hearing_id": hearing_id,
        "evidence_pack_hash": evidence_pack_hash,
        "shadow_session_id": shadow_kickoff.get("session_id") if shadow_kickoff else None,
        "shadow_status": shadow_kickoff.get("status") if shadow_kickoff else None,
        "shadow_reused": shadow_kickoff.get("reused") if shadow_kickoff else None,
        "refresh_stance": refresh_payload.get("stance") if isinstance(refresh_payload, dict) else None,
    }


@router.post("/trades/actions/{action_id}/committee2/orchestrate")
async def orchestrate_committee2_structural_entry(
    action_id: str,
    req: Committee2OrchestrateRequest = Body(default_factory=Committee2OrchestrateRequest),
):
    """
    Single operational path for structural ENTRY: refresh hearing, commit final decision bound to
    this action_id, materialize LIVE committee tables (no separate Sync step).

    Phase 1 dual-hearing: after the deterministic real board refreshes, we
    compute and persist EVIDENCE_PACK_HASH onto COMMITTEE_HEARING and fire an
    idempotent shadow-board kickoff bound to that same snapshot identity. The
    kickoff inserts a RUNNING placeholder synchronously and schedules the
    full agentic run as a background asyncio task — the real-board response
    is unaffected by shadow latency.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")
        assert_live_committee_policy(
            action,
            _live_structural_only_enabled(cur),
            is_structural_fn=is_structural_live_action,
        )
        if not is_structural_live_action(action):
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Committee 2.0 orchestration applies to structural live actions only.",
                    "reason_codes": ["COMMITTEE2_ORCHESTRATE_STRUCTURAL_ONLY"],
                },
            )
        action_intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
        if action_intent == "EXIT":
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Structural EXIT is execution-only; orchestration is for ENTRY only.",
                    "reason_codes": ["COMMITTEE2_ORCHESTRATE_ENTRY_ONLY"],
                },
            )

        # Phase 5C: fail fast for structural ENTRY rows whose underlying
        # proposal has been superseded by a newer board run. Running the
        # orchestrate just refreshes evidence and re-runs the Agentic
        # Committee but cannot unstick the supersedure gate — Submit will
        # still be blocked downstream. Returning 409 here prevents the
        # operator from getting stuck in a re-validate loop on dead rows
        # if they bypass the disabled UI button (curl / replay).
        proposal_freshness = "CURRENT"
        try:
            proposal_freshness = _compute_action_proposal_freshness(
                cur, action.get("PROPOSAL_ID")
            )
        except Exception as fresh_exc:  # noqa: BLE001
            _log.warning(
                "orchestrate: proposal freshness lookup raised (treated as CURRENT) action=%s: %s",
                action_id, fresh_exc,
            )
            proposal_freshness = "CURRENT"
        if proposal_freshness and str(proposal_freshness).upper() != "CURRENT":
            raise HTTPException(
                status_code=409,
                detail={
                    "message": (
                        "Stale proposal — this action's parent proposal is from a superseded "
                        "board run. Reject this action and validate a proposal from the current "
                        "board run instead."
                    ),
                    "reason_codes": [
                        "PROPOSAL_EXPIRED_OR_SUPERSEDED",
                        "COMMITTEE2_ORCHESTRATE_STALE_PROPOSAL",
                    ],
                    "proposal_freshness": str(proposal_freshness),
                },
            )

        status_upper = (action.get("STATUS") or "").upper()
        # LPA-first parity with the legacy /committee/run endpoint: when the action
        # hasn't yet passed opening validation, auto-run the opening sanity gate so
        # the operator doesn't have to drive a separate pre-step. If the gate hard-
        # blocks (OPEN_BLOCKED), surface a structured 409 with the validation result;
        # otherwise re-fetch and continue with the normal allowed-status check.
        opening_gate_payload: dict | None = None
        if status_upper in ("RESEARCH_IMPORTED", "PROPOSED", "PENDING_OPEN_VALIDATION"):
            opening_gate_payload = _run_opening_sanity_gate(
                cur,
                action,
                force_refresh_1m=True,
                now_utc=datetime.now(timezone.utc),
            )
            try:
                conn.commit()
            except Exception:
                pass
            action = _fetch_live_action(cur, action_id) or action
            status_upper = (action.get("STATUS") or "").upper()
            if status_upper == "OPEN_BLOCKED":
                # Intelligence/execution separation: the opening sanity gate is
                # a safety/execution guard (e.g. OPEN_MARKET_CLOSED outside the
                # extended trading window, snapshot stale, gap block, live
                # activation guard). Those reasons correctly prevent
                # materialization, FD commit, and Submit — but they must NOT
                # also gate the agentic/shadow review, which is an
                # intelligence/audit process. Attempt a best-effort
                # intelligence-only shadow kickoff so the Shadow Chair Verdict
                # can still evaluate and an AUTO_AUDIT authority row can be
                # written. Failure here is swallowed inside the helper.
                intelligence_only_shadow = None
                try:
                    intelligence_only_shadow = await _intelligence_only_shadow_kickoff(
                        conn, cur, action_id, action,
                    )
                except Exception as intel_exc:  # noqa: BLE001
                    _log.warning(
                        "orchestrate: intelligence-only shadow kickoff raised (swallowed) action=%s: %s",
                        action_id, intel_exc,
                    )
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "Opening validation blocked Committee 2.0 (OPEN_BLOCKED).",
                        "reason_codes": opening_gate_payload.get("reason_codes") or ["OPEN_BLOCKED"],
                        "blocked_stage": "OPENING_SANITY_GATE",
                        "opening_validation": opening_gate_payload.get("opening_validation") or {},
                        "status": status_upper,
                        "intelligence_only_shadow": intelligence_only_shadow,
                    },
                )

        if status_upper not in _ORCHESTRATE_ALLOWED_STATUSES:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": f"Committee orchestrate blocked for current status: {status_upper}.",
                    "reason_codes": ["COMMITTEE2_ORCHESTRATE_STATUS_BLOCKED"],
                    "status": status_upper,
                    "opening_validation": (opening_gate_payload or {}).get("opening_validation") or {},
                },
            )

        pid = action.get("PROPOSAL_ID")
        if pid is None:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "LIVE_ACTIONS.PROPOSAL_ID is required for structural committee orchestration.",
                    "reason_codes": ["COMMITTEE2_PROPOSAL_ID_REQUIRED"],
                },
            )
        try:
            proposal_id_int = int(pid)
        except (TypeError, ValueError):
            raise HTTPException(status_code=409, detail="Invalid PROPOSAL_ID on LIVE_ACTIONS row.")

        _require_enabled(conn)
        proposal = _fetch_proposal(cur, proposal_id_int)
        if not proposal:
            raise HTTPException(status_code=404, detail="Proposal not found.")
        snapshot = _fetch_snapshot(cur, proposal_id_int)
        if not snapshot:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "NO_SNAPSHOT",
                    "message": "Immutable proposal snapshot missing; re-run structural propose or backfill snapshots.",
                },
            )

        existing = _fetch_hearing_by_proposal(cur, proposal_id_int)
        hearing_id = str(existing["HEARING_ID"]) if existing else str(uuid.uuid4())
        _ = req.force_rebuild_hearing  # reserved; hearing is always refreshed (see LPA-first spec)

        # Always pull a fresh 1-minute IBKR bar before re-running the committee.
        # When the action is RESEARCH_IMPORTED / PROPOSED / PENDING_OPEN_VALIDATION
        # the opening sanity gate above already triggered this refresh; for all
        # other statuses (OPEN_BLOCKED, READY_FOR_APPROVAL_FLOW, etc.) the chair
        # would otherwise re-evaluate against whatever bar is sitting in MART —
        # which can be tens of minutes stale during normal trading. That stale
        # price is what causes "Run Committee 2.0" to keep the same verdict and
        # show STALE — CONSIDER REFRESH even after the operator clicks the
        # button. Force-refreshing here keeps the chair's `latest_price` /
        # `latest_price_age_sec` evidence in sync with the live tape on every
        # orchestrate.
        if status_upper not in ("RESEARCH_IMPORTED", "PROPOSED", "PENDING_OPEN_VALIDATION"):
            try:
                _force_refresh_latest_one_minute_bars(
                    cur,
                    action.get("SYMBOL"),
                    portfolio_id=int(action["PORTFOLIO_ID"]) if action.get("PORTFOLIO_ID") is not None else None,
                )
                try:
                    conn.commit()
                except Exception:
                    pass
            except Exception:
                # Refresh is best-effort — committee re-run still proceeds with
                # whatever MART has. The chair will simply report the stale
                # `latest_price_age_sec` and the inline freshness hint will fire.
                pass

        raw = _underlying_sf_conn(conn)
        # Phase 5B: decide once per orchestrate request whether the agentic
        # path owns LIVE_ACTIONS materialization. When this flag is on the
        # deterministic chair is NOT executed and COMMITTEE_FINAL_DECISION
        # is NOT written — instead the new agentic-only refresh writes
        # COMMITTEE_HEARING as an evidence-only container (NULL chair /
        # stance / confidence) and the operator commit endpoint becomes
        # the sole source of truth for LIVE_ACTIONS.
        agentic_primary_enabled = False
        try:
            agentic_primary_enabled = is_agentic_primary_materialization_enabled(raw)
        except Exception:  # noqa: BLE001 — fail closed to legacy behavior
            agentic_primary_enabled = False

        raw.autocommit(False)
        refresh_payload: dict = {}
        commit_payload: dict = {}
        materialize_out: dict | None = None
        idempotent_replay = False
        try:
            if agentic_primary_enabled:
                # Phase 5B: evidence-only refresh — no deterministic chair,
                # no COMMITTEE_FINAL_DECISION write, no LIVE_ACTIONS
                # materialization here. The shadow board kickoff below
                # then runs the Agentic Committee against this dossier;
                # operator commit is what eventually drives Submit.
                refresh_payload = _run_evidence_only_refresh(
                    conn, hearing_id, proposal_id_int, snapshot, proposal,
                )
                commit_payload = {}
                materialize_out = None
                idempotent_replay = False
            else:
                refresh_payload = _run_refresh(conn, hearing_id, proposal_id_int, snapshot, proposal)
                commit_payload = committee_final_decision_commit_for_action(
                    cur,
                    hearing_id,
                    HearingCommitRequest(action_id=action_id, note="LPA Committee 2.0 orchestrate"),
                )
                action_refresh = _fetch_live_action(cur, action_id) or action
                committee_status = (action_refresh.get("COMMITTEE_STATUS") or "").upper()
                already_fd = bool(commit_payload.get("already_committed"))
                idempotent_replay = bool(already_fd and committee_status == "COMPLETED")

                if idempotent_replay:
                    materialize_out = None
                else:
                    materialize_out = _materialize_structural_entry_committee_apply(
                        cur,
                        action_id,
                        dict(action_refresh),
                        ApplyCommitteeVerdictRequest(),
                        apply_detail_source="COMMITTEE2_ORCHESTRATE",
                    )
            raw.commit()
        except HTTPException:
            raw.rollback()
            raise
        except Exception:
            raw.rollback()
            raise
        finally:
            raw.autocommit(True)

        stance = refresh_payload.get("stance")
        conf = refresh_payload.get("confidence")
        action_after = _fetch_live_action(cur, action_id) or action

        # ------------------------------------------------------------------
        # Phase 1 dual-hearing: snapshot identity + idempotent shadow kickoff
        # ------------------------------------------------------------------
        # Compute deterministic EVIDENCE_PACK_HASH over (snapshot_id, snapshot
        # row, proposal stable fields). Persist onto COMMITTEE_HEARING so the
        # real and shadow boards both bind to the same snapshot identity.
        evidence_pack_hash: str | None = None
        shadow_kickoff: dict | None = None
        try:
            from app.committee.shadow_board import (
                compute_evidence_pack_hash,
                kickoff_shadow_board_for_snapshot,
            )

            cur.execute(
                "SELECT * FROM MIP.APP.COMMITTEE_HEARING WHERE HEARING_ID = %s",
                (hearing_id,),
            )
            _hearing_rows_for_hash = fetch_all(cur)
            _hearing_row_for_hash = _hearing_rows_for_hash[0] if _hearing_rows_for_hash else None
            evidence_pack_hash = compute_evidence_pack_hash(
                snapshot_id=int(snapshot["SNAPSHOT_ID"]),
                snapshot_row=dict(snapshot),
                proposal_row=dict(proposal),
                hearing_row=_hearing_row_for_hash,
            )
            cur.execute(
                """
                UPDATE MIP.APP.COMMITTEE_HEARING
                   SET EVIDENCE_PACK_HASH = %s
                 WHERE HEARING_ID = %s
                """,
                (evidence_pack_hash, hearing_id),
            )

            if _shadow_board_enabled_for_orchestrate(cur):
                shadow_kickoff = await kickoff_shadow_board_for_snapshot(
                    hearing_id=hearing_id,
                    proposal_id=proposal_id_int,
                    snapshot_id=int(snapshot["SNAPSHOT_ID"]),
                    evidence_pack_hash=evidence_pack_hash,
                    timeout_sec=_shadow_timeout_for_orchestrate(cur),
                    force=False,
                    # Stage 4b: thread action_id so the auto-audit hook can
                    # write an AGENTIC_REVALIDATION_AUTHORITY row when the
                    # shadow board finalizes. Best-effort; never gates Submit.
                    action_id=action_id,
                )
        except Exception as kickoff_exc:
            # Real board must remain authoritative — shadow kickoff is advisory.
            _log.warning(
                "committee2_orchestrate: shadow kickoff failed (real board unaffected): %s",
                kickoff_exc,
            )

        inline_hearing = _build_inline_hearing_payload(
            action_id=action_id,
            proposal_id=proposal_id_int,
            hearing_id=hearing_id,
            proposal=dict(proposal),
            refresh_payload=refresh_payload,
        )
        if evidence_pack_hash:
            inline_hearing["evidence_pack_hash"] = evidence_pack_hash
        if shadow_kickoff:
            inline_hearing["shadow_session_id"] = shadow_kickoff.get("session_id")
            inline_hearing["shadow_status"] = shadow_kickoff.get("status")
            inline_hearing["shadow_reused"] = shadow_kickoff.get("reused")

        if idempotent_replay:
            fd = fetch_committee2_final_decision_for_action(cur, action_id)
            if not fd:
                raise HTTPException(status_code=500, detail="Final decision missing after orchestrate replay.")
            vr = structural_entry_verdict_from_committee2_final(fd, dict(action_after))
            return {
                "ok": True,
                "action_id": action_id,
                "proposal_id": proposal_id_int,
                "hearing_id": hearing_id,
                "stance": stance,
                "confidence": conf,
                "blocked": bool(vr.get("blocked")),
                "recommendation": str(vr.get("recommendation") or "").upper(),
                "reason_codes": list(vr.get("reason_codes") or [])[:12],
                "committee_run_id": str(action_after.get("COMMITTEE_RUN_ID") or ""),
                "action_status": str(action_after.get("STATUS") or ""),
                "already_committed": True,
                "idempotent_replay": True,
                "joint_decision": vr.get("joint_decision"),
                "inline_hearing": inline_hearing,
                "agentic_primary_enabled": agentic_primary_enabled,
            }

        if agentic_primary_enabled:
            # Phase 5B: orchestrate refreshed the evidence-only hearing
            # (NULL chair / stance / confidence). NO COMMITTEE_FINAL_DECISION
            # is written. LIVE_ACTIONS materialization is deferred to the
            # operator-commit endpoint after the Agentic Committee finishes.
            # Return a payload that makes the new mode explicit so the LPA
            # can render the agentic-pending state.
            return {
                "ok": True,
                "action_id": action_id,
                "proposal_id": proposal_id_int,
                "hearing_id": hearing_id,
                "stance": stance,
                "confidence": conf,
                "blocked": False,
                "recommendation": "AGENTIC_PENDING_COMMIT",
                "reason_codes": ["AGENTIC_PRIMARY_MATERIALIZATION_PENDING"],
                "committee_run_id": "",
                "action_status": str(action_after.get("STATUS") or ""),
                "already_committed": bool(commit_payload.get("already_committed")),
                "idempotent_replay": False,
                "joint_decision": None,
                "derived_sizing": None,
                "inline_hearing": inline_hearing,
                "agentic_primary_enabled": True,
                "shadow_reused": shadow_kickoff.get("reused") if shadow_kickoff else None,
            }

        mv = materialize_out or {}
        verdict = mv.get("verdict") or {}
        rc = list(mv.get("reason_codes") or [])[:12]
        return {
            "ok": True,
            "action_id": action_id,
            "proposal_id": proposal_id_int,
            "hearing_id": hearing_id,
            "stance": stance,
            "confidence": conf,
            "blocked": bool(verdict.get("blocked")),
            "recommendation": str(verdict.get("recommendation") or "").upper(),
            "reason_codes": rc,
            "committee_run_id": str(mv.get("run_id") or ""),
            "action_status": str(mv.get("action_status") or action_after.get("STATUS") or ""),
            "already_committed": bool(commit_payload.get("already_committed")),
            "idempotent_replay": False,
            "joint_decision": mv.get("joint_decision"),
            "derived_sizing": mv.get("derived_sizing"),
            "inline_hearing": inline_hearing,
            "agentic_primary_enabled": False,
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
        assert_live_committee_policy(
            action,
            _live_structural_only_enabled(cur),
            is_structural_fn=is_structural_live_action,
        )

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

        is_structural = is_structural_live_action(action)
        action_intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
        is_exit = action_intent == "EXIT"

        # Stage 4f — explicit route-level guard for structural ENTRY actions.
        # When agentic-primary materialization is on, this endpoint cannot
        # materialize from a deterministic verdict; the operator must commit
        # an agentic authority row instead. EXIT actions are exempt (they
        # never run shadow board and use execution-only verdicts).
        if is_structural and not is_exit and _read_agentic_primary_flag_via_cursor(cur):
            _log.error(
                "STAGE_4F_GUARD apply_live_trade_committee blocked deterministic "
                "materialize for structural ENTRY action_id=%s (agentic-primary on).",
                action_id,
            )
            raise HTTPException(
                status_code=409,
                detail={
                    "message": (
                        "Deterministic committee apply is disabled for structural ENTRY "
                        "while agentic-primary materialization is on. Commit agentic "
                        "authority via POST /live/trades/actions/{id}/agentic-authority/commit."
                    ),
                    "reason_codes": [
                        "DETERMINISTIC_APPLY_DISABLED_AGENTIC_PRIMARY",
                        "STAGE_4F_NO_C2_FALLBACK",
                    ],
                    "action_id": action_id,
                },
            )

        if is_structural:
            if is_exit:
                exit_position_qty_apply = _fetch_live_symbol_position_qty(
                    cur, action.get("PORTFOLIO_ID"), action.get("SYMBOL")
                )
                struct_result = build_structural_exit_execution_only_verdict(
                    dict(action), exit_position_qty=exit_position_qty_apply
                )
            else:
                return _materialize_structural_entry_committee_apply(cur, action_id, dict(action), req)
            blocked = bool(struct_result.get("blocked"))
            recommendation = str(struct_result.get("recommendation") or "BLOCK").upper()
            size_factor = float(struct_result.get("size_factor") or 0.0)
            confidence = float(struct_result.get("confidence") or 0.5)
            jd = struct_result.get("joint_decision") or {}
            reason_codes = list(struct_result.get("reason_codes") or [])
            if not is_exit:
                reason_codes.append("STRUCTURAL_COMMITTEE_REVIEWED")
            verdict = {
                "recommendation": recommendation,
                "size_factor": max(0.0, min(1.0, size_factor)),
                "confidence": max(0.0, min(1.0, confidence)),
                "blocked": blocked,
                "joint_decision": jd,
                "structural_source": True,
                "tier_c_conflict": False,
            }
            next_status = "OPEN_BLOCKED" if verdict["blocked"] else "READY_FOR_APPROVAL_FLOW"
            context = {"entry_intel_baseline": {}, "parallel_worlds_evidence": {}, "news_context_snapshot": {}}
        else:
            verdict_in = dict(req.verdict or {})
            jd = _parse_variant(verdict_in.get("joint_decision"))
            allows_execution = _decision_allows_execution(jd, action_intent)
            recommendation = str(verdict_in.get("recommendation") or ("BLOCK" if not allows_execution else "PROCEED_REDUCED")).upper()
            size_factor = float(verdict_in.get("size_factor") or jd.get("position_size_factor") or 1.0)
            confidence = float(verdict_in.get("confidence") or 0.5)
            blocked = bool(verdict_in.get("blocked")) or recommendation == "BLOCK" or (not allows_execution)
            context = _build_action_decision_context(cur, action)
            pw_evidence = context.get("parallel_worlds_evidence") or {}
            decision_news_snapshot = context.get("news_context_snapshot") or {}

            verdict = {
                "recommendation": recommendation,
                "size_factor": max(0.0, min(1.0, size_factor)),
                "confidence": max(0.0, min(1.0, confidence)),
                "blocked": blocked,
                "joint_decision": jd,
            }
            verdict = _backfill_joint_decision_from_policy(verdict, context)
            jd = _parse_variant(verdict.get("joint_decision"))
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
            tier_c_conflict = _has_tier_c_conflict(verdict, pw_evidence, decision_news_snapshot)
            if tier_c_conflict:
                reason_codes.append("TIER_C_CONFLICT_ALERT")
            if verdict.get("quality_backfilled"):
                reason_codes.append("COMMITTEE_POLICY_BACKFILL_APPLIED")
            reason_codes.append("COMMITTEE_REVIEWED")
            verdict["tier_c_conflict"] = tier_c_conflict
            next_status = "OPEN_BLOCKED" if verdict["blocked"] else "READY_FOR_APPROVAL_FLOW"

        # Derive proposed price/qty so row no longer remains fully pending.
        proposed_price_derived = _fetch_ibkr_mart_reference_close(cur, action.get("SYMBOL"))
        if proposed_price_derived is None and not is_exit:
            for key in ("REVALIDATION_PRICE", "PROPOSED_PRICE", "CURRENT_PRICE", "ONE_MIN_BAR_CLOSE"):
                v = action.get(key)
                if v is not None:
                    try:
                        proposed_price_derived = float(v)
                        break
                    except (TypeError, ValueError):
                        pass
            if proposed_price_derived is None and action.get("ENTRY_ZONE_LOW") is not None and action.get("ENTRY_ZONE_HIGH") is not None:
                try:
                    proposed_price_derived = (
                        float(action["ENTRY_ZONE_LOW"]) + float(action["ENTRY_ZONE_HIGH"])
                    ) / 2.0
                except (TypeError, ValueError):
                    pass
        if proposed_price_derived is None and is_exit:
            for key in ("REVALIDATION_PRICE", "PROPOSED_PRICE", "CURRENT_PRICE", "ONE_MIN_BAR_CLOSE"):
                v = action.get(key)
                if v is not None:
                    try:
                        proposed_price_derived = float(v)
                        break
                    except (TypeError, ValueError):
                        pass
        proposed_qty_derived = None

        try:
            committee_size_factor = float(verdict.get("size_factor") or 1.0)
            training_size_cap = float(action.get("TRAINING_SIZE_CAP_FACTOR") or 1.0)
            open_factor = float(action.get("TARGET_OPEN_CONDITION_FACTOR") or 1.0)
            if proposed_price_derived is None:
                raise ValueError("no reference price for sizing")

            cur.execute(
                """
                select
                  c.IBKR_ACCOUNT_ID,
                  c.MAX_POSITION_PCT,
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

            if is_structural and is_exit:
                pq = _fetch_live_symbol_position_qty(cur, action.get("PORTFOLIO_ID"), action.get("SYMBOL"))
                aq = abs(float(pq or 0.0))
                proposed_qty_derived = max(int(aq), 1) if aq > 0 else None
            elif is_structural and not is_exit and nav_eur > 0:
                max_position_pct = (nav_rows[0] or {}).get("MAX_POSITION_PCT")
                pos_pct = float(max_position_pct or 0.05)
                max_notional = (
                    nav_eur * pos_pct * committee_size_factor * training_size_cap * open_factor
                )
                proposed_qty_derived = max(int(max_notional / max(proposed_price_derived, 1e-9)), 1)
            else:
                param_snapshot = _parse_variant(action.get("PARAM_SNAPSHOT"))
                target_weight = param_snapshot.get("target_weight")
                target_weight_abs = abs(float(target_weight)) if target_weight is not None else None
                effective_weight = None
                if target_weight_abs is not None:
                    effective_weight = target_weight_abs * committee_size_factor * training_size_cap * open_factor
                if effective_weight is not None and effective_weight > 0 and nav_eur > 0:
                    est_notional = nav_eur * effective_weight
                    proposed_qty_derived = max(int(est_notional / max(proposed_price_derived, 1e-9)), 1)
        except Exception:
            proposed_qty_derived = None

        proposed_qty_derived, reason_codes = _apply_post_committee_entry_viability_and_qty(
            cur,
            action_id=action_id,
            portfolio_id=int(action.get("PORTFOLIO_ID") or 0),
            side=str(action.get("SIDE") or "").upper(),
            is_exit=is_exit,
            is_committee_blocked=bool(verdict.get("blocked")),
            proposed_price=proposed_price_derived,
            committee_qty=proposed_qty_derived,
            joint_decision=_parse_variant(verdict.get("joint_decision")),
            reason_codes=reason_codes,
        )

        if is_structural:
            verdict_phase3_apply = build_structural_verdict_envelope_v1(
                action=action,
                verdict=verdict,
                reason_codes=reason_codes,
                committee_run_id="",  # filled after insert
                outputs_wrapper=[],
            )
        else:
            verdict_phase3_apply = _committee_alpha_phase3_envelope(
                context,
                verdict,
                [],
                action_intent=action_intent,
                manual_apply=True,
                reason_codes=reason_codes,
            )

        run_id = str(uuid.uuid4())
        if is_structural and isinstance(verdict_phase3_apply.get("structural_verdict_envelope_v1"), dict):
            verdict_phase3_apply["structural_verdict_envelope_v1"]["committee_run_id"] = run_id
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
                json.dumps(
                    {
                        **{
                            "verdict": verdict,
                            "joint_decision": verdict.get("joint_decision"),
                            "entry_intel_snapshot_id": context.get("entry_intel_snapshot_id"),
                        },
                        **verdict_phase3_apply,
                    }
                ),
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
        if is_structural:
            _merge_structural_contract_and_diagnostics(
                cur,
                action_id=action_id,
                committee_run_id=run_id,
                verdict=verdict,
                reason_codes=reason_codes,
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
    model: str = Query(default="claude-4-sonnet"),
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
                try:
                    assert_live_committee_policy(
                        action,
                        _live_structural_only_enabled(cur),
                        is_structural_fn=is_structural_live_action,
                    )
                except HTTPException as hex_stream:
                    det = hex_stream.detail
                    result["error"] = det if isinstance(det, str) else json.dumps(det)
                    return
                is_structural = is_structural_live_action(action)

                if is_structural:
                    action_intent_stream = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
                    is_exit_stream = action_intent_stream == "EXIT"
                    if is_exit_stream:
                        exit_qty_sse = _fetch_live_symbol_position_qty(
                            cur, action.get("PORTFOLIO_ID"), action.get("SYMBOL")
                        )
                        struct_verdict = build_structural_exit_execution_only_verdict(
                            dict(action), exit_position_qty=exit_qty_sse
                        )
                        result["outputs"] = []
                        result["verdict"] = struct_verdict
                        summary0 = (struct_verdict.get("role_outputs") or [{}])[0].get("summary") or ""
                        out_queue.put(
                            (
                                "agent_turn",
                                {
                                    "action_id": action_id,
                                    "role": "Execution",
                                    "type": "agent_turn",
                                    "summary": summary0,
                                },
                            )
                        )
                        time.sleep(_STRUCTURAL_COMMITTEE_SSE_ROLE_DELAY_SEC)
                    else:
                        fd_sse = fetch_committee2_final_decision_for_action(cur, action_id)
                        if not fd_sse:
                            result["error"] = (
                                "Committee 2.0 required: Structural entry uses the hearing room. "
                                "Commit with this action_id, then use Sync Committee 2.0 in Live Portfolio Activity."
                            )
                            return
                        struct_verdict = structural_entry_verdict_from_committee2_final(fd_sse, dict(action))
                        role_outputs = list(struct_verdict.get("role_outputs") or [])
                        result["outputs"] = []
                        result["verdict"] = struct_verdict
                        out_queue.put(
                            (
                                "agent_turn",
                                {
                                    "action_id": action_id,
                                    "role": "Chair",
                                    "type": "agent_turn",
                                    "summary": "Committee 2.0 — replaying committed specialist summaries (no legacy structural evaluator).",
                                },
                            )
                        )
                        time.sleep(_STRUCTURAL_COMMITTEE_SSE_ROLE_DELAY_SEC)
                        for out in role_outputs:
                            out_queue.put(
                                (
                                    "role_summary",
                                    {
                                        "action_id": action_id,
                                        "role": out.get("role"),
                                        "stance": out.get("stance"),
                                        "confidence": out.get("confidence"),
                                        "summary": out.get("summary"),
                                    },
                                )
                            )
                            time.sleep(_STRUCTURAL_COMMITTEE_SSE_ROLE_DELAY_SEC)
                elif not _live_structural_only_enabled(cur):
                    context = _build_action_decision_context(cur, action)

                    def cb(ev_name: str, payload: dict):
                        out_queue.put((ev_name, {"action_id": action_id, **payload}))

                    outputs, verdict = _run_multiagent_dialogue(
                        cur,
                        model=model,
                        context=context,
                        persist_run_id=None,
                        emit=cb,
                        live_action=action,
                    )
                    result["outputs"] = outputs or []
                    result["verdict"] = verdict or {}
                else:
                    result["error"] = "Legacy committee stream is disabled under LIVE_STRUCTURAL_ONLY."
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
    model: str = Query(default="claude-4-sonnet"),
):
    def event_stream():
        conn = get_connection()
        try:
            cur = conn.cursor()
            action = _fetch_live_action(cur, action_id)
            if not action:
                yield _sse_event("error", {"message": "Action not found."})
                return
            yield _sse_event("start", {"action_id": action_id, "stage": "revalidation", "model": model})
            if is_structural_live_action(action):
                intent_rv = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
                if intent_rv == "EXIT":
                    exit_qty_rv = _fetch_live_symbol_position_qty(
                        cur, action.get("PORTFOLIO_ID"), action.get("SYMBOL")
                    )
                    sv = build_structural_exit_execution_only_verdict(
                        dict(action), exit_position_qty=exit_qty_rv
                    )
                    ro = (sv.get("role_outputs") or [{}])[0]
                    yield _sse_event(
                        "role_summary",
                        {
                            "action_id": action_id,
                            "role": ro.get("role"),
                            "stance": ro.get("stance"),
                            "confidence": ro.get("confidence"),
                            "summary": ro.get("summary"),
                        },
                    )
                    yield _sse_event(
                        "final",
                        {
                            "action_id": action_id,
                            "joint_decision": sv.get("joint_decision"),
                            "verdict": sv,
                            "committee_model": "STRUCTURAL_EXIT_EXECUTION_ONLY",
                        },
                    )
                    return
                fd_rv = fetch_committee2_final_decision_for_action(cur, action_id)
                if not fd_rv:
                    yield _sse_event(
                        "error",
                        {
                            "action_id": action_id,
                            "message": "Committee 2.0 final decision required for structural entry revalidation preview.",
                            "reason_codes": ["COMMITTEE2_FINAL_DECISION_REQUIRED"],
                        },
                    )
                    return
                sv = structural_entry_verdict_from_committee2_final(fd_rv, dict(action))
                role_outputs = list(sv.get("role_outputs") or [])
                for i, out in enumerate(role_outputs):
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
                    if i + 1 < len(role_outputs):
                        time.sleep(_STRUCTURAL_COMMITTEE_SSE_ROLE_DELAY_SEC)
                yield _sse_event(
                    "final",
                    {
                        "action_id": action_id,
                        "joint_decision": sv.get("joint_decision"),
                        "verdict": sv,
                        "committee_model": "COMMITTEE2",
                    },
                )
                return
            if _live_structural_only_enabled(cur):
                yield _sse_event(
                    "error",
                    {
                        "action_id": action_id,
                        "message": "Legacy revalidation preview is disabled under LIVE_STRUCTURAL_ONLY.",
                        "reason_codes": ["LEGACY_REVALIDATION_SSE_FORBIDDEN", "LIVE_STRUCTURAL_ONLY_ENFORCED"],
                    },
                )
                return
            context = _build_action_decision_context(cur, action)
            context["stage"] = "REVALIDATION_PREVIEW"
            context["revalidation"] = {
                "last_outcome": action.get("REVALIDATION_OUTCOME"),
                "last_ts": str(action.get("REVALIDATION_TS")) if action.get("REVALIDATION_TS") is not None else None,
            }
            outputs, verdict = _run_multiagent_dialogue(
                cur, model=model, context=context, persist_run_id=None, emit=None, live_action=action
            )
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
                broker_truth = _fetch_latest_broker_truth(
                    cur,
                    str(account_id),
                    str(action.get("SYMBOL") or ""),
                    action.get("ASSET_CLASS"),
                )
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

        # Phase 3A — verify broker universe type still matches config (mirrors Gate 2 at execute).
        _reval_portfolio_id = action.get("PORTFOLIO_ID")
        _reval_universe_type = str(action.get("BROKER_UNIVERSE_TYPE") or "").strip().upper()
        if _reval_universe_type and _reval_portfolio_id is not None:
            try:
                cur.execute(
                    "select IBKR_ACCOUNT_MODE from MIP.LIVE.LIVE_PORTFOLIO_CONFIG where PORTFOLIO_ID = %s",
                    (_reval_portfolio_id,),
                )
                _reval_cfg = fetch_all(cur)
                if _reval_cfg:
                    _reval_cfg_mode = str(_reval_cfg[0].get("IBKR_ACCOUNT_MODE") or "").strip().upper()
                    if _reval_cfg_mode and _reval_universe_type != _reval_cfg_mode:
                        raise HTTPException(
                            status_code=400,
                            detail=f"UNIVERSE_MISMATCH: action BROKER_UNIVERSE_TYPE={_reval_universe_type} "
                                   f"does not match portfolio IBKR_ACCOUNT_MODE={_reval_cfg_mode}.",
                        )
            except HTTPException:
                raise
            except Exception:
                pass  # best-effort; config join failure must not silently block revalidation

        symbol = action.get("SYMBOL")
        proposed_price = action.get("PROPOSED_PRICE")
        portfolio_id = action.get("PORTFOLIO_ID")
        action_intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
        is_exit = action_intent == "EXIT"
        quote_freshness_threshold_sec = 900
        max_bar_end_lag_sec: int | None = None
        try:
            cur.execute(
                """
                select coalesce(QUOTE_FRESHNESS_THRESHOLD_SEC, 900), MAX_BAR_END_LAG_SEC
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where PORTFOLIO_ID = %s
                limit 1
                """,
                (portfolio_id,),
            )
            cfg_row = cur.fetchone()
            if cfg_row and cfg_row[0] is not None:
                quote_freshness_threshold_sec = int(cfg_row[0])
            if cfg_row and len(cfg_row) > 1 and cfg_row[1] is not None:
                max_bar_end_lag_sec = int(cfg_row[1])
        except Exception:
            quote_freshness_threshold_sec = 900
            max_bar_end_lag_sec = None
        freshness_threshold_sec = quote_freshness_threshold_sec
        effective_entry_bar_age_threshold_sec = (
            min(quote_freshness_threshold_sec, max_bar_end_lag_sec)
            if max_bar_end_lag_sec is not None
            else quote_freshness_threshold_sec
        )
        refresh_info = {"attempted": False}
        direct_ibkr_one_min = None
        refresh_fallback_codes: list[str] = []
        if req.force_refresh_1m:
            refresh_info = _force_refresh_latest_one_minute_bars(
                cur,
                symbol,
                portfolio_id=int(action["PORTFOLIO_ID"]) if action.get("PORTFOLIO_ID") is not None else None,
            )
            if str(refresh_info.get("status") or "").upper() == "SUCCESS":
                direct_ibkr_one_min = _extract_latest_one_min_bar_from_refresh(refresh_info, symbol)
            else:
                refresh_fallback_codes.append("IBKR_DIRECT_REFRESH_FAILED")

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
        if (
            bar_age_sec > effective_entry_bar_age_threshold_sec
            and not is_exit
            and market_open_now
            and source != "BAR_FALLBACK"
        ):
            raise HTTPException(
                status_code=400,
                detail={
                    "message": (
                        f"IBKR bar is stale ({int(bar_age_sec)}s old > "
                        f"{int(effective_entry_bar_age_threshold_sec)}s effective entry threshold), revalidation blocked."
                    ),
                    "reason_codes": ["IBKR_BAR_STALE_ENTRY_BLOCKED"],
                    "bar_age_sec": int(bar_age_sec),
                    "quote_freshness_threshold_sec": int(quote_freshness_threshold_sec),
                    "max_bar_end_lag_sec": max_bar_end_lag_sec,
                    "effective_entry_bar_age_threshold_sec": int(effective_entry_bar_age_threshold_sec),
                },
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
        # Drop prior price-guard noise when revalidating an exit — it will be replaced by PASS + bypass tag.
        if is_exit:
            _exit_reval_strip = {"PRICE_GUARD_FAIL", "REDUCED_SIZE_DUE_TO_PRICE_DEVIATION"}
            existing_reason_codes = [rc for rc in existing_reason_codes if str(rc).upper() not in _exit_reval_strip]
        elif str(action.get("STATUS") or "").upper() not in (
            "EXECUTED",
            "EXECUTION_REQUESTED",
            "EXECUTION_PARTIAL",
        ):
            existing_reason_codes = _strip_recomputable_entry_bracket_codes(existing_reason_codes)
        reason_codes: list[str] = list(refresh_fallback_codes)
        reduced_size_factor = None
        target_open_condition_factor = 1.0
        if is_exit and bar_age_sec > quote_freshness_threshold_sec:
            reason_codes.append("EXIT_REVALIDATION_STALE_BAR_BYPASS")
        if (not is_exit) and (not market_open_now) and bar_age_sec > effective_entry_bar_age_threshold_sec:
            reason_codes.append("REVALIDATION_STALE_BAR_OUTSIDE_SESSION_ALLOWED")
        if (
            (not is_exit)
            and source == "BAR_FALLBACK"
            and bar_age_sec > effective_entry_bar_age_threshold_sec
        ):
            reason_codes.append("REVALIDATION_DAILY_BAR_FALLBACK")
            target_open_condition_factor = min(float(target_open_condition_factor), 0.85)
            reduced_size_factor = 0.75
        if source == "IBKR_DIRECT_1M":
            reason_codes.append("REVALIDATION_PRICE_FROM_IBKR_DIRECT")
            rp = refresh_info.get("payload") if isinstance(refresh_info, dict) else None
            if isinstance(rp, dict) and rp.get("fetched_at_utc"):
                reason_codes.append("IBKR_BAR_FETCH_INSTRUMENTATION_V1")
        if (
            max_bar_end_lag_sec is not None
            and effective_entry_bar_age_threshold_sec < quote_freshness_threshold_sec
        ):
            reason_codes.append("MAX_BAR_END_LAG_CAP_ACTIVE")

        if is_exit:
            # Live IB exits submit MKT; PROPOSED_PRICE is reference-only. A tight % band vs a 1m bar
            # would strand closes after normal price movement — do not fail exits on price guard.
            revalidation_outcome = "PASS"
            status = "REVALIDATED_PASS"
            target_open_condition_factor = 1.0
            reduced_size_factor = None
            reason_codes.append("EXIT_REVALIDATION_MARKET_BYPASS")
        elif deviation is None or deviation <= 0.02:
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
                if is_exit:
                    # Full-size market exit; do not shrink exit qty on news shock.
                    reason_codes.append("NEWS_REVALIDATION_CAUTION")
                else:
                    revalidation_outcome = "PASS_WITH_REDUCED_SIZE"
                    status = "REVALIDATED_PASS"
                    reduced_size_factor = min(reduced_size_factor or 1.0, 0.5)
                    reason_codes.append("NEWS_REVALIDATION_CAUTION")
            elif revalidation_outcome == "FAIL":
                reason_codes.append("NEWS_EVENT_SHOCK_BLOCK")
        elif news_causes_caution and revalidation_outcome == "PASS":
            reason_codes.append("NEWS_REVALIDATION_CAUTION")
        if (
            not is_exit
            and str(action.get("STATUS") or "").upper()
            not in ("EXECUTED", "EXECUTION_REQUESTED", "EXECUTION_PARTIAL")
        ):
            action_for_bracket = dict(action)
            action_for_bracket["REVALIDATION_PRICE"] = ref_price
            bracket_recompute = _preflight_entry_bracket_hard_block_reason_codes(cur, action_for_bracket)
            for brc in bracket_recompute:
                if brc not in reason_codes:
                    reason_codes.append(brc)
        merged_reason_codes: list[str] = []
        for rc in existing_reason_codes + reason_codes:
            rc_text = str(rc)
            if rc_text and rc_text not in merged_reason_codes:
                merged_reason_codes.append(rc_text)

        if (not is_exit) and is_structural_live_action(action):
            from app.committee.agentic_authority import (
                evaluate_authority_gate,
                reconcile_agentic_authority_reason_codes,
            )

            agentic_gate = evaluate_authority_gate(conn, action_id)
            merged_reason_codes = reconcile_agentic_authority_reason_codes(
                merged_reason_codes,
                authority_status=agentic_gate.get("authority_status"),
                is_stale=bool(agentic_gate.get("is_stale")),
                gate_ok=(
                    agentic_gate.get("gate_ok")
                    if agentic_gate.get("gate_enabled")
                    else True
                ),
            )

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

        # Client-assertion pre-gate: if the caller supplied context fields,
        # validate them against the action row before running the full gate chain.
        # These are assertions only — the Phase 1 frozen-context gates below remain
        # authoritative. A mismatch here means the UI sent stale/wrong context.
        if req.portfolio_id is not None and req.portfolio_id != action.get("PORTFOLIO_ID"):
            raise HTTPException(
                status_code=400,
                detail=f"Client assertion mismatch: portfolio_id={req.portfolio_id} vs action.PORTFOLIO_ID={action.get('PORTFOLIO_ID')}.",
            )
        if req.ibkr_account_id and str(req.ibkr_account_id).strip() != str(action.get("IBKR_ACCOUNT_ID") or "").strip():
            raise HTTPException(
                status_code=400,
                detail=f"Client assertion mismatch: ibkr_account_id={req.ibkr_account_id} vs action.IBKR_ACCOUNT_ID={action.get('IBKR_ACCOUNT_ID')}.",
            )
        if req.broker_name and str(req.broker_name).strip().upper() != str(action.get("BROKER_NAME") or "").strip().upper():
            raise HTTPException(
                status_code=400,
                detail=f"Client assertion mismatch: broker_name={req.broker_name} vs action.BROKER_NAME={action.get('BROKER_NAME')}.",
            )
        if req.broker_universe_type and str(req.broker_universe_type).strip().upper() != str(action.get("BROKER_UNIVERSE_TYPE") or "").strip().upper():
            raise HTTPException(
                status_code=400,
                detail=f"Client assertion mismatch: broker_universe_type={req.broker_universe_type} vs action.BROKER_UNIVERSE_TYPE={action.get('BROKER_UNIVERSE_TYPE')}.",
            )

        assert_legacy_execute_forbidden(action, _live_structural_only_enabled(cur))

        # LPA stale-lifecycle gate. A structural ENTRY action whose
        # parent proposal is no longer CURRENT (EXPIRED or from a
        # superseded board run) must never reach the broker. The
        # overview already hides these rows from pending_decisions;
        # this check is defense in depth for direct API callers.
        # EXIT actions are exempt — closing a live position must
        # remain available regardless of proposal lineage.
        _exec_intent_for_freshness = _normalize_action_intent(
            action.get("SIDE"), action.get("ACTION_INTENT")
        )
        if _exec_intent_for_freshness != "EXIT" and is_structural_live_action(action):
            freshness = _compute_action_proposal_freshness(cur, action.get("PROPOSAL_ID"))
            if freshness != "CURRENT":
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": (
                            "Stale proposal — this action's parent proposal is no longer "
                            "current. The next daily pipeline run will terminalize it; "
                            "reject it explicitly if you need to clean it up sooner."
                        ),
                        "reason_codes": ["PROPOSAL_EXPIRED_OR_SUPERSEDED"],
                        "proposal_freshness": freshness,
                    },
                )

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

        # Stage 4d — Agentic authority gate (Submit gating switch).
        # Only applies to STRUCTURAL ENTRY actions. EXIT actions never run
        # shadow board and must not be gated by agentic authority. The gate
        # is fully disabled when `AGENTIC_AUTHORITY_ENABLED` is false in
        # APP_CONFIG; the helper evaluates the flag and returns gate_enabled
        # in the verdict.
        agentic_gate_verdict = None
        _exec_intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
        is_exit = _exec_intent == "EXIT"
        if _exec_intent != "EXIT" and is_structural_live_action(action):
            agentic_gate_verdict = evaluate_authority_gate(conn, action_id)
            if (
                agentic_gate_verdict.get("gate_enabled")
                and not agentic_gate_verdict.get("gate_ok")
            ):
                for rc in agentic_gate_verdict.get("reason_codes") or []:
                    if rc not in reason_codes:
                        reason_codes.append(rc)

        cur.execute(
            """
            select
              IBKR_ACCOUNT_ID, BROKER_NAME, ADAPTER_MODE, MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, BUST_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC, DRIFT_STATUS, IS_ACTIVE,
              IBKR_ACCOUNT_MODE, IS_EXECUTION_ENABLED, REAL_MONEY_ENABLED,
              DRAWDOWN_STOP_PCT, MAX_SLIPPAGE_PCT, COOLDOWN_BARS,
              ALLOW_SHORT_SELLING, TRAIL_ENABLED
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
        account_id = str(cfg.get("IBKR_ACCOUNT_ID") or "").strip()
        drift_status = (cfg.get("DRIFT_STATUS") or "").upper()
        if drift_status and drift_status not in ("OK", "CLEAR", "HEALTHY"):
            reason_codes.append("BROKER_TRUTH_DRIFT_UNRESOLVED")

        # ── Broker-universe safety gates ──────────────────────────────────────
        # Phase 1: enforce frozen execution context on the action row and
        # validate it against the live portfolio config before any order can
        # reach the broker. Gates run in strict order; LIVE_EXECUTION_MODE=IBKR
        # may only select the broker submit path at the end, after all pass.

        # Gate 1 — frozen context completeness
        action_broker_name   = str(action.get("BROKER_NAME")         or "").strip()
        action_ibkr_id       = str(action.get("IBKR_ACCOUNT_ID")     or "").strip()
        action_universe_type = str(action.get("BROKER_UNIVERSE_TYPE") or "").strip()
        if not action_broker_name:
            raise HTTPException(status_code=400, detail="Action BROKER_NAME is missing — action context is incomplete.")
        if not action_ibkr_id:
            raise HTTPException(status_code=400, detail="Action IBKR_ACCOUNT_ID is missing — action context is incomplete.")
        if not action_universe_type:
            raise HTTPException(status_code=400, detail="Action BROKER_UNIVERSE_TYPE is missing — action context is incomplete.")

        # Gate 2 — frozen context must match live portfolio config
        config_broker     = str(cfg.get("BROKER_NAME")      or "IBKR").strip()
        ibkr_account_mode = str(cfg.get("IBKR_ACCOUNT_MODE") or "UNKNOWN").upper()
        if action_broker_name != config_broker:
            raise HTTPException(status_code=400, detail=f"Broker mismatch: action={action_broker_name}, config={config_broker}.")
        if action_ibkr_id != account_id:
            raise HTTPException(status_code=400, detail=f"Account mismatch: action={action_ibkr_id}, config={account_id}.")
        if action_universe_type.upper() != ibkr_account_mode:
            raise HTTPException(status_code=400, detail=f"Universe mismatch: action={action_universe_type}, config={ibkr_account_mode}.")

        # Gate 3 — execution must be explicitly enabled on this portfolio
        if not cfg.get("IS_EXECUTION_ENABLED"):
            raise HTTPException(status_code=400, detail="Execution not enabled on this portfolio (IS_EXECUTION_ENABLED=false).")

        # Gate 4 — account mode must be PAPER or REAL (UNKNOWN fails closed)
        if ibkr_account_mode == "UNKNOWN":
            raise HTTPException(status_code=400, detail="IBKR_ACCOUNT_MODE is UNKNOWN — set to PAPER or REAL before executing.")

        # Gate 5 — REAL requires dual approval: env flag AND DB flag
        if ibkr_account_mode == "REAL":
            if str(os.getenv("ENABLE_REAL_MONEY_TRADING", "false")).lower() != "true":
                raise HTTPException(status_code=400, detail="Real-money execution blocked: ENABLE_REAL_MONEY_TRADING env flag not set.")
            if not cfg.get("REAL_MONEY_ENABLED"):
                raise HTTPException(status_code=400, detail="Real-money execution blocked: REAL_MONEY_ENABLED=false on portfolio config.")

        # Gate 5b — REAL: short selling requires per-portfolio explicit opt-in.
        # ALLOW_SHORT_SELLING=FALSE (default) blocks all non-exit SELL entries for REAL portfolios.
        # Paper uses the global LIVE_ENFORCE_LONG_ONLY APP_CONFIG flag instead.
        if ibkr_account_mode == "REAL" and not is_exit:
            action_side = str(action.get("SIDE") or "").upper()
            if action_side == "SELL" and not cfg.get("ALLOW_SHORT_SELLING"):
                raise HTTPException(
                    status_code=400,
                    detail="Short selling is not enabled for this real-money portfolio (ALLOW_SHORT_SELLING=false).",
                )

        # Gate 6b — session probe: connected IBKR session must expose the expected account.
        # Probe before any broker contact so the operator gets a clear 409 instead of a
        # confusing 502 from the TWS/Gateway adapter.
        _exec_portfolio_id = action.get("PORTFOLIO_ID")
        _exec_account_id   = action.get("IBKR_ACCOUNT_ID")
        if _exec_account_id:
            _exec_params = _snapshot_sync_params_for_portfolio(_exec_portfolio_id)
            _probe = _probe_ibkr_session(
                host=_exec_params["host"],
                port=_exec_params["port"],
                client_id=9410,
                expected_account=_exec_account_id,
            )
            if not _probe.get("account_match"):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "SESSION_MISMATCH",
                        "status": _probe.get("status"),
                        "detected_accounts": _probe.get("detected_accounts", []),
                        "expected_account": _exec_account_id,
                        "message": _probe.get(
                            "message",
                            "IBKR session does not match this portfolio. Start the correct TWS/Gateway before executing.",
                        ),
                    },
                )

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

        unmapped_exec_summary = _recent_unmapped_execution_summary(
            cur,
            int(action.get("PORTFOLIO_ID")),
            str(account_id),
            lookback_days=14,
        )
        if int(unmapped_exec_summary.get("count") or 0) > 0:
            reason_codes.append("BROKER_EXECUTION_UNMAPPED")

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
        exec_intent = _normalize_action_intent(action.get("SIDE"), action.get("ACTION_INTENT"))
        jd_exec = _fetch_committee_joint_decision_for_action(cur, action.get("COMMITTEE_RUN_ID"))
        committee_overrides_news_caution = _committee_joint_decision_explicitly_allows_trade(jd_exec, exec_intent)
        if news_event_shock_flag and news_freshness_bucket in ("FRESH", "OVERNIGHT"):
            reason_codes.append("NEWS_EXECUTION_BLOCKED_EVENT_SHOCK")
        elif news_context_state in ("CAUTIONARY", "DESTABILIZING"):
            if not committee_overrides_news_caution:
                reason_codes.append("NEWS_EXECUTION_CAUTION")

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

        # ── Phase 3A safety gates ──────────────────────────────────────────────
        # For REAL accounts these are hard HTTPException blocks (400/409).
        # For PAPER they add reason_codes (soft 409 via the existing block below).
        #
        # is_exit is re-derived again later in the execution-path block; define it
        # here from the already-computed exec_intent so these entry-only gates can
        # reference it (they run before that later assignment).
        is_exit = exec_intent == "EXIT"

        # Gate: DRAWDOWN_STOP_PCT — block new entries when NAV has drawn down past threshold.
        # Baseline = rolling max NAV over LIVE_DRAWDOWN_BASELINE_WINDOW_DAYS (APP_CONFIG, default 30d).
        _drawdown_stop_pct = cfg.get("DRAWDOWN_STOP_PCT")
        if not is_exit and nav_eur is not None and _drawdown_stop_pct is not None:
            try:
                _drawdown_window_cfg = _read_app_config(cur, ["LIVE_DRAWDOWN_BASELINE_WINDOW_DAYS"])
                _drawdown_window_days = int(
                    _drawdown_window_cfg.get("LIVE_DRAWDOWN_BASELINE_WINDOW_DAYS") or 30
                )
                cur.execute(
                    """
                    select MAX(NET_LIQUIDATION_EUR) as PEAK_NAV
                    from MIP.LIVE.BROKER_SNAPSHOTS
                    where IBKR_ACCOUNT_ID = %s
                      and SNAPSHOT_TYPE = 'NAV'
                      and SNAPSHOT_TS >= DATEADD(day, -%s, CURRENT_TIMESTAMP())
                    """,
                    (account_id, _drawdown_window_days),
                )
                _peak_rows = fetch_all(cur)
                _peak_nav = float((_peak_rows[0] or {}).get("PEAK_NAV") or 0.0) if _peak_rows else 0.0
                if _peak_nav > 0 and nav_eur > 0:
                    _drawdown = (_peak_nav - nav_eur) / _peak_nav
                    if _drawdown > float(_drawdown_stop_pct):
                        _dd_msg = (
                            f"Drawdown stop exceeded: current NAV drawdown {_drawdown:.1%} "
                            f"> DRAWDOWN_STOP_PCT {float(_drawdown_stop_pct):.1%}. "
                            "Exits are still allowed."
                        )
                        if ibkr_account_mode == "REAL":
                            raise HTTPException(status_code=400, detail=_dd_msg)
                        reason_codes.append("DRAWDOWN_STOP_EXCEEDED")
            except HTTPException:
                raise
            except Exception:
                pass  # best-effort; never block execution on drawdown query failure

        # Gate: MAX_SLIPPAGE_PCT — block entries when revalidation price has drifted beyond
        # the per-portfolio slippage ceiling (distinct from the bracket realism BPS system).
        _max_slippage_pct = cfg.get("MAX_SLIPPAGE_PCT")
        if not is_exit and _max_slippage_pct is not None:
            _rev_px = action.get("REVALIDATION_PRICE")
            _prop_px = action.get("PROPOSED_PRICE")
            if _rev_px is not None and _prop_px is not None and float(_prop_px) > 0:
                _price_drift = abs(float(_rev_px) - float(_prop_px)) / float(_prop_px)
                if _price_drift > float(_max_slippage_pct):
                    _slip_msg = (
                        f"Price drift {_price_drift:.1%} exceeds MAX_SLIPPAGE_PCT "
                        f"{float(_max_slippage_pct):.1%} for this portfolio. "
                        "Re-run revalidation after the price settles."
                    )
                    if ibkr_account_mode == "REAL":
                        raise HTTPException(status_code=400, detail=_slip_msg)
                    reason_codes.append("MAX_SLIPPAGE_PCT_EXCEEDED")

        # Gate: COOLDOWN_BARS — block new entries within N bars of the last fill.
        # Bar size proxy: LIVE_BAR_MINUTES APP_CONFIG key (default 5 min).
        _cooldown_bars = cfg.get("COOLDOWN_BARS")
        if not is_exit and _cooldown_bars is not None and int(_cooldown_bars) > 0:
            try:
                _bar_min_cfg = _read_app_config(cur, ["LIVE_BAR_MINUTES"])
                _bar_minutes = int(_bar_min_cfg.get("LIVE_BAR_MINUTES") or 5)
                cur.execute(
                    """
                    select MAX(FILLED_AT) as LAST_FILL_TS
                    from MIP.LIVE.LIVE_ORDERS
                    where PORTFOLIO_ID = %s
                      and STATUS = 'FILLED'
                    """,
                    (action.get("PORTFOLIO_ID"),),
                )
                _fill_rows = fetch_all(cur)
                _last_fill_ts = (_fill_rows[0] or {}).get("LAST_FILL_TS") if _fill_rows else None
                if _last_fill_ts is not None:
                    _elapsed_min = (now_utc - _last_fill_ts.replace(tzinfo=timezone.utc)).total_seconds() / 60.0
                    _bars_since = _elapsed_min / max(_bar_minutes, 1)
                    if _bars_since < int(_cooldown_bars):
                        _cool_msg = (
                            f"Cooldown active: only {_bars_since:.1f} bars since last fill, "
                            f"need {int(_cooldown_bars)}. Entries are blocked during cooldown."
                        )
                        if ibkr_account_mode == "REAL":
                            raise HTTPException(status_code=400, detail=_cool_msg)
                        reason_codes.append("COOLDOWN_BARS_ACTIVE")
            except HTTPException:
                raise
            except Exception:
                pass  # best-effort

        # ── End Phase 3A safety gates ──────────────────────────────────────────

        cur.execute(
            """
            with latest_sync_ts as (
                select max(SNAPSHOT_TS) as SNAPSHOT_TS
                from MIP.LIVE.BROKER_SNAPSHOTS
                where IBKR_ACCOUNT_ID = %s
            )
            select count(distinct SYMBOL) as OPEN_POSITIONS
            from MIP.LIVE.BROKER_SNAPSHOTS s
            join latest_sync_ts ls on s.SNAPSHOT_TS = ls.SNAPSHOT_TS
            where s.SNAPSHOT_TYPE = 'POSITION'
              and s.IBKR_ACCOUNT_ID = %s
              and coalesce(s.POSITION_QTY, 0) <> 0
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
                with latest_sync_ts as (
                    select max(SNAPSHOT_TS) as SNAPSHOT_TS
                    from MIP.LIVE.BROKER_SNAPSHOTS
                    where IBKR_ACCOUNT_ID = %s
                )
                select
                    upper(s.SYMBOL) as SYMBOL,
                    upper(coalesce(s.CURRENCY, '')) as CURRENCY,
                    upper(coalesce(s.SECURITY_TYPE, '')) as SECURITY_TYPE,
                    s.POSITION_QTY as POSITION_QTY,
                    s.SNAPSHOT_TS as SNAPSHOT_TS
                from MIP.LIVE.BROKER_SNAPSHOTS s
                join latest_sync_ts ls on s.SNAPSHOT_TS = ls.SNAPSHOT_TS
                where s.SNAPSHOT_TYPE = 'POSITION'
                  and s.IBKR_ACCOUNT_ID = %s
                  and coalesce(s.POSITION_QTY, 0) <> 0
                order by s.SYMBOL, s.CURRENCY
                """,
                (account_id, account_id),
            )
            broker_pos_rows = fetch_all(cur)
            action_symbol = str(action.get("SYMBOL") or "").strip()
            asset_class = action.get("ASSET_CLASS")
            short_symbols: list[str] = []
            symbol_position_qty: float | None = None
            snapshot_ts = None
            matched_qty_sum = 0.0
            match_rows = 0
            for row in broker_pos_rows:
                sym = str(row.get("SYMBOL") or "").upper()
                ccy = str(row.get("CURRENCY") or "").upper()
                sec = str(row.get("SECURITY_TYPE") or "").upper()
                qty = float(row.get("POSITION_QTY") or 0.0)
                if snapshot_ts is None:
                    snapshot_ts = row.get("SNAPSHOT_TS")
                if _broker_position_matches_live_action(sym, ccy, sec, action_symbol, asset_class):
                    matched_qty_sum += qty
                    match_rows += 1
                if _negative_broker_line_blocks_long_only_entry(
                    sym, ccy, sec, qty, action_symbol, asset_class
                ):
                    label = f"{sym}/{ccy}" if sec == "CASH" and ccy and sym != ccy else sym
                    short_symbols.append(label)
            if snapshot_ts is None:
                cur.execute(
                    """
                    select max(SNAPSHOT_TS) as SNAPSHOT_TS
                    from MIP.LIVE.BROKER_SNAPSHOTS
                    where IBKR_ACCOUNT_ID = %s
                    """,
                    (account_id,),
                )
                ts_only = fetch_all(cur)
                snapshot_ts = (ts_only[0] or {}).get("SNAPSHOT_TS") if ts_only else None
            if match_rows:
                symbol_position_qty = matched_qty_sum
            long_only_guard["short_symbols"] = sorted(set(short_symbols))
            long_only_guard["symbol_position_qty"] = symbol_position_qty
            long_only_guard["snapshot_ts"] = (
                snapshot_ts.isoformat() if hasattr(snapshot_ts, "isoformat") else (str(snapshot_ts) if snapshot_ts is not None else None)
            )
            if side != "BUY":
                reason_codes.append("ENTRY_SIDE_NOT_ALLOWED_LONG_ONLY")
            if long_only_guard["short_symbols"]:
                reason_codes.append("BROKER_SHORT_POSITION_OUT_OF_POLICY")
            # BUY reduces an existing short; do not block as "you are short" (common FX/stock cover).
            if symbol_position_qty is not None and symbol_position_qty < 0 and side != "BUY":
                reason_codes.append("SYMBOL_SHORT_POSITION_OUT_OF_POLICY")

        max_positions = cfg.get("MAX_POSITIONS")
        if not is_exit and max_positions is not None and open_positions >= int(max_positions):
            reason_codes.append("MAX_POSITIONS_EXCEEDED")

        proposed_qty = action.get("PROPOSED_QTY")
        if is_exit and (proposed_qty is None or float(proposed_qty) <= 0):
            broker_truth_for_exit = _fetch_latest_broker_truth(
                cur, str(account_id), str(action.get("SYMBOL")), action.get("ASSET_CLASS")
            )
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

        # Idempotency is scoped to ACTION_ID so duplicate LIVE_ACTION rows for the same PROPOSAL_ID
        # cannot block each other. Rows written before this change used portfolio:PROPOSAL_ID:attempt
        # for the parent key; for the same action only, we still honor that legacy parent key so
        # retries after deploy cannot double-submit.
        portfolio_id_val = action.get("PORTFOLIO_ID")
        idempotency_key = f"{portfolio_id_val}:{action_id}:{req.attempt_n}"
        proposal_id_val = action.get("PROPOSAL_ID")
        legacy_parent_key = (
            f"{portfolio_id_val}:{proposal_id_val}:{req.attempt_n}" if proposal_id_val is not None else None
        )
        if legacy_parent_key:
            cur.execute(
                """
                select ORDER_ID, STATUS
                from MIP.LIVE.LIVE_ORDERS
                where IDEMPOTENCY_KEY = %s
                   or (ACTION_ID = %s and IDEMPOTENCY_KEY = %s)
                limit 1
                """,
                (idempotency_key, action_id, legacy_parent_key),
            )
        else:
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
            st = (existing_order.get("STATUS") or "").upper()
            if st == "FILLED":
                return {
                    "ok": True,
                    "action_id": action_id,
                    "status": "EXECUTION_REQUESTED",
                    "order_id": existing_order.get("ORDER_ID"),
                    "idempotency_key": idempotency_key,
                    "mode": "PAPER_PLACEHOLDER",
                    "idempotent_replay": True,
                }
            if st == "UNCONFIRMED_AT_BROKER":
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": (
                            "A prior submit for this execution attempt may have reached IBKR but was not "
                            "confirmed in our broker snapshot. Do not press Submit again for the same attempt—"
                            "refresh IB, verify orders/fills in TWS, then reconcile. Use a higher attempt "
                            "number only if IB confirms nothing was placed."
                        ),
                        "reason_codes": ["PRIOR_SUBMIT_BROKER_UNCONFIRMED"],
                        "order_id": existing_order.get("ORDER_ID"),
                        "broker_order_id": existing_order.get("BROKER_ORDER_ID"),
                        "idempotency_key": idempotency_key,
                    },
                )
            if st in ("CANCELED", "CANCELLED", "REJECTED"):
                # Prior attempt ended at broker or was reconciled locally; same attempt_n may submit again.
                pass
            else:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": (
                            "This execution attempt already has live order rows. Duplicate IB submission is blocked. "
                            "Check LIVE orders and IBKR; use attempt_n+1 only after confirming the prior order state."
                        ),
                        "reason_codes": ["LIVE_IDEMPOTENT_SUBMIT_ALREADY_RECORDED"],
                        "order_id": existing_order.get("ORDER_ID"),
                        "order_status": st,
                        "idempotency_key": idempotency_key,
                    },
                )

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
                    "unmapped_execution_summary": unmapped_exec_summary,
                    "news_context_snapshot": news_snapshot,
                },
            )
            block_msg = "Execution blocked by safety gates."
            if "BROKER_SHORT_POSITION_OUT_OF_POLICY" in final_reason_codes:
                shorts = long_only_guard.get("short_symbols") or []
                block_msg = (
                    "Long-only policy: the broker snapshot shows short quantity on one or more position lines "
                    f"({', '.join(shorts) if shorts else 'see long_only_guard'}). "
                    "This is usually a short stock/FX pair or a position line to close—not your new BUY itself. "
                    "Refresh IB, cover those lines, or set LIVE_BLOCK_ON_BROKER_SHORT=false only if you accept shorts."
                )
            elif "SYMBOL_SHORT_POSITION_OUT_OF_POLICY" in final_reason_codes:
                block_msg = (
                    "Long-only policy: you already have a short position in this symbol on the broker snapshot. "
                    "Close or cover it before adding to the same side, or refresh from IB if the snapshot is stale."
                )
            agentic_block = None
            if agentic_gate_verdict and any(
                str(rc).startswith("AGENTIC_AUTHORITY_")
                for rc in final_reason_codes
            ):
                agentic_block = {
                    "gate_enabled": agentic_gate_verdict.get("gate_enabled"),
                    "reason_code": agentic_gate_verdict.get("reason_code"),
                    "tooltip": agentic_gate_verdict.get("tooltip"),
                    "authority_mode": agentic_gate_verdict.get("authority_mode"),
                    "authority_status": agentic_gate_verdict.get("authority_status"),
                    "is_stale": agentic_gate_verdict.get("is_stale"),
                    "authority_id": agentic_gate_verdict.get("authority_id"),
                }
                if agentic_gate_verdict.get("tooltip"):
                    block_msg = agentic_gate_verdict["tooltip"]
            raise HTTPException(
                status_code=409,
                detail={
                    "message": block_msg,
                    "reason_codes": final_reason_codes,
                    "long_only_guard": long_only_guard,
                    "unmapped_execution_summary": unmapped_exec_summary if "BROKER_EXECUTION_UNMAPPED" in final_reason_codes else None,
                    "agentic_authority": agentic_block,
                },
            )

        order_id = str(uuid.uuid4())
        entry_price = float(action.get("REVALIDATION_PRICE") or action.get("PROPOSED_PRICE"))
        qty_ordered = float(proposed_qty)
        is_structural = is_structural_live_action(action)
        structural_direction = (action.get("DIRECTION") or "").upper() if is_structural else None
        if structural_direction == "SHORT" and not is_exit:
            side = "SELL"
            exit_side = "BUY"
        elif structural_direction == "LONG" and not is_exit:
            side = "BUY"
            exit_side = "SELL"
        else:
            exit_side = "SELL" if side == "BUY" else "BUY"
        structural_oca_group = str(uuid.uuid4())[:8] if is_structural else None
        adapter_mode = str(cfg.get("ADAPTER_MODE") or "PAPER").upper()
        execution_mode = str(os.getenv("LIVE_EXECUTION_MODE", "AUTO")).upper()
        # Gate 6 — broker submit path selection. LIVE_EXECUTION_MODE=IBKR may
        # only select the IBKR submit path here, AFTER all safety gates above
        # have passed. It cannot bypass frozen-context checks, IS_EXECUTION_ENABLED,
        # IBKR_ACCOUNT_MODE, or the REAL dual-approval requirement.
        use_ibkr_submit = adapter_mode == "LIVE"
        if execution_mode == "IBKR":
            use_ibkr_submit = True
        elif execution_mode == "PLACEHOLDER":
            use_ibkr_submit = False

        # Fix #1 — REAL placeholder path is forbidden (fail closed).
        # Real-money execution MUST use the IBKR submit path. Paper validated the
        # IBKR submit path (ADAPTER_MODE=LIVE against the paper IBKR account); real
        # money must inherit that exact tested path. The placeholder builder is
        # distinct code that real money has never exercised, so it is blocked here
        # BEFORE any order construction or broker contact.
        if ibkr_account_mode == "REAL" and not use_ibkr_submit:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": (
                        "Real-money execution must use the IBKR submit path. "
                        "Placeholder execution is not allowed for REAL portfolios."
                    ),
                    "reason_codes": ["REAL_PLACEHOLDER_PATH_FORBIDDEN"],
                    "adapter_mode": adapter_mode,
                    "execution_mode": execution_mode,
                },
            )

        stop_loss_pct_default = float(cfg.get("BUST_PCT")) if cfg.get("BUST_PCT") is not None else None
        target_return, stop_loss_pct, bracket_src = _load_executable_entry_bracket_for_action(
            cur,
            action=action,
            committee_run_id=action.get("COMMITTEE_RUN_ID"),
            bust_pct_default=stop_loss_pct_default,
        )
        if bracket_src == "blocked":
            ps_blk = _parse_variant(action.get("PARAM_SNAPSHOT"))
            eb_blk = ps_blk.get("executable_bracket") if isinstance(ps_blk, dict) else None
            blk_codes = (
                list(eb_blk.get("reason_codes"))
                if isinstance(eb_blk, dict) and eb_blk.get("reason_codes")
                else ["LIVE_BRACKET_NOT_VIABLE_WITHIN_GUARDRAILS"]
            )
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Execution blocked: live bracket not viable at committee calibration (see reason_codes).",
                    "reason_codes": blk_codes,
                },
            )

        # Structural entries: COMMITTEE_VERDICT / PARAM_SNAPSHOT may predate joint_decision TP/SL
        # (legacy apply). IB submit requires positive target_return + stop_loss_pct before risk gates.
        heal_meta = {"attempted": False, "ok": False, "bracket_src_before": bracket_src}
        if (
            is_structural
            and not is_exit
            and (
                target_return is None
                or float(target_return) <= 0
                or stop_loss_pct is None
                or float(stop_loss_pct) <= 0
            )
        ):
            heal_meta["attempted"] = True
            try:
                jd_h = build_structural_entry_joint_decision(dict(action))
                if isinstance(jd_h, dict):
                    tr_h, sl_h = _live_target_and_stop_from_joint_decision(
                        jd_h, stop_loss_pct_default
                    )
                    if (
                        tr_h is not None
                        and sl_h is not None
                        and float(tr_h) > 0
                        and float(sl_h) > 0
                    ):
                        target_return = float(tr_h)
                        stop_loss_pct = float(sl_h)
                        heal_meta["ok"] = True
            except Exception:
                _log.exception(
                    "structural execute: bracket self-heal (build_structural_entry_joint_decision) failed"
                )
            try:
                ps_h = _parse_variant(action.get("PARAM_SNAPSHOT"))
                diag_h = ps_h.get("structural_diagnostics_v1") if isinstance(ps_h, dict) else {}
                if not isinstance(diag_h, dict):
                    diag_h = {}
                diag_h["structural_self_heal"] = {
                    **heal_meta,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
                _merge_live_action_param_snapshot_patch(
                    action_id,
                    {"structural_diagnostics_v1": diag_h},
                )
            except Exception:
                _log.exception("structural execute: failed to persist self-heal diagnostics")

        if (
            use_ibkr_submit
            and is_structural
            and not is_exit
            and (
                target_return is None
                or float(target_return) <= 0
                or stop_loss_pct is None
                or float(stop_loss_pct) <= 0
            )
        ):
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Structural execution contract incomplete: missing TP/SL after committee and self-heal.",
                    "reason_codes": ["STRUCT_SUBMIT_CONTRACT_INCOMPLETE"],
                    "structural_self_heal": heal_meta,
                },
            )

        tp_price, sl_price = _live_entry_tp_sl_prices(
            str(side),
            float(entry_price),
            target_return,
            stop_loss_pct,
        )
        structural_trail_amount: float | None = None
        structural_trail_percent: float | None = None
        structural_exit_policy: str = exit_policy_service.EXIT_POLICY_FIXED
        if is_structural and not is_exit:
            inv_level = action.get("INVALIDATION_LEVEL")
            if inv_level is not None:
                sl_price = float(inv_level)

            # Trailing Stop Phase 1: EXIT_POLICY-driven execution.
            # The persisted contract on LIVE_ACTIONS is the source of truth.
            # Legacy TRAIL_ENABLED / TRAIL_DRY_RUN flags are no longer
            # consulted here; they remain in APP_CONFIG only for the
            # deferred Phase 2 replacement path.
            structural_exit_policy = (
                str(action.get("EXIT_POLICY") or "").strip().upper()
                or exit_policy_service.EXIT_POLICY_FIXED
            )

            # Fail-closed consistency guard. If the persisted action carries
            # any trailing intent (TRAIL_STATUS = REQUESTED or TRAIL_PARAMS
            # populated) but EXIT_POLICY did not resolve to TRAIL_BRACKET,
            # something upstream is inconsistent (e.g. read-side SELECT
            # missing the new EXIT_POLICY columns, partial migration, or
            # manual SQL edit). Refuse to execute rather than silently
            # downgrading to a fixed stop.
            persisted_trail_status = (
                str(action.get("TRAIL_STATUS") or "").strip().upper()
            )
            persisted_trail_params_raw = action.get("TRAIL_PARAMS")
            has_trail_params = False
            if isinstance(persisted_trail_params_raw, dict):
                has_trail_params = bool(persisted_trail_params_raw)
            elif isinstance(persisted_trail_params_raw, str):
                stripped = persisted_trail_params_raw.strip()
                has_trail_params = bool(stripped) and stripped.lower() not in (
                    "null",
                    "{}",
                )
            trailing_intent_persisted = (
                persisted_trail_status == exit_policy_service.TRAIL_STATUS_REQUESTED
                or has_trail_params
            )
            if (
                trailing_intent_persisted
                and structural_exit_policy != exit_policy_service.EXIT_POLICY_TRAIL
            ):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": (
                            "Action carries trailing intent (TRAIL_STATUS or "
                            "TRAIL_PARAMS) but EXIT_POLICY did not resolve to "
                            "TRAIL_BRACKET. Refusing to execute to avoid silent "
                            "downgrade to a fixed stop. Investigate the "
                            "EXIT_POLICY column on LIVE_ACTIONS for this row "
                            "and any read-side SELECT that may be omitting it."
                        ),
                        "reason_codes": ["EXIT_POLICY_INCONSISTENT"],
                        "exit_policy": structural_exit_policy,
                        "trail_status": persisted_trail_status or None,
                        "has_trail_params": has_trail_params,
                    },
                )

            if structural_exit_policy == exit_policy_service.EXIT_POLICY_TRAIL:
                # Per-portfolio trail CERTIFICATION gate (Phase 3A) — checked before global kill switch.
                #
                # TRAIL_ENABLED is a real-money readiness CERTIFICATION GATE, not a long-term
                # prohibition. Trailing stops are a required risk-management / profit-locking
                # mechanism for this operator workflow (trades are not continuously monitored).
                # The gate exists only to prevent ACCIDENTAL real-money trailing orders before
                # the full trailing path is verified end-to-end: order placement, broker
                # persistence, reconciliation, cancel handling, and UI visibility.
                #
                # For REAL accounts this is a hard block until certified (TRAIL_ENABLED=true).
                # For PAPER it adds a reason_code so trailing can be exercised and verified.
                if not cfg.get("TRAIL_ENABLED"):
                    _trail_portfolio_msg = (
                        "Trailing stop pending real-money certification: TRAIL_ENABLED=false on this "
                        "portfolio config. Trailing-stop support must be verified end-to-end "
                        "(placement, broker persistence, reconciliation, cancel handling, UI visibility) "
                        "before real-money trailing orders are permitted. Set TRAIL_ENABLED=true once "
                        "certified."
                    )
                    if ibkr_account_mode == "REAL":
                        raise HTTPException(
                            status_code=409,
                            detail={
                                "message": _trail_portfolio_msg,
                                "reason_codes": ["TRAIL_PENDING_REALMONEY_CERTIFICATION"],
                                "exit_policy": structural_exit_policy,
                            },
                        )
                    reason_codes.append("TRAIL_PENDING_REALMONEY_CERTIFICATION")

                # Global Phase 1 kill switch — never silently downgrades.
                trail_phase1_cfg = _read_app_config(
                    cur, ["TRAIL_PHASE1_ENABLED"]
                )
                trail_phase1_enabled = _parse_bool_config(
                    trail_phase1_cfg.get("TRAIL_PHASE1_ENABLED"), False
                )
                if not trail_phase1_enabled:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": (
                                "Trailing stop entry blocked: TRAIL_PHASE1_ENABLED is false. "
                                "Re-enable in APP_CONFIG to allow TRAIL_BRACKET execution. "
                                "Action will not be silently downgraded to a fixed stop."
                            ),
                            "reason_codes": ["TRAIL_PHASE1_DISABLED"],
                            "exit_policy": structural_exit_policy,
                        },
                    )

                trail_params_raw = action.get("TRAIL_PARAMS")
                if isinstance(trail_params_raw, str):
                    try:
                        trail_params_parsed = json.loads(trail_params_raw)
                    except Exception:
                        trail_params_parsed = None
                elif isinstance(trail_params_raw, dict):
                    trail_params_parsed = trail_params_raw
                else:
                    trail_params_parsed = None

                trail_violations = exit_policy_service.validate_trail_params(
                    trail_params_parsed
                )
                if trail_violations:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": (
                                "Trailing stop entry blocked: TRAIL_PARAMS on the action "
                                "is invalid or incomplete. No fixed-stop fallback is allowed."
                            ),
                            "reason_codes": ["TRAIL_PARAMS_INVALID", *trail_violations],
                            "exit_policy": structural_exit_policy,
                        },
                    )

                try:
                    structural_trail_amount, structural_trail_percent = (
                        exit_policy_service.broker_trail_args(trail_params_parsed)
                    )
                except ValueError as trail_exc:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": f"Trailing stop entry blocked: {trail_exc}",
                            "reason_codes": ["TRAIL_PARAMS_INVALID"],
                            "exit_policy": structural_exit_policy,
                        },
                    ) from trail_exc

                # NOTE: sl_price intentionally LEFT POPULATED here so that the
                # downstream realism gates (_live_bracket_realism_reason_codes)
                # and target/stop calibration still see the structural
                # invalidation level. It is explicitly cleared just before
                # _submit_ibkr_order_bundle so the broker never receives both
                # a fixed-stop and a trailing-stop. See the
                # `if structural_exit_policy == TRAIL_BRACKET: sl_price = None`
                # block guarding the IBKR submit below.
        if is_exit:
            tp_price = None
            sl_price = None
        if use_ibkr_submit and not is_exit:
            fee_params = _load_live_entry_fee_params(cur)
            realism_cfg_x = _load_bracket_realism_config(cur)
            calib_cfg_x = _read_live_bracket_calibration_settings(cur)

            def _execute_recalibrate_from_baseline() -> tuple[float | None, float | None]:
                ps0 = _parse_variant(action.get("PARAM_SNAPSHOT"))
                if not isinstance(ps0, dict):
                    ps0 = {}
                base = ps0.get("committee_bracket_baseline")
                b_tr: float | None = None
                b_sl: float | None = None
                if isinstance(base, dict):
                    jd0 = {
                        "realistic_target_return": base.get("realistic_target_return"),
                        "acceptable_early_exit_target_return": base.get("acceptable_early_exit_target_return"),
                        "stop_loss_pct": base.get("stop_loss_pct"),
                    }
                    b_tr, b_sl = _live_target_and_stop_from_joint_decision(jd0, stop_loss_pct_default)
                if (
                    b_tr is None
                    or b_sl is None
                    or float(b_tr) <= 0
                    or float(b_sl) <= 0
                ):
                    jd_fallback = build_structural_entry_joint_decision(dict(action))
                    b_tr, b_sl = _live_target_and_stop_from_joint_decision(
                        jd_fallback, stop_loss_pct_default
                    )
                if (
                    b_tr is None
                    or b_sl is None
                    or float(b_tr) <= 0
                    or float(b_sl) <= 0
                    or entry_price is None
                    or float(entry_price) <= 0
                    or qty_ordered is None
                    or float(qty_ordered) <= 0
                ):
                    return None, None
                acct = str(cfg.get("IBKR_ACCOUNT_ID") or "").strip()
                nav_x = 0.0
                if acct:
                    try:
                        cur.execute(
                            """
                            select NET_LIQUIDATION_EUR
                            from MIP.LIVE.BROKER_SNAPSHOTS
                            where SNAPSHOT_TYPE = 'NAV'
                              and IBKR_ACCOUNT_ID = %s
                            order by SNAPSHOT_TS desc
                            limit 1
                            """,
                            (acct,),
                        )
                        nr = fetch_all(cur)
                        nav_x = float((nr[0] or {}).get("NET_LIQUIDATION_EUR") or 0.0) if nr else 0.0
                    except Exception:
                        nav_x = 0.0
                cr = _calibrate_live_entry_bracket_to_min_viable(
                    side=side,
                    entry_price=float(entry_price),
                    qty=float(qty_ordered),
                    nav_scale=nav_x,
                    baseline_target_return=float(b_tr),
                    baseline_stop_loss_pct=float(b_sl),
                    bust_pct=stop_loss_pct_default,
                    fee_params=fee_params,
                    rcfg=realism_cfg_x,
                    calib_cfg=calib_cfg_x,
                )
                if cr.ok and cr.target_return is not None and cr.stop_loss_pct is not None:
                    return float(cr.target_return), float(cr.stop_loss_pct)
                return None, None

            risk_reason_codes = _live_ib_entry_risk_reason_codes(
                side=side,
                is_exit=False,
                entry_price=float(entry_price) if entry_price is not None else None,
                target_return=target_return,
                stop_loss_pct=stop_loss_pct,
                fee_params=fee_params,
            )
            bracket_realism_codes = _live_bracket_realism_reason_codes(
                cur,
                live_cfg=cfg,
                side=side,
                entry_price=float(entry_price) if entry_price is not None else None,
                qty=float(qty_ordered) if qty_ordered is not None else None,
                tp_price=tp_price,
                sl_price=sl_price,
                target_return=target_return,
                fee_params=fee_params,
            )
            if risk_reason_codes or bracket_realism_codes:
                tr_new, sl_new = _execute_recalibrate_from_baseline()
                if tr_new is not None and sl_new is not None:
                    target_return = tr_new
                    stop_loss_pct = sl_new
                    tp_price, sl_price = _live_entry_tp_sl_prices(
                        str(side),
                        float(entry_price),
                        target_return,
                        stop_loss_pct,
                    )
                    risk_reason_codes = _live_ib_entry_risk_reason_codes(
                        side=side,
                        is_exit=False,
                        entry_price=float(entry_price) if entry_price is not None else None,
                        target_return=target_return,
                        stop_loss_pct=stop_loss_pct,
                        fee_params=fee_params,
                    )
                    bracket_realism_codes = _live_bracket_realism_reason_codes(
                        cur,
                        live_cfg=cfg,
                        side=side,
                        entry_price=float(entry_price) if entry_price is not None else None,
                        qty=float(qty_ordered) if qty_ordered is not None else None,
                        tp_price=tp_price,
                        sl_price=sl_price,
                        target_return=target_return,
                        fee_params=fee_params,
                    )
                    if not risk_reason_codes and not bracket_realism_codes:
                        _merge_live_action_param_snapshot_patch(
                            cur,
                            action_id,
                            {
                                "executable_bracket": {
                                    "target_return": float(target_return),
                                    "stop_loss_pct": float(stop_loss_pct),
                                    "calibrated": True,
                                    "blocked": False,
                                    "meta": {"execute_time_recalibrate": True},
                                }
                            },
                        )
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
            if bracket_realism_codes:
                final_reason_codes = sorted(set(reason_codes + bracket_realism_codes))
                _write_reason_codes(cur, action_id, final_reason_codes)
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "Execution blocked: bracket does not meet portfolio-relative realism gates.",
                        "reason_codes": final_reason_codes,
                    },
                )
        order_legs = []
        broker_submit_payload = None
        broker_truth_check = None
        ib_live_orders_inserted = False
        # Trailing Stop Phase 1: when EXIT_POLICY = TRAIL_BRACKET we send a
        # TRAIL protective leg, never a fixed STP. Clear the broker-bound
        # sl_price now that all upstream realism / risk gates have already
        # consumed it. This guarantees place_ibkr_order.py receives only the
        # trail args for the protective child.
        broker_sl_price = sl_price
        if (
            structural_exit_policy == exit_policy_service.EXIT_POLICY_TRAIL
            and not is_exit
        ):
            broker_sl_price = None
        _portfolio_gateway = _snapshot_sync_params_for_portfolio(action.get("PORTFOLIO_ID"))
        _exec_gateway_params = {
            "host": _portfolio_gateway["host"],
            "port": _portfolio_gateway["port"],
            "client_id": int(os.getenv("IBKR_EXEC_CLIENT_ID", "9410")),
        }
        if use_ibkr_submit:
            exit_symbol_qty_before = 0.0
            entry_symbol_qty_before = 0.0
            ibkr_entry_price = None if is_exit else (float(entry_price) if entry_price is not None else None)
            submit_attempt_payload = {
                "account": str(account_id),
                "symbol": str(action.get("SYMBOL")),
                "side": side,
                "action_intent": action_intent,
                "exit_type": exit_type,
                "qty": qty_ordered,
                "entry_price": float(entry_price) if entry_price is not None else None,
                "ibkr_entry_price": ibkr_entry_price,
                "ibkr_order_type": "MKT" if is_exit else "LMT",
                "tp_price": float(tp_price) if tp_price is not None else None,
                "sl_price": float(broker_sl_price) if broker_sl_price is not None else None,
                "exit_policy": structural_exit_policy,
                "trail_amount": structural_trail_amount,
                "trail_percent": structural_trail_percent,
                "tif": "DAY",
                "runtime": {
                    **_exec_gateway_params,
                    "connect_timeout_sec": int(os.getenv("IBKR_EXEC_CONNECT_TIMEOUT_SEC", "12")),
                    "exchange": os.getenv("IBKR_EXEC_EXCHANGE", "SMART"),
                    "currency": os.getenv("IBKR_EXEC_CURRENCY", "USD"),
                    "outside_rth": os.getenv("IBKR_EXEC_OUTSIDE_RTH", "1").strip().lower() in ("1", "true", "yes", "on"),
                    "child_tif": os.getenv("IBKR_EXEC_CHILD_TIF", "GTC").upper(),
                },
            }

            if is_exit:
                pre_exit_cancel_result = None
                try:
                    pre_exit_cancel_result = _cancel_ibkr_open_orders(
                        account=str(account_id),
                        symbol=str(action.get("SYMBOL")),
                    )
                except Exception as cancel_exc:
                    pre_exit_cancel_result = {"warning": f"Pre-exit bracket cancel failed (non-fatal): {cancel_exc}"}
                submit_attempt_payload["pre_exit_cancel_result"] = pre_exit_cancel_result
                truth_before_submit = _fetch_latest_broker_truth(
                    cur, str(account_id), str(action.get("SYMBOL") or ""), action.get("ASSET_CLASS")
                )
                exit_symbol_qty_before = float(truth_before_submit.get("symbol_position_qty") or 0.0)
                pos_gate_eps = 1e-5
                if is_exit and side == "SELL" and exit_symbol_qty_before <= pos_gate_eps:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": (
                                "Sell-to-close blocked: broker snapshot shows no long position for this symbol. "
                                "Refresh From IB—your prior exit may already have filled; do not resubmit the same exit."
                            ),
                            "reason_codes": ["EXIT_NO_LONG_POSITION_AT_BROKER"],
                            "symbol_position_qty": exit_symbol_qty_before,
                        },
                    )
                if is_exit and side == "BUY" and exit_symbol_qty_before >= -pos_gate_eps:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": (
                                "Buy-to-cover blocked: broker snapshot shows no short position for this symbol. "
                                "Refresh From IB before retrying."
                            ),
                            "reason_codes": ["EXIT_NO_SHORT_POSITION_AT_BROKER"],
                            "symbol_position_qty": exit_symbol_qty_before,
                        },
                    )

            else:
                truth_before_entry = _fetch_latest_broker_truth(
                    cur, str(account_id), str(action.get("SYMBOL") or ""), action.get("ASSET_CLASS")
                )
                entry_symbol_qty_before = float(truth_before_entry.get("symbol_position_qty") or 0.0)

            # TOCTOU pre-record — insert a PENDING_SUBMIT sentinel to LIVE_ORDERS before
            # any broker contact. If a concurrent request already inserted this key the
            # unique constraint raises immediately and we block the duplicate submission.
            # On broker failure the sentinel is updated to SUBMIT_FAILED so retries must
            # increment attempt_n (the idempotency key changes).
            _sentinel_order_id   = str(uuid.uuid4())
            _sentinel_idem_key   = f"{idempotency_key}:PENDING"
            _sentinel_broker_ut  = action.get("BROKER_UNIVERSE_TYPE") or ibkr_account_mode
            _sentinel_inserted   = False
            try:
                cur.execute(
                    """
                    INSERT INTO MIP.LIVE.LIVE_ORDERS (
                      ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID, IDEMPOTENCY_KEY,
                      STATUS, SYMBOL, SIDE, ACTION_INTENT, BROKER_UNIVERSE_TYPE,
                      QTY_ORDERED, ORDER_TYPE,
                      SUBMITTED_AT, ACKNOWLEDGED_AT, LAST_UPDATED_AT, CREATED_AT
                    ) VALUES (
                      %(order_id)s, %(action_id)s, %(portfolio_id)s, %(account_id)s, %(idem_key)s,
                      'PENDING_SUBMIT', %(symbol)s, %(side)s, %(action_intent)s, %(broker_universe_type)s,
                      %(qty)s, 'PENDING_SUBMIT',
                      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                    )
                    """,
                    {
                        "order_id":            _sentinel_order_id,
                        "action_id":           action_id,
                        "portfolio_id":        action.get("PORTFOLIO_ID"),
                        "account_id":          account_id,
                        "idem_key":            _sentinel_idem_key,
                        "symbol":              action.get("SYMBOL"),
                        "side":                side,
                        "action_intent":       action_intent,
                        "broker_universe_type": _sentinel_broker_ut,
                        "qty":                 qty_ordered,
                    },
                )
                _sentinel_inserted = True
            except Exception as _sent_exc:
                _sent_msg = str(_sent_exc)
                if "unique" in _sent_msg.lower() or "duplicate" in _sent_msg.lower():
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": (
                                "Concurrent submission detected: a PENDING_SUBMIT sentinel already "
                                "exists for this action and attempt. Increment attempt_n to retry."
                            ),
                            "reason_codes": ["LIVE_IDEMPOTENT_SUBMIT_ALREADY_RECORDED"],
                            "idempotency_key": _sentinel_idem_key,
                        },
                    )
                # Non-unique constraint failure: log and continue (sentinel is best-effort
                # except for the duplicate-key case handled above).

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
                    entry_price=ibkr_entry_price,
                    tp_price=float(tp_price) if tp_price is not None else None,
                    sl_price=float(broker_sl_price) if broker_sl_price is not None else None,
                    tif="DAY",
                    child_tif=os.getenv("IBKR_EXEC_CHILD_TIF", "GTC"),
                    direction=structural_direction,
                    trail_amount=structural_trail_amount,
                    trail_percent=structural_trail_percent,
                    oca_group=structural_oca_group,
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
                # Sentinel cleanup — mark the pre-record as SUBMIT_FAILED so the
                # idempotency key is no longer PENDING and retries get a clean slate
                # when the operator increments attempt_n.
                if _sentinel_inserted:
                    try:
                        cur.execute(
                            """
                            UPDATE MIP.LIVE.LIVE_ORDERS
                            SET STATUS = 'SUBMIT_FAILED', LAST_UPDATED_AT = CURRENT_TIMESTAMP()
                            WHERE ORDER_ID = %s AND STATUS = 'PENDING_SUBMIT'
                            """,
                            (_sentinel_order_id,),
                        )
                    except Exception:
                        pass
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
            # Sentinel cleanup (success) — the real order legs persisted below
            # fully supersede the TOCTOU pre-record, so remove it. Otherwise it
            # lingers forever as a phantom PENDING_SUBMIT row (no broker_order_id)
            # that pollutes reconciliation and the UI. Re-submit protection is
            # preserved by the action-status idempotent-replay guard (status is
            # now EXECUTION_REQUESTED), so dropping the sentinel is safe.
            if _sentinel_inserted:
                try:
                    cur.execute(
                        """
                        DELETE FROM MIP.LIVE.LIVE_ORDERS
                        WHERE ORDER_ID = %s AND STATUS = 'PENDING_SUBMIT'
                        """,
                        (_sentinel_order_id,),
                    )
                except Exception:
                    pass
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
                leg_order_role = None
                leg_protection_type = None
                leg_stop_price = None
                if is_structural:
                    if role == "PARENT":
                        leg_order_role = "ENTRY"
                    elif role == "STOP_LOSS":
                        leg_order_role = "PROTECTIVE_STOP"
                        leg_protection_type = "FIXED_STOP"
                        leg_stop_price = ib_aux_price or (float(sl_price) if sl_price is not None else None)
                    elif role == "TAKE_PROFIT":
                        leg_order_role = "PROTECTIVE_TP"
                        leg_protection_type = "TAKE_PROFIT"
                    elif role == "TRAILING_STOP":
                        leg_order_role = "TRAILING_STOP"
                        leg_protection_type = "TRAILING_STOP"
                        leg_stop_price = ib_aux_price
                # Trailing Stop Phase 1: stamp trail_* on the protective leg
                # so persistence in LIVE_ORDERS reflects the actual broker
                # contract that place_ibkr_order.py used.
                leg_trail_style = None
                leg_trail_amount = None
                leg_trail_percent = None
                if role == "TRAILING_STOP":
                    leg_trail_amount = structural_trail_amount
                    leg_trail_percent = structural_trail_percent
                    leg_trail_style = (
                        action.get("TRAIL_STYLE")
                        or (
                            "PCT" if structural_trail_percent is not None
                            else ("ABS" if structural_trail_amount is not None else None)
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
                        "order_role": leg_order_role,
                        "protection_type": leg_protection_type,
                        "stop_price": leg_stop_price,
                        "oca_group": ib_leg.get("oca_group") or structural_oca_group if role != "PARENT" else None,
                        "trail_style": leg_trail_style,
                        "trail_amount": leg_trail_amount,
                        "trail_percent": leg_trail_percent,
                    }
                )
            if not order_legs:
                raise HTTPException(
                    status_code=409,
                    detail={"message": "IBKR submission returned no orders.", "reason_codes": ["IBKR_EMPTY_ORDER_BUNDLE"]},
                )
            entry_order_id = None
            for leg in order_legs:
                if leg.get("order_role") == "ENTRY" or leg.get("role") == "PARENT":
                    entry_order_id = leg["order_id"]
                    break
            for leg in order_legs:
                parent_order_id = None
                if leg.get("order_role") and leg["order_role"] != "ENTRY":
                    parent_order_id = entry_order_id
                cur.execute(
                    """
                    INSERT INTO MIP.LIVE.LIVE_ORDERS (
                      ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID, IDEMPOTENCY_KEY, BROKER_ORDER_ID, STATUS,
                      SYMBOL, SIDE, ACTION_INTENT, EXIT_TYPE, ORDER_TYPE, QTY_ORDERED, LIMIT_PRICE,
                      PARENT_ORDER_ID, ORDER_ROLE, PROTECTION_TYPE, OCA_GROUP, STOP_PRICE,
                      TRAIL_STYLE, TRAIL_AMOUNT, TRAIL_PERCENT,
                      BROKER_UNIVERSE_TYPE,
                      SUBMITTED_AT, ACKNOWLEDGED_AT, LAST_UPDATED_AT, CREATED_AT
                    )
                    VALUES (
                      %(order_id)s, %(action_id)s, %(portfolio_id)s, %(account_id)s, %(idempotency_key)s, %(broker_order_id)s, %(status)s,
                      %(symbol)s, %(side)s, %(action_intent)s, %(exit_type)s, %(order_type)s, %(qty_ordered)s, %(limit_price)s,
                      %(parent_order_id)s, %(order_role)s, %(protection_type)s, %(oca_group)s, %(stop_price)s,
                      %(trail_style)s, %(trail_amount)s, %(trail_percent)s,
                      %(broker_universe_type)s,
                      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                    )
                    """,
                    {
                        "order_id": leg["order_id"],
                        "action_id": action_id,
                        "portfolio_id": action.get("PORTFOLIO_ID"),
                        "account_id": account_id,
                        "idempotency_key": leg["idempotency_key"],
                        "broker_order_id": leg.get("broker_order_id"),
                        "status": leg.get("status") or "SUBMITTED",
                        "symbol": action.get("SYMBOL"),
                        "side": leg["side"],
                        "action_intent": action_intent,
                        "exit_type": exit_type,
                        "order_type": leg["order_type"],
                        "qty_ordered": qty_ordered,
                        "limit_price": leg["limit_price"],
                        "parent_order_id": parent_order_id,
                        "order_role": leg.get("order_role"),
                        "protection_type": leg.get("protection_type"),
                        "oca_group": leg.get("oca_group"),
                        "stop_price": leg.get("stop_price"),
                        "trail_style": leg.get("trail_style"),
                        "trail_amount": leg.get("trail_amount"),
                        "trail_percent": leg.get("trail_percent"),
                        "broker_universe_type": action.get("BROKER_UNIVERSE_TYPE") or ibkr_account_mode,
                    },
                )
            ib_live_orders_inserted = True
            # Fail closed: confirm **this** bundle's broker ids (or entry position change vs pre-submit),
            # not unrelated account-wide open trades intersecting a stale snapshot.
            placement_broker_ids = {
                _normalize_broker_order_id(leg.get("broker_order_id"))
                for leg in order_legs
                if _normalize_broker_order_id(leg.get("broker_order_id"))
            }
            placement_broker_ids |= _broker_placement_order_ids_from_ibkr_submit_payload(broker_submit_payload)
            broker_order_ids = set(placement_broker_ids)
            broker_order_ids |= _broker_order_ids_from_ibkr_submit_payload(broker_submit_payload)
            truth_attempts = max(1, int(os.getenv("LIVE_BROKER_TRUTH_RETRIES", "2")))
            truth_sleep_sec = max(0.0, float(os.getenv("LIVE_BROKER_TRUTH_RETRY_SLEEP_SEC", "1.5")))
            sym_u = str(action.get("SYMBOL") or "")
            if str(action.get("ASSET_CLASS") or "").upper() == "FX" or _live_parse_fx_pair_for_broker(sym_u):
                truth_attempts = max(truth_attempts, 3)
                truth_sleep_sec = max(truth_sleep_sec, 2.0)
            exec_port = int(os.getenv("IBKR_EXEC_PORT", "7497"))
            snapshot_port = int(os.getenv("IBKR_SNAPSHOT_PORT", os.getenv("IBKR_EXEC_PORT", "7497")))
            broker_truth_raw: dict = {}
            broker_open_ids: set[str] = set()
            truth_ack = False
            pos_truth_eps = 1e-5
            for attempt_idx in range(truth_attempts):
                if attempt_idx > 0 and truth_sleep_sec > 0:
                    time.sleep(truth_sleep_sec)
                _run_on_demand_snapshot_sync(
                    **_snapshot_sync_params_for_portfolio(action.get("PORTFOLIO_ID")),
                    account=str(account_id),
                    portfolio_id=action.get("PORTFOLIO_ID"),
                )
                broker_truth_raw = _fetch_latest_broker_truth(
                    cur, str(account_id), str(action.get("SYMBOL") or ""), action.get("ASSET_CLASS")
                )
                broker_open_ids = broker_truth_raw.get("open_order_ids") or set()
                has_placements_in_snapshot = bool(
                    placement_broker_ids and placement_broker_ids.intersection(broker_open_ids)
                )
                qty_snap = float(broker_truth_raw.get("symbol_position_qty") or 0.0)
                if is_exit:
                    has_position = bool(broker_truth_raw.get("has_symbol_position"))
                    if has_placements_in_snapshot or has_position:
                        truth_ack = True
                        break
                elif has_placements_in_snapshot or abs(qty_snap - entry_symbol_qty_before) > pos_truth_eps:
                    truth_ack = True
                    break
            qty_after = float(broker_truth_raw.get("symbol_position_qty") or 0.0)
            pos_flat_eps = 1e-5
            exit_ack_via_flat = False
            if is_exit and abs(exit_symbol_qty_before) > pos_flat_eps:
                # MKT exit can fill before the next snapshot: no working order + flat book reads as
                # "missing ack" unless we compare to pre-submit position size.
                if abs(qty_after) <= pos_flat_eps:
                    exit_ack_via_flat = True
                elif exit_symbol_qty_before > 0 and qty_after < exit_symbol_qty_before - pos_flat_eps:
                    exit_ack_via_flat = True
                elif exit_symbol_qty_before < 0 and qty_after > exit_symbol_qty_before + pos_flat_eps:
                    exit_ack_via_flat = True
            broker_truth_check = {
                **broker_truth_raw,
                "open_order_ids": sorted(broker_open_ids),
                "truth_attempts": truth_attempts,
                "exit_symbol_qty_before": exit_symbol_qty_before,
                "symbol_position_qty_after": qty_after,
                "exit_ack_via_flat": exit_ack_via_flat,
            }
            # Best-effort: if any EXECUTION snapshot for this action already exposes
            # local order_id ↔ perm_id, rewrite LIVE_ORDERS.BROKER_ORDER_ID to the
            # perm_id so the reconciliation gate stays clean for follow-on submits.
            try:
                bf = _backfill_local_order_id_to_perm_id(
                    cur,
                    portfolio_id=action.get("PORTFOLIO_ID"),
                    account_id=str(account_id) if account_id else None,
                    action_id=action_id,
                )
                broker_truth_check["perm_id_backfill"] = bf
            except Exception as bf_exc:
                broker_truth_check["perm_id_backfill"] = {"error": str(bf_exc)}
            if not truth_ack and not exit_ack_via_flat:
                idem_keys = [str(leg["idempotency_key"]) for leg in order_legs if leg.get("idempotency_key")]
                if idem_keys:
                    placeholders = ",".join(["%s"] * len(idem_keys))
                    cur.execute(
                        f"""
                        update MIP.LIVE.LIVE_ORDERS
                           set STATUS = 'UNCONFIRMED_AT_BROKER',
                               LAST_UPDATED_AT = current_timestamp()
                         where ACTION_ID = %s
                           and IDEMPOTENCY_KEY in ({placeholders})
                        """,
                        tuple([action_id] + idem_keys),
                    )
                final_reason_codes = ["IBKR_TRUTH_MISSING_ORDER_ACK"]
                _write_reason_codes(cur, action_id, final_reason_codes)
                submit_trade_cnt = broker_submit_payload.get("open_trade_count_account")
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": (
                            "IBKR did not confirm open-order or position after submit; execution blocked to prevent drift. "
                            "Order rows were recorded—do not resubmit the same attempt; refresh IB and reconcile."
                        ),
                        "reason_codes": final_reason_codes,
                        "broker_order_ids": sorted(broker_order_ids),
                        "placement_broker_ids": sorted(placement_broker_ids),
                        "submit_open_trade_count": submit_trade_cnt,
                        "submit_open_trade_ids": broker_submit_payload.get("open_trade_ids_account"),
                        "ibkr_exec_port": exec_port,
                        "ibkr_snapshot_port": snapshot_port,
                        "gateway_port_mismatch": exec_port != snapshot_port,
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
                    "order_role": "ENTRY" if is_structural else None,
                    "protection_type": None,
                    "stop_price": None,
                    "oca_group": None,
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
                        "order_role": "PROTECTIVE_TP" if is_structural else None,
                        "protection_type": "TAKE_PROFIT" if is_structural else None,
                        "stop_price": None,
                        "oca_group": structural_oca_group,
                    }
                )
            # Trailing Stop Phase 1 paper representation.
            #
            # PHASE 1 NOTE — TRAIL_PAPER is INTENT-ONLY in this phase:
            # it proves contract wiring, persistence, and visibility of the
            # trailing-bracket execution path (no fixed STP). It does NOT yet
            # implement a high-water mark, ratchet, or trigger lifecycle —
            # those are deferred to a follow-up phase. The paper protective
            # leg is suppressed entirely when EXIT_POLICY = TRAIL_BRACKET so
            # there is never both a fixed-stop and a trailing leg in paper.
            if structural_exit_policy == exit_policy_service.EXIT_POLICY_TRAIL:
                trail_params_for_paper = action.get("TRAIL_PARAMS")
                if isinstance(trail_params_for_paper, str):
                    try:
                        trail_params_for_paper = json.loads(trail_params_for_paper)
                    except Exception:
                        trail_params_for_paper = {}
                if not isinstance(trail_params_for_paper, dict):
                    trail_params_for_paper = {}
                # Fix #3 — normalize trail_style to canonical PCT/ABS, matching the
                # IBKR submit path's inference. Falls back to TRAIL_PARAMS.trail_mode
                # only if neither percent nor amount is present, so paper-IBKR and
                # real-IBKR trailing legs are represented identically in LIVE_ORDERS.
                trail_style_for_paper = (
                    action.get("TRAIL_STYLE")
                    or (
                        "PCT" if structural_trail_percent is not None
                        else ("ABS" if structural_trail_amount is not None else None)
                    )
                    or trail_params_for_paper.get("trail_mode")
                )
                order_legs.append(
                    {
                        "order_id": str(uuid.uuid4()),
                        "broker_order_id": None,
                        "idempotency_key": f"{idempotency_key}:TRAIL",
                        "side": exit_side,
                        "order_type": "TRAIL_PAPER",
                        "limit_price": None,
                        "role": "TRAILING_STOP",
                        "status": "ACKNOWLEDGED",
                        # Fix #3 — use canonical TRAILING_STOP order_role (was PROTECTIVE_TRAIL)
                        # so reconciliation and UI treat paper and real trailing legs identically.
                        "order_role": "TRAILING_STOP" if is_structural else None,
                        "protection_type": "TRAILING_STOP" if is_structural else None,
                        "stop_price": None,
                        "oca_group": structural_oca_group,
                        "trail_style": trail_style_for_paper,
                        "trail_amount": structural_trail_amount,
                        "trail_percent": structural_trail_percent,
                    }
                )
            elif sl_price is not None:
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
                        "order_role": "PROTECTIVE_STOP" if is_structural else None,
                        "protection_type": "FIXED_STOP" if is_structural else None,
                        "stop_price": float(sl_price),
                        "oca_group": structural_oca_group,
                    }
                )

        # Keep API response anchored to the parent leg order id.
        order_id = str((order_legs[0] or {}).get("order_id") or order_id)

        if not ib_live_orders_inserted:
            entry_order_id = None
            for leg in order_legs:
                if leg.get("order_role") == "ENTRY" or leg.get("role") == "PARENT":
                    entry_order_id = leg["order_id"]
                    break
            for leg in order_legs:
                parent_order_id = None
                if leg.get("order_role") and leg["order_role"] != "ENTRY":
                    parent_order_id = entry_order_id
                cur.execute(
                    """
                    INSERT INTO MIP.LIVE.LIVE_ORDERS (
                      ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID, IDEMPOTENCY_KEY, BROKER_ORDER_ID, STATUS,
                      SYMBOL, SIDE, ACTION_INTENT, EXIT_TYPE, ORDER_TYPE, QTY_ORDERED, LIMIT_PRICE,
                      PARENT_ORDER_ID, ORDER_ROLE, PROTECTION_TYPE, OCA_GROUP, STOP_PRICE,
                      TRAIL_STYLE, TRAIL_AMOUNT, TRAIL_PERCENT,
                      BROKER_UNIVERSE_TYPE,
                      SUBMITTED_AT, ACKNOWLEDGED_AT, LAST_UPDATED_AT, CREATED_AT
                    )
                    VALUES (
                      %(order_id)s, %(action_id)s, %(portfolio_id)s, %(account_id)s, %(idempotency_key)s, %(broker_order_id)s, %(status)s,
                      %(symbol)s, %(side)s, %(action_intent)s, %(exit_type)s, %(order_type)s, %(qty_ordered)s, %(limit_price)s,
                      %(parent_order_id)s, %(order_role)s, %(protection_type)s, %(oca_group)s, %(stop_price)s,
                      %(trail_style)s, %(trail_amount)s, %(trail_percent)s,
                      %(broker_universe_type)s,
                      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
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
                        "parent_order_id": parent_order_id,
                        "order_role": leg.get("order_role"),
                        "protection_type": leg.get("protection_type"),
                        "oca_group": leg.get("oca_group"),
                        "stop_price": leg.get("stop_price"),
                        # Trailing fields are only populated for TRAIL_PAPER /
                        # TRAIL legs. LIMIT_PRICE / STOP_PRICE remain NULL for
                        # TRAIL_PAPER per the execution contract.
                        "trail_style": leg.get("trail_style"),
                        "trail_amount": leg.get("trail_amount"),
                        "trail_percent": leg.get("trail_percent"),
                        "broker_universe_type": action.get("BROKER_UNIVERSE_TYPE") or ibkr_account_mode,
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

        # ------------------------------------------------------------------
        # Phase 1 dual-hearing: durable executed-vs-shadow linkage.
        # Fired the moment IBKR submit returns the pending-order ack.
        # Failures inside write_shadow_trade_linkage are logged-only — the
        # real order path never blocks on shadow observability.
        # ------------------------------------------------------------------
        try:
            from app.committee.shadow_linkage import write_shadow_trade_linkage

            real_trade_cfg_for_link = {
                "side": side,
                "qty_ordered": qty_ordered,
                "entry_price": entry_price,
                "tp_price": tp_price,
                "sl_price": sl_price,
                "trail_amount": structural_trail_amount,
                "trail_percent": structural_trail_percent,
                "order_legs": order_legs,
                "idempotency_key": idempotency_key,
                # Trailing Stop Phase 1: persist the executed exit-policy
                # contract so the shadow vs real comparison surfaces both the
                # executed bracket type and the resolved profile reason.
                "exit_policy": structural_exit_policy,
                "exit_profile_reason": action.get("EXIT_POLICY_REASON"),
                "trail_status": action.get("TRAIL_STATUS"),
                "broker_sl_price": broker_sl_price,
            }
            write_shadow_trade_linkage(
                cur,
                action_row=_fetch_live_action(cur, action_id) or action,
                broker_order_payload=broker_submit_payload,
                real_trade_config=real_trade_cfg_for_link,
            )
        except Exception as link_exc:
            _log.warning(
                "shadow_linkage: invocation failed action_id=%s (real order unaffected): %s",
                action_id, link_exc,
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
            # Reconcile may set FILLED before avg price is known; closeout hooks need AVG_FILL_PRICE.
            if (
                target_status == "FILLED"
                and order.get("AVG_FILL_PRICE") is None
                and req.avg_fill_price is not None
            ):
                cur.execute(
                    """
                    update MIP.LIVE.LIVE_ORDERS
                       set AVG_FILL_PRICE = %s,
                           LAST_UPDATED_AT = current_timestamp()
                     where ORDER_ID = %s
                    """,
                    (float(req.avg_fill_price), order_id),
                )
                return {
                    "ok": True,
                    "order_id": order_id,
                    "status": target_status,
                    "avg_fill_price_patched": True,
                    "avg_fill_price": float(req.avg_fill_price),
                }
            return {"ok": True, "order_id": order_id, "status": target_status, "idempotent_replay": True}

        qty_ordered = float(order.get("QTY_ORDERED") or 0.0)
        existing_qty_filled = float(order.get("QTY_FILLED") or 0.0)
        new_qty_filled = req.qty_filled if req.qty_filled is not None else existing_qty_filled
        if target_status == "PARTIAL_FILL":
            if new_qty_filled <= 0 or (qty_ordered > 0 and new_qty_filled >= qty_ordered):
                raise HTTPException(status_code=400, detail="PARTIAL_FILL requires qty_filled between 0 and qty_ordered.")
        if target_status == "FILLED":
            new_qty_filled = qty_ordered if qty_ordered > 0 else (req.qty_filled or existing_qty_filled)

        # When a reconcile/backfill caller supplies the broker's true fill time,
        # preserve it; otherwise fall back to FILLED_AT (idempotent replay) or
        # current_timestamp() (live ack with no broker time available).
        filled_at_param = (
            req.filled_at.replace(tzinfo=None) if req.filled_at is not None else None
        )
        cur.execute(
            """
            update MIP.LIVE.LIVE_ORDERS
               set STATUS = %s,
                   BROKER_ORDER_ID = coalesce(%s, BROKER_ORDER_ID),
                   QTY_FILLED = %s,
                   AVG_FILL_PRICE = coalesce(%s, AVG_FILL_PRICE),
                   TOTAL_COMMISSION = coalesce(%s, TOTAL_COMMISSION),
                   FILLED_AT = case
                                 when %s = 'FILLED'
                                   then coalesce(%s, FILLED_AT, current_timestamp())
                                 else FILLED_AT
                               end,
                   LAST_UPDATED_AT = current_timestamp()
             where ORDER_ID = %s
            """,
            (
                target_status,
                req.broker_order_id,
                new_qty_filled,
                req.avg_fill_price,
                req.total_commission,
                target_status,
                filled_at_param,
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
              IDEMPOTENCY_KEY, BROKER_ORDER_ID, SYMBOL, SIDE, QTY, PRICE,
              COMMISSION, PAYLOAD
            )
            select
              %s, current_timestamp(), %s, %s, %s,
              %s, %s, %s, %s, %s, %s,
              %s, try_parse_json(%s)
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
                req.total_commission,
                json.dumps({"actor": req.actor, "notes": req.notes, "total_commission": req.total_commission}),
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

        if action_id and target_status == "FILLED":
            # v1: closeout only on full FILLED (not PARTIAL_FILL)
            post_order = {
                **order,
                "STATUS": target_status,
                "QTY_FILLED": new_qty_filled,
            }
            try:
                if is_protective_leg_order(post_order):
                    co = maybe_write_trade_closeout_on_protective_leg_filled(cur, order_id)
                else:
                    co = maybe_write_trade_closeout_on_exit_filled(cur, str(action_id))
                extra_keys = (
                    "entry_action_id",
                    "exit_type",
                    "status",
                    "action_intent",
                    "entry_status",
                    "qty_ordered",
                    "qty_filled",
                )
                _log.info(
                    "trade_closeout_attempt order_id=%s broker_order_id=%s action_id=%s path=%s outcome=%s reason=%s extra=%s",
                    order_id,
                    req.broker_order_id or order.get("BROKER_ORDER_ID"),
                    action_id,
                    co.get("path"),
                    co.get("outcome"),
                    co.get("reason"),
                    {k: co.get(k) for k in extra_keys if co.get(k) is not None},
                )
            except Exception as exc:
                _log.exception(
                    "trade_closeout_attempt_failed order_id=%s broker_order_id=%s action_id=%s protective=%s err=%s",
                    order_id,
                    req.broker_order_id or order.get("BROKER_ORDER_ID"),
                    action_id,
                    is_protective_leg_order(post_order),
                    exc,
                )

        return {
            "ok": True,
            "order_id": order_id,
            "status": target_status,
            "qty_filled": new_qty_filled,
            "avg_fill_price": req.avg_fill_price if req.avg_fill_price is not None else order.get("AVG_FILL_PRICE"),
            "total_commission": req.total_commission,
        }
    finally:
        conn.close()


class ReconcileV2Request(BaseModel):
    live_portfolio_id: int
    lookback_days: int = Field(default=7, ge=1, le=30)


@router.post("/trades/reconcile-v2")
def run_reconciliation_v2(req: ReconcileV2Request):
    """
    Two-layer reconciliation: broker mirror (Layer 1) + semantic comparison (Layer 2).
    Returns per-symbol reconciliation with position, entry, protection, and orphan checks.
    """
    from app.services.live_intelligence.broker_mirror import build_broker_mirror
    from app.services.live_intelligence.semantic_reconciliation_v2 import run_semantic_reconciliation

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT IBKR_ACCOUNT_ID FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG WHERE PORTFOLIO_ID = %s LIMIT 1",
            (req.live_portfolio_id,),
        )
        cfg_rows = fetch_all(cur)
        if not cfg_rows or not cfg_rows[0].get("IBKR_ACCOUNT_ID"):
            raise HTTPException(status_code=404, detail="Portfolio not found or no IBKR account configured.")
        account_id = str(cfg_rows[0]["IBKR_ACCOUNT_ID"])

        broker_mirrors = build_broker_mirror(cur, account_id, lookback_days=req.lookback_days)
        recon_by_symbol, recon_meta = run_semantic_reconciliation(
            cur, req.live_portfolio_id, account_id, broker_mirrors,
        )

        mirror_summary = {
            sym: m.to_dict() for sym, m in broker_mirrors.items()
        }

        return {
            "ok": True,
            "portfolio_id": req.live_portfolio_id,
            "account_id": account_id,
            "broker_mirror": mirror_summary,
            "reconciliation": recon_by_symbol,
            "meta": recon_meta,
        }
    finally:
        conn.close()


class TrailActivationRequest(BaseModel):
    live_portfolio_id: int
    dry_run: bool = True


@router.post("/trades/trail-activation")
def run_trail_activation(req: TrailActivationRequest):
    """
    Evaluate filled structural positions for trailing stop activation.
    When activation conditions are met and dry_run=False, replaces fixed stops
    with trailing stops at IB.
    """
    from app.services.live_intelligence.trail_activation import run_trail_activation_cycle

    conn = get_connection()
    try:
        cur = conn.cursor()

        # Trailing Stop Phase 2 hard block.
        #
        # Post-fill trail replacement is structurally unsafe today: the
        # standalone-trail-after-cancel path places a MarketOrder parent
        # alongside the TRAIL child, which would close the position
        # immediately. This endpoint MUST remain disabled until that path
        # is redesigned. Phase 1 only ships entry-time TRAIL_BRACKET via
        # execute_live_action; replacement must not be relied on.
        replacement_cfg = _read_app_config(cur, ["TRAIL_REPLACEMENT_ENABLED"])
        replacement_enabled = _parse_bool_config(
            replacement_cfg.get("TRAIL_REPLACEMENT_ENABLED"), False
        )
        if not replacement_enabled:
            return {
                "ok": True,
                "status": "TRAIL_REPLACEMENT_DISABLED",
                "message": (
                    "Post-fill trail replacement is disabled (TRAIL_REPLACEMENT_ENABLED=false). "
                    "Phase 2 is deferred; Phase 1 entry-time trailing is handled by execute_live_action."
                ),
                "reason_codes": ["TRAIL_REPLACEMENT_PHASE2_NOT_ENABLED"],
                "evaluated_count": 0,
            }

        trail_cfg = _read_app_config(cur, ["TRAIL_ENABLED", "TRAIL_DRY_RUN"])
        trail_enabled = _parse_bool_config(trail_cfg.get("TRAIL_ENABLED"), False)
        trail_dry_run = _parse_bool_config(trail_cfg.get("TRAIL_DRY_RUN"), True)

        if not trail_enabled:
            return {
                "ok": True,
                "status": "TRAIL_DISABLED",
                "message": "Trailing stops are disabled (TRAIL_ENABLED=false).",
                "evaluated_count": 0,
            }

        effective_dry_run = req.dry_run or trail_dry_run

        cur.execute(
            "SELECT IBKR_ACCOUNT_ID FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG WHERE PORTFOLIO_ID = %s LIMIT 1",
            (req.live_portfolio_id,),
        )
        cfg_rows = fetch_all(cur)
        if not cfg_rows or not cfg_rows[0].get("IBKR_ACCOUNT_ID"):
            raise HTTPException(status_code=404, detail="Portfolio not found or no IBKR account configured.")

        def _cancel_adapter(*, account, symbol, broker_order_id):
            return _cancel_ibkr_open_orders(
                account=account,
                symbol=symbol,
                broker_order_id=broker_order_id,
            )

        def _place_adapter(*, account, symbol, side, qty, trail_amount, trail_percent, oca_group):
            return _submit_ibkr_order_bundle(
                account=account,
                symbol=symbol,
                side=side,
                qty=qty,
                entry_price=None,
                tp_price=None,
                sl_price=None,
                tif="GTC",
                trail_amount=trail_amount,
                trail_percent=trail_percent,
                oca_group=oca_group,
            )

        result = run_trail_activation_cycle(
            cur,
            cancel_fn=_cancel_adapter,
            place_fn=_place_adapter,
            portfolio_id=req.live_portfolio_id,
            dry_run=effective_dry_run,
        )

        return {
            "ok": True,
            "portfolio_id": req.live_portfolio_id,
            "dry_run": effective_dry_run,
            "trail_enabled": trail_enabled,
            "trail_dry_run_config": trail_dry_run,
            "evaluated_count": result.evaluated_count,
            "activated_count": result.activated_count,
            "skipped_count": result.skipped_count,
            "error_count": result.error_count,
            "candidates": result.candidates,
            "errors": result.errors,
        }
    finally:
        conn.close()


@router.post("/trades/reconcile-executions/dry-run")
def reconcile_executions_dry_run(req: ReconcileExecutionsDryRunRequest):
    """
    Match BROKER_SNAPSHOTS EXECUTION rows to LIVE_ORDERS by broker id (read-only).
    Returns matched / ambiguous / unmatched / already_synced — no writes.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select IBKR_ACCOUNT_ID
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            limit 1
            """,
            (req.portfolio_id,),
        )
        row = cur.fetchone()
        account_id = str((row or [None])[0] or "").strip()
        if not account_id:
            raise HTTPException(
                status_code=404,
                detail={"message": "Live portfolio config or IBKR_ACCOUNT_ID not found.", "reason_codes": ["MISSING_IBKR_ACCOUNT"]},
            )
        out = run_reconcile_dry_run(
            cur,
            portfolio_id=req.portfolio_id,
            account_id=account_id,
            lookback_days=req.lookback_days,
        )
        out["dry_run"] = True
        out["actor"] = req.actor
        return {"ok": True, **out}
    finally:
        conn.close()


@router.post("/trades/reconcile-executions/apply")
def reconcile_executions_apply(req: ReconcileExecutionsApplyRequest):
    """
    Apply broker execution → LIVE_ORDERS status updates after dry-run review.
    Each item is re-validated; every applied row writes BROKER_EVENT_LEDGER (EXECUTION_RECONCILE_APPLY).
    """
    if not req.confirm_apply:
        raise HTTPException(
            status_code=400,
            detail={"message": "confirm_apply must be true.", "reason_codes": ["RECONCILE_APPLY_NOT_CONFIRMED"]},
        )
    reconcile_run_id = str(uuid.uuid4())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select IBKR_ACCOUNT_ID
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            limit 1
            """,
            (req.portfolio_id,),
        )
        row = cur.fetchone()
        account_id = str((row or [None])[0] or "").strip()
        if not account_id:
            raise HTTPException(
                status_code=404,
                detail={"message": "Live portfolio config or IBKR_ACCOUNT_ID not found.", "reason_codes": ["MISSING_IBKR_ACCOUNT"]},
            )
    finally:
        conn.close()

    results: list[dict] = []
    for item in req.items:
        clf: dict | None = None
        before: dict = {}
        conn_i = get_connection()
        try:
            cur_i = conn_i.cursor()
            v = verify_apply_item(
                cur_i,
                portfolio_id=req.portfolio_id,
                account_id=account_id,
                lookback_days=req.lookback_days,
                exec_key=item.exec_key,
                order_id=item.order_id,
            )
            if not v.get("ok"):
                results.append(
                    {
                        "exec_key": item.exec_key,
                        "order_id": item.order_id,
                        "ok": False,
                        "reason": v.get("reason"),
                        "classification": v.get("classification"),
                    }
                )
                continue
            clf = v["classification"]
            cur_i.execute(
                """
                select ORDER_ID, STATUS, BROKER_ORDER_ID, QTY_ORDERED, QTY_FILLED, AVG_FILL_PRICE, ACTION_ID
                from MIP.LIVE.LIVE_ORDERS
                where ORDER_ID = %s
                  and PORTFOLIO_ID = %s
                limit 1
                """,
                (item.order_id, req.portfolio_id),
            )
            orow = cur_i.fetchone()
            cols = [d[0] for d in (cur_i.description or [])]
            before = dict(zip(cols, orow)) if orow and cols else {}
            insert_reconcile_audit(
                cur_i,
                portfolio_id=req.portfolio_id,
                action_id=str(before.get("ACTION_ID") or "") or None,
                event_type="EXECUTION_RECONCILE_APPLY",
                payload={
                    "reconcile_run_id": reconcile_run_id,
                    "actor": req.actor,
                    "phase": "before_update_live_order_status",
                    "exec_key": item.exec_key,
                    "order_id": item.order_id,
                    "order_before": {k: before.get(k) for k in ("STATUS", "BROKER_ORDER_ID", "QTY_FILLED", "AVG_FILL_PRICE") if k in before},
                    "classification": {
                        "proposed_status": clf.get("proposed_status"),
                        "proposed_qty_filled": clf.get("proposed_qty_filled"),
                        "proposed_avg_fill_price": clf.get("proposed_avg_fill_price"),
                        "proposed_filled_at": clf.get("proposed_filled_at"),
                        "reason_detail": clf.get("reason_detail"),
                    },
                },
            )
            try:
                conn_i.commit()
            except Exception:
                pass
        finally:
            conn_i.close()

        if clf is None:
            continue

        st = str(clf.get("proposed_status") or "FILLED").upper()
        # Prefer the execution's stable id (perm_id) over whatever LIVE_ORDERS already
        # has — typically the TWS local order_id stored when IB returned perm_id=0 in
        # the initial ack. Rewriting BROKER_ORDER_ID to perm_id restores ground-truth
        # lineage and lets _recent_unmapped_execution_summary clear without manual help.
        existing_bid = str(before.get("BROKER_ORDER_ID") or "").strip()
        preferred_bid = str(clf.get("preferred_broker_order_id") or "").strip()
        new_bid = preferred_bid or existing_bid or None
        ureq = UpdateLiveOrderStatusRequest(
            actor=req.actor,
            status=st,  # type: ignore[arg-type]
            qty_filled=float(clf["proposed_qty_filled"]) if clf.get("proposed_qty_filled") is not None else None,
            avg_fill_price=float(clf["proposed_avg_fill_price"]) if clf.get("proposed_avg_fill_price") is not None else None,
            broker_order_id=new_bid,
            # Preserve broker truth: stamp FILLED_AT with the actual IB
            # execution time, not current_timestamp(). Without this, syncing
            # a fill that happened days ago would back-date the trade to now.
            filled_at=clf.get("proposed_filled_at"),
            notes=(
                f"broker_execution_reconcile run={reconcile_run_id} exec_key={item.exec_key}"
                + (
                    f" perm_id_backfill {existing_bid}->{preferred_bid}"
                    if preferred_bid and existing_bid and preferred_bid != existing_bid
                    else ""
                )
            ),
        )
        try:
            uout = update_live_order_status(item.order_id, ureq)
            results.append(
                {
                    "exec_key": item.exec_key,
                    "order_id": item.order_id,
                    "ok": True,
                    "update_live_order_status": uout,
                }
            )
        except HTTPException as hex_exc:
            results.append(
                {
                    "exec_key": item.exec_key,
                    "order_id": item.order_id,
                    "ok": False,
                    "reason": "update_live_order_status_failed",
                    "http_detail": hex_exc.detail,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "exec_key": item.exec_key,
                    "order_id": item.order_id,
                    "ok": False,
                    "reason": "update_live_order_status_error",
                    "error": str(exc),
                }
            )

    return {
        "ok": True,
        "reconcile_run_id": reconcile_run_id,
        "portfolio_id": req.portfolio_id,
        "account_id": account_id,
        "results": results,
    }


@router.post("/trades/smoke/paper-workflow")
def run_paper_workflow_smoke(req: SimulatePaperWorkflowRequest):
    """
    One-click paper workflow simulation:
    structural import -> PM accept -> compliance approve -> revalidate -> execute -> order status progression.
    """
    steps: list[dict] = []

    _lim = max(1, min(int(req.limit or 5), 50))
    import_result = import_structural_proposals(
        ImportStructuralProposalsRequest(
            live_portfolio_id=req.live_portfolio_id,
            limit=_lim,
            max_proposal_age_days=30,
        )
    )
    steps.append({"step": "import_structural_proposals", "result": import_result})

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
                  and coalesce(LIVE_INTENT_KIND, '') = 'STRUCTURAL'
                  and STATUS in (
                    'PENDING_OPEN_VALIDATION','OPEN_ELIGIBLE','OPEN_CAUTION','OPEN_BLOCKED',
                    'PENDING_OPEN_STABILITY_REVIEW','READY_FOR_APPROVAL_FLOW'
                  )
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
            detail="No structural proposal imported and no pending structural live action found for workflow smoke.",
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
              PORTFOLIO_ID, IBKR_ACCOUNT_ID, BROKER_NAME, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              MAX_BAR_END_LAG_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS,
              DRIFT_STATUS, CONFIG_VERSION, IS_ACTIVE,
              IBKR_ACCOUNT_MODE, IS_EXECUTION_ENABLED, REAL_MONEY_ENABLED,
              CREATED_AT, UPDATED_AT
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
              PORTFOLIO_ID, IBKR_ACCOUNT_ID, BROKER_NAME, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              MAX_BAR_END_LAG_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS,
              DRIFT_STATUS, CONFIG_VERSION, IS_ACTIVE,
              IBKR_ACCOUNT_MODE, IS_EXECUTION_ENABLED, REAL_MONEY_ENABLED,
              CREATED_AT, UPDATED_AT
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
                       BROKER_NAME = coalesce(%s, BROKER_NAME),
                       ADAPTER_MODE = coalesce(%s, ADAPTER_MODE),
                       BASE_CURRENCY = coalesce(%s, BASE_CURRENCY),
                       MAX_POSITIONS = coalesce(%s, MAX_POSITIONS),
                       MAX_POSITION_PCT = coalesce(%s, MAX_POSITION_PCT),
                       CASH_BUFFER_PCT = coalesce(%s, CASH_BUFFER_PCT),
                       MAX_SLIPPAGE_PCT = coalesce(%s, MAX_SLIPPAGE_PCT),
                       VALIDITY_WINDOW_SEC = coalesce(%s, VALIDITY_WINDOW_SEC),
                       QUOTE_FRESHNESS_THRESHOLD_SEC = coalesce(%s, QUOTE_FRESHNESS_THRESHOLD_SEC),
                       SNAPSHOT_FRESHNESS_THRESHOLD_SEC = coalesce(%s, SNAPSHOT_FRESHNESS_THRESHOLD_SEC),
                       MAX_BAR_END_LAG_SEC = coalesce(%s, MAX_BAR_END_LAG_SEC),
                       DRAWDOWN_STOP_PCT = coalesce(%s, DRAWDOWN_STOP_PCT),
                       BUST_PCT = coalesce(%s, BUST_PCT),
                       COOLDOWN_BARS = coalesce(%s, COOLDOWN_BARS),
                       IS_ACTIVE = coalesce(%s, IS_ACTIVE),
                       IS_EXECUTION_ENABLED = coalesce(%s, IS_EXECUTION_ENABLED),
                       REAL_MONEY_ENABLED = coalesce(%s, REAL_MONEY_ENABLED),
                       CONFIG_VERSION = coalesce(CONFIG_VERSION, 1) + 1,
                       UPDATED_AT = current_timestamp()
                 where PORTFOLIO_ID = %s
                """,
                (
                    req.ibkr_account_id,
                    req.broker_name,
                    req.adapter_mode,
                    req.base_currency.upper() if req.base_currency else None,
                    req.max_positions,
                    req.max_position_pct,
                    req.cash_buffer_pct,
                    req.max_slippage_pct,
                    req.validity_window_sec,
                    req.quote_freshness_threshold_sec,
                    req.snapshot_freshness_threshold_sec,
                    req.max_bar_end_lag_sec,
                    req.drawdown_stop_pct,
                    req.bust_pct,
                    req.cooldown_bars,
                    req.is_active,
                    req.is_execution_enabled,
                    req.real_money_enabled,
                    portfolio_id,
                ),
            )
        else:
            if not req.ibkr_account_id:
                raise HTTPException(status_code=400, detail="ibkr_account_id is required when creating config.")
            cur.execute(
                """
                insert into MIP.LIVE.LIVE_PORTFOLIO_CONFIG (
                  PORTFOLIO_ID, IBKR_ACCOUNT_ID, BROKER_NAME, ADAPTER_MODE, BASE_CURRENCY,
                  MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
                  VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
                  MAX_BAR_END_LAG_SEC,
                  DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS, IS_ACTIVE,
                  IS_EXECUTION_ENABLED, REAL_MONEY_ENABLED, CONFIG_VERSION,
                  CREATED_AT, UPDATED_AT
                )
                values (
                  %s, %s, coalesce(%s, 'IBKR'), coalesce(%s, 'PAPER'), coalesce(%s, 'EUR'),
                  %s, %s, %s, %s,
                  coalesce(%s, 14400), coalesce(%s, 60), coalesce(%s, 300),
                  %s,
                  %s, %s, coalesce(%s, 3), coalesce(%s, true),
                  coalesce(%s, false), coalesce(%s, false), 1,
                  current_timestamp(), current_timestamp()
                )
                """,
                (
                    portfolio_id,
                    req.ibkr_account_id,
                    req.broker_name,
                    req.adapter_mode,
                    req.base_currency.upper() if req.base_currency else None,
                    req.max_positions,
                    req.max_position_pct,
                    req.cash_buffer_pct,
                    req.max_slippage_pct,
                    req.validity_window_sec,
                    req.quote_freshness_threshold_sec,
                    req.snapshot_freshness_threshold_sec,
                    req.max_bar_end_lag_sec,
                    req.drawdown_stop_pct,
                    req.bust_pct,
                    req.cooldown_bars,
                    req.is_active,
                    req.is_execution_enabled,
                    req.real_money_enabled,
                ),
            )
        cur.execute(
            """
            select
              PORTFOLIO_ID, IBKR_ACCOUNT_ID, BROKER_NAME, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              MAX_BAR_END_LAG_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS,
              DRIFT_STATUS, CONFIG_VERSION, IS_ACTIVE,
              IBKR_ACCOUNT_MODE, IS_EXECUTION_ENABLED, REAL_MONEY_ENABLED,
              CREATED_AT, UPDATED_AT
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
              PORTFOLIO_ID, IBKR_ACCOUNT_ID, BROKER_NAME, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              MAX_BAR_END_LAG_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS, IS_ACTIVE,
              IS_EXECUTION_ENABLED, REAL_MONEY_ENABLED, CONFIG_VERSION,
              CREATED_AT, UPDATED_AT
            )
            values (
              %s, %s, coalesce(%s, 'IBKR'), coalesce(%s, 'PAPER'), coalesce(%s, 'EUR'),
              %s, %s, %s, %s,
              coalesce(%s, 14400), coalesce(%s, 60), coalesce(%s, 300),
              %s,
              %s, %s, coalesce(%s, 3), coalesce(%s, true),
              coalesce(%s, false), coalesce(%s, false), 1,
              current_timestamp(), current_timestamp()
            )
            """,
            (
                next_id,
                req.ibkr_account_id,
                req.broker_name,
                req.adapter_mode,
                req.base_currency.upper() if req.base_currency else None,
                req.max_positions,
                req.max_position_pct,
                req.cash_buffer_pct,
                req.max_slippage_pct,
                req.validity_window_sec,
                req.quote_freshness_threshold_sec,
                req.snapshot_freshness_threshold_sec,
                req.max_bar_end_lag_sec,
                req.drawdown_stop_pct,
                req.bust_pct,
                req.cooldown_bars,
                req.is_active,
                req.is_execution_enabled,
                req.real_money_enabled,
            ),
        )

        cur.execute(
            """
            select
              PORTFOLIO_ID, IBKR_ACCOUNT_ID, BROKER_NAME, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              MAX_BAR_END_LAG_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS,
              DRIFT_STATUS, CONFIG_VERSION, IS_ACTIVE,
              IBKR_ACCOUNT_MODE, IS_EXECUTION_ENABLED, REAL_MONEY_ENABLED,
              CREATED_AT, UPDATED_AT
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
        assert_legacy_order_proposals_import_allowed(_live_structural_only_enabled(cur))

        cur.execute(
            """
            select
              coalesce(VALIDITY_WINDOW_SEC, 14400) as VALIDITY_WINDOW_SEC,
              IBKR_ACCOUNT_ID,
              COALESCE(BROKER_NAME, 'IBKR') AS BROKER_NAME,
              COALESCE(IBKR_ACCOUNT_MODE, 'PAPER') AS IBKR_ACCOUNT_MODE
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
        validity_window_sec, ibkr_account_id, import_broker_name, import_universe_type = cfg
        ibkr_account_id = str(ibkr_account_id or "").strip()
        import_broker_name = str(import_broker_name or "IBKR").strip()
        import_universe_type = str(import_universe_type or "PAPER").strip()
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
                    broker_truth = _fetch_latest_broker_truth(
                        cur, ibkr_account_id, symbol_upper, p.get("MARKET_TYPE")
                    )
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
                  ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, BROKER_NAME, IBKR_ACCOUNT_ID, BROKER_UNIVERSE_TYPE,
                  SYMBOL, SIDE, ACTION_INTENT, EXIT_TYPE, EXIT_REASON, PROPOSED_QTY, ASSET_CLASS,
                  STATUS, VALIDITY_WINDOW_END, COMPLIANCE_STATUS, PARAM_SNAPSHOT, REASON_CODES,
                  TRAINING_QUALIFICATION_SNAPSHOT, TRAINING_LIVE_ELIGIBLE, TRAINING_RANK_IMPACT, TRAINING_SIZE_CAP_FACTOR,
                  TARGET_EXPECTATION_SNAPSHOT, TARGET_OPEN_CONDITION_FACTOR, TARGET_EXPECTATION_POLICY_VERSION,
                  NEWS_CONTEXT_SNAPSHOT, NEWS_CONTEXT_STATE, NEWS_EVENT_SHOCK_FLAG, NEWS_FRESHNESS_BUCKET, NEWS_CONTEXT_POLICY_VERSION,
                  COMMITTEE_REQUIRED, COMMITTEE_STATUS, LIVE_INTENT_KIND,
                  CREATED_AT, UPDATED_AT
                )
                select
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  'PENDING_OPEN_VALIDATION', dateadd(second, %s, current_timestamp()), 'PENDING', parse_json(%s), parse_json(%s),
                  parse_json(%s), %s, %s, %s,
                  parse_json(%s), %s, %s,
                  parse_json(%s), %s, %s, %s, %s,
                  true, 'PENDING', 'LEGACY_PATTERN',
                  current_timestamp(), current_timestamp()
                """,
                (
                    action_id,
                    proposal_id,
                    req.live_portfolio_id,
                    import_broker_name,
                    ibkr_account_id,
                    import_universe_type,
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
            if proposal_id is not None and str(action_intent or "").upper() == "ENTRY":
                try:
                    ensure_entry_intel_for_proposal(cur, int(proposal_id))
                    insert_entry_intel_action_link(cur, int(proposal_id), action_id)
                except Exception as exc:
                    _log.warning(
                        "EIS ensure/link failed on research import proposal_id=%s action_id=%s: %s",
                        proposal_id,
                        action_id,
                        exc,
                        exc_info=True,
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


# ── Structural proposal import bridge ─────────────────────────────────

_STRUCTURAL_PROPOSAL_QUERY = """
WITH latest_bars AS (
    SELECT SYMBOL,
           MAX(TO_DATE(TS))              AS LATEST_BAR_DATE,
           MAX_BY(CLOSE, TS)             AS CLOSE_PRICE
    FROM MIP.MART.MARKET_BARS
    WHERE INTERVAL_MINUTES = 1440
    GROUP BY SYMBOL
),
latest_regime AS (
    SELECT SYMBOL, MARKET_TYPE,
           MAX_BY(OBJECT_CONSTRUCT(
               'vol_regime',   VOL_REGIME,
               'trend_regime', TREND_REGIME,
               'range_regime', RANGE_REGIME
           ), AS_OF_DATE) AS REGIME_TAGS
    FROM MIP.APP.STRUCTURAL_REGIME_TAG
    GROUP BY SYMBOL, MARKET_TYPE
)
SELECT
    stp.PROPOSAL_ID,
    stp.SETUP_EVENT_ID,
    stp.PRIMARY_EVIDENCE_SETUP_EVENT_ID,
    stp.SYMBOL,
    COALESCE(se.MARKET_TYPE, se_ev.MARKET_TYPE)             AS MARKET_TYPE,
    stp.SETUP_FAMILY,
    stp.DIRECTION,
    stp.ENTRY_ZONE_LOW,
    stp.ENTRY_ZONE_HIGH,
    -- SUPPORTING_LEVEL from same-direction evidence event only
    se.LEVEL_PRICE                                          AS SUPPORTING_LEVEL,
    stp.PRICE_INVALIDATION_LEVEL                            AS INVALIDATION_LEVEL,
    stp.INVALIDATION_RULE,
    stp.STRUCTURE_CONFIDENCE,
    COALESCE(se.LEVEL_SIGNIFICANCE, stp.LEVEL_SIGNIFICANCE) AS LEVEL_SIGNIFICANCE,
    COALESCE(se.STRUCTURAL_STATE, se_ev.STRUCTURAL_STATE)   AS STRUCTURAL_STATE,
    lr.REGIME_TAGS,
    stp.REGIME_COMPAT,
    COALESCE(st.TRUST_LABEL, 'UNKNOWN')                     AS TRUST_LABEL,
    stp.MEANINGFUL_HIT_RATE,
    stp.PATH_SURVIVAL_HIT_RATE                              AS PATH_SURVIVAL_RATE,
    stp.MFE_MAE_RATIO,
    st.AVG_BARS_TO_THRESHOLD,
    st.FAILURE_MODE_DISTRIBUTION,
    st.BEST_WINDOW,
    stp.RISK_CLASS,
    COALESCE(stp.EXIT_STYLE, rp.EXIT_STYLE, 'STRUCTURAL_TARGET') AS EXIT_STYLE,
    COALESCE(stp.TRAIL_STYLE, rp.TRAIL_STYLE)               AS TRAIL_STYLE,
    COALESCE(stp.TRAIL_PARAMS, rp.TRAIL_PARAMS)              AS TRAIL_PARAMS,
    -- Trailing Stop Phase 1: bounded exit profile feeds EXIT_POLICY resolution
    -- (proposal override > policy default > FIXED_STANDARD). NOTE: this
    -- column drives the broker-executable TRAIL_PARAMS written into
    -- LIVE_ACTIONS. The legacy COALESCE(TRAIL_PARAMS) above remains for
    -- management-style consumers and is intentionally not overwritten.
    COALESCE(stp.EXIT_PROFILE, rp.EXIT_PROFILE, 'FIXED_STANDARD') AS EXIT_PROFILE,
    rp.TRAIL_ACTIVATION_TYPE,
    rp.TRAIL_ACTIVATION_PARAM,
    COALESCE(rp.MAX_HOLD_BARS, 20)                           AS MAX_HOLD_BARS,
    stp.RATIONALE_TEXT                                       AS PROPOSAL_RATIONALE,
    mb.LATEST_BAR_DATE,
    mb.CLOSE_PRICE                                           AS CURRENT_PRICE,
    COALESCE(se.SETUP_STATUS, se_ev.SETUP_STATUS)           AS SETUP_STATUS,
    stp.EXECUTION_POLICY_STATUS,
    stp.EXECUTION_POLICY_REASON,
    stp.IS_RESEARCH_ONLY,
    stp.CREATED_AT                                           AS PROPOSAL_CREATED_AT
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS stp
-- LEFT JOIN: SETUP_EVENT_ID is NULL for cross-direction proposals.
-- These are blocked by EXECUTION_POLICY_STATUS and must not be imported.
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se
    ON se.SETUP_EVENT_ID = stp.SETUP_EVENT_ID
-- Evidence event for cross-direction context (always populated)
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS se_ev
    ON se_ev.SETUP_EVENT_ID = stp.PRIMARY_EVIDENCE_SETUP_EVENT_ID
LEFT JOIN MIP.APP.STRUCTURAL_SETUP_TRUST st
    ON st.SETUP_FAMILY = stp.SETUP_FAMILY
   AND st.MARKET_TYPE  = COALESCE(se.MARKET_TYPE, se_ev.MARKET_TYPE)
   AND st.EVAL_WINDOW  = 20
LEFT JOIN MIP.APP.STRUCTURAL_RISK_POLICY rp
    ON rp.SETUP_FAMILY = stp.SETUP_FAMILY
   AND rp.DIRECTION    = stp.DIRECTION
   AND rp.IS_ACTIVE    = TRUE
LEFT JOIN latest_bars mb
    ON mb.SYMBOL = stp.SYMBOL
LEFT JOIN latest_regime lr
    ON lr.SYMBOL      = stp.SYMBOL
   AND lr.MARKET_TYPE = COALESCE(se.MARKET_TYPE, se_ev.MARKET_TYPE)
-- PROPOSAL-CURRENCY CONTRACT (must match the structural timeline / cockpit
-- pending list, which use V_LATEST_AUTHORITATIVE_BOARD_RUN). A proposal is
-- valid for exactly one daily cycle: it is importable only while it is
-- PROPOSED *and* belongs to the latest authoritative board run. Proposals
-- from a superseded/older run (or after the next daily run expires them)
-- are never importable. This is the authority for freshness — NOT the
-- underlying setup-event lifecycle status (a fresh proposal can reference a
-- setup whose deterministic SETUP_STATUS has aged to STALE; that must not
-- veto a current-cycle proposal).
JOIN MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN auth
    ON auth.RUN_ID = stp.BOARD_RUN_ID
WHERE stp.STATUS = 'PROPOSED'
  AND COALESCE(stp.EXECUTION_POLICY_STATUS, 'EXECUTABLE') = 'EXECUTABLE'
  AND stp.CREATED_AT >= DATEADD('day', -%s, CURRENT_DATE())
  -- STOCK-only hard gate: MIP trades STOCK live only. Non-STOCK (FX/ETF)
  -- proposals must never import into LIVE_ACTIONS as tradeable, even if a
  -- stale row was mislabelled EXECUTABLE by an older board path.
  AND COALESCE(se.MARKET_TYPE, se_ev.MARKET_TYPE, 'STOCK') = 'STOCK'
ORDER BY stp.CREATED_AT DESC
LIMIT %s
"""


def _compute_freshness(
    p: dict,
    entry_zone_tolerance_pct: float,
) -> dict:
    """Compute freshness/validation fields for a structural proposal."""
    entry_low = float(p["ENTRY_ZONE_LOW"]) if p.get("ENTRY_ZONE_LOW") is not None else None
    entry_high = float(p["ENTRY_ZONE_HIGH"]) if p.get("ENTRY_ZONE_HIGH") is not None else None
    current_price = float(p["CURRENT_PRICE"]) if p.get("CURRENT_PRICE") is not None else None
    setup_status = (p.get("SETUP_STATUS") or "").upper()
    direction = (p.get("DIRECTION") or "LONG").upper()

    setup_still_valid = setup_status in ("DETECTED", "ELIGIBLE")

    zone_width = abs(entry_high - entry_low) if entry_low is not None and entry_high is not None else 0.0
    if current_price is not None and entry_low is not None and entry_high is not None:
        if direction == "LONG":
            if current_price < entry_low:
                distance = (entry_low - current_price) / entry_low * 100
            elif current_price > entry_high:
                distance = (current_price - entry_high) / entry_high * 100
            else:
                distance = 0.0
        else:
            if current_price > entry_high:
                distance = (current_price - entry_high) / entry_high * 100
            elif current_price < entry_low:
                distance = (entry_low - current_price) / entry_low * 100
            else:
                distance = 0.0
    else:
        distance = None

    price_moved_too_far = False
    if distance is not None and zone_width > 0:
        threshold = max(entry_zone_tolerance_pct, (zone_width / ((entry_low + entry_high) / 2)) * 200)
        price_moved_too_far = distance > threshold

    # Proposal-currency is the authority for "is this still valid this cycle",
    # enforced upstream by the V_LATEST_AUTHORITATIVE_BOARD_RUN join in
    # _STRUCTURAL_PROPOSAL_QUERY (the same contract the structural timeline /
    # cockpit pending list use). The underlying setup-event SETUP_STATUS is
    # therefore intentionally NOT a freshness veto here: a fresh, current-cycle
    # proposal can legitimately reference a setup whose deterministic lifecycle
    # has aged to STALE/TRIGGERED, and must still materialise in LPA. The only
    # remaining veto is the entry-quality guard (price ran beyond a tolerant
    # entry window); downstream opening-validation / committee gates remain
    # authoritative for whether it can actually execute. setup_still_valid is
    # retained below for display/telemetry only.
    if price_moved_too_far:
        freshness = "STALE_INVALID"
    elif distance is not None and distance > entry_zone_tolerance_pct:
        freshness = "STALE_BUT_VALID"
    else:
        freshness = "CURRENT"

    return {
        "distance_to_entry_zone": round(distance, 4) if distance is not None else None,
        "setup_still_valid": setup_still_valid,
        "price_moved_too_far": price_moved_too_far,
        "freshness_assessment": freshness,
    }


def _derive_hold_character(max_hold_bars: int | None) -> str:
    if max_hold_bars is None:
        return "MEDIUM_SWING"
    if max_hold_bars <= 10:
        return "SHORT_SWING"
    if max_hold_bars <= 20:
        return "MEDIUM_SWING"
    return "PATIENT_STRUCTURAL"


def _extract_dominant_failure(failure_dist) -> str | None:
    """Extract top failure mode from FAILURE_MODE_DISTRIBUTION variant."""
    if failure_dist is None:
        return None
    if isinstance(failure_dist, str):
        try:
            failure_dist = json.loads(failure_dist)
        except (json.JSONDecodeError, TypeError):
            return None
    if isinstance(failure_dist, dict):
        if not failure_dist:
            return None
        return max(failure_dist, key=lambda k: failure_dist[k])
    return None


def _build_setup_narrative(p: dict) -> str:
    """Build a plain-language narrative from structural proposal fields."""
    family = p.get("SETUP_FAMILY") or "UNKNOWN"
    symbol = p.get("SYMBOL") or "?"
    direction = (p.get("DIRECTION") or "LONG").upper()
    level = p.get("SUPPORTING_LEVEL")
    entry_low = p.get("ENTRY_ZONE_LOW")
    entry_high = p.get("ENTRY_ZONE_HIGH")
    state = p.get("STRUCTURAL_STATE") or "?"
    regime = p.get("REGIME_COMPAT") or "?"

    level_type = "SUPPORT" if direction == "LONG" else "RESISTANCE"
    level_str = f"${level:.2f}" if level else "?"

    zone_str = ""
    if entry_low is not None and entry_high is not None:
        zone_str = f"${entry_low:.2f}\u2013${entry_high:.2f}"

    parts = [
        f"{family.replace('_', ' ').title()} near {level_type} at {level_str}.",
    ]
    if zone_str:
        parts.append(f"Entry zone: {zone_str}.")
    parts.append(f"State: {state}. Regime: {regime}.")
    return " ".join(parts)


_STRUCTURAL_IMPORT_LOCKS: dict[int, Lock] = {}
_STRUCTURAL_IMPORT_LOCKS_GUARD = Lock()


def _get_structural_import_lock(portfolio_id: int) -> Lock:
    """Per-portfolio mutex so concurrent /import-structural-proposals (and the
    auto-import path triggered by overview reloads / multiple browser tabs)
    cannot race past the "Already imported?" check and double-insert
    PENDING_OPEN_VALIDATION rows for the same PROPOSAL_ID. Snowflake offers no
    cheap row lock, so serialise at the API layer.
    """
    with _STRUCTURAL_IMPORT_LOCKS_GUARD:
        lock = _STRUCTURAL_IMPORT_LOCKS.get(int(portfolio_id))
        if lock is None:
            lock = Lock()
            _STRUCTURAL_IMPORT_LOCKS[int(portfolio_id)] = lock
        return lock


@router.post("/trades/actions/import-structural-proposals")
def import_structural_proposals(req: ImportStructuralProposalsRequest):
    """Import structural trade proposals into LIVE_ACTIONS with full canonical context."""
    _import_lock = _get_structural_import_lock(int(req.live_portfolio_id))
    if not _import_lock.acquire(timeout=30.0):
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Structural import already running for this portfolio; retry shortly.",
                "reason_codes": ["LIVE_STRUCTURAL_IMPORT_BUSY"],
            },
        )
    try:
        return _import_structural_proposals_locked(req)
    finally:
        _import_lock.release()


def _import_structural_proposals_locked(req: ImportStructuralProposalsRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()

        # 1. Validate live portfolio config
        cur.execute(
            """
            SELECT COALESCE(VALIDITY_WINDOW_SEC, 14400) AS VALIDITY_WINDOW_SEC,
                   IBKR_ACCOUNT_ID,
                   COALESCE(BROKER_NAME, 'IBKR') AS BROKER_NAME,
                   COALESCE(IBKR_ACCOUNT_MODE, 'PAPER') AS IBKR_ACCOUNT_MODE
            FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            WHERE PORTFOLIO_ID = %s AND COALESCE(IS_ACTIVE, TRUE) = TRUE
            """,
            (req.live_portfolio_id,),
        )
        cfg = cur.fetchone()
        if not cfg:
            raise HTTPException(status_code=400, detail="Live portfolio config not found or inactive.")
        validity_window_sec, ibkr_account_id, structural_broker_name, structural_universe_type = cfg
        ibkr_account_id = str(ibkr_account_id or "").strip()
        structural_broker_name = str(structural_broker_name or "IBKR").strip()
        structural_universe_type = str(structural_universe_type or "PAPER").strip()

        # 2. Read feature flags
        flags = _read_app_config(cur, ["LIVE_ENFORCE_LONG_ONLY"])
        enforce_long_only = _parse_bool_config(flags.get("LIVE_ENFORCE_LONG_ONLY"), True)

        # 3. Fetch structural proposals with all canonical joins
        cur.execute(
            _STRUCTURAL_PROPOSAL_QUERY,
            (req.max_proposal_age_days, req.limit * 3),
        )
        proposals = fetch_all(cur)

        # 4. Process each proposal
        imported = 0
        skipped_existing = 0
        skipped_long_only = 0
        skipped_stale = 0
        skipped_live_position = 0
        skipped_duplicate_symbol = 0
        skipped_contract_violations = 0
        skipped_contract_violation_details: list[dict[str, Any]] = []
        imported_action_ids: list[str] = []
        seen_symbols: set[str] = set()
        live_position_cache: dict[str, bool] = {}

        for p in proposals:
            if imported >= req.limit:
                break

            proposal_id = p.get("PROPOSAL_ID")
            symbol = (p.get("SYMBOL") or "").upper().strip()
            direction = (p.get("DIRECTION") or "LONG").upper()

            if not proposal_id or not symbol:
                continue

            p_norm = dict(p)
            p_norm["SETUP_NARRATIVE"] = p_norm.get("SETUP_NARRATIVE") or _build_setup_narrative(p_norm)

            # Trailing Stop Phase 1: resolve EXIT_POLICY from EXIT_PROFILE
            # BEFORE validation. structural_proposal_minimum_contract_violations
            # is now EXIT_POLICY-aware and only requires TRAIL_STYLE/TRAIL_PARAMS
            # when EXIT_POLICY = TRAIL_BRACKET. Unknown profile -> reject as
            # contract violation (never silently coerce to fixed).
            try:
                _resolved_policy = exit_policy_service.resolve_exit_policy_for_action(p_norm)
                p_norm["EXIT_POLICY"] = _resolved_policy["exit_policy"]
                p_norm["TRAIL_STATUS"] = _resolved_policy["trail_status"]
                p_norm["EXIT_POLICY_REASON"] = _resolved_policy["resolved_profile"]
                if _resolved_policy["exit_policy"] == exit_policy_service.EXIT_POLICY_TRAIL:
                    # Overwrite TRAIL_STYLE/TRAIL_PARAMS with the broker-executable
                    # shape for execution. STRUCTURAL_RISK_POLICY.TRAIL_PARAMS keeps
                    # its management-style shape and is not modified.
                    p_norm["TRAIL_STYLE"] = _resolved_policy["trail_style"]
                    p_norm["TRAIL_PARAMS"] = _resolved_policy["trail_params"]
                    _trail_viol = exit_policy_service.validate_trail_params(
                        _resolved_policy["trail_params"]
                    )
                    if _trail_viol:
                        skipped_contract_violations += 1
                        skipped_contract_violation_details.append({
                            "proposal_id": proposal_id,
                            "symbol": symbol,
                            "reason": "TRAIL_PARAMS_INVALID",
                            "violations": _trail_viol,
                        })
                        continue
                else:
                    # FIXED_BRACKET: clear trailing fields so execution path is
                    # unambiguous. Risk policy management TRAIL_PARAMS untouched.
                    p_norm["TRAIL_STYLE"] = None
                    p_norm["TRAIL_PARAMS"] = None
            except ValueError as exc:
                skipped_contract_violations += 1
                skipped_contract_violation_details.append({
                    "proposal_id": proposal_id,
                    "symbol": symbol,
                    "reason": "EXIT_POLICY_RESOLVE_FAILED",
                    "detail": str(exc),
                })
                continue

            _viol = structural_proposal_minimum_contract_violations(p_norm)
            if _viol:
                skipped_contract_violations += 1
                skipped_contract_violation_details.append({
                    "proposal_id": proposal_id,
                    "symbol": symbol,
                    "reason": "MINIMUM_CONTRACT",
                    "violations": _viol,
                })
                continue

            # Dedupe by symbol
            if req.dedupe_by_symbol:
                if symbol in seen_symbols:
                    skipped_duplicate_symbol += 1
                    continue
                seen_symbols.add(symbol)

            # Already imported?
            cur.execute(
                """
                SELECT ACTION_ID FROM MIP.LIVE.LIVE_ACTIONS
                WHERE PORTFOLIO_ID = %s AND PROPOSAL_ID = %s
                LIMIT 1
                """,
                (req.live_portfolio_id, proposal_id),
            )
            if cur.fetchone():
                skipped_existing += 1
                continue

            # Execution policy hard gate (backend — not UI-only).
            # Proposals with any status other than EXECUTABLE must never become
            # live actions regardless of direction, freshness, or other flags.
            exec_policy = (p.get("EXECUTION_POLICY_STATUS") or "EXECUTABLE").upper()
            if exec_policy != "EXECUTABLE":
                skipped_contract_violations += 1
                continue

            # Long-only gate
            if enforce_long_only and direction == "SHORT":
                skipped_long_only += 1
                continue

            # Freshness check
            freshness = _compute_freshness(p, req.entry_zone_tolerance_pct)
            if req.skip_stale and freshness["freshness_assessment"] == "STALE_INVALID":
                skipped_stale += 1
                continue

            # Live position check
            if ibkr_account_id and symbol:
                has_live_pos = live_position_cache.get(symbol)
                if has_live_pos is None:
                    broker_truth = _fetch_latest_broker_truth(
                        cur, ibkr_account_id, symbol, p.get("MARKET_TYPE")
                    )
                    has_live_pos = bool(broker_truth.get("has_symbol_position"))
                    live_position_cache[symbol] = has_live_pos
                if has_live_pos:
                    skipped_live_position += 1
                    continue

            # Derive computed fields
            max_hold = p.get("MAX_HOLD_BARS")
            hold_character = _derive_hold_character(max_hold)
            dominant_failure = _extract_dominant_failure(p.get("FAILURE_MODE_DISTRIBUTION"))
            narrative = str(p_norm.get("SETUP_NARRATIVE") or "")
            side = "BUY" if direction == "LONG" else "SELL"
            action_intent = "ENTRY"

            # Serialize regime tags
            regime_tags_raw = p.get("REGIME_TAGS")
            if regime_tags_raw and isinstance(regime_tags_raw, str):
                regime_tags_json = regime_tags_raw
            elif regime_tags_raw and isinstance(regime_tags_raw, dict):
                regime_tags_json = json.dumps(regime_tags_raw)
            else:
                regime_tags_json = None

            # Serialize trail params (broker-executable shape from resolved
            # exit policy; NULL for FIXED_BRACKET). Risk-policy management
            # shape is intentionally not propagated here.
            trail_params_raw = p_norm.get("TRAIL_PARAMS")
            if trail_params_raw and isinstance(trail_params_raw, str):
                trail_params_json = trail_params_raw
            elif trail_params_raw and isinstance(trail_params_raw, dict):
                trail_params_json = json.dumps(trail_params_raw)
            else:
                trail_params_json = None

            action_id = str(uuid.uuid4())
            _import_ts = datetime.now(timezone.utc).isoformat()
            proposed_price_import = _structural_ref_price_for_bracket(
                {
                    "ENTRY_ZONE_LOW": p.get("ENTRY_ZONE_LOW"),
                    "ENTRY_ZONE_HIGH": p.get("ENTRY_ZONE_HIGH"),
                    "CURRENT_PRICE": p.get("CURRENT_PRICE"),
                }
            )
            param_snapshot_structural = json.dumps(
                {
                    "structural_source": True,
                    "live_intent_kind": "STRUCTURAL",
                    "import_route": "POST /live/trades/actions/import-structural-proposals",
                    "source_table": "MIP.APP.STRUCTURAL_TRADE_PROPOSALS",
                    "import_ts": _import_ts,
                    "proposal_id": proposal_id,
                    "setup_event_id": p.get("SETUP_EVENT_ID"),
                }
            )

            cur.execute(
                """
                INSERT INTO MIP.LIVE.LIVE_ACTIONS (
                    ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, BROKER_NAME, IBKR_ACCOUNT_ID, BROKER_UNIVERSE_TYPE,
                    SYMBOL, SIDE, PROPOSED_PRICE,
                    ACTION_INTENT, STATUS,
                    VALIDITY_WINDOW_END, ASSET_CLASS,
                    COMMITTEE_REQUIRED, COMMITTEE_STATUS,
                    PARAM_SNAPSHOT, LIVE_INTENT_KIND,
                    -- Structural identity
                    SETUP_EVENT_ID, MARKET_TYPE, SETUP_FAMILY, DIRECTION,
                    -- Structural thesis
                    ENTRY_ZONE_LOW, ENTRY_ZONE_HIGH, SUPPORTING_LEVEL,
                    INVALIDATION_LEVEL, INVALIDATION_RULE,
                    STRUCTURE_CONFIDENCE, LEVEL_SIGNIFICANCE,
                    STRUCTURAL_STATE, REGIME_TAGS, REGIME_COMPAT,
                    -- Training / path evidence
                    TRUST_LABEL, MEANINGFUL_HIT_RATE, PATH_SURVIVAL_RATE,
                    MFE_MAE_RATIO, AVG_BARS_TO_THRESHOLD,
                    DOMINANT_FAILURE_MODE, BEST_WINDOW,
                    -- Trade management
                    RISK_CLASS, EXIT_STYLE, TRAIL_STYLE, TRAIL_PARAMS,
                    EXIT_POLICY, TRAIL_STATUS, EXIT_POLICY_REASON,
                    TRAIL_ACTIVATION_TYPE, TRAIL_ACTIVATION_PARAM,
                    EXPECTED_HOLD_CHARACTER, MAX_HOLD_BARS,
                    -- Narrative
                    SETUP_NARRATIVE, PROPOSAL_RATIONALE,
                    -- Freshness
                    LATEST_BAR_DATE, CURRENT_PRICE, DISTANCE_TO_ENTRY_ZONE,
                    SETUP_STILL_VALID, PRICE_MOVED_TOO_FAR, FRESHNESS_ASSESSMENT,
                    -- Timestamps
                    CREATED_AT, UPDATED_AT
                )
                SELECT
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, 'PENDING_OPEN_VALIDATION',
                    DATEADD(SECOND, %s, CURRENT_TIMESTAMP()), %s,
                    TRUE, 'PENDING',
                    PARSE_JSON(%s), 'STRUCTURAL',
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, PARSE_JSON(%s), %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s, PARSE_JSON(%s),
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                """,
                (
                    action_id, proposal_id, req.live_portfolio_id,
                    structural_broker_name, ibkr_account_id, structural_universe_type,
                    symbol, side, proposed_price_import,
                    action_intent,
                    int(validity_window_sec) if validity_window_sec else 14400, p.get("MARKET_TYPE"),
                    param_snapshot_structural,
                    p.get("SETUP_EVENT_ID"), p.get("MARKET_TYPE"), p.get("SETUP_FAMILY"), direction,
                    p.get("ENTRY_ZONE_LOW"), p.get("ENTRY_ZONE_HIGH"), p.get("SUPPORTING_LEVEL"),
                    p.get("INVALIDATION_LEVEL"), p.get("INVALIDATION_RULE"),
                    p.get("STRUCTURE_CONFIDENCE"), p.get("LEVEL_SIGNIFICANCE"),
                    p.get("STRUCTURAL_STATE"), regime_tags_json, p.get("REGIME_COMPAT"),
                    p.get("TRUST_LABEL"), p.get("MEANINGFUL_HIT_RATE"), p.get("PATH_SURVIVAL_RATE"),
                    p.get("MFE_MAE_RATIO"), p.get("AVG_BARS_TO_THRESHOLD"),
                    dominant_failure, p.get("BEST_WINDOW"),
                    p.get("RISK_CLASS"), p.get("EXIT_STYLE"), p_norm.get("TRAIL_STYLE"), trail_params_json,
                    p_norm.get("EXIT_POLICY"), p_norm.get("TRAIL_STATUS"), p_norm.get("EXIT_POLICY_REASON"),
                    p.get("TRAIL_ACTIVATION_TYPE"), p.get("TRAIL_ACTIVATION_PARAM"),
                    hold_character, max_hold,
                    narrative, p.get("PROPOSAL_RATIONALE"),
                    p.get("LATEST_BAR_DATE"), p.get("CURRENT_PRICE"), freshness["distance_to_entry_zone"],
                    freshness["setup_still_valid"], freshness["price_moved_too_far"], freshness["freshness_assessment"],
                ),
            )

            seed_row = dict(p_norm)
            seed_row.update(
                {
                    "ACTION_ID": action_id,
                    "PORTFOLIO_ID": req.live_portfolio_id,
                    "SYMBOL": symbol,
                    "SIDE": side,
                    "DIRECTION": direction,
                    "PROPOSED_PRICE": proposed_price_import,
                    "INVALIDATION_LEVEL": p.get("INVALIDATION_LEVEL"),
                    "INVALIDATION_RULE": p.get("INVALIDATION_RULE"),
                    "ENTRY_ZONE_LOW": p.get("ENTRY_ZONE_LOW"),
                    "ENTRY_ZONE_HIGH": p.get("ENTRY_ZONE_HIGH"),
                    "MAX_HOLD_BARS": max_hold,
                    "EXPECTED_HOLD_CHARACTER": hold_character,
                    "EXIT_STYLE": p.get("EXIT_STYLE"),
                    "TRAIL_STYLE": p_norm.get("TRAIL_STYLE"),
                    "TRAIL_ACTIVATION_TYPE": p.get("TRAIL_ACTIVATION_TYPE"),
                    "TRAIL_ACTIVATION_PARAM": p.get("TRAIL_ACTIVATION_PARAM"),
                }
            )
            _seed_executable_bracket_from_joint_decision(
                cur,
                action_id,
                build_structural_entry_joint_decision(seed_row),
                meta={"import_seed": True},
            )

            _append_learning_ledger_event(
                cur,
                event_name="STRUCTURAL_PROPOSAL_IMPORT",
                status="PENDING_OPEN_VALIDATION",
                action_before=None,
                action_after={
                    "ACTION_ID": action_id,
                    "PROPOSAL_ID": proposal_id,
                    "PORTFOLIO_ID": req.live_portfolio_id,
                    "SYMBOL": symbol,
                    "SIDE": side,
                    "DIRECTION": direction,
                    "SETUP_FAMILY": p.get("SETUP_FAMILY"),
                    "TRUST_LABEL": p.get("TRUST_LABEL"),
                    "FRESHNESS_ASSESSMENT": freshness["freshness_assessment"],
                },
                influence_delta={
                    "source": "STRUCTURAL_TRADE_PROPOSALS",
                    "setup_event_id": p.get("SETUP_EVENT_ID"),
                    "direction": direction,
                    "trust_label": p.get("TRUST_LABEL"),
                    "regime_compat": p.get("REGIME_COMPAT"),
                    "freshness": freshness["freshness_assessment"],
                    "trail_style": p.get("TRAIL_STYLE"),
                    "exit_style": p.get("EXIT_STYLE"),
                },
                policy_version=LIVE_POLICY_VERSION,
            )

            imported += 1
            imported_action_ids.append(action_id)

        return {
            "ok": True,
            "source": "STRUCTURAL_TRADE_PROPOSALS",
            "live_portfolio_id": req.live_portfolio_id,
            "enforce_long_only": enforce_long_only,
            "candidate_count": len(proposals),
            "imported_count": imported,
            "skipped_existing_count": skipped_existing,
            "skipped_long_only_count": skipped_long_only,
            "skipped_stale_count": skipped_stale,
            "skipped_live_position_count": skipped_live_position,
            "skipped_duplicate_symbol_count": skipped_duplicate_symbol,
            "skipped_contract_violations_count": skipped_contract_violations,
            "skipped_contract_violation_details": skipped_contract_violation_details,
            "imported_action_ids": imported_action_ids,
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
        if _live_structural_only_enabled(cur):
            return {
                "candidates": [],
                "count": 0,
                "legacy_order_proposals_retired": True,
                "message": "ORDER_PROPOSALS listing is retired under LIVE_STRUCTURAL_ONLY. Use structural proposals import.",
            }
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

# --- Entry Intelligence Snapshot read-only API ---


@router.get("/entry-intel/snapshot/by-proposal/{proposal_id}")
def get_entry_intel_snapshot_by_proposal(proposal_id: int):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select *
            from MIP.LIVE.ENTRY_INTEL_SNAPSHOT
            where PROPOSAL_ID = %s
            order by EIS_VERSION desc
            """,
            (proposal_id,),
        )
        rows = fetch_all(cur)
        return {"ok": True, "proposal_id": proposal_id, "snapshots": serialize_rows(rows)}
    finally:
        conn.close()


@router.get("/entry-intel/snapshot/by-action/{action_id}")
def get_entry_intel_snapshot_by_action(action_id: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select e.*
            from MIP.LIVE.ENTRY_INTEL_ACTION_LINK l
            join MIP.LIVE.ENTRY_INTEL_SNAPSHOT e on e.SNAPSHOT_ID = l.SNAPSHOT_ID
            where l.ENTRY_ACTION_ID = %s
            limit 1
            """,
            (action_id,),
        )
        rows = fetch_all(cur)
        return {"ok": True, "action_id": action_id, "snapshot": serialize_rows(rows)[0] if rows else None}
    finally:
        conn.close()


@router.get("/entry-intel/summary/by-action/{action_id}")
def get_entry_intel_summary_by_action(action_id: str):
    """Compact EIS projection for debugging / LIC page-open bootstrap (single fetch, no polling)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select
              e.SNAPSHOT_ID,
              e.PROPOSAL_ID,
              e.WORLDS_SPEC,
              e.ALPHA_SPEC,
              e.SOURCE_VERSION
            from MIP.LIVE.ENTRY_INTEL_ACTION_LINK l
            join MIP.LIVE.ENTRY_INTEL_SNAPSHOT e on e.SNAPSHOT_ID = l.SNAPSHOT_ID
            where l.ENTRY_ACTION_ID = %s
            limit 1
            """,
            (action_id,),
        )
        rows = fetch_all(cur)
        cur.execute(
            """
            select *
            from MIP.LIVE.TRADE_CLOSEOUT
            where ENTRY_ACTION_ID = %s
            limit 1
            """,
            (action_id,),
        )
        co_rows = fetch_all(cur)
        co_raw = serialize_rows(co_rows)[0] if co_rows else None
        if not rows:
            return {
                "ok": True,
                "action_id": action_id,
                "summary": None,
                "closeout_summary": build_closeout_summary_for_api(co_raw),
            }
        r = rows[0]
        ws = _parse_variant(r.get("WORLDS_SPEC"))
        al = _parse_variant(r.get("ALPHA_SPEC"))
        dist = ws.get("historical_distribution") if isinstance(ws.get("historical_distribution"), dict) else {}
        sup = ws.get("supporting") if isinstance(ws.get("supporting"), dict) else {}
        worlds_summary = {
            "schema_version": ws.get("schema_version"),
            "symbol": ws.get("symbol"),
            "proposal_id": ws.get("proposal_id"),
            "sample_size": dist.get("sample_size"),
            "horizon_bars": sup.get("horizon_bars"),
            "insufficient_sample": sup.get("insufficient_sample"),
        }
        alpha_summary = {
            "alpha_schema_version": al.get("alpha_schema_version"),
            "expected_value_gross": al.get("expected_value_gross"),
            "expected_value_net": al.get("expected_value_net"),
            "estimated_cost_floor": al.get("estimated_cost_floor"),
            "confidence_band": al.get("confidence_band"),
            "downside_risk_band": al.get("downside_risk_band"),
            "recommended_action": al.get("recommended_action"),
            "recommended_size_band": al.get("recommended_size_band"),
            "alpha_summary_text": al.get("alpha_summary_text"),
        }
        return {
            "ok": True,
            "action_id": action_id,
            "summary": {
                "snapshot_id": r.get("SNAPSHOT_ID"),
                "proposal_id": r.get("PROPOSAL_ID"),
                "source_version": r.get("SOURCE_VERSION"),
                "worlds_summary": worlds_summary,
                "alpha_summary": alpha_summary,
            },
            "closeout_summary": build_closeout_summary_for_api(co_raw),
        }
    finally:
        conn.close()


@router.get("/entry-intel/closeout/by-entry-action/{entry_action_id}")
def get_trade_closeout_by_entry_action(entry_action_id: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select *
            from MIP.LIVE.TRADE_CLOSEOUT
            where ENTRY_ACTION_ID = %s
            limit 1
            """,
            (entry_action_id,),
        )
        rows = fetch_all(cur)
        raw = serialize_rows(rows)[0] if rows else None
        return {
            "ok": True,
            "entry_action_id": entry_action_id,
            "closeout": raw,
            "closeout_intel": build_closeout_api_intel(raw),
        }
    finally:
        conn.close()
