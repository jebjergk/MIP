"""
Learning-to-Decision Ledger API.

Primary mode:
- Read canonical immutable events from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER.

Fallback mode (for pre-deploy compatibility):
- Derive activity from training snapshots + audit logs.
"""
import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Query, HTTPException

from app.db import get_connection, fetch_all, serialize_row, serialize_rows

router = APIRouter(prefix="/learning-ledger", tags=["learning-ledger"])


def _parse_json(v):
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


def _to_iso(v):
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _as_dt(v) -> datetime:
    if isinstance(v, datetime):
        return v
    if v is None:
        return datetime.min
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return datetime.min


def _taxonomy_for_event(event_name: str | None, event_type: str | None) -> str:
    name = (event_name or "").upper()
    et = (event_type or "").upper()
    if "NEWS" in name:
        return "NEWS_CONTEXT"
    if name.startswith("LIVE_") or et == "LIVE_EVENT":
        return "LIVE_EXECUTION"
    if name.startswith("TRAINING_") or et == "TRAINING_EVENT":
        return "TRAINING"
    if "PARALLEL_WORLD" in name or "PW_" in name:
        return "PARALLEL_WORLDS"
    if "COMMITTEE" in name:
        return "COMMITTEE"
    if "INTENT" in name or "COMPLIANCE" in name or "PM_" in name:
        return "APPROVALS"
    return "GENERAL"


def _chain_stage_for_event(event_name: str | None) -> str:
    name = (event_name or "").upper()
    if "RESEARCH_IMPORT" in name:
        return "RESEARCH_IMPORT"
    if "COMMITTEE" in name:
        return "COMMITTEE"
    if "PM_ACCEPT" in name:
        return "PM_APPROVAL"
    if "COMPLIANCE" in name:
        return "COMPLIANCE"
    if "INTENT_SUBMITTED" in name:
        return "INTENT_SUBMITTED"
    if "INTENT_APPROVED" in name:
        return "INTENT_APPROVED"
    if "REVALIDATION" in name:
        return "REVALIDATION"
    if "EXECUTION_REQUESTED" in name:
        return "EXECUTION_REQUESTED"
    if "ORDER_STATUS_UPDATE" in name:
        return "ORDER_STATUS"
    if "REBUILD_STATE" in name:
        return "RECOVERY"
    return "OTHER"


def _chain_key_for_event(row: dict) -> str:
    action_id = row.get("LIVE_ACTION_ID") or row.get("live_action_id")
    proposal_id = row.get("PROPOSAL_ID") or row.get("proposal_id")
    run_id = row.get("RUN_ID") or row.get("run_id")
    if action_id:
        return f"action::{action_id}"
    if proposal_id:
        return f"proposal::{proposal_id}"
    if run_id:
        return f"run::{run_id}"
    return "unknown"


def _group_events_into_chains(events: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for e in events:
        key = e.get("chain_key") or "unknown"
        grouped.setdefault(key, []).append(e)
    chains: list[dict] = []
    for key, items in grouped.items():
        ordered = sorted(items, key=lambda x: _as_dt(x.get("event_ts")))
        latest = ordered[-1] if ordered else {}
        chains.append(
            {
                "chain_key": key,
                "taxonomy_category": latest.get("taxonomy_category"),
                "latest_event_ts": latest.get("event_ts"),
                "latest_title": latest.get("title"),
                "latest_summary": latest.get("summary"),
                "run_id": latest.get("run_id"),
                "portfolio_id": latest.get("portfolio_id"),
                "live_action_id": latest.get("live_action_id"),
                "proposal_id": latest.get("proposal_id"),
                "event_count": len(ordered),
                "stages": [e.get("chain_stage") for e in ordered if e.get("chain_stage")],
                "events": ordered,
            }
        )
    chains.sort(key=lambda c: _as_dt(c.get("latest_event_ts")), reverse=True)
    return chains


def _ledger_table_exists(cur) -> bool:
    try:
        cur.execute(
            """
            select count(*) as CNT
            from MIP.INFORMATION_SCHEMA.TABLES
            where TABLE_SCHEMA = 'AGENT_OUT'
              and TABLE_NAME = 'LEARNING_DECISION_LEDGER'
            """
        )
        rows = fetch_all(cur)
        return bool(rows and (rows[0].get("CNT") or 0) > 0)
    except Exception:
        return False


def _fetch_run_impact(cur, run_id: str) -> dict:
    cur.execute(
        """
        with p as (
          select
            count(*) as proposal_count,
            count_if(STATUS = 'PROPOSED') as proposed_count,
            count_if(STATUS = 'APPROVED') as approved_count,
            count_if(STATUS = 'EXECUTED') as executed_count,
            avg(TARGET_WEIGHT) as avg_target_weight
          from MIP.AGENT_OUT.ORDER_PROPOSALS
          where RUN_ID_VARCHAR = %s
        ),
        la as (
          select
            count(*) as live_action_count
          from MIP.LIVE.LIVE_ACTIONS a
          join MIP.AGENT_OUT.ORDER_PROPOSALS p
            on p.PROPOSAL_ID = a.PROPOSAL_ID
          where p.RUN_ID_VARCHAR = %s
        ),
        lo as (
          select
            count(*) as live_order_count,
            count_if(STATUS in ('FILLED', 'PARTIAL_FILL')) as filled_or_partial_count
          from MIP.LIVE.LIVE_ORDERS o
          join MIP.LIVE.LIVE_ACTIONS a
            on a.ACTION_ID = o.ACTION_ID
          join MIP.AGENT_OUT.ORDER_PROPOSALS p
            on p.PROPOSAL_ID = a.PROPOSAL_ID
          where p.RUN_ID_VARCHAR = %s
        )
        select
          p.proposal_count,
          p.proposed_count,
          p.approved_count,
          p.executed_count,
          p.avg_target_weight,
          la.live_action_count,
          lo.live_order_count,
          lo.filled_or_partial_count
        from p cross join la cross join lo
        """,
        (run_id, run_id, run_id),
    )
    rows = fetch_all(cur)
    return serialize_row(rows[0]) if rows else {
        "proposal_count": 0,
        "proposed_count": 0,
        "approved_count": 0,
        "executed_count": 0,
        "avg_target_weight": None,
        "live_action_count": 0,
        "live_order_count": 0,
        "filled_or_partial_count": 0,
    }


def _build_canonical_feed(cur, limit: int, portfolio_id: Optional[int], event_type: Optional[str]) -> list[dict]:
    wheres = ["1=1"]
    params: list = []
    if portfolio_id is not None:
        wheres.append("PORTFOLIO_ID = %s")
        params.append(portfolio_id)
    if event_type:
        wheres.append("EVENT_TYPE = %s")
        params.append(event_type.upper())
    params.append(limit)

    cur.execute(
        f"""
        select
          LEDGER_ID,
          EVENT_TS,
          EVENT_TYPE,
          EVENT_NAME,
          STATUS,
          RUN_ID,
          PARENT_RUN_ID,
          PORTFOLIO_ID,
          PROPOSAL_ID,
          LIVE_ACTION_ID,
          LIVE_ORDER_ID,
          SYMBOL,
          MARKET_TYPE,
          TRAINING_VERSION,
          POLICY_VERSION,
          FEATURE_FLAGS,
          BEFORE_STATE,
          AFTER_STATE,
          INFLUENCE_DELTA,
          CAUSALITY_LINKS,
          OUTCOME_STATE,
          SOURCE_FACTS_HASH
        from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
        where {' and '.join(wheres)}
        order by EVENT_TS desc, LEDGER_ID desc
        limit %s
        """,
        tuple(params),
    )
    rows = fetch_all(cur)
    out = []
    for r in rows:
        influence = _parse_json(r.get("INFLUENCE_DELTA"))
        outcome = _parse_json(r.get("OUTCOME_STATE"))
        status = (r.get("STATUS") or "").upper()
        event_name = r.get("EVENT_NAME") or "LEDGER_EVENT"
        news_influence = bool(
            influence.get("news_context_state") is not None
            or influence.get("news_event_shock_flag") is not None
            or outcome.get("news_context_snapshot") is not None
            or outcome.get("news_monitoring") is not None
        )
        severity = "high" if status in ("FAIL", "ERROR") else ("medium" if status in ("FALLBACK", "PARTIAL") else "info")
        out.append({
            "event_key": f"ledger::{r.get('LEDGER_ID')}",
            "ledger_id": r.get("LEDGER_ID"),
            "event_type": r.get("EVENT_TYPE"),
            "event_name": event_name,
            "event_ts": _to_iso(r.get("EVENT_TS")),
            "severity": severity,
            "title": event_name.replace("_", " ").title(),
            "summary": f"{event_name} ({r.get('STATUS') or 'UNKNOWN'})",
            "status": r.get("STATUS"),
            "run_id": r.get("RUN_ID"),
            "portfolio_id": r.get("PORTFOLIO_ID"),
            "proposal_id": r.get("PROPOSAL_ID"),
            "live_action_id": r.get("LIVE_ACTION_ID"),
            "live_order_id": r.get("LIVE_ORDER_ID"),
            "symbol": r.get("SYMBOL"),
            "market_type": r.get("MARKET_TYPE"),
            "training_version": r.get("TRAINING_VERSION"),
            "policy_version": r.get("POLICY_VERSION"),
            "impact": serialize_row(influence),
            "news_influence_used": news_influence,
            "taxonomy_category": _taxonomy_for_event(event_name, r.get("EVENT_TYPE")),
            "chain_key": _chain_key_for_event(r),
            "chain_stage": _chain_stage_for_event(event_name),
        })
    return out


def _build_training_events(cur, limit: int) -> list[dict]:
    cur.execute(
        """
        with snapshots as (
          select
            AS_OF_TS,
            RUN_ID,
            SNAPSHOT_JSON,
            CREATED_AT,
            lag(SNAPSHOT_JSON) over (order by AS_OF_TS) as PREV_SNAPSHOT_JSON
          from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
          where SCOPE = 'GLOBAL_TRAINING'
            and SYMBOL is null
          order by AS_OF_TS desc
          limit %s
        )
        select *
        from snapshots
        order by AS_OF_TS desc
        """,
        (limit,),
    )
    rows = fetch_all(cur)
    events = []
    for r in rows:
        snapshot = _parse_json(r.get("SNAPSHOT_JSON"))
        prev_snapshot = _parse_json(r.get("PREV_SNAPSHOT_JSON"))
        run_id = r.get("RUN_ID")
        as_of_ts = r.get("AS_OF_TS")

        stages = snapshot.get("stages") or {}
        trust = snapshot.get("trust") or {}
        prev_trust = prev_snapshot.get("trust") or {}

        trusted_now = trust.get("trusted_count", 0) or 0
        trusted_prev = prev_trust.get("trusted_count", trusted_now) or trusted_now
        trusted_delta = trusted_now - trusted_prev

        detectors = snapshot.get("detectors") or []
        fired = [d for d in detectors if d.get("fired")]
        fired_names = [d.get("detector") for d in fired if d.get("detector")]
        severity = "high" if trusted_delta != 0 or len(fired) >= 2 else ("medium" if len(fired) == 1 else "info")

        run_impact = _fetch_run_impact(cur, run_id) if run_id else {}
        summary_parts = [
            f"Trusted {trusted_prev} -> {trusted_now}",
            f"Confident symbols: {stages.get('confident_count', 'n/a')}",
        ]
        if fired_names:
            summary_parts.append(f"Detectors fired: {', '.join(fired_names[:3])}")

        events.append({
            "event_key": f"training::{run_id}::{_to_iso(as_of_ts)}",
            "event_type": "TRAINING_EVENT",
            "event_ts": _to_iso(as_of_ts),
            "severity": severity,
            "title": "Training state update influenced decision readiness",
            "summary": " | ".join(summary_parts),
            "run_id": run_id,
            "portfolio_id": None,
            "event_name": "TRAINING_DIGEST_SNAPSHOT",
            "impact": {
                "trusted_delta": trusted_delta,
                "trusted_now": trusted_now,
                "confident_count": stages.get("confident_count"),
                "fired_detectors": fired_names,
                "proposal_count": run_impact.get("proposal_count"),
                "executed_count": run_impact.get("executed_count"),
                "avg_target_weight": run_impact.get("avg_target_weight"),
                "live_action_count": run_impact.get("live_action_count"),
                "live_order_count": run_impact.get("live_order_count"),
            },
        })
    return events


def _build_decision_events(cur, limit: int, portfolio_id: Optional[int]) -> list[dict]:
    wheres = [
        "EVENT_TYPE = 'AGENT'",
        "EVENT_NAME in ('SP_AGENT_PROPOSE_TRADES', 'SP_VALIDATE_AND_EXECUTE_PROPOSALS')",
    ]
    params: list = []
    if portfolio_id is not None:
        wheres.append("try_to_number(DETAILS:portfolio_id::string) = %s")
        params.append(portfolio_id)
    params.append(limit)

    cur.execute(
        f"""
        select
          EVENT_TS,
          RUN_ID,
          EVENT_NAME,
          STATUS,
          ROWS_AFFECTED,
          DETAILS
        from MIP.APP.MIP_AUDIT_LOG
        where {' and '.join(wheres)}
        order by EVENT_TS desc
        limit %s
        """,
        tuple(params),
    )
    rows = fetch_all(cur)
    events = []
    for r in rows:
        details = _parse_json(r.get("DETAILS"))
        run_id = r.get("RUN_ID")
        event_name = r.get("EVENT_NAME")
        row_portfolio_id = details.get("portfolio_id")
        run_impact = _fetch_run_impact(cur, run_id) if run_id else {}

        candidate_count = details.get("candidate_count") or details.get("candidate_count_trusted")
        proposed_count = details.get("proposed_count")
        approved_count = run_impact.get("approved_count")
        executed_count = run_impact.get("executed_count")
        trusted_rejected_count = details.get("trusted_rejected_count")
        summary = (
            f"{event_name} ({r.get('STATUS')}) | "
            f"Candidates: {candidate_count if candidate_count is not None else 'n/a'} | "
            f"Proposed: {proposed_count if proposed_count is not None else 'n/a'} | "
            f"Executed: {executed_count if executed_count is not None else 0}"
        )

        events.append({
            "event_key": f"decision::{run_id}::{event_name}::{_to_iso(r.get('EVENT_TS'))}",
            "event_type": "DECISION_EVENT",
            "event_ts": _to_iso(r.get("EVENT_TS")),
            "severity": "high" if event_name == "SP_VALIDATE_AND_EXECUTE_PROPOSALS" and (executed_count or 0) > 0 else "info",
            "title": "Learning-adjusted decision step",
            "summary": summary,
            "run_id": run_id,
            "portfolio_id": row_portfolio_id,
            "event_name": event_name,
            "impact": {
                "rows_affected": r.get("ROWS_AFFECTED"),
                "candidate_count": candidate_count,
                "proposed_count": proposed_count,
                "approved_count": approved_count,
                "executed_count": executed_count,
                "trusted_rejected_count": trusted_rejected_count,
                "avg_target_weight": run_impact.get("avg_target_weight"),
                "live_action_count": run_impact.get("live_action_count"),
                "live_order_count": run_impact.get("live_order_count"),
                "filled_or_partial_count": run_impact.get("filled_or_partial_count"),
            },
        })
    return events


@router.get("/feed")
def get_learning_ledger_feed(
    limit: int = Query(80, ge=10, le=300),
    portfolio_id: Optional[int] = Query(None),
    event_type: Optional[str] = Query(None, description="TRAINING_EVENT or DECISION_EVENT"),
    group_by_chain: bool = Query(False, description="Group canonical feed into causal chains."),
):
    """
    Combined activity feed:
    - Training digest state-change events
    - Decision-step events with proposal/live impact
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        if _ledger_table_exists(cur):
            events = _build_canonical_feed(cur, limit, portfolio_id, event_type)
            if group_by_chain:
                chains = _group_events_into_chains(events)
                return {
                    "events": serialize_rows(events),
                    "chains": serialize_rows(chains),
                    "chain_count": len(chains),
                    "count": len(events),
                    "source": "canonical_ledger",
                    "group_by_chain": True,
                }
            return {"events": serialize_rows(events), "count": len(events), "source": "canonical_ledger", "group_by_chain": False}

        training_events = _build_training_events(cur, max(20, limit // 2))
        decision_events = _build_decision_events(cur, max(20, limit // 2), portfolio_id)
        merged = training_events + decision_events
        if event_type:
            merged = [e for e in merged if e.get("event_type") == event_type.upper()]
        merged.sort(key=lambda e: _as_dt(e.get("event_ts")), reverse=True)
        merged = merged[:limit]
        return {"events": serialize_rows(merged), "count": len(merged), "source": "derived_fallback"}
    finally:
        conn.close()


@router.get("/detail")
def get_learning_ledger_detail(
    run_id: Optional[str] = Query(None),
    event_name: Optional[str] = Query(None),
    portfolio_id: Optional[int] = Query(None),
    ledger_id: Optional[int] = Query(None),
):
    """
    Drill-down for exact learning-to-decision causality around a run:
    - audit events
    - proposal records
    - linked live actions/orders
    - nearest training snapshot
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        # 0) Optional canonical ledger event payload
        ledger_row = None
        has_ledger = _ledger_table_exists(cur)
        causal_chain_rows = []
        if ledger_id is not None and has_ledger:
            cur.execute(
                """
                select *
                from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
                where LEDGER_ID = %s
                limit 1
                """,
                (ledger_id,),
            )
            lr = fetch_all(cur)
            ledger_row = serialize_row(lr[0]) if lr else None

        effective_run_id = run_id
        if not effective_run_id and ledger_row:
            effective_run_id = ledger_row.get("RUN_ID") or ledger_row.get("run_id")

        if not effective_run_id and not ledger_row:
            raise HTTPException(status_code=400, detail="Provide run_id or ledger_id.")

        if has_ledger and (ledger_row or effective_run_id):
            chain_wheres = []
            chain_params: list = []
            if ledger_row and (ledger_row.get("LIVE_ACTION_ID") or ledger_row.get("live_action_id")):
                chain_wheres.append("LIVE_ACTION_ID = %s")
                chain_params.append(ledger_row.get("LIVE_ACTION_ID") or ledger_row.get("live_action_id"))
            elif ledger_row and (ledger_row.get("PROPOSAL_ID") or ledger_row.get("proposal_id")):
                chain_wheres.append("PROPOSAL_ID = %s")
                chain_params.append(ledger_row.get("PROPOSAL_ID") or ledger_row.get("proposal_id"))
            elif effective_run_id:
                chain_wheres.append("(RUN_ID = %s OR PARENT_RUN_ID = %s)")
                chain_params.extend([effective_run_id, effective_run_id])
            if chain_wheres:
                cur.execute(
                    f"""
                    select
                      LEDGER_ID, EVENT_TS, EVENT_TYPE, EVENT_NAME, STATUS,
                      RUN_ID, PORTFOLIO_ID, PROPOSAL_ID, LIVE_ACTION_ID, LIVE_ORDER_ID,
                      INFLUENCE_DELTA, OUTCOME_STATE
                    from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
                    where {' and '.join(chain_wheres)}
                    order by EVENT_TS asc, LEDGER_ID asc
                    limit 500
                    """,
                    tuple(chain_params),
                )
                causal_chain_rows = fetch_all(cur)

        # 1) Audit trail for the run
        audit_rows = []
        if effective_run_id:
            wheres = ["(RUN_ID = %s OR PARENT_RUN_ID = %s)"]
            params = [effective_run_id, effective_run_id]
            if event_name:
                wheres.append("EVENT_NAME = %s")
                params.append(event_name)
            if portfolio_id is not None:
                wheres.append("try_to_number(DETAILS:portfolio_id::string) = %s")
                params.append(portfolio_id)

            cur.execute(
                f"""
                select
                  EVENT_TS, EVENT_TYPE, EVENT_NAME, STATUS, ROWS_AFFECTED, DETAILS,
                  ERROR_MESSAGE, ERROR_SQLSTATE, ERROR_QUERY_ID
                from MIP.APP.MIP_AUDIT_LOG
                where {' and '.join(wheres)}
                order by EVENT_TS desc
                limit 200
                """,
                tuple(params),
            )
            audit_rows = fetch_all(cur)

        # 2) Proposals for run
        proposal_rows = []
        if effective_run_id:
            proposal_wheres = ["RUN_ID_VARCHAR = %s"]
            proposal_params = [effective_run_id]
            if portfolio_id is not None:
                proposal_wheres.append("PORTFOLIO_ID = %s")
                proposal_params.append(portfolio_id)

            cur.execute(
                f"""
                select
                  PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, MARKET_TYPE, SIDE, TARGET_WEIGHT,
                  STATUS, SIGNAL_PATTERN_ID, SIGNAL_TS, APPROVED_AT, EXECUTED_AT, RATIONALE, SOURCE_SIGNALS
                from MIP.AGENT_OUT.ORDER_PROPOSALS
                where {' and '.join(proposal_wheres)}
                order by PROPOSAL_ID desc
                limit 300
                """,
                tuple(proposal_params),
            )
            proposal_rows = fetch_all(cur)

        # 3) Linked live actions/orders from those proposals
        if proposal_rows:
            proposal_ids = [r["PROPOSAL_ID"] for r in proposal_rows if r.get("PROPOSAL_ID") is not None]
        else:
            proposal_ids = []

        action_rows = []
        order_rows = []
        if proposal_ids:
            placeholders = ", ".join(["%s"] * len(proposal_ids))
            cur.execute(
                f"""
                select
                  ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, SIDE, STATUS,
                  COMPLIANCE_STATUS, PM_APPROVED_TS, COMPLIANCE_DECISION_TS,
                  REVALIDATION_TS, PRICE_GUARD_RESULT, REASON_CODES, CREATED_AT, UPDATED_AT
                from MIP.LIVE.LIVE_ACTIONS
                where PROPOSAL_ID in ({placeholders})
                order by CREATED_AT desc
                limit 300
                """,
                tuple(proposal_ids),
            )
            action_rows = fetch_all(cur)

            action_ids = [r["ACTION_ID"] for r in action_rows if r.get("ACTION_ID")]
            if action_ids:
                action_placeholders = ", ".join(["%s"] * len(action_ids))
                cur.execute(
                    f"""
                    select
                      ORDER_ID, ACTION_ID, PORTFOLIO_ID, STATUS, SYMBOL, SIDE,
                      QTY_ORDERED, QTY_FILLED, AVG_FILL_PRICE,
                      SUBMITTED_AT, ACKNOWLEDGED_AT, FILLED_AT, LAST_UPDATED_AT
                    from MIP.LIVE.LIVE_ORDERS
                    where ACTION_ID in ({action_placeholders})
                    order by LAST_UPDATED_AT desc
                    limit 300
                    """,
                    tuple(action_ids),
                )
                order_rows = fetch_all(cur)

        # 4) Nearest training snapshot for run
        training_snapshot = None
        if effective_run_id:
            cur.execute(
                """
                select
                  AS_OF_TS, RUN_ID, SNAPSHOT_JSON, SOURCE_FACTS_HASH, CREATED_AT
                from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
                where SCOPE = 'GLOBAL_TRAINING'
                  and RUN_ID = %s
                order by CREATED_AT desc
                limit 1
                """,
                (effective_run_id,),
            )
            training_rows = fetch_all(cur)
            training_snapshot = training_rows[0] if training_rows else None

        summary = {
            "audit_event_count": len(audit_rows),
            "proposal_count": len(proposal_rows),
            "live_action_count": len(action_rows),
            "live_order_count": len(order_rows),
            "executed_proposals": sum(1 for r in proposal_rows if (r.get("STATUS") or "").upper() == "EXECUTED"),
            "filled_or_partial_orders": sum(
                1 for r in order_rows if (r.get("STATUS") or "").upper() in ("FILLED", "PARTIAL_FILL")
            ),
        }

        return {
            "run_id": effective_run_id,
            "ledger_id": ledger_id,
            "event_name_filter": event_name,
            "portfolio_id_filter": portfolio_id,
            "ledger_event": ledger_row,
            "summary": serialize_row(summary),
            "training_snapshot": serialize_row(training_snapshot) if training_snapshot else None,
            "causal_chain": serialize_rows(causal_chain_rows),
            "audit_events": serialize_rows(audit_rows),
            "proposals": serialize_rows(proposal_rows),
            "live_actions": serialize_rows(action_rows),
            "live_orders": serialize_rows(order_rows),
        }
    finally:
        conn.close()


@router.get("/effectiveness")
def get_learning_effectiveness(
    days: int = Query(30, ge=7, le=180),
    portfolio_id: Optional[int] = Query(None),
):
    """
    Effectiveness summary:
    - event counts by type/status
    - proposal execution conversion
    - live order fill ratio
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        has_ledger = _ledger_table_exists(cur)
        ledger_summary = {}
        news_effectiveness = {}
        if has_ledger:
            wheres = ["EVENT_TS >= dateadd(day, -%s, current_timestamp())"]
            params: list = [days]
            if portfolio_id is not None:
                wheres.append("PORTFOLIO_ID = %s")
                params.append(portfolio_id)
            cur.execute(
                f"""
                select
                  EVENT_TYPE,
                  STATUS,
                  count(*) as CNT
                from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
                where {' and '.join(wheres)}
                group by EVENT_TYPE, STATUS
                """,
                tuple(params),
            )
            ledger_summary = serialize_rows(fetch_all(cur))
            cur.execute(
                f"""
                select
                  count(*) as NEWS_INFLUENCED_EVENTS,
                  count_if(EVENT_NAME = 'LIVE_EXECUTION_BLOCKED') as NEWS_BLOCK_EVENTS,
                  count_if(EVENT_NAME = 'LIVE_REVALIDATION') as NEWS_REVALIDATION_EVENTS,
                  count_if(EVENT_NAME = 'LIVE_EARLY_EXIT_MONITOR_RUN') as NEWS_MONITOR_EVENTS
                from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
                where {' and '.join(wheres)}
                  and (
                    INFLUENCE_DELTA:news_context_state is not null
                    or INFLUENCE_DELTA:news_event_shock_flag is not null
                    or OUTCOME_STATE:news_context_snapshot is not null
                    or OUTCOME_STATE:news_monitoring is not null
                  )
                """,
                tuple(params),
            )
            rows = fetch_all(cur)
            news_effectiveness = serialize_row(rows[0]) if rows else {}

        proposal_wheres = ["PROPOSED_AT >= dateadd(day, -%s, current_timestamp())"]
        proposal_params: list = [days]
        if portfolio_id is not None:
            proposal_wheres.append("PORTFOLIO_ID = %s")
            proposal_params.append(portfolio_id)
        cur.execute(
            f"""
            select
              count(*) as PROPOSAL_COUNT,
              count_if(STATUS = 'APPROVED') as APPROVED_COUNT,
              count_if(STATUS = 'EXECUTED') as EXECUTED_COUNT
            from MIP.AGENT_OUT.ORDER_PROPOSALS
            where {' and '.join(proposal_wheres)}
            """,
            tuple(proposal_params),
        )
        proposal_summary = serialize_row(fetch_all(cur)[0])

        action_wheres = ["a.CREATED_AT >= dateadd(day, -%s, current_timestamp())"]
        action_params: list = [days]
        if portfolio_id is not None:
            action_wheres.append("a.PORTFOLIO_ID = %s")
            action_params.append(portfolio_id)
        cur.execute(
            f"""
            select
              count(distinct a.ACTION_ID) as LIVE_ACTION_COUNT,
              count(distinct o.ORDER_ID) as LIVE_ORDER_COUNT,
              count_if(o.STATUS in ('FILLED', 'PARTIAL_FILL')) as FILLED_OR_PARTIAL_COUNT
            from MIP.LIVE.LIVE_ACTIONS a
            left join MIP.LIVE.LIVE_ORDERS o
              on o.ACTION_ID = a.ACTION_ID
            where {' and '.join(action_wheres)}
            """,
            tuple(action_params),
        )
        live_summary = serialize_row(fetch_all(cur)[0])

        return {
            "days": days,
            "portfolio_id_filter": portfolio_id,
            "ledger_source_present": has_ledger,
            "ledger_event_breakdown": ledger_summary,
            "news_effectiveness": news_effectiveness,
            "proposal_summary": proposal_summary,
            "live_summary": live_summary,
        }
    finally:
        conn.close()
