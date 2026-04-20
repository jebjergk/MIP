"""
Committee 2.0 API — structural hearing room (proposal-scoped).
Separate from MIP.LIVE.COMMITTEE_*.

Shadow Board Phase 1 endpoints are additive, feature-flagged, and read-only
with respect to all real board tables (COMMITTEE_HEARING, COMMITTEE_FINAL_DECISION).
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

_MARKET_BARS_TZ = ZoneInfo("America/New_York")

from fastapi import APIRouter, BackgroundTasks, HTTPException, Body, Query
from pydantic import BaseModel, Field

from app.db import get_connection, fetch_all, serialize_row

logger = logging.getLogger(__name__)
from app.committee.engine import (
    LiveContext,
    compute_hearing_bundle,
    bundle_to_db_json,
)
from app.committee.intraday_substantiation import (
    build_intraday_substantiation_artifact,
    fetch_intraday_bars_15m_ib,
)
from app.committee.live_politician_disclosure import build_exhibit_live_politician_disclosure_context
from app.committee.public_disclosure_context import build_exhibit_public_disclosure_context

router = APIRouter(prefix="/committee", tags=["committee"])


def _underlying_sf_conn(conn):
    """PooledConnection wraps the raw Snowflake connection in `_conn`."""
    return getattr(conn, "_conn", conn)


class HearingOpenRequest(BaseModel):
    proposal_id: int = Field(..., ge=1)
    force_rebuild: bool = False


class HearingCommitRequest(BaseModel):
    action_id: Optional[str] = None
    trade_id: Optional[str] = None
    note: Optional[str] = None


def _norm_action_id(action_id: Any) -> Optional[str]:
    if action_id is None:
        return None
    s = str(action_id).strip()
    return s if s else None


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, default=str)


def _variant(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            return v
    return v


def _committee2_enabled(cur) -> bool:
    cur.execute(
        "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
        ("COMMITTEE2_ENABLED",),
    )
    rows = fetch_all(cur)
    if not rows:
        return False
    val = (rows[0].get("CONFIG_VALUE") or "").strip().lower()
    return val in ("1", "true", "yes")


def _require_enabled(conn):
    cur = conn.cursor()
    if not _committee2_enabled(cur):
        raise HTTPException(status_code=503, detail="Committee 2.0 is disabled (COMMITTEE2_ENABLED).")


def _fetch_proposal(cur, proposal_id: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT PROPOSAL_ID, SYMBOL, DIRECTION, SETUP_FAMILY, STATUS, CREATED_AT,
               COMMITTEE_PAYLOAD, REGIME_COMPAT, STRUCTURE_CONFIDENCE
        FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
        WHERE PROPOSAL_ID = %s
        """,
        (proposal_id,),
    )
    rows = fetch_all(cur)
    return rows[0] if rows else None


def _fetch_snapshot(cur, proposal_id: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT *
        FROM MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT
        WHERE PROPOSAL_ID = %s
        """,
        (proposal_id,),
    )
    rows = fetch_all(cur)
    return rows[0] if rows else None


def _fetch_hearing_by_proposal(cur, proposal_id: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        "SELECT * FROM MIP.APP.COMMITTEE_HEARING WHERE PROPOSAL_ID = %s",
        (proposal_id,),
    )
    rows = fetch_all(cur)
    return rows[0] if rows else None


def _fetch_hearing_by_id(cur, hearing_id: str) -> Optional[Dict[str, Any]]:
    cur.execute(
        "SELECT * FROM MIP.APP.COMMITTEE_HEARING WHERE HEARING_ID = %s",
        (hearing_id,),
    )
    rows = fetch_all(cur)
    return rows[0] if rows else None


def _fetch_latest_intraday_bar(cur, symbol: str) -> Optional[Tuple[datetime, float, str, int]]:
    """
    Return (ts_utc, close, source_label, interval_minutes) for the freshest
    IBKR bar available in MIP.MART.MARKET_BARS, preferring 1-minute then
    falling back to 15/60-minute. Returns None when nothing intraday exists.

    The committee hearing's `latest_price` MUST track the live tape during the
    session — defaulting to the prior daily close (V_STRUCTURAL_TIMELINE_PRICE)
    causes every re-run to read the same stale price all day, producing
    deterministic WAIT_RECLAIM/chase-severe verdicts that never update. This
    helper supplies the fresher intraday reference; daily-bar lineage (regimes,
    structural state, recent_bar_trace) is preserved separately.
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    try:
        cur.execute(
            """
            SELECT TS, CLOSE, INTERVAL_MINUTES, SOURCE
            FROM MIP.MART.MARKET_BARS
            WHERE SYMBOL = %s
              AND INTERVAL_MINUTES = 1
              AND UPPER(COALESCE(SOURCE, '')) = 'IBKR'
            ORDER BY TS DESC
            LIMIT 1
            """,
            (sym,),
        )
        row = cur.fetchone()
        if row and row[0] is not None and row[1] is not None:
            return row[0], float(row[1]), "MARKET_BARS_1M_IBKR", 1
        cur.execute(
            """
            SELECT TS, CLOSE, INTERVAL_MINUTES, SOURCE
            FROM MIP.MART.MARKET_BARS
            WHERE SYMBOL = %s
              AND INTERVAL_MINUTES IN (15, 60)
              AND UPPER(COALESCE(SOURCE, '')) = 'IBKR'
            ORDER BY TS DESC
            LIMIT 1
            """,
            (sym,),
        )
        row = cur.fetchone()
        if row and row[0] is not None and row[1] is not None:
            interval = int(row[2]) if row[2] is not None else 0
            label = "MARKET_BARS_15M_IBKR" if interval == 15 else "MARKET_BARS_60M_IBKR"
            return row[0], float(row[1]), label, interval
    except Exception as exc:
        logger.warning("intraday bar lookup failed for %s: %s", sym, exc)
    return None


def _intraday_bar_ts_to_utc(ts: Any) -> Optional[datetime]:
    """
    Coerce a `MIP.MART.MARKET_BARS.TS` value (TIMESTAMP_NTZ stored as NY session
    clock time) into a tz-aware UTC datetime. ISO strings carrying explicit
    offsets are honored as-is; naive datetimes / naive ISO strings are assumed
    to be NY-local (matching the live router's `_market_bar_ts_to_utc`).
    """
    if ts is None:
        return None
    if isinstance(ts, datetime):
        if ts.tzinfo:
            return ts.astimezone(timezone.utc)
        return ts.replace(tzinfo=_MARKET_BARS_TZ).astimezone(timezone.utc)
    if isinstance(ts, str):
        try:
            parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo:
            return parsed.astimezone(timezone.utc)
        return parsed.replace(tzinfo=_MARKET_BARS_TZ).astimezone(timezone.utc)
    return None


def _live_context(cur, symbol: str, market_type: str = "STOCK") -> LiveContext:
    cur.execute(
        """
        SELECT BAR_DATE, OPEN, CLOSE, HIGH, LOW, STRUCTURAL_STATE, TREND_REGIME, VOL_REGIME
        FROM MIP.MART.V_STRUCTURAL_TIMELINE_PRICE
        WHERE SYMBOL = %s AND MARKET_TYPE = %s
        ORDER BY BAR_DATE DESC
        LIMIT 8
        """,
        (symbol.upper(), market_type.upper()),
    )
    bars = fetch_all(cur)
    if not bars:
        raise HTTPException(
            status_code=400,
            detail={"code": "NO_MARKET_BARS", "message": f"No daily bars for {symbol} ({market_type})."},
        )
    latest = bars[0]
    prior = bars[1] if len(bars) > 1 else None
    daily_close = float(latest["CLOSE"])
    open_p = float(latest["OPEN"]) if latest.get("OPEN") is not None else None
    prior_close = float(prior["CLOSE"]) if prior and prior.get("CLOSE") is not None else None
    dates = [str(b["BAR_DATE"]) for b in bars if b.get("BAR_DATE") is not None]
    trace_chron: List[Dict[str, Any]] = []
    for b in reversed(bars):
        if b.get("BAR_DATE") is not None and b.get("CLOSE") is not None:
            trace_chron.append({"bar_date": str(b["BAR_DATE"]), "close": float(b["CLOSE"])})

    # ── Intraday overlay ─────────────────────────────────────────────────────
    # If a fresher IBKR intraday bar exists in MIP.MART.MARKET_BARS, prefer
    # that close as `latest_price`. We require the intraday bar to be strictly
    # *newer* than the latest daily bar's date — otherwise the daily close is
    # the most recent observation and we keep it.
    price = daily_close
    price_source: Optional[str] = "DAILY_CLOSE"
    price_ts_iso: Optional[str] = None
    price_age_sec: Optional[float] = None

    intraday = _fetch_latest_intraday_bar(cur, symbol)
    if intraday is not None:
        intra_ts_raw, intra_close, intra_label, _interval = intraday
        intra_ts_utc = _intraday_bar_ts_to_utc(intra_ts_raw)
        latest_daily_date = latest.get("BAR_DATE")
        latest_daily_str = str(latest_daily_date)[:10] if latest_daily_date is not None else None
        # MARKET_BARS.TS is NY-local; daily BAR_DATE is the NY trading date.
        # Compare on NY-date so an evening UTC tick doesn't accidentally appear
        # to be a "next-day" observation relative to the daily close.
        intra_ny_date_str = (
            intra_ts_utc.astimezone(_MARKET_BARS_TZ).date().isoformat()
            if intra_ts_utc is not None else None
        )
        is_strictly_newer = (
            latest_daily_str is not None
            and intra_ny_date_str is not None
            and intra_ny_date_str > latest_daily_str
        )
        if is_strictly_newer:
            price = intra_close
            price_source = intra_label
            if intra_ts_utc is not None:
                price_ts_iso = intra_ts_utc.isoformat()
                price_age_sec = max(
                    0.0,
                    (datetime.now(timezone.utc) - intra_ts_utc).total_seconds(),
                )

    return LiveContext(
        latest_price=price,
        open_price=open_p,
        prior_close=prior_close,
        structural_state_now=latest.get("STRUCTURAL_STATE"),
        trend_regime_now=latest.get("TREND_REGIME"),
        vol_regime_now=latest.get("VOL_REGIME"),
        bar_dates=dates,
        recent_bar_trace=trace_chron,
        price_source=price_source,
        price_ts_utc=price_ts_iso,
        price_age_sec=price_age_sec,
    )


def _roles_rows(cur, hearing_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT ROLE_NAME, OUTPUT_JSON, EVIDENCE_REFS
        FROM MIP.APP.COMMITTEE_ROLE_OUTPUT
        WHERE HEARING_ID = %s
        ORDER BY ROLE_NAME
        """,
        (hearing_id,),
    )
    return fetch_all(cur)


def _artifacts_rows(cur, hearing_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT ARTIFACT_KIND, PAYLOAD_JSON, SCHEMA_VERSION, EVIDENCE_REFS
        FROM MIP.APP.COMMITTEE_EVIDENCE_ARTIFACT
        WHERE HEARING_ID = %s
        ORDER BY ARTIFACT_KIND
        """,
        (hearing_id,),
    )
    return fetch_all(cur)


def _build_snapshot_engine_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize DB snapshot row for engine (include PROPOSAL_ID / SNAPSHOT_ID keys)."""
    out = dict(row)
    for k in list(out.keys()):
        if k.endswith("_JSON") or k in ("PROPOSAL_SUMMARY_JSON",):
            out[k] = _variant(out.get(k))
    return out


def _assemble_payload(
    hearing: Dict[str, Any],
    snapshot: Dict[str, Any],
    proposal: Dict[str, Any],
    cur=None,
) -> Dict[str, Any]:
    hid = hearing.get("HEARING_ID")
    roles = []
    arts = []
    exhibit_public_disclosure_context = None
    exhibit_intraday_substantiation_map = None
    own_conn = None
    row_cur = cur
    if row_cur is None:
        own_conn = get_connection()
        row_cur = own_conn.cursor()
    try:
        for r in _roles_rows(row_cur, hid):
            roles.append(
                {
                    "role_name": r.get("ROLE_NAME"),
                    "output": _variant(r.get("OUTPUT_JSON")),
                    "evidence_refs": _variant(r.get("EVIDENCE_REFS")),
                }
            )
        for a in _artifacts_rows(row_cur, hid):
            arts.append(
                {
                    "artifact_kind": a.get("ARTIFACT_KIND"),
                    "schema_version": a.get("SCHEMA_VERSION"),
                    "payload": _variant(a.get("PAYLOAD_JSON")),
                    "evidence_refs": _variant(a.get("EVIDENCE_REFS")),
                }
            )
            if (a.get("ARTIFACT_KIND") or "").upper() == "INTRADAY_SUBSTANTIATION_MAP":
                exhibit_intraday_substantiation_map = _variant(a.get("PAYLOAD_JSON"))
        exhibit_public_disclosure_context = build_exhibit_public_disclosure_context(row_cur, proposal)
    finally:
        if own_conn is not None:
            own_conn.close()

    evidence = _variant(hearing.get("EVIDENCE_JSON")) or {}
    deltas = _variant(hearing.get("DELTAS_JSON")) or {}
    chair = _variant(hearing.get("CHAIR_OUTPUT_JSON")) or {}
    operational = _variant(hearing.get("OPERATIONAL_JSON")) or {}

    out: Dict[str, Any] = {
        "ok": True,
        "hearing_id": hid,
        "proposal_id": hearing.get("PROPOSAL_ID"),
        "snapshot_id": hearing.get("SNAPSHOT_ID"),
        "hearing_ts": serialize_row(hearing).get("HEARING_TS"),
        "updated_at": serialize_row(hearing).get("UPDATED_AT"),
        "stance": hearing.get("STANCE"),
        "confidence": hearing.get("CONFIDENCE"),
        "evidence_pack_version": hearing.get("EVIDENCE_PACK_VERSION"),
        "proposal": {
            "proposal_id": proposal.get("PROPOSAL_ID"),
            "symbol": proposal.get("SYMBOL"),
            "direction": proposal.get("DIRECTION"),
            "setup_family": proposal.get("SETUP_FAMILY"),
            "status": proposal.get("STATUS"),
        },
        "snapshot_panel": serialize_row(snapshot),
        "hearing_evidence": evidence,
        "deltas": deltas.get("categories") if isinstance(deltas, dict) else deltas,
        "chair": chair,
        "operational": operational,
        "roles": roles,
        "artifacts": arts,
    }
    if exhibit_intraday_substantiation_map is not None:
        out["exhibit_intraday_substantiation_map"] = exhibit_intraday_substantiation_map
    if exhibit_public_disclosure_context is not None:
        out["exhibit_public_disclosure_context"] = exhibit_public_disclosure_context
        exhibit_live = build_exhibit_live_politician_disclosure_context(proposal)
        if exhibit_live is not None:
            out["exhibit_live_politician_disclosure_context"] = exhibit_live
    return out


def _persist_hearing_atomic(
    conn,
    hearing_id: str,
    proposal_id: int,
    snapshot_id: int,
    bundle: Dict[str, Any],
    evidence_pack_version: str,
) -> None:
    evidence_j, deltas_j, chair_j, op_j = bundle_to_db_json(bundle)
    stance = bundle["stance"]
    conf = bundle["confidence"]
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE MIP.APP.COMMITTEE_HEARING
           SET HEARING_TS = CURRENT_TIMESTAMP(),
               SNAPSHOT_ID = %s,
               EVIDENCE_JSON = PARSE_JSON(%s),
               DELTAS_JSON = PARSE_JSON(%s),
               CHAIR_OUTPUT_JSON = PARSE_JSON(%s),
               OPERATIONAL_JSON = PARSE_JSON(%s),
               STANCE = %s,
               CONFIDENCE = %s,
               EVIDENCE_PACK_VERSION = %s,
               UPDATED_AT = CURRENT_TIMESTAMP()
         WHERE HEARING_ID = %s
        """,
        (
            snapshot_id,
            _json_dumps(evidence_j),
            _json_dumps(deltas_j),
            _json_dumps(chair_j),
            _json_dumps(op_j),
            stance,
            conf,
            evidence_pack_version,
            hearing_id,
        ),
    )
    if cur.rowcount == 0:
        # INSERT ... SELECT: Snowflake rejects PARSE_JSON(%s) inside VALUES when binds are inlined.
        cur.execute(
            """
            INSERT INTO MIP.APP.COMMITTEE_HEARING (
                HEARING_ID, PROPOSAL_ID, SNAPSHOT_ID, HEARING_TS,
                EVIDENCE_JSON, DELTAS_JSON, CHAIR_OUTPUT_JSON, OPERATIONAL_JSON,
                STANCE, CONFIDENCE, STATUS, EVIDENCE_PACK_VERSION, UPDATED_AT
            )
            SELECT
                %s, %s, %s, CURRENT_TIMESTAMP(),
                PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s),
                %s, %s, 'OPEN', %s, CURRENT_TIMESTAMP()
            """,
            (
                hearing_id,
                proposal_id,
                snapshot_id,
                _json_dumps(evidence_j),
                _json_dumps(deltas_j),
                _json_dumps(chair_j),
                _json_dumps(op_j),
                stance,
                conf,
                evidence_pack_version,
            ),
        )

    cur.execute("DELETE FROM MIP.APP.COMMITTEE_ROLE_OUTPUT WHERE HEARING_ID = %s", (hearing_id,))
    for role in bundle["roles"]:
        cur.execute(
            """
            INSERT INTO MIP.APP.COMMITTEE_ROLE_OUTPUT (HEARING_ID, ROLE_NAME, OUTPUT_JSON, EVIDENCE_REFS, UPDATED_AT)
            SELECT %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), CURRENT_TIMESTAMP()
            """,
            (
                hearing_id,
                role["role_name"],
                _json_dumps(
                    {
                        "stance_badge": role.get("stance_badge"),
                        "one_liner": role.get("one_liner"),
                        "bullets": role.get("bullets"),
                        "influence": role.get("influence"),
                        "output": role.get("output"),
                    }
                ),
                _json_dumps(role.get("evidence_refs") or []),
            ),
        )

    cur.execute("DELETE FROM MIP.APP.COMMITTEE_EVIDENCE_ARTIFACT WHERE HEARING_ID = %s", (hearing_id,))
    for art in bundle["artifacts"]:
        cur.execute(
            """
            INSERT INTO MIP.APP.COMMITTEE_EVIDENCE_ARTIFACT (
                HEARING_ID, ARTIFACT_KIND, PAYLOAD_JSON, SCHEMA_VERSION, EVIDENCE_REFS, UPDATED_AT
            )
            SELECT %s, %s, PARSE_JSON(%s), %s, PARSE_JSON(%s), CURRENT_TIMESTAMP()
            """,
            (
                hearing_id,
                art["artifact_kind"],
                _json_dumps(art.get("payload") or {}),
                art.get("schema_version") or "1",
                _json_dumps(art.get("evidence_refs") or []),
            ),
        )


def _run_refresh(conn, hearing_id: str, proposal_id: int, snapshot: Dict[str, Any], proposal: Dict[str, Any]) -> Dict[str, Any]:
    symbol = proposal.get("SYMBOL") or snapshot.get("SYMBOL")
    cur = conn.cursor()
    live = _live_context(cur, symbol)
    snap_eng = _build_snapshot_engine_dict(snapshot)
    bundle = compute_hearing_bundle(snap_eng, live)
    try:
        bars_15 = fetch_intraday_bars_15m_ib(str(symbol or "").strip(), market_type=None)
        intraday_art = build_intraday_substantiation_artifact(
            snap_eng,
            live,
            bars_15,
            snapshot.get("PROPOSAL_TS"),
        )
        if intraday_art:
            bundle["artifacts"].append(intraday_art)
    except Exception:
        pass
    ver = "1.0.0"
    _persist_hearing_atomic(conn, hearing_id, proposal_id, int(snapshot["SNAPSHOT_ID"]), bundle, ver)
    cur.execute("SELECT * FROM MIP.APP.COMMITTEE_HEARING WHERE HEARING_ID = %s", (hearing_id,))
    hrows = fetch_all(cur)
    hearing = hrows[0]
    return _assemble_payload(hearing, snapshot, proposal, cur=cur)


def committee_final_decision_commit_for_action(cur, hearing_id: str, req: HearingCommitRequest) -> Dict[str, Any]:
    """
    Insert or reconcile COMMITTEE_FINAL_DECISION for a hearing with ACTION_ID-aware rules:
    same action_id -> idempotent; null ACTION_ID + request action_id -> UPDATE; conflicting ACTION_ID -> 409.
    """
    cur.execute(
        "SELECT * FROM MIP.APP.COMMITTEE_FINAL_DECISION WHERE HEARING_ID = %s",
        (hearing_id,),
    )
    existing_fd = fetch_all(cur)
    req_aid = _norm_action_id(req.action_id)

    if existing_fd:
        row_raw = existing_fd[0]
        ex_aid = _norm_action_id(row_raw.get("ACTION_ID"))
        if ex_aid is not None and req_aid is None:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "This hearing is already bound to an action; pass action_id to confirm.",
                    "reason_codes": ["COMMITTEE2_ACTION_ID_REQUIRED"],
                },
            )
        if ex_aid is not None and req_aid is not None and ex_aid != req_aid:
            # Before refusing, check whether the previously-bound action is
            # actually a *legitimate* binding for this hearing. The hearing's
            # PROPOSAL_ID must match the bound action's PROPOSAL_ID, and the
            # bound action must still exist and not be in a terminal state.
            # Otherwise we are looking at a corrupted / stale binding from a
            # prior import / action-supersede flow — re-bind to the current
            # request rather than block the operator forever.
            stale_bind = False
            stale_reason: str | None = None
            try:
                hearing_lookup = _fetch_hearing_by_id(cur, hearing_id)
                hearing_proposal_id = (
                    int(hearing_lookup["PROPOSAL_ID"])
                    if hearing_lookup and hearing_lookup.get("PROPOSAL_ID") is not None
                    else None
                )
            except Exception:
                hearing_lookup = None
                hearing_proposal_id = None
            try:
                cur.execute(
                    """
                    SELECT ACTION_ID, PROPOSAL_ID, STATUS
                    FROM MIP.LIVE.LIVE_ACTIONS
                    WHERE ACTION_ID = %s
                    """,
                    (ex_aid,),
                )
                bound_rows = fetch_all(cur)
            except Exception:
                bound_rows = []
            if not bound_rows:
                stale_bind = True
                stale_reason = "BOUND_ACTION_MISSING"
            else:
                bound_row = bound_rows[0]
                bound_proposal = bound_row.get("PROPOSAL_ID")
                bound_status = str(bound_row.get("STATUS") or "").upper()
                if (
                    hearing_proposal_id is not None
                    and bound_proposal is not None
                    and int(bound_proposal) != int(hearing_proposal_id)
                ):
                    stale_bind = True
                    stale_reason = "BOUND_ACTION_PROPOSAL_MISMATCH"
                elif bound_status in {
                    "EXECUTED",
                    "REJECTED",
                    "CANCELLED",
                    "CLOSED",
                    "EXPIRED",
                    "SUPERSEDED",
                }:
                    stale_bind = True
                    stale_reason = f"BOUND_ACTION_TERMINAL:{bound_status}"
            if stale_bind:
                cur.execute(
                    """
                    UPDATE MIP.APP.COMMITTEE_FINAL_DECISION
                       SET ACTION_ID = %s,
                           TRADE_ID = COALESCE(%s, TRADE_ID),
                           COMMIT_NOTE = COALESCE(%s, COMMIT_NOTE)
                     WHERE HEARING_ID = %s
                    """,
                    (req.action_id, req.trade_id, req.note, hearing_id),
                )
                # Refresh row_raw / ex_aid so the downstream stance-drift
                # handler sees the up-to-date binding.
                cur.execute(
                    "SELECT * FROM MIP.APP.COMMITTEE_FINAL_DECISION WHERE HEARING_ID = %s",
                    (hearing_id,),
                )
                refreshed = fetch_all(cur)
                if refreshed:
                    row_raw = refreshed[0]
                    ex_aid = _norm_action_id(row_raw.get("ACTION_ID"))
            else:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "Final decision for this hearing is bound to a different LIVE_ACTIONS row.",
                        "reason_codes": ["COMMITTEE2_HEARING_BOUND_TO_OTHER_ACTION"],
                        "bound_action_id": ex_aid,
                    },
                )
        if ex_aid is None and req_aid is not None:
            cur.execute(
                """
                UPDATE MIP.APP.COMMITTEE_FINAL_DECISION
                   SET ACTION_ID = %s,
                       TRADE_ID = COALESCE(%s, TRADE_ID),
                       COMMIT_NOTE = COALESCE(%s, COMMIT_NOTE)
                 WHERE HEARING_ID = %s
                """,
                (req.action_id, req.trade_id, req.note, hearing_id),
            )
            cur.execute(
                "SELECT * FROM MIP.APP.COMMITTEE_FINAL_DECISION WHERE HEARING_ID = %s",
                (hearing_id,),
            )
            row = serialize_row(fetch_all(cur)[0])
            row["already_committed"] = True
            row["action_id_bound_updated"] = True
            row["ok"] = True
            return row

        # Same action already bound to this hearing. The committee may have
        # been re-run against a fresher intraday price (LPA "Re-run hearing"),
        # producing a new stance on COMMITTEE_HEARING while the existing
        # FINAL_DECISION row still carries the previous stance. Detect stance
        # drift and UPDATE the FD row in-place so the downstream materialize
        # step can transition LIVE_ACTIONS.STATUS (e.g. OPEN_BLOCKED ->
        # READY_FOR_APPROVAL_FLOW) instead of silently re-playing the stale
        # BLOCK verdict.
        hearing_latest = _fetch_hearing_by_id(cur, hearing_id)
        if hearing_latest is not None:
            hearing_stance = str(hearing_latest.get("STANCE") or "").upper()
            hearing_conf = hearing_latest.get("CONFIDENCE")
            committed_stance = str(row_raw.get("STANCE") or "").upper()
            try:
                committed_conf = (
                    float(row_raw.get("CONFIDENCE"))
                    if row_raw.get("CONFIDENCE") is not None
                    else None
                )
            except (TypeError, ValueError):
                committed_conf = None
            try:
                hearing_conf_f = (
                    float(hearing_conf) if hearing_conf is not None else None
                )
            except (TypeError, ValueError):
                hearing_conf_f = None
            stance_drift = bool(hearing_stance) and hearing_stance != committed_stance
            conf_drift = (
                hearing_conf_f is not None
                and committed_conf is not None
                and abs(hearing_conf_f - committed_conf) >= 1e-6
            )
            if stance_drift or conf_drift:
                chair_h = _variant(hearing_latest.get("CHAIR_OUTPUT_JSON")) or {}
                deltas_h = _variant(hearing_latest.get("DELTAS_JSON")) or {}
                operational_h = _variant(hearing_latest.get("OPERATIONAL_JSON")) or {}
                roles_out_h: Dict[str, Any] = {}
                for r in _roles_rows(cur, hearing_id):
                    roles_out_h[r["ROLE_NAME"]] = _variant(r.get("OUTPUT_JSON"))
                arts_h: Dict[str, Any] = {}
                for a in _artifacts_rows(cur, hearing_id):
                    arts_h[a["ARTIFACT_KIND"]] = {
                        "payload": _variant(a.get("PAYLOAD_JSON")),
                        "schema_version": a.get("SCHEMA_VERSION"),
                        "evidence_refs": _variant(a.get("EVIDENCE_REFS")),
                    }
                posture_h = (operational_h or {}).get("posture") or {}
                evidence_refs_h = (chair_h or {}).get("evidence_refs") or []
                cur.execute(
                    """
                    UPDATE MIP.APP.COMMITTEE_FINAL_DECISION
                       SET STANCE = %s,
                           CONFIDENCE = %s,
                           CHAIR_OUTPUT_JSON = PARSE_JSON(%s),
                           ROLE_OUTPUTS_JSON = PARSE_JSON(%s),
                           DELTA_SUMMARY_JSON = PARSE_JSON(%s),
                           POSTURE_JSON = PARSE_JSON(%s),
                           EVIDENCE_REFS_JSON = PARSE_JSON(%s),
                           ARTIFACTS_JSON = PARSE_JSON(%s),
                           COMMIT_NOTE = COALESCE(%s, COMMIT_NOTE),
                           DECISION_TS = CURRENT_TIMESTAMP()
                     WHERE HEARING_ID = %s
                    """,
                    (
                        hearing_latest.get("STANCE"),
                        hearing_conf,
                        _json_dumps(chair_h),
                        _json_dumps(roles_out_h),
                        _json_dumps(deltas_h),
                        _json_dumps(posture_h),
                        _json_dumps(evidence_refs_h),
                        _json_dumps(arts_h),
                        req.note,
                        hearing_id,
                    ),
                )
                cur.execute(
                    "SELECT * FROM MIP.APP.COMMITTEE_FINAL_DECISION WHERE HEARING_ID = %s",
                    (hearing_id,),
                )
                row = serialize_row(fetch_all(cur)[0])
                # Signal to orchestrate that materialize MUST re-run so
                # LIVE_ACTIONS.STATUS / COMMITTEE_VERDICT can catch up.
                row["already_committed"] = False
                row["stance_drift_applied"] = True
                row["previous_stance"] = committed_stance or None
                row["ok"] = True
                return row

        row = serialize_row(row_raw)
        row["already_committed"] = True
        row["ok"] = True
        return row

    hearing = _fetch_hearing_by_id(cur, hearing_id)
    if not hearing:
        raise HTTPException(status_code=404, detail="Hearing not found.")
    proposal_id = int(hearing["PROPOSAL_ID"])
    snapshot = _fetch_snapshot(cur, proposal_id)
    if not snapshot:
        raise HTTPException(status_code=400, detail="Snapshot missing.")

    evidence = _variant(hearing.get("EVIDENCE_JSON")) or {}
    deltas = _variant(hearing.get("DELTAS_JSON")) or {}
    chair = _variant(hearing.get("CHAIR_OUTPUT_JSON")) or {}
    operational = _variant(hearing.get("OPERATIONAL_JSON")) or {}
    stance = hearing.get("STANCE")
    conf = hearing.get("CONFIDENCE")

    roles_out: Dict[str, Any] = {}
    for r in _roles_rows(cur, hearing_id):
        roles_out[r["ROLE_NAME"]] = _variant(r.get("OUTPUT_JSON"))
    arts: Dict[str, Any] = {}
    for a in _artifacts_rows(cur, hearing_id):
        arts[a["ARTIFACT_KIND"]] = {
            "payload": _variant(a.get("PAYLOAD_JSON")),
            "schema_version": a.get("SCHEMA_VERSION"),
            "evidence_refs": _variant(a.get("EVIDENCE_REFS")),
        }

    posture = (operational or {}).get("posture") or {}
    evidence_refs = (chair or {}).get("evidence_refs") or []

    try:
        cur.execute(
            """
            INSERT INTO MIP.APP.COMMITTEE_FINAL_DECISION (
                HEARING_ID, PROPOSAL_ID, SNAPSHOT_ID, STANCE, CONFIDENCE,
                CHAIR_OUTPUT_JSON, ROLE_OUTPUTS_JSON, DELTA_SUMMARY_JSON, POSTURE_JSON,
                EVIDENCE_REFS_JSON, ARTIFACTS_JSON, ACTION_ID, TRADE_ID, COMMIT_NOTE, DECISION_TS
            )
            SELECT
                %s, %s, %s, %s, %s,
                PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s),
                PARSE_JSON(%s), PARSE_JSON(%s), %s, %s, %s, CURRENT_TIMESTAMP()
            """,
            (
                hearing_id,
                proposal_id,
                int(hearing["SNAPSHOT_ID"]),
                stance,
                conf,
                _json_dumps(chair),
                _json_dumps(roles_out),
                _json_dumps(deltas),
                _json_dumps(posture),
                _json_dumps(evidence_refs),
                _json_dumps(arts),
                req.action_id,
                req.trade_id,
                req.note,
            ),
        )
    except Exception as exc:
        err = str(exc).lower()
        if "unique" in err or "already exists" in err:
            cur.execute(
                "SELECT * FROM MIP.APP.COMMITTEE_FINAL_DECISION WHERE HEARING_ID = %s",
                (hearing_id,),
            )
            rows = fetch_all(cur)
            if rows:
                return committee_final_decision_commit_for_action(cur, hearing_id, req)
        raise
    cur.execute(
        "SELECT * FROM MIP.APP.COMMITTEE_FINAL_DECISION WHERE HEARING_ID = %s",
        (hearing_id,),
    )
    row = serialize_row(fetch_all(cur)[0])
    row["already_committed"] = False
    row["ok"] = True
    return row


@router.post("/hearing/open")
def committee_hearing_open(req: HearingOpenRequest = Body(...)):
    conn = get_connection()
    try:
        _require_enabled(conn)
        cur = conn.cursor()
        proposal = _fetch_proposal(cur, req.proposal_id)
        if not proposal:
            raise HTTPException(status_code=404, detail="Proposal not found.")
        snapshot = _fetch_snapshot(cur, req.proposal_id)
        if not snapshot:
            raise HTTPException(
                status_code=400,
                detail={"code": "NO_SNAPSHOT", "message": "Immutable proposal snapshot missing; re-run structural propose or backfill snapshots."},
            )

        existing = _fetch_hearing_by_proposal(cur, req.proposal_id)
        if existing and not req.force_rebuild:
            hearing = existing
            return _assemble_payload(hearing, snapshot, proposal)

        hearing_id = existing["HEARING_ID"] if existing else str(uuid.uuid4())

        raw = conn._conn  # underlying snowflake connection for transaction
        raw.autocommit(False)
        try:
            payload = _run_refresh(conn, hearing_id, req.proposal_id, snapshot, proposal)
            raw.commit()
        except Exception:
            raw.rollback()
            raise
        finally:
            raw.autocommit(True)

        payload["rebuilt"] = bool(req.force_rebuild or not existing)
        return payload
    finally:
        conn.close()


@router.get("/hearing/{hearing_id}")
def committee_hearing_get(hearing_id: str):
    conn = get_connection()
    try:
        _require_enabled(conn)
        cur = conn.cursor()
        hearing = _fetch_hearing_by_id(cur, hearing_id)
        if not hearing:
            raise HTTPException(status_code=404, detail="Hearing not found.")
        proposal = _fetch_proposal(cur, int(hearing["PROPOSAL_ID"]))
        snapshot = _fetch_snapshot(cur, int(hearing["PROPOSAL_ID"]))
        if not proposal or not snapshot:
            raise HTTPException(status_code=404, detail="Proposal or snapshot missing for hearing.")
        return _assemble_payload(hearing, snapshot, proposal)
    finally:
        conn.close()


@router.post("/hearing/{hearing_id}/refresh")
def committee_hearing_refresh(hearing_id: str):
    conn = get_connection()
    try:
        _require_enabled(conn)
        cur = conn.cursor()
        hearing = _fetch_hearing_by_id(cur, hearing_id)
        if not hearing:
            raise HTTPException(status_code=404, detail="Hearing not found.")
        proposal_id = int(hearing["PROPOSAL_ID"])
        proposal = _fetch_proposal(cur, proposal_id)
        snapshot = _fetch_snapshot(cur, proposal_id)
        if not proposal or not snapshot:
            raise HTTPException(status_code=404, detail="Proposal or snapshot missing.")

        raw = _underlying_sf_conn(conn)
        raw.autocommit(False)
        try:
            payload = _run_refresh(conn, hearing_id, proposal_id, snapshot, proposal)
            raw.commit()
        except Exception:
            raw.rollback()
            raise
        finally:
            raw.autocommit(True)
        payload["refreshed"] = True
        return payload
    finally:
        conn.close()


@router.post("/hearing/{hearing_id}/commit")
def committee_hearing_commit(hearing_id: str, req: HearingCommitRequest = Body(default_factory=HearingCommitRequest)):
    conn = get_connection()
    try:
        _require_enabled(conn)
        cur = conn.cursor()
        return committee_final_decision_commit_for_action(cur, hearing_id, req)
    finally:
        conn.close()


@router.get("/proposal/{proposal_id}/final-decision")
def committee_proposal_final_decision(proposal_id: int):
    conn = get_connection()
    try:
        _require_enabled(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM MIP.APP.COMMITTEE_FINAL_DECISION
            WHERE PROPOSAL_ID = %s
            ORDER BY DECISION_TS DESC
            LIMIT 1
            """,
            (proposal_id,),
        )
        rows = fetch_all(cur)
        if not rows:
            return {"ok": True, "final_decision": None}
        return {"ok": True, "final_decision": serialize_row(rows[0])}
    finally:
        conn.close()


# ===========================================================================
# Shadow Board Phase 1 — feature-flagged, zero live authority
# ===========================================================================

def _shadow_board_enabled(cur) -> bool:
    cur.execute(
        "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
        ("SHADOW_BOARD_ENABLED",),
    )
    rows = fetch_all(cur)
    if not rows:
        return False
    val = (rows[0].get("CONFIG_VALUE") or "").strip().lower()
    return val in ("1", "true", "yes")


def _shadow_timeout_sec(cur) -> float:
    cur.execute(
        "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
        ("SHADOW_BOARD_TIMEOUT_SEC",),
    )
    rows = fetch_all(cur)
    if not rows:
        return 120.0
    try:
        return float(rows[0].get("CONFIG_VALUE") or "120")
    except (TypeError, ValueError):
        return 120.0


def _require_shadow_enabled(conn):
    cur = conn.cursor()
    if not _committee2_enabled(cur):
        raise HTTPException(status_code=503, detail="Committee 2.0 is disabled (COMMITTEE2_ENABLED).")
    if not _shadow_board_enabled(cur):
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SHADOW_BOARD_DISABLED",
                "message": "Shadow Board is disabled (SHADOW_BOARD_ENABLED). Set to 'true' in APP_CONFIG to enable.",
            },
        )


@router.post("/hearing/{hearing_id}/shadow-board/run")
async def committee_shadow_board_run(
    hearing_id: str,
    force: bool = Query(
        False,
        description=(
            "Phase 1 dual-hearing: this manual endpoint is now diagnostics/replay only. "
            "The primary kickoff fires automatically from the LPA orchestrate path. "
            "Pass force=true to bypass the diagnostics gate and re-run anyway."
        ),
    ),
):
    """
    Diagnostics/replay endpoint — runs a shadow board session for the given hearing.

    Phase 1 dual-hearing: the shadow board is normally launched automatically
    by the LPA orchestrate path against the same frozen snapshot as the real
    board. This endpoint is reserved for replay / diagnostic re-runs and is
    gated behind `?force=true`. Without `force=true` it returns 409.

    Shadow Board is non-executing. Zero interaction with COMMITTEE_FINAL_DECISION.
    Requires SHADOW_BOARD_ENABLED=true in APP_CONFIG.
    """
    from app.committee.shadow_board import orchestrate_shadow_board, fetch_shadow_session

    if not force:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "SHADOW_MANUAL_RUN_GATED",
                "message": (
                    "Shadow board now runs automatically alongside the LPA hearing. "
                    "Pass ?force=true to re-run for diagnostics/replay."
                ),
            },
        )

    conn = get_connection()
    try:
        _require_shadow_enabled(conn)
        cur = conn.cursor()
        timeout_sec = _shadow_timeout_sec(cur)

        # Verify hearing exists
        cur.execute("SELECT HEARING_ID, PROPOSAL_ID FROM MIP.APP.COMMITTEE_HEARING WHERE HEARING_ID = %s", (hearing_id,))
        rows = fetch_all(cur)
        if not rows:
            raise HTTPException(status_code=404, detail="Hearing not found.")
    finally:
        conn.close()

    logger.info("shadow_board_run: starting async run for hearing %s (timeout=%ss)", hearing_id, timeout_sec)

    # Run shadow board fully — this is an async endpoint so we can await it.
    # For long-running cases the client can poll; we return the completed result.
    try:
        result = await asyncio.wait_for(
            orchestrate_shadow_board(hearing_id, timeout_sec=timeout_sec),
            timeout=timeout_sec + 30,
        )
        return {
            "ok": True,
            "session_id": result.session_id,
            "hearing_id": hearing_id,
            "status": result.status,
            "shadow_stance": result.shadow_stance,
            "shadow_confidence": result.shadow_confidence,
            "stage_reached": result.stage_reached,
            "degraded": result.degraded,
            "degraded_reason": result.degraded_reason,
            "run_ms": result.run_ms,
        }
    except asyncio.TimeoutError:
        logger.error("shadow_board_run: timed out for hearing %s", hearing_id)
        raise HTTPException(
            status_code=504,
            detail={
                "code": "SHADOW_BOARD_TIMEOUT",
                "message": f"Shadow board run timed out after {timeout_sec + 30:.0f}s.",
            },
        )
    except Exception as exc:
        logger.error("shadow_board_run: failed for hearing %s: %s", hearing_id, exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "code": "SHADOW_BOARD_ERROR",
                "message": f"Shadow board run failed: {str(exc)[:200]}",
            },
        )


@router.get("/hearing/{hearing_id}/shadow-board")
def committee_shadow_board_get(
    hearing_id: str,
    include_progress: bool = Query(
        False,
        description=(
            "If true, return only the lightweight session-status payload "
            "(status, stage_reached, evidence_pack_hash). Used by the LPA "
            "auto-poll loop while the shadow board is still RUNNING."
        ),
    ),
):
    """
    Retrieve the most recent shadow board session for a hearing.
    Returns full structured payload (positions, conflicts, challenge, revisions, chair).

    Pass ?include_progress=1 to get a lightweight status-only payload
    suitable for high-frequency polling while the session is still RUNNING.

    Returns 404 if no shadow session exists yet for this hearing.
    Shadow data is advisory only. Real board stance is never included here.
    """
    from app.committee.shadow_board import fetch_shadow_session, fetch_shadow_progress

    conn = get_connection()
    try:
        _require_shadow_enabled(conn)
        # Verify hearing exists
        cur = conn.cursor()
        cur.execute("SELECT HEARING_ID FROM MIP.APP.COMMITTEE_HEARING WHERE HEARING_ID = %s", (hearing_id,))
        if not fetch_all(cur):
            raise HTTPException(status_code=404, detail="Hearing not found.")
    finally:
        conn.close()

    if include_progress:
        progress = fetch_shadow_progress(hearing_id)
        if progress is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "SHADOW_SESSION_NOT_FOUND",
                    "message": "No shadow board session found for this hearing yet.",
                },
            )
        return progress

    result = fetch_shadow_session(hearing_id)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "SHADOW_SESSION_NOT_FOUND",
                "message": "No shadow board session found for this hearing. The LPA orchestrate path normally creates one automatically.",
            },
        )
    return result
