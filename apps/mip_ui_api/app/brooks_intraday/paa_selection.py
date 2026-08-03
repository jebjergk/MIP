from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from app.brooks_intraday.db_util import query_rows

from .calendar import rth_open_utc_naive
from .constants import DEFAULT_PAA_LOOKBACK_BARS
from .errors import BrooksIntradayError


@dataclass
class PaaSelectionResult:
    symbol: str
    trading_date: date
    status: str
    analysis_id: str | None
    scanned_at_utc: datetime | None
    as_of_date: date | None
    board_run_id: str | None
    selection_rule: str
    candidate_count: int
    rejected: list[dict[str, Any]]
    record: dict[str, Any] | None


def _parse_json_field(value: Any) -> dict | list | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return None


def _row_to_record(row: dict) -> dict[str, Any]:
    return {
        "analysis_id": row.get("analysis_id") or row.get("ANALYSIS_ID"),
        "symbol": row.get("symbol") or row.get("SYMBOL"),
        "created_at": row.get("created_at") or row.get("CREATED_AT"),
        "as_of_date": row.get("as_of_date") or row.get("AS_OF_DATE"),
        "verdict": row.get("verdict") or row.get("VERDICT"),
        "confidence": row.get("confidence") if row.get("confidence") is not None else row.get("CONFIDENCE"),
        "geometry_summary_json": _parse_json_field(row.get("geometry_summary_json") or row.get("GEOMETRY_SUMMARY_JSON")),
        "situation_model_json": _parse_json_field(row.get("situation_model_json") or row.get("SITUATION_MODEL_JSON")),
        "methodologist_output_json": _parse_json_field(
            row.get("methodologist_output_json") or row.get("METHODOLOGIST_OUTPUT_JSON")
        ),
        "card_ids": _parse_json_field(row.get("card_ids") or row.get("CARD_IDS")) or [],
        "lookback_bars": row.get("lookback_bars") or row.get("LOOKBACK_BARS"),
    }


def select_pre_rth_paa(
    symbol: str,
    trading_date: date,
    *,
    lookback_bars: int = DEFAULT_PAA_LOOKBACK_BARS,
    rth_open_utc: datetime | None = None,
) -> PaaSelectionResult:
    sym = symbol.strip().upper()
    cutoff = rth_open_utc or rth_open_utc_naive(trading_date)
    rows = query_rows(
        """
        SELECT
            ANALYSIS_ID,
            SYMBOL,
            CREATED_AT,
            AS_OF_DATE,
            VERDICT,
            CONFIDENCE,
            GEOMETRY_SUMMARY_JSON,
            SITUATION_MODEL_JSON,
            METHODOLOGIST_OUTPUT_JSON,
            CARD_IDS,
            LOOKBACK_BARS,
            ERROR_CLASS
        FROM MIP.KNOWLEDGE.PRICE_ACTION_ANALYSER_AUDIT
        WHERE UPPER(TRIM(SYMBOL)) = %s
          AND SIDE = 'LONG'
          AND ERROR_CLASS IS NULL
          AND NULLIF(TRIM(ANALYSIS_ID), '') IS NOT NULL
        ORDER BY CREATED_AT DESC
        """,
        (sym,),
    )

    rejected: list[dict[str, Any]] = []
    candidates: list[dict] = []
    for row in rows:
        rec = _row_to_record(row)
        aid = str(rec.get("analysis_id") or "")
        created = rec.get("created_at")
        if isinstance(created, str):
            created = datetime.fromisoformat(created[:26])
        as_of = rec.get("as_of_date")
        if isinstance(as_of, str):
            as_of = date.fromisoformat(as_of[:10])
        elif isinstance(as_of, datetime):
            as_of = as_of.date()

        if created and created >= cutoff:
            rejected.append(
                {
                    "analysis_id": aid,
                    "reason": "SCAN_AFTER_RTH_OPEN",
                    "created_at": str(created),
                }
            )
            continue
        if as_of and as_of > trading_date:
            rejected.append(
                {
                    "analysis_id": aid,
                    "reason": "AS_OF_DATE_AFTER_TRADING_DATE",
                    "as_of_date": str(as_of),
                }
            )
            continue
        candidates.append(rec)

    def _sort_key(rec: dict) -> tuple:
        created = rec.get("created_at")
        if isinstance(created, str):
            created = datetime.fromisoformat(created[:26])
        lb = int(rec.get("lookback_bars") or 0)
        lb_pref = 0 if lb == lookback_bars else 1
        return (lb_pref, -(created.timestamp() if created else 0))

    candidates.sort(key=_sort_key)

    if not candidates:
        return PaaSelectionResult(
            symbol=sym,
            trading_date=trading_date,
            status="PAA_DOSSIER_NOT_AVAILABLE_BEFORE_RTH",
            analysis_id=None,
            scanned_at_utc=None,
            as_of_date=None,
            board_run_id=None,
            selection_rule="NEWEST_PRE_RTH_AUDIT_V0_1",
            candidate_count=len(rows),
            rejected=rejected,
            record=None,
        )

    chosen = candidates[0]
    created = chosen.get("created_at")
    if isinstance(created, str):
        created = datetime.fromisoformat(created[:26])

    board_run_id = _lookup_board_run_id(sym, trading_date, str(chosen.get("analysis_id")))

    return PaaSelectionResult(
        symbol=sym,
        trading_date=trading_date,
        status="SELECTED",
        analysis_id=str(chosen.get("analysis_id")),
        scanned_at_utc=created,
        as_of_date=chosen.get("as_of_date"),
        board_run_id=board_run_id,
        selection_rule="NEWEST_PRE_RTH_AUDIT_V0_1",
        candidate_count=len(rows),
        rejected=rejected,
        record=chosen,
    )


def _lookup_board_run_id(symbol: str, trading_date: date, analysis_id: str) -> str | None:
    """Optional contextual link — warn only if board evidence references another analysis."""
    rows = query_rows(
        """
        SELECT RUN_ID, EVIDENCE_SUMMARY_JSON
        FROM MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY
        WHERE UPPER(TRIM(SYMBOL)) = %s
          AND AS_OF_DATE = %s
        ORDER BY CREATED_AT DESC
        LIMIT 5
        """,
        (symbol, trading_date),
    )
    if not rows:
        return None
    run_id = rows[0].get("run_id") or rows[0].get("RUN_ID")
    evidence = _parse_json_field(rows[0].get("evidence_summary_json") or rows[0].get("EVIDENCE_SUMMARY_JSON"))
    if isinstance(evidence, dict):
        linked = evidence.get("paa_analysis_id") or evidence.get("PAA_ANALYSIS_ID")
        if linked and str(linked) != analysis_id:
            return str(run_id)
    return str(run_id) if run_id else None


def require_paa_selection(selection: PaaSelectionResult, run_id: str) -> dict:
    if selection.status == "SELECTED" and selection.record:
        return selection.record
    raise BrooksIntradayError(
        "PAA_DOSSIER_NOT_AVAILABLE_BEFORE_RTH",
        "No eligible PAA analysis was completed before the RTH open.",
        run_id=run_id,
        symbol=selection.symbol,
        trading_date=selection.trading_date.isoformat(),
        details={
            "selection_rule": selection.selection_rule,
            "candidate_count": selection.candidate_count,
            "rejected": selection.rejected[:20],
        },
    )
