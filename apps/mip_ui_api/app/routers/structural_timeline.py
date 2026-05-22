"""
Structural Market Timeline: API endpoints for the symbol-first
structural timeline page.  All data from MIP.MART.V_STRUCTURAL_TIMELINE_* views.
"""
import json
from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, Query

from app.db import get_connection, fetch_all, serialize_rows, serialize_row

router = APIRouter(prefix="/structural-timeline", tags=["structural-timeline"])

# Phase 4 thesis verdict rows surfaced as amber monitor markers (not publication anchors).
_MONITOR_FINAL_ACTIONS = (
    "WATCH_LONG",
    "WATCH_SHORT",
    "WATCH_LONG_FAILURE",
    "WATCH_SHORT_FAILURE",
    "WAIT_FOR_CONFIRMATION",
    "CHAIR_WATCH_LONG",
    "CHAIR_WATCH_SHORT",
    "CHAIR_WATCH_LONG_FAILURE",
    "CHAIR_WATCH_SHORT_FAILURE",
    "CHAIR_WAIT_FOR_CONFIRMATION",
)


def _decode_variant_object(val: Any) -> Optional[dict]:
    """Snowflake VARIANT → dict for JSON responses (read-only surfacing)."""
    if val is None:
        return None
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s or s.lower() == "null":
            return None
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, dict) else None
        except Exception:  # noqa: BLE001
            return None
    return None


def _date_clauses(start: Optional[str], end: Optional[str], date_col: str):
    clauses, params = [], []
    if start:
        clauses.append(f"{date_col} >= %s")
        params.append(start)
    if end:
        clauses.append(f"{date_col} <= %s")
        params.append(end)
    return clauses, params


@router.get("/symbols")
def get_available_symbols(market_type: Optional[str] = Query(None)):
    """Symbol grid with setup/proposal counts, current state, and trust info."""
    conn = get_connection()
    try:
        clauses = []
        params = []
        if market_type:
            clauses.append("MARKET_TYPE = %s")
            params.append(market_type.upper())
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = (
            "SELECT SYMBOL, MARKET_TYPE,"
            " TOTAL_SETUPS, ELIGIBLE_SETUPS, PROPOSALS_CREATED, ACTIVE_PROPOSALS, TRADES_EXECUTED,"
            " DOMINANT_STATE, STRONGEST_FAMILY"
            " FROM MIP.MART.V_STRUCTURAL_TIMELINE_SUMMARY"
            + where
            + " ORDER BY ACTIVE_PROPOSALS DESC NULLS LAST, PROPOSALS_CREATED DESC NULLS LAST,"
              " TOTAL_SETUPS DESC NULLS LAST, SYMBOL"
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"symbols": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/summary")
def get_structural_timeline_summary(
    symbol: str = Query(...),
    market_type: str = Query("STOCK"),
):
    """Symbol-level KPIs for the summary strip."""
    conn = get_connection()
    try:
        sql = (
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TIMELINE_SUMMARY"
            " WHERE SYMBOL = %s AND MARKET_TYPE = %s"
        )
        cur = conn.cursor()
        cur.execute(sql, [symbol.upper(), market_type.upper()])
        rows = fetch_all(cur)
        return serialize_row(rows[0]) if rows else {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/price")
def get_structural_timeline_price(
    symbol: str = Query(...),
    market_type: str = Query("STOCK"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Daily price bars with state/regime per bar for the chart."""
    conn = get_connection()
    try:
        clauses = ["SYMBOL = %s", "MARKET_TYPE = %s"]
        params = [symbol.upper(), market_type.upper()]
        dc, dp = _date_clauses(start, end, "BAR_DATE")
        clauses.extend(dc)
        params.extend(dp)
        where = " WHERE " + " AND ".join(clauses)
        sql = "SELECT * FROM MIP.MART.V_STRUCTURAL_TIMELINE_PRICE" + where + " ORDER BY BAR_DATE"
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"bars": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/levels")
def get_structural_timeline_levels(
    symbol: str = Query(...),
    market_type: str = Query("STOCK"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Structural levels/zones — historically faithful snapshots."""
    conn = get_connection()
    try:
        clauses = ["SYMBOL = %s", "MARKET_TYPE = %s"]
        params = [symbol.upper(), market_type.upper()]
        dc, dp = _date_clauses(start, end, "AS_OF_DATE")
        clauses.extend(dc)
        params.extend(dp)
        where = " WHERE " + " AND ".join(clauses)
        sql = (
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TIMELINE_LEVELS"
            + where
            + " ORDER BY AS_OF_DATE DESC, LEVEL_SIGNIFICANCE DESC"
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"levels": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/setups")
def get_structural_timeline_setups(
    symbol: str = Query(...),
    market_type: str = Query("STOCK"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    setup_family: Optional[str] = Query(None),
    direction: Optional[str] = Query(None),
):
    """Setup events enriched with lifecycle, readiness, outcome, narrative."""
    conn = get_connection()
    try:
        clauses = ["SYMBOL = %s", "MARKET_TYPE = %s"]
        params = [symbol.upper(), market_type.upper()]
        dc, dp = _date_clauses(start, end, "SETUP_DATE")
        clauses.extend(dc)
        params.extend(dp)
        if setup_family:
            clauses.append("SETUP_FAMILY = %s")
            params.append(setup_family.upper())
        if direction:
            clauses.append("DIRECTION = %s")
            params.append(direction.upper())
        where = " WHERE " + " AND ".join(clauses)
        sql = (
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TIMELINE_SETUPS"
            + where
            + " ORDER BY SETUP_DATE DESC"
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"setups": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/proposals")
def get_structural_timeline_proposals(
    symbol: str = Query(...),
    market_type: str = Query("STOCK"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Proposals with full setup-to-trade lineage."""
    conn = get_connection()
    try:
        clauses = ["SYMBOL = %s", "MARKET_TYPE = %s"]
        params = [symbol.upper(), market_type.upper()]
        # SETUP_DATE is a legacy alias on the view (evidence date or proposal created date).
        # Fall back to PROPOSAL_CREATED_AT so filtering still works if the alias is absent.
        dc, dp = _date_clauses(start, end, "COALESCE(SETUP_DATE, PROPOSAL_CREATED_AT::DATE)")
        clauses.extend(dc)
        params.extend(dp)
        where = " WHERE " + " AND ".join(clauses)
        sql = (
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TIMELINE_PROPOSALS"
            + where
            + " ORDER BY PROPOSAL_CREATED_AT DESC"
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"proposals": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/events")
def get_structural_timeline_events(
    symbol: str = Query(...),
    market_type: str = Query("STOCK"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
):
    """Unified chronological event stream for the timeline rail."""
    conn = get_connection()
    try:
        clauses = ["SYMBOL = %s", "MARKET_TYPE = %s"]
        params = [symbol.upper(), market_type.upper()]
        dc, dp = _date_clauses(start, end, "EVENT_DATE")
        clauses.extend(dc)
        params.extend(dp)
        if event_type:
            clauses.append("EVENT_TYPE = %s")
            params.append(event_type.upper())
        where = " WHERE " + " AND ".join(clauses)
        sql = (
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TIMELINE_EVENTS"
            + where
            + " ORDER BY EVENT_DATE DESC"
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"events": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/market-structure")
def get_structural_timeline_market_structure(
    symbol: str = Query(...),
    market_type: str = Query("STOCK"),
):
    """Deterministic market_structure_map from V_SYMBOL_MARKET_STRUCTURE_MAP (read-only)."""
    conn = get_connection()
    try:
        sql = (
            "SELECT MARKET_STRUCTURE_MAP FROM MIP.MART.V_SYMBOL_MARKET_STRUCTURE_MAP"
            " WHERE SYMBOL = %s AND MARKET_TYPE = %s"
        )
        cur = conn.cursor()
        cur.execute(sql, [symbol.upper(), market_type.upper()])
        rows = fetch_all(cur)
        if not rows:
            return {"market_structure_map": None}
        row = serialize_row(rows[0])
        raw = row.get("MARKET_STRUCTURE_MAP") or row.get("market_structure_map")
        return {"market_structure_map": _decode_variant_object(raw)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/board-markers")
def get_structural_timeline_board_markers(
    symbol: str = Query(...),
    market_type: str = Query("STOCK"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Watch / monitor thesis verdicts — amber markers only; excludes PROPOSE_* and NO_TRADE/REJECT."""
    conn = get_connection()
    try:
        in_list = ",".join(["%s"] * len(_MONITOR_FINAL_ACTIONS))
        inner_where = ["v.SYMBOL = %s", "UPPER(COALESCE(v.MARKET_TYPE, 'STOCK')) = %s"]
        params: List[Any] = [symbol.upper(), market_type.upper()]
        if start:
            inner_where.append("v.CREATED_AT::DATE >= %s")
            params.append(start)
        if end:
            inner_where.append("v.CREATED_AT::DATE <= %s")
            params.append(end)
        iw = " AND ".join(inner_where)

        sql = (
            "WITH ranked AS ("
            " SELECT"
            " v.CREATED_AT::DATE AS MARKER_DATE,"
            " v.FINAL_ACTION AS FINAL_ACTION,"
            " v.RUN_ID AS RUN_ID,"
            " v.DOSSIER_ID AS DOSSIER_ID,"
            " ROW_NUMBER() OVER ("
            " PARTITION BY v.CREATED_AT::DATE, v.FINAL_ACTION ORDER BY v.CREATED_AT DESC"
            " ) AS RN"
            " FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT v"
            f" WHERE {iw}"
            ") SELECT MARKER_DATE, FINAL_ACTION, RUN_ID, DOSSIER_ID FROM ranked"
            " WHERE RN = 1 AND FINAL_ACTION IN (" + in_list + ")"
            " ORDER BY MARKER_DATE DESC"
        )
        params.extend(_MONITOR_FINAL_ACTIONS)
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"markers": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/detail")
def get_structural_timeline_detail(
    setup_event_id: int = Query(...),
):
    """Full setup detail with structural evidence and symbol-vs-family comparison."""
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TIMELINE_SETUPS WHERE SETUP_EVENT_ID = %s",
            [setup_event_id],
        )
        rows = fetch_all(cur)
        if not rows:
            raise HTTPException(status_code=404, detail="Setup event not found")
        setup = serialize_row(rows[0])

        family = setup.get("SETUP_FAMILY") or setup.get("setup_family")
        mtype = setup.get("MARKET_TYPE") or setup.get("market_type")
        symbol = setup.get("SYMBOL") or setup.get("symbol")

        symbol_comparison = {}
        if family and mtype and symbol:
            cur.execute(
                "SELECT * FROM MIP.MART.V_STRUCTURAL_TRAINING_SYMBOL"
                " WHERE SYMBOL = %s AND SETUP_FAMILY = %s AND MARKET_TYPE = %s",
                [symbol, family, mtype],
            )
            cmp_rows = fetch_all(cur)
            if cmp_rows:
                symbol_comparison = serialize_row(cmp_rows[0])

        return {
            "setup": setup,
            "symbol_vs_family": symbol_comparison,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()
