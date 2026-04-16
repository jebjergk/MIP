"""
Structural Market Timeline: API endpoints for the symbol-first
structural timeline page.  All data from MIP.MART.V_STRUCTURAL_TIMELINE_* views.
"""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.db import get_connection, fetch_all, serialize_rows, serialize_row

router = APIRouter(prefix="/structural-timeline", tags=["structural-timeline"])


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
    """Distinct symbols that have structural setup events."""
    conn = get_connection()
    try:
        clauses = ["MARKET_TYPE != 'ETF'"]
        params = []
        if market_type:
            clauses.append("MARKET_TYPE = %s")
            params.append(market_type.upper())
        where = " WHERE " + " AND ".join(clauses)
        sql = (
            "SELECT DISTINCT SYMBOL, MARKET_TYPE"
            " FROM MIP.APP.STRUCTURAL_SETUP_EVENTS"
            + where
            + " ORDER BY SYMBOL"
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
        dc, dp = _date_clauses(start, end, "SETUP_DATE")
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
