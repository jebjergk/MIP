"""
Structural Training Intelligence: API endpoints powering the new
Structural Training Intelligence page.  All data comes from the
MIP.MART.V_STRUCTURAL_TRAINING_* view family.
"""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.db import get_connection, fetch_all, serialize_rows, serialize_row

router = APIRouter(prefix="/structural-training", tags=["structural-training"])


@router.get("/summary")
def get_structural_training_summary():
    """Top-strip KPIs for the Structural Training Intelligence page."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM MIP.MART.V_STRUCTURAL_TRAINING_SUMMARY")
        rows = fetch_all(cur)
        return serialize_row(rows[0]) if rows else {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/leaderboard")
def get_structural_training_leaderboard(
    market_type: Optional[str] = Query(None),
    direction: Optional[str] = Query(None),
    trust_label: Optional[str] = Query(None),
    setup_family: Optional[str] = Query(None),
    proposal_ready_only: bool = Query(False),
):
    """Main leaderboard grid: one row per (SETUP_FAMILY, MARKET_TYPE, DIRECTION)."""
    conn = get_connection()
    try:
        clauses = []
        params = []
        if market_type:
            clauses.append("MARKET_TYPE = %s")
            params.append(market_type.upper())
        if direction:
            clauses.append("DIRECTION = %s")
            params.append(direction.upper())
        if trust_label:
            clauses.append("TRUST_LABEL = %s")
            params.append(trust_label.upper())
        if setup_family:
            clauses.append("SETUP_FAMILY = %s")
            params.append(setup_family.upper())
        if proposal_ready_only:
            clauses.append("IS_PROPOSAL_READY = TRUE")

        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = (
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TRAINING_LEADERBOARD"
            + where
            + " ORDER BY"
            "  CASE TRUST_LABEL"
            "    WHEN 'TRUSTED' THEN 1"
            "    WHEN 'PROVISIONAL' THEN 2"
            "    WHEN 'RESEARCH' THEN 3"
            "    WHEN 'REJECTED' THEN 4"
            "    ELSE 5 END,"
            "  MEANINGFUL_HIT_RATE DESC NULLS LAST"
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"rows": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/detail")
def get_structural_training_detail(
    setup_family: str = Query(..., description="Setup family name"),
    market_type: str = Query(..., description="Market type (STOCK, ETF, FX)"),
):
    """Detail view: all 3 eval windows + failure mode distribution."""
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TRAINING_DETAIL"
            " WHERE SETUP_FAMILY = %s AND MARKET_TYPE = %s"
            " ORDER BY EVAL_WINDOW",
            (setup_family.upper(), market_type.upper()),
        )
        windows = serialize_rows(fetch_all(cur))

        cur.execute(
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TRAINING_FAILURE_DIST"
            " WHERE SETUP_FAMILY = %s AND MARKET_TYPE = %s"
            " ORDER BY EVAL_WINDOW, PCT DESC",
            (setup_family.upper(), market_type.upper()),
        )
        failure_dist = serialize_rows(fetch_all(cur))

        return {"windows": windows, "failure_distribution": failure_dist}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/trend")
def get_structural_training_trend(
    setup_family: str = Query(...),
    market_type: str = Query(...),
    direction: Optional[str] = Query(None),
):
    """Monthly trend time series for a setup family."""
    conn = get_connection()
    try:
        clauses = ["SETUP_FAMILY = %s", "MARKET_TYPE = %s"]
        params = [setup_family.upper(), market_type.upper()]
        if direction:
            clauses.append("DIRECTION = %s")
            params.append(direction.upper())

        sql = (
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TRAINING_TREND"
            " WHERE " + " AND ".join(clauses) +
            " ORDER BY PERIOD_END"
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"series": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/symbol-breakdown")
def get_structural_training_symbol_breakdown(
    setup_family: str = Query(...),
    market_type: str = Query(...),
    direction: Optional[str] = Query(None),
):
    """Symbol-level breakdown within a setup family."""
    conn = get_connection()
    try:
        clauses = ["SETUP_FAMILY = %s", "MARKET_TYPE = %s"]
        params = [setup_family.upper(), market_type.upper()]
        if direction:
            clauses.append("DIRECTION = %s")
            params.append(direction.upper())

        sql = (
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TRAINING_SYMBOL"
            " WHERE " + " AND ".join(clauses) +
            " ORDER BY N_SETUPS DESC"
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"rows": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/examples")
def get_structural_training_examples(
    setup_family: str = Query(...),
    market_type: str = Query(...),
    direction: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    """Recent example setups for a given family."""
    conn = get_connection()
    try:
        clauses = ["SETUP_FAMILY = %s", "MARKET_TYPE = %s"]
        params = [setup_family.upper(), market_type.upper()]
        if direction:
            clauses.append("DIRECTION = %s")
            params.append(direction.upper())

        sql = (
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TRAINING_EXAMPLES"
            " WHERE " + " AND ".join(clauses) +
            " ORDER BY SETUP_DATE DESC"
            " LIMIT %s"
        )
        params.append(limit)
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"rows": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/comparison")
def get_structural_training_comparison(
    family_a: str = Query(...),
    family_b: str = Query(...),
    market_type: str = Query(...),
):
    """Side-by-side comparison of two setup families."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        sql = (
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TRAINING_DETAIL"
            " WHERE SETUP_FAMILY IN (%s, %s) AND MARKET_TYPE = %s"
            " ORDER BY SETUP_FAMILY, EVAL_WINDOW"
        )
        cur.execute(sql, (family_a.upper(), family_b.upper(), market_type.upper()))
        detail = serialize_rows(fetch_all(cur))

        cur.execute(
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TRAINING_FAILURE_DIST"
            " WHERE SETUP_FAMILY IN (%s, %s) AND MARKET_TYPE = %s"
            " ORDER BY SETUP_FAMILY, EVAL_WINDOW, PCT DESC",
            (family_a.upper(), family_b.upper(), market_type.upper()),
        )
        failure = serialize_rows(fetch_all(cur))

        cur.execute(
            "SELECT * FROM MIP.MART.V_STRUCTURAL_TRAINING_TREND"
            " WHERE SETUP_FAMILY IN (%s, %s) AND MARKET_TYPE = %s"
            " ORDER BY SETUP_FAMILY, PERIOD_END",
            (family_a.upper(), family_b.upper(), market_type.upper()),
        )
        trend = serialize_rows(fetch_all(cur))

        return {
            "families": [family_a.upper(), family_b.upper()],
            "market_type": market_type.upper(),
            "detail": detail,
            "failure_distribution": failure,
            "trend": trend,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()
