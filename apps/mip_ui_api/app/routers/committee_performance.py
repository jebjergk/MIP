"""
Committee Performance (Bake-off) — read-only sidecar API.

All endpoints are GET-only and read exclusively from the sidecar views:
    - MIP.MART.V_COMMITTEE_BAKEOFF_OPPORTUNITIES
    - MIP.MART.V_COMMITTEE_BAKEOFF_SCORECARD
    - MIP.MART.V_COMMITTEE_BAKEOFF_LABEL_DIST
    - MIP.MART.V_COMMITTEE_BAKEOFF_DISAGREEMENTS
    - MIP.MART.V_COMMITTEE_BAKEOFF_MTD
and the latch/outcome tables they roll up over.

No live execution, proposal pipeline, committee, shadow board, or
IBKR object is read or written by this router. The data is refreshed
out-of-band by `MIP.APP.SP_REFRESH_COMMITTEE_BAKEOFF`.
"""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.db import fetch_all, get_connection, serialize_row, serialize_rows

router = APIRouter(prefix="/committee-performance", tags=["committee-performance"])


# ---------------------------------------------------------------------------
# Top-strip KPIs
# ---------------------------------------------------------------------------
@router.get("/scorecard")
def get_scorecard():
    """Top-strip KPIs (TOTAL/SCORED/EXCLUDED + per-board hit rate, avg return)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM MIP.MART.V_COMMITTEE_BAKEOFF_SCORECARD")
        rows = fetch_all(cur)
        return serialize_row(rows[0]) if rows else {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MTD recommendation roll-up
# ---------------------------------------------------------------------------
@router.get("/mtd")
def get_mtd():
    """Month-to-date roll-up + simple PREFER_REAL/PREFER_SHADOW/TIE recommendation."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM MIP.MART.V_COMMITTEE_BAKEOFF_MTD")
        rows = fetch_all(cur)
        return serialize_row(rows[0]) if rows else {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Comparison label distribution (REAL_WIN__SHADOW_CASH etc.)
# ---------------------------------------------------------------------------
@router.get("/labels")
def get_label_distribution():
    """Counts and avg returns per COMPARISON_LABEL bucket."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM MIP.MART.V_COMMITTEE_BAKEOFF_LABEL_DIST")
        rows = fetch_all(cur)
        return {"rows": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Per-opportunity grid (real + shadow side-by-side)
# ---------------------------------------------------------------------------
@router.get("/opportunities")
def get_opportunities(
    disagreements_only: bool = Query(False),
    excluded_only: bool = Query(False),
    symbol: Optional[str] = Query(None),
    comparison_label: Optional[str] = Query(None),
    limit: int = Query(500, ge=1, le=2000),
):
    """One row per shared opportunity with both boards' verdicts and outcomes."""
    conn = get_connection()
    try:
        clauses = []
        params: list = []
        if disagreements_only:
            clauses.append("(BOARDS_DISAGREE_NORMALIZED = TRUE OR BOARDS_DISAGREE_RAW = TRUE)")
        if excluded_only:
            clauses.append("(COALESCE(REAL_EXCLUDED, FALSE) = TRUE OR COALESCE(SHADOW_EXCLUDED, FALSE) = TRUE)")
        if symbol:
            clauses.append("SYMBOL = %s")
            params.append(symbol.upper())
        if comparison_label:
            clauses.append("COMPARISON_LABEL = %s")
            params.append(comparison_label.upper())

        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = (
            "SELECT * FROM MIP.MART.V_COMMITTEE_BAKEOFF_OPPORTUNITIES"
            + where
            + " ORDER BY FIRST_DECISION_TS DESC"
            + f" LIMIT {limit}"
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"rows": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Per-opportunity drill-down (latched config provenance + outcome)
# ---------------------------------------------------------------------------
@router.get("/opportunity/{proposal_id}")
def get_opportunity_detail(proposal_id: int):
    """Latched config + outcome for both boards on a single proposal."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT l.LATCH_ID, l.PROPOSAL_ID, l.BOARD_KIND, l.SYMBOL, l.SIDE, l.DIRECTION,
                   l.DECISION_TS, l.HEARING_ID, l.SNAPSHOT_ID, l.ACTION_ID,
                   l.SHADOW_SESSION_ID, l.LATCH_SOURCE_TABLE, l.LATCH_SOURCE_ID,
                   l.LATCH_REASON, l.RAW_STANCE, l.NORMALIZED_ACTION, l.CONFIDENCE,
                   l.EVAL_CONFIG_JSON, l.CONFIG_STATUS, l.CONFIG_STATUS_REASON,
                   o.OUTCOME_ID, o.ENTRY_BAR_TS, o.ENTRY_PRICE,
                   o.EXIT_BAR_TS, o.EXIT_PRICE, o.BARS_HELD,
                   o.RAW_OUTCOME, o.REALIZED_RETURN_PCT,
                   o.MAX_FAVORABLE_PCT, o.MAX_ADVERSE_PCT,
                   o.COMPARISON_LABEL, o.SCORING_EXCLUDED_FLAG,
                   o.SCORING_EXCLUDED_REASON, o.EVAL_NOTES_JSON
              FROM MIP.APP.COMMITTEE_BAKEOFF_LATCH l
              LEFT JOIN MIP.APP.COMMITTEE_BAKEOFF_OUTCOME o
                    ON o.LATCH_ID = l.LATCH_ID
             WHERE l.PROPOSAL_ID = %s
             ORDER BY l.BOARD_KIND
            """,
            [proposal_id],
        )
        rows = fetch_all(cur)
        if not rows:
            raise HTTPException(status_code=404, detail=f"Proposal {proposal_id} not found in bake-off scope")
        return {"proposal_id": proposal_id, "boards": serialize_rows(rows)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Disagreements only (compact)
# ---------------------------------------------------------------------------
@router.get("/disagreements")
def get_disagreements(limit: int = Query(500, ge=1, le=2000)):
    """Opportunities where the two boards disagree (raw or normalized)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM MIP.MART.V_COMMITTEE_BAKEOFF_DISAGREEMENTS LIMIT {limit}"
        )
        rows = fetch_all(cur)
        return {"rows": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()
