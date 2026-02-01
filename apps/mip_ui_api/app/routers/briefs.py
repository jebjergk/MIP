from fastapi import APIRouter, HTTPException

from app.db import get_connection, fetch_all, serialize_row

router = APIRouter(prefix="/briefs", tags=["briefs"])


@router.get("/latest")
def get_latest_brief(portfolio_id: int):
    """Latest morning brief for portfolio from MIP.AGENT_OUT.MORNING_BRIEF."""
    sql = """
    select *
    from MIP.AGENT_OUT.MORNING_BRIEF
    where PORTFOLIO_ID = %s
      and coalesce(AGENT_NAME, '') = 'MORNING_BRIEF'
    order by AS_OF_TS desc
    limit 1
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (portfolio_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No brief found for this portfolio")
        columns = [d[0] for d in cur.description]
        return serialize_row(dict(zip(columns, row)))
    finally:
        conn.close()
