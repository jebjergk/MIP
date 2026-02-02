from fastapi import APIRouter

from app.db import get_connection, serialize_row

router = APIRouter(prefix="/briefs", tags=["briefs"])


@router.get("/latest")
def get_latest_brief(portfolio_id: int):
    """
    Latest morning brief for portfolio from MIP.AGENT_OUT.MORNING_BRIEF.
    Returns 200 with found=true and contract fields, or found=false when no brief exists.
    """
    sql = """
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
      mb.AGENT_NAME as agent_name,
      mb.BRIEF as brief_json
    from MIP.AGENT_OUT.MORNING_BRIEF mb
    where mb.PORTFOLIO_ID = %s
      and coalesce(mb.AGENT_NAME, '') = 'MORNING_BRIEF'
    order by mb.AS_OF_TS desc
    limit 1
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (portfolio_id,))
        row = cur.fetchone()
        if not row:
            return {
                "found": False,
                "message": "No brief exists yet for this portfolio.",
            }
        columns = [d[0] for d in cur.description]
        out = serialize_row(dict(zip(columns, row)))
        out["found"] = True
        return out
    finally:
        conn.close()
