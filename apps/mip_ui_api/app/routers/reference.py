"""
Reference metadata endpoints used by the UI.
"""
from fastapi import APIRouter

from app.db import get_connection, fetch_all, serialize_rows

router = APIRouter(prefix="/reference", tags=["reference"])


@router.get("/symbols")
def get_symbol_reference():
    """
    Returns symbol metadata for the enabled daily universe.
    display_name is sourced from active aliases when available.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            with universe as (
                select distinct
                    upper(SYMBOL) as SYMBOL,
                    upper(MARKET_TYPE) as MARKET_TYPE
                from MIP.APP.INGEST_UNIVERSE
                where coalesce(IS_ENABLED, true)
                  and INTERVAL_MINUTES = 1440
            ),
            alias_rollup as (
                select
                    upper(SYMBOL) as SYMBOL,
                    upper(MARKET_TYPE) as MARKET_TYPE,
                    max(case when upper(ALIAS_TYPE) = 'COMPANY_NAME' then ALIAS end) as COMPANY_NAME,
                    max(case when upper(ALIAS_TYPE) = 'ETF_NAME' then ALIAS end) as ETF_NAME,
                    max(case when upper(ALIAS_TYPE) = 'FX_PAIR' then ALIAS end) as FX_PAIR
                from MIP.NEWS.SYMBOL_ALIAS_DICT
                where coalesce(IS_ACTIVE, true)
                group by 1, 2
            )
            select
                u.SYMBOL,
                u.MARKET_TYPE,
                case
                    when u.MARKET_TYPE = 'STOCK' then coalesce(a.COMPANY_NAME, u.SYMBOL)
                    when u.MARKET_TYPE = 'ETF' then coalesce(a.ETF_NAME, a.COMPANY_NAME, u.SYMBOL)
                    when u.MARKET_TYPE = 'FX' then coalesce(a.FX_PAIR, u.SYMBOL)
                    else coalesce(a.COMPANY_NAME, a.ETF_NAME, a.FX_PAIR, u.SYMBOL)
                end as DISPLAY_NAME
            from universe u
            left join alias_rollup a
              on a.SYMBOL = u.SYMBOL
             and a.MARKET_TYPE = u.MARKET_TYPE
            order by u.MARKET_TYPE, u.SYMBOL
            """
        )
        rows = serialize_rows(fetch_all(cur))
        symbols = [
            {
                "symbol": (r.get("SYMBOL") or r.get("symbol")),
                "market_type": (r.get("MARKET_TYPE") or r.get("market_type")),
                "display_name": (r.get("DISPLAY_NAME") or r.get("display_name")),
            }
            for r in rows
        ]
        return {"symbols": symbols}
    finally:
        conn.close()
