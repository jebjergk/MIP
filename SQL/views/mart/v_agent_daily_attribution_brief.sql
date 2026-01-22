-- v_agent_daily_attribution_brief.sql
-- Purpose: Daily top contributors/detractors and market type split

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_AGENT_DAILY_ATTRIBUTION_BRIEF as
with base as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        MARKET_TYPE,
        SYMBOL,
        TOTAL_REALIZED_PNL,
        ROUNDTRIPS,
        AVG_PNL_PER_TRADE,
        WIN_RATE,
        CONTRIBUTION_PCT
    from MIP.MART.V_PORTFOLIO_ATTRIBUTION
),
run_totals as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        sum(TOTAL_REALIZED_PNL) as TOTAL_REALIZED_PNL
    from base
    group by
        PORTFOLIO_ID,
        RUN_ID
),
market_type_rollup as (
    select
        b.PORTFOLIO_ID,
        b.RUN_ID,
        b.MARKET_TYPE,
        sum(b.TOTAL_REALIZED_PNL) as TOTAL_REALIZED_PNL,
        sum(b.ROUNDTRIPS) as ROUNDTRIPS,
        sum(b.TOTAL_REALIZED_PNL) / nullif(sum(b.ROUNDTRIPS), 0) as AVG_PNL_PER_TRADE,
        sum(b.WIN_RATE * b.ROUNDTRIPS) / nullif(sum(b.ROUNDTRIPS), 0) as WIN_RATE,
        sum(b.TOTAL_REALIZED_PNL) / nullif(t.TOTAL_REALIZED_PNL, 0) as CONTRIBUTION_PCT
    from base b
    left join run_totals t
      on t.PORTFOLIO_ID = b.PORTFOLIO_ID
     and t.RUN_ID = b.RUN_ID
    group by
        b.PORTFOLIO_ID,
        b.RUN_ID,
        b.MARKET_TYPE,
        t.TOTAL_REALIZED_PNL
),
contributors as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        MARKET_TYPE,
        array_agg(
            object_construct(
                'symbol', SYMBOL,
                'market_type', MARKET_TYPE,
                'total_realized_pnl', TOTAL_REALIZED_PNL,
                'roundtrips', ROUNDTRIPS,
                'avg_pnl_per_trade', AVG_PNL_PER_TRADE,
                'win_rate', WIN_RATE,
                'contribution_pct', CONTRIBUTION_PCT
            )
        ) within group (order by TOTAL_REALIZED_PNL desc) as TOP_CONTRIBUTORS
    from (
        select
            b.*,
            row_number() over (
                partition by b.PORTFOLIO_ID, b.RUN_ID, b.MARKET_TYPE
                order by b.TOTAL_REALIZED_PNL desc
            ) as RN
        from base b
    ) ranked
    where RN <= 5
    group by
        PORTFOLIO_ID,
        RUN_ID,
        MARKET_TYPE
),
detractors as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        MARKET_TYPE,
        array_agg(
            object_construct(
                'symbol', SYMBOL,
                'market_type', MARKET_TYPE,
                'total_realized_pnl', TOTAL_REALIZED_PNL,
                'roundtrips', ROUNDTRIPS,
                'avg_pnl_per_trade', AVG_PNL_PER_TRADE,
                'win_rate', WIN_RATE,
                'contribution_pct', CONTRIBUTION_PCT
            )
        ) within group (order by TOTAL_REALIZED_PNL asc) as TOP_DETRACTORS
    from base
    qualify row_number() over (
        partition by PORTFOLIO_ID, RUN_ID, MARKET_TYPE
        order by TOTAL_REALIZED_PNL asc
    ) <= 5
    group by
        PORTFOLIO_ID,
        RUN_ID,
        MARKET_TYPE
)
select
    r.PORTFOLIO_ID,
    r.RUN_ID,
    r.MARKET_TYPE,
    r.TOTAL_REALIZED_PNL,
    r.ROUNDTRIPS,
    r.AVG_PNL_PER_TRADE,
    r.WIN_RATE,
    r.CONTRIBUTION_PCT,
    c.TOP_CONTRIBUTORS,
    d.TOP_DETRACTORS,
    current_timestamp() as AS_OF_TS
from market_type_rollup r
left join contributors c
  on c.PORTFOLIO_ID = r.PORTFOLIO_ID
 and c.RUN_ID = r.RUN_ID
 and c.MARKET_TYPE = r.MARKET_TYPE
left join detractors d
  on d.PORTFOLIO_ID = r.PORTFOLIO_ID
 and d.RUN_ID = r.RUN_ID
 and d.MARKET_TYPE = r.MARKET_TYPE;
