-- v_parallel_world_actual.sql
-- Purpose: Derive ACTUAL-world baseline from live broker snapshots/actions only.
-- For live mode, this view intentionally avoids APP portfolio simulation tables.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PARALLEL_WORLD_ACTUAL (
    PORTFOLIO_ID,
    AS_OF_TS,
    EPISODE_ID,
    STARTING_CASH,
    TRADES_ACTUAL,
    BUY_COUNT,
    SELL_COUNT,
    REALIZED_PNL,
    TOTAL_EQUITY,
    CASH,
    EQUITY_VALUE,
    OPEN_POSITIONS,
    DAILY_PNL,
    DAILY_RETURN,
    PEAK_EQUITY,
    DRAWDOWN,
    MAX_POSITIONS,
    MAX_POSITION_PCT
) as
with live_cfg as (
    select
        c.PORTFOLIO_ID,
        coalesce(c.MAX_POSITIONS, 5) as MAX_POSITIONS,
        coalesce(c.MAX_POSITION_PCT, 0.05)::number(18,6) as MAX_POSITION_PCT
    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG c
    where coalesce(c.IS_ACTIVE, false) = true
),
nav_latest_per_day as (
    select
        bs.PORTFOLIO_ID,
        date_trunc('day', bs.SNAPSHOT_TS)::timestamp_ntz as AS_OF_TS,
        bs.SNAPSHOT_TS,
        bs.NET_LIQUIDATION_EUR,
        bs.TOTAL_CASH_EUR,
        bs.GROSS_POSITION_VALUE_EUR,
        row_number() over (
            partition by bs.PORTFOLIO_ID, date_trunc('day', bs.SNAPSHOT_TS)
            order by bs.SNAPSHOT_TS desc, bs.CREATED_AT desc nulls last
        ) as RN
    from MIP.LIVE.BROKER_SNAPSHOTS bs
    join live_cfg cfg
      on cfg.PORTFOLIO_ID = bs.PORTFOLIO_ID
    where upper(coalesce(bs.SNAPSHOT_TYPE, '')) = 'NAV'
      and bs.NET_LIQUIDATION_EUR is not null
),
nav_daily as (
    select
        PORTFOLIO_ID,
        AS_OF_TS,
        SNAPSHOT_TS,
        NET_LIQUIDATION_EUR::number(18,4) as TOTAL_EQUITY,
        coalesce(
            TOTAL_CASH_EUR,
            NET_LIQUIDATION_EUR - coalesce(GROSS_POSITION_VALUE_EUR, 0)
        )::number(18,4) as CASH,
        coalesce(
            GROSS_POSITION_VALUE_EUR,
            greatest(
                NET_LIQUIDATION_EUR - coalesce(TOTAL_CASH_EUR, NET_LIQUIDATION_EUR),
                0
            )
        )::number(18,4) as EQUITY_VALUE
    from nav_latest_per_day
    where RN = 1
),
action_daily as (
    select
        la.PORTFOLIO_ID,
        date_trunc('day', coalesce(la.UPDATED_AT, la.CREATED_AT))::timestamp_ntz as AS_OF_TS,
        count(*) as TRADES_ACTUAL,
        count_if(upper(coalesce(la.SIDE, '')) = 'BUY') as BUY_COUNT,
        count_if(upper(coalesce(la.SIDE, '')) = 'SELL') as SELL_COUNT
    from MIP.LIVE.LIVE_ACTIONS la
    join live_cfg cfg
      on cfg.PORTFOLIO_ID = la.PORTFOLIO_ID
    where upper(coalesce(la.STATUS, '')) in (
        'EXECUTION_COMPLETED',
        'EXECUTED',
        'FILLED',
        'PARTIAL_FILL',
        'PARTIALLYFILLED'
    )
    group by la.PORTFOLIO_ID, date_trunc('day', coalesce(la.UPDATED_AT, la.CREATED_AT))
),
position_daily as (
    with latest_pos as (
        select
            bs.PORTFOLIO_ID,
            date_trunc('day', bs.SNAPSHOT_TS)::timestamp_ntz as AS_OF_TS,
            bs.SNAPSHOT_TS,
            bs.SYMBOL,
            bs.POSITION_QTY,
            row_number() over (
                partition by bs.PORTFOLIO_ID, date_trunc('day', bs.SNAPSHOT_TS), bs.SYMBOL
                order by bs.SNAPSHOT_TS desc, bs.CREATED_AT desc nulls last
            ) as RN
        from MIP.LIVE.BROKER_SNAPSHOTS bs
        join live_cfg cfg
          on cfg.PORTFOLIO_ID = bs.PORTFOLIO_ID
        where upper(coalesce(bs.SNAPSHOT_TYPE, '')) = 'POSITION'
    )
    select
        PORTFOLIO_ID,
        AS_OF_TS,
        count_if(coalesce(POSITION_QTY, 0) != 0) as OPEN_POSITIONS
    from latest_pos
    where RN = 1
    group by PORTFOLIO_ID, AS_OF_TS
),
joined as (
    select
        n.PORTFOLIO_ID,
        n.AS_OF_TS,
        cast(null as number) as EPISODE_ID,
        first_value(n.TOTAL_EQUITY) over (
            partition by n.PORTFOLIO_ID
            order by n.AS_OF_TS
            rows between unbounded preceding and unbounded following
        )::number(18,4) as STARTING_CASH,
        coalesce(a.TRADES_ACTUAL, 0) as TRADES_ACTUAL,
        coalesce(a.BUY_COUNT, 0) as BUY_COUNT,
        coalesce(a.SELL_COUNT, 0) as SELL_COUNT,
        (n.TOTAL_EQUITY - coalesce(lag(n.TOTAL_EQUITY) over (
            partition by n.PORTFOLIO_ID
            order by n.AS_OF_TS
        ), n.TOTAL_EQUITY))::number(18,4) as REALIZED_PNL,
        n.TOTAL_EQUITY,
        n.CASH,
        n.EQUITY_VALUE,
        coalesce(p.OPEN_POSITIONS, 0) as OPEN_POSITIONS,
        (n.TOTAL_EQUITY - coalesce(lag(n.TOTAL_EQUITY) over (
            partition by n.PORTFOLIO_ID
            order by n.AS_OF_TS
        ), n.TOTAL_EQUITY))::number(18,4) as DAILY_PNL,
        iff(
            coalesce(lag(n.TOTAL_EQUITY) over (partition by n.PORTFOLIO_ID order by n.AS_OF_TS), 0) > 0,
            (n.TOTAL_EQUITY - lag(n.TOTAL_EQUITY) over (partition by n.PORTFOLIO_ID order by n.AS_OF_TS))
            / lag(n.TOTAL_EQUITY) over (partition by n.PORTFOLIO_ID order by n.AS_OF_TS),
            0
        )::number(18,8) as DAILY_RETURN,
        max(n.TOTAL_EQUITY) over (
            partition by n.PORTFOLIO_ID
            order by n.AS_OF_TS
            rows between unbounded preceding and current row
        )::number(18,4) as PEAK_EQUITY,
        cfg.MAX_POSITIONS,
        cfg.MAX_POSITION_PCT
    from nav_daily n
    join live_cfg cfg
      on cfg.PORTFOLIO_ID = n.PORTFOLIO_ID
    left join action_daily a
      on a.PORTFOLIO_ID = n.PORTFOLIO_ID
     and a.AS_OF_TS = n.AS_OF_TS
    left join position_daily p
      on p.PORTFOLIO_ID = n.PORTFOLIO_ID
     and p.AS_OF_TS = n.AS_OF_TS
)
select
    PORTFOLIO_ID,
    AS_OF_TS,
    EPISODE_ID,
    STARTING_CASH,
    TRADES_ACTUAL,
    BUY_COUNT,
    SELL_COUNT,
    REALIZED_PNL,
    TOTAL_EQUITY,
    CASH,
    EQUITY_VALUE,
    OPEN_POSITIONS,
    DAILY_PNL,
    DAILY_RETURN,
    PEAK_EQUITY,
    iff(PEAK_EQUITY > 0, (TOTAL_EQUITY - PEAK_EQUITY) / PEAK_EQUITY, 0)::number(18,8) as DRAWDOWN,
    MAX_POSITIONS,
    MAX_POSITION_PCT
from joined;
