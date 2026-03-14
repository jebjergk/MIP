-- v_parallel_world_actual.sql
-- Purpose: Derive ACTUAL-world baseline from research artifacts only.
-- No dependency on simulated execution tables (PORTFOLIO_DAILY/TRADES/POSITIONS).

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
with portfolio_cfg as (
    select
        p.PORTFOLIO_ID,
        coalesce(p.STARTING_CASH, 100000)::number(18,4) as STARTING_CASH,
        coalesce(pp.MAX_POSITIONS, 5) as MAX_POSITIONS,
        coalesce(pp.MAX_POSITION_PCT, 0.05)::number(18,6) as MAX_POSITION_PCT
    from MIP.APP.PORTFOLIO p
    left join MIP.APP.PORTFOLIO_PROFILE pp
      on pp.PROFILE_ID = p.PROFILE_ID
    where p.STATUS = 'ACTIVE'
),
proposal_day as (
    with live_action_latest as (
        select
            la.PROPOSAL_ID,
            upper(coalesce(la.STATUS, '')) as ACTION_STATUS
        from MIP.LIVE.LIVE_ACTIONS la
        qualify row_number() over (
            partition by la.PROPOSAL_ID
            order by la.UPDATED_AT desc nulls last, la.CREATED_AT desc nulls last
        ) = 1
    ),
    live_cfg_active as (
        select
            c.PORTFOLIO_ID
        from MIP.LIVE.LIVE_PORTFOLIO_CONFIG c
        where coalesce(c.IS_ACTIVE, false) = true
    )
    select
        op.PORTFOLIO_ID,
        op.RECOMMENDATION_ID,
        upper(coalesce(op.SIDE, 'BUY')) as SIDE,
        coalesce(op.TARGET_WEIGHT, 0.05)::number(18,6) as TARGET_WEIGHT,
        coalesce(try_to_double(op.SOURCE_SIGNALS:score::string), 0)::number(18,8) as EST_RETURN,
        coalesce(op.EXECUTED_AT, op.APPROVED_AT, op.PROPOSED_AT)::timestamp_ntz as EVENT_TS
    from MIP.AGENT_OUT.ORDER_PROPOSALS op
    left join live_cfg_active lc
      on lc.PORTFOLIO_ID = op.PORTFOLIO_ID
    left join live_action_latest la
      on la.PROPOSAL_ID = op.PROPOSAL_ID
    where op.PORTFOLIO_ID is not null
      and coalesce(op.EXECUTED_AT, op.APPROVED_AT, op.PROPOSED_AT) is not null
      -- "ACTUAL" world should represent executed decisions only.
      and op.STATUS = 'EXECUTED'
      -- For active live portfolios, only include proposals that actually completed
      -- through the live execution path. For non-live portfolios, retain legacy behavior.
      and (
            lc.PORTFOLIO_ID is null
            or coalesce(la.ACTION_STATUS, '') in ('EXECUTION_COMPLETED', 'EXECUTED', 'FILLED', 'PARTIAL_FILL', 'PARTIALLYFILLED')
          )
),
outcome_best as (
    select
        ro.RECOMMENDATION_ID,
        ro.REALIZED_RETURN
    from MIP.APP.RECOMMENDATION_OUTCOMES ro
    where ro.REALIZED_RETURN is not null
    qualify row_number() over (
        partition by ro.RECOMMENDATION_ID
        order by
            case when ro.EVAL_STATUS = 'SUCCESS' then 0 else 1 end,
            ro.HORIZON_BARS asc
    ) = 1
),
daily as (
    select
        pd.PORTFOLIO_ID,
        date_trunc('day', pd.EVENT_TS)::timestamp_ntz as AS_OF_TS,
        count(*) as TRADES_ACTUAL,
        count_if(pd.SIDE = 'BUY') as BUY_COUNT,
        count_if(pd.SIDE = 'SELL') as SELL_COUNT,
        sum(
            cfg.STARTING_CASH
            * least(cfg.MAX_POSITION_PCT, greatest(pd.TARGET_WEIGHT, 0))
            * coalesce(ob.REALIZED_RETURN, pd.EST_RETURN, 0)
        )::number(18,4) as REALIZED_PNL,
        sum(
            iff(pd.SIDE = 'BUY',
                cfg.STARTING_CASH * least(cfg.MAX_POSITION_PCT, greatest(pd.TARGET_WEIGHT, 0)),
                0
            )
        )::number(18,4) as BUY_NOTIONAL
    from proposal_day pd
    join portfolio_cfg cfg
      on cfg.PORTFOLIO_ID = pd.PORTFOLIO_ID
    left join outcome_best ob
      on ob.RECOMMENDATION_ID = pd.RECOMMENDATION_ID
    group by pd.PORTFOLIO_ID, date_trunc('day', pd.EVENT_TS)
),
finalized as (
    select
        d.PORTFOLIO_ID,
        d.AS_OF_TS,
        cast(null as number) as EPISODE_ID,
        cfg.STARTING_CASH,
        d.TRADES_ACTUAL,
        d.BUY_COUNT,
        d.SELL_COUNT,
        d.REALIZED_PNL,
        (cfg.STARTING_CASH + d.REALIZED_PNL)::number(18,4) as TOTAL_EQUITY,
        greatest(cfg.STARTING_CASH - d.BUY_NOTIONAL, 0)::number(18,4) as CASH,
        ((cfg.STARTING_CASH + d.REALIZED_PNL) - greatest(cfg.STARTING_CASH - d.BUY_NOTIONAL, 0))::number(18,4) as EQUITY_VALUE,
        greatest(d.BUY_COUNT - d.SELL_COUNT, 0) as OPEN_POSITIONS,
        d.REALIZED_PNL::number(18,4) as DAILY_PNL,
        iff(cfg.STARTING_CASH > 0, d.REALIZED_PNL / cfg.STARTING_CASH, 0)::number(18,8) as DAILY_RETURN,
        max((cfg.STARTING_CASH + d.REALIZED_PNL)) over (
            partition by d.PORTFOLIO_ID
            order by d.AS_OF_TS
            rows between unbounded preceding and current row
        )::number(18,4) as PEAK_EQUITY,
        cfg.MAX_POSITIONS,
        cfg.MAX_POSITION_PCT
    from daily d
    join portfolio_cfg cfg
      on cfg.PORTFOLIO_ID = d.PORTFOLIO_ID
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
from finalized;
