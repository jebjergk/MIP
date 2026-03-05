-- v_portfolio_reality_check.sql
-- Purpose: Non-destructive reconciliation view so UI/API can detect drift.
-- Shows ledger-implied cash/equity vs latest daily snapshot vs canonical open positions.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PORTFOLIO_REALITY_CHECK as
with active_episode as (
    select
        e.PORTFOLIO_ID,
        e.EPISODE_ID
    from MIP.APP.PORTFOLIO_EPISODE e
    where e.STATUS = 'ACTIVE'
),
latest_daily as (
    select
        d.PORTFOLIO_ID,
        d.RUN_ID,
        d.EPISODE_ID,
        d.TS,
        d.CASH,
        d.EQUITY_VALUE,
        d.TOTAL_EQUITY,
        d.OPEN_POSITIONS,
        d.CREATED_AT
    from MIP.APP.PORTFOLIO_DAILY d
    qualify row_number() over (
        partition by d.PORTFOLIO_ID
        order by d.TS desc, d.CREATED_AT desc nulls last
    ) = 1
),
latest_price as (
    select
        b.SYMBOL,
        b.MARKET_TYPE,
        b.CLOSE
    from MIP.MART.MARKET_BARS b
    qualify row_number() over (
        partition by b.SYMBOL, b.MARKET_TYPE
        order by b.TS desc
    ) = 1
),
canonical_open as (
    select
        p.PORTFOLIO_ID,
        count(*) as CANONICAL_OPEN_POSITIONS,
        sum(coalesce(lp.CLOSE, 0) * coalesce(p.QUANTITY, 0)) as CANONICAL_OPEN_MV
    from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL p
    left join latest_price lp
      on lp.SYMBOL = p.SYMBOL
     and lp.MARKET_TYPE = p.MARKET_TYPE
    group by p.PORTFOLIO_ID
),
episode_trades as (
    select
        t.PORTFOLIO_ID,
        sum(
            case
                when t.SIDE = 'BUY'
                    then -(t.NOTIONAL + greatest(abs(t.NOTIONAL) * 1 / 10000, 0.01))
                when t.SIDE = 'SELL'
                    then +(t.NOTIONAL - greatest(abs(t.NOTIONAL) * 1 / 10000, 0.01))
                else 0
            end
        ) as LEDGER_NET_CASH_DELTA
    from MIP.APP.PORTFOLIO_TRADES t
    join active_episode e
      on e.PORTFOLIO_ID = t.PORTFOLIO_ID
     and e.EPISODE_ID = t.EPISODE_ID
    group by t.PORTFOLIO_ID
)
select
    p.PORTFOLIO_ID,
    p.STATUS,
    p.STARTING_CASH,
    p.FINAL_EQUITY as HEADER_FINAL_EQUITY,
    p.LAST_SIMULATION_RUN_ID as HEADER_LAST_RUN_ID,
    ld.RUN_ID as DAILY_RUN_ID,
    ld.EPISODE_ID as DAILY_EPISODE_ID,
    ld.TS as DAILY_TS,
    ld.CASH as DAILY_CASH,
    ld.EQUITY_VALUE as DAILY_EQUITY_VALUE,
    ld.TOTAL_EQUITY as DAILY_TOTAL_EQUITY,
    ld.OPEN_POSITIONS as DAILY_OPEN_POSITIONS,
    coalesce(co.CANONICAL_OPEN_POSITIONS, 0) as CANONICAL_OPEN_POSITIONS,
    coalesce(co.CANONICAL_OPEN_MV, 0) as CANONICAL_OPEN_MV,
    p.STARTING_CASH + coalesce(et.LEDGER_NET_CASH_DELTA, 0) as LEDGER_IMPLIED_CASH,
    (p.STARTING_CASH + coalesce(et.LEDGER_NET_CASH_DELTA, 0)) + coalesce(co.CANONICAL_OPEN_MV, 0) as LEDGER_PLUS_CANONICAL_TOTAL,
    round(coalesce(ld.CASH, 0) - (p.STARTING_CASH + coalesce(et.LEDGER_NET_CASH_DELTA, 0)), 6) as DIFF_DAILY_CASH_VS_LEDGER,
    round(coalesce(ld.TOTAL_EQUITY, 0) - ((p.STARTING_CASH + coalesce(et.LEDGER_NET_CASH_DELTA, 0)) + coalesce(co.CANONICAL_OPEN_MV, 0)), 6) as DIFF_DAILY_TOTAL_VS_LEDGER_CANONICAL,
    round(coalesce(p.FINAL_EQUITY, 0) - coalesce(ld.TOTAL_EQUITY, 0), 6) as DIFF_HEADER_VS_DAILY_TOTAL,
    case when coalesce(ld.OPEN_POSITIONS, 0) <> coalesce(co.CANONICAL_OPEN_POSITIONS, 0) then true else false end as FLAG_OPEN_POSITION_MISMATCH,
    case when abs(coalesce(ld.CASH, 0) - (p.STARTING_CASH + coalesce(et.LEDGER_NET_CASH_DELTA, 0))) > 0.01 then true else false end as FLAG_CASH_MISMATCH,
    case when abs(coalesce(ld.TOTAL_EQUITY, 0) - ((p.STARTING_CASH + coalesce(et.LEDGER_NET_CASH_DELTA, 0)) + coalesce(co.CANONICAL_OPEN_MV, 0))) > 0.01 then true else false end as FLAG_TOTAL_EQUITY_MISMATCH
from MIP.APP.PORTFOLIO p
left join latest_daily ld
  on ld.PORTFOLIO_ID = p.PORTFOLIO_ID
left join canonical_open co
  on co.PORTFOLIO_ID = p.PORTFOLIO_ID
left join episode_trades et
  on et.PORTFOLIO_ID = p.PORTFOLIO_ID;
