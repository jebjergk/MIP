-- 01_trade_scorecard.sql — POPULATION A (executed entries post-reset) scorecard and breakdowns.
-- Population B committee summary: use 00_scope_and_trade_set.sql (statement 2) or 03_committee_vs_outcome.sql.
-- Read-only.

use role MIP_ADMIN_ROLE;
use database MIP;

with
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts
),
live_portfolios as (
    select PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE
    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where upper(coalesce(ADAPTER_MODE, '')) = 'LIVE'
),
entry_leg_orders as (
    select
        lo.ORDER_ID, lo.ACTION_ID, lo.FILLED_AT, lo.AVG_FILL_PRICE, lo.QTY_FILLED,
        lo.IDEMPOTENCY_KEY, lo.BROKER_ORDER_ID,
        la.PORTFOLIO_ID, la.SYMBOL, la.SIDE, la.PROPOSAL_ID,
        la.STATUS as action_status, la.UPDATED_AT as action_updated_at
    from MIP.LIVE.LIVE_ORDERS lo
    inner join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = lo.ACTION_ID
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    where lo.FILLED_AT is not null and coalesce(lo.QTY_FILLED, 0) > 0
      and not regexp_like(coalesce(lo.IDEMPOTENCY_KEY, ''), ':(TP|SL|tp|sl)$')
      and upper(coalesce(lo.ACTION_INTENT, la.ACTION_INTENT,
        iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
),
first_entry_fill as (
    select * from entry_leg_orders
    qualify row_number() over (
        partition by ACTION_ID order by FILLED_AT asc nulls last, ORDER_ID asc
    ) = 1
),
executed_entries_scoped as (
    select
        la.ACTION_ID as entry_action_id,
        la.PORTFOLIO_ID, la.SYMBOL, la.SIDE, la.PROPOSAL_ID,
        la.STATUS as entry_action_status,
        la.COMMITTEE_RUN_ID,
        f.FILLED_AT as order_entry_fill_ts,
        f.AVG_FILL_PRICE as order_entry_avg_price,
        f.QTY_FILLED as order_entry_qty,
        tc.CLOSEOUT_ID, tc.EXIT_TS, tc.EXIT_TYPE,
        tc.REALIZED_PNL, tc.REALIZED_RETURN_PCT, tc.REALIZED_SIZE, tc.HOLDING_PERIOD_SEC,
        tc.ENTRY_TS as closeout_entry_ts_fallback,
        coalesce(
            f.FILLED_AT, tc.ENTRY_TS,
            iff(f.FILLED_AT is null and tc.ENTRY_TS is null
                and upper(coalesce(la.STATUS, '')) in (
                    'EXECUTED','EXECUTION_REQUESTED','INTENT_APPROVED','REVALIDATED_PASS'),
                la.UPDATED_AT, null)
        ) as canonical_entry_ts,
        case
            when f.FILLED_AT is not null then 'ORDER_FILL'
            when tc.ENTRY_TS is not null then 'CLOSEOUT_FALLBACK'
            when f.FILLED_AT is null and tc.ENTRY_TS is null
                 and upper(coalesce(la.STATUS, '')) in (
                     'EXECUTED','EXECUTION_REQUESTED','INTENT_APPROVED','REVALIDATED_PASS')
                then 'EMERGENCY_ACTION_UPDATED_AT'
            else 'UNKNOWN'
        end as entry_ts_source
    from MIP.LIVE.LIVE_ACTIONS la
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    left join first_entry_fill f on f.ACTION_ID = la.ACTION_ID
    left join MIP.LIVE.TRADE_CLOSEOUT tc on tc.ENTRY_ACTION_ID = la.ACTION_ID
    where upper(coalesce(la.ACTION_INTENT,
        iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
      and (f.ACTION_ID is not null or tc.ENTRY_ACTION_ID is not null)
),
executed_entries_post_reset as (
    select e.* from executed_entries_scoped e
    cross join params p
    where e.canonical_entry_ts is not null and e.canonical_entry_ts >= p.reset_ts
),
enriched as (
    select
        e.*,
        cv.RECOMMENDATION as committee_recommendation,
        mr.REGIME as market_regime_at_entry_day
    from executed_entries_post_reset e
    left join MIP.LIVE.COMMITTEE_VERDICT cv
        on cv.RUN_ID = e.COMMITTEE_RUN_ID
    left join MIP.MART.V_MARKET_REGIME mr
        on mr.REGIME_DATE = e.canonical_entry_ts::date
)
select
    'POP_A_OVERALL' as report_section,
    count(*) as n_entries,
    count_if(closeout_id is not null) as n_closed,
    count_if(closeout_id is null) as n_open,
    count_if(closeout_id is not null and coalesce(realized_pnl, 0) > 0) as n_wins,
    count_if(closeout_id is not null and coalesce(realized_pnl, 0) < 0) as n_losses,
    count_if(closeout_id is not null and coalesce(realized_pnl, 0) = 0) as n_flat,
    sum(iff(closeout_id is not null, realized_pnl, 0)) as gross_realized_pnl_closed_only,
    avg(iff(closeout_id is not null, realized_pnl, null)) as avg_realized_pnl_closed,
    median(iff(closeout_id is not null, realized_pnl, null)) as median_realized_pnl_closed,
    avg(iff(closeout_id is not null, realized_return_pct, null)) as avg_realized_return_pct_closed,
    avg(iff(closeout_id is not null, holding_period_sec, null)) as avg_holding_sec_closed,
    avg(iff(closeout_id is not null and coalesce(realized_pnl, 0) < 0, realized_pnl, null)) as avg_loss_size,
    avg(iff(closeout_id is not null and coalesce(realized_pnl, 0) > 0, realized_pnl, null)) as avg_win_size,
    div0(
        sum(iff(closeout_id is not null and coalesce(realized_pnl, 0) > 0, abs(realized_pnl), 0)),
        sum(iff(closeout_id is not null and coalesce(realized_pnl, 0) < 0, abs(realized_pnl), 0))
    ) as profit_factor_abs,
    count_if(upper(coalesce(exit_type, '')) like '%STOP%' or upper(coalesce(exit_type, '')) like '%SL%') as n_exit_sl_like,
    count_if(upper(coalesce(exit_type, '')) like '%TAKE%' or upper(coalesce(exit_type, '')) like '%TP%'
        or upper(coalesce(exit_type, '')) like '%PROFIT%') as n_exit_tp_like,
    count_if(closeout_id is not null
        and not (
            upper(coalesce(exit_type, '')) like '%STOP%' or upper(coalesce(exit_type, '')) like '%SL%'
            or upper(coalesce(exit_type, '')) like '%TAKE%' or upper(coalesce(exit_type, '')) like '%TP%'
            or upper(coalesce(exit_type, '')) like '%PROFIT%'
        )) as n_exit_other
from enriched;

with
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts
),
live_portfolios as (
    select PORTFOLIO_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where upper(coalesce(ADAPTER_MODE, '')) = 'LIVE'
),
entry_leg_orders as (
    select lo.ORDER_ID, lo.ACTION_ID, lo.FILLED_AT, la.PORTFOLIO_ID, la.SYMBOL, la.SIDE, la.PROPOSAL_ID, la.STATUS as action_status, la.UPDATED_AT, la.COMMITTEE_RUN_ID
    from MIP.LIVE.LIVE_ORDERS lo
    inner join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = lo.ACTION_ID
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    where lo.FILLED_AT is not null and coalesce(lo.QTY_FILLED, 0) > 0
      and not regexp_like(coalesce(lo.IDEMPOTENCY_KEY, ''), ':(TP|SL|tp|sl)$')
      and upper(coalesce(lo.ACTION_INTENT, la.ACTION_INTENT, iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
),
first_entry_fill as (
    select * from entry_leg_orders
    qualify row_number() over (partition by ACTION_ID order by FILLED_AT asc nulls last, ORDER_ID asc) = 1
),
executed_entries_scoped as (
    select
        la.ACTION_ID as entry_action_id, la.PORTFOLIO_ID, la.SYMBOL, la.PROPOSAL_ID, la.COMMITTEE_RUN_ID,
        coalesce(f.FILLED_AT, tc.ENTRY_TS, iff(f.FILLED_AT is null and tc.ENTRY_TS is null
            and upper(coalesce(la.STATUS, '')) in ('EXECUTED','EXECUTION_REQUESTED','INTENT_APPROVED','REVALIDATED_PASS'),
            la.UPDATED_AT, null)) as canonical_entry_ts,
        tc.CLOSEOUT_ID, tc.REALIZED_PNL, tc.EXIT_TYPE
    from MIP.LIVE.LIVE_ACTIONS la
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    left join first_entry_fill f on f.ACTION_ID = la.ACTION_ID
    left join MIP.LIVE.TRADE_CLOSEOUT tc on tc.ENTRY_ACTION_ID = la.ACTION_ID
    where upper(coalesce(la.ACTION_INTENT, iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
      and (f.ACTION_ID is not null or tc.ENTRY_ACTION_ID is not null)
),
executed_entries_post_reset as (
    select e.* from executed_entries_scoped e cross join params p
    where e.canonical_entry_ts is not null and e.canonical_entry_ts >= p.reset_ts
),
enriched as (
    select e.*, cv.RECOMMENDATION as committee_recommendation, mr.REGIME as market_regime_at_entry_day
    from executed_entries_post_reset e
    left join MIP.LIVE.COMMITTEE_VERDICT cv on cv.RUN_ID = e.COMMITTEE_RUN_ID
    left join MIP.MART.V_MARKET_REGIME mr on mr.REGIME_DATE = e.canonical_entry_ts::date
)
select
    'POP_A_BY_SYMBOL' as report_section,
    symbol as breakdown_key,
    count(*) as n_entries,
    count_if(closeout_id is not null) as n_closed,
    count_if(closeout_id is not null and coalesce(realized_pnl, 0) > 0) as n_wins,
    count_if(closeout_id is not null and coalesce(realized_pnl, 0) < 0) as n_losses,
    sum(iff(closeout_id is not null, realized_pnl, 0)) as gross_realized_pnl
from enriched
group by symbol
order by n_entries desc;

with
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts
),
live_portfolios as (
    select PORTFOLIO_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where upper(coalesce(ADAPTER_MODE, '')) = 'LIVE'
),
entry_leg_orders as (
    select lo.ORDER_ID, lo.ACTION_ID, lo.FILLED_AT, la.PORTFOLIO_ID, la.SYMBOL, la.PROPOSAL_ID, la.STATUS, la.UPDATED_AT, la.COMMITTEE_RUN_ID
    from MIP.LIVE.LIVE_ORDERS lo
    inner join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = lo.ACTION_ID
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    where lo.FILLED_AT is not null and coalesce(lo.QTY_FILLED, 0) > 0
      and not regexp_like(coalesce(lo.IDEMPOTENCY_KEY, ''), ':(TP|SL|tp|sl)$')
      and upper(coalesce(lo.ACTION_INTENT, la.ACTION_INTENT, iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
),
first_entry_fill as (
    select * from entry_leg_orders
    qualify row_number() over (partition by ACTION_ID order by FILLED_AT asc nulls last, ORDER_ID asc) = 1
),
executed_entries_scoped as (
    select
        la.ACTION_ID as entry_action_id,
        la.COMMITTEE_RUN_ID,
        coalesce(f.FILLED_AT, tc.ENTRY_TS, iff(f.FILLED_AT is null and tc.ENTRY_TS is null
            and upper(coalesce(la.STATUS, '')) in ('EXECUTED','EXECUTION_REQUESTED','INTENT_APPROVED','REVALIDATED_PASS'),
            la.UPDATED_AT, null)) as canonical_entry_ts,
        tc.CLOSEOUT_ID, tc.REALIZED_PNL
    from MIP.LIVE.LIVE_ACTIONS la
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    left join first_entry_fill f on f.ACTION_ID = la.ACTION_ID
    left join MIP.LIVE.TRADE_CLOSEOUT tc on tc.ENTRY_ACTION_ID = la.ACTION_ID
    where upper(coalesce(la.ACTION_INTENT, iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
      and (f.ACTION_ID is not null or tc.ENTRY_ACTION_ID is not null)
),
executed_entries_post_reset as (
    select e.* from executed_entries_scoped e cross join params p
    where e.canonical_entry_ts is not null and e.canonical_entry_ts >= p.reset_ts
),
enriched as (
    select e.*, coalesce(cv.RECOMMENDATION, 'NO_VERDICT_ROW') as committee_recommendation
    from executed_entries_post_reset e
    left join MIP.LIVE.COMMITTEE_VERDICT cv on cv.RUN_ID = e.COMMITTEE_RUN_ID
)
select
    'POP_A_BY_COMMITTEE_REC' as report_section,
    committee_recommendation as breakdown_key,
    count(*) as n_entries,
    count_if(closeout_id is not null) as n_closed,
    sum(iff(closeout_id is not null, realized_pnl, 0)) as gross_realized_pnl
from enriched
group by committee_recommendation
order by n_entries desc;

with
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts
),
live_portfolios as (
    select PORTFOLIO_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where upper(coalesce(ADAPTER_MODE, '')) = 'LIVE'
),
entry_leg_orders as (
    select lo.ORDER_ID, lo.ACTION_ID, lo.FILLED_AT, la.PORTFOLIO_ID, la.SYMBOL, la.STATUS, la.UPDATED_AT
    from MIP.LIVE.LIVE_ORDERS lo
    inner join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = lo.ACTION_ID
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    where lo.FILLED_AT is not null and coalesce(lo.QTY_FILLED, 0) > 0
      and not regexp_like(coalesce(lo.IDEMPOTENCY_KEY, ''), ':(TP|SL|tp|sl)$')
      and upper(coalesce(lo.ACTION_INTENT, la.ACTION_INTENT, iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
),
first_entry_fill as (
    select * from entry_leg_orders
    qualify row_number() over (partition by ACTION_ID order by FILLED_AT asc nulls last, ORDER_ID asc) = 1
),
executed_entries_scoped as (
    select
        la.ACTION_ID as entry_action_id,
        coalesce(f.FILLED_AT, tc.ENTRY_TS, iff(f.FILLED_AT is null and tc.ENTRY_TS is null
            and upper(coalesce(la.STATUS, '')) in ('EXECUTED','EXECUTION_REQUESTED','INTENT_APPROVED','REVALIDATED_PASS'),
            la.UPDATED_AT, null)) as canonical_entry_ts,
        tc.CLOSEOUT_ID, tc.REALIZED_PNL
    from MIP.LIVE.LIVE_ACTIONS la
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    left join first_entry_fill f on f.ACTION_ID = la.ACTION_ID
    left join MIP.LIVE.TRADE_CLOSEOUT tc on tc.ENTRY_ACTION_ID = la.ACTION_ID
    where upper(coalesce(la.ACTION_INTENT, iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
      and (f.ACTION_ID is not null or tc.ENTRY_ACTION_ID is not null)
),
executed_entries_post_reset as (
    select e.* from executed_entries_scoped e cross join params p
    where e.canonical_entry_ts is not null and e.canonical_entry_ts >= p.reset_ts
),
enriched as (
    select e.*, mr.REGIME as market_regime_at_entry_day
    from executed_entries_post_reset e
    left join MIP.MART.V_MARKET_REGIME mr on mr.REGIME_DATE = e.canonical_entry_ts::date
)
select
    'POP_A_BY_REGIME' as report_section,
    coalesce(market_regime_at_entry_day, 'UNKNOWN') as breakdown_key,
    count(*) as n_entries,
    count_if(closeout_id is not null) as n_closed,
    sum(iff(closeout_id is not null, realized_pnl, 0)) as gross_realized_pnl
from enriched
group by coalesce(market_regime_at_entry_day, 'UNKNOWN')
order by n_entries desc;

with
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts
),
live_portfolios as (
    select PORTFOLIO_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where upper(coalesce(ADAPTER_MODE, '')) = 'LIVE'
),
entry_leg_orders as (
    select lo.ORDER_ID, lo.ACTION_ID, lo.FILLED_AT, la.STATUS, la.UPDATED_AT
    from MIP.LIVE.LIVE_ORDERS lo
    inner join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = lo.ACTION_ID
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    where lo.FILLED_AT is not null and coalesce(lo.QTY_FILLED, 0) > 0
      and not regexp_like(coalesce(lo.IDEMPOTENCY_KEY, ''), ':(TP|SL|tp|sl)$')
      and upper(coalesce(lo.ACTION_INTENT, la.ACTION_INTENT, iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
),
first_entry_fill as (
    select * from entry_leg_orders
    qualify row_number() over (partition by ACTION_ID order by FILLED_AT asc nulls last, ORDER_ID asc) = 1
),
executed_entries_scoped as (
    select
        la.ACTION_ID as entry_action_id,
        coalesce(f.FILLED_AT, tc.ENTRY_TS, iff(f.FILLED_AT is null and tc.ENTRY_TS is null
            and upper(coalesce(la.STATUS, '')) in ('EXECUTED','EXECUTION_REQUESTED','INTENT_APPROVED','REVALIDATED_PASS'),
            la.UPDATED_AT, null)) as canonical_entry_ts,
        tc.CLOSEOUT_ID, tc.REALIZED_PNL
    from MIP.LIVE.LIVE_ACTIONS la
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    left join first_entry_fill f on f.ACTION_ID = la.ACTION_ID
    left join MIP.LIVE.TRADE_CLOSEOUT tc on tc.ENTRY_ACTION_ID = la.ACTION_ID
    where upper(coalesce(la.ACTION_INTENT, iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
      and (f.ACTION_ID is not null or tc.ENTRY_ACTION_ID is not null)
),
executed_entries_post_reset as (
    select e.* from executed_entries_scoped e cross join params p
    where e.canonical_entry_ts is not null and e.canonical_entry_ts >= p.reset_ts
)
select
    'POP_A_BY_ENTRY_DAY' as report_section,
    canonical_entry_ts::date as breakdown_key,
    count(*) as n_entries,
    count_if(closeout_id is not null) as n_closed,
    sum(iff(closeout_id is not null, realized_pnl, 0)) as gross_realized_pnl
from executed_entries_post_reset
group by canonical_entry_ts::date
order by breakdown_key desc;
