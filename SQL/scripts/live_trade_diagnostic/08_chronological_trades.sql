-- 08_chronological_trades.sql — POPULATION A: simple chronological trade list + ENTRY_LAG_CLASS.
-- NEXT_SESSION uses calendar +1 day from signal anchor (proxy for session; see architecture doc).

use role MIP_ADMIN_ROLE;
use database MIP;

with
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts
),
live_portfolios as (
    select PORTFOLIO_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where upper(coalesce(ADAPTER_MODE, '')) = 'LIVE'
),
entry_leg_orders as (
    select lo.ORDER_ID, lo.ACTION_ID, lo.FILLED_AT, lo.AVG_FILL_PRICE, lo.IDEMPOTENCY_KEY,
        la.PORTFOLIO_ID, la.SYMBOL, la.SIDE, la.PROPOSAL_ID, la.STATUS, la.UPDATED_AT
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
        la.PORTFOLIO_ID,
        la.SYMBOL,
        la.SIDE,
        la.PROPOSAL_ID,
        f.AVG_FILL_PRICE as order_entry_avg_price,
        coalesce(f.FILLED_AT, tc.ENTRY_TS, iff(f.FILLED_AT is null and tc.ENTRY_TS is null
            and upper(coalesce(la.STATUS, '')) in ('EXECUTED','EXECUTION_REQUESTED','INTENT_APPROVED','REVALIDATED_PASS'),
            la.UPDATED_AT, null)) as canonical_entry_ts,
        tc.EXIT_TS,
        tc.EXIT_TYPE,
        tc.REALIZED_PNL,
        tc.CLOSEOUT_ID
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
lag_calc as (
    select
        e.*,
        coalesce(op.SIGNAL_TS, op.PROPOSED_AT) as signal_anchor_ts,
        case
            when coalesce(op.SIGNAL_TS, op.PROPOSED_AT) is null then 'UNKNOWN'
            when e.canonical_entry_ts::date = coalesce(op.SIGNAL_TS, op.PROPOSED_AT)::date then 'SAME_SIGNAL_DAY'
            when datediff('day', coalesce(op.SIGNAL_TS, op.PROPOSED_AT)::date, e.canonical_entry_ts::date) = 1 then 'NEXT_SESSION'
            when datediff('day', coalesce(op.SIGNAL_TS, op.PROPOSED_AT)::date, e.canonical_entry_ts::date) > 1 then 'LATER'
            when e.canonical_entry_ts < coalesce(op.SIGNAL_TS, op.PROPOSED_AT) then 'UNKNOWN'
            else 'UNKNOWN'
        end as entry_lag_class
    from executed_entries_post_reset e
    left join MIP.AGENT_OUT.ORDER_PROPOSALS op on op.PROPOSAL_ID = e.PROPOSAL_ID
)
select
    entry_action_id,
    portfolio_id,
    symbol,
    side,
    proposal_id,
    canonical_entry_ts,
    order_entry_avg_price,
    signal_anchor_ts,
    entry_lag_class,
    exit_ts,
    exit_type,
    realized_pnl,
    closeout_id
from lag_calc
order by canonical_entry_ts asc, entry_action_id asc;
