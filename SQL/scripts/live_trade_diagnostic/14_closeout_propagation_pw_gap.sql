-- 14_closeout_propagation_pw_gap.sql
-- Phase 3 — H3: Feedback loop / lifecycle propagation vs broker-truth closes (8 FIFO losers).
-- Does NOT use LIVE_ORDERS.FILLED_AT as proof of trade existence; reports MIP persistence state only.
-- Read-only.

use role MIP_ADMIN_ROLE;
use database MIP;

with closed_fifo as (
    select * from values
        ('SBUX', '29bef288-cf20-424b-8aec-f112e9add739', 1,
         '2026-04-07T18:51:18'::timestamp_ntz),
        ('APA', 'f14f4e4f-1d3f-40a1-9e6b-99eda96f0c0f', 1,
         '2026-04-08T13:30:50'::timestamp_ntz),
        ('COP', 'd4a18543-9785-4d24-ae6c-3ed62f194776', 1,
         '2026-04-08T13:32:10'::timestamp_ntz),
        ('XOM', '82590777-f2b9-4060-bfbd-cb0a7c022d43', 1,
         '2026-04-08T13:30:58'::timestamp_ntz),
        ('WMT', '39a042a1-414a-442f-91cf-6f843a031ebe', 1,
         '2026-04-10T14:32:05'::timestamp_ntz),
        ('JNJ', '858c0adc-6443-48cd-a267-14a75d35d4e1', 1,
         '2026-04-13T13:32:16'::timestamp_ntz),
        ('NEE', '8cd64d92-11ad-4db1-a3bb-36c89eff3e09', 1,
         '2026-04-13T15:21:47'::timestamp_ntz),
        ('PG', '34d2756e-4ec1-44ae-9c05-71a3cd4f4ad7', 1,
         '2026-04-13T15:30:14'::timestamp_ntz)
    as t(symbol, entry_action_id, portfolio_id, exit_ts)
),
lo as (
    select
        ACTION_ID,
        count_if(coalesce(QTY_FILLED, 0) > 0) as n_orders_qty_positive,
        count_if(upper(coalesce(STATUS, '')) in ('FILLED', 'PARTIAL_FILL')) as n_status_filled_like,
        count(*) as n_orders
    from MIP.LIVE.LIVE_ORDERS
    where ACTION_ID in (select entry_action_id from closed_fifo)
    group by ACTION_ID
)
select
    c.symbol,
    c.entry_action_id,
    c.portfolio_id,
    c.exit_ts::date as exit_date,
    coalesce(lo.n_orders_qty_positive, 0) > 0 as in_live_orders_qty_positive,
    coalesce(lo.n_status_filled_like, 0) > 0 as in_live_orders_status_filled_like,
    tc.CLOSEOUT_ID is not null as in_trade_closeout,
    eial.LINK_ID is not null as in_entry_intel_action_link,
    tir.CLOSEOUT_ID is not null as in_trade_intelligence,
    exists (
        select 1
        from MIP.MART.V_PARALLEL_WORLD_DIFF d
        where d.PORTFOLIO_ID = c.portfolio_id
          and d.AS_OF_TS::date = c.exit_ts::date
    ) as pw_diff_rows_exist_exit_day,
    case
        when tc.CLOSEOUT_ID is null then 'NO_TRADE_CLOSEOUT'
        when tir.CLOSEOUT_ID is null then 'NO_TIR_ROW'
        when eial.LINK_ID is null then 'NO_ENTRY_INTEL_LINK'
        when not coalesce(lo.n_status_filled_like, 0) > 0 and not coalesce(lo.n_orders_qty_positive, 0) > 0
            then 'LIVE_ORDERS_NOT_MARKED_FILLED'
        else 'MIP_LIFECYCLE_MARKS_FILLED_AND_CLOSEOUT'
    end as first_missing_stage_note,
    tc.CLOSEOUT_ID,
    tir.ENTRY_ACTION_ID as tir_entry_action
from closed_fifo c
left join lo on lo.ACTION_ID = c.entry_action_id
left join MIP.LIVE.TRADE_CLOSEOUT tc on tc.ENTRY_ACTION_ID = c.entry_action_id
left join MIP.LIVE.ENTRY_INTEL_ACTION_LINK eial on eial.ENTRY_ACTION_ID = c.entry_action_id
left join MIP.MART.V_TRADE_INTELLIGENCE tir on tir.ENTRY_ACTION_ID = c.entry_action_id
order by c.exit_ts;

-- Summary counts by first missing stage (same cohort)
with closed_fifo as (
    select * from values
        ('SBUX', '29bef288-cf20-424b-8aec-f112e9add739', 1, '2026-04-07T18:51:18'::timestamp_ntz),
        ('APA', 'f14f4e4f-1d3f-40a1-9e6b-99eda96f0c0f', 1, '2026-04-08T13:30:50'::timestamp_ntz),
        ('COP', 'd4a18543-9785-4d24-ae6c-3ed62f194776', 1, '2026-04-08T13:32:10'::timestamp_ntz),
        ('XOM', '82590777-f2b9-4060-bfbd-cb0a7c022d43', 1, '2026-04-08T13:30:58'::timestamp_ntz),
        ('WMT', '39a042a1-414a-442f-91cf-6f843a031ebe', 1, '2026-04-10T14:32:05'::timestamp_ntz),
        ('JNJ', '858c0adc-6443-48cd-a267-14a75d35d4e1', 1, '2026-04-13T13:32:16'::timestamp_ntz),
        ('NEE', '8cd64d92-11ad-4db1-a3bb-36c89eff3e09', 1, '2026-04-13T15:21:47'::timestamp_ntz),
        ('PG', '34d2756e-4ec1-44ae-9c05-71a3cd4f4ad7', 1, '2026-04-13T15:30:14'::timestamp_ntz)
    as t(symbol, entry_action_id, portfolio_id, exit_ts)
),
st as (
    select
        c.entry_action_id,
        case
            when tc.CLOSEOUT_ID is null then 'NO_TRADE_CLOSEOUT'
            when tir.CLOSEOUT_ID is null then 'NO_TIR_ROW'
            else 'HAS_TIR'
        end as stage
    from closed_fifo c
    left join MIP.LIVE.TRADE_CLOSEOUT tc on tc.ENTRY_ACTION_ID = c.entry_action_id
    left join MIP.MART.V_TRADE_INTELLIGENCE tir on tir.ENTRY_ACTION_ID = c.entry_action_id
)
select 'PROPAGATION_SUMMARY' as section, stage, count(*) as n_trades
from st
group by stage
order by n_trades desc;
