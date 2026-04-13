-- 04_entry_quality.sql — POPULATION A. Entry timing vs short-term path.
-- LONG-focused: distance from session high before entry (approx: max HIGH on entry date with TS <= entry).
-- Path interval: 5m → 60m fallback → daily (PATH_INTERVAL_USED).

use role MIP_ADMIN_ROLE;
use database MIP;

with
params as (
    select
        to_timestamp_ntz('2026-04-07') as reset_ts,
        60 as diag_path_intraday_fallback_minutes
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
        la.SYMBOL, la.SIDE, la.PROPOSAL_ID,
        f.AVG_FILL_PRICE as order_entry_avg_price,
        coalesce(f.FILLED_AT, tc.ENTRY_TS, iff(f.FILLED_AT is null and tc.ENTRY_TS is null
            and upper(coalesce(la.STATUS, '')) in ('EXECUTED','EXECUTION_REQUESTED','INTENT_APPROVED','REVALIDATED_PASS'),
            la.UPDATED_AT, null)) as canonical_entry_ts,
        tc.EXIT_TS as exit_ts
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
path_base as (
    select
        e.*,
        coalesce(op.MARKET_TYPE, 'STOCK') as eff_market_type,
        coalesce(e.exit_ts, current_timestamp()) as path_end_ts
    from executed_entries_post_reset e
    left join MIP.AGENT_OUT.ORDER_PROPOSALS op on op.PROPOSAL_ID = e.PROPOSAL_ID
),
bar_hits as (
    select
        pb.entry_action_id,
        b.INTERVAL_MINUTES,
        count(*) as bar_cnt
    from path_base pb
    inner join MIP.MART.MARKET_BARS b
        on upper(trim(b.SYMBOL)) = upper(trim(pb.SYMBOL))
       and b.MARKET_TYPE = pb.eff_market_type
       and b.TS >= pb.canonical_entry_ts
       and b.TS <= pb.path_end_ts
       and b.INTERVAL_MINUTES in (5, 60, 1440)
    group by pb.entry_action_id, b.INTERVAL_MINUTES
),
interval_pick as (
    select
        pb.entry_action_id,
        case
            when coalesce(b5.bar_cnt, 0) > 0 then 5
            when coalesce(b60.bar_cnt, 0) > 0 then p.diag_path_intraday_fallback_minutes
            else 1440
        end as path_interval_used,
        case
            when coalesce(b5.bar_cnt, 0) > 0 then 'INTRADAY_5M'
            when coalesce(b60.bar_cnt, 0) > 0 then 'INTRADAY_FALLBACK'
            else 'DAILY'
        end as path_approximation_level
    from path_base pb
    cross join params p
    left join bar_hits b5
        on b5.entry_action_id = pb.entry_action_id and b5.INTERVAL_MINUTES = 5
    left join bar_hits b60
        on b60.entry_action_id = pb.entry_action_id and b60.INTERVAL_MINUTES = p.diag_path_intraday_fallback_minutes
),
session_high as (
    select
        pb.entry_action_id,
        ip.path_interval_used,
        ip.path_approximation_level,
        max(b.HIGH) as max_high_before_entry
    from path_base pb
    inner join interval_pick ip on ip.entry_action_id = pb.entry_action_id
    inner join MIP.MART.MARKET_BARS b
        on upper(trim(b.SYMBOL)) = upper(trim(pb.SYMBOL))
       and b.MARKET_TYPE = pb.eff_market_type
       and b.INTERVAL_MINUTES = ip.path_interval_used
       and b.TS::date = pb.canonical_entry_ts::date
       and b.TS <= pb.canonical_entry_ts
    group by pb.entry_action_id, ip.path_interval_used, ip.path_approximation_level
),
post_entry_bars as (
    select
        pb.entry_action_id,
        b.TS,
        b.CLOSE,
        row_number() over (partition by pb.entry_action_id order by b.TS asc) as rn
    from path_base pb
    inner join interval_pick ip on ip.entry_action_id = pb.entry_action_id
    inner join MIP.MART.MARKET_BARS b
        on upper(trim(b.SYMBOL)) = upper(trim(pb.SYMBOL))
       and b.MARKET_TYPE = pb.eff_market_type
       and b.INTERVAL_MINUTES = ip.path_interval_used
       and b.TS > pb.canonical_entry_ts
       and b.TS <= dateadd('hour', 8, pb.canonical_entry_ts)
),
first_three as (
    select
        entry_action_id,
        max(iff(rn = 1, close, null)) as bar1_close,
        max(iff(rn = 2, close, null)) as bar2_close,
        max(iff(rn = 3, close, null)) as bar3_close
    from post_entry_bars
    group by entry_action_id
)
select
    pb.entry_action_id,
    pb.symbol,
    pb.side,
    pb.canonical_entry_ts,
    pb.order_entry_avg_price,
    ip.path_interval_used,
    ip.path_approximation_level,
    sh.max_high_before_entry,
    iff(
        upper(pb.side) = 'BUY' and pb.order_entry_avg_price is not null and sh.max_high_before_entry is not null,
        (sh.max_high_before_entry - pb.order_entry_avg_price) / nullif(pb.order_entry_avg_price, 0),
        null
    ) as dist_from_session_high_pct_long,
    iff(
        upper(pb.side) = 'BUY' and pb.order_entry_avg_price is not null and ft.bar1_close is not null,
        (ft.bar1_close - pb.order_entry_avg_price) / nullif(pb.order_entry_avg_price, 0),
        iff(
            upper(pb.side) = 'SELL' and pb.order_entry_avg_price is not null and ft.bar1_close is not null,
            (pb.order_entry_avg_price - ft.bar1_close) / nullif(pb.order_entry_avg_price, 0),
            null
        )
    ) as first_bar_after_entry_return_pct,
    iff(
        upper(pb.side) = 'BUY' and pb.order_entry_avg_price is not null and ft.bar3_close is not null,
        (ft.bar3_close - pb.order_entry_avg_price) / nullif(pb.order_entry_avg_price, 0),
        null
    ) as three_bar_return_pct_long_proxy
from path_base pb
inner join interval_pick ip on ip.entry_action_id = pb.entry_action_id
left join session_high sh on sh.entry_action_id = pb.entry_action_id
left join first_three ft on ft.entry_action_id = pb.entry_action_id
order by pb.canonical_entry_ts asc;
