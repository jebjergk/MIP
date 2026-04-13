-- 02_trade_path_mfe_mae.sql — POPULATION A only. MFE/MAE + PATH_INTERVAL_USED + PATH_APPROXIMATION_LEVEL.
-- Interval precedence: 5m bars in window → else 60 (DIAG_PATH_INTRADAY_FALLBACK_MINUTES) → else 1440 daily.
-- Open trades: path_end = current_timestamp(); PATH_IS_PARTIAL = true.

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
    select
        lo.ORDER_ID, lo.ACTION_ID, lo.FILLED_AT, lo.AVG_FILL_PRICE,
        lo.IDEMPOTENCY_KEY,
        la.PORTFOLIO_ID, la.SYMBOL, la.SIDE, la.PROPOSAL_ID, la.STATUS as action_status, la.UPDATED_AT
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
        la.PORTFOLIO_ID, la.SYMBOL, la.SIDE, la.PROPOSAL_ID,
        f.FILLED_AT as order_entry_fill_ts,
        f.AVG_FILL_PRICE as order_entry_avg_price,
        tc.CLOSEOUT_ID, tc.EXIT_TS, tc.EXIT_TYPE,
        tc.REALIZED_PNL, tc.REALIZED_RETURN_PCT,
        coalesce(
            f.FILLED_AT, tc.ENTRY_TS,
            iff(f.FILLED_AT is null and tc.ENTRY_TS is null
                and upper(coalesce(la.STATUS, '')) in (
                    'EXECUTED','EXECUTION_REQUESTED','INTENT_APPROVED','REVALIDATED_PASS'),
                la.UPDATED_AT, null)
        ) as canonical_entry_ts
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
        coalesce(e.exit_ts, current_timestamp()) as path_end_ts,
        e.closeout_id is null as path_is_partial
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
        end as path_approximation_level,
        coalesce(b5.bar_cnt, 0) as bars_5m_in_window,
        coalesce(b60.bar_cnt, 0) as bars_fallback_in_window,
        coalesce(b1d.bar_cnt, 0) as bars_daily_in_window
    from path_base pb
    cross join params p
    left join bar_hits b5
        on b5.entry_action_id = pb.entry_action_id and b5.INTERVAL_MINUTES = 5
    left join bar_hits b60
        on b60.entry_action_id = pb.entry_action_id and b60.INTERVAL_MINUTES = p.diag_path_intraday_fallback_minutes
    left join bar_hits b1d
        on b1d.entry_action_id = pb.entry_action_id and b1d.INTERVAL_MINUTES = 1440
),
path_bars as (
    select
        pb.entry_action_id,
        ip.path_interval_used,
        ip.path_approximation_level,
        pb.path_is_partial,
        pb.canonical_entry_ts,
        pb.path_end_ts,
        pb.order_entry_avg_price,
        pb.side,
        pb.closeout_id,
        pb.exit_ts,
        pb.exit_type,
        pb.realized_pnl,
        pb.realized_return_pct,
        b.TS as bar_ts,
        b.HIGH as bar_high,
        b.LOW as bar_low,
        b.CLOSE as bar_close
    from path_base pb
    inner join interval_pick ip on ip.entry_action_id = pb.entry_action_id
    left join MIP.MART.MARKET_BARS b
        on upper(trim(b.SYMBOL)) = upper(trim(pb.SYMBOL))
       and b.MARKET_TYPE = pb.eff_market_type
       and b.INTERVAL_MINUTES = ip.path_interval_used
       and b.TS >= pb.canonical_entry_ts
       and b.TS <= pb.path_end_ts
),
path_agg as (
    select
        entry_action_id,
        max(path_interval_used) as path_interval_used,
        max(path_approximation_level) as path_approximation_level,
        boolor_agg(path_is_partial) as path_is_partial,
        max(canonical_entry_ts) as canonical_entry_ts,
        max(path_end_ts) as path_end_ts,
        max(order_entry_avg_price) as order_entry_avg_price,
        max(side) as side,
        max(closeout_id) as closeout_id,
        max(exit_ts) as exit_ts,
        max(exit_type) as exit_type,
        max(realized_pnl) as realized_pnl,
        max(realized_return_pct) as realized_return_pct,
        max(bar_high) as path_high,
        min(bar_low) as path_low,
        iff(
            upper(max(side)) = 'BUY',
            (max(bar_high) - max(order_entry_avg_price)) / nullif(max(order_entry_avg_price), 0),
            (max(order_entry_avg_price) - min(bar_low)) / nullif(max(order_entry_avg_price), 0)
        ) as mfe_pct,
        iff(
            upper(max(side)) = 'BUY',
            (max(order_entry_avg_price) - min(bar_low)) / nullif(max(order_entry_avg_price), 0),
            (max(bar_high) - max(order_entry_avg_price)) / nullif(max(order_entry_avg_price), 0)
        ) as mae_pct
    from path_bars
    group by entry_action_id
),
first_bar as (
    select
        entry_action_id,
        bar_close as first_bar_close
    from path_bars
    where bar_ts is not null
    qualify row_number() over (partition by entry_action_id order by bar_ts asc) = 1
),
path_enriched as (
    select
        pa.*,
        fb.first_bar_close,
        iff(
            upper(pa.side) = 'BUY',
            (fb.first_bar_close - pa.order_entry_avg_price) / nullif(pa.order_entry_avg_price, 0),
            (pa.order_entry_avg_price - fb.first_bar_close) / nullif(pa.order_entry_avg_price, 0)
        ) as first_bar_return_pct
    from path_agg pa
    left join first_bar fb on fb.entry_action_id = pa.entry_action_id
),
labeled as (
    select
        pe.*,
        case
            when pe.closeout_id is null then 'OPEN_NO_EXIT_YET'
            when pe.order_entry_avg_price is null or pe.mfe_pct is null then 'NO_PRICE_PATH'
            when upper(coalesce(pe.exit_type, '')) like '%STOP%'
                 or upper(coalesce(pe.exit_type, '')) like '%SL%'
            then
                case
                    when coalesce(pe.mfe_pct, 0) < 0.001 then 'STRAIGHT_TO_STOP'
                    when coalesce(pe.mfe_pct, 0) >= 0.005 and coalesce(pe.realized_pnl, 0) < 0
                        then 'WORKED_THEN_REVERSED'
                    else 'STOP_HIT_OTHER'
                end
            when coalesce(pe.first_bar_return_pct, 0) <= -0.002
                 and coalesce(pe.mfe_pct, 0) < 0.002
                then 'FAILED_IMMEDIATELY'
            when coalesce(pe.mfe_pct, 0) >= 0.005
                 and coalesce(pe.realized_pnl, 0) < 0
                then 'WORKED_THEN_REVERSED'
            when coalesce(pe.mfe_pct, 0) > 0 and coalesce(pe.mfe_pct, 0) < 0.003
                 and coalesce(pe.realized_pnl, 0) < 0
                then 'SMALL_POSITIVE_ONLY'
            when coalesce(pe.mfe_pct, 0) >= 0.01
                 and (upper(coalesce(pe.exit_type, '')) like '%STOP%' or upper(coalesce(pe.exit_type, '')) like '%SL%')
                then 'NEAR_TARGET_THEN_FAILED'
            else 'UNCLASSIFIED_PATH'
        end as failure_path_category
    from path_enriched pe
)
select
    entry_action_id,
    path_interval_used,
    path_approximation_level,
    path_is_partial,
    canonical_entry_ts,
    path_end_ts,
    order_entry_avg_price,
    side,
    closeout_id,
    exit_ts,
    exit_type,
    realized_pnl,
    realized_return_pct,
    round(mfe_pct, 6) as mfe_pct,
    round(mae_pct, 6) as mae_pct,
    round(first_bar_return_pct, 6) as first_bar_return_pct,
    failure_path_category
from labeled
order by canonical_entry_ts asc;
