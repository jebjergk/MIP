-- 09_ib_snapshot_executions_post_reset.sql
-- Population A′ (UI-aligned): broker execution rows ingested like GET /live/activity/overview `executions`.
--
-- Source: MIP.LIVE.BROKER_SNAPSHOTS where SNAPSHOT_TYPE = 'EXECUTION'
-- Join: LIVE_ORDERS on BROKER_ORDER_ID (latest row per key) -> LIVE_ACTIONS for PROPOSAL_ID / COMMITTEE_RUN_ID
--
-- Filter: SNAPSHOT_TS >= 2026-04-07 and LIVE adapter portfolios only.
-- Read-only. Does not dedupe duplicate snapshot replays — use PAYLOAD:exec_id when present for uniqueness.
--
-- Field mapping mirrors mip_ui_api live.py get_live_activity_overview (~7147–7318) where practical.

use role MIP_ADMIN_ROLE;
use database MIP;

with
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts
),
live_book as (
    select PORTFOLIO_ID, IBKR_ACCOUNT_ID
    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where upper(coalesce(ADAPTER_MODE, '')) = 'LIVE'
),
exec_staging as (
    select
        bs.SNAPSHOT_ROW_ID,
        bs.SNAPSHOT_TS,
        bs.IBKR_ACCOUNT_ID,
        lb.PORTFOLIO_ID,
        upper(trim(coalesce(bs.SYMBOL, bs.PAYLOAD:symbol::varchar, ''))) as symbol,
        bs.SECURITY_TYPE,
        bs.PAYLOAD,
        bs.OPEN_ORDER_ID,
        bs.OPEN_ORDER_FILLED,
        bs.OPEN_ORDER_LIMIT_PRICE,
        bs.AVG_COST,
        bs.REALIZED_PNL as snapshot_realized_pnl_col,
        nullif(
            trim(
                coalesce(
                    bs.OPEN_ORDER_ID::varchar,
                    bs.PAYLOAD:perm_id::varchar,
                    bs.PAYLOAD:order_id::varchar,
                    ''
                )
            ),
            ''
        ) as broker_order_key,
        nullif(trim(bs.PAYLOAD:exec_id::varchar), '') as exec_id,
        try_to_double(bs.PAYLOAD:shares::varchar) as qty_from_payload,
        try_to_double(bs.OPEN_ORDER_FILLED::varchar) as qty_from_snap_col,
        try_to_double(bs.PAYLOAD:price::varchar) as price_from_payload,
        try_to_double(bs.OPEN_ORDER_LIMIT_PRICE::varchar) as price_from_limit_col,
        try_to_double(bs.AVG_COST::varchar) as price_from_avg_cost,
        try_to_double(coalesce(bs.PAYLOAD:realized_pnl::varchar, bs.PAYLOAD:realizedPNL::varchar)) as pnl_from_payload,
        try_to_double(bs.PAYLOAD:commission::varchar) as commission_from_payload,
        upper(trim(bs.PAYLOAD:side::varchar)) as side_raw
    from MIP.LIVE.BROKER_SNAPSHOTS bs
    inner join live_book lb
        on lb.IBKR_ACCOUNT_ID = bs.IBKR_ACCOUNT_ID
    cross join params p
    where bs.SNAPSHOT_TYPE = 'EXECUTION'
      and bs.SNAPSHOT_TS >= p.reset_ts
),
exec_norm as (
    select
        e.*,
        abs(coalesce(e.qty_from_payload, e.qty_from_snap_col, 0)) as qty_filled_abs,
        coalesce(
            e.price_from_payload,
            e.price_from_limit_col,
            e.price_from_avg_cost
        ) as avg_fill_price_eff,
        coalesce(e.pnl_from_payload, try_to_double(e.snapshot_realized_pnl_col::varchar)) as realized_pnl_eff,
        case
            when e.side_raw in ('BUY', 'BOT', 'B') then 'BUY'
            when e.side_raw in ('SELL', 'SLD', 'S') then 'SELL'
            when coalesce(e.qty_from_payload, e.qty_from_snap_col, 0) >= 0 then 'BUY'
            else 'SELL'
        end as side_normalized,
        e.SNAPSHOT_TS as execution_ts_for_cohort
    from exec_staging e
),
ranked_orders as (
    select
        lo.ORDER_ID,
        lo.ACTION_ID,
        lo.BROKER_ORDER_ID,
        lo.PORTFOLIO_ID,
        lo.STATUS as lo_status,
        lo.FILLED_AT as lo_filled_at,
        lo.QTY_FILLED as lo_qty_filled,
        row_number() over (
            partition by lo.PORTFOLIO_ID, upper(trim(coalesce(lo.BROKER_ORDER_ID, '')))
            order by lo.LAST_UPDATED_AT desc nulls last, lo.CREATED_AT desc nulls last, lo.ORDER_ID asc
        ) as rn
    from MIP.LIVE.LIVE_ORDERS lo
    inner join live_book lb on lb.PORTFOLIO_ID = lo.PORTFOLIO_ID
    where lo.BROKER_ORDER_ID is not null
      and trim(lo.BROKER_ORDER_ID::varchar) <> ''
)
select
    e.SNAPSHOT_ROW_ID,
    e.PORTFOLIO_ID,
    e.IBKR_ACCOUNT_ID,
    e.symbol,
    e.SECURITY_TYPE,
    e.execution_ts_for_cohort as snapshot_ts,
    e.exec_id,
    e.broker_order_key,
    e.side_normalized as side,
    e.qty_filled_abs as qty_filled,
    e.avg_fill_price_eff as avg_fill_price,
    e.realized_pnl_eff as realized_pnl,
    e.commission_from_payload as commission,
    ro.ORDER_ID as mip_order_id,
    ro.ACTION_ID as mip_action_id,
    ro.lo_status,
    ro.lo_filled_at,
    la.PROPOSAL_ID,
    la.COMMITTEE_RUN_ID,
    la.STATUS as live_action_status,
    'POP_A_PRIME_IB_SNAPSHOT_EXECUTION' as population_label
from exec_norm e
left join ranked_orders ro
    on ro.PORTFOLIO_ID = e.PORTFOLIO_ID
   and ro.rn = 1
   and e.broker_order_key is not null
   and upper(trim(ro.BROKER_ORDER_ID::varchar)) = upper(trim(e.broker_order_key))
left join MIP.LIVE.LIVE_ACTIONS la
    on la.ACTION_ID = ro.ACTION_ID
order by e.execution_ts_for_cohort desc, e.SNAPSHOT_ROW_ID desc;

-- Summary: counts by symbol (same cohort)
with
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts
),
live_book as (
    select PORTFOLIO_ID, IBKR_ACCOUNT_ID
    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where upper(coalesce(ADAPTER_MODE, '')) = 'LIVE'
)
select
    'POP_A_PRIME_SUMMARY_BY_SYMBOL' as report_section,
    upper(trim(coalesce(bs.SYMBOL, bs.PAYLOAD:symbol::varchar, ''))) as symbol,
    count(*) as n_execution_snapshot_rows,
    count_if(nullif(trim(coalesce(bs.OPEN_ORDER_ID::varchar, bs.PAYLOAD:perm_id::varchar, bs.PAYLOAD:order_id::varchar, '')), '') is not null) as n_with_broker_key,
    sum(try_to_double(coalesce(bs.PAYLOAD:realized_pnl::varchar, bs.PAYLOAD:realizedPNL::varchar, bs.REALIZED_PNL::varchar))) as sum_payload_or_col_realized_pnl
from MIP.LIVE.BROKER_SNAPSHOTS bs
inner join live_book lb on lb.IBKR_ACCOUNT_ID = bs.IBKR_ACCOUNT_ID
cross join params p
where bs.SNAPSHOT_TYPE = 'EXECUTION'
  and bs.SNAPSHOT_TS >= p.reset_ts
group by upper(trim(coalesce(bs.SYMBOL, bs.PAYLOAD:symbol::varchar, '')))
order by n_execution_snapshot_rows desc;
