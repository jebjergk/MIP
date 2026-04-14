-- 10_broker_truth_duq101771_post_reset.sql
-- Broker-execution truth only for IBKR account DUQ101771, post-reset (>= 2026-04-07).
-- Dedupes replayed BROKER_SNAPSHOTS rows by PAYLOAD:exec_id (latest SNAPSHOT_ROW_ID wins).
-- Chronology: coalesce(PAYLOAD:time parsed, SNAPSHOT_TS).
-- LIVE_ORDERS / TRADE_CLOSEOUT are NOT used to define trade existence.
-- Read-only.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Cohort summary (raw vs deduped)
with params as (select to_timestamp_ntz('2026-04-07') as reset_ts),
raw as (
    select *
    from MIP.LIVE.BROKER_SNAPSHOTS bs
    cross join params p
    where bs.IBKR_ACCOUNT_ID = 'DUQ101771'
      and bs.SNAPSHOT_TYPE = 'EXECUTION'
      and bs.SNAPSHOT_TS >= p.reset_ts
),
dedup as (
    select r.*,
        row_number() over (
            partition by trim(r.PAYLOAD:exec_id::varchar)
            order by r.SNAPSHOT_ROW_ID desc
        ) as rn
    from raw r
)
select
    'COHORT_SUMMARY' as section,
    (select count(*) from raw) as n_snapshot_rows_raw,
    (select count(*) from dedup where rn = 1) as n_executions_deduped,
    (select count(distinct trim(PAYLOAD:exec_id::varchar)) from raw) as n_distinct_exec_id;

-- 2) Deduped execution facts + secondary lineage (LIVE_ORDERS -> LIVE_ACTIONS)
with params as (select to_timestamp_ntz('2026-04-07') as reset_ts),
raw as (
    select bs.*,
        row_number() over (
            partition by trim(bs.PAYLOAD:exec_id::varchar)
            order by bs.SNAPSHOT_ROW_ID desc
        ) as rn
    from MIP.LIVE.BROKER_SNAPSHOTS bs
    cross join params p
    where bs.IBKR_ACCOUNT_ID = 'DUQ101771'
      and bs.SNAPSHOT_TYPE = 'EXECUTION'
      and bs.SNAPSHOT_TS >= p.reset_ts
),
dedup as (select * from raw where rn = 1),
norm as (
    select
        d.SNAPSHOT_ROW_ID,
        trim(d.PAYLOAD:exec_id::varchar) as exec_id,
        coalesce(
            try_to_timestamp_ntz(d.PAYLOAD:time::varchar),
            d.SNAPSHOT_TS
        ) as exec_ts,
        d.SNAPSHOT_TS as snapshot_ingest_ts,
        upper(trim(coalesce(d.SYMBOL, d.PAYLOAD:symbol::varchar))) as symbol,
        upper(trim(d.SECURITY_TYPE)) as security_type,
        case
            when upper(trim(d.PAYLOAD:side::varchar)) in ('BUY', 'BOT', 'B') then 'BUY'
            when upper(trim(d.PAYLOAD:side::varchar)) in ('SELL', 'SLD', 'S') then 'SELL'
            else 'UNKNOWN'
        end as side,
        abs(
            coalesce(
                try_to_double(d.PAYLOAD:shares::varchar),
                try_to_double(d.OPEN_ORDER_FILLED::varchar),
                0
            )
        ) as qty,
        coalesce(
            try_to_double(d.PAYLOAD:price::varchar),
            try_to_double(d.OPEN_ORDER_LIMIT_PRICE::varchar),
            try_to_double(d.AVG_COST::varchar)
        ) as price,
        coalesce(
            try_to_double(d.PAYLOAD:realized_pnl::varchar),
            try_to_double(d.PAYLOAD:realizedPNL::varchar),
            try_to_double(d.REALIZED_PNL::varchar)
        ) as broker_realized_pnl,
        try_to_double(d.PAYLOAD:commission::varchar) as broker_commission_scalar,
        nullif(
            trim(
                coalesce(
                    d.OPEN_ORDER_ID::varchar,
                    d.PAYLOAD:perm_id::varchar,
                    d.PAYLOAD:order_id::varchar,
                    ''
                )
            ),
            ''
        ) as broker_order_key,
        d.PAYLOAD:exchange::varchar as venue
    from dedup d
),
ranked_orders as (
    select
        lo.ORDER_ID,
        lo.ACTION_ID,
        lo.BROKER_ORDER_ID,
        lo.PORTFOLIO_ID,
        lo.STATUS as lo_status,
        row_number() over (
            partition by lo.PORTFOLIO_ID, upper(trim(coalesce(lo.BROKER_ORDER_ID, '')))
            order by lo.LAST_UPDATED_AT desc nulls last, lo.CREATED_AT desc nulls last, lo.ORDER_ID asc
        ) as rn
    from MIP.LIVE.LIVE_ORDERS lo
    where lo.PORTFOLIO_ID = 1
      and lo.BROKER_ORDER_ID is not null
      and trim(lo.BROKER_ORDER_ID::varchar) <> ''
)
select
    n.exec_ts,
    n.exec_id,
    n.symbol,
    n.side,
    n.qty,
    n.price,
    n.venue,
    n.broker_realized_pnl,
    n.broker_commission_scalar,
    n.broker_order_key,
    ro.ORDER_ID as mip_order_id,
    ro.ACTION_ID as mip_action_id,
    ro.lo_status,
    la.PROPOSAL_ID,
    la.COMMITTEE_RUN_ID,
    la.STATUS as live_action_status,
    la.ACTION_INTENT
from norm n
left join ranked_orders ro
    on ro.rn = 1
   and ro.PORTFOLIO_ID = 1
   and n.broker_order_key is not null
   and upper(trim(ro.BROKER_ORDER_ID::varchar)) = upper(trim(n.broker_order_key))
left join MIP.LIVE.LIVE_ACTIONS la
    on la.ACTION_ID = ro.ACTION_ID
order by n.exec_ts asc, n.exec_id asc;

-- 3) Symbol-level breakdown (deduped executions)
with params as (select to_timestamp_ntz('2026-04-07') as reset_ts),
raw as (
    select bs.*,
        row_number() over (
            partition by trim(bs.PAYLOAD:exec_id::varchar)
            order by bs.SNAPSHOT_ROW_ID desc
        ) as rn
    from MIP.LIVE.BROKER_SNAPSHOTS bs
    cross join params p
    where bs.IBKR_ACCOUNT_ID = 'DUQ101771'
      and bs.SNAPSHOT_TYPE = 'EXECUTION'
      and bs.SNAPSHOT_TS >= p.reset_ts
),
dedup as (select * from raw where rn = 1)
select
    'SYMBOL_BREAKDOWN' as section,
    upper(trim(coalesce(SYMBOL, PAYLOAD:symbol::varchar))) as symbol,
    count(*) as n_fills,
    count_if(upper(trim(PAYLOAD:side::varchar)) in ('BUY', 'BOT', 'B')) as n_buy_fills,
    count_if(upper(trim(PAYLOAD:side::varchar)) in ('SELL', 'SLD', 'S')) as n_sell_fills,
    sum(
        iff(
            upper(trim(PAYLOAD:side::varchar)) in ('BUY', 'BOT', 'B'),
            abs(coalesce(try_to_double(PAYLOAD:shares::varchar), try_to_double(OPEN_ORDER_FILLED::varchar), 0)),
            -abs(coalesce(try_to_double(PAYLOAD:shares::varchar), try_to_double(OPEN_ORDER_FILLED::varchar), 0))
        )
    ) as net_qty_signed_buys_minus_sells,
    sum(
        coalesce(
            try_to_double(PAYLOAD:realized_pnl::varchar),
            try_to_double(PAYLOAD:realizedPNL::varchar),
            try_to_double(REALIZED_PNL::varchar)
        )
    ) as sum_broker_realized_pnl
from dedup
group by 2
order by n_fills desc;

-- 4) Supporting context: open-order snapshots (not executions) — cancelled/working brackets, not scorecard
select
    'OPEN_ORDER_SNAPSHOTS_CONTEXT' as section,
    count(*) as n_rows,
    count_if(upper(trim(coalesce(OPEN_ORDER_STATUS, PAYLOAD:status::varchar, ''))) like '%CANCEL%') as n_status_like_cancel
from MIP.LIVE.BROKER_SNAPSHOTS bs
cross join (select to_timestamp_ntz('2026-04-07') as reset_ts) p
where bs.IBKR_ACCOUNT_ID = 'DUQ101771'
  and bs.SNAPSHOT_TYPE = 'OPEN_ORDER'
  and bs.SNAPSHOT_TS >= p.reset_ts;
