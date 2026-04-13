-- live_trade_diagnostic_populations.sql
-- NOT EXECUTABLE AS A SINGLE SCRIPT — paste the blocks below into other files' WITH clauses.
-- Keep in sync when cohort logic changes.
--
-- === BLOCK_A: params through executed_entries_post_reset (POPULATION A) ===
/*
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts, 60 as path_intraday_fallback_minutes
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
        la.STATUS as entry_action_status, la.COMMITTEE_RUN_ID, la.PARAM_SNAPSHOT,
        f.FILLED_AT as order_entry_fill_ts,
        f.AVG_FILL_PRICE as order_entry_avg_price,
        f.QTY_FILLED as order_entry_qty,
        f.ORDER_ID as entry_order_id,
        f.BROKER_ORDER_ID as entry_broker_order_id,
        tc.CLOSEOUT_ID, tc.EXIT_TS, tc.EXIT_TYPE,
        tc.REALIZED_PNL, tc.REALIZED_RETURN_PCT, tc.REALIZED_SIZE, tc.HOLDING_PERIOD_SEC,
        tc.ENTRY_TS as closeout_entry_ts_fallback,
        la.UPDATED_AT as action_updated_at,
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
*/

-- === BLOCK_B: committee_considered_post_reset (POPULATION B; requires params, live_portfolios, first_entry_fill) ===
/*
committee_considered_post_reset as (
    select
        cr.RUN_ID, cr.ACTION_ID, cr.STARTED_AT as committee_started_at,
        cr.STATUS as committee_run_status,
        la.PORTFOLIO_ID, la.SYMBOL, la.SIDE, la.PROPOSAL_ID, la.STATUS as action_status,
        la.COMMITTEE_RUN_ID,
        cv.RECOMMENDATION as committee_recommendation,
        cv.SIZE_FACTOR as committee_size_factor,
        cv.IS_BLOCKED as committee_is_blocked,
        cv.CREATED_AT as verdict_created_at,
        iff(f.ACTION_ID is not null, true, false) as has_entry_fill,
        f.FILLED_AT as first_entry_fill_ts
    from MIP.LIVE.COMMITTEE_RUN cr
    inner join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = cr.ACTION_ID
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    left join MIP.LIVE.COMMITTEE_VERDICT cv on cv.RUN_ID = cr.RUN_ID
    left join first_entry_fill f on f.ACTION_ID = la.ACTION_ID
    cross join params p
    where cr.STARTED_AT >= p.reset_ts
      and upper(coalesce(la.ACTION_INTENT,
        iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
)
*/
-- End of paste blocks (do not execute this file as-is).
