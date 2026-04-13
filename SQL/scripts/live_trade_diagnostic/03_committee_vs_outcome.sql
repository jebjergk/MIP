-- 03_committee_vs_outcome.sql — POPULATION A vs POPULATION B kept in separate result sets.
-- Read-only. No counterfactual prices for blocked/non-filled actions.

use role MIP_ADMIN_ROLE;
use database MIP;

-- POPULATION A: executed entries with committee verdict + realized outcome
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
)
select
    'POP_A_EXECUTED_WITH_OUTCOME' as report_section,
    e.entry_action_id,
    e.canonical_entry_ts,
    cv.RECOMMENDATION as committee_recommendation,
    cv.SIZE_FACTOR as committee_size_factor,
    e.closeout_id,
    e.realized_pnl,
    e.exit_type,
    case
        when e.closeout_id is null then 'OPEN'
        when coalesce(e.realized_pnl, 0) > 0 then 'WIN'
        when coalesce(e.realized_pnl, 0) < 0 then 'LOSS'
        else 'FLAT'
    end as outcome_bucket
from executed_entries_post_reset e
left join MIP.LIVE.COMMITTEE_VERDICT cv on cv.RUN_ID = e.COMMITTEE_RUN_ID
order by e.canonical_entry_ts asc;

-- POPULATION B: committee-considered actions (includes never-filled / BLOCK)
with
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts
),
live_portfolios as (
    select PORTFOLIO_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where upper(coalesce(ADAPTER_MODE, '')) = 'LIVE'
),
entry_leg_orders as (
    select lo.ORDER_ID, lo.ACTION_ID, lo.FILLED_AT
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
committee_considered_post_reset as (
    select
        cr.RUN_ID,
        cr.ACTION_ID,
        cr.STARTED_AT as committee_started_at,
        la.PORTFOLIO_ID,
        la.SYMBOL,
        la.PROPOSAL_ID,
        la.STATUS as action_status,
        cv.RECOMMENDATION as committee_recommendation,
        cv.SIZE_FACTOR as committee_size_factor,
        iff(f.ACTION_ID is not null, true, false) as has_entry_fill_after_committee,
        f.FILLED_AT as first_entry_fill_ts
    from MIP.LIVE.COMMITTEE_RUN cr
    inner join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = cr.ACTION_ID
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    left join MIP.LIVE.COMMITTEE_VERDICT cv on cv.RUN_ID = cr.RUN_ID
    left join first_entry_fill f on f.ACTION_ID = la.ACTION_ID
    cross join params p
    where cr.STARTED_AT >= p.reset_ts
      and upper(coalesce(la.ACTION_INTENT, iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
)
select
    'POP_B_COMMITTEE_CONSIDERED' as report_section,
    c.*,
    case
        when not has_entry_fill_after_committee and upper(coalesce(committee_recommendation, '')) = 'BLOCK'
            then 'BLOCKED_NO_FILL_COUNTERFACTUAL_UNAVAILABLE'
        when not has_entry_fill_after_committee
            then 'NO_FILL_OTHER_REASON_COUNTERFACTUAL_UNAVAILABLE'
        else 'HAS_ENTRY_FILL'
    end as counterfactual_note
from committee_considered_post_reset c
order by committee_started_at asc;

-- POP B summary: overlap with executed cohort
with
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts
),
live_portfolios as (
    select PORTFOLIO_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where upper(coalesce(ADAPTER_MODE, '')) = 'LIVE'
),
entry_leg_orders as (
    select lo.ORDER_ID, lo.ACTION_ID, lo.FILLED_AT, la.ACTION_INTENT, la.SIDE, la.STATUS, la.UPDATED_AT
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
            la.UPDATED_AT, null)) as canonical_entry_ts
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
committee_considered_post_reset as (
    select cr.ACTION_ID, cv.RECOMMENDATION
    from MIP.LIVE.COMMITTEE_RUN cr
    inner join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = cr.ACTION_ID
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    left join MIP.LIVE.COMMITTEE_VERDICT cv on cv.RUN_ID = cr.RUN_ID
    cross join params p
    where cr.STARTED_AT >= p.reset_ts
      and upper(coalesce(la.ACTION_INTENT, iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
)
select
    'POP_B_SUMMARY_OVERLAP' as report_section,
    count(distinct c.ACTION_ID) as committee_actions,
    count(distinct e.entry_action_id) as executed_actions_post_reset,
    count(distinct case when e.entry_action_id is not null then c.ACTION_ID end) as committee_actions_that_executed
from committee_considered_post_reset c
left join executed_entries_post_reset e on e.entry_action_id = c.ACTION_ID;
