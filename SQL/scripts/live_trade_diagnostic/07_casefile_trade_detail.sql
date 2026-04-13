-- 07_casefile_trade_detail.sql — POPULATION A case file + conservative FAILURE_MODE_LABEL / FAILURE_MODE_CONFIDENCE.
-- Uses INSUFFICIENT_EVIDENCE when regime vs path signals conflict.

use role MIP_ADMIN_ROLE;
use database MIP;

with
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts, 60 as diag_path_intraday_fallback_minutes
),
live_portfolios as (
    select PORTFOLIO_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where upper(coalesce(ADAPTER_MODE, '')) = 'LIVE'
),
entry_leg_orders as (
    select lo.ORDER_ID, lo.ACTION_ID, lo.FILLED_AT, lo.AVG_FILL_PRICE, lo.IDEMPOTENCY_KEY,
        la.PORTFOLIO_ID, la.SYMBOL, la.SIDE, la.PROPOSAL_ID, la.PARAM_SNAPSHOT, la.COMMITTEE_RUN_ID, la.STATUS, la.UPDATED_AT
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
        la.PARAM_SNAPSHOT,
        la.COMMITTEE_RUN_ID,
        f.AVG_FILL_PRICE as order_entry_avg_price,
        coalesce(f.FILLED_AT, tc.ENTRY_TS, iff(f.FILLED_AT is null and tc.ENTRY_TS is null
            and upper(coalesce(la.STATUS, '')) in ('EXECUTED','EXECUTION_REQUESTED','INTENT_APPROVED','REVALIDATED_PASS'),
            la.UPDATED_AT, null)) as canonical_entry_ts,
        tc.CLOSEOUT_ID,
        tc.EXIT_TS,
        tc.EXIT_TYPE,
        tc.REALIZED_PNL,
        tc.REALIZED_RETURN_PCT
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
bracket as (
    select
        e.entry_action_id,
        case
            when not coalesce(e.PARAM_SNAPSHOT:executable_bracket:blocked::boolean, false)
                 and try_to_double(e.PARAM_SNAPSHOT:executable_bracket:target_return::varchar) is not null
                 and try_to_double(e.PARAM_SNAPSHOT:executable_bracket:stop_loss_pct::varchar) is not null
                then 'EXECUTABLE_BRACKET'
            when try_to_double(e.PARAM_SNAPSHOT:committee_bracket_baseline:stop_loss_pct::varchar) is not null
                then 'COMMITTEE_BRACKET_BASELINE'
            else 'OTHER_OR_FAILED'
        end as bracket_layer_simplified,
        try_to_double(e.PARAM_SNAPSHOT:executable_bracket:stop_loss_pct::varchar) as stop_pct_snapshot
    from executed_entries_post_reset e
),
path_core as (
    select
        e.entry_action_id,
        e.symbol,
        e.canonical_entry_ts,
        coalesce(op.MARKET_TYPE, 'STOCK') as eff_market_type,
        coalesce(e.exit_ts, current_timestamp()) as path_end_ts,
        e.order_entry_avg_price,
        e.side
    from executed_entries_post_reset e
    left join MIP.AGENT_OUT.ORDER_PROPOSALS op on op.PROPOSAL_ID = e.PROPOSAL_ID
),
bar_hits as (
    select
        pc.entry_action_id,
        b.INTERVAL_MINUTES,
        count(*) as bar_cnt
    from path_core pc
    inner join MIP.MART.MARKET_BARS b
        on upper(trim(b.SYMBOL)) = upper(trim(pc.SYMBOL))
       and b.MARKET_TYPE = pc.eff_market_type
       and b.TS >= pc.canonical_entry_ts
       and b.TS <= pc.path_end_ts
       and b.INTERVAL_MINUTES in (5, 60, 1440)
    group by pc.entry_action_id, b.INTERVAL_MINUTES
),
path_pick as (
    select
        pc.entry_action_id,
        pc.symbol,
        pc.eff_market_type,
        pc.path_end_ts,
        pc.order_entry_avg_price,
        pc.side,
        pc.canonical_entry_ts,
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
    from path_core pc
    cross join params p
    left join bar_hits b5
        on b5.entry_action_id = pc.entry_action_id and b5.INTERVAL_MINUTES = 5
    left join bar_hits b60
        on b60.entry_action_id = pc.entry_action_id and b60.INTERVAL_MINUTES = p.diag_path_intraday_fallback_minutes
),
path_agg as (
    select
        pp.entry_action_id,
        max(pp.path_interval_used) as path_interval_used,
        max(pp.path_approximation_level) as path_approximation_level,
        iff(
            upper(max(pp.side)) = 'BUY',
            (max(b.HIGH) - max(pp.order_entry_avg_price)) / nullif(max(pp.order_entry_avg_price), 0),
            (max(pp.order_entry_avg_price) - min(b.LOW)) / nullif(max(pp.order_entry_avg_price), 0)
        ) as mfe_pct,
        iff(
            upper(max(pp.side)) = 'BUY',
            (max(pp.order_entry_avg_price) - min(b.LOW)) / nullif(max(pp.order_entry_avg_price), 0),
            (max(b.HIGH) - max(pp.order_entry_avg_price)) / nullif(max(pp.order_entry_avg_price), 0)
        ) as mae_pct
    from path_pick pp
    left join MIP.MART.MARKET_BARS b
        on upper(trim(b.SYMBOL)) = upper(trim(pp.SYMBOL))
       and b.MARKET_TYPE = pp.eff_market_type
       and b.INTERVAL_MINUTES = pp.path_interval_used
       and b.TS >= pp.canonical_entry_ts
       and b.TS <= pp.path_end_ts
    group by pp.entry_action_id
),
ctx as (
    select
        e.*,
        cv.RECOMMENDATION as committee_recommendation,
        mr.REGIME as regime,
        b.bracket_layer_simplified,
        b.stop_pct_snapshot,
        pa.mfe_pct,
        pa.mae_pct,
        pa.path_interval_used,
        pa.path_approximation_level,
        tir.OUTCOME_CLASS as tir_outcome_class,
        coalesce(op.SIGNAL_TS, op.PROPOSED_AT) as signal_anchor_ts,
        case
            when coalesce(op.SIGNAL_TS, op.PROPOSED_AT) is null then 'UNKNOWN'
            when e.canonical_entry_ts::date = coalesce(op.SIGNAL_TS, op.PROPOSED_AT)::date then 'SAME_SIGNAL_DAY'
            when datediff('day', coalesce(op.SIGNAL_TS, op.PROPOSED_AT)::date, e.canonical_entry_ts::date) = 1 then 'NEXT_SESSION'
            when datediff('day', coalesce(op.SIGNAL_TS, op.PROPOSED_AT)::date, e.canonical_entry_ts::date) > 1 then 'LATER'
            else 'UNKNOWN'
        end as entry_lag_class
    from executed_entries_post_reset e
    left join MIP.LIVE.COMMITTEE_VERDICT cv on cv.RUN_ID = e.COMMITTEE_RUN_ID
    left join MIP.MART.V_MARKET_REGIME mr on mr.REGIME_DATE = e.canonical_entry_ts::date
    left join bracket b on b.entry_action_id = e.entry_action_id
    left join path_agg pa on pa.entry_action_id = e.entry_action_id
    left join MIP.MART.V_TRADE_INTELLIGENCE tir on tir.ENTRY_ACTION_ID = e.entry_action_id
    left join MIP.AGENT_OUT.ORDER_PROPOSALS op on op.PROPOSAL_ID = e.PROPOSAL_ID
),
scored as (
    select
        c.*,
        (upper(c.side) = 'BUY' and c.regime in ('BEAR', 'VOLATILE_BEAR')) as regime_headwind_long,
        (coalesce(c.stop_pct_snapshot, 0) > 0 and coalesce(c.stop_pct_snapshot, 0) < 0.004) as very_tight_stop_proxy,
        (c.mfe_pct is not null and c.mfe_pct >= 0.003) as had_meaningful_mfe,
        (c.mae_pct is not null and c.mae_pct >= 0.003) as had_meaningful_mae
    from ctx c
),
fm as (
    select
        s.*,
        case
            when s.closeout_id is null then 'INSUFFICIENT_EVIDENCE'
            when s.regime_headwind_long and s.had_meaningful_mfe and coalesce(s.realized_pnl, 0) < 0
                then 'INSUFFICIENT_EVIDENCE'
            when s.regime_headwind_long and upper(s.side) = 'BUY' and coalesce(s.realized_pnl, 0) < 0
                 and not coalesce(s.had_meaningful_mfe, false)
                then 'MARKET_REGIME_MISMATCH'
            when s.very_tight_stop_proxy
                 and (upper(coalesce(s.exit_type, '')) like '%STOP%' or upper(coalesce(s.exit_type, '')) like '%SL%')
                 and coalesce(s.realized_pnl, 0) < 0
                then 'STOP_TOO_TIGHT'
            when coalesce(s.had_meaningful_mfe, false) and coalesce(s.realized_pnl, 0) < 0
                 and not (upper(coalesce(s.exit_type, '')) like '%STOP%' or upper(coalesce(s.exit_type, '')) like '%SL%')
                then 'INSUFFICIENT_EVIDENCE'
            when coalesce(s.had_meaningful_mfe, false) and coalesce(s.realized_pnl, 0) < 0
                then 'BAD_ENTRY_TIMING'
            when coalesce(s.realized_pnl, 0) < 0 and s.bracket_layer_simplified = 'OTHER_OR_FAILED'
                then 'WEAK_RR_STRUCTURE'
            when coalesce(s.realized_pnl, 0) < 0
                then 'BAD_SELECTION'
            when coalesce(s.realized_pnl, 0) >= 0
                then 'NO_DOMINANT_FAILURE'
            else 'INSUFFICIENT_EVIDENCE'
        end as failure_mode_label,
        case
            when s.closeout_id is null then 'LOW'
            when s.regime_headwind_long and s.had_meaningful_mfe and coalesce(s.realized_pnl, 0) < 0
                then 'LOW'
            when s.regime_headwind_long and upper(s.side) = 'BUY' and coalesce(s.realized_pnl, 0) < 0
                 and not coalesce(s.had_meaningful_mfe, false)
                then 'MEDIUM'
            when s.very_tight_stop_proxy
                 and (upper(coalesce(s.exit_type, '')) like '%STOP%' or upper(coalesce(s.exit_type, '')) like '%SL%')
                then 'MEDIUM'
            when coalesce(s.had_meaningful_mfe, false) and coalesce(s.realized_pnl, 0) < 0
                 and not (upper(coalesce(s.exit_type, '')) like '%STOP%' or upper(coalesce(s.exit_type, '')) like '%SL%')
                then 'LOW'
            when coalesce(s.had_meaningful_mae, false) and coalesce(s.had_meaningful_mfe, false)
                then 'LOW'
            when coalesce(s.realized_pnl, 0) < 0
                then 'LOW'
            else 'MEDIUM'
        end as failure_mode_confidence
    from scored s
)
select
    entry_action_id,
    portfolio_id,
    symbol,
    side,
    proposal_id,
    canonical_entry_ts,
    exit_ts,
    exit_type,
    realized_pnl,
    realized_return_pct,
    closeout_id,
    committee_recommendation,
    regime as market_regime_entry_day,
    bracket_layer_simplified,
    path_interval_used,
    path_approximation_level,
    round(mfe_pct, 6) as mfe_pct,
    round(mae_pct, 6) as mae_pct,
    entry_lag_class,
    signal_anchor_ts,
    tir_outcome_class,
    failure_mode_label,
    failure_mode_confidence
from fm
order by canonical_entry_ts asc;
