-- 06_market_context.sql — POPULATION A. Regime + EIS + training digest + PW evidence + TIR (closed).
-- V_TRUSTED_SIGNALS_LATEST_TS is a current snapshot — join is approximate (documented in memo).

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
    select lo.ORDER_ID, lo.ACTION_ID, lo.FILLED_AT, lo.IDEMPOTENCY_KEY,
        la.PORTFOLIO_ID, la.SYMBOL, la.SIDE, la.PROPOSAL_ID, la.COMMITTEE_RUN_ID, la.STATUS, la.UPDATED_AT
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
        la.COMMITTEE_RUN_ID,
        coalesce(f.FILLED_AT, tc.ENTRY_TS, iff(f.FILLED_AT is null and tc.ENTRY_TS is null
            and upper(coalesce(la.STATUS, '')) in ('EXECUTED','EXECUTION_REQUESTED','INTENT_APPROVED','REVALIDATED_PASS'),
            la.UPDATED_AT, null)) as canonical_entry_ts,
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
enriched as (
    select
        e.*,
        op.MARKET_TYPE as proposal_market_type,
        op.SIGNAL_PATTERN_ID as proposal_pattern_id,
        op.SIGNAL_TS as proposal_signal_ts,
        op.PROPOSED_AT as proposal_proposed_at,
        mr.REGIME as market_regime_entry_day,
        mr.REGIME_CONFIDENCE as market_regime_confidence,
        eis.ALPHA_SPEC as eis_alpha_spec,
        eis.WORLDS_SPEC as eis_worlds_spec,
        td.SNAPSHOT_JSON as training_digest_snapshot_json,
        pw.EVIDENCE_SUMMARY as pw_evidence_summary,
        tir.COMMITTEE_ACTION_NORMALIZED as tir_committee_action_normalized,
        tir.BEST_PW_SCENARIO_NAME as tir_best_pw_scenario,
        tir.PW_REGRET_DRIVER as tir_pw_regret_driver,
        tir.ALIGNMENT_CLASS as tir_alignment_class,
        tr.SYMBOL as trusted_signal_symbol_match,
        tr.CONFIDENCE as trusted_signal_confidence_approx
    from executed_entries_post_reset e
    left join MIP.AGENT_OUT.ORDER_PROPOSALS op on op.PROPOSAL_ID = e.PROPOSAL_ID
    left join MIP.MART.V_MARKET_REGIME mr on mr.REGIME_DATE = e.canonical_entry_ts::date
    left join MIP.LIVE.ENTRY_INTEL_ACTION_LINK eial on eial.ENTRY_ACTION_ID = e.entry_action_id
    left join MIP.LIVE.ENTRY_INTEL_SNAPSHOT eis on eis.SNAPSHOT_ID = eial.SNAPSHOT_ID
    left join MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL td
        on upper(trim(td.SYMBOL)) = upper(trim(e.SYMBOL))
       and td.MARKET_TYPE = coalesce(op.MARKET_TYPE, 'STOCK')
       and td.PATTERN_ID = op.SIGNAL_PATTERN_ID
    left join MIP.APP.V_LIVE_ACTION_PARALLEL_WORLDS_EVIDENCE pw on pw.ACTION_ID = e.entry_action_id
    left join MIP.MART.V_TRADE_INTELLIGENCE tir on tir.ENTRY_ACTION_ID = e.entry_action_id
    left join (
        select
            SYMBOL,
            PATTERN_ID,
            CONFIDENCE,
            row_number() over (
                partition by SYMBOL, PATTERN_ID
                order by EFFECTIVE_TARGET desc nulls last, HORIZON_BARS asc
            ) as rn
        from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS
    ) tr
        on upper(trim(tr.SYMBOL)) = upper(trim(e.SYMBOL))
       and tr.PATTERN_ID = op.SIGNAL_PATTERN_ID
       and tr.rn = 1
)
select
    entry_action_id,
    portfolio_id,
    symbol,
    side,
    proposal_id,
    canonical_entry_ts,
    closeout_id,
    proposal_market_type,
    proposal_pattern_id,
    proposal_signal_ts,
    market_regime_entry_day,
    market_regime_confidence,
    eis_alpha_spec:confidence_band::varchar as eis_confidence_band,
    eis_alpha_spec:expected_value_net::float as eis_expected_value_net,
    trusted_signal_confidence_approx,
    trusted_signal_symbol_match is not null as has_trusted_signal_row_approx,
    pw_evidence_summary,
    tir_committee_action_normalized,
    tir_best_pw_scenario,
    tir_pw_regret_driver,
    tir_alignment_class,
    training_digest_snapshot_json is not null as has_training_digest_row
from enriched
order by canonical_entry_ts asc;
