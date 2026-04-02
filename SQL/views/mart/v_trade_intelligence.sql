-- v_trade_intelligence.sql
-- MIP.MART.V_TRADE_INTELLIGENCE — Trade Intelligence Record (TIR), Phase 1 / 1.5.
--
-- GRAIN
--   One row per MIP.LIVE.TRADE_CLOSEOUT.CLOSEOUT_ID.
--   Excluded: orphan closeouts where coalesce(ENTRY_INTEL_SNAPSHOT.PORTFOLIO_ID, entry LIVE_ACTIONS.PORTFOLIO_ID) is null.
--
-- COMMITTEE (operational vs alpha stance)
--   COMMITTEE_ACTION_RAW / COMMITTEE_ACTION_NORMALIZED: from COMMITTEE_VERDICT.RECOMMENDATION only (frozen CASE).
--     No verdict row => both NULL. Unmapped raw => NORMALIZED = UNKNOWN.
--   OVERRIDE_CLASS: committee vs EIS baseline from VERDICT_JSON (alpha_override_class or entry_intel_audit_v1 path).
--     Not derived from COMMITTEE_ACTION_*.
--
-- EXPECTATION (EIS)
--   HAS_EIS: snapshot row joined on closeout SNAPSHOT_ID.
--   EXPECTATION_SUMMARY: substr(trim(ALPHA_SPEC.alpha_summary_text), 400) — empty => NULL; not full WORLDS_SPEC/ALPHA_SPEC.
--   EXPECTED_RETURN: try_to_double(ALPHA_SPEC.expected_value_net) — invalid/missing => NULL.
--
-- PARALLEL WORLDS (PW_*) — portfolio-day, not trade replay
--   Join V_PARALLEL_WORLD_DIFF on PORTFOLIO_ID and calendar date:
--     coalesce(EXIT_TS::date, ENTRY_TS::date)
--   Primary semantic: exit day. Fallback: entry day only when EXIT_TS is null (rare).
--   Best scenario: max PNL_DELTA among IS_ACTIVE scenarios; tie-break SCENARIO_ID asc.
--   If no diff rows for that day: all PW_* NULL.
--
-- PW_REGRET_DRIVER (frozen mapping from PARALLEL_WORLD_SCENARIO.SCENARIO_TYPE — Phase 1.5)
--   THRESHOLD   -> FILTER   (signal / threshold tuning)
--   SIZING      -> SIZE     (position size)
--   TIMING      -> HORIZON  (entry bar delay — labeled HORIZON for UX consistency)
--   BASELINE    -> BASELINE (e.g. stay in cash)
--   HORIZON     -> HORIZON  (explicit horizon scenarios)
--   EARLY_EXIT  -> EXIT     (early exit policy)
--   (any other value) -> passthrough as raw SCENARIO_TYPE (forward compatibility; review if new types appear)
--
-- RECONCILIATION
--   Left join LIFECYCLE_RECONCILIATION_STATE on (PORTFOLIO_ID, upper(SYMBOL)).
--   RECON_STATE_AS_OF_TS = UPDATED_TS of that row — current broker/MIP overlay, not historical at exit.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_TRADE_INTELLIGENCE (
    PORTFOLIO_ID,
    SYMBOL,
    ENTRY_ACTION_ID,
    CLOSEOUT_ID,
    ENTRY_TS,
    EXIT_TS,
    HAS_EIS,
    EXPECTED_RETURN,
    EXPECTATION_SUMMARY,
    COMMITTEE_ACTION_RAW,
    COMMITTEE_ACTION_NORMALIZED,
    OVERRIDE_CLASS,
    REALIZED_RETURN,
    REALIZED_PNL,
    ALIGNMENT_CLASS,
    OUTCOME_CLASS,
    BEST_PW_SCENARIO_NAME,
    BEST_PW_SCENARIO_RETURN,
    BEST_PW_VS_ACTUAL_DELTA,
    PW_REGRET_AMOUNT,
    PW_REGRET_DRIVER,
    RECONCILIATION_CLASS,
    HAS_ENTRY_INTEL_LINK,
    RECON_STATE_AS_OF_TS
) as
with tir_base as (
    select
        tc.CLOSEOUT_ID,
        tc.ENTRY_ACTION_ID,
        tc.SYMBOL,
        tc.ENTRY_TS,
        tc.EXIT_TS,
        tc.REALIZED_RETURN_PCT,
        tc.REALIZED_PNL,
        tc.ALIGNMENT_JSON,
        coalesce(e.PORTFOLIO_ID, la.PORTFOLIO_ID) as PORTFOLIO_ID,
        case when e.SNAPSHOT_ID is not null then true else false end as HAS_EIS,
        try_to_double(e.ALPHA_SPEC:expected_value_net::varchar) as EXPECTED_RETURN,
        nullif(trim(substr(coalesce(e.ALPHA_SPEC:alpha_summary_text::varchar, ''), 1, 400)), '') as EXPECTATION_SUMMARY
    from MIP.LIVE.TRADE_CLOSEOUT tc
    left join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = tc.ENTRY_ACTION_ID
    left join MIP.LIVE.ENTRY_INTEL_SNAPSHOT e on e.SNAPSHOT_ID = tc.SNAPSHOT_ID
    where coalesce(e.PORTFOLIO_ID, la.PORTFOLIO_ID) is not null
),
pw_best as (
    select CLOSEOUT_ID, SCENARIO_DISPLAY_NAME, SCENARIO_NAME, CF_RETURN_PCT, PNL_DELTA, SCENARIO_TYPE
    from (
        select b.CLOSEOUT_ID, d.SCENARIO_DISPLAY_NAME, d.SCENARIO_NAME, d.CF_RETURN_PCT, d.PNL_DELTA, d.SCENARIO_TYPE, d.SCENARIO_ID,
            row_number() over (partition by b.CLOSEOUT_ID order by d.PNL_DELTA desc nulls last, d.SCENARIO_ID asc) as rn
        from tir_base b
        -- PW join date: exit calendar day; fallback entry day if EXIT_TS null (portfolio-day context only).
        inner join MIP.MART.V_PARALLEL_WORLD_DIFF d
            on d.PORTFOLIO_ID = b.PORTFOLIO_ID
           and d.AS_OF_TS::date = coalesce(b.EXIT_TS::date, b.ENTRY_TS::date)
        inner join MIP.APP.PARALLEL_WORLD_SCENARIO s on s.SCENARIO_ID = d.SCENARIO_ID and coalesce(s.IS_ACTIVE, false) = true
    ) x where rn = 1
)
select
    b.PORTFOLIO_ID, b.SYMBOL, b.ENTRY_ACTION_ID, b.CLOSEOUT_ID, b.ENTRY_TS, b.EXIT_TS,
    b.HAS_EIS, b.EXPECTED_RETURN, b.EXPECTATION_SUMMARY,
    cv.RECOMMENDATION as COMMITTEE_ACTION_RAW,
    case
        when cv.RUN_ID is null then null
        when trim(upper(cv.RECOMMENDATION)) = 'BLOCK' then 'BLOCK'
        when trim(upper(cv.RECOMMENDATION)) = 'PROCEED_REDUCED' then 'REDUCE'
        when trim(upper(cv.RECOMMENDATION)) is null or trim(upper(cv.RECOMMENDATION)) = '' then 'UNKNOWN'
        else 'UNKNOWN'
    end as COMMITTEE_ACTION_NORMALIZED,
    coalesce(cv.VERDICT_JSON:alpha_override_class::varchar, cv.VERDICT_JSON:entry_intel_audit_v1:alpha_override_class::varchar) as OVERRIDE_CLASS,
    b.REALIZED_RETURN_PCT as REALIZED_RETURN, b.REALIZED_PNL,
    b.ALIGNMENT_JSON:alignment_class::varchar as ALIGNMENT_CLASS,
    b.ALIGNMENT_JSON:realized_outcome_class::varchar as OUTCOME_CLASS,
    coalesce(pw.SCENARIO_DISPLAY_NAME, pw.SCENARIO_NAME) as BEST_PW_SCENARIO_NAME,
    pw.CF_RETURN_PCT as BEST_PW_SCENARIO_RETURN, pw.PNL_DELTA as BEST_PW_VS_ACTUAL_DELTA,
    iff(pw.PNL_DELTA is not null, greatest(pw.PNL_DELTA, 0), null) as PW_REGRET_AMOUNT,
    -- Full SCENARIO_TYPE mapping documented in file header; else = passthrough for unknown future types.
    case
        when pw.SCENARIO_TYPE is null then null
        when pw.SCENARIO_TYPE = 'THRESHOLD' then 'FILTER'
        when pw.SCENARIO_TYPE = 'SIZING' then 'SIZE'
        when pw.SCENARIO_TYPE = 'TIMING' then 'HORIZON'
        when pw.SCENARIO_TYPE = 'BASELINE' then 'BASELINE'
        when pw.SCENARIO_TYPE = 'HORIZON' then 'HORIZON'
        when pw.SCENARIO_TYPE = 'EARLY_EXIT' then 'EXIT'
        else pw.SCENARIO_TYPE
    end as PW_REGRET_DRIVER,
    rs.RECONCILIATION_CLASS, rs.HAS_ENTRY_INTEL_LINK, rs.UPDATED_TS as RECON_STATE_AS_OF_TS
from tir_base b
left join MIP.LIVE.LIVE_ACTIONS la_in on la_in.ACTION_ID = b.ENTRY_ACTION_ID
left join MIP.LIVE.COMMITTEE_VERDICT cv on cv.RUN_ID = la_in.COMMITTEE_RUN_ID
left join pw_best pw on pw.CLOSEOUT_ID = b.CLOSEOUT_ID
left join MIP.LIVE.LIFECYCLE_RECONCILIATION_STATE rs
    on rs.PORTFOLIO_ID = b.PORTFOLIO_ID and upper(coalesce(rs.SYMBOL, '')) = upper(coalesce(b.SYMBOL, ''));
