-- v_pw_recommendations.sql
-- Purpose: Enriched view of recommendations with display names, formatted values,
-- and rank per portfolio for UI consumption.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PW_RECOMMENDATIONS (
    REC_ID,
    RUN_ID,
    PORTFOLIO_ID,
    AS_OF_TS,
    RECOMMENDATION_TYPE,
    DOMAIN,
    SWEEP_FAMILY,
    SCENARIO_ID,
    PARAMETER_NAME,
    CURRENT_VALUE,
    RECOMMENDED_VALUE,
    EXPECTED_DAILY_DELTA,
    EXPECTED_CUMULATIVE_DELTA,
    WIN_RATE_PCT,
    OBSERVATION_DAYS,
    CONFIDENCE_CLASS,
    CONFIDENCE_REASON,
    REGIME_FRAGILE,
    REGIME_DETAIL,
    SAFETY_STATUS,
    SAFETY_DETAIL,
    EVIDENCE_HASH,
    APPROVAL_STATUS,
    ROLLBACK_NOTE,
    CREATED_AT,
    -- Enriched fields
    DOMAIN_LABEL,
    FAMILY_LABEL,
    TYPE_LABEL,
    CONFIDENCE_EMOJI,
    REC_RANK
) as
select
    r.REC_ID,
    r.RUN_ID,
    r.PORTFOLIO_ID,
    r.AS_OF_TS,
    r.RECOMMENDATION_TYPE,
    r.DOMAIN,
    r.SWEEP_FAMILY,
    r.SCENARIO_ID,
    r.PARAMETER_NAME,
    r.CURRENT_VALUE,
    r.RECOMMENDED_VALUE,
    r.EXPECTED_DAILY_DELTA,
    r.EXPECTED_CUMULATIVE_DELTA,
    r.WIN_RATE_PCT,
    r.OBSERVATION_DAYS,
    r.CONFIDENCE_CLASS,
    r.CONFIDENCE_REASON,
    r.REGIME_FRAGILE,
    r.REGIME_DETAIL,
    r.SAFETY_STATUS,
    r.SAFETY_DETAIL,
    r.EVIDENCE_HASH,
    r.APPROVAL_STATUS,
    r.ROLLBACK_NOTE,
    r.CREATED_AT,
    -- Labels
    case r.DOMAIN when 'SIGNAL' then 'Signal Tuning' else 'Portfolio Tuning' end as DOMAIN_LABEL,
    case r.SWEEP_FAMILY
        when 'ZSCORE_SWEEP' then 'Z-Score Threshold'
        when 'RETURN_SWEEP' then 'Return Threshold'
        when 'SIZING_SWEEP' then 'Position Sizing'
        when 'TIMING_SWEEP' then 'Entry Timing'
        else r.SWEEP_FAMILY
    end as FAMILY_LABEL,
    case r.RECOMMENDATION_TYPE
        when 'CONSERVATIVE' then 'Conservative'
        when 'AGGRESSIVE' then 'Aggressive'
        else r.RECOMMENDATION_TYPE
    end as TYPE_LABEL,
    case r.CONFIDENCE_CLASS
        when 'STRONG'   then 'S'
        when 'EMERGING' then 'E'
        when 'WEAK'     then 'W'
        else 'N'
    end as CONFIDENCE_EMOJI,
    row_number() over (
        partition by r.PORTFOLIO_ID
        order by
            case r.CONFIDENCE_CLASS when 'STRONG' then 1 when 'EMERGING' then 2 when 'WEAK' then 3 else 4 end,
            r.EXPECTED_CUMULATIVE_DELTA desc
    ) as REC_RANK
from MIP.APP.PARALLEL_WORLD_RECOMMENDATION r
where r.APPROVAL_STATUS != 'STALE';
