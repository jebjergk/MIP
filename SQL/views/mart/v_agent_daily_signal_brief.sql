-- v_agent_daily_signal_brief.sql
-- Purpose: Daily signal changes for newly trusted, downgraded, and weak watchlist signals

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_AGENT_DAILY_SIGNAL_BRIEF as
with current_policy as (
    select
        PATTERN_ID,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        HORIZON_BARS,
        TRUST_LABEL,
        RECOMMENDED_ACTION,
        REASON
    from MIP.MART.V_TRUSTED_SIGNAL_POLICY
),
previous_kpis as (
    select
        r.PATTERN_ID,
        r.MARKET_TYPE,
        r.INTERVAL_MINUTES,
        o.HORIZON_BARS,
        count(*) as N_TOTAL,
        count_if(o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null) as N_SUCCESS,
        count_if(o.EVAL_STATUS in ('PENDING', 'INSUFFICIENT_FUTURE_DATA', 'INSUFFICIENT_DATA')) as N_PENDING,
        count_if(o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null) / nullif(count(*), 0) as COVERAGE_RATE,
        avg(case
            when o.EVAL_STATUS = 'SUCCESS'
             and o.REALIZED_RETURN is not null
            then o.REALIZED_RETURN
        end) as AVG_RETURN,
        median(case
            when o.EVAL_STATUS = 'SUCCESS'
             and o.REALIZED_RETURN is not null
            then o.REALIZED_RETURN
        end) as MEDIAN_RETURN,
        avg(case
            when o.EVAL_STATUS = 'SUCCESS'
             and o.REALIZED_RETURN is not null
             and o.HIT_FLAG is not null
            then iff(o.HIT_FLAG, 1, 0)
        end) as HIT_RATE,
        case
            when count_if(o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null) >= 10
            then corr(
                case
                    when o.EVAL_STATUS = 'SUCCESS'
                     and o.REALIZED_RETURN is not null
                    then r.SCORE
                end,
                case
                    when o.EVAL_STATUS = 'SUCCESS'
                     and o.REALIZED_RETURN is not null
                    then o.REALIZED_RETURN
                end
            )
            else null
        end as SCORE_RETURN_CORR
    from MIP.APP.RECOMMENDATION_OUTCOMES o
    join MIP.APP.RECOMMENDATION_LOG r
      on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
    where r.INTERVAL_MINUTES = 1440
      and o.ENTRY_TS < current_date()
    group by
        r.PATTERN_ID,
        r.MARKET_TYPE,
        r.INTERVAL_MINUTES,
        o.HORIZON_BARS
),
previous_policy as (
    select
        PATTERN_ID,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        HORIZON_BARS,
        case
            when N_SUCCESS >= 30
             and COVERAGE_RATE >= 0.8
             and (AVG_RETURN > 0 or MEDIAN_RETURN > 0)
            then 'TRUSTED'
            when N_SUCCESS >= 30
             and COVERAGE_RATE >= 0.8
            then 'WATCH'
            else 'UNTRUSTED'
        end as TRUST_LABEL,
        case
            when N_SUCCESS >= 30
             and COVERAGE_RATE >= 0.8
             and (AVG_RETURN > 0 or MEDIAN_RETURN > 0)
            then 'ENABLE'
            when N_SUCCESS >= 30
             and COVERAGE_RATE >= 0.8
            then 'MONITOR'
            else 'DISABLE'
        end as RECOMMENDED_ACTION
    from previous_kpis
),
scored as (
    select
        c.PATTERN_ID,
        c.MARKET_TYPE,
        c.INTERVAL_MINUTES,
        c.HORIZON_BARS,
        c.TRUST_LABEL,
        c.RECOMMENDED_ACTION,
        c.REASON,
        p.TRUST_LABEL as PREVIOUS_TRUST_LABEL,
        p.RECOMMENDED_ACTION as PREVIOUS_RECOMMENDED_ACTION,
        case c.TRUST_LABEL
            when 'TRUSTED' then 3
            when 'WATCH' then 2
            else 1
        end as TRUST_RANK,
        case p.TRUST_LABEL
            when 'TRUSTED' then 3
            when 'WATCH' then 2
            else 1
        end as PREVIOUS_TRUST_RANK,
        c.REASON:avg_return::float as AVG_RETURN
    from current_policy c
    left join previous_policy p
      on p.PATTERN_ID = c.PATTERN_ID
     and p.MARKET_TYPE = c.MARKET_TYPE
     and p.INTERVAL_MINUTES = c.INTERVAL_MINUTES
     and p.HORIZON_BARS = c.HORIZON_BARS
)
select
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    TRUST_LABEL,
    RECOMMENDED_ACTION,
    PREVIOUS_TRUST_LABEL,
    PREVIOUS_RECOMMENDED_ACTION,
    REASON,
    'NEWLY_TRUSTED' as BRIEF_CATEGORY,
    current_timestamp() as AS_OF_TS
from scored
where TRUST_LABEL = 'TRUSTED'
  and coalesce(PREVIOUS_TRUST_LABEL, 'UNTRUSTED') != 'TRUSTED'

union all

select
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    TRUST_LABEL,
    RECOMMENDED_ACTION,
    PREVIOUS_TRUST_LABEL,
    PREVIOUS_RECOMMENDED_ACTION,
    REASON,
    'DOWNGRADED' as BRIEF_CATEGORY,
    current_timestamp() as AS_OF_TS
from scored
where PREVIOUS_TRUST_LABEL is not null
  and TRUST_RANK < PREVIOUS_TRUST_RANK

union all

select
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    TRUST_LABEL,
    RECOMMENDED_ACTION,
    PREVIOUS_TRUST_LABEL,
    PREVIOUS_RECOMMENDED_ACTION,
    REASON,
    'WATCH_NEGATIVE_RETURN' as BRIEF_CATEGORY,
    current_timestamp() as AS_OF_TS
from scored
where TRUST_LABEL = 'WATCH'
  and AVG_RETURN < 0;
