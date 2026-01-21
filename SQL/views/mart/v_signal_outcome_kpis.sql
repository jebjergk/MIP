-- v_signal_outcome_kpis.sql
-- Purpose: Signal/outcome KPIs by pattern and horizon

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_SIGNAL_OUTCOME_KPIS as
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
    stddev_samp(case
        when o.EVAL_STATUS = 'SUCCESS'
         and o.REALIZED_RETURN is not null
        then o.REALIZED_RETURN
    end) as STDDEV_RETURN,
    min(case
        when o.EVAL_STATUS = 'SUCCESS'
         and o.REALIZED_RETURN is not null
        then o.REALIZED_RETURN
    end) as MIN_RETURN,
    max(case
        when o.EVAL_STATUS = 'SUCCESS'
         and o.REALIZED_RETURN is not null
        then o.REALIZED_RETURN
    end) as MAX_RETURN,
    avg(case
        when o.EVAL_STATUS = 'SUCCESS'
         and o.REALIZED_RETURN is not null
         and o.HIT_FLAG is not null
        then iff(o.HIT_FLAG, 1, 0)
    end) as HIT_RATE,
    avg(case
        when o.EVAL_STATUS = 'SUCCESS'
         and o.REALIZED_RETURN is not null
         and o.REALIZED_RETURN > 0
        then o.REALIZED_RETURN
    end) as AVG_WIN,
    avg(case
        when o.EVAL_STATUS = 'SUCCESS'
         and o.REALIZED_RETURN is not null
         and o.REALIZED_RETURN < 0
        then o.REALIZED_RETURN
    end) as AVG_LOSS,
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
    end as SCORE_RETURN_CORR,
    min(case when o.EVAL_STATUS != 'SUCCESS' then o.ENTRY_TS end) as OLDEST_NOT_READY_ENTRY_TS,
    max(case when o.EVAL_STATUS != 'SUCCESS' then o.ENTRY_TS end) as NEWEST_NOT_READY_ENTRY_TS,
    max(case when o.EVAL_STATUS = 'SUCCESS' then o.ENTRY_TS end) as LATEST_MATURED_ENTRY_TS
from MIP.APP.RECOMMENDATION_OUTCOMES o
join MIP.APP.RECOMMENDATION_LOG r
  on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
where r.INTERVAL_MINUTES = 1440
group by
    r.PATTERN_ID,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    o.HORIZON_BARS;
