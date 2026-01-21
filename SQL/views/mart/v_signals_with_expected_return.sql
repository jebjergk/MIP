-- v_signals_with_expected_return.sql
-- Purpose: Attach expected return from score calibration to recommendations

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_SIGNALS_WITH_EXPECTED_RETURN as
with scored as (
    select
        r.RECOMMENDATION_ID,
        r.PATTERN_ID,
        r.MARKET_TYPE,
        r.INTERVAL_MINUTES,
        o.HORIZON_BARS,
        r.SCORE,
        ntile(10) over (
            partition by r.PATTERN_ID, r.MARKET_TYPE, r.INTERVAL_MINUTES, o.HORIZON_BARS
            order by r.SCORE
        ) as SCORE_DECILE
    from MIP.APP.RECOMMENDATION_OUTCOMES o
    join MIP.APP.RECOMMENDATION_LOG r
      on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
    where r.SCORE is not null
      and o.HORIZON_BARS is not null
)
select
    s.RECOMMENDATION_ID,
    s.PATTERN_ID,
    s.MARKET_TYPE,
    s.INTERVAL_MINUTES,
    s.HORIZON_BARS,
    s.SCORE,
    s.SCORE_DECILE,
    c.AVG_RETURN as EXPECTED_RETURN
from scored s
left join MIP.MART.V_SCORE_CALIBRATION c
  on c.PATTERN_ID = s.PATTERN_ID
 and c.MARKET_TYPE = s.MARKET_TYPE
 and c.INTERVAL_MINUTES = s.INTERVAL_MINUTES
 and c.HORIZON_BARS = s.HORIZON_BARS
 and c.SCORE_DECILE = s.SCORE_DECILE;
