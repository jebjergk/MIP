-- v_intraday_outcomes_fee_adjusted.sql
-- Purpose: Fee-adjusted outcomes for intraday recommendations.
-- Deducts round-trip costs from REALIZED_RETURN to compute NET_RETURN.
-- Fee config comes from INTRADAY_FEE_CONFIG (active profile).

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_INTRADAY_OUTCOMES_FEE_ADJUSTED as
with fee_config as (
    select
        FEE_BPS,
        SLIPPAGE_BPS,
        SPREAD_BPS,
        (2 * SLIPPAGE_BPS + 2 * FEE_BPS + SPREAD_BPS) / 10000.0 as ROUND_TRIP_COST
    from MIP.APP.INTRADAY_FEE_CONFIG
    where IS_ACTIVE
    qualify row_number() over (order by FEE_PROFILE) = 1
)
select
    o.RECOMMENDATION_ID,
    o.HORIZON_BARS,
    r.PATTERN_ID,
    r.SYMBOL,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    r.TS as SIGNAL_TS,
    r.SCORE,
    o.ENTRY_TS,
    o.EXIT_TS,
    o.ENTRY_PRICE,
    o.EXIT_PRICE,
    o.REALIZED_RETURN as GROSS_RETURN,
    f.ROUND_TRIP_COST,
    o.REALIZED_RETURN - f.ROUND_TRIP_COST as NET_RETURN,
    o.REALIZED_RETURN - f.ROUND_TRIP_COST >= 0 as NET_HIT_FLAG,
    o.HIT_FLAG as GROSS_HIT_FLAG,
    o.EVAL_STATUS,
    o.CALCULATED_AT,
    o.MAX_FAVORABLE_EXCURSION,
    o.MAX_ADVERSE_EXCURSION,
    f.FEE_BPS,
    f.SLIPPAGE_BPS,
    f.SPREAD_BPS
from MIP.APP.RECOMMENDATION_OUTCOMES o
join MIP.APP.RECOMMENDATION_LOG r
  on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
cross join fee_config f
where r.INTERVAL_MINUTES != 1440;
