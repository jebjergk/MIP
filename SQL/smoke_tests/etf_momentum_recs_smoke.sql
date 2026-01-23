-- etf_momentum_recs_smoke.sql
-- Smoke test for ETF momentum recommendations

merge into MIP.APP.PATTERN_DEFINITION t
using (
    select
        'ETF_MOMENTUM_SMOKE' as NAME,
        'Smoke test ETF momentum pattern' as DESCRIPTION,
        object_construct(
            'fast_window', 20,
            'slow_window', 3,
            'lookback_days', 90,
            'min_return', 0.0,
            'min_zscore', 1.0,
            'market_type', 'ETF',
            'interval_minutes', 1440
        ) as PARAMS_JSON,
        'Y' as IS_ACTIVE,
        true as ENABLED
) s
on t.NAME = s.NAME
when matched then update set
    t.DESCRIPTION = s.DESCRIPTION,
    t.PARAMS_JSON = s.PARAMS_JSON,
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.ENABLED = s.ENABLED,
    t.UPDATED_AT = current_timestamp(),
    t.UPDATED_BY = current_user()
when not matched then insert (
    NAME,
    DESCRIPTION,
    PARAMS_JSON,
    IS_ACTIVE,
    ENABLED
) values (
    s.NAME,
    s.DESCRIPTION,
    s.PARAMS_JSON,
    s.IS_ACTIVE,
    s.ENABLED
);

call MIP.APP.SP_GENERATE_MOMENTUM_RECS(0.0, 'ETF', 1440, 90, null);

with latest_returns as (
    select max(TS) as latest_ts
    from MIP.MART.MARKET_RETURNS
    where MARKET_TYPE = 'ETF'
      and INTERVAL_MINUTES = 1440
), recs as (
    select count(*) as rec_count
    from MIP.APP.RECOMMENDATION_LOG r
    join latest_returns l
      on r.TS = l.latest_ts
    where r.MARKET_TYPE = 'ETF'
      and r.INTERVAL_MINUTES = 1440
)
select
    latest_returns.latest_ts as latest_returns_ts,
    recs.rec_count as recs_at_returns_ts
from latest_returns, recs;
