-- 234_intraday_ui_smoke_checks.sql
-- Smoke checks for intraday UI-facing data contracts.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) KPI/series reconciliation (signals 30d).
with kpi as (
    select count(*) as SIGNALS_30D
    from MIP.MART.V_INTRADAY_UI_SIGNALS
    where SIGNAL_DATE >= dateadd(day, -30, current_date())
),
series as (
    select sum(SIGNALS_TOTAL) as SIGNALS_30D_SERIES
    from (
        select SIGNAL_DATE, count(*) as SIGNALS_TOTAL
        from MIP.MART.V_INTRADAY_UI_SIGNALS
        where SIGNAL_DATE >= dateadd(day, -30, current_date())
        group by SIGNAL_DATE
    )
)
select
    'signals_reconciliation' as CHECK_NAME,
    k.SIGNALS_30D as KPI_VALUE,
    s.SIGNALS_30D_SERIES as SERIES_VALUE,
    (k.SIGNALS_30D = s.SIGNALS_30D_SERIES) as PASS
from kpi k
cross join series s;

-- 2) Deterministic trust snapshot selection check.
with as_of_input as (
    select current_timestamp() as AS_OF_TS
),
snapshot_rows_a as (
    select *
    from MIP.MART.V_INTRADAY_UI_TRUST t
    join as_of_input a on 1 = 1
    where t.CALCULATED_AT <= a.AS_OF_TS
    qualify row_number() over (
        partition by t.PATTERN_ID, t.MARKET_TYPE, t.INTERVAL_MINUTES, t.HORIZON_BARS, t.STATE_BUCKET_ID
        order by t.CALCULATED_AT desc, t.TRAIN_WINDOW_END desc
    ) = 1
),
snapshot_rows_b as (
    select *
    from MIP.MART.V_INTRADAY_UI_TRUST t
    join as_of_input a on 1 = 1
    where t.CALCULATED_AT <= a.AS_OF_TS
    qualify row_number() over (
        partition by t.PATTERN_ID, t.MARKET_TYPE, t.INTERVAL_MINUTES, t.HORIZON_BARS, t.STATE_BUCKET_ID
        order by t.CALCULATED_AT desc, t.TRAIN_WINDOW_END desc
    ) = 1
),
snapshot_a as (
    select
        count(*) as ROWS_A,
        hash_agg(
            concat_ws(
                '|',
                PATTERN_ID::varchar,
                MARKET_TYPE,
                HORIZON_BARS::varchar,
                coalesce(STATE_BUCKET_ID::varchar, 'NULL'),
                coalesce(FALLBACK_LEVEL, 'NULL'),
                coalesce(N_SIGNALS::varchar, 'NULL')
            )
        ) as HASH_A
    from snapshot_rows_a
),
snapshot_b as (
    select
        count(*) as ROWS_B,
        hash_agg(
            concat_ws(
                '|',
                PATTERN_ID::varchar,
                MARKET_TYPE,
                HORIZON_BARS::varchar,
                coalesce(STATE_BUCKET_ID::varchar, 'NULL'),
                coalesce(FALLBACK_LEVEL, 'NULL'),
                coalesce(N_SIGNALS::varchar, 'NULL')
            )
        ) as HASH_B
    from snapshot_rows_b
)
select
    'snapshot_determinism' as CHECK_NAME,
    a.ROWS_A,
    b.ROWS_B,
    (a.HASH_A = b.HASH_B) as PASS
from snapshot_a a
cross join snapshot_b b;

-- 3) Terrain dispersion sanity (non-degenerate).
select
    'terrain_dispersion' as CHECK_NAME,
    count(distinct TERRAIN_SCORE) as DISTINCT_TERRAIN_SCORES,
    stddev(TERRAIN_SCORE) as TERRAIN_STDDEV,
    (count(distinct TERRAIN_SCORE) > 10 and stddev(TERRAIN_SCORE) > 0) as PASS
from MIP.MART.V_INTRADAY_UI_TERRAIN
where CALCULATED_DATE >= dateadd(day, -30, current_date());
