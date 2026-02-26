-- 232_intraday_v2_stage3b_dryrun_stage1.sql
-- Purpose: Stage 1 dry-run signal generation for:
--   - ORB_PROTO_RELAXED (stock/etf/fx)
--   - STATE_CHOP_TO_TREND_ENTRY
-- Notes:
--   - Existing production patterns remain unchanged.
--   - Proto patterns are inserted as disabled/inactive definitions.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Seed/refresh Stage 1 proto pattern definitions (disabled).
merge into MIP.APP.PATTERN_DEFINITION t
using (
    select
        'ORB_PROTO_RELAXED_STOCK_15MIN' as NAME,
        'ORB' as PATTERN_TYPE,
        'Stage1 dry-run ORB proto with relaxed thresholds (STOCK).' as DESCRIPTION,
        object_construct(
            'pattern_type', 'ORB',
            'interval_minutes', 15,
            'market_type', 'STOCK',
            'range_bars', 2,
            'breakout_buffer_pct', 0.0007,
            'min_range_pct', 0.0022,
            'session_start_hour_utc', 10,
            'direction', 'BOTH',
            'proto_stage', '3b_stage1',
            'dry_run', true
        ) as PARAMS_JSON,
        'N' as IS_ACTIVE,
        false as ENABLED
    union all
    select
        'ORB_PROTO_RELAXED_ETF_15MIN',
        'ORB',
        'Stage1 dry-run ORB proto with relaxed thresholds (ETF).',
        object_construct(
            'pattern_type', 'ORB',
            'interval_minutes', 15,
            'market_type', 'ETF',
            'range_bars', 2,
            'breakout_buffer_pct', 0.0007,
            'min_range_pct', 0.0015,
            'session_start_hour_utc', 10,
            'direction', 'BOTH',
            'proto_stage', '3b_stage1',
            'dry_run', true
        ),
        'N',
        false
    union all
    select
        'ORB_PROTO_RELAXED_FX_15MIN',
        'ORB',
        'Stage1 dry-run ORB proto with relaxed thresholds (FX).',
        object_construct(
            'pattern_type', 'ORB',
            'interval_minutes', 15,
            'market_type', 'FX',
            'range_bars', 2,
            'breakout_buffer_pct', 0.00035,
            'min_range_pct', 0.0007,
            'session_start_hour_utc', 14,
            'direction', 'BOTH',
            'proto_stage', '3b_stage1',
            'dry_run', true
        ),
        'N',
        false
    union all
    select
        'STATE_CHOP_TO_TREND_ENTRY_15MIN',
        'MEAN_REVERSION',
        'Stage1 dry-run state-transition trigger (CHOP -> TREND).',
        object_construct(
            'pattern_type', 'STATE_CHOP_TO_TREND',
            'interval_minutes', 15,
            'market_type', 'ALL',
            'cooldown_bars', 8,
            'direction', 'BOTH',
            'proto_stage', '3b_stage1',
            'dry_run', true
        ),
        'N',
        false
) s
on t.NAME = s.NAME
when matched then update set
    t.PATTERN_TYPE = s.PATTERN_TYPE,
    t.DESCRIPTION = s.DESCRIPTION,
    t.PARAMS_JSON = s.PARAMS_JSON,
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.ENABLED = s.ENABLED
when not matched then insert (
    NAME, PATTERN_TYPE, DESCRIPTION, PARAMS_JSON, IS_ACTIVE, ENABLED
) values (
    s.NAME, s.PATTERN_TYPE, s.DESCRIPTION, s.PARAMS_JSON, s.IS_ACTIVE, s.ENABLED
);

-- 2) Ensure proto defs exist in intraday v2 registry.
merge into MIP.APP.INTRA_PATTERN_DEFS t
using (
    select
        p.PATTERN_ID,
        p.NAME as PATTERN_NAME,
        'v1' as VERSION,
        'PROTO_DRYRUN' as PATTERN_FAMILY,
        p.PATTERN_TYPE,
        p.PARAMS_JSON,
        false as IS_ENABLED
    from MIP.APP.PATTERN_DEFINITION p
    where p.NAME in (
        'ORB_PROTO_RELAXED_STOCK_15MIN',
        'ORB_PROTO_RELAXED_ETF_15MIN',
        'ORB_PROTO_RELAXED_FX_15MIN',
        'STATE_CHOP_TO_TREND_ENTRY_15MIN'
    )
) s
on t.PATTERN_ID = s.PATTERN_ID
when matched then update set
    t.PATTERN_NAME = s.PATTERN_NAME,
    t.PATTERN_FAMILY = s.PATTERN_FAMILY,
    t.PATTERN_TYPE = s.PATTERN_TYPE,
    t.PARAMS_JSON = s.PARAMS_JSON,
    t.IS_ENABLED = s.IS_ENABLED,
    t.UPDATED_AT = current_timestamp(),
    t.UPDATED_BY = current_user()
when not matched then insert (
    PATTERN_ID, PATTERN_NAME, VERSION, PATTERN_FAMILY, PATTERN_TYPE, PARAMS_JSON, IS_ENABLED
) values (
    s.PATTERN_ID, s.PATTERN_NAME, s.VERSION, s.PATTERN_FAMILY, s.PATTERN_TYPE, s.PARAMS_JSON, s.IS_ENABLED
);

-- 3) ORB_PROTO_RELAXED dry-run signals into RECOMMENDATION_LOG.
insert into MIP.APP.RECOMMENDATION_LOG (
    PATTERN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, TS, SCORE, DETAILS
)
with bounds as (
    select
        dateadd(month, -6, max(TS)) as START_TS,
        max(TS) as END_TS
    from MIP.MART.MARKET_BARS
    where INTERVAL_MINUTES = 15
),
proto_orb as (
    select
        p.PATTERN_ID,
        p.NAME,
        upper(coalesce(p.PARAMS_JSON:market_type::string, 'STOCK')) as MARKET_TYPE,
        coalesce(p.PARAMS_JSON:range_bars::number, 2) as RANGE_BARS,
        coalesce(p.PARAMS_JSON:breakout_buffer_pct::float, 0.001) as BREAKOUT_BUFFER_PCT,
        coalesce(p.PARAMS_JSON:min_range_pct::float, 0.002) as MIN_RANGE_PCT,
        coalesce(p.PARAMS_JSON:session_start_hour_utc::number, 10) as SESSION_START_HOUR_UTC,
        coalesce(p.PARAMS_JSON:direction::string, 'BOTH') as DIRECTION
    from MIP.APP.PATTERN_DEFINITION p
    where p.NAME in (
        'ORB_PROTO_RELAXED_STOCK_15MIN',
        'ORB_PROTO_RELAXED_ETF_15MIN',
        'ORB_PROTO_RELAXED_FX_15MIN'
    )
),
session_bars as (
    select
        po.PATTERN_ID,
        po.NAME,
        po.MARKET_TYPE,
        po.RANGE_BARS,
        po.BREAKOUT_BUFFER_PCT,
        po.MIN_RANGE_PCT,
        po.SESSION_START_HOUR_UTC,
        po.DIRECTION,
        b.SYMBOL,
        b.TS,
        b.OPEN,
        b.HIGH,
        b.LOW,
        b.CLOSE,
        b.VOLUME,
        b.TS::date as SESSION_DATE,
        hour(b.TS) as BAR_HOUR,
        row_number() over (
            partition by po.PATTERN_ID, b.SYMBOL, b.TS::date
            order by b.TS
        ) as SESSION_BAR_NUM
    from proto_orb po
    join MIP.MART.MARKET_BARS b
      on b.MARKET_TYPE = po.MARKET_TYPE
     and b.INTERVAL_MINUTES = 15
    join bounds x
      on b.TS between x.START_TS and x.END_TS
    where hour(b.TS) >= po.SESSION_START_HOUR_UTC
),
opening_range as (
    select
        PATTERN_ID,
        SYMBOL,
        MARKET_TYPE,
        SESSION_DATE,
        max(HIGH) as RANGE_HIGH,
        min(LOW) as RANGE_LOW,
        min(OPEN) as SESSION_OPEN,
        max(HIGH) - min(LOW) as RANGE_SIZE
    from session_bars
    where SESSION_BAR_NUM <= RANGE_BARS
    group by 1,2,3,4
    having RANGE_SIZE > 0
),
breakouts as (
    select
        sb.PATTERN_ID,
        sb.SYMBOL,
        sb.MARKET_TYPE,
        sb.TS,
        sb.CLOSE,
        sb.VOLUME,
        sb.SESSION_BAR_NUM,
        sb.BAR_HOUR,
        sb.DIRECTION as CFG_DIRECTION,
        sb.BREAKOUT_BUFFER_PCT,
        sb.MIN_RANGE_PCT,
        orng.RANGE_HIGH,
        orng.RANGE_LOW,
        orng.RANGE_SIZE,
        orng.SESSION_OPEN,
        orng.RANGE_SIZE / nullif(orng.SESSION_OPEN, 0) as RANGE_PCT,
        case
            when sb.CLOSE > orng.RANGE_HIGH * (1 + sb.BREAKOUT_BUFFER_PCT) then 'BULLISH'
            when sb.CLOSE < orng.RANGE_LOW * (1 - sb.BREAKOUT_BUFFER_PCT) then 'BEARISH'
        end as SIGNAL_DIRECTION,
        case
            when sb.CLOSE > orng.RANGE_HIGH * (1 + sb.BREAKOUT_BUFFER_PCT)
                then (sb.CLOSE - orng.RANGE_HIGH) / nullif(orng.RANGE_HIGH, 0)
            when sb.CLOSE < orng.RANGE_LOW * (1 - sb.BREAKOUT_BUFFER_PCT)
                then (orng.RANGE_LOW - sb.CLOSE) / nullif(orng.RANGE_LOW, 0)
        end as BREAKOUT_DISTANCE_PCT
    from session_bars sb
    join opening_range orng
      on orng.PATTERN_ID = sb.PATTERN_ID
     and orng.SYMBOL = sb.SYMBOL
     and orng.MARKET_TYPE = sb.MARKET_TYPE
     and orng.SESSION_DATE = sb.SESSION_DATE
    where sb.SESSION_BAR_NUM > sb.RANGE_BARS
      and orng.RANGE_SIZE / nullif(orng.SESSION_OPEN, 0) >= sb.MIN_RANGE_PCT
      and (
            sb.CLOSE > orng.RANGE_HIGH * (1 + sb.BREAKOUT_BUFFER_PCT)
            or sb.CLOSE < orng.RANGE_LOW * (1 - sb.BREAKOUT_BUFFER_PCT)
      )
),
first_breakout as (
    select *
    from breakouts
    where SIGNAL_DIRECTION is not null
      and (CFG_DIRECTION = 'BOTH' or SIGNAL_DIRECTION = CFG_DIRECTION)
    qualify row_number() over (
        partition by PATTERN_ID, SYMBOL, MARKET_TYPE, TS::date
        order by TS
    ) = 1
)
select
    fb.PATTERN_ID,
    fb.SYMBOL,
    fb.MARKET_TYPE,
    15 as INTERVAL_MINUTES,
    fb.TS,
    fb.BREAKOUT_DISTANCE_PCT as SCORE,
    object_construct(
        'pattern_type', 'ORB',
        'proto_variant', 'ORB_PROTO_RELAXED',
        'dry_run_stage', '3b_stage1',
        'direction', fb.SIGNAL_DIRECTION,
        'opening_range_high', fb.RANGE_HIGH,
        'opening_range_low', fb.RANGE_LOW,
        'range_pct', fb.RANGE_PCT,
        'breakout_buffer_pct', fb.BREAKOUT_BUFFER_PCT,
        'min_range_pct', fb.MIN_RANGE_PCT,
        'breakout_distance_pct', fb.BREAKOUT_DISTANCE_PCT,
        'time_bucket', case
            when fb.BAR_HOUR < 16 then 'MORNING'
            when fb.BAR_HOUR < 18 then 'MIDDAY'
            else 'AFTERNOON'
        end,
        'volume', fb.VOLUME
    ) as DETAILS
from first_breakout fb
where not exists (
    select 1
    from MIP.APP.RECOMMENDATION_LOG r
    where r.PATTERN_ID = fb.PATTERN_ID
      and r.SYMBOL = fb.SYMBOL
      and r.MARKET_TYPE = fb.MARKET_TYPE
      and r.INTERVAL_MINUTES = 15
      and r.TS = fb.TS
);

-- 4) STATE_CHOP_TO_TREND_ENTRY dry-run signals into RECOMMENDATION_LOG.
insert into MIP.APP.RECOMMENDATION_LOG (
    PATTERN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, TS, SCORE, DETAILS
)
with bounds as (
    select
        dateadd(month, -6, max(TS_TO)) as START_TS,
        max(TS_TO) as END_TS
    from MIP.APP.STATE_TRANSITIONS
    where INTERVAL_MINUTES = 15
),
pid as (
    select PATTERN_ID
    from MIP.APP.PATTERN_DEFINITION
    where NAME = 'STATE_CHOP_TO_TREND_ENTRY_15MIN'
),
candidates as (
    select
        (select PATTERN_ID from pid) as PATTERN_ID,
        st.SYMBOL,
        st.MARKET_TYPE,
        st.TS_TO as TS,
        st.FROM_STATE_BUCKET_ID,
        st.TO_STATE_BUCKET_ID,
        st.DURATION_BARS,
        ss.BELIEF_STRENGTH,
        case
            when st.TO_STATE_BUCKET_ID like 'UP_TREND%' then 'BULLISH'
            when st.TO_STATE_BUCKET_ID like 'DOWN_TREND%' then 'BEARISH'
            else null
        end as SIGNAL_DIRECTION
    from MIP.APP.STATE_TRANSITIONS st
    join MIP.APP.STATE_SNAPSHOT_15M ss
      on ss.MARKET_TYPE = st.MARKET_TYPE
     and ss.SYMBOL = st.SYMBOL
     and ss.INTERVAL_MINUTES = st.INTERVAL_MINUTES
     and ss.TS = st.TS_TO
     and ss.METRIC_VERSION = st.METRIC_VERSION
     and ss.BUCKET_VERSION = st.BUCKET_VERSION
    join bounds b
      on st.TS_TO between b.START_TS and b.END_TS
    where st.INTERVAL_MINUTES = 15
      and st.METRIC_VERSION = 'v1_1'
      and st.BUCKET_VERSION = 'v1'
      and st.FROM_STATE_BUCKET_ID like '%CHOP%'
      and st.TO_STATE_BUCKET_ID like '%TREND%'
),
cooldown_filtered as (
    select *
    from candidates
    where SIGNAL_DIRECTION is not null
    qualify
        lag(TS) over (partition by SYMBOL, MARKET_TYPE order by TS) is null
        or datediff(
            minute,
            lag(TS) over (partition by SYMBOL, MARKET_TYPE order by TS),
            TS
        ) >= 120
)
select
    c.PATTERN_ID,
    c.SYMBOL,
    c.MARKET_TYPE,
    15 as INTERVAL_MINUTES,
    c.TS,
    coalesce(c.BELIEF_STRENGTH, 0) as SCORE,
    object_construct(
        'pattern_type', 'STATE_CHOP_TO_TREND',
        'proto_variant', 'STATE_CHOP_TO_TREND_ENTRY',
        'dry_run_stage', '3b_stage1',
        'direction', c.SIGNAL_DIRECTION,
        'from_state_bucket_id', c.FROM_STATE_BUCKET_ID,
        'to_state_bucket_id', c.TO_STATE_BUCKET_ID,
        'duration_bars', c.DURATION_BARS,
        'cooldown_bars', 8,
        'belief_strength', c.BELIEF_STRENGTH
    ) as DETAILS
from cooldown_filtered c
where not exists (
    select 1
    from MIP.APP.RECOMMENDATION_LOG r
    where r.PATTERN_ID = c.PATTERN_ID
      and r.SYMBOL = c.SYMBOL
      and r.MARKET_TYPE = c.MARKET_TYPE
      and r.INTERVAL_MINUTES = 15
      and r.TS = c.TS
);

-- 5) Return seeded Stage 1 pattern IDs.
select
    PATTERN_ID,
    NAME
from MIP.APP.PATTERN_DEFINITION
where NAME in (
    'ORB_PROTO_RELAXED_STOCK_15MIN',
    'ORB_PROTO_RELAXED_ETF_15MIN',
    'ORB_PROTO_RELAXED_FX_15MIN',
    'STATE_CHOP_TO_TREND_ENTRY_15MIN'
)
order by NAME;

