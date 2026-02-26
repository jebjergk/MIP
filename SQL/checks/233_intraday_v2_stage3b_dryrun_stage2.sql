-- 233_intraday_v2_stage3b_dryrun_stage2.sql
-- Purpose: Stage 2 dry-run signal generation for ORB_PROTO_RETRIGGER variants.
-- Rule: retrigger requires (state change OR volatility expansion), with hard cap 2 signals
--       per symbol per session/day.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Seed/refresh Stage 2 proto pattern definitions (disabled).
merge into MIP.APP.PATTERN_DEFINITION t
using (
    select
        'ORB_PROTO_RETRIGGER_STOCK_15MIN' as NAME,
        'ORB' as PATTERN_TYPE,
        'Stage2 dry-run ORB retrigger proto (STOCK): state change OR vol expansion; cap 2/day.' as DESCRIPTION,
        object_construct(
            'pattern_type', 'ORB',
            'interval_minutes', 15,
            'market_type', 'STOCK',
            'range_bars', 2,
            'breakout_buffer_pct', 0.0010,
            'min_range_pct', 0.0030,
            'session_start_hour_utc', 10,
            'direction', 'BOTH',
            'retrigger_rule', 'STATE_CHANGE_OR_VOL_EXPANSION',
            'vol_expansion_mult', 1.30,
            'max_signals_per_session', 2,
            'proto_stage', '3b_stage2',
            'dry_run', true
        ) as PARAMS_JSON,
        'N' as IS_ACTIVE,
        false as ENABLED
    union all
    select
        'ORB_PROTO_RETRIGGER_ETF_15MIN',
        'ORB',
        'Stage2 dry-run ORB retrigger proto (ETF): state change OR vol expansion; cap 2/day.',
        object_construct(
            'pattern_type', 'ORB',
            'interval_minutes', 15,
            'market_type', 'ETF',
            'range_bars', 2,
            'breakout_buffer_pct', 0.0010,
            'min_range_pct', 0.0020,
            'session_start_hour_utc', 10,
            'direction', 'BOTH',
            'retrigger_rule', 'STATE_CHANGE_OR_VOL_EXPANSION',
            'vol_expansion_mult', 1.30,
            'max_signals_per_session', 2,
            'proto_stage', '3b_stage2',
            'dry_run', true
        ),
        'N',
        false
    union all
    select
        'ORB_PROTO_RETRIGGER_FX_15MIN',
        'ORB',
        'Stage2 dry-run ORB retrigger proto (FX): state change OR vol expansion; cap 2/day.',
        object_construct(
            'pattern_type', 'ORB',
            'interval_minutes', 15,
            'market_type', 'FX',
            'range_bars', 2,
            'breakout_buffer_pct', 0.0005,
            'min_range_pct', 0.0010,
            'session_start_hour_utc', 14,
            'direction', 'BOTH',
            'retrigger_rule', 'STATE_CHANGE_OR_VOL_EXPANSION',
            'vol_expansion_mult', 1.25,
            'max_signals_per_session', 2,
            'proto_stage', '3b_stage2',
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

-- 2) Ensure Stage 2 defs exist in intraday v2 registry.
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
        'ORB_PROTO_RETRIGGER_STOCK_15MIN',
        'ORB_PROTO_RETRIGGER_ETF_15MIN',
        'ORB_PROTO_RETRIGGER_FX_15MIN'
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

-- 3) Stage 2 ORB retrigger dry-run signals into RECOMMENDATION_LOG.
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
proto as (
    select
        p.PATTERN_ID,
        p.NAME,
        upper(coalesce(p.PARAMS_JSON:market_type::string, 'STOCK')) as MARKET_TYPE,
        coalesce(p.PARAMS_JSON:range_bars::number, 2) as RANGE_BARS,
        coalesce(p.PARAMS_JSON:breakout_buffer_pct::float, 0.001) as BREAKOUT_BUFFER_PCT,
        coalesce(p.PARAMS_JSON:min_range_pct::float, 0.002) as MIN_RANGE_PCT,
        coalesce(p.PARAMS_JSON:session_start_hour_utc::number, 10) as SESSION_START_HOUR_UTC,
        coalesce(p.PARAMS_JSON:direction::string, 'BOTH') as DIRECTION,
        coalesce(p.PARAMS_JSON:vol_expansion_mult::float, 1.30) as VOL_EXPANSION_MULT,
        coalesce(p.PARAMS_JSON:max_signals_per_session::number, 2) as MAX_SIGNALS_PER_SESSION
    from MIP.APP.PATTERN_DEFINITION p
    where p.NAME in (
        'ORB_PROTO_RETRIGGER_STOCK_15MIN',
        'ORB_PROTO_RETRIGGER_ETF_15MIN',
        'ORB_PROTO_RETRIGGER_FX_15MIN'
    )
),
session_bars as (
    select
        pr.PATTERN_ID,
        pr.NAME,
        pr.MARKET_TYPE,
        pr.RANGE_BARS,
        pr.BREAKOUT_BUFFER_PCT,
        pr.MIN_RANGE_PCT,
        pr.SESSION_START_HOUR_UTC,
        pr.DIRECTION,
        pr.VOL_EXPANSION_MULT,
        pr.MAX_SIGNALS_PER_SESSION,
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
            partition by pr.PATTERN_ID, b.SYMBOL, b.TS::date
            order by b.TS
        ) as SESSION_BAR_NUM,
        (b.HIGH - b.LOW) / nullif(b.CLOSE, 0) as BAR_RANGE_PCT
    from proto pr
    join MIP.MART.MARKET_BARS b
      on b.MARKET_TYPE = pr.MARKET_TYPE
     and b.INTERVAL_MINUTES = 15
    join bounds x
      on b.TS between x.START_TS and x.END_TS
    where hour(b.TS) >= pr.SESSION_START_HOUR_UTC
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
breakouts_raw as (
    select
        sb.PATTERN_ID,
        sb.NAME,
        sb.SYMBOL,
        sb.MARKET_TYPE,
        sb.TS,
        sb.CLOSE,
        sb.VOLUME,
        sb.SESSION_DATE,
        sb.SESSION_BAR_NUM,
        sb.BAR_HOUR,
        sb.DIRECTION as CFG_DIRECTION,
        sb.BREAKOUT_BUFFER_PCT,
        sb.MIN_RANGE_PCT,
        sb.VOL_EXPANSION_MULT,
        sb.MAX_SIGNALS_PER_SESSION,
        sb.BAR_RANGE_PCT,
        avg(sb.BAR_RANGE_PCT) over (
            partition by sb.PATTERN_ID, sb.SYMBOL, sb.SESSION_DATE
            order by sb.TS
            rows between 8 preceding and 1 preceding
        ) as PREV_AVG_RANGE_PCT,
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
with_state as (
    select
        br.*,
        ss.STATE_BUCKET_ID
    from breakouts_raw br
    join MIP.APP.STATE_SNAPSHOT_15M ss
      on ss.MARKET_TYPE = br.MARKET_TYPE
     and ss.SYMBOL = br.SYMBOL
     and ss.INTERVAL_MINUTES = 15
     and ss.TS = br.TS
     and ss.METRIC_VERSION = 'v1_1'
     and ss.BUCKET_VERSION = 'v1'
    where br.SIGNAL_DIRECTION is not null
      and (br.CFG_DIRECTION = 'BOTH' or br.SIGNAL_DIRECTION = br.CFG_DIRECTION)
),
gated as (
    select
        ws.*,
        lag(ws.STATE_BUCKET_ID) over (
            partition by ws.PATTERN_ID, ws.SYMBOL, ws.SESSION_DATE
            order by ws.TS
        ) as PREV_BREAKOUT_STATE_BUCKET_ID,
        case
            when coalesce(ws.PREV_AVG_RANGE_PCT, 0) <= 0 then false
            when ws.BAR_RANGE_PCT >= ws.VOL_EXPANSION_MULT * ws.PREV_AVG_RANGE_PCT then true
            else false
        end as VOL_EXPANSION_FLAG
    from with_state ws
),
triggered as (
    select
        g.*,
        case
            when row_number() over (
                    partition by g.PATTERN_ID, g.SYMBOL, g.SESSION_DATE
                    order by g.TS
                 ) = 1 then true
            when g.STATE_BUCKET_ID <> g.PREV_BREAKOUT_STATE_BUCKET_ID then true
            when g.VOL_EXPANSION_FLAG then true
            else false
        end as TRIGGER_OK
    from gated g
),
capped as (
    select *
    from triggered
    where TRIGGER_OK
    qualify row_number() over (
        partition by PATTERN_ID, SYMBOL, SESSION_DATE
        order by TS
    ) <= MAX_SIGNALS_PER_SESSION
)
select
    c.PATTERN_ID,
    c.SYMBOL,
    c.MARKET_TYPE,
    15 as INTERVAL_MINUTES,
    c.TS,
    c.BREAKOUT_DISTANCE_PCT as SCORE,
    object_construct(
        'pattern_type', 'ORB',
        'proto_variant', 'ORB_PROTO_RETRIGGER',
        'dry_run_stage', '3b_stage2',
        'direction', c.SIGNAL_DIRECTION,
        'opening_range_high', c.RANGE_HIGH,
        'opening_range_low', c.RANGE_LOW,
        'range_pct', c.RANGE_PCT,
        'breakout_buffer_pct', c.BREAKOUT_BUFFER_PCT,
        'min_range_pct', c.MIN_RANGE_PCT,
        'state_bucket_id', c.STATE_BUCKET_ID,
        'prev_breakout_state_bucket_id', c.PREV_BREAKOUT_STATE_BUCKET_ID,
        'vol_expansion_flag', c.VOL_EXPANSION_FLAG,
        'bar_range_pct', c.BAR_RANGE_PCT,
        'prev_avg_range_pct', c.PREV_AVG_RANGE_PCT,
        'max_signals_per_session', c.MAX_SIGNALS_PER_SESSION,
        'rule', 'STATE_CHANGE_OR_VOL_EXPANSION',
        'time_bucket', case
            when c.BAR_HOUR < 16 then 'MORNING'
            when c.BAR_HOUR < 18 then 'MIDDAY'
            else 'AFTERNOON'
        end,
        'volume', c.VOLUME
    ) as DETAILS
from capped c
where not exists (
    select 1
    from MIP.APP.RECOMMENDATION_LOG r
    where r.PATTERN_ID = c.PATTERN_ID
      and r.SYMBOL = c.SYMBOL
      and r.MARKET_TYPE = c.MARKET_TYPE
      and r.INTERVAL_MINUTES = 15
      and r.TS = c.TS
);

-- 4) Return Stage 2 pattern IDs.
select
    PATTERN_ID,
    NAME
from MIP.APP.PATTERN_DEFINITION
where NAME in (
    'ORB_PROTO_RETRIGGER_STOCK_15MIN',
    'ORB_PROTO_RETRIGGER_ETF_15MIN',
    'ORB_PROTO_RETRIGGER_FX_15MIN'
)
order by NAME;

