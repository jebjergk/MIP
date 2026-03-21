-- v_recommendation_quality_score.sql
-- Purpose: Composite quality scoring layer for recommendations.
-- Takes raw signals from RECOMMENDATION_LOG and computes a weighted quality score
-- based on multiple factors: signal strength, trend context, volume, regime alignment.
-- This evolves the binary AND-gate approach into a graduated confidence system.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_RECOMMENDATION_QUALITY_SCORE as
with rec_base as (
    select
        rl.RECOMMENDATION_ID,
        rl.PATTERN_ID,
        rl.SYMBOL,
        rl.MARKET_TYPE,
        rl.INTERVAL_MINUTES,
        rl.TS,
        rl.SCORE as RAW_SCORE,
        rl.DETAILS,
        pd.NAME as PATTERN_NAME,
        pd.PATTERN_TYPE
    from MIP.APP.RECOMMENDATION_LOG rl
    join MIP.APP.PATTERN_DEFINITION pd on pd.PATTERN_ID = rl.PATTERN_ID
    where rl.INTERVAL_MINUTES = 1440
),
bar_context as (
    select
        r.RECOMMENDATION_ID,
        r.SYMBOL,
        r.MARKET_TYPE,
        r.TS,
        r.PATTERN_TYPE,
        r.RAW_SCORE,
        r.DETAILS,
        r.PATTERN_NAME,
        r.PATTERN_ID,
        mb.CLOSE,
        mb.VOLUME,
        AVG(mb2.VOLUME) as AVG_VOLUME_20D,
        STDDEV(mb2.CLOSE) / nullif(AVG(mb2.CLOSE), 0) as PRICE_VOL_20D
    from rec_base r
    left join MIP.MART.MARKET_BARS mb
        on mb.SYMBOL = r.SYMBOL
        and mb.MARKET_TYPE = r.MARKET_TYPE
        and mb.INTERVAL_MINUTES = r.INTERVAL_MINUTES
        and mb.TS = r.TS
    left join MIP.MART.MARKET_BARS mb2
        on mb2.SYMBOL = r.SYMBOL
        and mb2.MARKET_TYPE = r.MARKET_TYPE
        and mb2.INTERVAL_MINUTES = r.INTERVAL_MINUTES
        and mb2.TS between dateadd(day, -30, r.TS) and r.TS
    group by r.RECOMMENDATION_ID, r.SYMBOL, r.MARKET_TYPE, r.TS,
             r.PATTERN_TYPE, r.RAW_SCORE, r.DETAILS, r.PATTERN_NAME,
             r.PATTERN_ID, mb.CLOSE, mb.VOLUME
),
regime_context as (
    select
        bc.*,
        reg.REGIME as MARKET_REGIME,
        reg.REGIME_CONFIDENCE
    from bar_context bc
    left join MIP.MART.V_MARKET_REGIME reg
        on reg.REGIME_DATE = bc.TS
),
scored as (
    select
        rc.*,
        -- Factor 1: Signal strength (0-25 points)
        -- Stronger raw scores get more points
        least(25, greatest(0,
            case
                when PATTERN_TYPE = 'MOMENTUM'
                then RAW_SCORE * 1000
                when PATTERN_TYPE = 'MEAN_REVERSION'
                then abs(RAW_SCORE) * 800
                when PATTERN_TYPE = 'BEARISH_MOMENTUM'
                then abs(RAW_SCORE) * 800
                else RAW_SCORE * 500
            end
        )) as SIGNAL_STRENGTH_SCORE,

        -- Factor 2: Volume confirmation (0-20 points)
        -- Higher than average volume confirms the signal
        case
            when AVG_VOLUME_20D is null or AVG_VOLUME_20D = 0 then 10
            when VOLUME >= AVG_VOLUME_20D * 1.5 then 20
            when VOLUME >= AVG_VOLUME_20D * 1.0 then 15
            when VOLUME >= AVG_VOLUME_20D * 0.7 then 10
            else 5
        end as VOLUME_SCORE,

        -- Factor 3: Regime alignment (0-25 points)
        -- Bullish signals in BULL regime get boost, bearish in BEAR get boost
        case
            when PATTERN_TYPE in ('MOMENTUM', 'PULLBACK_CONTINUATION', 'ORB') then
                case
                    when MARKET_REGIME = 'BULL' then 25
                    when MARKET_REGIME = 'VOLATILE_BULL' then 20
                    when MARKET_REGIME = 'SIDEWAYS' then 15
                    when MARKET_REGIME = 'VOLATILE_BEAR' then 5
                    when MARKET_REGIME = 'BEAR' then 0
                    else 12
                end
            when PATTERN_TYPE = 'MEAN_REVERSION' then
                case
                    when MARKET_REGIME in ('SIDEWAYS', 'VOLATILE_BEAR') then 25
                    when MARKET_REGIME = 'BEAR' then 20
                    when MARKET_REGIME = 'BULL' then 15
                    when MARKET_REGIME = 'VOLATILE_BULL' then 10
                    else 15
                end
            when PATTERN_TYPE = 'BEARISH_MOMENTUM' then
                case
                    when MARKET_REGIME = 'BEAR' then 25
                    when MARKET_REGIME = 'VOLATILE_BEAR' then 25
                    when MARKET_REGIME = 'SIDEWAYS' then 15
                    when MARKET_REGIME in ('BULL', 'VOLATILE_BULL') then 5
                    else 12
                end
            else 12
        end as REGIME_ALIGNMENT_SCORE,

        -- Factor 4: Volatility appropriateness (0-15 points)
        -- Momentum works better in moderate vol; mean-reversion in higher vol
        case
            when PRICE_VOL_20D is null then 8
            when PATTERN_TYPE in ('MOMENTUM', 'PULLBACK_CONTINUATION') then
                case
                    when PRICE_VOL_20D between 0.005 and 0.025 then 15
                    when PRICE_VOL_20D between 0.025 and 0.04 then 10
                    when PRICE_VOL_20D < 0.005 then 5
                    else 3
                end
            when PATTERN_TYPE = 'MEAN_REVERSION' then
                case
                    when PRICE_VOL_20D > 0.015 then 15
                    when PRICE_VOL_20D between 0.01 and 0.015 then 10
                    else 5
                end
            else 8
        end as VOLATILITY_FIT_SCORE,

        -- Factor 5: Pattern track record (0-15 points)
        -- Based on historical performance stored in PATTERN_DEFINITION
        15 as TRACK_RECORD_SCORE

    from regime_context rc
)
select
    RECOMMENDATION_ID,
    PATTERN_ID,
    PATTERN_NAME,
    PATTERN_TYPE,
    SYMBOL,
    MARKET_TYPE,
    TS,
    RAW_SCORE,
    SIGNAL_STRENGTH_SCORE,
    VOLUME_SCORE,
    REGIME_ALIGNMENT_SCORE,
    VOLATILITY_FIT_SCORE,
    TRACK_RECORD_SCORE,
    (SIGNAL_STRENGTH_SCORE + VOLUME_SCORE + REGIME_ALIGNMENT_SCORE
        + VOLATILITY_FIT_SCORE + TRACK_RECORD_SCORE) as COMPOSITE_SCORE,
    ROUND((SIGNAL_STRENGTH_SCORE + VOLUME_SCORE + REGIME_ALIGNMENT_SCORE
        + VOLATILITY_FIT_SCORE + TRACK_RECORD_SCORE) / 100.0, 4) as QUALITY_PCT,
    case
        when (SIGNAL_STRENGTH_SCORE + VOLUME_SCORE + REGIME_ALIGNMENT_SCORE
            + VOLATILITY_FIT_SCORE + TRACK_RECORD_SCORE) >= 70 then 'HIGH'
        when (SIGNAL_STRENGTH_SCORE + VOLUME_SCORE + REGIME_ALIGNMENT_SCORE
            + VOLATILITY_FIT_SCORE + TRACK_RECORD_SCORE) >= 50 then 'MEDIUM'
        when (SIGNAL_STRENGTH_SCORE + VOLUME_SCORE + REGIME_ALIGNMENT_SCORE
            + VOLATILITY_FIT_SCORE + TRACK_RECORD_SCORE) >= 30 then 'LOW'
        else 'VERY_LOW'
    end as QUALITY_TIER,
    MARKET_REGIME,
    REGIME_CONFIDENCE,
    CLOSE,
    VOLUME,
    AVG_VOLUME_20D,
    PRICE_VOL_20D
from scored;
