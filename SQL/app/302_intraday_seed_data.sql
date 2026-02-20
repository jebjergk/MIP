-- 302_intraday_seed_data.sql
-- Purpose: Seed intraday pattern definitions and ingest universe rows.
-- All patterns start dormant (IS_ACTIVE='N') and ingest rows start disabled (IS_ENABLED=false).
-- Nothing runs until explicitly activated in Phase 1b.

use role MIP_ADMIN_ROLE;
use database MIP;

------------------------------
-- 1. Intraday pattern definitions (dormant)
------------------------------

-- ORB: Opening Range Breakout (hourly, stocks)
merge into MIP.APP.PATTERN_DEFINITION t
using (
    select
        'ORB_STOCK_HOURLY' as NAME,
        'ORB' as PATTERN_TYPE,
        'Opening Range Breakout on hourly bars: detects price breaking above/below the first-hour range' as DESCRIPTION,
        parse_json('{
            "pattern_type": "ORB",
            "interval_minutes": 60,
            "market_type": "STOCK",
            "range_bars": 1,
            "breakout_buffer_pct": 0.001,
            "min_range_pct": 0.003,
            "session_start_hour_utc": 14,
            "direction": "BOTH"
        }') as PARAMS_JSON,
        'N' as IS_ACTIVE,
        false as ENABLED
) s
on t.NAME = s.NAME
when not matched then insert (NAME, PATTERN_TYPE, DESCRIPTION, PARAMS_JSON, IS_ACTIVE, ENABLED)
    values (s.NAME, s.PATTERN_TYPE, s.DESCRIPTION, s.PARAMS_JSON, s.IS_ACTIVE, s.ENABLED);

-- Pullback Continuation (hourly, stocks)
merge into MIP.APP.PATTERN_DEFINITION t
using (
    select
        'PULLBACK_STOCK_HOURLY' as NAME,
        'PULLBACK_CONTINUATION' as PATTERN_TYPE,
        'Pullback Continuation on hourly bars: detects consolidation after impulse move followed by breakout in impulse direction' as DESCRIPTION,
        parse_json('{
            "pattern_type": "PULLBACK_CONTINUATION",
            "interval_minutes": 60,
            "market_type": "STOCK",
            "impulse_bars": 3,
            "impulse_min_return": 0.01,
            "consolidation_max_bars": 3,
            "consolidation_max_range_pct": 0.005,
            "breakout_buffer_pct": 0.001
        }') as PARAMS_JSON,
        'N' as IS_ACTIVE,
        false as ENABLED
) s
on t.NAME = s.NAME
when not matched then insert (NAME, PATTERN_TYPE, DESCRIPTION, PARAMS_JSON, IS_ACTIVE, ENABLED)
    values (s.NAME, s.PATTERN_TYPE, s.DESCRIPTION, s.PARAMS_JSON, s.IS_ACTIVE, s.ENABLED);

-- Mean-Reversion Overshoot (hourly, stocks)
merge into MIP.APP.PATTERN_DEFINITION t
using (
    select
        'MEANREV_STOCK_HOURLY' as NAME,
        'MEAN_REVERSION' as PATTERN_TYPE,
        'Mean-Reversion Overshoot on hourly bars: detects extreme deviation from rolling intraday average, expects reversion' as DESCRIPTION,
        parse_json('{
            "pattern_type": "MEAN_REVERSION",
            "interval_minutes": 60,
            "market_type": "STOCK",
            "anchor_window": 5,
            "deviation_threshold_pct": 0.015,
            "min_bars_for_anchor": 3,
            "direction": "BOTH"
        }') as PARAMS_JSON,
        'N' as IS_ACTIVE,
        false as ENABLED
) s
on t.NAME = s.NAME
when not matched then insert (NAME, PATTERN_TYPE, DESCRIPTION, PARAMS_JSON, IS_ACTIVE, ENABLED)
    values (s.NAME, s.PATTERN_TYPE, s.DESCRIPTION, s.PARAMS_JSON, s.IS_ACTIVE, s.ENABLED);

------------------------------
-- 2. Intraday ingest universe rows (disabled)
--    Top liquid US stocks for initial intraday research.
--    PRIORITY lower than daily rows so daily ingestion is unaffected.
------------------------------
merge into MIP.APP.INGEST_UNIVERSE t
using (
    select 'AAPL'  as SYMBOL, 'STOCK' as MARKET_TYPE, 60 as INTERVAL_MINUTES,
           false as IS_ENABLED, 50 as PRIORITY, 'Intraday hourly - Phase 1 research' as NOTES
    union all
    select 'MSFT',  'STOCK', 60, false, 50, 'Intraday hourly - Phase 1 research'
    union all
    select 'GOOGL', 'STOCK', 60, false, 50, 'Intraday hourly - Phase 1 research'
    union all
    select 'AMZN',  'STOCK', 60, false, 50, 'Intraday hourly - Phase 1 research'
    union all
    select 'NVDA',  'STOCK', 60, false, 50, 'Intraday hourly - Phase 1 research'
) s
on  t.SYMBOL = s.SYMBOL
and t.MARKET_TYPE = s.MARKET_TYPE
and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
when not matched then insert (SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, IS_ENABLED, PRIORITY, NOTES)
    values (s.SYMBOL, s.MARKET_TYPE, s.INTERVAL_MINUTES, s.IS_ENABLED, s.PRIORITY, s.NOTES);
