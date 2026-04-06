-- Relaxed SHORT patterns (research only): lower min_zscore and min_return vs 1101/1102.
-- Does NOT relax new-local-low or down-streak logic (same generator branch as strict).
-- Start inactive until explicitly enabled for backfill experiments.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.PATTERN_DEFINITION t
using (
    select
        1103 as PATTERN_ID,
        'STOCK_MOMENTUM_FAST_SHORT_RELAXED' as NAME,
        'Stock momentum short (fast, daily, relaxed thresholds) — research only' as DESCRIPTION,
        'MOMENTUM_SHORT' as PATTERN_TYPE,
        object_construct(
            'fast_window', 20,
            'slow_window', 3,
            'lookback_days', 30,
            'min_return', 0.0015,
            'min_zscore', 0.5,
            'market_type', 'STOCK',
            'interval_minutes', 1440
        ) as PARAMS_JSON,
        'MOMENTUM_SHORT_RELAXED' as PATTERN_FAMILY,
        'Momentum (Fast, SHORT, relaxed)' as DISPLAY_NAME,
        'Mom F SHORT · relaxed' as DISPLAY_SHORT_NAME,
        'SHORT' as SIGNAL_DIRECTION,
        20 as PARAM_FAST,
        3 as PARAM_SLOW,
        '1440m' as PARAM_HORIZON,
        'N' as IS_ACTIVE,
        false as ENABLED
    union all
    select
        1104,
        'STOCK_MOMENTUM_SLOW_SHORT_RELAXED',
        'Stock momentum short (slow, daily, relaxed thresholds) — research only',
        'MOMENTUM_SHORT',
        object_construct(
            'fast_window', 30,
            'slow_window', 2,
            'lookback_days', 60,
            'min_return', 0.0008,
            'min_zscore', 0.5,
            'market_type', 'STOCK',
            'interval_minutes', 1440
        ),
        'MOMENTUM_SHORT_RELAXED',
        'Momentum (Slow, SHORT, relaxed)',
        'Mom S SHORT · relaxed',
        'SHORT',
        30,
        2,
        '1440m',
        'N',
        false
) s
on t.NAME = s.NAME
when matched then update set
    t.DESCRIPTION = s.DESCRIPTION,
    t.PATTERN_TYPE = s.PATTERN_TYPE,
    t.PARAMS_JSON = s.PARAMS_JSON,
    t.PATTERN_FAMILY = s.PATTERN_FAMILY,
    t.DISPLAY_NAME = s.DISPLAY_NAME,
    t.DISPLAY_SHORT_NAME = s.DISPLAY_SHORT_NAME,
    t.SIGNAL_DIRECTION = s.SIGNAL_DIRECTION,
    t.PARAM_FAST = s.PARAM_FAST,
    t.PARAM_SLOW = s.PARAM_SLOW,
    t.PARAM_HORIZON = s.PARAM_HORIZON,
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.ENABLED = s.ENABLED,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (
    PATTERN_ID, NAME, DESCRIPTION, PATTERN_TYPE, PARAMS_JSON, PATTERN_FAMILY,
    DISPLAY_NAME, DISPLAY_SHORT_NAME, SIGNAL_DIRECTION,
    PARAM_FAST, PARAM_SLOW, PARAM_HORIZON, IS_ACTIVE, ENABLED
)
values (
    s.PATTERN_ID, s.NAME, s.DESCRIPTION, s.PATTERN_TYPE, s.PARAMS_JSON, s.PATTERN_FAMILY,
    s.DISPLAY_NAME, s.DISPLAY_SHORT_NAME, s.SIGNAL_DIRECTION,
    s.PARAM_FAST, s.PARAM_SLOW, s.PARAM_HORIZON, s.IS_ACTIVE, s.ENABLED
);
