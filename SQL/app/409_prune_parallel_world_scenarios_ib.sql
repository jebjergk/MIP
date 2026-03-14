-- 409_prune_parallel_world_scenarios_ib.sql
-- Purpose: Prune Parallel Worlds scenario catalog to a manageable IB-era set.
-- Strategy:
--   1) Persist sweep family config so noisy families stay off.
--   2) Deactivate legacy/noise scenarios in PARALLEL_WORLD_SCENARIO.
--   3) Reactivate only curated scenarios.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Persist per-family sweep switches so SP_RUN_PW_SWEEP cannot re-enable
-- large legacy families due to missing config keys.
merge into MIP.APP.APP_CONFIG t
using (
    select column1 as CONFIG_KEY, column2 as CONFIG_VALUE, column3 as DESCRIPTION
    from values
        ('PW_SWEEP_ZSCORE_ENABLED', 'false', 'Parallel Worlds sweep family toggle: z-score threshold sweep'),
        ('PW_SWEEP_RETURN_ENABLED', 'false', 'Parallel Worlds sweep family toggle: return threshold sweep'),
        ('PW_SWEEP_SIZING_ENABLED', 'false', 'Parallel Worlds sweep family toggle: position sizing sweep'),
        ('PW_SWEEP_TIMING_ENABLED', 'false', 'Parallel Worlds sweep family toggle: entry delay sweep'),
        ('PW_SWEEP_HORIZON_ENABLED', 'true', 'Parallel Worlds sweep family toggle: holding horizon sweep'),
        ('PW_SWEEP_EARLY_EXIT_ENABLED', 'true', 'Parallel Worlds sweep family toggle: early-exit payoff sweep')
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
    t.CONFIG_VALUE = s.CONFIG_VALUE,
    t.DESCRIPTION = s.DESCRIPTION,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (
    CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT
) values (
    s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp()
);

-- Deactivate legacy/noise families for current IB-first operating mode.
update MIP.APP.PARALLEL_WORLD_SCENARIO
set IS_ACTIVE = false,
    UPDATED_AT = current_timestamp()
where
    SCENARIO_TYPE in ('THRESHOLD', 'TIMING')
    or (coalesce(IS_SWEEP, false) = true and coalesce(SWEEP_FAMILY, '') in (
        'ZSCORE_SWEEP',
        'RETURN_SWEEP',
        'SIZING_SWEEP',
        'TIMING_SWEEP'
    ));

-- Reactivate only curated scenarios.
update MIP.APP.PARALLEL_WORLD_SCENARIO
set IS_ACTIVE = true,
    UPDATED_AT = current_timestamp()
where NAME in (
    -- Core default comparators
    'DO_NOTHING',
    'SIZING_75PCT',
    'SIZING_125PCT',
    -- Sweep families retained for now
    'SWEEP_HORIZON_01',
    'SWEEP_HORIZON_02',
    'SWEEP_HORIZON_03',
    'SWEEP_HORIZON_04',
    'SWEEP_EARLY_EXIT_01',
    'SWEEP_EARLY_EXIT_02',
    'SWEEP_EARLY_EXIT_03',
    'SWEEP_EARLY_EXIT_04',
    'SWEEP_EARLY_EXIT_05',
    'SWEEP_EARLY_EXIT_06'
);
