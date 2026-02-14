-- 230_parallel_world_scenario.sql
-- Purpose: Scenario catalog for Parallel Worlds — defines alternative world configurations
-- (threshold variants, sizing variants, timing variants, baselines).
-- Each row is a named, versioned scenario with a PARAMS_JSON variant config.
-- IS_ACTIVE controls which scenarios are included in default runs.
-- MERGE key on NAME ensures seed data is idempotent.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.PARALLEL_WORLD_SCENARIO (
    SCENARIO_ID     number        autoincrement,
    NAME            varchar(128)  not null,
    DESCRIPTION     varchar(1024),
    SCENARIO_TYPE   varchar(32)   not null,       -- THRESHOLD | SIZING | TIMING | BASELINE
    PARAMS_JSON     variant       not null,
    IS_ACTIVE       boolean       default true,
    CREATED_AT      timestamp_ntz default current_timestamp(),
    UPDATED_AT      timestamp_ntz default current_timestamp(),

    constraint PK_PW_SCENARIO primary key (SCENARIO_ID),
    constraint UQ_PW_SCENARIO_NAME unique (NAME)
);

-- Seed scenarios (idempotent via MERGE on NAME)
merge into MIP.APP.PARALLEL_WORLD_SCENARIO as target
using (
    select column1 as NAME,
           column2 as DESCRIPTION,
           column3 as SCENARIO_TYPE,
           parse_json(column4) as PARAMS_JSON
    from values
        ('THRESH_ZSCORE_MINUS_0_25',
         'Lower z-score threshold by 0.25 — admits more signals (looser filter).',
         'THRESHOLD',
         '{"min_zscore_delta": -0.25}'),
        ('THRESH_ZSCORE_PLUS_0_25',
         'Raise z-score threshold by 0.25 — admits fewer signals (stricter filter).',
         'THRESHOLD',
         '{"min_zscore_delta": 0.25}'),
        ('THRESH_RETURN_MINUS_0_0005',
         'Lower min-return threshold by 0.05% — admits weaker momentum signals.',
         'THRESHOLD',
         '{"min_return_delta": -0.0005}'),
        ('THRESH_RETURN_PLUS_0_0005',
         'Raise min-return threshold by 0.05% — requires stronger momentum signals.',
         'THRESHOLD',
         '{"min_return_delta": 0.0005}'),
        ('SIZING_75PCT',
         'Reduce position sizing to 75% of profile max — more conservative allocation.',
         'SIZING',
         '{"position_pct_multiplier": 0.75}'),
        ('SIZING_125PCT',
         'Increase position sizing to 125% of profile max (capped at capacity) — more aggressive allocation.',
         'SIZING',
         '{"position_pct_multiplier": 1.25}'),
        ('TIMING_DELAY_1_BAR',
         'Delay entry by 1 bar — enter on the following day instead of the signal day.',
         'TIMING',
         '{"entry_delay_bars": 1}'),
        ('DO_NOTHING',
         'Baseline: skip all entries, hold cash only. Shows the cost/benefit of any trading.',
         'BASELINE',
         '{"skip_all_entries": true}')
) as source
on target.NAME = source.NAME
when not matched then insert (
    NAME, DESCRIPTION, SCENARIO_TYPE, PARAMS_JSON, IS_ACTIVE, CREATED_AT, UPDATED_AT
) values (
    source.NAME, source.DESCRIPTION, source.SCENARIO_TYPE, source.PARAMS_JSON,
    true, current_timestamp(), current_timestamp()
)
when matched then update set
    target.DESCRIPTION   = source.DESCRIPTION,
    target.SCENARIO_TYPE = source.SCENARIO_TYPE,
    target.PARAMS_JSON   = source.PARAMS_JSON,
    target.UPDATED_AT    = current_timestamp();
