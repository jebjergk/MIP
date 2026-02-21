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
    DISPLAY_NAME    varchar(256),
    DESCRIPTION     varchar(1024),
    SCENARIO_TYPE   varchar(32)   not null,       -- THRESHOLD | SIZING | TIMING | BASELINE | HORIZON
    PARAMS_JSON     variant       not null,
    IS_ACTIVE       boolean       default true,
    CREATED_AT      timestamp_ntz default current_timestamp(),
    UPDATED_AT      timestamp_ntz default current_timestamp(),

    constraint PK_PW_SCENARIO primary key (SCENARIO_ID),
    constraint UQ_PW_SCENARIO_NAME unique (NAME)
);

-- Sweep columns (added for Policy Tuning Lab)
alter table MIP.APP.PARALLEL_WORLD_SCENARIO add column if not exists
    IS_SWEEP      boolean       default false;
alter table MIP.APP.PARALLEL_WORLD_SCENARIO add column if not exists
    SWEEP_FAMILY  varchar(64);
alter table MIP.APP.PARALLEL_WORLD_SCENARIO add column if not exists
    SWEEP_ORDER   number;

-- Seed scenarios (idempotent via MERGE on NAME)
merge into MIP.APP.PARALLEL_WORLD_SCENARIO as target
using (
    select column1 as NAME,
           column2 as DISPLAY_NAME,
           column3 as DESCRIPTION,
           column4 as SCENARIO_TYPE,
           parse_json(column5) as PARAMS_JSON
    from values
        ('THRESH_ZSCORE_MINUS_0_25',
         'Looser Signal Filter (z-score -0.25)',
         'Lower z-score threshold by 0.25 — admits more signals (looser filter).',
         'THRESHOLD',
         '{"min_zscore_delta": -0.25}'),
        ('THRESH_ZSCORE_PLUS_0_25',
         'Stricter Signal Filter (z-score +0.25)',
         'Raise z-score threshold by 0.25 — admits fewer signals (stricter filter).',
         'THRESHOLD',
         '{"min_zscore_delta": 0.25}'),
        ('THRESH_RETURN_MINUS_0_0005',
         'Lower Return Bar (-0.05%)',
         'Lower min-return threshold by 0.05% — admits weaker momentum signals.',
         'THRESHOLD',
         '{"min_return_delta": -0.0005}'),
        ('THRESH_RETURN_PLUS_0_0005',
         'Higher Return Bar (+0.05%)',
         'Raise min-return threshold by 0.05% — requires stronger momentum signals.',
         'THRESHOLD',
         '{"min_return_delta": 0.0005}'),
        ('SIZING_75PCT',
         'Smaller Positions (75%)',
         'Reduce position sizing to 75% of profile max — more conservative allocation.',
         'SIZING',
         '{"position_pct_multiplier": 0.75}'),
        ('SIZING_125PCT',
         'Larger Positions (125%)',
         'Increase position sizing to 125% of profile max (capped at capacity) — more aggressive allocation.',
         'SIZING',
         '{"position_pct_multiplier": 1.25}'),
        ('TIMING_DELAY_1_BAR',
         'Wait 1 Day Before Entering',
         'Delay entry by 1 bar — enter on the following day instead of the signal day.',
         'TIMING',
         '{"entry_delay_bars": 1}'),
        ('DO_NOTHING',
         'Stay in Cash (No Trades)',
         'Baseline: skip all entries, hold cash only. Shows the cost/benefit of any trading.',
         'BASELINE',
         '{"skip_all_entries": true}')
) as source
on target.NAME = source.NAME
when not matched then insert (
    NAME, DISPLAY_NAME, DESCRIPTION, SCENARIO_TYPE, PARAMS_JSON, IS_ACTIVE, CREATED_AT, UPDATED_AT
) values (
    source.NAME, source.DISPLAY_NAME, source.DESCRIPTION, source.SCENARIO_TYPE, source.PARAMS_JSON,
    true, current_timestamp(), current_timestamp()
)
when matched then update set
    target.DISPLAY_NAME  = source.DISPLAY_NAME,
    target.DESCRIPTION   = source.DESCRIPTION,
    target.SCENARIO_TYPE = source.SCENARIO_TYPE,
    target.PARAMS_JSON   = source.PARAMS_JSON,
    target.UPDATED_AT    = current_timestamp();

-- Horizon sweep scenarios (idempotent via MERGE on NAME)
merge into MIP.APP.PARALLEL_WORLD_SCENARIO as target
using (
    select column1 as NAME,
           column2 as DISPLAY_NAME,
           column3 as DESCRIPTION,
           column4 as SCENARIO_TYPE,
           parse_json(column5) as PARAMS_JSON,
           column6 as IS_SWEEP,
           column7 as SWEEP_FAMILY,
           column8 as SWEEP_ORDER
    from values
        ('SWEEP_HORIZON_01',
         'Hold 1 Bar (Baseline)',
         'Horizon sweep: hold each position for 1 bar.',
         'HORIZON',
         '{"hold_horizon_bars": 1}',
         true, 'HORIZON_SWEEP', 1),
        ('SWEEP_HORIZON_02',
         'Hold 3 Bars',
         'Horizon sweep: hold each position for 3 bars.',
         'HORIZON',
         '{"hold_horizon_bars": 3}',
         true, 'HORIZON_SWEEP', 2),
        ('SWEEP_HORIZON_03',
         'Hold 5 Bars',
         'Horizon sweep: hold each position for 5 bars.',
         'HORIZON',
         '{"hold_horizon_bars": 5}',
         true, 'HORIZON_SWEEP', 3),
        ('SWEEP_HORIZON_04',
         'Hold 10 Bars',
         'Horizon sweep: hold each position for 10 bars.',
         'HORIZON',
         '{"hold_horizon_bars": 10}',
         true, 'HORIZON_SWEEP', 4)
) as source
on target.NAME = source.NAME
when not matched then insert (
    NAME, DISPLAY_NAME, DESCRIPTION, SCENARIO_TYPE, PARAMS_JSON,
    IS_ACTIVE, IS_SWEEP, SWEEP_FAMILY, SWEEP_ORDER,
    CREATED_AT, UPDATED_AT
) values (
    source.NAME, source.DISPLAY_NAME, source.DESCRIPTION, source.SCENARIO_TYPE, source.PARAMS_JSON,
    true, source.IS_SWEEP, source.SWEEP_FAMILY, source.SWEEP_ORDER,
    current_timestamp(), current_timestamp()
)
when matched then update set
    target.DISPLAY_NAME  = source.DISPLAY_NAME,
    target.DESCRIPTION   = source.DESCRIPTION,
    target.SCENARIO_TYPE = source.SCENARIO_TYPE,
    target.PARAMS_JSON   = source.PARAMS_JSON,
    target.IS_SWEEP      = source.IS_SWEEP,
    target.SWEEP_FAMILY  = source.SWEEP_FAMILY,
    target.SWEEP_ORDER   = source.SWEEP_ORDER,
    target.UPDATED_AT    = current_timestamp();
