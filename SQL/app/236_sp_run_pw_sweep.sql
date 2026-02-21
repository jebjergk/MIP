-- 236_sp_run_pw_sweep.sql
-- Purpose: Parameter sweep engine for the Policy Tuning Lab.
-- Seeds sweep scenario rows into PARALLEL_WORLD_SCENARIO (idempotent MERGE),
-- then calls SP_RUN_PARALLEL_WORLDS with SCENARIO_SET='SWEEP' to run them all.
--
-- Sweep families:
--   ZSCORE_SWEEP   — 9 points: min_zscore_delta from -0.50 to +0.50
--   RETURN_SWEEP   — 9 points: min_return_delta from -0.0010 to +0.0010
--   SIZING_SWEEP   — 7 points: position_pct_multiplier from 0.50 to 1.50
--   TIMING_SWEEP   — 4 points: entry_delay_bars from 0 to 3
--   HORIZON_SWEEP    — 4 points: hold_horizon_bars from 1 to 10
--   EARLY_EXIT_SWEEP — 6 points: payoff_multiplier from 0.6 to 2.0
--
-- Config-gated via PW_SWEEP_ENABLED + per-family flags.
-- Non-fatal: failures return error JSON, don't halt pipeline.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_RUN_PW_SWEEP(
    P_RUN_ID        varchar,
    P_AS_OF_TS      timestamp_ntz,
    P_PORTFOLIO_ID  number default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id        varchar := :P_RUN_ID;
    v_as_of_ts      timestamp_ntz := :P_AS_OF_TS;
    v_sweep_enabled boolean := false;
    v_zscore_on     boolean := false;
    v_return_on     boolean := false;
    v_sizing_on     boolean := false;
    v_timing_on     boolean := false;
    v_horizon_on    boolean := false;
    v_early_exit_on boolean := false;
    v_max_scenarios number := 30;
    v_seeded_count  number := 0;
    v_pw_result     variant;
begin
    -- Check global sweep flag
    begin
        v_sweep_enabled := (select try_to_boolean(CONFIG_VALUE)
                            from MIP.APP.APP_CONFIG
                            where CONFIG_KEY = 'PW_SWEEP_ENABLED');
    exception when other then
        v_sweep_enabled := false;
    end;

    if (not :v_sweep_enabled) then
        return object_construct('status', 'SKIPPED', 'reason', 'PW_SWEEP_ENABLED is false');
    end if;

    -- Load per-family flags (default ON if sweep is globally enabled)
    begin
        select
            coalesce(max(case when CONFIG_KEY = 'PW_SWEEP_ZSCORE_ENABLED' then try_to_boolean(CONFIG_VALUE) end), true),
            coalesce(max(case when CONFIG_KEY = 'PW_SWEEP_RETURN_ENABLED' then try_to_boolean(CONFIG_VALUE) end), true),
            coalesce(max(case when CONFIG_KEY = 'PW_SWEEP_SIZING_ENABLED' then try_to_boolean(CONFIG_VALUE) end), true),
            coalesce(max(case when CONFIG_KEY = 'PW_SWEEP_TIMING_ENABLED' then try_to_boolean(CONFIG_VALUE) end), true),
            coalesce(max(case when CONFIG_KEY = 'PW_SWEEP_HORIZON_ENABLED' then try_to_boolean(CONFIG_VALUE) end), true),
            coalesce(max(case when CONFIG_KEY = 'PW_SWEEP_EARLY_EXIT_ENABLED' then try_to_boolean(CONFIG_VALUE) end), true),
            coalesce(max(case when CONFIG_KEY = 'PW_SWEEP_MAX_SCENARIOS' then CONFIG_VALUE::number end), 30)
        into :v_zscore_on, :v_return_on, :v_sizing_on, :v_timing_on, :v_horizon_on, :v_early_exit_on, :v_max_scenarios
        from MIP.APP.APP_CONFIG
        where CONFIG_KEY like 'PW_SWEEP_%';
    exception when other then
        null;
    end;

    -- ═════════════════════════════════════════════════════════════
    -- SEED SWEEP SCENARIOS (idempotent MERGE on NAME)
    -- ═════════════════════════════════════════════════════════════

    -- ZSCORE_SWEEP: 9 points from -0.50 to +0.50 in 0.125 steps
    if (:v_zscore_on) then
        merge into MIP.APP.PARALLEL_WORLD_SCENARIO as t
        using (
            select
                'SWEEP_ZSCORE_' || lpad(row_number() over (order by v.val), 2, '0') as NAME,
                'Z-Score ' || case when v.val > 0 then '+' else '' end || round(v.val, 3) as DISPLAY_NAME,
                'Sweep: z-score threshold delta = ' || round(v.val, 3) as DESCRIPTION,
                'THRESHOLD' as SCENARIO_TYPE,
                parse_json('{"min_zscore_delta": ' || round(v.val, 4) || '}') as PARAMS_JSON,
                true as IS_SWEEP,
                'ZSCORE_SWEEP' as SWEEP_FAMILY,
                row_number() over (order by v.val) as SWEEP_ORDER
            from (
                select -0.500 as val union all select -0.375 union all select -0.250
                union all select -0.125 union all select 0.000
                union all select 0.125 union all select 0.250
                union all select 0.375 union all select 0.500
            ) v
        ) as s on t.NAME = s.NAME
        when not matched then insert (
            NAME, DISPLAY_NAME, DESCRIPTION, SCENARIO_TYPE, PARAMS_JSON,
            IS_ACTIVE, IS_SWEEP, SWEEP_FAMILY, SWEEP_ORDER, CREATED_AT, UPDATED_AT
        ) values (
            s.NAME, s.DISPLAY_NAME, s.DESCRIPTION, s.SCENARIO_TYPE, s.PARAMS_JSON,
            true, true, s.SWEEP_FAMILY, s.SWEEP_ORDER, current_timestamp(), current_timestamp()
        )
        when matched then update set
            t.DISPLAY_NAME = s.DISPLAY_NAME,
            t.PARAMS_JSON = s.PARAMS_JSON,
            t.IS_ACTIVE = true,
            t.IS_SWEEP = true,
            t.SWEEP_FAMILY = s.SWEEP_FAMILY,
            t.SWEEP_ORDER = s.SWEEP_ORDER,
            t.UPDATED_AT = current_timestamp();
    end if;

    -- RETURN_SWEEP: 9 points from -0.0010 to +0.0010 in 0.00025 steps
    if (:v_return_on) then
        merge into MIP.APP.PARALLEL_WORLD_SCENARIO as t
        using (
            select
                'SWEEP_RETURN_' || lpad(row_number() over (order by v.val), 2, '0') as NAME,
                'Return ' || case when v.val > 0 then '+' else '' end || round(v.val * 100, 3) || '%' as DISPLAY_NAME,
                'Sweep: return threshold delta = ' || round(v.val, 5) as DESCRIPTION,
                'THRESHOLD' as SCENARIO_TYPE,
                parse_json('{"min_return_delta": ' || round(v.val, 5) || '}') as PARAMS_JSON,
                true as IS_SWEEP,
                'RETURN_SWEEP' as SWEEP_FAMILY,
                row_number() over (order by v.val) as SWEEP_ORDER
            from (
                select -0.00100 as val union all select -0.00075 union all select -0.00050
                union all select -0.00025 union all select 0.00000
                union all select 0.00025 union all select 0.00050
                union all select 0.00075 union all select 0.00100
            ) v
        ) as s on t.NAME = s.NAME
        when not matched then insert (
            NAME, DISPLAY_NAME, DESCRIPTION, SCENARIO_TYPE, PARAMS_JSON,
            IS_ACTIVE, IS_SWEEP, SWEEP_FAMILY, SWEEP_ORDER, CREATED_AT, UPDATED_AT
        ) values (
            s.NAME, s.DISPLAY_NAME, s.DESCRIPTION, s.SCENARIO_TYPE, s.PARAMS_JSON,
            true, true, s.SWEEP_FAMILY, s.SWEEP_ORDER, current_timestamp(), current_timestamp()
        )
        when matched then update set
            t.DISPLAY_NAME = s.DISPLAY_NAME,
            t.PARAMS_JSON = s.PARAMS_JSON,
            t.IS_ACTIVE = true,
            t.IS_SWEEP = true,
            t.SWEEP_FAMILY = s.SWEEP_FAMILY,
            t.SWEEP_ORDER = s.SWEEP_ORDER,
            t.UPDATED_AT = current_timestamp();
    end if;

    -- SIZING_SWEEP: 7 points from 0.50 to 1.50
    if (:v_sizing_on) then
        merge into MIP.APP.PARALLEL_WORLD_SCENARIO as t
        using (
            select
                'SWEEP_SIZING_' || lpad(row_number() over (order by v.val), 2, '0') as NAME,
                'Size ' || round(v.val * 100, 0) || '%' as DISPLAY_NAME,
                'Sweep: position size multiplier = ' || round(v.val, 2) as DESCRIPTION,
                'SIZING' as SCENARIO_TYPE,
                parse_json('{"position_pct_multiplier": ' || round(v.val, 4) || '}') as PARAMS_JSON,
                true as IS_SWEEP,
                'SIZING_SWEEP' as SWEEP_FAMILY,
                row_number() over (order by v.val) as SWEEP_ORDER
            from (
                select 0.50 as val union all select 0.667 union all select 0.833
                union all select 1.000
                union all select 1.167 union all select 1.333 union all select 1.500
            ) v
        ) as s on t.NAME = s.NAME
        when not matched then insert (
            NAME, DISPLAY_NAME, DESCRIPTION, SCENARIO_TYPE, PARAMS_JSON,
            IS_ACTIVE, IS_SWEEP, SWEEP_FAMILY, SWEEP_ORDER, CREATED_AT, UPDATED_AT
        ) values (
            s.NAME, s.DISPLAY_NAME, s.DESCRIPTION, s.SCENARIO_TYPE, s.PARAMS_JSON,
            true, true, s.SWEEP_FAMILY, s.SWEEP_ORDER, current_timestamp(), current_timestamp()
        )
        when matched then update set
            t.DISPLAY_NAME = s.DISPLAY_NAME,
            t.PARAMS_JSON = s.PARAMS_JSON,
            t.IS_ACTIVE = true,
            t.IS_SWEEP = true,
            t.SWEEP_FAMILY = s.SWEEP_FAMILY,
            t.SWEEP_ORDER = s.SWEEP_ORDER,
            t.UPDATED_AT = current_timestamp();
    end if;

    -- TIMING_SWEEP: 4 points: 0, 1, 2, 3 bar delay
    if (:v_timing_on) then
        merge into MIP.APP.PARALLEL_WORLD_SCENARIO as t
        using (
            select
                'SWEEP_TIMING_' || lpad(row_number() over (order by v.val), 2, '0') as NAME,
                'Delay ' || v.val || ' bar' || case when v.val != 1 then 's' else '' end as DISPLAY_NAME,
                'Sweep: entry delay = ' || v.val || ' bars' as DESCRIPTION,
                'TIMING' as SCENARIO_TYPE,
                parse_json('{"entry_delay_bars": ' || v.val || '}') as PARAMS_JSON,
                true as IS_SWEEP,
                'TIMING_SWEEP' as SWEEP_FAMILY,
                row_number() over (order by v.val) as SWEEP_ORDER
            from (
                select 0 as val union all select 1 union all select 2 union all select 3
            ) v
        ) as s on t.NAME = s.NAME
        when not matched then insert (
            NAME, DISPLAY_NAME, DESCRIPTION, SCENARIO_TYPE, PARAMS_JSON,
            IS_ACTIVE, IS_SWEEP, SWEEP_FAMILY, SWEEP_ORDER, CREATED_AT, UPDATED_AT
        ) values (
            s.NAME, s.DISPLAY_NAME, s.DESCRIPTION, s.SCENARIO_TYPE, s.PARAMS_JSON,
            true, true, s.SWEEP_FAMILY, s.SWEEP_ORDER, current_timestamp(), current_timestamp()
        )
        when matched then update set
            t.DISPLAY_NAME = s.DISPLAY_NAME,
            t.PARAMS_JSON = s.PARAMS_JSON,
            t.IS_ACTIVE = true,
            t.IS_SWEEP = true,
            t.SWEEP_FAMILY = s.SWEEP_FAMILY,
            t.SWEEP_ORDER = s.SWEEP_ORDER,
            t.UPDATED_AT = current_timestamp();
    end if;

    -- HORIZON_SWEEP: 4 points: hold 1, 3, 5, 10 bars
    if (:v_horizon_on) then
        merge into MIP.APP.PARALLEL_WORLD_SCENARIO as t
        using (
            select
                'SWEEP_HORIZON_' || lpad(row_number() over (order by v.val), 2, '0') as NAME,
                'Hold ' || v.val || ' bar' || case when v.val != 1 then 's' else '' end as DISPLAY_NAME,
                'Sweep: hold horizon = ' || v.val || ' bars' as DESCRIPTION,
                'HORIZON' as SCENARIO_TYPE,
                parse_json('{"hold_horizon_bars": ' || v.val || '}') as PARAMS_JSON,
                true as IS_SWEEP,
                'HORIZON_SWEEP' as SWEEP_FAMILY,
                row_number() over (order by v.val) as SWEEP_ORDER
            from (
                select 1 as val union all select 3 union all select 5 union all select 10
            ) v
        ) as s on t.NAME = s.NAME
        when not matched then insert (
            NAME, DISPLAY_NAME, DESCRIPTION, SCENARIO_TYPE, PARAMS_JSON,
            IS_ACTIVE, IS_SWEEP, SWEEP_FAMILY, SWEEP_ORDER, CREATED_AT, UPDATED_AT
        ) values (
            s.NAME, s.DISPLAY_NAME, s.DESCRIPTION, s.SCENARIO_TYPE, s.PARAMS_JSON,
            true, true, s.SWEEP_FAMILY, s.SWEEP_ORDER, current_timestamp(), current_timestamp()
        )
        when matched then update set
            t.DISPLAY_NAME = s.DISPLAY_NAME,
            t.PARAMS_JSON = s.PARAMS_JSON,
            t.IS_ACTIVE = true,
            t.IS_SWEEP = true,
            t.SWEEP_FAMILY = s.SWEEP_FAMILY,
            t.SWEEP_ORDER = s.SWEEP_ORDER,
            t.UPDATED_AT = current_timestamp();
    end if;

    -- EARLY_EXIT_SWEEP: 6 points: payoff_multiplier 0.6, 0.8, 1.0, 1.2, 1.5, 2.0
    if (:v_early_exit_on) then
        merge into MIP.APP.PARALLEL_WORLD_SCENARIO as t
        using (
            select
                'SWEEP_EARLY_EXIT_' || lpad(row_number() over (order by v.val), 2, '0') as NAME,
                'Early Exit ' || round(v.val, 1) || 'x' as DISPLAY_NAME,
                'Sweep: early-exit payoff multiplier = ' || round(v.val, 2) as DESCRIPTION,
                'EARLY_EXIT' as SCENARIO_TYPE,
                parse_json('{"payoff_multiplier": ' || round(v.val, 2) || '}') as PARAMS_JSON,
                true as IS_SWEEP,
                'EARLY_EXIT_SWEEP' as SWEEP_FAMILY,
                row_number() over (order by v.val) as SWEEP_ORDER
            from (
                select 0.6 as val union all select 0.8 union all select 1.0
                union all select 1.2 union all select 1.5 union all select 2.0
            ) v
        ) as s on t.NAME = s.NAME
        when not matched then insert (
            NAME, DISPLAY_NAME, DESCRIPTION, SCENARIO_TYPE, PARAMS_JSON,
            IS_ACTIVE, IS_SWEEP, SWEEP_FAMILY, SWEEP_ORDER, CREATED_AT, UPDATED_AT
        ) values (
            s.NAME, s.DISPLAY_NAME, s.DESCRIPTION, s.SCENARIO_TYPE, s.PARAMS_JSON,
            true, true, s.SWEEP_FAMILY, s.SWEEP_ORDER, current_timestamp(), current_timestamp()
        )
        when matched then update set
            t.DISPLAY_NAME = s.DISPLAY_NAME,
            t.PARAMS_JSON = s.PARAMS_JSON,
            t.IS_ACTIVE = true,
            t.IS_SWEEP = true,
            t.SWEEP_FAMILY = s.SWEEP_FAMILY,
            t.SWEEP_ORDER = s.SWEEP_ORDER,
            t.UPDATED_AT = current_timestamp();
    end if;

    -- Count seeded sweep scenarios
    v_seeded_count := (select count(*) from MIP.APP.PARALLEL_WORLD_SCENARIO where IS_SWEEP = true and IS_ACTIVE = true);

    -- ═════════════════════════════════════════════════════════════
    -- RUN SIMULATION for sweep scenarios
    -- ═════════════════════════════════════════════════════════════
    begin
        v_pw_result := (call MIP.APP.SP_RUN_PARALLEL_WORLDS(
            :v_run_id,
            :v_as_of_ts,
            :P_PORTFOLIO_ID,
            'SWEEP'
        ));
    exception when other then
        return object_construct(
            'status', 'FAIL',
            'error', sqlerrm,
            'seeded_scenarios', :v_seeded_count
        );
    end;

    return object_construct(
        'status', 'SUCCESS',
        'seeded_scenarios', :v_seeded_count,
        'families', object_construct(
            'zscore', :v_zscore_on,
            'return', :v_return_on,
            'sizing', :v_sizing_on,
            'timing', :v_timing_on,
            'horizon', :v_horizon_on,
            'early_exit', :v_early_exit_on
        ),
        'simulation_result', :v_pw_result
    );
end;
$$;
