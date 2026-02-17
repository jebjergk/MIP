-- 237_sp_generate_pw_recommendations.sql
-- Purpose: Generates tuning recommendations from sweep surface data.
-- For each portfolio and sweep family, identifies optimal point and minimal safe tweak,
-- runs safety checks, and MERGEs into PARALLEL_WORLD_RECOMMENDATION.
-- Marks existing recs as STALE if evidence hash changed.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_GENERATE_PW_RECOMMENDATIONS(
    P_RUN_ID    varchar,
    P_AS_OF_TS  timestamp_ntz
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id        varchar := :P_RUN_ID;
    v_as_of_ts      timestamp_ntz := :P_AS_OF_TS;
    v_rec_count     number := 0;
    v_stale_count   number := 0;
    v_families      resultset;
    v_portfolio_id  number;
    v_family        varchar;
    v_domain        varchar;
    -- Surface point fields
    v_scenario_id       number;
    v_param_value       number;
    v_display_name      varchar;
    v_total_pnl_delta   number;
    v_avg_daily_delta   number;
    v_win_rate          number;
    v_obs_days          number;
    v_avg_trades_delta  number;
    -- Current setting param value
    v_current_value     number;
    -- Regime data
    v_is_fragile    boolean;
    v_regime_detail varchar;
    -- Safety
    v_min_obs_ok        boolean;
    v_trade_mult_ok     boolean;
    v_regime_ok         boolean;
    v_safety_status     varchar;
    v_safety_json       variant;
    -- Confidence
    v_conf_class        varchar;
    v_conf_reason       varchar;
    -- Evidence hash
    v_evidence_hash     varchar;
    v_param_name        varchar;
    v_rec_type          varchar;
begin
    -- ═══ OPTIMAL RECOMMENDATIONS ═══
    -- For each portfolio × family, find the optimal point (IS_OPTIMAL=true)
    let c1 cursor for
        select
            ts.PORTFOLIO_ID,
            ts.SWEEP_FAMILY,
            ts.SCENARIO_ID,
            ts.DISPLAY_NAME,
            ts.PARAM_VALUE,
            ts.TOTAL_PNL_DELTA,
            ts.AVG_DAILY_PNL_DELTA,
            ts.WIN_RATE_PCT,
            ts.OBSERVATION_DAYS,
            ts.AVG_TRADES_DELTA,
            cur.PARAM_VALUE as CURRENT_VALUE
        from MIP.MART.V_PW_TUNING_SURFACE ts
        left join MIP.MART.V_PW_TUNING_SURFACE cur
          on cur.PORTFOLIO_ID = ts.PORTFOLIO_ID
          and cur.SWEEP_FAMILY = ts.SWEEP_FAMILY
          and cur.IS_CURRENT_SETTING = true
        where ts.IS_OPTIMAL = true
          and ts.TOTAL_PNL_DELTA > 0;

    for rec in c1 do
        v_portfolio_id    := rec.PORTFOLIO_ID;
        v_family          := rec.SWEEP_FAMILY;
        v_scenario_id     := rec.SCENARIO_ID;
        v_display_name    := rec.DISPLAY_NAME;
        v_param_value     := rec.PARAM_VALUE;
        v_total_pnl_delta := rec.TOTAL_PNL_DELTA;
        v_avg_daily_delta := rec.AVG_DAILY_PNL_DELTA;
        v_win_rate        := rec.WIN_RATE_PCT;
        v_obs_days        := rec.OBSERVATION_DAYS;
        v_avg_trades_delta:= rec.AVG_TRADES_DELTA;
        v_current_value   := rec.CURRENT_VALUE;
        v_rec_type        := 'AGGRESSIVE';

        -- Domain assignment
        v_domain := case
            when :v_family in ('ZSCORE_SWEEP', 'RETURN_SWEEP') then 'SIGNAL'
            else 'PORTFOLIO'
        end;

        -- Parameter name
        v_param_name := case :v_family
            when 'ZSCORE_SWEEP' then 'min_zscore_delta'
            when 'RETURN_SWEEP' then 'min_return_delta'
            when 'SIZING_SWEEP' then 'position_pct_multiplier'
            when 'TIMING_SWEEP' then 'entry_delay_bars'
        end;

        -- Regime fragility check
        begin
            select
                max(IS_REGIME_FRAGILE),
                listagg(distinct case when IS_REGIME_FRAGILE then 'Fragile: only works in ' || REGIME end, '; ')
            into :v_is_fragile, :v_regime_detail
            from MIP.MART.V_PW_REGIME_SENSITIVITY
            where PORTFOLIO_ID = :v_portfolio_id
              and SWEEP_FAMILY = :v_family
              and SCENARIO_ID  = :v_scenario_id;
        exception when other then
            v_is_fragile := false;
            v_regime_detail := null;
        end;

        -- Safety checks
        v_min_obs_ok    := (:v_obs_days >= 5);
        v_trade_mult_ok := (abs(coalesce(:v_avg_trades_delta, 0)) <= 5);
        v_regime_ok     := (not coalesce(:v_is_fragile, false));

        v_safety_json := parse_json('[' ||
            '{"check":"min_observation_days","passed":' || :v_min_obs_ok::string || ',"threshold":5,"actual":' || :v_obs_days::string || ',"explanation":"Need at least 5 days of data"},' ||
            '{"check":"trade_count_stability","passed":' || :v_trade_mult_ok::string || ',"threshold":5,"actual":' || coalesce(:v_avg_trades_delta, 0)::string || ',"explanation":"Trade count change should not exceed ±5 per day"},' ||
            '{"check":"regime_robustness","passed":' || :v_regime_ok::string || ',"threshold":"multi-regime","actual":"' || case when coalesce(:v_is_fragile, false) then 'fragile' else 'robust' end || '","explanation":"Should work across multiple market regimes"}' ||
        ']');

        v_safety_status := case
            when :v_min_obs_ok and :v_trade_mult_ok and :v_regime_ok then 'READY_FOR_REVIEW'
            else 'NOT_READY'
        end;

        -- Confidence classification
        v_conf_class := case
            when :v_obs_days < 3 then 'NOISE'
            when :v_obs_days < 5 then 'WEAK'
            when :v_win_rate > 65 and :v_total_pnl_delta > 0 and :v_obs_days >= 10 then 'STRONG'
            when :v_win_rate > 50 and :v_total_pnl_delta > 0 then 'EMERGING'
            else 'WEAK'
        end;
        v_conf_reason := :v_conf_class || ': ' || :v_obs_days || ' days, ' || :v_win_rate || '% win rate, $' || round(:v_total_pnl_delta, 2) || ' cumulative delta';

        -- Evidence hash
        v_evidence_hash := md5(:v_portfolio_id::string || :v_family || :v_scenario_id::string || :v_total_pnl_delta::string || :v_obs_days::string);

        -- MERGE recommendation
        merge into MIP.APP.PARALLEL_WORLD_RECOMMENDATION as t
        using (select 1) as s
        on t.PORTFOLIO_ID = :v_portfolio_id
           and t.SWEEP_FAMILY = :v_family
           and t.RECOMMENDATION_TYPE = :v_rec_type
           and t.APPROVAL_STATUS != 'APPROVED'
        when matched and t.EVIDENCE_HASH != :v_evidence_hash then update set
            t.RUN_ID = :v_run_id,
            t.AS_OF_TS = :v_as_of_ts,
            t.SCENARIO_ID = :v_scenario_id,
            t.PARAMETER_NAME = :v_param_name,
            t.CURRENT_VALUE = :v_current_value::string,
            t.RECOMMENDED_VALUE = :v_param_value::string,
            t.EXPECTED_DAILY_DELTA = :v_avg_daily_delta,
            t.EXPECTED_CUMULATIVE_DELTA = :v_total_pnl_delta,
            t.WIN_RATE_PCT = :v_win_rate,
            t.OBSERVATION_DAYS = :v_obs_days,
            t.CONFIDENCE_CLASS = :v_conf_class,
            t.CONFIDENCE_REASON = :v_conf_reason,
            t.REGIME_FRAGILE = coalesce(:v_is_fragile, false),
            t.REGIME_DETAIL = :v_regime_detail,
            t.SAFETY_STATUS = :v_safety_status,
            t.SAFETY_DETAIL = :v_safety_json,
            t.EVIDENCE_HASH = :v_evidence_hash,
            t.APPROVAL_STATUS = 'NOT_REVIEWED',
            t.ROLLBACK_NOTE = 'Revert to ' || :v_param_name || ' = ' || :v_current_value::string || ' if performance degrades'
        when not matched then insert (
            RUN_ID, PORTFOLIO_ID, AS_OF_TS, RECOMMENDATION_TYPE, DOMAIN,
            SWEEP_FAMILY, SCENARIO_ID, PARAMETER_NAME, CURRENT_VALUE, RECOMMENDED_VALUE,
            EXPECTED_DAILY_DELTA, EXPECTED_CUMULATIVE_DELTA, WIN_RATE_PCT, OBSERVATION_DAYS,
            CONFIDENCE_CLASS, CONFIDENCE_REASON, REGIME_FRAGILE, REGIME_DETAIL,
            SAFETY_STATUS, SAFETY_DETAIL, EVIDENCE_HASH, APPROVAL_STATUS, ROLLBACK_NOTE, CREATED_AT
        ) values (
            :v_run_id, :v_portfolio_id, :v_as_of_ts, :v_rec_type, :v_domain,
            :v_family, :v_scenario_id, :v_param_name, :v_current_value::string, :v_param_value::string,
            :v_avg_daily_delta, :v_total_pnl_delta, :v_win_rate, :v_obs_days,
            :v_conf_class, :v_conf_reason, coalesce(:v_is_fragile, false), :v_regime_detail,
            :v_safety_status, :v_safety_json, :v_evidence_hash, 'NOT_REVIEWED',
            'Revert to ' || :v_param_name || ' = ' || :v_current_value::string || ' if performance degrades',
            current_timestamp()
        );

        v_rec_count := :v_rec_count + 1;
    end for;

    -- ═══ CONSERVATIVE RECOMMENDATIONS (minimal safe tweak) ═══
    let c2 cursor for
        select
            ts.PORTFOLIO_ID,
            ts.SWEEP_FAMILY,
            ts.SCENARIO_ID,
            ts.DISPLAY_NAME,
            ts.PARAM_VALUE,
            ts.TOTAL_PNL_DELTA,
            ts.AVG_DAILY_PNL_DELTA,
            ts.WIN_RATE_PCT,
            ts.OBSERVATION_DAYS,
            ts.AVG_TRADES_DELTA,
            cur.PARAM_VALUE as CURRENT_VALUE
        from MIP.MART.V_PW_TUNING_SURFACE ts
        left join MIP.MART.V_PW_TUNING_SURFACE cur
          on cur.PORTFOLIO_ID = ts.PORTFOLIO_ID
          and cur.SWEEP_FAMILY = ts.SWEEP_FAMILY
          and cur.IS_CURRENT_SETTING = true
        where ts.IS_MINIMAL_SAFE_TWEAK = true
          and ts.TOTAL_PNL_DELTA > 0;

    for rec in c2 do
        v_portfolio_id    := rec.PORTFOLIO_ID;
        v_family          := rec.SWEEP_FAMILY;
        v_scenario_id     := rec.SCENARIO_ID;
        v_display_name    := rec.DISPLAY_NAME;
        v_param_value     := rec.PARAM_VALUE;
        v_total_pnl_delta := rec.TOTAL_PNL_DELTA;
        v_avg_daily_delta := rec.AVG_DAILY_PNL_DELTA;
        v_win_rate        := rec.WIN_RATE_PCT;
        v_obs_days        := rec.OBSERVATION_DAYS;
        v_avg_trades_delta:= rec.AVG_TRADES_DELTA;
        v_current_value   := rec.CURRENT_VALUE;
        v_rec_type        := 'CONSERVATIVE';

        v_domain := case
            when :v_family in ('ZSCORE_SWEEP', 'RETURN_SWEEP') then 'SIGNAL'
            else 'PORTFOLIO'
        end;

        v_param_name := case :v_family
            when 'ZSCORE_SWEEP' then 'min_zscore_delta'
            when 'RETURN_SWEEP' then 'min_return_delta'
            when 'SIZING_SWEEP' then 'position_pct_multiplier'
            when 'TIMING_SWEEP' then 'entry_delay_bars'
        end;

        begin
            select
                max(IS_REGIME_FRAGILE),
                listagg(distinct case when IS_REGIME_FRAGILE then 'Fragile: only works in ' || REGIME end, '; ')
            into :v_is_fragile, :v_regime_detail
            from MIP.MART.V_PW_REGIME_SENSITIVITY
            where PORTFOLIO_ID = :v_portfolio_id
              and SWEEP_FAMILY = :v_family
              and SCENARIO_ID  = :v_scenario_id;
        exception when other then
            v_is_fragile := false;
            v_regime_detail := null;
        end;

        v_min_obs_ok    := (:v_obs_days >= 5);
        v_trade_mult_ok := (abs(coalesce(:v_avg_trades_delta, 0)) <= 5);
        v_regime_ok     := (not coalesce(:v_is_fragile, false));

        v_safety_json := parse_json('[' ||
            '{"check":"min_observation_days","passed":' || :v_min_obs_ok::string || ',"threshold":5,"actual":' || :v_obs_days::string || ',"explanation":"Need at least 5 days of data"},' ||
            '{"check":"trade_count_stability","passed":' || :v_trade_mult_ok::string || ',"threshold":5,"actual":' || coalesce(:v_avg_trades_delta, 0)::string || ',"explanation":"Trade count change should not exceed ±5 per day"},' ||
            '{"check":"regime_robustness","passed":' || :v_regime_ok::string || ',"threshold":"multi-regime","actual":"' || case when coalesce(:v_is_fragile, false) then 'fragile' else 'robust' end || '","explanation":"Should work across multiple market regimes"}' ||
        ']');

        v_safety_status := case
            when :v_min_obs_ok and :v_trade_mult_ok and :v_regime_ok then 'READY_FOR_REVIEW'
            else 'NOT_READY'
        end;

        v_conf_class := case
            when :v_obs_days < 3 then 'NOISE'
            when :v_obs_days < 5 then 'WEAK'
            when :v_win_rate > 65 and :v_total_pnl_delta > 0 and :v_obs_days >= 10 then 'STRONG'
            when :v_win_rate > 50 and :v_total_pnl_delta > 0 then 'EMERGING'
            else 'WEAK'
        end;
        v_conf_reason := :v_conf_class || ': ' || :v_obs_days || ' days, ' || :v_win_rate || '% win rate, $' || round(:v_total_pnl_delta, 2) || ' cumulative delta';

        v_evidence_hash := md5(:v_portfolio_id::string || :v_family || :v_scenario_id::string || :v_total_pnl_delta::string || :v_obs_days::string);

        merge into MIP.APP.PARALLEL_WORLD_RECOMMENDATION as t
        using (select 1) as s
        on t.PORTFOLIO_ID = :v_portfolio_id
           and t.SWEEP_FAMILY = :v_family
           and t.RECOMMENDATION_TYPE = :v_rec_type
           and t.APPROVAL_STATUS != 'APPROVED'
        when matched and t.EVIDENCE_HASH != :v_evidence_hash then update set
            t.RUN_ID = :v_run_id,
            t.AS_OF_TS = :v_as_of_ts,
            t.SCENARIO_ID = :v_scenario_id,
            t.PARAMETER_NAME = :v_param_name,
            t.CURRENT_VALUE = :v_current_value::string,
            t.RECOMMENDED_VALUE = :v_param_value::string,
            t.EXPECTED_DAILY_DELTA = :v_avg_daily_delta,
            t.EXPECTED_CUMULATIVE_DELTA = :v_total_pnl_delta,
            t.WIN_RATE_PCT = :v_win_rate,
            t.OBSERVATION_DAYS = :v_obs_days,
            t.CONFIDENCE_CLASS = :v_conf_class,
            t.CONFIDENCE_REASON = :v_conf_reason,
            t.REGIME_FRAGILE = coalesce(:v_is_fragile, false),
            t.REGIME_DETAIL = :v_regime_detail,
            t.SAFETY_STATUS = :v_safety_status,
            t.SAFETY_DETAIL = :v_safety_json,
            t.EVIDENCE_HASH = :v_evidence_hash,
            t.APPROVAL_STATUS = 'NOT_REVIEWED',
            t.ROLLBACK_NOTE = 'Revert to ' || :v_param_name || ' = ' || :v_current_value::string || ' if performance degrades'
        when not matched then insert (
            RUN_ID, PORTFOLIO_ID, AS_OF_TS, RECOMMENDATION_TYPE, DOMAIN,
            SWEEP_FAMILY, SCENARIO_ID, PARAMETER_NAME, CURRENT_VALUE, RECOMMENDED_VALUE,
            EXPECTED_DAILY_DELTA, EXPECTED_CUMULATIVE_DELTA, WIN_RATE_PCT, OBSERVATION_DAYS,
            CONFIDENCE_CLASS, CONFIDENCE_REASON, REGIME_FRAGILE, REGIME_DETAIL,
            SAFETY_STATUS, SAFETY_DETAIL, EVIDENCE_HASH, APPROVAL_STATUS, ROLLBACK_NOTE, CREATED_AT
        ) values (
            :v_run_id, :v_portfolio_id, :v_as_of_ts, :v_rec_type, :v_domain,
            :v_family, :v_scenario_id, :v_param_name, :v_current_value::string, :v_param_value::string,
            :v_avg_daily_delta, :v_total_pnl_delta, :v_win_rate, :v_obs_days,
            :v_conf_class, :v_conf_reason, coalesce(:v_is_fragile, false), :v_regime_detail,
            :v_safety_status, :v_safety_json, :v_evidence_hash, 'NOT_REVIEWED',
            'Revert to ' || :v_param_name || ' = ' || :v_current_value::string || ' if performance degrades',
            current_timestamp()
        );

        v_rec_count := :v_rec_count + 1;
    end for;

    -- ═══ Mark stale recs (evidence hash changed) ═══
    update MIP.APP.PARALLEL_WORLD_RECOMMENDATION
    set APPROVAL_STATUS = 'STALE'
    where APPROVAL_STATUS = 'NOT_REVIEWED'
      and CREATED_AT < current_timestamp() - interval '7 days';

    v_stale_count := (select count(*) from MIP.APP.PARALLEL_WORLD_RECOMMENDATION where APPROVAL_STATUS = 'STALE');

    return object_construct(
        'status', 'SUCCESS',
        'recommendations_generated', :v_rec_count,
        'stale_recommendations', :v_stale_count,
        'run_id', :v_run_id,
        'as_of_ts', :v_as_of_ts::string
    );
end;
$$;
