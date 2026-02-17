-- 235_sp_run_parallel_worlds.sql
-- Purpose: Counterfactual simulation engine for Parallel Worlds.
-- Computes "what would have happened" for each active scenario and each active portfolio
-- for a given AS_OF_TS. Results are written to PARALLEL_WORLD_RESULT.
--
-- This procedure is READ-ONLY against core pipeline tables — it never writes to
-- PORTFOLIO_TRADES, PORTFOLIO_POSITIONS, PORTFOLIO_DAILY, or any other core table.
-- It only writes to PARALLEL_WORLD_RESULT and PARALLEL_WORLD_RUN_LOG.
--
-- Scenario types:
--   THRESHOLD — re-evaluate signals with modified min_zscore / min_return thresholds
--   SIZING   — same trades as actual, but with adjusted position sizing
--   TIMING   — same signals as actual, but with shifted entry bar
--   BASELINE — do nothing (cash-only), shows the cost/benefit of any trading

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_RUN_PARALLEL_WORLDS(
    P_RUN_ID        varchar,
    P_AS_OF_TS      timestamp_ntz,
    P_PORTFOLIO_ID  number default null,
    P_SCENARIO_SET  varchar default 'DEFAULT_ACTIVE'
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id            varchar := :P_RUN_ID;
    v_as_of_ts          timestamp_ntz := :P_AS_OF_TS;
    v_portfolios        resultset;
    v_scenarios         resultset;
    v_portfolio_id      number;
    v_scenario_id       number;
    v_scenario_name     varchar;
    v_scenario_type     varchar;
    v_params_json       variant;
    v_portfolio_count   number := 0;
    v_scenario_count    number := 0;
    v_result_count      number := 0;
    v_error_count       number := 0;
    v_starting_cash     number(18,2);
    v_max_positions     number;
    v_max_position_pct  number(18,6);
    v_episode_id        number;
    v_slippage_bps      number(18,8);
    v_fee_bps           number(18,8);
    v_min_fee           number(18,8);
    v_spread_bps        number(18,8);
    v_actual_equity     number(18,4);
    v_actual_cash       number(18,4);
    v_actual_pnl        number(18,4);
    v_actual_return     number(18,8);
    v_actual_drawdown   number(18,8);
    v_actual_trades     number;
    v_actual_positions  number;
    -- Simulation outputs
    v_sim_trades        number;
    v_sim_pnl           number(18,4);
    v_sim_return        number(18,8);
    v_sim_equity        number(18,4);
    v_sim_cash          number(18,4);
    v_sim_positions     number;
    v_sim_drawdown      number(18,8);
    v_cf_decision_trace variant;
    v_cf_trades_json    variant;
    -- Gate info
    v_risk_status       varchar;
    v_entries_blocked   boolean;
    v_block_reason      varchar;
    v_actual_signals_json variant;
    v_actual_trades_json  variant;
    v_actual_decision_trace variant;
    v_min_scenario_id   number;
    -- Temp for new signal PnL
    v_new_signal_pnl    number(18,4);
    v_newly_eligible    number;
    v_newly_excluded    number;
begin
    -- Log run start
    insert into MIP.APP.PARALLEL_WORLD_RUN_LOG (
        RUN_ID, AS_OF_TS, PORTFOLIO_ID, SCENARIO_SET, STATUS, STARTED_AT
    ) values (
        :v_run_id, :v_as_of_ts, :P_PORTFOLIO_ID, :P_SCENARIO_SET, 'RUNNING', current_timestamp()
    );

    -- Load execution cost params
    select coalesce(max(case when CONFIG_KEY = 'SLIPPAGE_BPS' then CONFIG_VALUE::number end), 2),
           coalesce(max(case when CONFIG_KEY = 'FEE_BPS' then CONFIG_VALUE::number end), 1),
           coalesce(max(case when CONFIG_KEY = 'MIN_FEE' then CONFIG_VALUE::number end), 0),
           coalesce(max(case when CONFIG_KEY = 'SPREAD_BPS' then CONFIG_VALUE::number end), 0)
      into :v_slippage_bps, :v_fee_bps, :v_min_fee, :v_spread_bps
      from MIP.APP.APP_CONFIG
     where CONFIG_KEY in ('SLIPPAGE_BPS', 'FEE_BPS', 'MIN_FEE', 'SPREAD_BPS');

    -- Min scenario id (for writing ACTUAL once)
    v_min_scenario_id := (select min(SCENARIO_ID) from MIP.APP.PARALLEL_WORLD_SCENARIO where IS_ACTIVE = true);

    -- Determine portfolio scope
    if (:P_PORTFOLIO_ID is not null and :P_PORTFOLIO_ID > 0) then
        v_portfolios := (select PORTFOLIO_ID from MIP.APP.PORTFOLIO where PORTFOLIO_ID = :P_PORTFOLIO_ID and STATUS = 'ACTIVE');
    else
        v_portfolios := (select PORTFOLIO_ID from MIP.APP.PORTFOLIO where STATUS = 'ACTIVE' order by PORTFOLIO_ID);
    end if;

    -- ===== MAIN LOOP =====
    for p_rec in v_portfolios do
        v_portfolio_id := p_rec.PORTFOLIO_ID;
        v_portfolio_count := :v_portfolio_count + 1;

        -- Load portfolio config
        begin
            select
                coalesce(ae.START_EQUITY, p.STARTING_CASH),
                coalesce(pp.MAX_POSITIONS, 5),
                coalesce(pp.MAX_POSITION_PCT, 0.05),
                ae.EPISODE_ID
            into :v_starting_cash, :v_max_positions, :v_max_position_pct, :v_episode_id
            from MIP.APP.PORTFOLIO p
            left join MIP.APP.PORTFOLIO_PROFILE pp on pp.PROFILE_ID = p.PROFILE_ID
            left join (
                select PORTFOLIO_ID, EPISODE_ID, START_EQUITY
                from MIP.APP.PORTFOLIO_EPISODE
                where STATUS = 'ACTIVE'
                qualify row_number() over (partition by PORTFOLIO_ID order by START_TS desc) = 1
            ) ae on ae.PORTFOLIO_ID = p.PORTFOLIO_ID
            where p.PORTFOLIO_ID = :v_portfolio_id;
        exception when other then
            v_error_count := :v_error_count + 1;
            continue;
        end;

        -- Load actual world metrics
        begin
            select TOTAL_EQUITY, CASH, DAILY_PNL, DAILY_RETURN, DRAWDOWN, TRADES_ACTUAL, OPEN_POSITIONS
            into :v_actual_equity, :v_actual_cash, :v_actual_pnl, :v_actual_return,
                 :v_actual_drawdown, :v_actual_trades, :v_actual_positions
            from MIP.MART.V_PARALLEL_WORLD_ACTUAL
            where PORTFOLIO_ID = :v_portfolio_id and AS_OF_TS::date = :v_as_of_ts::date
            limit 1;
        exception when other then
            v_actual_equity := :v_starting_cash;
            v_actual_cash := :v_starting_cash;
            v_actual_pnl := 0;
            v_actual_return := 0;
            v_actual_drawdown := 0;
            v_actual_trades := 0;
            v_actual_positions := 0;
        end;

        -- ===== Write ACTUAL world row (once per portfolio) =====
        begin
            v_actual_signals_json := (
                select coalesce(array_agg(object_construct(
                    'symbol', SYMBOL, 'market_type', MARKET_TYPE,
                    'score', round(SCORE, 6), 'horizon_bars', HORIZON_BARS, 'ts', TS::string
                )), array_construct())
                from MIP.MART.V_PORTFOLIO_SIGNALS
                where INTERVAL_MINUTES = 1440 and TS::date = :v_as_of_ts::date
            );
        exception when other then
            v_actual_signals_json := array_construct();
        end;

        begin
            v_actual_trades_json := (
                select coalesce(array_agg(object_construct(
                    'symbol', SYMBOL, 'side', SIDE, 'price', round(PRICE, 4),
                    'quantity', round(QUANTITY, 4), 'notional', round(NOTIONAL, 2),
                    'realized_pnl', round(coalesce(REALIZED_PNL, 0), 2)
                )), array_construct())
                from MIP.APP.PORTFOLIO_TRADES
                where PORTFOLIO_ID = :v_portfolio_id and TRADE_TS::date = :v_as_of_ts::date
                  and (EPISODE_ID = :v_episode_id or (:v_episode_id is null and EPISODE_ID is null))
            );
        exception when other then
            v_actual_trades_json := array_construct();
        end;

        begin
            select coalesce(RISK_STATUS, 'OK'), coalesce(ENTRIES_BLOCKED, false), BLOCK_REASON
            into :v_risk_status, :v_entries_blocked, :v_block_reason
            from MIP.MART.V_PORTFOLIO_RISK_GATE where PORTFOLIO_ID = :v_portfolio_id limit 1;
        exception when other then
            v_risk_status := 'UNKNOWN';
            v_entries_blocked := false;
            v_block_reason := null;
        end;

        v_actual_decision_trace := object_construct(
            'world', 'ACTUAL',
            'decision_trace', array_construct(
                object_construct('gate', 'RISK_STATE', 'status', iff(:v_entries_blocked, 'BLOCKED', 'PASSED'),
                    'risk_status', :v_risk_status, 'entries_blocked', :v_entries_blocked, 'block_reason', :v_block_reason),
                object_construct('gate', 'CAPACITY', 'status', iff(:v_actual_positions >= :v_max_positions, 'FULL', 'PASSED'),
                    'open_positions', :v_actual_positions, 'max_positions', :v_max_positions,
                    'remaining', :v_max_positions - :v_actual_positions)
            ),
            'signals_available', :v_actual_signals_json,
            'trades_executed', :v_actual_trades_json
        );

        merge into MIP.APP.PARALLEL_WORLD_RESULT as target
        using (select :v_run_id as RUN_ID, :v_portfolio_id as PORTFOLIO_ID, :v_as_of_ts as AS_OF_TS,
                      0 as SCENARIO_ID, 'ACTUAL' as WORLD_KEY, :v_episode_id as EPISODE_ID,
                      :v_actual_trades as TRADES_SIMULATED, :v_actual_pnl as PNL_SIMULATED,
                      :v_actual_return as RETURN_PCT_SIMULATED, :v_actual_drawdown as MAX_DRAWDOWN_PCT_SIMULATED,
                      :v_actual_equity as END_EQUITY_SIMULATED, :v_actual_cash as CASH_END_SIMULATED,
                      :v_actual_positions as OPEN_POSITIONS_END, :v_actual_decision_trace as RESULT_JSON
        ) as source
        on target.RUN_ID = source.RUN_ID and target.PORTFOLIO_ID = source.PORTFOLIO_ID
           and target.AS_OF_TS = source.AS_OF_TS and target.SCENARIO_ID = source.SCENARIO_ID
        when matched then update set
            target.WORLD_KEY = source.WORLD_KEY, target.EPISODE_ID = source.EPISODE_ID,
            target.TRADES_SIMULATED = source.TRADES_SIMULATED, target.PNL_SIMULATED = source.PNL_SIMULATED,
            target.RETURN_PCT_SIMULATED = source.RETURN_PCT_SIMULATED,
            target.MAX_DRAWDOWN_PCT_SIMULATED = source.MAX_DRAWDOWN_PCT_SIMULATED,
            target.END_EQUITY_SIMULATED = source.END_EQUITY_SIMULATED,
            target.CASH_END_SIMULATED = source.CASH_END_SIMULATED,
            target.OPEN_POSITIONS_END = source.OPEN_POSITIONS_END,
            target.RESULT_JSON = source.RESULT_JSON
        when not matched then insert (
            RUN_ID, PORTFOLIO_ID, AS_OF_TS, SCENARIO_ID, WORLD_KEY, EPISODE_ID,
            TRADES_SIMULATED, PNL_SIMULATED, RETURN_PCT_SIMULATED,
            MAX_DRAWDOWN_PCT_SIMULATED, END_EQUITY_SIMULATED, CASH_END_SIMULATED,
            OPEN_POSITIONS_END, RESULT_JSON, CREATED_AT
        ) values (
            source.RUN_ID, source.PORTFOLIO_ID, source.AS_OF_TS, source.SCENARIO_ID,
            source.WORLD_KEY, source.EPISODE_ID,
            source.TRADES_SIMULATED, source.PNL_SIMULATED, source.RETURN_PCT_SIMULATED,
            source.MAX_DRAWDOWN_PCT_SIMULATED, source.END_EQUITY_SIMULATED,
            source.CASH_END_SIMULATED, source.OPEN_POSITIONS_END,
            source.RESULT_JSON, current_timestamp()
        );
        v_result_count := :v_result_count + 1;

        -- ===== SCENARIO LOOP =====
        if (:P_SCENARIO_SET = 'SWEEP') then
            v_scenarios := (
                select SCENARIO_ID, NAME, SCENARIO_TYPE, PARAMS_JSON
                from MIP.APP.PARALLEL_WORLD_SCENARIO
                where IS_ACTIVE = true and IS_SWEEP = true
                order by SWEEP_FAMILY, SWEEP_ORDER
            );
        else
            v_scenarios := (
                select SCENARIO_ID, NAME, SCENARIO_TYPE, PARAMS_JSON
                from MIP.APP.PARALLEL_WORLD_SCENARIO
                where IS_ACTIVE = true and coalesce(IS_SWEEP, false) = false
                order by SCENARIO_ID
            );
        end if;

        for s_rec in v_scenarios do
            v_scenario_id := s_rec.SCENARIO_ID;
            v_scenario_name := s_rec.NAME;
            v_scenario_type := s_rec.SCENARIO_TYPE;
            v_params_json := s_rec.PARAMS_JSON;
            v_scenario_count := :v_scenario_count + 1;

            -- Reset simulation outputs
            v_sim_trades := 0;
            v_sim_pnl := 0;
            v_sim_return := 0;
            v_sim_equity := :v_starting_cash;
            v_sim_cash := :v_starting_cash;
            v_sim_positions := 0;
            v_sim_drawdown := 0;
            v_cf_decision_trace := null;
            v_cf_trades_json := array_construct();

            begin
                -- ==== BASELINE: DO_NOTHING ====
                if (:v_scenario_type = 'BASELINE') then
                    v_sim_equity := :v_starting_cash;
                    v_sim_cash := :v_starting_cash;
                    v_sim_pnl := 0;
                    v_sim_return := 0;
                    v_sim_trades := 0;
                    v_sim_positions := 0;
                    v_cf_decision_trace := object_construct(
                        'world', 'COUNTERFACTUAL', 'scenario', :v_scenario_name,
                        'decision_trace', array_construct(
                            object_construct('gate', 'BASELINE_SKIP', 'status', 'BLOCKED',
                                'reason', 'DO_NOTHING scenario — all entries skipped')
                        ),
                        'trades_simulated', array_construct()
                    );

                -- ==== SIZING SCENARIOS ====
                elseif (:v_scenario_type = 'SIZING') then
                    -- Same trades as actual, but with adjusted position sizing
                    -- Scale PnL proportionally to the sizing multiplier
                    declare
                        v_pct_multiplier number(18,6) := coalesce(:v_params_json:position_pct_multiplier::number, 1.0);
                        v_adj_max_pct number(18,6);
                        v_sizing_pnl_delta number(18,4) := 0;
                    begin
                        v_adj_max_pct := least(:v_max_position_pct * :v_pct_multiplier, 1.0);

                        -- Approximate: scale total daily PnL by the sizing multiplier
                        -- This is valid because PnL scales linearly with position size
                        v_sizing_pnl_delta := :v_actual_pnl * (:v_pct_multiplier - 1.0);
                        v_sim_pnl := :v_actual_pnl + :v_sizing_pnl_delta;
                        v_sim_equity := :v_actual_equity + :v_sizing_pnl_delta;
                        v_sim_return := iff(:v_starting_cash > 0, :v_sim_pnl / :v_starting_cash, 0);
                        v_sim_trades := :v_actual_trades;
                        v_sim_positions := :v_actual_positions;
                        v_sim_cash := :v_actual_cash;

                        v_cf_trades_json := (
                            select coalesce(array_agg(object_construct(
                                'symbol', SYMBOL, 'side', SIDE,
                                'orig_notional', round(NOTIONAL, 2),
                                'adj_notional', round(NOTIONAL * :v_pct_multiplier, 2),
                                'orig_pnl', round(coalesce(REALIZED_PNL, 0), 2),
                                'adj_pnl', round(coalesce(REALIZED_PNL, 0) * :v_pct_multiplier, 2)
                            )), array_construct())
                            from MIP.APP.PORTFOLIO_TRADES
                            where PORTFOLIO_ID = :v_portfolio_id and TRADE_TS::date = :v_as_of_ts::date
                              and (EPISODE_ID = :v_episode_id or (:v_episode_id is null and EPISODE_ID is null))
                        );

                        v_cf_decision_trace := object_construct(
                            'world', 'COUNTERFACTUAL', 'scenario', :v_scenario_name,
                            'decision_trace', array_construct(
                                object_construct('gate', 'SIZING', 'status', 'MODIFIED',
                                    'original_max_position_pct', :v_max_position_pct,
                                    'adjusted_max_position_pct', :v_adj_max_pct,
                                    'multiplier', :v_pct_multiplier,
                                    'pnl_delta', round(:v_sizing_pnl_delta, 2))
                            ),
                            'trades_simulated', :v_cf_trades_json
                        );
                    end;

                -- ==== TIMING SCENARIOS ====
                elseif (:v_scenario_type = 'TIMING') then
                    declare
                        v_delay_bars number := coalesce(:v_params_json:entry_delay_bars::number, 1);
                        v_timing_pnl number(18,4) := 0;
                        v_timing_count number := 0;
                    begin
                        -- Get PnL impact of delaying entry by N bars on BUY trades
                        begin
                            select
                                coalesce(sum(-1 * (vb_delayed.CLOSE - t.PRICE) * t.QUANTITY), 0),
                                count(vb_delayed.CLOSE)
                            into :v_timing_pnl, :v_timing_count
                            from MIP.APP.PORTFOLIO_TRADES t
                            join MIP.MART.V_BAR_INDEX vb_entry
                              on vb_entry.SYMBOL = t.SYMBOL
                             and vb_entry.MARKET_TYPE = t.MARKET_TYPE
                             and vb_entry.INTERVAL_MINUTES = 1440
                             and vb_entry.TS = t.TRADE_TS
                            join MIP.MART.V_BAR_INDEX vb_delayed
                              on vb_delayed.SYMBOL = t.SYMBOL
                             and vb_delayed.MARKET_TYPE = t.MARKET_TYPE
                             and vb_delayed.INTERVAL_MINUTES = 1440
                             and vb_delayed.BAR_INDEX = vb_entry.BAR_INDEX + :v_delay_bars
                            where t.PORTFOLIO_ID = :v_portfolio_id
                              and t.TRADE_TS::date = :v_as_of_ts::date
                              and t.SIDE = 'BUY'
                              and (t.EPISODE_ID = :v_episode_id or (:v_episode_id is null and t.EPISODE_ID is null));
                        exception when other then
                            v_timing_pnl := 0;
                            v_timing_count := 0;
                        end;

                        v_sim_pnl := :v_actual_pnl + :v_timing_pnl;
                        v_sim_equity := :v_actual_equity + :v_timing_pnl;
                        v_sim_return := iff(:v_starting_cash > 0, :v_sim_pnl / :v_starting_cash, 0);
                        v_sim_trades := :v_actual_trades;
                        v_sim_positions := :v_actual_positions;
                        v_sim_cash := :v_actual_cash;

                        v_cf_trades_json := (
                            select coalesce(array_agg(object_construct(
                                'symbol', t.SYMBOL, 'orig_price', round(t.PRICE, 4),
                                'delayed_price', round(vb_delayed.CLOSE, 4),
                                'pnl_impact', round(-1 * (vb_delayed.CLOSE - t.PRICE) * t.QUANTITY, 2)
                            )), array_construct())
                            from MIP.APP.PORTFOLIO_TRADES t
                            join MIP.MART.V_BAR_INDEX vb_entry
                              on vb_entry.SYMBOL = t.SYMBOL and vb_entry.MARKET_TYPE = t.MARKET_TYPE
                             and vb_entry.INTERVAL_MINUTES = 1440 and vb_entry.TS = t.TRADE_TS
                            join MIP.MART.V_BAR_INDEX vb_delayed
                              on vb_delayed.SYMBOL = t.SYMBOL and vb_delayed.MARKET_TYPE = t.MARKET_TYPE
                             and vb_delayed.INTERVAL_MINUTES = 1440
                             and vb_delayed.BAR_INDEX = vb_entry.BAR_INDEX + :v_delay_bars
                            where t.PORTFOLIO_ID = :v_portfolio_id and t.TRADE_TS::date = :v_as_of_ts::date
                              and t.SIDE = 'BUY'
                              and (t.EPISODE_ID = :v_episode_id or (:v_episode_id is null and t.EPISODE_ID is null))
                        );

                        v_cf_decision_trace := object_construct(
                            'world', 'COUNTERFACTUAL', 'scenario', :v_scenario_name,
                            'decision_trace', array_construct(
                                object_construct('gate', 'TIMING', 'status', 'DELAYED',
                                    'delay_bars', :v_delay_bars, 'trades_affected', :v_timing_count,
                                    'pnl_impact', round(:v_timing_pnl, 2))
                            ),
                            'trades_simulated', :v_cf_trades_json
                        );
                    end;

                -- ==== THRESHOLD SCENARIOS ====
                elseif (:v_scenario_type = 'THRESHOLD') then
                    declare
                        v_zscore_delta number(18,6) := coalesce(:v_params_json:min_zscore_delta::number, 0);
                        v_return_delta number(18,8) := coalesce(:v_params_json:min_return_delta::number, 0);
                    begin
                        v_newly_eligible := 0;
                        v_newly_excluded := 0;
                        v_new_signal_pnl := 0;

                        -- Count signals that would become newly eligible or excluded
                        -- Note: V_PORTFOLIO_SIGNALS is keyed on RECOMMENDATION_ID + HORIZON_BARS,
                        -- so we dedup to avoid counting the same signal multiple times
                        begin
                            select
                                coalesce(sum(case
                                    when rl.SCORE >= (coalesce(pd.PARAMS_JSON:min_return::float, 0.002) + :v_return_delta)
                                     and ps.RECOMMENDATION_ID is null
                                    then 1 else 0 end), 0),
                                coalesce(sum(case
                                    when rl.SCORE < (coalesce(pd.PARAMS_JSON:min_return::float, 0.002) + :v_return_delta)
                                     and ps.RECOMMENDATION_ID is not null
                                    then 1 else 0 end), 0)
                            into :v_newly_eligible, :v_newly_excluded
                            from MIP.APP.RECOMMENDATION_LOG rl
                            join MIP.APP.PATTERN_DEFINITION pd on pd.PATTERN_ID = rl.PATTERN_ID
                            left join (
                                select distinct RECOMMENDATION_ID
                                from MIP.MART.V_PORTFOLIO_SIGNALS
                            ) ps on ps.RECOMMENDATION_ID = rl.RECOMMENDATION_ID
                            where rl.TS::date = :v_as_of_ts::date and rl.INTERVAL_MINUTES = 1440;
                        exception when other then
                            v_newly_eligible := 0;
                            v_newly_excluded := 0;
                        end;

                        -- Estimate PnL from newly eligible signals using realized outcomes
                        begin
                            select coalesce(sum(pnl_est), 0)
                            into :v_new_signal_pnl
                            from (
                                select
                                    rl.RECOMMENDATION_ID,
                                    ro.REALIZED_RETURN * (:v_starting_cash * :v_max_position_pct) as pnl_est
                                from MIP.APP.RECOMMENDATION_LOG rl
                                join MIP.APP.RECOMMENDATION_OUTCOMES ro
                                  on ro.RECOMMENDATION_ID = rl.RECOMMENDATION_ID and ro.EVAL_STATUS = 'SUCCESS'
                                join MIP.APP.PATTERN_DEFINITION pd on pd.PATTERN_ID = rl.PATTERN_ID
                                left join (
                                    select distinct RECOMMENDATION_ID
                                    from MIP.MART.V_PORTFOLIO_SIGNALS
                                ) ps on ps.RECOMMENDATION_ID = rl.RECOMMENDATION_ID
                                where rl.TS::date = :v_as_of_ts::date and rl.INTERVAL_MINUTES = 1440
                                  and ps.RECOMMENDATION_ID is null
                                  and rl.SCORE >= (coalesce(pd.PARAMS_JSON:min_return::float, 0.002) + :v_return_delta)
                                qualify row_number() over (partition by rl.RECOMMENDATION_ID order by ro.HORIZON_BARS) = 1
                            );
                        exception when other then
                            v_new_signal_pnl := 0;
                        end;

                        v_sim_pnl := :v_actual_pnl + :v_new_signal_pnl;
                        v_sim_equity := :v_actual_equity + :v_new_signal_pnl;
                        v_sim_return := iff(:v_starting_cash > 0, :v_sim_pnl / :v_starting_cash, 0);
                        v_sim_trades := :v_actual_trades + :v_newly_eligible - :v_newly_excluded;
                        v_sim_positions := :v_actual_positions + :v_newly_eligible;
                        v_sim_cash := :v_actual_cash;

                        -- Build threshold-specific trades detail
                        v_cf_trades_json := (
                            select coalesce(array_agg(object_construct(
                                'symbol', rl.SYMBOL,
                                'market_type', rl.MARKET_TYPE,
                                'score', round(rl.SCORE, 6),
                                'orig_min_return', coalesce(pd.PARAMS_JSON:min_return::float, 0.002),
                                'adj_min_return', round(coalesce(pd.PARAMS_JSON:min_return::float, 0.002) + :v_return_delta, 6),
                                'was_trusted', iff(ps.RECOMMENDATION_ID is not null, true, false),
                                'passes_adjusted', iff(rl.SCORE >= (coalesce(pd.PARAMS_JSON:min_return::float, 0.002) + :v_return_delta), true, false),
                                'trust_label', coalesce(tc.TRUST_LABEL, 'UNKNOWN')
                            )), array_construct())
                            from MIP.APP.RECOMMENDATION_LOG rl
                            join MIP.APP.PATTERN_DEFINITION pd on pd.PATTERN_ID = rl.PATTERN_ID
                            left join (
                                select distinct RECOMMENDATION_ID
                                from MIP.MART.V_PORTFOLIO_SIGNALS
                            ) ps on ps.RECOMMENDATION_ID = rl.RECOMMENDATION_ID
                            left join MIP.APP.V_TRUSTED_SIGNAL_CLASSIFICATION tc
                              on tc.SYMBOL = rl.SYMBOL
                             and tc.MARKET_TYPE = rl.MARKET_TYPE
                             and tc.INTERVAL_MINUTES = rl.INTERVAL_MINUTES
                             and tc.TS = rl.TS
                             and tc.PATTERN_ID = rl.PATTERN_ID
                            where rl.TS::date = :v_as_of_ts::date and rl.INTERVAL_MINUTES = 1440
                        );

                        v_cf_decision_trace := object_construct(
                            'world', 'COUNTERFACTUAL', 'scenario', :v_scenario_name,
                            'decision_trace', array_construct(
                                object_construct('gate', 'THRESHOLD', 'status', 'MODIFIED',
                                    'zscore_delta', :v_zscore_delta, 'return_delta', :v_return_delta,
                                    'newly_eligible', :v_newly_eligible, 'newly_excluded', :v_newly_excluded,
                                    'estimated_new_pnl', round(:v_new_signal_pnl, 2)),
                                object_construct('gate', 'TRUST', 'status', 'INFO',
                                    'note', 'Threshold scenarios modify signal filtering; trust gate still applies to pattern-level eligibility')
                            ),
                            'trades_simulated', :v_cf_trades_json
                        );
                    end;

                else
                    -- Unknown scenario type — skip
                    continue;
                end if;

                -- ==== MERGE counterfactual result ====
                merge into MIP.APP.PARALLEL_WORLD_RESULT as target
                using (select :v_run_id as RUN_ID, :v_portfolio_id as PORTFOLIO_ID, :v_as_of_ts as AS_OF_TS,
                              :v_scenario_id as SCENARIO_ID, 'COUNTERFACTUAL' as WORLD_KEY,
                              :v_episode_id as EPISODE_ID,
                              :v_sim_trades as TRADES_SIMULATED, :v_sim_pnl as PNL_SIMULATED,
                              :v_sim_return as RETURN_PCT_SIMULATED, :v_sim_drawdown as MAX_DRAWDOWN_PCT_SIMULATED,
                              :v_sim_equity as END_EQUITY_SIMULATED, :v_sim_cash as CASH_END_SIMULATED,
                              :v_sim_positions as OPEN_POSITIONS_END, :v_cf_decision_trace as RESULT_JSON
                ) as source
                on target.RUN_ID = source.RUN_ID and target.PORTFOLIO_ID = source.PORTFOLIO_ID
                   and target.AS_OF_TS = source.AS_OF_TS and target.SCENARIO_ID = source.SCENARIO_ID
                when matched then update set
                    target.WORLD_KEY = source.WORLD_KEY, target.EPISODE_ID = source.EPISODE_ID,
                    target.TRADES_SIMULATED = source.TRADES_SIMULATED, target.PNL_SIMULATED = source.PNL_SIMULATED,
                    target.RETURN_PCT_SIMULATED = source.RETURN_PCT_SIMULATED,
                    target.MAX_DRAWDOWN_PCT_SIMULATED = source.MAX_DRAWDOWN_PCT_SIMULATED,
                    target.END_EQUITY_SIMULATED = source.END_EQUITY_SIMULATED,
                    target.CASH_END_SIMULATED = source.CASH_END_SIMULATED,
                    target.OPEN_POSITIONS_END = source.OPEN_POSITIONS_END,
                    target.RESULT_JSON = source.RESULT_JSON
                when not matched then insert (
                    RUN_ID, PORTFOLIO_ID, AS_OF_TS, SCENARIO_ID, WORLD_KEY, EPISODE_ID,
                    TRADES_SIMULATED, PNL_SIMULATED, RETURN_PCT_SIMULATED,
                    MAX_DRAWDOWN_PCT_SIMULATED, END_EQUITY_SIMULATED, CASH_END_SIMULATED,
                    OPEN_POSITIONS_END, RESULT_JSON, CREATED_AT
                ) values (
                    source.RUN_ID, source.PORTFOLIO_ID, source.AS_OF_TS, source.SCENARIO_ID,
                    source.WORLD_KEY, source.EPISODE_ID,
                    source.TRADES_SIMULATED, source.PNL_SIMULATED, source.RETURN_PCT_SIMULATED,
                    source.MAX_DRAWDOWN_PCT_SIMULATED, source.END_EQUITY_SIMULATED,
                    source.CASH_END_SIMULATED, source.OPEN_POSITIONS_END,
                    source.RESULT_JSON, current_timestamp()
                );
                v_result_count := :v_result_count + 1;

            exception when other then
                v_error_count := :v_error_count + 1;
            end;
        end for; -- scenarios
    end for; -- portfolios

    -- Update run log
    update MIP.APP.PARALLEL_WORLD_RUN_LOG
       set STATUS = iff(:v_error_count = 0, 'COMPLETED', 'COMPLETED_WITH_ERRORS'),
           COMPLETED_AT = current_timestamp(),
           DETAILS = object_construct(
               'portfolio_count', :v_portfolio_count, 'scenario_count', :v_scenario_count,
               'result_count', :v_result_count, 'error_count', :v_error_count
           )
     where RUN_ID = :v_run_id;

    return object_construct(
        'status', iff(:v_error_count = 0, 'COMPLETED', 'COMPLETED_WITH_ERRORS'),
        'run_id', :v_run_id, 'as_of_ts', :v_as_of_ts::string,
        'portfolio_count', :v_portfolio_count, 'scenario_count', :v_scenario_count,
        'result_count', :v_result_count, 'error_count', :v_error_count
    );
end;
$$;
