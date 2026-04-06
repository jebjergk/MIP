-- 072_sp_backfill_short_momentum.sql
-- Purpose: Research backfill — historical SHORT momentum signals/outcomes using the same
--          generation + evaluation pipeline as live (SP_GENERATE_MOMENTUM_RECS, SP_EVALUATE_RECOMMENDATIONS).
--          Idempotent (dedupe in generator + MERGE in evaluator). Chunkable by P_BATCH_DAYS.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_BACKFILL_SHORT_MOMENTUM(
    P_START_DATE       date,
    P_END_DATE         date,
    P_MARKET_TYPE      string default 'STOCK',
    P_INTERVAL_MINUTES number default 1440,
    P_MIN_RETURN       number default null,
    P_MIN_ZSCORE       float default null,
    P_LOOKBACK_DAYS    number default null,
    P_BATCH_DAYS       number default 7,
    P_DRY_RUN          boolean default false
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id                    string := coalesce(nullif(current_query_tag(), ''), uuid_string());
    v_gate_prev                 string;
    v_batch_start               date;
    v_batch_end                 date;
    v_batch_end_planned         date;
    v_cur_day                   date;
    v_rng_lo_str                varchar;
    v_rng_hi_str                varchar;
    v_eval_from                 timestamp_ntz;
    v_eval_to                   timestamp_ntz;
    v_day_ts                    timestamp_ntz;
    v_batch_started_at          timestamp_ntz;
    v_batch_ended_at            timestamp_ntz;
    v_log_short_before          number;
    v_log_short_after           number;
    v_log_long_before           number;
    v_log_long_after            number;
    v_out_short_before          number;
    v_out_short_after           number;
    v_batches_done              number := 0;
    v_active_short_patterns     number;
    v_rb_cnt                    number;
    v_rb_sum                    number;
    v_ra_cnt                    number;
    v_ra_sum                    number;
    v_research_short_in_range   number;
    v_base_short_join           number;
    v_trusted_short_join        number;
    v_total_short_log_range     number;
    v_total_short_out_ok_range  number;
    v_sample_log                variant;
    v_sample_out                variant;
    v_by_pattern                variant;
    v_top_symbols               variant;
    v_gate_captured             boolean := false;
begin
    if (:P_START_DATE > :P_END_DATE) then
        return object_construct('error', true, 'message', 'P_START_DATE must be <= P_END_DATE');
    end if;
    if (:P_BATCH_DAYS is null or :P_BATCH_DAYS < 1) then
        return object_construct('error', true, 'message', 'P_BATCH_DAYS must be >= 1');
    end if;

    select count(*), coalesce(sum(OUTCOMES_N), 0)
      into :v_rb_cnt, :v_rb_sum
      from MIP.MART.V_SYMBOL_TRAINING_READINESS;

    select count(*)
      into :v_active_short_patterns
      from MIP.APP.PATTERN_DEFINITION
     where upper(coalesce(PATTERN_TYPE, '')) = 'MOMENTUM_SHORT'
       and coalesce(IS_ACTIVE, 'N') = 'Y'
       and coalesce(ENABLED, true);

    if (not :P_DRY_RUN and v_active_short_patterns = 0) then
        return object_construct(
            'error', true,
            'message', 'No active MOMENTUM_SHORT patterns (IS_ACTIVE=Y, ENABLED). Nothing to backfill.'
        );
    end if;

    if (not :P_DRY_RUN) then
        select CONFIG_VALUE
          into :v_gate_prev
          from MIP.APP.APP_CONFIG
         where CONFIG_KEY = 'SHORT_MOMENTUM_GENERATION_ENABLED'
         limit 1;
        if (v_gate_prev is null) then
            v_gate_prev := 'false';
        end if;

        update MIP.APP.APP_CONFIG
           set CONFIG_VALUE = 'true'
         where CONFIG_KEY = 'SHORT_MOMENTUM_GENERATION_ENABLED';
        v_gate_captured := true;
    end if;

    begin
        v_cur_day := :P_START_DATE;
        v_batch_start := :P_START_DATE;
        v_batch_end_planned := least(dateadd(day, :P_BATCH_DAYS - 1, v_batch_start), :P_END_DATE);
        v_batch_started_at := current_timestamp();

        while (v_cur_day <= :P_END_DATE) loop
            if (not :P_DRY_RUN) then
                if (v_cur_day = v_batch_start) then
                    v_rng_lo_str := to_char(v_batch_start, 'YYYY-MM-DD');
                    v_rng_hi_str := to_char(v_batch_end_planned, 'YYYY-MM-DD');

                    select count(*)
                      into :v_log_short_before
                      from MIP.APP.RECOMMENDATION_LOG
                     where coalesce(SIGNAL_DIRECTION, '') = 'SHORT'
                       and upper(MARKET_TYPE) = upper(:P_MARKET_TYPE)
                       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES
                       and TS::date between to_date(:v_rng_lo_str) and to_date(:v_rng_hi_str);

                    select count(*)
                      into :v_log_long_before
                      from MIP.APP.RECOMMENDATION_LOG
                     where (SIGNAL_DIRECTION is null or SIGNAL_DIRECTION = 'LONG')
                       and upper(MARKET_TYPE) = upper(:P_MARKET_TYPE)
                       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES
                       and TS::date between to_date(:v_rng_lo_str) and to_date(:v_rng_hi_str);

                    select count(*)
                      into :v_out_short_before
                      from MIP.APP.RECOMMENDATION_OUTCOMES o
                      join MIP.APP.RECOMMENDATION_LOG r
                        on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
                     where coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
                       and upper(r.MARKET_TYPE) = upper(:P_MARKET_TYPE)
                       and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
                       and r.TS::date between to_date(:v_rng_lo_str) and to_date(:v_rng_hi_str)
                       and o.EVAL_STATUS = 'SUCCESS';
                end if;

                v_day_ts := to_timestamp_ntz(to_char(v_cur_day, 'YYYY-MM-DD'));
                call MIP.APP.SP_GENERATE_MOMENTUM_RECS(
                    :P_MIN_RETURN,
                    :P_MARKET_TYPE,
                    :P_INTERVAL_MINUTES,
                    :P_LOOKBACK_DAYS,
                    :P_MIN_ZSCORE,
                    :v_day_ts
                );
            end if;

            if (
                v_cur_day = v_batch_end_planned
                or v_cur_day = :P_END_DATE
            ) then
                v_batch_end := v_cur_day;

                if (not :P_DRY_RUN) then
                    v_eval_from := to_timestamp_ntz(to_char(v_batch_start, 'YYYY-MM-DD'));
                    v_eval_to := dateadd(
                        nanosecond,
                        -1,
                        dateadd(day, 1, to_timestamp_ntz(to_char(v_batch_end, 'YYYY-MM-DD')))
                    );

                    call MIP.APP.SP_EVALUATE_RECOMMENDATIONS(:v_eval_from, :v_eval_to, 0);

                    select count(*)
                      into :v_log_short_after
                      from MIP.APP.RECOMMENDATION_LOG
                     where coalesce(SIGNAL_DIRECTION, '') = 'SHORT'
                       and upper(MARKET_TYPE) = upper(:P_MARKET_TYPE)
                       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES
                       and TS::date between to_date(:v_rng_lo_str) and to_date(:v_rng_hi_str);

                    select count(*)
                      into :v_log_long_after
                      from MIP.APP.RECOMMENDATION_LOG
                     where (SIGNAL_DIRECTION is null or SIGNAL_DIRECTION = 'LONG')
                       and upper(MARKET_TYPE) = upper(:P_MARKET_TYPE)
                       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES
                       and TS::date between to_date(:v_rng_lo_str) and to_date(:v_rng_hi_str);

                    select count(*)
                      into :v_out_short_after
                      from MIP.APP.RECOMMENDATION_OUTCOMES o
                      join MIP.APP.RECOMMENDATION_LOG r
                        on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
                     where coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
                       and upper(r.MARKET_TYPE) = upper(:P_MARKET_TYPE)
                       and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
                       and r.TS::date between to_date(:v_rng_lo_str) and to_date(:v_rng_hi_str)
                       and o.EVAL_STATUS = 'SUCCESS';
                end if;

                v_batch_ended_at := current_timestamp();
                v_batches_done := v_batches_done + 1;

                if (not :P_DRY_RUN) then
                    call MIP.APP.SP_LOG_EVENT(
                        'RESEARCH_BACKFILL',
                        'SP_BACKFILL_SHORT_MOMENTUM',
                        'BATCH',
                        null,
                        object_construct(
                            'run_id', :v_run_id,
                            'batch_start', :v_batch_start,
                            'batch_end', :v_batch_end,
                            'batch_end_planned', :v_batch_end_planned,
                            'market_type', :P_MARKET_TYPE,
                            'interval_minutes', :P_INTERVAL_MINUTES,
                            'short_log_delta', coalesce(:v_log_short_after, 0) - coalesce(:v_log_short_before, 0),
                            'long_log_delta', coalesce(:v_log_long_after, 0) - coalesce(:v_log_long_before, 0),
                            'short_outcomes_success_delta',
                                coalesce(:v_out_short_after, 0) - coalesce(:v_out_short_before, 0),
                            'started_at', :v_batch_started_at,
                            'completed_at', :v_batch_ended_at,
                            'eval_from_ts', :v_eval_from,
                            'eval_to_ts', :v_eval_to
                        ),
                        null,
                        null,
                        null,
                        null,
                        :v_run_id
                    );
                end if;

                v_batch_start := dateadd(day, 1, v_cur_day);
                if (v_batch_start <= :P_END_DATE) then
                    v_batch_end_planned := least(
                        dateadd(day, :P_BATCH_DAYS - 1, v_batch_start),
                        :P_END_DATE
                    );
                    v_batch_started_at := current_timestamp();
                end if;
            end if;

            v_cur_day := dateadd(day, 1, v_cur_day);
        end loop;

        if (not :P_DRY_RUN) then
            update MIP.APP.APP_CONFIG
               set CONFIG_VALUE = :v_gate_prev
             where CONFIG_KEY = 'SHORT_MOMENTUM_GENERATION_ENABLED';
        end if;

        select count(*), coalesce(sum(OUTCOMES_N), 0)
          into :v_ra_cnt, :v_ra_sum
          from MIP.MART.V_SYMBOL_TRAINING_READINESS;

        if (not :P_DRY_RUN) then
            select count(*)
              into :v_total_short_log_range
              from MIP.APP.RECOMMENDATION_LOG
             where coalesce(SIGNAL_DIRECTION, '') = 'SHORT'
               and upper(MARKET_TYPE) = upper(:P_MARKET_TYPE)
               and INTERVAL_MINUTES = :P_INTERVAL_MINUTES
               and TS::date between :P_START_DATE and :P_END_DATE;

            select count(*)
              into :v_total_short_out_ok_range
              from MIP.APP.RECOMMENDATION_OUTCOMES o
              join MIP.APP.RECOMMENDATION_LOG r
                on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
             where coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
               and upper(r.MARKET_TYPE) = upper(:P_MARKET_TYPE)
               and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
               and r.TS::date between :P_START_DATE and :P_END_DATE
               and o.EVAL_STATUS = 'SUCCESS';

            select count(*)
              into :v_research_short_in_range
              from MIP.MART.V_SIGNAL_OUTCOMES_BASE_RESEARCH v
             where v.SIGNAL_DIRECTION = 'SHORT'
               and upper(v.MARKET_TYPE) = upper(:P_MARKET_TYPE)
               and v.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
               and v.SIGNAL_TS::date between :P_START_DATE and :P_END_DATE;

            select count(*)
              into :v_base_short_join
              from MIP.MART.V_SIGNAL_OUTCOMES_BASE b
              join MIP.APP.RECOMMENDATION_LOG r
                on r.RECOMMENDATION_ID = b.RECOMMENDATION_ID
             where coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
               and b.SIGNAL_TS::date between :P_START_DATE and :P_END_DATE;

            select count(*)
              into :v_trusted_short_join
              from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS t
              join MIP.APP.RECOMMENDATION_LOG r
                on r.RECOMMENDATION_ID = t.RECOMMENDATION_ID
             where coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT';

            select array_agg(
                       object_construct(
                           'PATTERN_ID', q.PATTERN_ID,
                           'N', q.N
                       )
                   ) within group (order by q.N desc)
              into :v_by_pattern
              from (
                       select r.PATTERN_ID, count(*) as N
                         from MIP.APP.RECOMMENDATION_LOG r
                        where coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
                          and upper(r.MARKET_TYPE) = upper(:P_MARKET_TYPE)
                          and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
                          and r.TS::date between :P_START_DATE and :P_END_DATE
                        group by r.PATTERN_ID
                   ) q;

            select array_agg(
                       object_construct(
                           'SYMBOL', q.SYMBOL,
                           'N', q.N
                       )
                   ) within group (order by q.N desc)
              into :v_top_symbols
              from (
                       select r.SYMBOL, count(*) as N
                         from MIP.APP.RECOMMENDATION_LOG r
                        where coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
                          and upper(r.MARKET_TYPE) = upper(:P_MARKET_TYPE)
                          and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
                          and r.TS::date between :P_START_DATE and :P_END_DATE
                        group by r.SYMBOL
                        order by N desc
                        limit 10
                   ) q;

            select array_agg(
                       object_construct(
                           'RECOMMENDATION_ID', s.RECOMMENDATION_ID,
                           'PATTERN_ID', s.PATTERN_ID,
                           'SYMBOL', s.SYMBOL,
                           'TS', s.TS,
                           'SIGNAL_DIRECTION', s.SIGNAL_DIRECTION,
                           'SCORE', s.SCORE
                       )
                   ) within group (order by s.TS desc)
              into :v_sample_log
              from (
                       select l.RECOMMENDATION_ID,
                              l.PATTERN_ID,
                              l.SYMBOL,
                              l.TS,
                              l.SIGNAL_DIRECTION,
                              l.SCORE
                         from MIP.APP.RECOMMENDATION_LOG l
                        where coalesce(l.SIGNAL_DIRECTION, '') = 'SHORT'
                          and upper(l.MARKET_TYPE) = upper(:P_MARKET_TYPE)
                          and l.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
                          and l.TS::date between :P_START_DATE and :P_END_DATE
                        order by l.TS desc
                        limit 5
                   ) s;

            select array_agg(
                       object_construct(
                           'RECOMMENDATION_ID', s.RECOMMENDATION_ID,
                           'HORIZON_BARS', s.HORIZON_BARS,
                           'ENTRY_TS', s.ENTRY_TS,
                           'EXIT_TS', s.EXIT_TS,
                           'REALIZED_RETURN', s.REALIZED_RETURN,
                           'EVAL_STATUS', s.EVAL_STATUS,
                           'SYMBOL', s.SYMBOL
                       )
                   ) within group (order by s.ENTRY_TS desc)
              into :v_sample_out
              from (
                       select o.RECOMMENDATION_ID,
                              o.HORIZON_BARS,
                              o.ENTRY_TS,
                              o.EXIT_TS,
                              o.REALIZED_RETURN,
                              o.EVAL_STATUS,
                              r.SYMBOL
                         from MIP.APP.RECOMMENDATION_OUTCOMES o
                         join MIP.APP.RECOMMENDATION_LOG r
                           on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
                        where coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
                          and upper(r.MARKET_TYPE) = upper(:P_MARKET_TYPE)
                          and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
                          and r.TS::date between :P_START_DATE and :P_END_DATE
                        order by o.ENTRY_TS desc
                        limit 5
                   ) s;
        end if;

        return object_construct(
            'dry_run', :P_DRY_RUN,
            'run_id', :v_run_id,
            'parameters',
                object_construct(
                    'P_START_DATE', :P_START_DATE,
                    'P_END_DATE', :P_END_DATE,
                    'P_MARKET_TYPE', :P_MARKET_TYPE,
                    'P_INTERVAL_MINUTES', :P_INTERVAL_MINUTES,
                    'P_MIN_RETURN', :P_MIN_RETURN,
                    'P_MIN_ZSCORE', :P_MIN_ZSCORE,
                    'P_LOOKBACK_DAYS', :P_LOOKBACK_DAYS,
                    'P_BATCH_DAYS', :P_BATCH_DAYS
                ),
            'batches_completed', :v_batches_done,
            'short_gate_restored', not :P_DRY_RUN,
            'summary',
                iff(
                    :P_DRY_RUN,
                    null,
                    object_construct(
                        'short_signals_in_date_range', :v_total_short_log_range,
                        'short_outcomes_success_in_date_range', :v_total_short_out_ok_range,
                        'short_outcomes_rows_research_in_date_range', :v_research_short_in_range,
                        'by_pattern_id', :v_by_pattern,
                        'top_10_symbols', :v_top_symbols
                    )
                ),
            'samples',
                iff(
                    :P_DRY_RUN,
                    null,
                    object_construct(
                        'recommendation_log_short', :v_sample_log,
                        'recommendation_outcomes_short', :v_sample_out
                    )
                ),
            'safety',
                object_construct(
                    'readiness_before',
                        object_construct('row_count', :v_rb_cnt, 'sum_outcomes_n', :v_rb_sum),
                    'readiness_after',
                        iff(:P_DRY_RUN, null, object_construct('row_count', :v_ra_cnt, 'sum_outcomes_n', :v_ra_sum)),
                    'readiness_unchanged',
                        iff(:P_DRY_RUN, null, :v_ra_cnt = :v_rb_cnt and :v_ra_sum = :v_rb_sum),
                    'v_signal_outcomes_base_short_rows_in_range',
                        iff(:P_DRY_RUN, null, :v_base_short_join),
                    'v_trusted_signals_latest_ts_short_rows',
                        iff(:P_DRY_RUN, null, :v_trusted_short_join),
                    'note',
                        'LONG-safe views filter SHORT; base/trusted SHORT counts should be 0.'
                ),
            'audit_log_hint',
                'select * from MIP.APP.MIP_AUDIT_LOG where EVENT_NAME = ''SP_BACKFILL_SHORT_MOMENTUM'' '
                || 'and DETAILS:run_id::string = ''' || :v_run_id || ''' order by EVENT_TS;'
        );
    exception
        when other then
            begin
                if (v_gate_captured) then
                    update MIP.APP.APP_CONFIG
                       set CONFIG_VALUE = :v_gate_prev
                     where CONFIG_KEY = 'SHORT_MOMENTUM_GENERATION_ENABLED';
                end if;
            end;
            raise;
    end;
end;
$$;
