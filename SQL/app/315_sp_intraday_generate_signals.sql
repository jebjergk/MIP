-- 315_sp_intraday_generate_signals.sql
-- Purpose: Dispatcher that reads active intraday patterns from PATTERN_DEFINITION
-- and routes each to the correct detector stored procedure by PATTERN_TYPE.
-- Non-fatal: if one detector fails, others still run.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_INTRADAY_GENERATE_SIGNALS(
    P_INTERVAL_MINUTES number default 60,
    P_PARENT_RUN_ID    string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_total_patterns  number := 0;
    v_total_signals   number := 0;
    v_success_count   number := 0;
    v_fail_count      number := 0;
    v_pattern_id      number;
    v_pattern_name    string;
    v_pattern_type    string;
    v_market_type     string;
    v_detector_result variant;
    v_detail_array    variant := parse_json('[]');
    c_patterns cursor for
        select PATTERN_ID, NAME, PATTERN_TYPE, MARKET_TYPE
        from MIP.APP.TMP_INTRADAY_ACTIVE_PATTERNS
        order by PATTERN_ID;
begin
    -- Stage active intraday patterns into a temp table to avoid bind issues
    create or replace temporary table MIP.APP.TMP_INTRADAY_ACTIVE_PATTERNS as
    select
        PATTERN_ID,
        NAME,
        PATTERN_TYPE,
        coalesce(PARAMS_JSON:market_type::string, 'STOCK') as MARKET_TYPE
    from MIP.APP.PATTERN_DEFINITION
    where ENABLED = true
      and IS_ACTIVE = 'Y'
      and coalesce(PARAMS_JSON:interval_minutes::number, 1440) = :P_INTERVAL_MINUTES
      and PATTERN_TYPE in ('ORB', 'PULLBACK_CONTINUATION', 'MEAN_REVERSION');

    for rec in c_patterns do
        v_pattern_id   := rec.PATTERN_ID;
        v_pattern_name := rec.NAME;
        v_pattern_type := rec.PATTERN_TYPE;
        v_market_type  := rec.MARKET_TYPE;
        v_total_patterns := :v_total_patterns + 1;

        begin
            if (:v_pattern_type = 'ORB') then
                call MIP.APP.SP_DETECT_ORB(
                    :v_pattern_id, :v_market_type, :P_INTERVAL_MINUTES, :P_PARENT_RUN_ID
                );
                v_detector_result := (select * from table(result_scan(last_query_id())));
            elseif (:v_pattern_type = 'PULLBACK_CONTINUATION') then
                call MIP.APP.SP_DETECT_PULLBACK_CONTINUATION(
                    :v_pattern_id, :v_market_type, :P_INTERVAL_MINUTES, :P_PARENT_RUN_ID
                );
                v_detector_result := (select * from table(result_scan(last_query_id())));
            elseif (:v_pattern_type = 'MEAN_REVERSION') then
                call MIP.APP.SP_DETECT_MEAN_REVERSION(
                    :v_pattern_id, :v_market_type, :P_INTERVAL_MINUTES, :P_PARENT_RUN_ID
                );
                v_detector_result := (select * from table(result_scan(last_query_id())));
            else
                v_detector_result := object_construct(
                    'status', 'SKIP',
                    'reason', 'UNKNOWN_PATTERN_TYPE',
                    'pattern_type', :v_pattern_type
                );
            end if;

            v_total_signals := :v_total_signals +
                coalesce(v_detector_result:signals_inserted::number, 0);
            v_success_count := :v_success_count + 1;

            v_detail_array := array_append(:v_detail_array, object_construct(
                'pattern_id', :v_pattern_id,
                'pattern_name', :v_pattern_name,
                'pattern_type', :v_pattern_type,
                'status', 'SUCCESS',
                'signals_inserted', coalesce(v_detector_result:signals_inserted::number, 0),
                'result', :v_detector_result
            ));
        exception
            when other then
                v_fail_count := :v_fail_count + 1;
                v_detail_array := array_append(:v_detail_array, object_construct(
                    'pattern_id', :v_pattern_id,
                    'pattern_name', :v_pattern_name,
                    'pattern_type', :v_pattern_type,
                    'status', 'FAIL',
                    'error', sqlerrm
                ));
        end;
    end for;

    drop table if exists MIP.APP.TMP_INTRADAY_ACTIVE_PATTERNS;

    return object_construct(
        'status', case
            when :v_fail_count > 0 and :v_success_count > 0 then 'PARTIAL'
            when :v_fail_count > 0 then 'FAIL'
            when :v_total_patterns = 0 then 'SKIP_NO_ACTIVE_PATTERNS'
            else 'SUCCESS'
        end,
        'total_patterns', :v_total_patterns,
        'patterns_succeeded', :v_success_count,
        'patterns_failed', :v_fail_count,
        'total_signals', :v_total_signals,
        'interval_minutes', :P_INTERVAL_MINUTES,
        'details', :v_detail_array
    );
end;
$$;
