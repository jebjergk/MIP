-- 353_sp_intra_bridge_legacy_signals.sql
-- Purpose: Phase 3 bridge from legacy intraday RECOMMENDATION_LOG into INTRA_SIGNALS.
-- Idempotent via deterministic SIGNAL_NK_HASH upsert.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Seed intraday-v2 registry from legacy pattern definitions (idempotent).
merge into MIP.APP.INTRA_PATTERN_DEFS t
using (
    select
        p.PATTERN_ID as PATTERN_ID,
        p.NAME as PATTERN_NAME,
        'v1' as VERSION,
        'LEGACY' as PATTERN_FAMILY,
        p.PATTERN_TYPE as PATTERN_TYPE,
        p.PARAMS_JSON as PARAMS_JSON,
        p.ENABLED as IS_ENABLED
    from MIP.APP.PATTERN_DEFINITION p
    where p.PATTERN_TYPE in ('ORB', 'PULLBACK_CONTINUATION', 'MEAN_REVERSION')
      and coalesce(p.PARAMS_JSON:interval_minutes::number, 1440) = 15
) s
on t.PATTERN_ID = s.PATTERN_ID
when matched then update set
    t.PATTERN_NAME = s.PATTERN_NAME,
    t.PATTERN_FAMILY = s.PATTERN_FAMILY,
    t.PATTERN_TYPE = s.PATTERN_TYPE,
    t.PARAMS_JSON = s.PARAMS_JSON,
    t.IS_ENABLED = s.IS_ENABLED,
    t.UPDATED_AT = current_timestamp(),
    t.UPDATED_BY = current_user()
when not matched then insert (
    PATTERN_ID, PATTERN_NAME, VERSION, PATTERN_FAMILY, PATTERN_TYPE, PARAMS_JSON, IS_ENABLED
) values (
    s.PATTERN_ID, s.PATTERN_NAME, s.VERSION, s.PATTERN_FAMILY, s.PATTERN_TYPE, s.PARAMS_JSON, s.IS_ENABLED
);

create or replace procedure MIP.APP.SP_INTRA_BRIDGE_LEGACY_SIGNALS(
    P_START_TS timestamp_ntz,
    P_END_TS timestamp_ntz,
    P_METRIC_VERSION string default 'v1_1',
    P_BUCKET_VERSION string default 'v1',
    P_PATTERN_SET string default 'ALL',
    P_RUN_LEGACY_DETECTORS boolean default false
)
returns variant
language sql
execute as caller
as
$$
declare
    v_start_ts timestamp_ntz;
    v_end_ts timestamp_ntz;
    v_run_id string := coalesce(nullif(current_query_tag(), ''), uuid_string());
    v_legacy_total number := 0;
    v_eligible_with_state number := 0;
    v_rows_merged number := 0;
begin
    v_end_ts := coalesce(:P_END_TS, current_timestamp());
    v_start_ts := coalesce(:P_START_TS, dateadd(day, -30, :v_end_ts));

    if (:P_RUN_LEGACY_DETECTORS) then
        call MIP.APP.SP_INTRADAY_GENERATE_SIGNALS(15, :v_run_id);
    end if;

    create or replace temporary table MIP.APP.TMP_INTRA_PATTERN_SET as
    select PATTERN_ID
    from MIP.APP.PATTERN_DEFINITION
    where (
        :P_PATTERN_SET is null
        or upper(trim(:P_PATTERN_SET)) = 'ALL'
        or PATTERN_ID in (
            select try_to_number(trim(value))
            from table(split_to_table(:P_PATTERN_SET, ','))
            where try_to_number(trim(value)) is not null
        )
    );

    select count(*)
      into :v_legacy_total
      from MIP.APP.RECOMMENDATION_LOG rl
      join MIP.APP.PATTERN_DEFINITION pd
        on pd.PATTERN_ID = rl.PATTERN_ID
      join MIP.APP.TMP_INTRA_PATTERN_SET ps
        on ps.PATTERN_ID = rl.PATTERN_ID
     where rl.INTERVAL_MINUTES = 15
       and rl.TS between :v_start_ts and :v_end_ts
       and pd.PATTERN_TYPE in ('ORB', 'PULLBACK_CONTINUATION', 'MEAN_REVERSION');

    select count(*)
      into :v_eligible_with_state
      from MIP.APP.RECOMMENDATION_LOG rl
      join MIP.APP.PATTERN_DEFINITION pd
        on pd.PATTERN_ID = rl.PATTERN_ID
      join MIP.APP.TMP_INTRA_PATTERN_SET ps
        on ps.PATTERN_ID = rl.PATTERN_ID
      join MIP.APP.STATE_SNAPSHOT_15M ss
        on ss.MARKET_TYPE = rl.MARKET_TYPE
       and ss.SYMBOL = rl.SYMBOL
       and ss.INTERVAL_MINUTES = rl.INTERVAL_MINUTES
       and ss.TS = rl.TS
       and ss.METRIC_VERSION = :P_METRIC_VERSION
       and ss.BUCKET_VERSION = :P_BUCKET_VERSION
     where rl.INTERVAL_MINUTES = 15
       and rl.TS between :v_start_ts and :v_end_ts
       and pd.PATTERN_TYPE in ('ORB', 'PULLBACK_CONTINUATION', 'MEAN_REVERSION');

    merge into MIP.APP.INTRA_SIGNALS t
    using (
        with src as (
            select
                rl.PATTERN_ID,
                rl.MARKET_TYPE,
                rl.SYMBOL,
                rl.INTERVAL_MINUTES,
                rl.TS as SIGNAL_TS,
                rl.SCORE,
                rl.DETAILS,
                ss.STATE_BUCKET_ID,
                ss.TS as STATE_SNAPSHOT_TS,
                case
                    when upper(coalesce(rl.DETAILS:direction::string, '')) in ('BULLISH', 'LONG', 'BUY') then 'LONG'
                    when upper(coalesce(rl.DETAILS:direction::string, '')) in ('BEARISH', 'SHORT', 'SELL') then 'SHORT'
                    when rl.SCORE is not null and rl.SCORE < 0 then 'SHORT'
                    else 'LONG'
                end as SIGNAL_SIDE
            from MIP.APP.RECOMMENDATION_LOG rl
            join MIP.APP.PATTERN_DEFINITION pd
              on pd.PATTERN_ID = rl.PATTERN_ID
            join MIP.APP.TMP_INTRA_PATTERN_SET ps
              on ps.PATTERN_ID = rl.PATTERN_ID
            join MIP.APP.STATE_SNAPSHOT_15M ss
              on ss.MARKET_TYPE = rl.MARKET_TYPE
             and ss.SYMBOL = rl.SYMBOL
             and ss.INTERVAL_MINUTES = rl.INTERVAL_MINUTES
             and ss.TS = rl.TS
             and ss.METRIC_VERSION = :P_METRIC_VERSION
             and ss.BUCKET_VERSION = :P_BUCKET_VERSION
            where rl.INTERVAL_MINUTES = 15
              and rl.TS between :v_start_ts and :v_end_ts
              and pd.PATTERN_TYPE in ('ORB', 'PULLBACK_CONTINUATION', 'MEAN_REVERSION')
        )
        select
            sha2(
                concat(
                    coalesce(to_varchar(PATTERN_ID), ''), '|',
                    coalesce(MARKET_TYPE, ''), '|',
                    coalesce(SYMBOL, ''), '|',
                    coalesce(to_varchar(INTERVAL_MINUTES), ''), '|',
                    coalesce(to_varchar(SIGNAL_TS, 'YYYY-MM-DD HH24:MI:SS.FF3'), ''), '|',
                    coalesce(SIGNAL_SIDE, '')
                ),
                256
            ) as SIGNAL_NK_HASH,
            PATTERN_ID,
            MARKET_TYPE,
            SYMBOL,
            INTERVAL_MINUTES,
            SIGNAL_TS,
            SIGNAL_SIDE,
            SCORE,
            STATE_BUCKET_ID,
            STATE_SNAPSHOT_TS,
            object_construct(
                'legacy_details', DETAILS,
                'bridge_proc', 'SP_INTRA_BRIDGE_LEGACY_SIGNALS',
                'bridged_at', current_timestamp()
            ) as FEATURES_JSON,
            'LEGACY_PATTERN' as SOURCE_MODE,
            :v_run_id as RUN_ID,
            :P_METRIC_VERSION as METRIC_VERSION,
            :P_BUCKET_VERSION as BUCKET_VERSION,
            current_timestamp() as GENERATED_AT
        from src
    ) s
    on t.SIGNAL_NK_HASH = s.SIGNAL_NK_HASH
    when matched then update set
        t.PATTERN_ID = s.PATTERN_ID,
        t.MARKET_TYPE = s.MARKET_TYPE,
        t.SYMBOL = s.SYMBOL,
        t.INTERVAL_MINUTES = s.INTERVAL_MINUTES,
        t.SIGNAL_TS = s.SIGNAL_TS,
        t.SIGNAL_SIDE = s.SIGNAL_SIDE,
        t.SCORE = s.SCORE,
        t.STATE_BUCKET_ID = s.STATE_BUCKET_ID,
        t.STATE_SNAPSHOT_TS = s.STATE_SNAPSHOT_TS,
        t.FEATURES_JSON = s.FEATURES_JSON,
        t.SOURCE_MODE = s.SOURCE_MODE,
        t.RUN_ID = s.RUN_ID,
        t.METRIC_VERSION = s.METRIC_VERSION,
        t.BUCKET_VERSION = s.BUCKET_VERSION,
        t.GENERATED_AT = s.GENERATED_AT
    when not matched then insert (
        SIGNAL_NK_HASH, PATTERN_ID, MARKET_TYPE, SYMBOL, INTERVAL_MINUTES,
        SIGNAL_TS, SIGNAL_SIDE, SCORE, STATE_BUCKET_ID, STATE_SNAPSHOT_TS,
        FEATURES_JSON, SOURCE_MODE, RUN_ID, METRIC_VERSION, BUCKET_VERSION, GENERATED_AT
    ) values (
        s.SIGNAL_NK_HASH, s.PATTERN_ID, s.MARKET_TYPE, s.SYMBOL, s.INTERVAL_MINUTES,
        s.SIGNAL_TS, s.SIGNAL_SIDE, s.SCORE, s.STATE_BUCKET_ID, s.STATE_SNAPSHOT_TS,
        s.FEATURES_JSON, s.SOURCE_MODE, s.RUN_ID, s.METRIC_VERSION, s.BUCKET_VERSION, s.GENERATED_AT
    );

    v_rows_merged := sqlrowcount;
    drop table if exists MIP.APP.TMP_INTRA_PATTERN_SET;

    return object_construct(
        'status', 'SUCCESS',
        'start_ts', :v_start_ts,
        'end_ts', :v_end_ts,
        'metric_version', :P_METRIC_VERSION,
        'bucket_version', :P_BUCKET_VERSION,
        'pattern_set', :P_PATTERN_SET,
        'legacy_rows_in_window', :v_legacy_total,
        'eligible_rows_with_state', :v_eligible_with_state,
        'rows_merged', :v_rows_merged
    );
end;
$$;
