-- 358_sp_intraday_backfill_training.sql
-- Purpose: Phase 7 chunked/resumable/idempotent backfill orchestration for intraday v2.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_INTRADAY_BACKFILL_TRAINING(
    P_START_TS timestamp_ntz,
    P_END_TS timestamp_ntz,
    P_PATTERN_SET string default 'ALL',
    P_FORCE_RECOMPUTE boolean default false,
    P_CHUNK_DAYS number default 1,
    P_METRIC_VERSION string default 'v1_1',
    P_BUCKET_VERSION string default 'v1',
    P_WINDOW_DAYS number default 90,
    P_MIN_SAMPLE number default 20,
    P_TRUST_CONFIG_VERSION string default 'BASELINE_FIXED20',
    P_TERRAIN_VERSION string default 'v1'
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id string := uuid_string();
    v_start_ts timestamp_ntz := :P_START_TS;
    v_end_ts timestamp_ntz := :P_END_TS;
    v_pattern_set string := coalesce(:P_PATTERN_SET, 'ALL');
    v_pattern_set_hash string := sha2(coalesce(:P_PATTERN_SET, 'ALL'), 256);
    v_chunk_days number := greatest(coalesce(:P_CHUNK_DAYS, 1), 1);
    v_chunk_start timestamp_ntz;
    v_chunk_end timestamp_ntz;
    v_chunk_id string;
    v_chunk_idx number := 0;
    v_trust_version string;
    v_existing_status string;
    v_done_count number := 0;
    v_skip_count number := 0;
    v_fail_count number := 0;
begin
    if (v_start_ts is null or v_end_ts is null or v_start_ts > v_end_ts) then
        return object_construct('status', 'FAIL', 'error', 'Invalid start/end range');
    end if;

    v_trust_version := sha2(
        concat(
            coalesce(:P_METRIC_VERSION, ''), '|',
            coalesce(:P_BUCKET_VERSION, ''), '|',
            to_varchar(coalesce(:P_WINDOW_DAYS, 90)), '|',
            to_varchar(coalesce(:P_MIN_SAMPLE, 20)), '|',
            coalesce(:P_TRUST_CONFIG_VERSION, ''), '|',
            'exact->regime_only->global'
        ),
        256
    );

    create or replace temporary table MIP.APP.TMP_BACKFILL_PATTERN_SET as
    select PATTERN_ID
    from MIP.APP.INTRA_PATTERN_DEFS
    where (
        upper(trim(:v_pattern_set)) = 'ALL'
        or PATTERN_ID in (
            select try_to_number(trim(value))
            from table(split_to_table(:v_pattern_set, ','))
            where try_to_number(trim(value)) is not null
        )
    )
      and coalesce(IS_ENABLED, true);

    v_chunk_start := v_start_ts;
    while (v_chunk_start <= v_end_ts) do
        v_chunk_idx := v_chunk_idx + 1;
        v_chunk_end := least(
            dateadd(millisecond, -1, dateadd(day, v_chunk_days, v_chunk_start)),
            v_end_ts
        );
        v_chunk_id := lpad(to_varchar(v_chunk_idx), 5, '0') || '_' || to_varchar(v_chunk_start, 'YYYYMMDDHH24MISS');

        merge into MIP.APP.INTRA_BACKFILL_RUN_LOG t
        using (
            select
                :v_run_id as RUN_ID,
                :v_chunk_id as CHUNK_ID,
                :v_chunk_start as START_TS,
                :v_chunk_end as END_TS,
                :v_pattern_set as PATTERN_SET,
                :v_pattern_set_hash as PATTERN_SET_HASH,
                :P_FORCE_RECOMPUTE as FORCE_RECOMPUTE,
                :P_METRIC_VERSION as METRIC_VERSION,
                :P_BUCKET_VERSION as BUCKET_VERSION,
                :v_trust_version as TRUST_VERSION,
                :P_TERRAIN_VERSION as TERRAIN_VERSION
        ) s
          on t.CHUNK_ID = s.CHUNK_ID
         and t.START_TS = s.START_TS
         and t.END_TS = s.END_TS
         and t.PATTERN_SET_HASH = s.PATTERN_SET_HASH
         and t.METRIC_VERSION = s.METRIC_VERSION
         and t.BUCKET_VERSION = s.BUCKET_VERSION
         and t.TRUST_VERSION = s.TRUST_VERSION
         and t.TERRAIN_VERSION = s.TERRAIN_VERSION
        when matched then update set
            t.FORCE_RECOMPUTE = s.FORCE_RECOMPUTE,
            t.UPDATED_AT = current_timestamp()
        when not matched then insert (
            RUN_ID, CHUNK_ID, START_TS, END_TS, PATTERN_SET, PATTERN_SET_HASH,
            FORCE_RECOMPUTE, METRIC_VERSION, BUCKET_VERSION, TRUST_VERSION, TERRAIN_VERSION,
            STATUS, CREATED_AT, UPDATED_AT
        ) values (
            s.RUN_ID, s.CHUNK_ID, s.START_TS, s.END_TS, s.PATTERN_SET, s.PATTERN_SET_HASH,
            s.FORCE_RECOMPUTE, s.METRIC_VERSION, s.BUCKET_VERSION, s.TRUST_VERSION, s.TERRAIN_VERSION,
            'PENDING', current_timestamp(), current_timestamp()
        );

        select STATUS
          into :v_existing_status
          from MIP.APP.INTRA_BACKFILL_RUN_LOG
         where CHUNK_ID = :v_chunk_id
           and START_TS = :v_chunk_start
           and END_TS = :v_chunk_end
           and PATTERN_SET_HASH = :v_pattern_set_hash
           and METRIC_VERSION = :P_METRIC_VERSION
           and BUCKET_VERSION = :P_BUCKET_VERSION
           and TRUST_VERSION = :v_trust_version
           and TERRAIN_VERSION = :P_TERRAIN_VERSION
         order by UPDATED_AT desc
         limit 1;

        if (coalesce(v_existing_status, '') = 'DONE' and not :P_FORCE_RECOMPUTE) then
            v_skip_count := v_skip_count + 1;
            v_chunk_start := dateadd(millisecond, 1, v_chunk_end);
            continue;
        end if;

        update MIP.APP.INTRA_BACKFILL_RUN_LOG
           set STATUS = 'RUNNING',
               STARTED_AT = current_timestamp(),
               ERROR_MESSAGE = null,
               UPDATED_AT = current_timestamp()
         where CHUNK_ID = :v_chunk_id
           and START_TS = :v_chunk_start
           and END_TS = :v_chunk_end
           and PATTERN_SET_HASH = :v_pattern_set_hash
           and METRIC_VERSION = :P_METRIC_VERSION
           and BUCKET_VERSION = :P_BUCKET_VERSION
           and TRUST_VERSION = :v_trust_version
           and TERRAIN_VERSION = :P_TERRAIN_VERSION;

        begin
            if (:P_FORCE_RECOMPUTE) then
                delete from MIP.APP.OPPORTUNITY_TERRAIN_15M
                 where TS between :v_chunk_start and :v_chunk_end
                   and METRIC_VERSION = :P_METRIC_VERSION
                   and BUCKET_VERSION = :P_BUCKET_VERSION
                   and TERRAIN_VERSION = :P_TERRAIN_VERSION
                   and PATTERN_ID in (select PATTERN_ID from MIP.APP.TMP_BACKFILL_PATTERN_SET);

                delete from MIP.APP.INTRA_TRUST_STATS
                 where CALCULATED_AT = :v_chunk_end
                   and METRIC_VERSION = :P_METRIC_VERSION
                   and BUCKET_VERSION = :P_BUCKET_VERSION
                   and TRUST_VERSION = :v_trust_version
                   and PATTERN_ID in (select PATTERN_ID from MIP.APP.TMP_BACKFILL_PATTERN_SET);

                delete from MIP.APP.INTRA_OUTCOMES
                 where SIGNAL_ID in (
                     select SIGNAL_ID
                     from MIP.APP.INTRA_SIGNALS
                     where SIGNAL_TS between :v_chunk_start and :v_chunk_end
                       and METRIC_VERSION = :P_METRIC_VERSION
                       and BUCKET_VERSION = :P_BUCKET_VERSION
                       and PATTERN_ID in (select PATTERN_ID from MIP.APP.TMP_BACKFILL_PATTERN_SET)
                 );

                delete from MIP.APP.INTRA_SIGNALS
                 where SIGNAL_TS between :v_chunk_start and :v_chunk_end
                   and METRIC_VERSION = :P_METRIC_VERSION
                   and BUCKET_VERSION = :P_BUCKET_VERSION
                   and PATTERN_ID in (select PATTERN_ID from MIP.APP.TMP_BACKFILL_PATTERN_SET);

                delete from MIP.APP.STATE_TRANSITIONS
                 where TS_FROM between :v_chunk_start and :v_chunk_end
                   and METRIC_VERSION = :P_METRIC_VERSION
                   and BUCKET_VERSION = :P_BUCKET_VERSION;

                delete from MIP.APP.STATE_SNAPSHOT_15M
                 where TS between :v_chunk_start and :v_chunk_end
                   and METRIC_VERSION = :P_METRIC_VERSION
                   and BUCKET_VERSION = :P_BUCKET_VERSION;
            end if;

            call MIP.APP.SP_INTRA_BUILD_STATE_SNAPSHOT_15M(:v_chunk_start, :v_chunk_end, :P_METRIC_VERSION, :P_BUCKET_VERSION);
            call MIP.APP.SP_INTRA_BUILD_STATE_TRANSITIONS(:v_chunk_start, :v_chunk_end, :P_METRIC_VERSION, :P_BUCKET_VERSION);
            call MIP.APP.SP_INTRA_BRIDGE_LEGACY_SIGNALS(:v_chunk_start, :v_chunk_end, :P_METRIC_VERSION, :P_BUCKET_VERSION, :v_pattern_set, false);
            call MIP.APP.SP_INTRA_COMPUTE_OUTCOMES(:v_chunk_start, :v_chunk_end, :P_METRIC_VERSION, :P_BUCKET_VERSION, :v_pattern_set, 0.0);
            call MIP.APP.SP_INTRA_COMPUTE_TRUST_SNAPSHOTS(:v_chunk_end, :P_WINDOW_DAYS, :P_MIN_SAMPLE, :P_METRIC_VERSION, :P_BUCKET_VERSION, :v_pattern_set, :P_TRUST_CONFIG_VERSION, :P_TERRAIN_VERSION);
            call MIP.APP.SP_INTRA_COMPUTE_OPPORTUNITY_TERRAIN(:v_chunk_start, :v_chunk_end, :v_chunk_end, :P_METRIC_VERSION, :P_BUCKET_VERSION, :v_pattern_set, :v_trust_version, :P_TERRAIN_VERSION, 20.0, 0.5, 0.3, 0.2);

            update MIP.APP.INTRA_BACKFILL_RUN_LOG
               set STATUS = 'DONE',
                   COMPLETED_AT = current_timestamp(),
                   ROWS_STATE_SNAPSHOT = (
                       select count(*) from MIP.APP.STATE_SNAPSHOT_15M
                       where TS between :v_chunk_start and :v_chunk_end
                         and METRIC_VERSION = :P_METRIC_VERSION
                         and BUCKET_VERSION = :P_BUCKET_VERSION
                   ),
                   ROWS_STATE_TRANSITIONS = (
                       select count(*) from MIP.APP.STATE_TRANSITIONS
                       where TS_FROM between :v_chunk_start and :v_chunk_end
                         and METRIC_VERSION = :P_METRIC_VERSION
                         and BUCKET_VERSION = :P_BUCKET_VERSION
                   ),
                   ROWS_SIGNALS = (
                       select count(*) from MIP.APP.INTRA_SIGNALS
                       where SIGNAL_TS between :v_chunk_start and :v_chunk_end
                         and METRIC_VERSION = :P_METRIC_VERSION
                         and BUCKET_VERSION = :P_BUCKET_VERSION
                         and PATTERN_ID in (select PATTERN_ID from MIP.APP.TMP_BACKFILL_PATTERN_SET)
                   ),
                   ROWS_OUTCOMES = (
                       select count(*)
                       from MIP.APP.INTRA_OUTCOMES o
                       join MIP.APP.INTRA_SIGNALS s on s.SIGNAL_ID = o.SIGNAL_ID
                       where s.SIGNAL_TS between :v_chunk_start and :v_chunk_end
                         and s.METRIC_VERSION = :P_METRIC_VERSION
                         and s.BUCKET_VERSION = :P_BUCKET_VERSION
                         and s.PATTERN_ID in (select PATTERN_ID from MIP.APP.TMP_BACKFILL_PATTERN_SET)
                   ),
                   ROWS_TRUST = (
                       select count(*) from MIP.APP.INTRA_TRUST_STATS
                       where CALCULATED_AT = :v_chunk_end
                         and METRIC_VERSION = :P_METRIC_VERSION
                         and BUCKET_VERSION = :P_BUCKET_VERSION
                         and TRUST_VERSION = :v_trust_version
                         and PATTERN_ID in (select PATTERN_ID from MIP.APP.TMP_BACKFILL_PATTERN_SET)
                   ),
                   ROWS_TERRAIN = (
                       select count(*) from MIP.APP.OPPORTUNITY_TERRAIN_15M
                       where TS between :v_chunk_start and :v_chunk_end
                         and METRIC_VERSION = :P_METRIC_VERSION
                         and BUCKET_VERSION = :P_BUCKET_VERSION
                         and TERRAIN_VERSION = :P_TERRAIN_VERSION
                         and PATTERN_ID in (select PATTERN_ID from MIP.APP.TMP_BACKFILL_PATTERN_SET)
                   ),
                   DETAILS = object_construct(
                       'note',
                       case
                           when coalesce((
                               select count(*) from MIP.APP.STATE_SNAPSHOT_15M
                               where TS between :v_chunk_start and :v_chunk_end
                                 and METRIC_VERSION = :P_METRIC_VERSION
                                 and BUCKET_VERSION = :P_BUCKET_VERSION
                           ), 0) = 0
                            and coalesce((
                               select count(*) from MIP.APP.INTRA_TRUST_STATS
                               where CALCULATED_AT = :v_chunk_end
                                 and METRIC_VERSION = :P_METRIC_VERSION
                                 and BUCKET_VERSION = :P_BUCKET_VERSION
                                 and TRUST_VERSION = :v_trust_version
                                 and PATTERN_ID in (select PATTERN_ID from MIP.APP.TMP_BACKFILL_PATTERN_SET)
                           ), 0) > 0
                           then 'Expected: trust snapshot derives from rolling window and may be non-zero even when this chunk has zero bars.'
                           else null
                       end
                   ),
                   UPDATED_AT = current_timestamp()
             where CHUNK_ID = :v_chunk_id
               and START_TS = :v_chunk_start
               and END_TS = :v_chunk_end
               and PATTERN_SET_HASH = :v_pattern_set_hash
               and METRIC_VERSION = :P_METRIC_VERSION
               and BUCKET_VERSION = :P_BUCKET_VERSION
               and TRUST_VERSION = :v_trust_version
               and TERRAIN_VERSION = :P_TERRAIN_VERSION;

            v_done_count := v_done_count + 1;
        exception
            when other then
                update MIP.APP.INTRA_BACKFILL_RUN_LOG
                   set STATUS = 'FAILED',
                       COMPLETED_AT = current_timestamp(),
                       ERROR_MESSAGE = sqlerrm,
                       UPDATED_AT = current_timestamp()
                 where CHUNK_ID = :v_chunk_id
                   and START_TS = :v_chunk_start
                   and END_TS = :v_chunk_end
                   and PATTERN_SET_HASH = :v_pattern_set_hash
                   and METRIC_VERSION = :P_METRIC_VERSION
                   and BUCKET_VERSION = :P_BUCKET_VERSION
                   and TRUST_VERSION = :v_trust_version
                   and TERRAIN_VERSION = :P_TERRAIN_VERSION;
                v_fail_count := v_fail_count + 1;
        end;

        v_chunk_start := dateadd(millisecond, 1, v_chunk_end);
    end while;

    drop table if exists MIP.APP.TMP_BACKFILL_PATTERN_SET;

    return object_construct(
        'status', case when v_fail_count > 0 then 'PARTIAL' else 'SUCCESS' end,
        'run_id', :v_run_id,
        'pattern_set', :v_pattern_set,
        'pattern_set_hash', :v_pattern_set_hash,
        'trust_version', :v_trust_version,
        'done_chunks', :v_done_count,
        'skipped_chunks', :v_skip_count,
        'failed_chunks', :v_fail_count
    );
end;
$$;
