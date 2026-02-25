-- 355_sp_intra_compute_trust_snapshots.sql
-- Purpose: Phase 5 trust snapshots for intraday v2 (state-conditioned).
-- Implements rolling window, deterministic fallback, and TRUST_VERSION contract.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_INTRA_COMPUTE_TRUST_SNAPSHOTS(
    P_AS_OF_TS timestamp_ntz,
    P_WINDOW_DAYS number default 90,
    P_MIN_SAMPLE number default 20,
    P_METRIC_VERSION string default 'v1_1',
    P_BUCKET_VERSION string default 'v1',
    P_PATTERN_SET string default 'ALL',
    P_TRUST_CONFIG_VERSION string default 'BASELINE_FIXED20',
    P_TERRAIN_VERSION string default 'v1'
)
returns variant
language sql
execute as caller
as
$$
declare
    v_as_of_ts timestamp_ntz;
    v_window_days number := coalesce(:P_WINDOW_DAYS, 90);
    v_min_sample number := coalesce(:P_MIN_SAMPLE, 20);
    v_train_window_end timestamp_ntz;
    v_train_window_start timestamp_ntz;
    v_fallback_rules string := 'exact->regime_only->global';
    v_trust_version string;
    v_rows_merged number := 0;
begin
    v_as_of_ts := coalesce(:P_AS_OF_TS, current_timestamp());
    v_train_window_end := :v_as_of_ts;
    v_train_window_start := dateadd(day, -:v_window_days, :v_train_window_end);
    v_trust_version := sha2(
        concat(
            coalesce(:P_METRIC_VERSION, ''), '|',
            coalesce(:P_BUCKET_VERSION, ''), '|',
            coalesce(to_varchar(:v_window_days), ''), '|',
            coalesce(to_varchar(:v_min_sample), ''), '|',
            coalesce(:P_TRUST_CONFIG_VERSION, ''), '|',
            :v_fallback_rules
        ),
        256
    );

    merge into MIP.APP.INTRA_TRUST_STATS t
    using (
        with base as (
            select
                s.PATTERN_ID,
                s.MARKET_TYPE,
                s.INTERVAL_MINUTES,
                o.HORIZON_BARS,
                s.STATE_BUCKET_ID,
                o.RETURN_NET,
                o.HIT_FLAG
            from MIP.APP.INTRA_OUTCOMES o
            join MIP.APP.INTRA_SIGNALS s
              on s.SIGNAL_ID = o.SIGNAL_ID
             and s.SIGNAL_NK_HASH = o.SIGNAL_NK_HASH
            where s.INTERVAL_MINUTES = 15
              and s.SIGNAL_TS > :v_train_window_start
              and s.SIGNAL_TS <= :v_train_window_end
              and s.METRIC_VERSION = :P_METRIC_VERSION
              and s.BUCKET_VERSION = :P_BUCKET_VERSION
              and (
                    :P_PATTERN_SET is null
                    or upper(trim(:P_PATTERN_SET)) = 'ALL'
                    or s.PATTERN_ID in (
                        select try_to_number(trim(value))
                        from table(split_to_table(:P_PATTERN_SET, ','))
                        where try_to_number(trim(value)) is not null
                    )
              )
              and o.EVAL_STATUS = 'SUCCESS'
              and o.METRIC_VERSION = :P_METRIC_VERSION
              and o.BUCKET_VERSION = :P_BUCKET_VERSION
        ),
        target_keys as (
            select distinct
                b.PATTERN_ID,
                b.MARKET_TYPE,
                b.INTERVAL_MINUTES,
                b.HORIZON_BARS
            from base b
        ),
        target_buckets as (
            select
                tk.PATTERN_ID,
                tk.MARKET_TYPE,
                tk.INTERVAL_MINUTES,
                tk.HORIZON_BARS,
                bd.STATE_BUCKET_ID,
                bd.REGIME_CLASS
            from target_keys tk
            join MIP.APP.STATE_BUCKET_DEF bd
              on bd.BUCKET_VERSION = :P_BUCKET_VERSION
             and bd.IS_ACTIVE = true
        ),
        min_sample_cfg as (
            select
                tk.PATTERN_ID,
                tk.HORIZON_BARS,
                coalesce(ms.MIN_SAMPLE, :v_min_sample) as MIN_SAMPLE_EFFECTIVE
            from target_keys tk
            left join MIP.APP.INTRA_TRUST_MIN_SAMPLE_CONFIG ms
              on ms.PATTERN_ID = tk.PATTERN_ID
             and ms.HORIZON_BARS = tk.HORIZON_BARS
             and ms.TRUST_CONFIG_VERSION = :P_TRUST_CONFIG_VERSION
             and ms.IS_ACTIVE = true
             and ms.VALID_FROM_TS <= :v_as_of_ts
             and coalesce(ms.VALID_TO_TS, '9999-12-31'::timestamp_ntz) > :v_as_of_ts
            qualify row_number() over (
                partition by tk.PATTERN_ID, tk.HORIZON_BARS
                order by ms.VALID_FROM_TS desc
            ) = 1
        ),
        exact_stats as (
            select
                b.PATTERN_ID,
                b.MARKET_TYPE,
                b.INTERVAL_MINUTES,
                b.HORIZON_BARS,
                b.STATE_BUCKET_ID,
                count(*) as N_SIGNALS,
                sum(iff(b.HIT_FLAG, 1, 0)) as N_HITS,
                avg(b.RETURN_NET) as AVG_RETURN_NET,
                stddev_samp(b.RETURN_NET) as RETURN_STDDEV
            from base b
            group by 1,2,3,4,5
        ),
        regime_stats as (
            select
                b.PATTERN_ID,
                b.MARKET_TYPE,
                b.INTERVAL_MINUTES,
                b.HORIZON_BARS,
                bd.REGIME_CLASS,
                count(*) as N_SIGNALS,
                sum(iff(b.HIT_FLAG, 1, 0)) as N_HITS,
                avg(b.RETURN_NET) as AVG_RETURN_NET,
                stddev_samp(b.RETURN_NET) as RETURN_STDDEV
            from base b
            join MIP.APP.STATE_BUCKET_DEF bd
              on bd.BUCKET_VERSION = :P_BUCKET_VERSION
             and bd.STATE_BUCKET_ID = b.STATE_BUCKET_ID
             and bd.IS_ACTIVE = true
            group by 1,2,3,4,5
        ),
        global_stats as (
            select
                b.PATTERN_ID,
                b.MARKET_TYPE,
                b.INTERVAL_MINUTES,
                b.HORIZON_BARS,
                count(*) as N_SIGNALS,
                sum(iff(b.HIT_FLAG, 1, 0)) as N_HITS,
                avg(b.RETURN_NET) as AVG_RETURN_NET,
                stddev_samp(b.RETURN_NET) as RETURN_STDDEV
            from base b
            group by 1,2,3,4
        ),
        selected as (
            select
                tb.PATTERN_ID,
                tb.MARKET_TYPE,
                tb.INTERVAL_MINUTES,
                tb.HORIZON_BARS,
                tb.STATE_BUCKET_ID,
                th.MIN_SAMPLE_EFFECTIVE,
                case
                    when coalesce(es.N_SIGNALS, 0) >= th.MIN_SAMPLE_EFFECTIVE then 'EXACT'
                    when coalesce(rs.N_SIGNALS, 0) >= th.MIN_SAMPLE_EFFECTIVE then 'REGIME_ONLY'
                    else 'GLOBAL'
                end as FALLBACK_LEVEL,
                case
                    when coalesce(es.N_SIGNALS, 0) >= th.MIN_SAMPLE_EFFECTIVE then tb.STATE_BUCKET_ID
                    when coalesce(rs.N_SIGNALS, 0) >= th.MIN_SAMPLE_EFFECTIVE then concat('REGIME:', tb.REGIME_CLASS)
                    else 'GLOBAL'
                end as FALLBACK_SOURCE_BUCKET_ID,
                case
                    when coalesce(es.N_SIGNALS, 0) >= th.MIN_SAMPLE_EFFECTIVE then es.N_SIGNALS
                    when coalesce(rs.N_SIGNALS, 0) >= th.MIN_SAMPLE_EFFECTIVE then rs.N_SIGNALS
                    else gs.N_SIGNALS
                end as N_SIGNALS,
                case
                    when coalesce(es.N_SIGNALS, 0) >= th.MIN_SAMPLE_EFFECTIVE then es.N_HITS
                    when coalesce(rs.N_SIGNALS, 0) >= th.MIN_SAMPLE_EFFECTIVE then rs.N_HITS
                    else gs.N_HITS
                end as N_HITS,
                case
                    when coalesce(es.N_SIGNALS, 0) >= th.MIN_SAMPLE_EFFECTIVE then es.AVG_RETURN_NET
                    when coalesce(rs.N_SIGNALS, 0) >= th.MIN_SAMPLE_EFFECTIVE then rs.AVG_RETURN_NET
                    else gs.AVG_RETURN_NET
                end as AVG_RETURN_NET,
                case
                    when coalesce(es.N_SIGNALS, 0) >= th.MIN_SAMPLE_EFFECTIVE then es.RETURN_STDDEV
                    when coalesce(rs.N_SIGNALS, 0) >= th.MIN_SAMPLE_EFFECTIVE then rs.RETURN_STDDEV
                    else gs.RETURN_STDDEV
                end as RETURN_STDDEV
            from target_buckets tb
            join min_sample_cfg th
              on th.PATTERN_ID = tb.PATTERN_ID
             and th.HORIZON_BARS = tb.HORIZON_BARS
            left join exact_stats es
              on es.PATTERN_ID = tb.PATTERN_ID
             and es.MARKET_TYPE = tb.MARKET_TYPE
             and es.INTERVAL_MINUTES = tb.INTERVAL_MINUTES
             and es.HORIZON_BARS = tb.HORIZON_BARS
             and es.STATE_BUCKET_ID = tb.STATE_BUCKET_ID
            left join regime_stats rs
              on rs.PATTERN_ID = tb.PATTERN_ID
             and rs.MARKET_TYPE = tb.MARKET_TYPE
             and rs.INTERVAL_MINUTES = tb.INTERVAL_MINUTES
             and rs.HORIZON_BARS = tb.HORIZON_BARS
             and rs.REGIME_CLASS = tb.REGIME_CLASS
            left join global_stats gs
              on gs.PATTERN_ID = tb.PATTERN_ID
             and gs.MARKET_TYPE = tb.MARKET_TYPE
             and gs.INTERVAL_MINUTES = tb.INTERVAL_MINUTES
             and gs.HORIZON_BARS = tb.HORIZON_BARS
        )
        select
            s.PATTERN_ID,
            s.MARKET_TYPE,
            s.INTERVAL_MINUTES,
            s.HORIZON_BARS,
            s.STATE_BUCKET_ID,
            :v_train_window_start as TRAIN_WINDOW_START,
            :v_train_window_end as TRAIN_WINDOW_END,
            :v_as_of_ts as CALCULATED_AT,
            coalesce(s.N_SIGNALS, 0) as N_SIGNALS,
            coalesce(s.N_HITS, 0) as N_HITS,
            case when coalesce(s.N_SIGNALS, 0) > 0 then s.N_HITS / s.N_SIGNALS::float else null end as HIT_RATE,
            s.AVG_RETURN_NET,
            s.RETURN_STDDEV,
            case
                when coalesce(s.N_SIGNALS, 0) > 1 and s.RETURN_STDDEV is not null
                then s.AVG_RETURN_NET - 1.96 * s.RETURN_STDDEV / sqrt(s.N_SIGNALS)
                else null
            end as CI_LOW,
            case
                when coalesce(s.N_SIGNALS, 0) > 1 and s.RETURN_STDDEV is not null
                then s.AVG_RETURN_NET + 1.96 * s.RETURN_STDDEV / sqrt(s.N_SIGNALS)
                else null
            end as CI_HIGH,
            case
                when coalesce(s.N_SIGNALS, 0) > 1 and s.RETURN_STDDEV is not null
                then 3.92 * s.RETURN_STDDEV / sqrt(s.N_SIGNALS)
                else null
            end as CI_WIDTH,
            s.FALLBACK_LEVEL,
            s.FALLBACK_SOURCE_BUCKET_ID,
            :P_METRIC_VERSION as METRIC_VERSION,
            :P_BUCKET_VERSION as BUCKET_VERSION,
            :v_trust_version as TRUST_VERSION,
            :P_TERRAIN_VERSION as TERRAIN_VERSION,
            current_timestamp() as CREATED_AT
        from selected s
    ) s
    on t.PATTERN_ID = s.PATTERN_ID
   and t.MARKET_TYPE = s.MARKET_TYPE
   and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
   and t.HORIZON_BARS = s.HORIZON_BARS
   and t.STATE_BUCKET_ID = s.STATE_BUCKET_ID
   and t.CALCULATED_AT = s.CALCULATED_AT
   and t.TRAIN_WINDOW_START = s.TRAIN_WINDOW_START
   and t.TRAIN_WINDOW_END = s.TRAIN_WINDOW_END
    when matched then update set
        t.N_SIGNALS = s.N_SIGNALS,
        t.N_HITS = s.N_HITS,
        t.HIT_RATE = s.HIT_RATE,
        t.AVG_RETURN_NET = s.AVG_RETURN_NET,
        t.RETURN_STDDEV = s.RETURN_STDDEV,
        t.CI_LOW = s.CI_LOW,
        t.CI_HIGH = s.CI_HIGH,
        t.CI_WIDTH = s.CI_WIDTH,
        t.FALLBACK_LEVEL = s.FALLBACK_LEVEL,
        t.FALLBACK_SOURCE_BUCKET_ID = s.FALLBACK_SOURCE_BUCKET_ID,
        t.METRIC_VERSION = s.METRIC_VERSION,
        t.BUCKET_VERSION = s.BUCKET_VERSION,
        t.TRUST_VERSION = s.TRUST_VERSION,
        t.TERRAIN_VERSION = s.TERRAIN_VERSION,
        t.CREATED_AT = current_timestamp()
    when not matched then insert (
        PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS, STATE_BUCKET_ID,
        TRAIN_WINDOW_START, TRAIN_WINDOW_END, CALCULATED_AT,
        N_SIGNALS, N_HITS, HIT_RATE, AVG_RETURN_NET, RETURN_STDDEV,
        CI_LOW, CI_HIGH, CI_WIDTH,
        FALLBACK_LEVEL, FALLBACK_SOURCE_BUCKET_ID,
        METRIC_VERSION, BUCKET_VERSION, TRUST_VERSION, TERRAIN_VERSION, CREATED_AT
    ) values (
        s.PATTERN_ID, s.MARKET_TYPE, s.INTERVAL_MINUTES, s.HORIZON_BARS, s.STATE_BUCKET_ID,
        s.TRAIN_WINDOW_START, s.TRAIN_WINDOW_END, s.CALCULATED_AT,
        s.N_SIGNALS, s.N_HITS, s.HIT_RATE, s.AVG_RETURN_NET, s.RETURN_STDDEV,
        s.CI_LOW, s.CI_HIGH, s.CI_WIDTH,
        s.FALLBACK_LEVEL, s.FALLBACK_SOURCE_BUCKET_ID,
        s.METRIC_VERSION, s.BUCKET_VERSION, s.TRUST_VERSION, s.TERRAIN_VERSION, s.CREATED_AT
    );

    v_rows_merged := sqlrowcount;

    return object_construct(
        'status', 'SUCCESS',
        'as_of_ts', :v_as_of_ts,
        'train_window_start', :v_train_window_start,
        'train_window_end', :v_train_window_end,
        'window_days', :v_window_days,
        'min_sample', :v_min_sample,
        'trust_config_version', :P_TRUST_CONFIG_VERSION,
        'pattern_set', :P_PATTERN_SET,
        'fallback_rules', :v_fallback_rules,
        'trust_version', :v_trust_version,
        'rows_merged', :v_rows_merged
    );
end;
$$;
