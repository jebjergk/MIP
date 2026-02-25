-- 356_sp_intra_compute_opportunity_terrain.sql
-- Purpose: Phase 6 opportunity terrain computation for intraday v2.
-- Guardrail: compute only at signal timestamps or state-transition timestamps.

use role MIP_ADMIN_ROLE;
use database MIP;

alter table if exists MIP.APP.OPPORTUNITY_TERRAIN_15M
    add column if not exists N_SIGNALS number;
alter table if exists MIP.APP.OPPORTUNITY_TERRAIN_15M
    add column if not exists GLOBAL_BASELINE_RETURN float;
alter table if exists MIP.APP.OPPORTUNITY_TERRAIN_15M
    add column if not exists UPLIFT_RAW float;
alter table if exists MIP.APP.OPPORTUNITY_TERRAIN_15M
    add column if not exists SHRINKAGE_FACTOR float;
alter table if exists MIP.APP.OPPORTUNITY_TERRAIN_15M
    add column if not exists CANDIDATE_SOURCE string;

create or replace procedure MIP.APP.SP_INTRA_COMPUTE_OPPORTUNITY_TERRAIN(
    P_START_TS timestamp_ntz,
    P_END_TS timestamp_ntz,
    P_AS_OF_TS timestamp_ntz,
    P_METRIC_VERSION string default 'v1_1',
    P_BUCKET_VERSION string default 'v1',
    P_PATTERN_SET string default 'ALL',
    P_TRUST_VERSION string default null,
    P_TERRAIN_VERSION string default 'v1',
    P_SHRINK_K float default 20.0,
    P_W_EDGE float default 0.50,
    P_W_UNCERTAINTY float default 0.30,
    P_W_SUITABILITY float default 0.20
)
returns variant
language sql
execute as caller
as
$$
declare
    v_start_ts timestamp_ntz;
    v_end_ts timestamp_ntz;
    v_as_of_ts timestamp_ntz;
    v_trust_version string := :P_TRUST_VERSION;
    v_rows_merged number := 0;
begin
    v_end_ts := coalesce(:P_END_TS, current_timestamp());
    v_start_ts := coalesce(:P_START_TS, dateadd(day, -30, :v_end_ts));
    v_as_of_ts := coalesce(:P_AS_OF_TS, :v_end_ts);

    if (v_trust_version is null) then
        select TRUST_VERSION
          into :v_trust_version
          from MIP.APP.INTRA_TRUST_STATS
         where METRIC_VERSION = :P_METRIC_VERSION
           and BUCKET_VERSION = :P_BUCKET_VERSION
           and CALCULATED_AT <= :v_as_of_ts
         order by CALCULATED_AT desc
         limit 1;
    end if;

    merge into MIP.APP.OPPORTUNITY_TERRAIN_15M t
    using (
        with trust_ranked as (
            select
                ts.*,
                row_number() over (
                    partition by PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS, STATE_BUCKET_ID
                    order by CALCULATED_AT desc, TRAIN_WINDOW_END desc
                ) as RN
            from MIP.APP.INTRA_TRUST_STATS ts
            where ts.METRIC_VERSION = :P_METRIC_VERSION
              and ts.BUCKET_VERSION = :P_BUCKET_VERSION
              and ts.TRUST_VERSION = :v_trust_version
              and ts.CALCULATED_AT <= :v_as_of_ts
        ),
        trust_selected as (
            select
                PATTERN_ID,
                MARKET_TYPE,
                INTERVAL_MINUTES,
                HORIZON_BARS,
                STATE_BUCKET_ID,
                N_SIGNALS,
                AVG_RETURN_NET,
                RETURN_STDDEV,
                CI_WIDTH
            from trust_ranked
            where RN = 1
        ),
        trust_global as (
            select
                PATTERN_ID,
                MARKET_TYPE,
                INTERVAL_MINUTES,
                HORIZON_BARS,
                sum(coalesce(AVG_RETURN_NET, 0) * greatest(coalesce(N_SIGNALS, 0), 0))
                    / nullif(sum(greatest(coalesce(N_SIGNALS, 0), 0)), 0) as GLOBAL_BASELINE_RETURN
            from trust_selected
            group by 1,2,3,4
        ),
        signal_candidates as (
            select
                s.PATTERN_ID,
                s.MARKET_TYPE,
                s.SYMBOL,
                s.INTERVAL_MINUTES,
                s.SIGNAL_TS as TS,
                s.STATE_BUCKET_ID,
                'SIGNAL' as CANDIDATE_SOURCE
            from MIP.APP.INTRA_SIGNALS s
            where s.INTERVAL_MINUTES = 15
              and s.SIGNAL_TS between :v_start_ts and :v_end_ts
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
        ),
        transition_candidates as (
            select
                p.PATTERN_ID,
                st.MARKET_TYPE,
                st.SYMBOL,
                st.INTERVAL_MINUTES,
                st.TS_TO as TS,
                ss.STATE_BUCKET_ID,
                'TRANSITION' as CANDIDATE_SOURCE
            from MIP.APP.STATE_TRANSITIONS st
            join MIP.APP.STATE_SNAPSHOT_15M ss
              on ss.MARKET_TYPE = st.MARKET_TYPE
             and ss.SYMBOL = st.SYMBOL
             and ss.INTERVAL_MINUTES = st.INTERVAL_MINUTES
             and ss.TS = st.TS_TO
             and ss.METRIC_VERSION = :P_METRIC_VERSION
             and ss.BUCKET_VERSION = :P_BUCKET_VERSION
            join MIP.APP.INTRA_PATTERN_DEFS p
              on p.IS_ENABLED = true
             and coalesce(p.PARAMS_JSON:market_type::string, st.MARKET_TYPE) = st.MARKET_TYPE
             and (
                    :P_PATTERN_SET is null
                    or upper(trim(:P_PATTERN_SET)) = 'ALL'
                    or p.PATTERN_ID in (
                        select try_to_number(trim(value))
                        from table(split_to_table(:P_PATTERN_SET, ','))
                        where try_to_number(trim(value)) is not null
                    )
             )
            where st.INTERVAL_MINUTES = 15
              and st.TS_TO between :v_start_ts and :v_end_ts
              and st.METRIC_VERSION = :P_METRIC_VERSION
              and st.BUCKET_VERSION = :P_BUCKET_VERSION
        ),
        candidates_raw as (
            select * from signal_candidates
            union all
            select * from transition_candidates
        ),
        candidates as (
            select
                PATTERN_ID,
                MARKET_TYPE,
                SYMBOL,
                INTERVAL_MINUTES,
                TS,
                STATE_BUCKET_ID,
                CANDIDATE_SOURCE
            from candidates_raw
            qualify row_number() over (
                partition by PATTERN_ID, MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS, STATE_BUCKET_ID
                order by case when CANDIDATE_SOURCE = 'SIGNAL' then 1 else 2 end
            ) = 1
        ),
        active_horizons as (
            select HORIZON_BARS
            from MIP.APP.INTRA_HORIZON_DEF
            where IS_ACTIVE = true
        ),
        scored as (
            select
                c.PATTERN_ID,
                c.MARKET_TYPE,
                c.SYMBOL,
                c.INTERVAL_MINUTES,
                c.TS,
                h.HORIZON_BARS,
                c.STATE_BUCKET_ID,
                coalesce(ts.AVG_RETURN_NET, 0) as EDGE,
                coalesce(ts.CI_WIDTH, abs(ts.RETURN_STDDEV), 0) as UNCERTAINTY,
                ts.N_SIGNALS,
                tg.GLOBAL_BASELINE_RETURN,
                coalesce(ts.AVG_RETURN_NET, 0) - coalesce(tg.GLOBAL_BASELINE_RETURN, 0) as UPLIFT_RAW,
                coalesce(ts.N_SIGNALS, 0) / nullif(coalesce(ts.N_SIGNALS, 0) + :P_SHRINK_K, 0) as SHRINKAGE_FACTOR,
                (
                    coalesce(ts.N_SIGNALS, 0) / nullif(coalesce(ts.N_SIGNALS, 0) + :P_SHRINK_K, 0)
                ) * (
                    coalesce(ts.AVG_RETURN_NET, 0) - coalesce(tg.GLOBAL_BASELINE_RETURN, 0)
                ) as SUITABILITY,
                c.CANDIDATE_SOURCE
            from candidates c
            cross join active_horizons h
            join trust_selected ts
              on ts.PATTERN_ID = c.PATTERN_ID
             and ts.MARKET_TYPE = c.MARKET_TYPE
             and ts.INTERVAL_MINUTES = c.INTERVAL_MINUTES
             and ts.HORIZON_BARS = h.HORIZON_BARS
             and ts.STATE_BUCKET_ID = c.STATE_BUCKET_ID
            left join trust_global tg
              on tg.PATTERN_ID = c.PATTERN_ID
             and tg.MARKET_TYPE = c.MARKET_TYPE
             and tg.INTERVAL_MINUTES = c.INTERVAL_MINUTES
             and tg.HORIZON_BARS = h.HORIZON_BARS
        ),
        z as (
            select
                s.*,
                coalesce(
                    (s.EDGE - avg(s.EDGE) over (
                        partition by s.PATTERN_ID, s.MARKET_TYPE, s.INTERVAL_MINUTES, s.HORIZON_BARS
                    )) / nullif(stddev_samp(s.EDGE) over (
                        partition by s.PATTERN_ID, s.MARKET_TYPE, s.INTERVAL_MINUTES, s.HORIZON_BARS
                    ), 0),
                    0
                ) as EDGE_Z,
                coalesce(
                    (s.UNCERTAINTY - avg(s.UNCERTAINTY) over (
                        partition by s.PATTERN_ID, s.MARKET_TYPE, s.INTERVAL_MINUTES, s.HORIZON_BARS
                    )) / nullif(stddev_samp(s.UNCERTAINTY) over (
                        partition by s.PATTERN_ID, s.MARKET_TYPE, s.INTERVAL_MINUTES, s.HORIZON_BARS
                    ), 0),
                    0
                ) as UNCERTAINTY_Z,
                coalesce(
                    (s.SUITABILITY - avg(s.SUITABILITY) over (
                        partition by s.PATTERN_ID, s.MARKET_TYPE, s.INTERVAL_MINUTES, s.HORIZON_BARS
                    )) / nullif(stddev_samp(s.SUITABILITY) over (
                        partition by s.PATTERN_ID, s.MARKET_TYPE, s.INTERVAL_MINUTES, s.HORIZON_BARS
                    ), 0),
                    0
                ) as SUITABILITY_Z
            from scored s
        )
        select
            PATTERN_ID,
            MARKET_TYPE,
            SYMBOL,
            INTERVAL_MINUTES,
            TS,
            HORIZON_BARS,
            STATE_BUCKET_ID,
            round(EDGE, 12) as EDGE,
            round(UNCERTAINTY, 12) as UNCERTAINTY,
            round(SUITABILITY, 12) as SUITABILITY,
            case when abs(EDGE_Z) < 1e-6 then 0 else round(EDGE_Z, 12) end as EDGE_Z,
            case when abs(UNCERTAINTY_Z) < 1e-6 then 0 else round(UNCERTAINTY_Z, 12) end as UNCERTAINTY_Z,
            case when abs(SUITABILITY_Z) < 1e-6 then 0 else round(SUITABILITY_Z, 12) end as SUITABILITY_Z,
            case
                when abs((:P_W_EDGE * EDGE_Z) - (:P_W_UNCERTAINTY * UNCERTAINTY_Z) + (:P_W_SUITABILITY * SUITABILITY_Z)) < 1e-6
                then 0
                else round((:P_W_EDGE * EDGE_Z) - (:P_W_UNCERTAINTY * UNCERTAINTY_Z) + (:P_W_SUITABILITY * SUITABILITY_Z), 12)
            end as TERRAIN_SCORE,
            :P_SHRINK_K as SHRINKAGE_K,
            object_construct(
                'w_edge', :P_W_EDGE,
                'w_uncertainty', :P_W_UNCERTAINTY,
                'w_suitability', :P_W_SUITABILITY
            ) as WEIGHTS_JSON,
            :P_METRIC_VERSION as METRIC_VERSION,
            :P_BUCKET_VERSION as BUCKET_VERSION,
            :P_TERRAIN_VERSION as TERRAIN_VERSION,
            current_timestamp() as CALCULATED_AT,
            N_SIGNALS,
            GLOBAL_BASELINE_RETURN,
            UPLIFT_RAW,
            SHRINKAGE_FACTOR,
            CANDIDATE_SOURCE
        from z
    ) s
    on t.PATTERN_ID = s.PATTERN_ID
   and t.MARKET_TYPE = s.MARKET_TYPE
   and t.SYMBOL = s.SYMBOL
   and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
   and t.TS = s.TS
   and t.HORIZON_BARS = s.HORIZON_BARS
   and t.STATE_BUCKET_ID = s.STATE_BUCKET_ID
    when matched then update set
        t.EDGE = s.EDGE,
        t.UNCERTAINTY = s.UNCERTAINTY,
        t.SUITABILITY = s.SUITABILITY,
        t.EDGE_Z = s.EDGE_Z,
        t.UNCERTAINTY_Z = s.UNCERTAINTY_Z,
        t.SUITABILITY_Z = s.SUITABILITY_Z,
        t.TERRAIN_SCORE = s.TERRAIN_SCORE,
        t.SHRINKAGE_K = s.SHRINKAGE_K,
        t.WEIGHTS_JSON = s.WEIGHTS_JSON,
        t.METRIC_VERSION = s.METRIC_VERSION,
        t.BUCKET_VERSION = s.BUCKET_VERSION,
        t.TERRAIN_VERSION = s.TERRAIN_VERSION,
        t.CALCULATED_AT = s.CALCULATED_AT,
        t.N_SIGNALS = s.N_SIGNALS,
        t.GLOBAL_BASELINE_RETURN = s.GLOBAL_BASELINE_RETURN,
        t.UPLIFT_RAW = s.UPLIFT_RAW,
        t.SHRINKAGE_FACTOR = s.SHRINKAGE_FACTOR,
        t.CANDIDATE_SOURCE = s.CANDIDATE_SOURCE
    when not matched then insert (
        PATTERN_ID, MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS, HORIZON_BARS, STATE_BUCKET_ID,
        EDGE, UNCERTAINTY, SUITABILITY, EDGE_Z, UNCERTAINTY_Z, SUITABILITY_Z, TERRAIN_SCORE,
        SHRINKAGE_K, WEIGHTS_JSON, METRIC_VERSION, BUCKET_VERSION, TERRAIN_VERSION, CALCULATED_AT,
        N_SIGNALS, GLOBAL_BASELINE_RETURN, UPLIFT_RAW, SHRINKAGE_FACTOR, CANDIDATE_SOURCE
    ) values (
        s.PATTERN_ID, s.MARKET_TYPE, s.SYMBOL, s.INTERVAL_MINUTES, s.TS, s.HORIZON_BARS, s.STATE_BUCKET_ID,
        s.EDGE, s.UNCERTAINTY, s.SUITABILITY, s.EDGE_Z, s.UNCERTAINTY_Z, s.SUITABILITY_Z, s.TERRAIN_SCORE,
        s.SHRINKAGE_K, s.WEIGHTS_JSON, s.METRIC_VERSION, s.BUCKET_VERSION, s.TERRAIN_VERSION, s.CALCULATED_AT,
        s.N_SIGNALS, s.GLOBAL_BASELINE_RETURN, s.UPLIFT_RAW, s.SHRINKAGE_FACTOR, s.CANDIDATE_SOURCE
    );

    v_rows_merged := sqlrowcount;

    return object_construct(
        'status', 'SUCCESS',
        'start_ts', :v_start_ts,
        'end_ts', :v_end_ts,
        'as_of_ts', :v_as_of_ts,
        'pattern_set', :P_PATTERN_SET,
        'trust_version', :v_trust_version,
        'terrain_version', :P_TERRAIN_VERSION,
        'rows_merged', :v_rows_merged
    );
end;
$$;
