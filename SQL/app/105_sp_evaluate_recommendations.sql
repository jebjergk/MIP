-- /sql/app/105_sp_evaluate_recommendations.sql
-- Purpose: Evaluate forward returns for recommendations over trading-bar horizons

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_EVALUATE_RECOMMENDATIONS(
    P_FROM_DATE timestamp_ntz,
    P_TO_DATE   timestamp_ntz
)
returns varchar
language sql
as
$$
declare
    v_from_ts timestamp_ntz := coalesce(:P_FROM_DATE, dateadd(day, -90, current_date()));
    v_to_ts   timestamp_ntz := coalesce(:P_TO_DATE, current_timestamp());
    v_merged  number := 0;
    v_horizon_counts variant;
    v_run_id  string := coalesce(nullif(current_query_tag(), ''), uuid_string());
begin
    call MIP.APP.SP_LOG_EVENT(
        'EVALUATION',
        'SP_EVALUATE_RECOMMENDATIONS',
        'START',
        null,
        object_construct('from_ts', v_from_ts, 'to_ts', v_to_ts),
        null,
        v_run_id,
        null
    );

    merge into MIP.APP.RECOMMENDATION_OUTCOMES t
    using (
        with horizons as (
            select column1::number as HORIZON_DAYS
            from values (1), (3), (5), (10)
        ),
        bar_index as (
            select
                SYMBOL,
                MARKET_TYPE,
                INTERVAL_MINUTES,
                TS,
                CLOSE::FLOAT as CLOSE,
                row_number() over (
                    partition by SYMBOL, MARKET_TYPE, INTERVAL_MINUTES
                    order by TS
                ) as BAR_INDEX
            from MIP.MART.MARKET_BARS
        ),
        base_recs as (
            select
                r.PATTERN_ID,
                r.SYMBOL,
                r.MARKET_TYPE,
                r.INTERVAL_MINUTES,
                r.TS as REC_TS,
                bi.CLOSE::FLOAT as REC_CLOSE,
                bi.BAR_INDEX as REC_BAR_INDEX
            from MIP.APP.RECOMMENDATION_LOG r
            join bar_index bi
              on bi.SYMBOL = r.SYMBOL
             and bi.MARKET_TYPE = r.MARKET_TYPE
             and bi.INTERVAL_MINUTES = r.INTERVAL_MINUTES
             and bi.TS = r.TS
            where r.TS >= :v_from_ts
              and r.TS <= :v_to_ts
        ),
        future_bars as (
            select
                r.PATTERN_ID,
                r.SYMBOL,
                r.MARKET_TYPE,
                r.INTERVAL_MINUTES,
                r.REC_TS,
                r.REC_CLOSE,
                h.HORIZON_DAYS,
                bf.TS as FUTURE_TS,
                bf.CLOSE::FLOAT as FUTURE_CLOSE
            from base_recs r
            join horizons h
              on 1 = 1
            join bar_index bf
              on bf.SYMBOL = r.SYMBOL
             and bf.MARKET_TYPE = r.MARKET_TYPE
             and bf.INTERVAL_MINUTES = r.INTERVAL_MINUTES
             and bf.BAR_INDEX = r.REC_BAR_INDEX + h.HORIZON_DAYS
        )
        select
            fb.PATTERN_ID,
            fb.SYMBOL,
            fb.MARKET_TYPE,
            fb.INTERVAL_MINUTES,
            fb.REC_TS,
            fb.HORIZON_DAYS,
            fb.REC_CLOSE,
            fb.FUTURE_CLOSE,
            (fb.FUTURE_CLOSE::FLOAT - fb.REC_CLOSE::FLOAT) / fb.REC_CLOSE::FLOAT as FORWARD_RETURN,
            (fb.FUTURE_CLOSE::FLOAT - fb.REC_CLOSE::FLOAT) / fb.REC_CLOSE::FLOAT > 0 as HIT,
            current_timestamp() as CALCULATED_AT
        from future_bars fb
        where fb.REC_CLOSE is not null
          and fb.REC_CLOSE <> 0
          and fb.FUTURE_CLOSE is not null
          and fb.FUTURE_CLOSE <> 0
    ) s
      on t.PATTERN_ID = s.PATTERN_ID
     and t.SYMBOL = s.SYMBOL
     and t.MARKET_TYPE = s.MARKET_TYPE
     and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
     and t.REC_TS = s.REC_TS
     and t.HORIZON_DAYS = s.HORIZON_DAYS
    when matched then update set
        t.REC_CLOSE = s.REC_CLOSE,
        t.FUTURE_CLOSE = s.FUTURE_CLOSE,
        t.FORWARD_RETURN = s.FORWARD_RETURN,
        t.HIT = s.HIT,
        t.CALCULATED_AT = s.CALCULATED_AT
    when not matched then insert (
        PATTERN_ID,
        SYMBOL,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        REC_TS,
        HORIZON_DAYS,
        REC_CLOSE,
        FUTURE_CLOSE,
        FORWARD_RETURN,
        HIT,
        CALCULATED_AT
    ) values (
        s.PATTERN_ID,
        s.SYMBOL,
        s.MARKET_TYPE,
        s.INTERVAL_MINUTES,
        s.REC_TS,
        s.HORIZON_DAYS,
        s.REC_CLOSE,
        s.FUTURE_CLOSE,
        s.FORWARD_RETURN,
        s.HIT,
        s.CALCULATED_AT
    );

    v_merged := sqlrowcount;

    select object_agg(HORIZON_DAYS, CNT)
      into :v_horizon_counts
    from (
        with horizons as (
            select column1::number as HORIZON_DAYS
            from values (1), (3), (5), (10)
        ),
        bar_index as (
            select
                SYMBOL,
                MARKET_TYPE,
                INTERVAL_MINUTES,
                TS,
                CLOSE::FLOAT as CLOSE,
                row_number() over (
                    partition by SYMBOL, MARKET_TYPE, INTERVAL_MINUTES
                    order by TS
                ) as BAR_INDEX
            from MIP.MART.MARKET_BARS
        ),
        base_recs as (
            select
                r.PATTERN_ID,
                r.SYMBOL,
                r.MARKET_TYPE,
                r.INTERVAL_MINUTES,
                r.TS as REC_TS,
                bi.CLOSE::FLOAT as REC_CLOSE,
                bi.BAR_INDEX as REC_BAR_INDEX
            from MIP.APP.RECOMMENDATION_LOG r
            join bar_index bi
              on bi.SYMBOL = r.SYMBOL
             and bi.MARKET_TYPE = r.MARKET_TYPE
             and bi.INTERVAL_MINUTES = r.INTERVAL_MINUTES
             and bi.TS = r.TS
            where r.TS >= :v_from_ts
              and r.TS <= :v_to_ts
        ),
        future_bars as (
            select
                r.PATTERN_ID,
                r.SYMBOL,
                r.MARKET_TYPE,
                r.INTERVAL_MINUTES,
                r.REC_TS,
                r.REC_CLOSE,
                h.HORIZON_DAYS,
                bf.TS as FUTURE_TS,
                bf.CLOSE::FLOAT as FUTURE_CLOSE
            from base_recs r
            join horizons h
              on 1 = 1
            join bar_index bf
              on bf.SYMBOL = r.SYMBOL
             and bf.MARKET_TYPE = r.MARKET_TYPE
             and bf.INTERVAL_MINUTES = r.INTERVAL_MINUTES
             and bf.BAR_INDEX = r.REC_BAR_INDEX + h.HORIZON_DAYS
        )
        select
            fb.HORIZON_DAYS,
            count(*) as CNT
        from future_bars fb
        where fb.REC_CLOSE is not null
          and fb.REC_CLOSE <> 0
          and fb.FUTURE_CLOSE is not null
          and fb.FUTURE_CLOSE <> 0
        group by fb.HORIZON_DAYS
    );

    call MIP.APP.SP_LOG_EVENT(
        'EVALUATION',
        'SP_EVALUATE_RECOMMENDATIONS',
        'SUCCESS',
        v_merged,
        object_construct(
            'from_ts', v_from_ts,
            'to_ts', v_to_ts,
            'horizon_counts', v_horizon_counts
        ),
        null,
        v_run_id,
        null
    );

    return 'Upserted ' || v_merged || ' recommendation outcomes from ' || v_from_ts || ' to ' || v_to_ts || '.';
exception
    when other then
        call MIP.APP.SP_LOG_EVENT(
            'EVALUATION',
            'SP_EVALUATE_RECOMMENDATIONS',
            'FAIL',
            v_merged,
            object_construct('from_ts', v_from_ts, 'to_ts', v_to_ts),
            :sqlerrm,
            v_run_id,
            null
        );
        raise;
end;
$$;
