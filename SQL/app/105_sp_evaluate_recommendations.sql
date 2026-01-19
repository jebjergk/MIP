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
        object_construct('from_ts', :v_from_ts, 'to_ts', :v_to_ts),
        null,
        :v_run_id,
        null
    );

    merge into MIP.APP.RECOMMENDATION_OUTCOMES t
    using (
        with horizons as (
            select column1::number as HORIZON_BARS
            from values (1), (3), (5), (10), (20)
        ),
        entry_bars as (
            select
                r.RECOMMENDATION_ID,
                r.SYMBOL,
                r.MARKET_TYPE,
                r.INTERVAL_MINUTES,
                r.TS as ENTRY_TS,
                b.CLOSE::FLOAT as ENTRY_PRICE
            from MIP.APP.RECOMMENDATION_LOG r
            left join MIP.MART.MARKET_BARS b
              on b.SYMBOL = r.SYMBOL
             and b.MARKET_TYPE = r.MARKET_TYPE
             and b.INTERVAL_MINUTES = r.INTERVAL_MINUTES
             and b.TS <= r.TS
            where r.TS >= :v_from_ts
              and r.TS <= :v_to_ts
            qualify row_number() over (
                partition by r.RECOMMENDATION_ID
                order by b.TS desc
            ) = 1
        ),
        future_ranked as (
            select
                e.RECOMMENDATION_ID,
                b.TS as EXIT_TS,
                b.CLOSE::FLOAT as EXIT_PRICE,
                row_number() over (
                    partition by e.RECOMMENDATION_ID
                    order by b.TS
                ) as FUTURE_RN
            from entry_bars e
            join MIP.MART.MARKET_BARS b
              on b.SYMBOL = e.SYMBOL
             and b.MARKET_TYPE = e.MARKET_TYPE
             and b.INTERVAL_MINUTES = e.INTERVAL_MINUTES
             and b.TS > e.ENTRY_TS
        ),
        future_bars as (
            select
                e.RECOMMENDATION_ID,
                e.ENTRY_TS,
                e.ENTRY_PRICE,
                h.HORIZON_BARS,
                fr.EXIT_TS,
                fr.EXIT_PRICE
            from entry_bars e
            join horizons h
              on 1 = 1
            left join future_ranked fr
              on fr.RECOMMENDATION_ID = e.RECOMMENDATION_ID
             and fr.FUTURE_RN = h.HORIZON_BARS
        )
        select
            fb.RECOMMENDATION_ID,
            fb.HORIZON_BARS,
            fb.ENTRY_TS,
            fb.EXIT_TS,
            fb.ENTRY_PRICE,
            fb.EXIT_PRICE,
            case
                when fb.ENTRY_PRICE is not null
                 and fb.ENTRY_PRICE <> 0
                 and fb.EXIT_PRICE is not null
                 and fb.EXIT_PRICE <> 0
                then (fb.EXIT_PRICE::FLOAT - fb.ENTRY_PRICE::FLOAT) / fb.ENTRY_PRICE::FLOAT
                else null
            end as REALIZED_RETURN,
            'LONG' as DIRECTION,
            case
                when fb.ENTRY_PRICE is not null
                 and fb.ENTRY_PRICE <> 0
                 and fb.EXIT_PRICE is not null
                 and fb.EXIT_PRICE <> 0
                then (fb.EXIT_PRICE::FLOAT - fb.ENTRY_PRICE::FLOAT) / fb.ENTRY_PRICE::FLOAT >= 0
                else null
            end as HIT_FLAG,
            'THRESHOLD' as HIT_RULE,
            0 as MIN_RETURN_THRESHOLD,
            case
                when fb.ENTRY_PRICE is null or fb.ENTRY_PRICE = 0 then 'INSUFFICIENT_DATA'
                when fb.EXIT_PRICE is null or fb.EXIT_PRICE = 0 then 'PENDING'
                else 'SUCCESS'
            end as EVAL_STATUS,
            current_timestamp() as CALCULATED_AT
        from future_bars fb
    ) s
      on t.RECOMMENDATION_ID = s.RECOMMENDATION_ID
     and t.HORIZON_BARS = s.HORIZON_BARS
    when matched then update set
        t.ENTRY_TS = s.ENTRY_TS,
        t.EXIT_TS = s.EXIT_TS,
        t.ENTRY_PRICE = s.ENTRY_PRICE,
        t.EXIT_PRICE = s.EXIT_PRICE,
        t.REALIZED_RETURN = s.REALIZED_RETURN,
        t.DIRECTION = s.DIRECTION,
        t.HIT_FLAG = s.HIT_FLAG,
        t.HIT_RULE = s.HIT_RULE,
        t.MIN_RETURN_THRESHOLD = s.MIN_RETURN_THRESHOLD,
        t.EVAL_STATUS = s.EVAL_STATUS,
        t.CALCULATED_AT = s.CALCULATED_AT
    when not matched then insert (
        RECOMMENDATION_ID,
        HORIZON_BARS,
        ENTRY_TS,
        EXIT_TS,
        ENTRY_PRICE,
        EXIT_PRICE,
        REALIZED_RETURN,
        DIRECTION,
        HIT_FLAG,
        HIT_RULE,
        MIN_RETURN_THRESHOLD,
        EVAL_STATUS,
        CALCULATED_AT
    ) values (
        s.RECOMMENDATION_ID,
        s.HORIZON_BARS,
        s.ENTRY_TS,
        s.EXIT_TS,
        s.ENTRY_PRICE,
        s.EXIT_PRICE,
        s.REALIZED_RETURN,
        s.DIRECTION,
        s.HIT_FLAG,
        s.HIT_RULE,
        s.MIN_RETURN_THRESHOLD,
        s.EVAL_STATUS,
        s.CALCULATED_AT
    );

    v_merged := sqlrowcount;

    select object_agg(HORIZON_BARS, CNT)
      into :v_horizon_counts
    from (
        with horizons as (
            select column1::number as HORIZON_BARS
            from values (1), (3), (5), (10), (20)
        ),
        entry_bars as (
            select
                r.RECOMMENDATION_ID,
                r.SYMBOL,
                r.MARKET_TYPE,
                r.INTERVAL_MINUTES,
                r.TS as ENTRY_TS,
                b.CLOSE::FLOAT as ENTRY_PRICE
            from MIP.APP.RECOMMENDATION_LOG r
            left join MIP.MART.MARKET_BARS b
              on b.SYMBOL = r.SYMBOL
             and b.MARKET_TYPE = r.MARKET_TYPE
             and b.INTERVAL_MINUTES = r.INTERVAL_MINUTES
             and b.TS <= r.TS
            where r.TS >= :v_from_ts
              and r.TS <= :v_to_ts
            qualify row_number() over (
                partition by r.RECOMMENDATION_ID
                order by b.TS desc
            ) = 1
        ),
        future_ranked as (
            select
                e.RECOMMENDATION_ID,
                b.TS as EXIT_TS,
                b.CLOSE::FLOAT as EXIT_PRICE,
                row_number() over (
                    partition by e.RECOMMENDATION_ID
                    order by b.TS
                ) as FUTURE_RN
            from entry_bars e
            join MIP.MART.MARKET_BARS b
              on b.SYMBOL = e.SYMBOL
             and b.MARKET_TYPE = e.MARKET_TYPE
             and b.INTERVAL_MINUTES = e.INTERVAL_MINUTES
             and b.TS > e.ENTRY_TS
        ),
        future_bars as (
            select
                e.RECOMMENDATION_ID,
                e.ENTRY_TS,
                e.ENTRY_PRICE,
                h.HORIZON_BARS,
                fr.EXIT_TS,
                fr.EXIT_PRICE
            from entry_bars e
            join horizons h
              on 1 = 1
            left join future_ranked fr
              on fr.RECOMMENDATION_ID = e.RECOMMENDATION_ID
             and fr.FUTURE_RN = h.HORIZON_BARS
        )
        select
            fb.HORIZON_BARS,
            count(*) as CNT
        from future_bars fb
        where fb.ENTRY_PRICE is not null
          and fb.ENTRY_PRICE <> 0
          and fb.EXIT_PRICE is not null
          and fb.EXIT_PRICE <> 0
        group by fb.HORIZON_BARS
    );

    call MIP.APP.SP_LOG_EVENT(
        'EVALUATION',
        'SP_EVALUATE_RECOMMENDATIONS',
        'SUCCESS',
        :v_merged,
        object_construct(
            'from_ts', :v_from_ts,
            'to_ts', :v_to_ts,
            'horizon_counts', :v_horizon_counts
        ),
        null,
        :v_run_id,
        null
    );

    return 'Upserted ' || :v_merged || ' recommendation outcomes from ' || :v_from_ts || ' to ' || :v_to_ts || '.';
exception
    when other then
        call MIP.APP.SP_LOG_EVENT(
            'EVALUATION',
            'SP_EVALUATE_RECOMMENDATIONS',
            'FAIL',
            :v_merged,
            object_construct('from_ts', :v_from_ts, 'to_ts', :v_to_ts),
            :sqlerrm,
            :v_run_id,
            null
        );
        raise;
end;
$$;
