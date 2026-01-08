-- /sql/app/105_sp_evaluate_recommendations.sql
-- Purpose: Evaluate forward returns for recommendations over a user-selected horizon (days)

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_EVALUATE_RECOMMENDATIONS(
    P_HORIZON_DAYS number
)
returns varchar
language sql
as
$$
declare
    v_horizon_days number := :P_HORIZON_DAYS;
    v_merged number := 0;
begin
    merge into MIP.APP.RECOMMENDATION_OUTCOMES t
    using (
        with base_recs as (
            select
                r.RECOMMENDATION_ID,
                r.SYMBOL,
                r.MARKET_TYPE,
                r.INTERVAL_MINUTES,
                r.TS as REC_TS,
                mb0.CLOSE::FLOAT as REC_CLOSE
            from MIP.APP.RECOMMENDATION_LOG r
            join MIP.MART.MARKET_BARS mb0
              on mb0.SYMBOL           = r.SYMBOL
             and mb0.MARKET_TYPE      = r.MARKET_TYPE
             and mb0.INTERVAL_MINUTES = r.INTERVAL_MINUTES
             and mb0.TS               = r.TS
        ),
        future_bars as (
            select
                r.RECOMMENDATION_ID,
                r.REC_TS,
                r.REC_CLOSE,
                mb.TS as FUTURE_TS,
                mb.CLOSE::FLOAT as FUTURE_CLOSE,
                row_number() over (
                    partition by r.RECOMMENDATION_ID
                    order by mb.TS
                ) as RN
            from base_recs r
            join MIP.MART.MARKET_BARS mb
              on mb.SYMBOL           = r.SYMBOL
             and mb.MARKET_TYPE      = r.MARKET_TYPE
             and mb.INTERVAL_MINUTES = r.INTERVAL_MINUTES
             and mb.TS >= dateadd(day, :v_horizon_days, r.REC_TS)
        ),
        chosen_future as (
            select *
            from future_bars
            where RN = 1
        )
        select
            cf.RECOMMENDATION_ID,
            :v_horizon_days as HORIZON_DAYS,
            (cf.FUTURE_CLOSE::FLOAT - cf.REC_CLOSE::FLOAT) / cf.REC_CLOSE::FLOAT as RETURN_FORWARD,
            object_construct(
                'rec_ts',       cf.REC_TS,
                'future_ts',    cf.FUTURE_TS,
                'rec_close',    cf.REC_CLOSE,
                'future_close', cf.FUTURE_CLOSE
            ) as DETAILS
        from chosen_future cf
        where cf.REC_CLOSE is not null
          and cf.REC_CLOSE <> 0
          and cf.FUTURE_CLOSE is not null
          and cf.FUTURE_CLOSE <> 0
    ) s
      on t.RECOMMENDATION_ID = s.RECOMMENDATION_ID
     and t.HORIZON_DAYS = s.HORIZON_DAYS
    when matched then update set
        t.RETURN_FORWARD = s.RETURN_FORWARD,
        t.DETAILS = s.DETAILS,
        t.EVALUATED_AT = current_timestamp()
    when not matched then insert (
        RECOMMENDATION_ID,
        HORIZON_DAYS,
        RETURN_FORWARD,
        DETAILS
    ) values (
        s.RECOMMENDATION_ID,
        s.HORIZON_DAYS,
        s.RETURN_FORWARD,
        s.DETAILS
    );

    v_merged := sqlrowcount;

    return 'Upserted ' || v_merged || ' recommendation outcomes for horizon ' || P_HORIZON_DAYS || ' days.';
end;
$$;
