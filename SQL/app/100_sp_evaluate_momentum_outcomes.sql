-- /sql/app/100_sp_evaluate_momentum_outcomes.sql
-- Purpose: Evaluate realized returns for momentum recommendations over a given time horizon

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_EVALUATE_MOMENTUM_OUTCOMES(
    P_HORIZON_MINUTES   number,   -- e.g. 15
    P_HIT_THRESHOLD     number,   -- e.g. 0.002  (>= +0.2% = HIT)
    P_MISS_THRESHOLD    number,   -- e.g. -0.002 (<= -0.2% = MISS)
    P_MARKET_TYPE       string default 'STOCK',
    P_INTERVAL_MINUTES  number default 5
)
returns varchar
language sql
as
$$
declare
    v_inserted number := 0;
    v_horizon_minutes number := P_HORIZON_MINUTES;
    v_hit_threshold number := P_HIT_THRESHOLD;
    v_miss_threshold number := P_MISS_THRESHOLD;
    v_market_type string;
    v_interval_minutes number;
begin
    v_market_type := coalesce(P_MARKET_TYPE, 'STOCK');
    v_interval_minutes := coalesce(P_INTERVAL_MINUTES, 5);

    -- Insert outcomes only for recommendations that don't have an outcome yet for this horizon
    insert into MIP.APP.OUTCOME_EVALUATION (
        RECOMMENDATION_ID,
        HORIZON_MINUTES,
        RETURN_REALIZED,
        OUTCOME_LABEL,
        DETAILS
    )
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
        where r.MARKET_TYPE      = :v_market_type
          and r.INTERVAL_MINUTES = :v_interval_minutes
    ),
    recs_without_outcome as (
        select b.*
        from base_recs b
        left join MIP.APP.OUTCOME_EVALUATION o
          on o.RECOMMENDATION_ID = b.RECOMMENDATION_ID
         and o.HORIZON_MINUTES   = :v_horizon_minutes
        where o.OUTCOME_ID is null
    ),
    future_bars as (
        select
            r.RECOMMENDATION_ID,
            r.SYMBOL,
            r.MARKET_TYPE,
            r.INTERVAL_MINUTES,
            r.REC_TS,
            r.REC_CLOSE,
            mb.TS as FUTURE_TS,
            mb.CLOSE::FLOAT as FUTURE_CLOSE,
            row_number() over (
                partition by r.RECOMMENDATION_ID
                order by mb.TS
            ) as RN
        from recs_without_outcome r
        join MIP.MART.MARKET_BARS mb
          on mb.SYMBOL          = r.SYMBOL
         and mb.MARKET_TYPE     = r.MARKET_TYPE
         and mb.INTERVAL_MINUTES= r.INTERVAL_MINUTES
         and mb.TS >= dateadd(minute, :v_horizon_minutes, r.REC_TS)
    ),
    chosen_future as (
        select *
        from future_bars
        where RN = 1
    )
    select
        cf.RECOMMENDATION_ID,
        :v_horizon_minutes as HORIZON_MINUTES,
        case
            when cf.REC_CLOSE is not null
             and cf.REC_CLOSE <> 0
            then (cf.FUTURE_CLOSE::FLOAT - cf.REC_CLOSE::FLOAT) / cf.REC_CLOSE::FLOAT
            else null
        end as RETURN_REALIZED,
        case
            when cf.REC_CLOSE is not null
             and cf.REC_CLOSE <> 0 then
                case
                    when (cf.FUTURE_CLOSE::FLOAT - cf.REC_CLOSE::FLOAT) / cf.REC_CLOSE::FLOAT >= :v_hit_threshold
                        then 'HIT'
                    when (cf.FUTURE_CLOSE::FLOAT - cf.REC_CLOSE::FLOAT) / cf.REC_CLOSE::FLOAT <= :v_miss_threshold
                        then 'MISS'
                    else 'NEUTRAL'
                end
            else 'UNKNOWN'
        end as OUTCOME_LABEL,
        object_construct(
            'rec_ts',        cf.REC_TS,
            'future_ts',     cf.FUTURE_TS,
            'rec_close',     cf.REC_CLOSE,
            'future_close',  cf.FUTURE_CLOSE
        ) as DETAILS
    from chosen_future cf
    where cf.REC_CLOSE is not null
      and cf.REC_CLOSE <> 0
      and cf.FUTURE_CLOSE is not null
      and cf.FUTURE_CLOSE <> 0;

    v_inserted := sqlrowcount;

    return 'Inserted ' || v_inserted || ' outcome rows for horizon ' || P_HORIZON_MINUTES || ' minutes.';
end;
$$;
