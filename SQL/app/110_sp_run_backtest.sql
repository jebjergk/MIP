-- 110_sp_run_backtest.sql
-- Purpose: Stored procedure to run backtests over recommendations and outcomes

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_RUN_BACKTEST(
    P_HORIZON_MINUTES   number,
    P_HIT_THRESHOLD     number,
    P_MISS_THRESHOLD    number,
    P_FROM_TS           timestamp_ntz,
    P_TO_TS             timestamp_ntz,
    P_MARKET_TYPE       string,
    P_INTERVAL_MINUTES  number
)
returns varchar
language sql
as
$$
declare
    v_market_type string;
    v_interval_minutes number;
    v_run_id number;
    v_rows number;
begin
    v_market_type := coalesce(P_MARKET_TYPE, 'STOCK');
    v_interval_minutes := coalesce(P_INTERVAL_MINUTES, 5);

    insert into MIP.APP.BACKTEST_RUN (
        MARKET_TYPE,
        INTERVAL_MINUTES,
        HORIZON_MINUTES,
        HIT_THRESHOLD,
        MISS_THRESHOLD,
        FROM_TS,
        TO_TS,
        NOTES
    )
    values (
        :v_market_type,
        :v_interval_minutes,
        :P_HORIZON_MINUTES,
        :P_HIT_THRESHOLD,
        :P_MISS_THRESHOLD,
        :P_FROM_TS,
        :P_TO_TS,
        null
    );

    select max(BACKTEST_RUN_ID) into v_run_id from MIP.APP.BACKTEST_RUN;

    insert into MIP.APP.BACKTEST_RESULT (
        BACKTEST_RUN_ID,
        PATTERN_ID,
        SYMBOL,
        TRADE_COUNT,
        HIT_COUNT,
        MISS_COUNT,
        NEUTRAL_COUNT,
        HIT_RATE,
        AVG_RETURN,
        STD_RETURN,
        CUM_RETURN,
        DETAILS
    )
    select
        :v_run_id,
        PATTERN_ID,
        SYMBOL,
        TRADE_COUNT,
        HIT_COUNT,
        MISS_COUNT,
        NEUTRAL_COUNT,
        case when TRADE_COUNT = 0 then null else HIT_COUNT / TRADE_COUNT end as HIT_RATE,
        AVG_RETURN,
        STD_RETURN,
        CUM_RETURN,
        DETAILS
    from (
        select
            r.PATTERN_ID,
            r.SYMBOL,
            count(*) as TRADE_COUNT,
            count_if(o.OUTCOME_LABEL = 'HIT') as HIT_COUNT,
            count_if(o.OUTCOME_LABEL = 'MISS') as MISS_COUNT,
            count_if(o.OUTCOME_LABEL = 'NEUTRAL') as NEUTRAL_COUNT,
            avg(o.RETURN_REALIZED_DEC) as AVG_RETURN,
            stddev(o.RETURN_REALIZED_DEC) as STD_RETURN,
            sum(o.RETURN_REALIZED_DEC) as CUM_RETURN,
            object_construct(
                'pattern_name', max(p.NAME),
                'example_symbol', max(r.SYMBOL),
                'horizon_minutes', :P_HORIZON_MINUTES,
                'hit_threshold', :P_HIT_THRESHOLD,
                'miss_threshold', :P_MISS_THRESHOLD,
                'market_type', :v_market_type,
                'interval_minutes', :v_interval_minutes
            ) as DETAILS
        from MIP.APP.RECOMMENDATION_LOG r
        join MIP.APP.OUTCOME_EVALUATION o
            on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
           and o.HORIZON_MINUTES = :P_HORIZON_MINUTES
        join MIP.APP.PATTERN_DEFINITION p
            on r.PATTERN_ID = p.PATTERN_ID
        where r.MARKET_TYPE = :v_market_type
          and r.INTERVAL_MINUTES = :v_interval_minutes
          and r.TS between :P_FROM_TS and :P_TO_TS
          and o.OUTCOME_LABEL in ('HIT', 'MISS', 'NEUTRAL')
          and o.RETURN_REALIZED_DEC is not null
        group by r.PATTERN_ID, r.SYMBOL
    );

    v_rows := sqlrowcount;

    return 'Backtest run ' || :v_run_id || ' created with ' || :v_rows || ' result rows.';
end;
$$;
