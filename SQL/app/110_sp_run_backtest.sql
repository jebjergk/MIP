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
returns variant
language sql
as
$$
declare
    v_market_type           string;
    v_interval_minutes      number;
    v_run_id                number;
    v_rows                  number;
    v_trade_count           number;
    v_hit_count             number;
    v_miss_count            number;
    v_neutral_count         number;
    v_hit_rate              float;
    v_avg_return            float;
    v_std_return            float;
    v_cum_return            float;
    v_pattern_score         float;
    v_pattern_results       array;
    v_pattern_row_count     number;
begin
    v_market_type := coalesce(P_MARKET_TYPE, 'STOCK');
    v_interval_minutes := coalesce(P_INTERVAL_MINUTES, 5);
    v_pattern_results := array_construct();

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

    for rec in (
        select PATTERN_ID, NAME
        from MIP.APP.PATTERN_DEFINITION
        where coalesce(ENABLED, true) = true
          and coalesce(IS_ACTIVE, 'Y') = 'Y'
    ) do
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
            rec.PATTERN_ID,
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
                r.SYMBOL,
                count(*) as TRADE_COUNT,
                count_if(o.OUTCOME_LABEL = 'HIT') as HIT_COUNT,
                count_if(o.OUTCOME_LABEL = 'MISS') as MISS_COUNT,
                count_if(o.OUTCOME_LABEL = 'NEUTRAL') as NEUTRAL_COUNT,
                avg(o.RETURN_REALIZED_DEC) as AVG_RETURN,
                stddev(o.RETURN_REALIZED_DEC) as STD_RETURN,
                sum(o.RETURN_REALIZED_DEC) as CUM_RETURN,
                object_construct(
                    'pattern_name', max(rec.NAME),
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
            where r.PATTERN_ID = rec.PATTERN_ID
              and r.MARKET_TYPE = :v_market_type
              and r.INTERVAL_MINUTES = :v_interval_minutes
              and r.TS between :P_FROM_TS and :P_TO_TS
              and o.OUTCOME_LABEL in ('HIT', 'MISS', 'NEUTRAL')
              and o.RETURN_REALIZED_DEC is not null
            group by r.SYMBOL
        );

        v_pattern_row_count := sqlrowcount;

        select
            coalesce(sum(TRADE_COUNT), 0),
            coalesce(sum(HIT_COUNT), 0),
            coalesce(sum(MISS_COUNT), 0),
            coalesce(sum(NEUTRAL_COUNT), 0),
            case when sum(TRADE_COUNT) = 0 then null else sum(HIT_COUNT) / sum(TRADE_COUNT) end,
            case when sum(TRADE_COUNT) = 0 then null else sum(AVG_RETURN * TRADE_COUNT) / sum(TRADE_COUNT) end,
            avg(STD_RETURN),
            coalesce(sum(CUM_RETURN), 0)
        into
            v_trade_count,
            v_hit_count,
            v_miss_count,
            v_neutral_count,
            v_hit_rate,
            v_avg_return,
            v_std_return,
            v_cum_return
        from MIP.APP.BACKTEST_RESULT
        where BACKTEST_RUN_ID = :v_run_id
          and PATTERN_ID = rec.PATTERN_ID;

        v_pattern_score := case
            when v_hit_rate is not null and v_cum_return is not null then v_hit_rate * v_cum_return
            else null
        end;

        update MIP.APP.PATTERN_DEFINITION
           set LAST_BACKTEST_RUN_ID = :v_run_id,
               LAST_TRADE_COUNT     = :v_trade_count,
               LAST_HIT_RATE        = :v_hit_rate,
               LAST_CUM_RETURN      = :v_cum_return,
               LAST_AVG_RETURN      = :v_avg_return,
               LAST_STD_RETURN      = :v_std_return,
               PATTERN_SCORE        = :v_pattern_score,
               UPDATED_AT           = current_timestamp(),
               UPDATED_BY           = current_user()
         where PATTERN_ID = rec.PATTERN_ID;

        v_pattern_results := array_append(
            v_pattern_results,
            object_construct(
                'pattern_id', rec.PATTERN_ID,
                'pattern_name', rec.NAME,
                'trade_count', v_trade_count,
                'hit_rate', v_hit_rate,
                'cum_return', v_cum_return,
                'avg_return', v_avg_return,
                'std_return', v_std_return,
                'pattern_score', v_pattern_score,
                'rows_inserted', v_pattern_row_count
            )
        );
    end for;

    select count(*) into v_rows from MIP.APP.BACKTEST_RESULT where BACKTEST_RUN_ID = :v_run_id;

    return object_construct(
        'backtest_run_id', :v_run_id,
        'market_type', :v_market_type,
        'interval_minutes', :v_interval_minutes,
        'horizon_minutes', :P_HORIZON_MINUTES,
        'from_ts', :P_FROM_TS,
        'to_ts', :P_TO_TS,
        'total_rows', :v_rows,
        'patterns', :v_pattern_results
    );
end;
$$;
