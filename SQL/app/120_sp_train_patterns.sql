-- 120_sp_train_patterns.sql
-- Purpose: Train pattern definitions from backtest results by updating metrics and activation flags

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_TRAIN_PATTERNS_FROM_BACKTEST(
    P_BACKTEST_RUN_ID   number,
    P_MARKET_TYPE       string,
    P_INTERVAL_MINUTES  number
)
returns varchar
language sql
as
$$
declare
    v_run_id           number;
    v_min_trades       number;
    v_min_hit_rate     float;
    v_min_cum_return   float;
    v_pattern_count    number;
begin
    if (:P_BACKTEST_RUN_ID is not null) then
        v_run_id := :P_BACKTEST_RUN_ID;
    else
        v_run_id := (select max(BACKTEST_RUN_ID)
          from MIP.APP.BACKTEST_RUN
         where MARKET_TYPE = :P_MARKET_TYPE
           and INTERVAL_MINUTES = :P_INTERVAL_MINUTES);
    end if;

    if (:v_run_id is null) then
        return 'No backtest runs found for given market/interval.';
    end if;

    v_min_trades := 
        (select coalesce(to_number(CONFIG_VALUE),30) from MIP.APP.APP_CONFIG where CONFIG_KEY = 'PATTERN_MIN_TRADES');
--        (select coalesce(select to_number(CONFIG_VALUE) from MIP.APP.APP_CONFIG where CONFIG_KEY = 'PATTERN_MIN_TRADES', 30));
    v_min_hit_rate :=
        (select coalesce(to_double(CONFIG_VALUE), 0.55) from MIP.APP.APP_CONFIG where CONFIG_KEY = 'PATTERN_MIN_HIT_RATE');
    v_min_cum_return :=
        (select coalesce(to_double(CONFIG_VALUE), 0.0) from MIP.APP.APP_CONFIG where CONFIG_KEY = 'PATTERN_MIN_CUM_RETURN');
    v_pattern_count := 
        (select count(distinct PATTERN_ID) from MIP.APP.BACKTEST_RESULT where BACKTEST_RUN_ID = :v_run_id);

    merge into MIP.APP.PATTERN_DEFINITION t
    using (
        select
            PATTERN_ID,
            TRADE_COUNT,
            HIT_RATE,
            CUM_RETURN,
            AVG_RETURN,
            STD_RETURN,
            CURRENT_TIMESTAMP() as LAST_TRAINED_AT,
            :v_run_id as LAST_BACKTEST_RUN_ID,
            case
                when TRADE_COUNT >= :v_min_trades
                 and HIT_RATE is not null and HIT_RATE >= :v_min_hit_rate
                 and CUM_RETURN is not null and CUM_RETURN >= :v_min_cum_return then 'Y'
                else 'N'
            end as IS_ACTIVE,
            case when HIT_RATE is not null and CUM_RETURN is not null then HIT_RATE * CUM_RETURN else null end as PATTERN_SCORE
        from (
            select
                PATTERN_ID,
                sum(TRADE_COUNT) as TRADE_COUNT,
                sum(HIT_COUNT) as HIT_COUNT,
                sum(MISS_COUNT) as MISS_COUNT,
                sum(NEUTRAL_COUNT) as NEUTRAL_COUNT,
                case when sum(TRADE_COUNT) = 0 then null else sum(HIT_COUNT) / sum(TRADE_COUNT) end as HIT_RATE,
                case when sum(TRADE_COUNT) = 0 then null else sum(AVG_RETURN * TRADE_COUNT) / sum(TRADE_COUNT) end as AVG_RETURN,
                avg(STD_RETURN) as STD_RETURN,
                sum(CUM_RETURN) as CUM_RETURN
            from MIP.APP.BACKTEST_RESULT
            where BACKTEST_RUN_ID = :v_run_id
            group by PATTERN_ID
        ) agg
    ) s
       on t.PATTERN_ID = s.PATTERN_ID
    when matched then update set
        t.LAST_TRAINED_AT      = s.LAST_TRAINED_AT,
        t.LAST_BACKTEST_RUN_ID = s.LAST_BACKTEST_RUN_ID,
        t.LAST_TRADE_COUNT     = s.TRADE_COUNT,
        t.LAST_HIT_RATE        = s.HIT_RATE,
        t.LAST_CUM_RETURN      = s.CUM_RETURN,
        t.LAST_AVG_RETURN      = s.AVG_RETURN,
        t.LAST_STD_RETURN      = s.STD_RETURN,
        t.PATTERN_SCORE        = s.PATTERN_SCORE,
        t.IS_ACTIVE            = s.IS_ACTIVE;

    return 'Trained ' || :v_pattern_count || ' patterns from backtest run ' || :v_run_id ||
           ' for ' || coalesce(P_MARKET_TYPE, 'UNKNOWN') || '/' || coalesce(P_INTERVAL_MINUTES::string, 'UNKNOWN') || '.';
end;
$$;
