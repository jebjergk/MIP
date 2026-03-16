-- 401_sp_ingest_market_bars.sql
-- Purpose: Provider-routed market bar ingestion wrapper.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_INGEST_MARKET_BARS(
    P_INTERVAL_FILTER number default null,
    P_PROVIDER string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_provider string;
    v_result variant;
    v_interval_minutes number;
    v_target_symbols number := 0;
    v_fresh_symbols number := 0;
begin
    v_interval_minutes := coalesce(:P_INTERVAL_FILTER, 1440);
    v_provider := upper(
        coalesce(
            nullif(:P_PROVIDER, ''),
            (
                select CONFIG_VALUE
                from MIP.APP.APP_CONFIG
                where CONFIG_KEY = 'MARKET_DATA_PROVIDER_DEFAULT'
                limit 1
            ),
            'ALPHAVANTAGE'
        )
    );

    if (v_provider = 'ALPHAVANTAGE') then
        v_result := (call MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS(:P_INTERVAL_FILTER));
        select object_insert(:v_result, 'provider', :v_provider, true) into :v_result;
        return :v_result;
    elseif (v_provider = 'IBKR') then
        -- IBKR bars are ingested by the runtime script outside Snowflake.
        -- Treat this call as SUCCESS only when the full enabled universe has
        -- fresh IBKR bars for the requested interval.
        with target as (
            select upper(replace(SYMBOL, '/', '')) as SYMBOL, upper(MARKET_TYPE) as MARKET_TYPE
            from MIP.APP.INGEST_UNIVERSE
            where coalesce(IS_ENABLED, true)
              and INTERVAL_MINUTES = :v_interval_minutes
        ),
        fresh as (
            select upper(replace(SYMBOL, '/', '')) as SYMBOL, upper(MARKET_TYPE) as MARKET_TYPE
            from MIP.MART.MARKET_BARS
            where INTERVAL_MINUTES = :v_interval_minutes
              and upper(coalesce(SOURCE, '')) = 'IBKR'
              and INGESTED_AT >= dateadd(minute, -(coalesce(:v_interval_minutes, 1440) + 60), current_timestamp())
            group by 1, 2
        )
        select count(*), count(f.SYMBOL)
          into :v_target_symbols, :v_fresh_symbols
          from target t
          left join fresh f
            on f.SYMBOL = t.SYMBOL
           and f.MARKET_TYPE = t.MARKET_TYPE;

        if (coalesce(:v_target_symbols, 0) > 0 and :v_fresh_symbols = :v_target_symbols) then
            return object_construct(
                'status', 'SUCCESS_EXTERNAL_IBKR',
                'provider', :v_provider,
                'rows_inserted', 0,
                'symbols_processed', :v_fresh_symbols,
                'external_fresh_symbols', :v_fresh_symbols,
                'external_target_symbols', :v_target_symbols,
                'message', 'Using freshly ingested external IBKR bars.'
            );
        end if;

        return object_construct(
            'status', 'FAIL_IBKR_AGENT_REQUIRED',
            'provider', :v_provider,
            'rows_inserted', 0,
            'symbols_processed', coalesce(:v_fresh_symbols, 0),
            'external_fresh_symbols', :v_fresh_symbols,
            'external_target_symbols', :v_target_symbols,
            'error', 'IBKR ingestion requires agent runtime execution (cursorfiles/ingest_ibkr_bars.py), and fresh bars are not yet complete.'
        );
    else
        return object_construct(
            'status', 'FAIL_UNKNOWN_PROVIDER',
            'provider', :v_provider,
            'rows_inserted', 0,
            'symbols_processed', 0,
            'error', 'Unsupported provider. Expected ALPHAVANTAGE or IBKR.'
        );
    end if;
end;
$$;
