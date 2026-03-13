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
begin
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
        -- IBKR ingestion is currently executed by agent runtime scripts that can
        -- reach local IB Gateway/TWS, then write into MIP.MART.MARKET_BARS.
        return object_construct(
            'status', 'SKIPPED_IBKR_AGENT_REQUIRED',
            'provider', :v_provider,
            'rows_inserted', 0,
            'symbols_processed', 0,
            'message', 'IBKR ingestion requires agent runtime execution (cursorfiles/ingest_ibkr_bars.py).'
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
