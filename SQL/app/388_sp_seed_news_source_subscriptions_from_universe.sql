-- 388_sp_seed_news_source_subscriptions_from_universe.sql
-- Purpose: Build per-symbol ticker RSS subscriptions from current ingest universe.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.NEWS.SP_SEED_NEWS_SOURCE_SUBSCRIPTIONS_FROM_UNIVERSE()
returns variant
language sql
execute as caller
as
$$
declare
    v_total number := 0;
    v_enabled number := 0;
begin
    merge into MIP.NEWS.NEWS_SOURCE_SUBSCRIPTIONS t
    using (
        with ticker_sources as (
            select
                SOURCE_ID,
                URL_TEMPLATE,
                upper(coalesce(SYMBOL_SCOPE, 'ALL')) as SYMBOL_SCOPE,
                coalesce(ENABLED_FLAG, true) as ENABLED_FLAG
            from MIP.NEWS.NEWS_SOURCE_REGISTRY
            where ALLOWED_FLAG = true
              and IS_ACTIVE = true
              and upper(coalesce(SOURCE_TYPE, 'GLOBAL_RSS')) = 'TICKER_RSS'
              and URL_TEMPLATE is not null
        ),
        universe as (
            select distinct
                upper(SYMBOL) as SYMBOL,
                upper(MARKET_TYPE) as MARKET_TYPE
            from MIP.APP.INGEST_UNIVERSE
            where coalesce(IS_ENABLED, true)
              and INTERVAL_MINUTES = 1440
        )
        select
            sha2_hex(concat(ts.SOURCE_ID, '|', u.SYMBOL, '|', u.MARKET_TYPE), 256) as SUBSCRIPTION_ID,
            ts.SOURCE_ID,
            u.SYMBOL,
            u.MARKET_TYPE,
            replace(ts.URL_TEMPLATE, '{SYMBOL}', u.SYMBOL) as RSS_URL_RESOLVED,
            ts.ENABLED_FLAG
        from ticker_sources ts
        join universe u
          on (
              ts.SYMBOL_SCOPE = 'ALL'
              or (ts.SYMBOL_SCOPE = 'STOCK_ONLY' and u.MARKET_TYPE = 'STOCK')
              or (ts.SYMBOL_SCOPE = 'ETF_ONLY' and u.MARKET_TYPE = 'ETF')
              or (ts.SYMBOL_SCOPE = 'FX_ONLY' and u.MARKET_TYPE = 'FX')
          )
    ) s
    on t.SUBSCRIPTION_ID = s.SUBSCRIPTION_ID
    when matched then update set
        t.RSS_URL_RESOLVED = s.RSS_URL_RESOLVED,
        t.ENABLED_FLAG = s.ENABLED_FLAG,
        t.UPDATED_AT = current_timestamp()
    when not matched then insert (
        SUBSCRIPTION_ID, SOURCE_ID, SYMBOL, MARKET_TYPE, RSS_URL_RESOLVED, ENABLED_FLAG, CREATED_AT, UPDATED_AT
    ) values (
        s.SUBSCRIPTION_ID, s.SOURCE_ID, s.SYMBOL, s.MARKET_TYPE, s.RSS_URL_RESOLVED, s.ENABLED_FLAG, current_timestamp(), current_timestamp()
    );

    select count(*) into :v_total from MIP.NEWS.NEWS_SOURCE_SUBSCRIPTIONS;
    select count(*) into :v_enabled from MIP.NEWS.NEWS_SOURCE_SUBSCRIPTIONS where ENABLED_FLAG = true;

    return object_construct(
        'status', 'SUCCESS',
        'subscriptions_total', :v_total,
        'subscriptions_enabled', :v_enabled
    );
end;
$$;
