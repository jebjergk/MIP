-- 370_news_source_registry.sql
-- Purpose: Phase 0 foundation for approved/licensable news sources.
-- Architecture contract:
--   - News is decision-time context only.
--   - No signal generation/training/trust object may depend on MIP.NEWS.
--   - No full article text is stored in v1 (title/snippet/link only later).
-- Time contract:
--   - published_at is normalized to UTC in downstream ingestion.
--   - as_of_date derivation is anchored to :as_of_ts interpreted in UTC.

use role MIP_ADMIN_ROLE;
use database MIP;

create schema if not exists MIP.NEWS;

create table if not exists MIP.NEWS.NEWS_SOURCE_REGISTRY (
    SOURCE_ID                string         not null,
    SOURCE_NAME              string         not null,
    FEED_URL                 string         not null,
    TERMS_URL                string         not null,
    LICENSE_NOTES            string,
    ALLOWED_FLAG             boolean        not null default false,
    REPUBLISH_OK_FLAG        boolean        not null default false,
    NORMALIZATION_TIMEZONE   string         not null default 'UTC',
    INGEST_CADENCE_MINUTES   number         not null default 30,
    IS_ACTIVE                boolean        not null default true,
    REVIEWED_BY              string,
    REVIEWED_AT              timestamp_ntz,
    CREATED_AT               timestamp_ntz  not null default current_timestamp(),
    UPDATED_AT               timestamp_ntz  not null default current_timestamp(),
    constraint PK_NEWS_SOURCE_REGISTRY primary key (SOURCE_ID)
);

-- Phase A extensions for scalable ticker feed management.
alter table if exists MIP.NEWS.NEWS_SOURCE_REGISTRY
    add column if not exists SOURCE_TYPE string default 'GLOBAL_RSS';
alter table if exists MIP.NEWS.NEWS_SOURCE_REGISTRY
    add column if not exists URL_TEMPLATE string;
alter table if exists MIP.NEWS.NEWS_SOURCE_REGISTRY
    add column if not exists SYMBOL_SCOPE string default 'ALL';
alter table if exists MIP.NEWS.NEWS_SOURCE_REGISTRY
    add column if not exists ENABLED_FLAG boolean default true;
alter table if exists MIP.NEWS.NEWS_SOURCE_REGISTRY
    add column if not exists POLL_MINUTES number default 30;

merge into MIP.NEWS.NEWS_SOURCE_REGISTRY t
using (
    select
        'SEC_XBRL_ALL' as SOURCE_ID,
        'SEC EDGAR XBRL (All Filings)' as SOURCE_NAME,
        'https://www.sec.gov/Archives/edgar/xbrlrss.all.xml' as FEED_URL,
        'https://www.sec.gov/about/rss-feeds' as TERMS_URL,
        'Regulatory filings feed, keep attribution and source links.' as LICENSE_NOTES,
        true as ALLOWED_FLAG,
        false as REPUBLISH_OK_FLAG,
        'UTC' as NORMALIZATION_TIMEZONE,
        30 as INGEST_CADENCE_MINUTES,
        true as IS_ACTIVE,
        'GLOBAL_RSS' as SOURCE_TYPE,
        null as URL_TEMPLATE,
        'ALL' as SYMBOL_SCOPE,
        true as ENABLED_FLAG,
        30 as POLL_MINUTES
    union all
    select
        'GLOBENEWSWIRE_RSS',
        'GlobeNewswire RSS',
        'https://www.globenewswire.com/rss/list',
        'https://www.globenewswire.com/Home/About/Media-Relations',
        'Use only approved feed categories, title/snippet/link display in v1.',
        true,
        false,
        'UTC',
        30,
        true,
        'GLOBAL_RSS',
        null,
        'STOCK_ONLY',
        true,
        120
    union all
    select
        'MARKETWATCH_RSS',
        'MarketWatch RSS',
        'https://www.marketwatch.com/site/rss',
        'https://www.marketwatch.com/site/rss',
        'Feed use must follow publisher terms, no full-text storage in v1.',
        true,
        false,
        'UTC',
        30,
        true,
        'GLOBAL_RSS',
        null,
        'STOCK_ONLY',
        true,
        120
    union all
    select
        'FED_RSS',
        'Federal Reserve Press Releases',
        'https://www.federalreserve.gov/feeds/press_all.xml',
        'https://www.federalreserve.gov/feeds/feeds.htm',
        'Macro policy context feed.',
        true,
        false,
        'UTC',
        30,
        true,
        'GLOBAL_RSS',
        null,
        'FX_ONLY',
        true,
        60
    union all
    select
        'ECB_RSS',
        'ECB Press Releases',
        'https://www.ecb.europa.eu/rss/press.html',
        'https://www.ecb.europa.eu/home/rss/html/index.en.html',
        'Macro policy context feed.',
        true,
        false,
        'UTC',
        30,
        true,
        'GLOBAL_RSS',
        null,
        'FX_ONLY',
        true,
        60
    union all
    select
        'SEC_RSS_INDEX',
        'SEC RSS Directory',
        'https://www.sec.gov/about/rss-feeds',
        'https://www.sec.gov/about/rss-feeds',
        'Directory source to support feed governance and backup references.',
        true,
        false,
        'UTC',
        30,
        true,
        'GLOBAL_RSS',
        null,
        'ALL',
        false,
        240
    union all
    select
        'SEEKING_ALPHA_TICKER_RSS',
        'Seeking Alpha Combined Symbol RSS',
        'https://seekingalpha.com/api/sa/combined/AAPL.xml',
        'https://seekingalpha.com/page/terms-of-use',
        'Ticker template feed. Validate licensing/terms before production use.',
        true,
        false,
        'UTC',
        120,
        true,
        'TICKER_RSS',
        'https://seekingalpha.com/api/sa/combined/{SYMBOL}.xml',
        'STOCK_ONLY',
        false,
        120
    union all
    select
        'NASDAQ_TICKER_RSS',
        'Nasdaq Symbol News RSS',
        'https://www.nasdaq.com/feed/rssoutbound?symbol=AAPL',
        'https://www.nasdaq.com/terms-and-conditions',
        'Ticker template feed. Validate endpoint stability and terms before use.',
        true,
        false,
        'UTC',
        120,
        true,
        'TICKER_RSS',
        'https://www.nasdaq.com/feed/rssoutbound?symbol={SYMBOL}',
        'STOCK_ONLY',
        false,
        120
    union all
    select
        'YAHOO_TICKER_RSS',
        'Yahoo Finance Symbol RSS',
        'https://feeds.finance.yahoo.com/rss/2.0/headline?s=AAPL&region=US&lang=en-US',
        'https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html',
        'Ticker template feed. Validate endpoint stability and terms before use.',
        true,
        false,
        'UTC',
        120,
        true,
        'TICKER_RSS',
        'https://feeds.finance.yahoo.com/rss/2.0/headline?s={SYMBOL}&region=US&lang=en-US',
        'STOCK_ONLY',
        false,
        120
) s
on t.SOURCE_ID = s.SOURCE_ID
when matched then update set
    t.SOURCE_NAME = s.SOURCE_NAME,
    t.FEED_URL = s.FEED_URL,
    t.TERMS_URL = s.TERMS_URL,
    t.LICENSE_NOTES = s.LICENSE_NOTES,
    t.ALLOWED_FLAG = s.ALLOWED_FLAG,
    t.REPUBLISH_OK_FLAG = s.REPUBLISH_OK_FLAG,
    t.NORMALIZATION_TIMEZONE = s.NORMALIZATION_TIMEZONE,
    t.INGEST_CADENCE_MINUTES = s.INGEST_CADENCE_MINUTES,
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.SOURCE_TYPE = s.SOURCE_TYPE,
    t.URL_TEMPLATE = s.URL_TEMPLATE,
    t.SYMBOL_SCOPE = s.SYMBOL_SCOPE,
    t.ENABLED_FLAG = s.ENABLED_FLAG,
    t.POLL_MINUTES = s.POLL_MINUTES,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (
    SOURCE_ID,
    SOURCE_NAME,
    FEED_URL,
    TERMS_URL,
    LICENSE_NOTES,
    ALLOWED_FLAG,
    REPUBLISH_OK_FLAG,
    NORMALIZATION_TIMEZONE,
    INGEST_CADENCE_MINUTES,
    IS_ACTIVE,
    SOURCE_TYPE,
    URL_TEMPLATE,
    SYMBOL_SCOPE,
    ENABLED_FLAG,
    POLL_MINUTES,
    UPDATED_AT
) values (
    s.SOURCE_ID,
    s.SOURCE_NAME,
    s.FEED_URL,
    s.TERMS_URL,
    s.LICENSE_NOTES,
    s.ALLOWED_FLAG,
    s.REPUBLISH_OK_FLAG,
    s.NORMALIZATION_TIMEZONE,
    s.INGEST_CADENCE_MINUTES,
    s.IS_ACTIVE,
    s.SOURCE_TYPE,
    s.URL_TEMPLATE,
    s.SYMBOL_SCOPE,
    s.ENABLED_FLAG,
    s.POLL_MINUTES,
    current_timestamp()
);

select
    SOURCE_ID,
    SOURCE_NAME,
    FEED_URL,
    TERMS_URL,
    ALLOWED_FLAG,
    REPUBLISH_OK_FLAG,
    NORMALIZATION_TIMEZONE,
    INGEST_CADENCE_MINUTES,
    IS_ACTIVE,
    SOURCE_TYPE,
    URL_TEMPLATE,
    SYMBOL_SCOPE,
    ENABLED_FLAG,
    POLL_MINUTES
from MIP.NEWS.NEWS_SOURCE_REGISTRY
order by SOURCE_ID;
