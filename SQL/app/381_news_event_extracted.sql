-- 381_news_event_extracted.sql
-- Purpose: Phase A table for structured news-event extraction output.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.NEWS.NEWS_EVENT_EXTRACTED (
    EXTRACT_ID              string         not null,
    NEWS_ID                 string         not null,
    SYMBOL                  string         not null,
    MARKET_TYPE             string         not null,
    EVENT_TS                timestamp_ntz  not null,
    EVENT_TYPE              string         not null,
    DIRECTION               string         not null,
    CONFIDENCE              number(6,5)    not null,
    IMPACT_HORIZON          string         not null,
    RELEVANCE_SCOPE         string         not null,
    THEME_TAGS              variant,
    EVENT_SUMMARY           string,
    KEY_FACTS               variant,
    EVENT_RISK_SCORE        number(10,6),
    RAW_EXTRACT_VARIANT     variant,
    LLM_USED                boolean        not null default false,
    LLM_MODEL               string,
    PROMPT_VERSION          string         not null,
    INPUT_HASH              string         not null,
    OUTPUT_HASH             string         not null,
    EXTRACTED_AT            timestamp_ntz  not null default current_timestamp(),
    RUN_ID                  string,
    CREATED_AT              timestamp_ntz  not null default current_timestamp(),
    UPDATED_AT              timestamp_ntz  not null default current_timestamp(),
    constraint PK_NEWS_EVENT_EXTRACTED primary key (EXTRACT_ID)
);

alter table if exists MIP.NEWS.NEWS_EVENT_EXTRACTED
    cluster by (EVENT_TS::date, SYMBOL, MARKET_TYPE);
