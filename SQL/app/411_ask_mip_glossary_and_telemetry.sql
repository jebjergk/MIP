use role MIP_ADMIN_ROLE;
use database MIP;

create schema if not exists MIP.APP;
create schema if not exists MIP.AGENT_OUT;
create schema if not exists MIP.MART;

create table if not exists MIP.APP.GLOSSARY_TERM (
  TERM_KEY varchar not null,
  DISPLAY_TERM varchar not null,
  ALIASES variant,
  CATEGORY varchar,
  DEFINITION_SHORT varchar,
  DEFINITION_LONG varchar,
  MIP_SPECIFIC_MEANING varchar,
  GENERAL_MARKET_MEANING varchar,
  EXAMPLE_IN_MIP varchar,
  RELATED_TERMS variant,
  SOURCE_TYPE varchar,
  SOURCE_REF varchar,
  IS_APPROVED boolean default false,
  REVIEW_STATUS varchar default 'pending',
  LAST_REVIEWED_AT timestamp_ntz,
  CREATED_AT timestamp_ntz default current_timestamp(),
  UPDATED_AT timestamp_ntz default current_timestamp(),
  constraint PK_GLOSSARY_TERM primary key (TERM_KEY)
);

create table if not exists MIP.AGENT_OUT.GLOSSARY_CANDIDATE_TERM (
  CANDIDATE_ID number autoincrement start 1 increment 1,
  TERM_TEXT varchar not null,
  CATEGORY varchar,
  SOURCE_TYPE varchar,
  SOURCE_REF varchar,
  RECOMMENDED_DEFINITION varchar,
  REVIEW_STATUS varchar default 'pending',
  REVIEWED_AT timestamp_ntz,
  CREATED_AT timestamp_ntz default current_timestamp(),
  constraint PK_GLOSSARY_CANDIDATE_TERM primary key (CANDIDATE_ID)
);

create table if not exists MIP.AGENT_OUT.GLOSSARY_REVIEW_EVENT (
  EVENT_ID number autoincrement start 1 increment 1,
  EVENT_TS timestamp_ntz default current_timestamp(),
  CANDIDATE_ID number,
  DECISION varchar,
  REVIEWER_NOTES varchar,
  constraint PK_GLOSSARY_REVIEW_EVENT primary key (EVENT_ID)
);

create table if not exists MIP.AGENT_OUT.ASK_QUERY_EVENT (
  QUERY_ID number autoincrement start 1 increment 1,
  QUERY_TS timestamp_ntz default current_timestamp(),
  QUESTION varchar,
  ROUTE varchar,
  INTENT varchar,
  NORMALIZED_TERMS variant,
  MATCHED_SOURCE_TYPES variant,
  DOCS_CONFIDENCE float,
  GLOSSARY_CONFIDENCE float,
  WEB_CONFIDENCE float,
  OVERALL_CONFIDENCE float,
  WEB_FALLBACK_USED boolean default false,
  ANSWER_FAILED boolean default false,
  SUGGESTED_TERMS variant,
  UNKNOWN_TERMS variant,
  constraint PK_ASK_QUERY_EVENT primary key (QUERY_ID)
);

create or replace view MIP.MART.V_ASK_COVERAGE_METRICS as
select
  to_date(QUERY_TS) as DAY,
  count(*) as TOTAL_QUERIES,
  sum(case when array_contains('DOC'::variant, MATCHED_SOURCE_TYPES) then 1 else 0 end) as DOC_QUERIES,
  sum(case when array_contains('GLOSSARY'::variant, MATCHED_SOURCE_TYPES) then 1 else 0 end) as GLOSSARY_QUERIES,
  sum(case when WEB_FALLBACK_USED then 1 else 0 end) as WEB_FALLBACK_QUERIES,
  sum(case when ANSWER_FAILED then 1 else 0 end) as UNRESOLVED_QUERIES
from MIP.AGENT_OUT.ASK_QUERY_EVENT
group by 1;

create or replace view MIP.MART.V_ASK_UNKNOWN_TERMS as
select
  value::string as UNKNOWN_TERM,
  count(*) as ASK_COUNT,
  max(QUERY_TS) as LAST_SEEN_AT
from MIP.AGENT_OUT.ASK_QUERY_EVENT,
lateral flatten(input => UNKNOWN_TERMS)
group by 1;

merge into MIP.APP.GLOSSARY_TERM t
using (
  select column1 as TERM_KEY, column2 as DISPLAY_TERM, column3 as CATEGORY
  from values
    ('trusted','trusted','signals'),
    ('watch','watch','signals'),
    ('enable','enable','ui'),
    ('disable','disable','ui'),
    ('conviction','conviction','signals'),
    ('confidence','confidence','signals'),
    ('max hold','max hold','trading'),
    ('horizon','horizon','trading'),
    ('payout ratio','payout ratio','risk'),
    ('drawdown','drawdown','risk'),
    ('retrigger','retrigger','signals'),
    ('volatility expansion','volatility expansion','signals'),
    ('entry feasibility','entry feasibility','risk'),
    ('risk overlay','risk overlay','risk'),
    ('signal quality','signal quality','signals'),
    ('catalyst','catalyst','research'),
    ('slippage','slippage','trading'),
    ('gtc','GTC','trading'),
    ('exposure','exposure','risk'),
    ('committee view','committee view','research'),
    ('morning brief','morning brief','brief')
) s
on t.TERM_KEY = s.TERM_KEY
when matched then update set
  DISPLAY_TERM = s.DISPLAY_TERM,
  CATEGORY = s.CATEGORY,
  SOURCE_TYPE = coalesce(t.SOURCE_TYPE, 'SEED'),
  SOURCE_REF = coalesce(t.SOURCE_REF, '411_ask_mip_glossary_and_telemetry.sql'),
  UPDATED_AT = current_timestamp()
when not matched then insert (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY, DEFINITION_SHORT, DEFINITION_LONG,
  MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING, EXAMPLE_IN_MIP, RELATED_TERMS,
  SOURCE_TYPE, SOURCE_REF, IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
)
values (
  s.TERM_KEY, s.DISPLAY_TERM, parse_json('[]'), s.CATEGORY, '', '', '', '', '', parse_json('[]'),
  'SEED', '411_ask_mip_glossary_and_telemetry.sql', true, 'approved', current_timestamp(), current_timestamp()
);
