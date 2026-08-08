-- Adviser V0.1 — structured watch/invalidation predicates + daily/intraday context
use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.APP;

alter table if exists MIP.APP.BROOKS_INTRADAY_ADVISER_CALL
    add column if not exists WATCH_PREDICATES variant;

alter table if exists MIP.APP.BROOKS_INTRADAY_ADVISER_CALL
    add column if not exists INVALIDATION_PREDICATES variant;

alter table if exists MIP.APP.BROOKS_INTRADAY_ADVISER_CALL
    add column if not exists CONFIRMATION_PREDICATES variant;

alter table if exists MIP.APP.BROOKS_INTRADAY_ADVISER_CALL
    add column if not exists DAILY_INTRADAY_CONTEXT variant;
