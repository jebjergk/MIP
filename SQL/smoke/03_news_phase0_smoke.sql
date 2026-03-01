-- 03_news_phase0_smoke.sql
-- Purpose: Phase 0 smoke checks for News Context governance setup.
-- Scope: source registry + architectural guardrail diagnostics.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Source registry baseline.
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
    REVIEWED_BY,
    REVIEWED_AT,
    UPDATED_AT
from MIP.NEWS.NEWS_SOURCE_REGISTRY
order by SOURCE_ID;

-- 2) Conservative licensing posture summary.
select
    count(*) as total_sources,
    count_if(ALLOWED_FLAG) as allowed_sources,
    count_if(ALLOWED_FLAG and IS_ACTIVE) as allowed_active_sources,
    count_if(coalesce(REPUBLISH_OK_FLAG, false)) as republish_allowed_sources,
    count_if(upper(coalesce(NORMALIZATION_TIMEZONE, '')) <> 'UTC') as non_utc_sources
from MIP.NEWS.NEWS_SOURCE_REGISTRY;

-- 3) Architecture contract diagnostic (informational):
--    This check remains display-only in SQL. Enforced static dependency scan
--    is executed from repository files during deployment.
select
    'NEWS_CONTEXT_ONLY_CONTRACT' as check_name,
    'Signals come from price, decisions are interpreted in context' as contract_text,
    'STATIC_SQL_SCAN_REQUIRED' as enforcement_mode;
