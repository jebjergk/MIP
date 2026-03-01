-- 237_news_architecture_contract_checks.sql
-- Purpose: Phase 0 architecture + governance checks for News Context.
--
-- Contract checks covered here:
-- 1) Source governance exists and has required attribution/license fields.
-- 2) Registry defaults are conservative for republishing (republish disabled).
-- 3) UTC normalization contract is set at source policy layer.
--
-- Note:
-- - Static code dependency firewall (no MIP.NEWS refs in signal/training/trust SQL)
--   is validated from repository SQL files in deployment execution step.

use role MIP_ADMIN_ROLE;
use database MIP;

with base as (
    select *
    from MIP.NEWS.NEWS_SOURCE_REGISTRY
),
checks as (
    select
        'REGISTRY_EXISTS' as check_name,
        iff(count(*) >= 1, 'PASS', 'FAIL') as status,
        count(*)::string as observed,
        '>= 1 source rows' as expected
    from base

    union all

    select
        'ATTRIBUTION_REQUIRED_FIELDS',
        iff(
            count_if(
                SOURCE_ID is null
                or trim(SOURCE_ID) = ''
                or SOURCE_NAME is null
                or trim(SOURCE_NAME) = ''
                or FEED_URL is null
                or trim(FEED_URL) = ''
                or TERMS_URL is null
                or trim(TERMS_URL) = ''
            ) = 0,
            'PASS',
            'FAIL'
        ) as status,
        count_if(
            SOURCE_ID is null
            or trim(SOURCE_ID) = ''
            or SOURCE_NAME is null
            or trim(SOURCE_NAME) = ''
            or FEED_URL is null
            or trim(FEED_URL) = ''
            or TERMS_URL is null
            or trim(TERMS_URL) = ''
        )::string as observed,
        '0 rows with missing source attribution fields' as expected
    from base

    union all

    select
        'REPUBLISH_DEFAULT_FALSE',
        iff(count_if(coalesce(REPUBLISH_OK_FLAG, false) = true) = 0, 'PASS', 'FAIL') as status,
        count_if(coalesce(REPUBLISH_OK_FLAG, false) = true)::string as observed,
        '0 sources with REPUBLISH_OK_FLAG=true by default' as expected
    from base

    union all

    select
        'UTC_NORMALIZATION_CONTRACT',
        iff(count_if(upper(coalesce(NORMALIZATION_TIMEZONE, '')) <> 'UTC') = 0, 'PASS', 'FAIL') as status,
        count_if(upper(coalesce(NORMALIZATION_TIMEZONE, '')) <> 'UTC')::string as observed,
        '0 non-UTC normalization timezone rows' as expected
    from base

    union all

    select
        'ALLOWED_ACTIVE_FEED_COUNT',
        iff(count_if(ALLOWED_FLAG and IS_ACTIVE) between 3 and 8, 'PASS', 'FAIL') as status,
        count_if(ALLOWED_FLAG and IS_ACTIVE)::string as observed,
        'between 3 and 8 allowed active feeds for v1' as expected
    from base
)
select *
from checks
order by check_name;
