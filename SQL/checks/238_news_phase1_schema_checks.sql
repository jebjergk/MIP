-- 238_news_phase1_schema_checks.sql
-- Purpose: Phase 1 schema and config acceptance checks.

use role MIP_ADMIN_ROLE;
use database MIP;

with table_checks as (
    select
        'TABLE_NEWS_RAW_EXISTS' as check_name,
        iff(count(*) = 1, 'PASS', 'FAIL') as status,
        count(*)::string as observed,
        '1' as expected
    from MIP.INFORMATION_SCHEMA.TABLES
    where TABLE_SCHEMA = 'NEWS'
      and TABLE_NAME = 'NEWS_RAW'

    union all

    select
        'TABLE_NEWS_SYMBOL_MAP_EXISTS',
        iff(count(*) = 1, 'PASS', 'FAIL'),
        count(*)::string,
        '1'
    from MIP.INFORMATION_SCHEMA.TABLES
    where TABLE_SCHEMA = 'NEWS'
      and TABLE_NAME = 'NEWS_SYMBOL_MAP'

    union all

    select
        'TABLE_NEWS_DEDUP_EXISTS',
        iff(count(*) = 1, 'PASS', 'FAIL'),
        count(*)::string,
        '1'
    from MIP.INFORMATION_SCHEMA.TABLES
    where TABLE_SCHEMA = 'NEWS'
      and TABLE_NAME = 'NEWS_DEDUP'

    union all

    select
        'TABLE_NEWS_INFO_STATE_DAILY_EXISTS',
        iff(count(*) = 1, 'PASS', 'FAIL'),
        count(*)::string,
        '1'
    from MIP.INFORMATION_SCHEMA.TABLES
    where TABLE_SCHEMA = 'NEWS'
      and TABLE_NAME = 'NEWS_INFO_STATE_DAILY'

    union all

    select
        'TABLE_SYMBOL_ALIAS_DICT_EXISTS',
        iff(count(*) = 1, 'PASS', 'FAIL'),
        count(*)::string,
        '1'
    from MIP.INFORMATION_SCHEMA.TABLES
    where TABLE_SCHEMA = 'NEWS'
      and TABLE_NAME = 'SYMBOL_ALIAS_DICT'
),
column_checks as (
    select
        'INFO_STATE_HAS_FRESHNESS_COLUMNS' as check_name,
        iff(
            count_if(COLUMN_NAME = 'LAST_NEWS_PUBLISHED_AT') = 1
            and count_if(COLUMN_NAME = 'LAST_INGESTED_AT') = 1
            and count_if(COLUMN_NAME = 'SNAPSHOT_TS') = 1,
            'PASS',
            'FAIL'
        ) as status,
        count(*)::string as observed,
        'contains LAST_NEWS_PUBLISHED_AT, LAST_INGESTED_AT, SNAPSHOT_TS' as expected
    from MIP.INFORMATION_SCHEMA.COLUMNS
    where TABLE_SCHEMA = 'NEWS'
      and TABLE_NAME = 'NEWS_INFO_STATE_DAILY'

    union all

    select
        'RAW_HAS_REQUIRED_ATTRIBUTION_COLUMNS',
        iff(
            count_if(COLUMN_NAME = 'SOURCE_ID') = 1
            and count_if(COLUMN_NAME = 'SOURCE_NAME') = 1
            and count_if(COLUMN_NAME = 'URL') = 1
            and count_if(COLUMN_NAME = 'FULL_TEXT_OPTIONAL') = 1,
            'PASS',
            'FAIL'
        ),
        count(*)::string,
        'contains SOURCE_ID, SOURCE_NAME, URL, FULL_TEXT_OPTIONAL'
    from MIP.INFORMATION_SCHEMA.COLUMNS
    where TABLE_SCHEMA = 'NEWS'
      and TABLE_NAME = 'NEWS_RAW'
),
config_checks as (
    select
        'NEWS_CONFIG_KEYS_PRESENT' as check_name,
        iff(count(*) = 9, 'PASS', 'FAIL') as status,
        count(*)::string as observed,
        '9' as expected
    from MIP.APP.APP_CONFIG
    where CONFIG_KEY in (
        'NEWS_ENABLED',
        'NEWS_SOURCES',
        'NEWS_MATCH_CONFIDENCE_MIN',
        'NEWS_BURST_Z_HOT',
        'NEWS_DISPLAY_ONLY',
        'NEWS_MODULATION_ENABLED',
        'NEWS_STALENESS_THRESHOLD_MINUTES',
        'NEWS_RETENTION_DAYS_HOT',
        'NEWS_RETENTION_DAYS_ARCHIVE'
    )

    union all

    select
        'NEWS_DISPLAY_ONLY_DEFAULT_TRUE',
        iff(max(iff(CONFIG_KEY = 'NEWS_DISPLAY_ONLY', lower(CONFIG_VALUE), null)) = 'true', 'PASS', 'FAIL'),
        coalesce(max(iff(CONFIG_KEY = 'NEWS_DISPLAY_ONLY', CONFIG_VALUE, null)), 'NULL') as observed,
        'true' as expected
    from MIP.APP.APP_CONFIG
    where CONFIG_KEY = 'NEWS_DISPLAY_ONLY'

    union all

    select
        'NEWS_MODULATION_DEFAULT_FALSE',
        iff(max(iff(CONFIG_KEY = 'NEWS_MODULATION_ENABLED', lower(CONFIG_VALUE), null)) = 'false', 'PASS', 'FAIL'),
        coalesce(max(iff(CONFIG_KEY = 'NEWS_MODULATION_ENABLED', CONFIG_VALUE, null)), 'NULL') as observed,
        'false' as expected
    from MIP.APP.APP_CONFIG
    where CONFIG_KEY = 'NEWS_MODULATION_ENABLED'
),
grant_checks as (
    select
        'UI_ROLE_SCHEMA_USAGE_NEWS' as check_name,
        iff(count(*) = 6, 'PASS', 'FAIL') as status,
        count(*)::string as observed,
        '6 NEWS table SELECT grants' as expected
    from MIP.INFORMATION_SCHEMA.TABLE_PRIVILEGES
    where GRANTEE = 'MIP_UI_API_ROLE'
      and TABLE_SCHEMA = 'NEWS'
      and PRIVILEGE_TYPE = 'SELECT'
)
select * from table_checks
union all
select * from column_checks
union all
select * from config_checks
union all
select * from grant_checks
order by check_name;
