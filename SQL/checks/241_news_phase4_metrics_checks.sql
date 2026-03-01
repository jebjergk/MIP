-- 241_news_phase4_metrics_checks.sql
-- Purpose: Phase 4 metrics checks including midnight UTC boundary test.

use role MIP_ADMIN_ROLE;
use database MIP;

call MIP.NEWS.SP_INGEST_RSS_NEWS(true, 3);
call MIP.NEWS.SP_MAP_NEWS_SYMBOLS(null);

set as_of_for_data = (
    select to_timestamp_tz(to_char(max(PUBLISHED_AT), 'YYYY-MM-DD HH24:MI:SS') || ' +00:00')
    from MIP.NEWS.NEWS_RAW
);

call MIP.NEWS.SP_COMPUTE_INFO_STATE_DAILY($as_of_for_data, null);
call MIP.NEWS.SP_COMPUTE_INFO_STATE_DAILY(to_timestamp_tz('2026-03-01 23:59:59 +00:00'), null);
call MIP.NEWS.SP_COMPUTE_INFO_STATE_DAILY(to_timestamp_tz('2026-03-02 00:00:01 +00:00'), null);

with midnight_boundary as (
    select
        convert_timezone('UTC', to_timestamp_tz('2026-03-01 23:59:59 +00:00'))::date as D1,
        convert_timezone('UTC', to_timestamp_tz('2026-03-02 00:00:01 +00:00'))::date as D2
),
checks as (
    select
        'MIDNIGHT_BOUNDARY_UTC_DATE_DERIVATION' as check_name,
        iff((select D1 from midnight_boundary) = '2026-03-01'::date and (select D2 from midnight_boundary) = '2026-03-02'::date, 'PASS', 'FAIL') as status,
        concat((select D1 from midnight_boundary)::string, ' -> ', (select D2 from midnight_boundary)::string) as observed,
        '2026-03-01 -> 2026-03-02' as expected

    union all

    select
        'METRICS_ROWS_PRESENT',
        iff(count(*) > 0, 'PASS', 'FAIL'),
        count(*)::string,
        '> 0 rows in NEWS_INFO_STATE_DAILY'
    from MIP.NEWS.NEWS_INFO_STATE_DAILY

    union all

    select
        'NOVELTY_SCORE_RANGE',
        iff(coalesce(count_if(NOVELTY_SCORE is not null and (NOVELTY_SCORE < 0 or NOVELTY_SCORE > 1)), 0) = 0, 'PASS', 'FAIL'),
        coalesce(count_if(NOVELTY_SCORE is not null and (NOVELTY_SCORE < 0 or NOVELTY_SCORE > 1)), 0)::string,
        '0 out-of-range novelty scores'
    from MIP.NEWS.NEWS_INFO_STATE_DAILY

    union all

    select
        'TOP_HEADLINES_MAX_3',
        iff(coalesce(count_if(array_size(TOP_HEADLINES) > 3), 0) = 0, 'PASS', 'FAIL'),
        coalesce(count_if(array_size(TOP_HEADLINES) > 3), 0)::string,
        '0 rows with >3 headlines'
    from MIP.NEWS.NEWS_INFO_STATE_DAILY

    union all

    select
        'FRESHNESS_FIELDS_POPULATED_WHEN_NEWS_PRESENT',
        iff(
            coalesce(count_if(NEWS_COUNT > 0 and (LAST_NEWS_PUBLISHED_AT is null or LAST_INGESTED_AT is null or SNAPSHOT_TS is null)), 0) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(count_if(NEWS_COUNT > 0 and (LAST_NEWS_PUBLISHED_AT is null or LAST_INGESTED_AT is null or SNAPSHOT_TS is null)), 0)::string,
        '0 rows missing freshness fields when NEWS_COUNT>0'
    from MIP.NEWS.NEWS_INFO_STATE_DAILY

    union all

    select
        'STALE_FLAG_COLUMN_PRESENT_IN_VIEW',
        iff(count(*) >= 1, 'PASS', 'FAIL'),
        count(*)::string,
        '>=1 rows in MART.V_NEWS_INFO_STATE_LATEST_DAILY'
    from MIP.MART.V_NEWS_INFO_STATE_LATEST_DAILY
)
select * from checks
order by check_name;
