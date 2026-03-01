-- 239_news_phase2_ingestion_idempotency_checks.sql
-- Purpose: Phase 2 ingestion checks (test mode deterministic idempotency).

use role MIP_ADMIN_ROLE;
use database MIP;

set c0 = (select count(*) from MIP.NEWS.NEWS_RAW);
call MIP.NEWS.SP_INGEST_RSS_NEWS(true, 2);
set c1 = (select count(*) from MIP.NEWS.NEWS_RAW);
call MIP.NEWS.SP_INGEST_RSS_NEWS(true, 2);
set c2 = (select count(*) from MIP.NEWS.NEWS_RAW);

with checks as (
    select
        'IDEMPOTENT_SECOND_RUN' as check_name,
        iff($c2 = $c1, 'PASS', 'FAIL') as status,
        ($c2 - $c1)::string as observed,
        '0 net new rows on second run' as expected

    union all

    select
        'FIRST_RUN_INSERTED_NON_NEGATIVE',
        iff(($c1 - $c0) >= 0, 'PASS', 'FAIL'),
        ($c1 - $c0)::string,
        '>= 0'

    union all

    select
        'UNIQUE_NEWS_ID',
        iff(count(*) = 0, 'PASS', 'FAIL'),
        count(*)::string,
        '0 duplicate NEWS_ID rows'
    from (
        select NEWS_ID
        from MIP.NEWS.NEWS_RAW
        group by NEWS_ID
        having count(*) > 1
    )

    union all

    select
        'DEDUP_CLUSTER_POPULATED',
        iff(count(*) > 0, 'PASS', 'FAIL'),
        count(*)::string,
        '> 0 dedup clusters'
    from MIP.NEWS.NEWS_DEDUP

    union all

    select
        'ATTRIBUTION_PRESENT_ON_RAW',
        iff(
            coalesce(count_if(
                SOURCE_ID is null or trim(SOURCE_ID) = ''
                or SOURCE_NAME is null or trim(SOURCE_NAME) = ''
                or URL is null or trim(URL) = ''
            ), 0) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(count_if(
            SOURCE_ID is null or trim(SOURCE_ID) = ''
            or SOURCE_NAME is null or trim(SOURCE_NAME) = ''
            or URL is null or trim(URL) = ''
        ), 0)::string,
        '0 rows with missing SOURCE_ID/SOURCE_NAME/URL'
    from MIP.NEWS.NEWS_RAW

    union all

    select
        'FULL_TEXT_V1_EMPTY',
        iff(coalesce(count_if(FULL_TEXT_OPTIONAL is not null), 0) = 0, 'PASS', 'FAIL'),
        coalesce(count_if(FULL_TEXT_OPTIONAL is not null), 0)::string,
        '0 non-null FULL_TEXT_OPTIONAL rows'
    from MIP.NEWS.NEWS_RAW
)
select * from checks
order by check_name;
