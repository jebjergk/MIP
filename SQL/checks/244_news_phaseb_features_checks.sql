-- 244_news_phaseb_features_checks.sql
-- Purpose: Phase B checks for bounded feature vectors and structural integrity.

use role MIP_ADMIN_ROLE;
use database MIP;

with checks as (
    select
        'ROWS_PRESENT' as check_name,
        iff(count(*) > 0, 'PASS', 'FAIL') as status,
        count(*)::string as observed,
        '> 0 rows in NEWS_FEATURES_SNAPSHOT' as expected
    from MIP.NEWS.NEWS_FEATURES_SNAPSHOT

    union all

    select
        'PRESSURE_BOUNDED_0_1',
        iff(coalesce(count_if(NEWS_PRESSURE < 0 or NEWS_PRESSURE > 1), 0) = 0, 'PASS', 'FAIL'),
        coalesce(count_if(NEWS_PRESSURE < 0 or NEWS_PRESSURE > 1), 0)::string,
        '0 rows outside NEWS_PRESSURE bounds'
    from MIP.NEWS.NEWS_FEATURES_SNAPSHOT

    union all

    select
        'SENTIMENT_BOUNDED_NEG1_POS1',
        iff(coalesce(count_if(NEWS_SENTIMENT < -1 or NEWS_SENTIMENT > 1), 0) = 0, 'PASS', 'FAIL'),
        coalesce(count_if(NEWS_SENTIMENT < -1 or NEWS_SENTIMENT > 1), 0)::string,
        '0 rows outside NEWS_SENTIMENT bounds'
    from MIP.NEWS.NEWS_FEATURES_SNAPSHOT

    union all

    select
        'RISK_UNCERTAINTY_MACRO_BOUNDED',
        iff(
            coalesce(
                count_if(
                    UNCERTAINTY_SCORE < 0 or UNCERTAINTY_SCORE > 1
                    or EVENT_RISK_SCORE < 0 or EVENT_RISK_SCORE > 1
                    or MACRO_HEAT < 0 or MACRO_HEAT > 1
                ),
                0
            ) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(
            count_if(
                UNCERTAINTY_SCORE < 0 or UNCERTAINTY_SCORE > 1
                or EVENT_RISK_SCORE < 0 or EVENT_RISK_SCORE > 1
                or MACRO_HEAT < 0 or MACRO_HEAT > 1
            ),
            0
        )::string,
        '0 rows outside [0,1] for uncertainty/risk/macro'
    from MIP.NEWS.NEWS_FEATURES_SNAPSHOT

    union all

    select
        'TOP_EVENTS_MAX_3',
        iff(coalesce(count_if(array_size(TOP_EVENTS) > 3), 0) = 0, 'PASS', 'FAIL'),
        coalesce(count_if(array_size(TOP_EVENTS) > 3), 0)::string,
        '0 rows with >3 top events'
    from MIP.NEWS.NEWS_FEATURES_SNAPSHOT

    union all

    select
        'RESOLVER_VIEW_PRESENT',
        iff(count(*) >= 1, 'PASS', 'FAIL'),
        count(*)::string,
        '>=1 rows queryable from MART.V_NEWS_FEATURES_BY_TS'
    from MIP.MART.V_NEWS_FEATURES_BY_TS
)
select * from checks
order by check_name;
