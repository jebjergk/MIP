-- 245_news_phased_checks.sql
-- Purpose: Phase D checks for outcome enrichment + trained multipliers + apply logs.

use role MIP_ADMIN_ROLE;
use database MIP;

with checks as (
    select
        'OUTCOMES_WITH_NEWS_BUCKETS_PRESENT' as check_name,
        iff(coalesce(count_if(NEWS_PRESSURE_BUCKET is not null), 0) > 0, 'PASS', 'FAIL') as status,
        coalesce(count_if(NEWS_PRESSURE_BUCKET is not null), 0)::string as observed,
        '> 0 outcome rows enriched with news buckets' as expected
    from MIP.APP.RECOMMENDATION_OUTCOMES

    union all

    select
        'TRAINED_ROWS_PRESENT',
        iff(count(*) > 0, 'PASS', 'FAIL'),
        count(*)::string,
        '> 0 rows in DAILY_NEWS_CALIBRATION_TRAINED'
    from MIP.APP.DAILY_NEWS_CALIBRATION_TRAINED

    union all

    select
        'MULTIPLIER_BOUNDED',
        iff(
            coalesce(count_if(MULTIPLIER_CAPPED < 0.5 or MULTIPLIER_CAPPED > 1.5), 0) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(count_if(MULTIPLIER_CAPPED < 0.5 or MULTIPLIER_CAPPED > 1.5), 0)::string,
        '0 trained rows outside sanity multiplier bounds [0.5,1.5]'
    from MIP.APP.DAILY_NEWS_CALIBRATION_TRAINED

    union all

    select
        'APPLY_LOG_ROWS_PRESENT',
        iff(count(*) > 0, 'PASS', 'FAIL'),
        count(*)::string,
        '> 0 rows in DAILY_NEWS_CALIBRATION_APPLY_LOG'
    from MIP.APP.DAILY_NEWS_CALIBRATION_APPLY_LOG

    union all

    select
        'PROPOSALS_HAVE_CALIBRATION_FIELDS',
        iff(
            coalesce(
                count_if(
                    array_contains('news_calibration_multiplier'::variant, object_keys(SOURCE_SIGNALS))
                    and array_contains('news_score_adj_calibrated'::variant, object_keys(SOURCE_SIGNALS))
                ),
                0
            ) > 0,
            'PASS',
            'FAIL'
        ),
        coalesce(
            count_if(
                array_contains('news_calibration_multiplier'::variant, object_keys(SOURCE_SIGNALS))
                and array_contains('news_score_adj_calibrated'::variant, object_keys(SOURCE_SIGNALS))
            ),
            0
        )::string,
        '> 0 proposed rows carry calibration fields'
    from MIP.AGENT_OUT.ORDER_PROPOSALS
    where STATUS = 'PROPOSED'
)
select * from checks
order by check_name;
