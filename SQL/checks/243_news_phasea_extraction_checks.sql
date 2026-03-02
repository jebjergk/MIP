-- 243_news_phasea_extraction_checks.sql
-- Purpose: Phase A checks for extraction schema and deterministic bounds.

use role MIP_ADMIN_ROLE;
use database MIP;

with checks as (
    select
        'ROWS_PRESENT' as check_name,
        iff(count(*) > 0, 'PASS', 'FAIL') as status,
        count(*)::string as observed,
        '> 0 rows in NEWS_EVENT_EXTRACTED' as expected
    from MIP.NEWS.NEWS_EVENT_EXTRACTED

    union all

    select
        'CONFIDENCE_BOUNDED_0_1',
        iff(coalesce(count_if(CONFIDENCE < 0 or CONFIDENCE > 1), 0) = 0, 'PASS', 'FAIL'),
        coalesce(count_if(CONFIDENCE < 0 or CONFIDENCE > 1), 0)::string,
        '0 rows outside confidence bounds'
    from MIP.NEWS.NEWS_EVENT_EXTRACTED

    union all

    select
        'EVENT_RISK_BOUNDED_0_1',
        iff(coalesce(count_if(EVENT_RISK_SCORE < 0 or EVENT_RISK_SCORE > 1), 0) = 0, 'PASS', 'FAIL'),
        coalesce(count_if(EVENT_RISK_SCORE < 0 or EVENT_RISK_SCORE > 1), 0)::string,
        '0 rows outside event_risk_score bounds'
    from MIP.NEWS.NEWS_EVENT_EXTRACTED

    union all

    select
        'ENUM_CONTRACT_VALID',
        iff(
            coalesce(
                count_if(
                    EVENT_TYPE not in ('earnings', 'macro_policy', 'mna', 'regulatory_legal', 'product_business', 'other')
                    or DIRECTION not in ('positive', 'negative', 'neutral')
                    or IMPACT_HORIZON not in ('short', 'medium', 'long')
                    or RELEVANCE_SCOPE not in ('symbol', 'sector', 'macro')
                ),
                0
            ) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(
            count_if(
                EVENT_TYPE not in ('earnings', 'macro_policy', 'mna', 'regulatory_legal', 'product_business', 'other')
                or DIRECTION not in ('positive', 'negative', 'neutral')
                or IMPACT_HORIZON not in ('short', 'medium', 'long')
                or RELEVANCE_SCOPE not in ('symbol', 'sector', 'macro')
            ),
            0
        )::string,
        '0 rows outside enum contract'
    from MIP.NEWS.NEWS_EVENT_EXTRACTED

    union all

    select
        'HASH_FIELDS_PRESENT',
        iff(
            coalesce(count_if(INPUT_HASH is null or OUTPUT_HASH is null or EXTRACT_ID is null), 0) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(count_if(INPUT_HASH is null or OUTPUT_HASH is null or EXTRACT_ID is null), 0)::string,
        '0 rows missing extraction hashes'
    from MIP.NEWS.NEWS_EVENT_EXTRACTED
)
select * from checks
order by check_name;
