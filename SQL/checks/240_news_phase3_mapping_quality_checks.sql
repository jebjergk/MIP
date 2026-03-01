-- 240_news_phase3_mapping_quality_checks.sql
-- Purpose: Phase 3 mapping checks + precision-first QA report.
-- Precision target (v1):
--   - precision >= 0.85
--   - precision >= coverage (precision favored over coverage in v1)

use role MIP_ADMIN_ROLE;
use database MIP;

set before_map_cnt = (select count(*) from MIP.NEWS.NEWS_SYMBOL_MAP);
call MIP.NEWS.SP_INGEST_RSS_NEWS(true, 3);
call MIP.NEWS.SP_MAP_NEWS_SYMBOLS(null);
set after_map_cnt = (select count(*) from MIP.NEWS.NEWS_SYMBOL_MAP);
set min_conf = (
    select coalesce(max(try_to_double(CONFIG_VALUE)), 0.70)
    from MIP.APP.APP_CONFIG
    where CONFIG_KEY = 'NEWS_MATCH_CONFIDENCE_MIN'
);

with base_checks as (
    select
        'MAPPING_ROWS_PRESENT' as check_name,
        iff($after_map_cnt > 0, 'PASS', 'FAIL') as status,
        $after_map_cnt::string as observed,
        '> 0 mapping rows' as expected

    union all

    select
        'MAPPING_INCREMENT_NON_NEGATIVE',
        iff(($after_map_cnt - $before_map_cnt) >= 0, 'PASS', 'FAIL'),
        ($after_map_cnt - $before_map_cnt)::string,
        '>= 0'

    union all

    select
        'CONFIDENCE_THRESHOLD_RESPECTED',
        iff(coalesce(count_if(MATCH_CONFIDENCE < $min_conf), 0) = 0, 'PASS', 'FAIL'),
        coalesce(count_if(MATCH_CONFIDENCE < $min_conf), 0)::string,
        '0 rows below NEWS_MATCH_CONFIDENCE_MIN'
    from MIP.NEWS.NEWS_SYMBOL_MAP

    union all

    select
        'MATCH_METHOD_ENUM_VALID',
        iff(
            coalesce(count_if(upper(MATCH_METHOD) not in ('TICKER_REGEX', 'ALIAS_DICT', 'COMPANY_NAME_MATCH')), 0) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(count_if(upper(MATCH_METHOD) not in ('TICKER_REGEX', 'ALIAS_DICT', 'COMPANY_NAME_MATCH')), 0)::string,
        '0 rows with invalid match_method'
    from MIP.NEWS.NEWS_SYMBOL_MAP
),
qa_calc as (
    select
        count(*) as qa_rows,
        count_if(qa.IS_RELEVANT and m.NEWS_ID is not null) as tp,
        count_if(not qa.IS_RELEVANT and m.NEWS_ID is not null) as fp,
        count_if(qa.IS_RELEVANT and m.NEWS_ID is null) as fn
    from MIP.NEWS.NEWS_MAPPING_QA_SAMPLE qa
    left join MIP.NEWS.NEWS_SYMBOL_MAP m
      on m.NEWS_ID = qa.NEWS_ID
     and m.SYMBOL = upper(qa.EXPECTED_SYMBOL)
     and m.MARKET_TYPE = upper(qa.EXPECTED_MARKET_TYPE)
),
qa_report as (
    select
        'QA_PRECISION_TARGET_DEFINITION' as check_name,
        'PASS' as status,
        'precision>=0.85 and precision>=coverage' as observed,
        'defined' as expected

    union all

    select
        'QA_SAMPLE_PRECISION',
        case
            when qa_rows = 0 then 'PENDING'
            when (tp + fp) = 0 then 'FAIL'
            when (tp::float / nullif(tp + fp, 0)) >= 0.85
                 and (tp::float / nullif(tp + fp, 0)) >= (tp::float / nullif(tp + fn, 0))
                then 'PASS'
            else 'FAIL'
        end as status,
        case
            when qa_rows = 0 then 'no QA sample rows'
            else concat(
                'precision=',
                to_varchar(round(tp::float / nullif(tp + fp, 0), 4)),
                ', coverage=',
                to_varchar(round(tp::float / nullif(tp + fn, 0), 4)),
                ', tp=', to_varchar(tp), ', fp=', to_varchar(fp), ', fn=', to_varchar(fn)
            )
        end as observed,
        'precision>=0.85 and precision>=coverage' as expected
    from qa_calc
)
select * from base_checks
union all
select * from qa_report
order by check_name;
