-- seed_news_mapping_qa_sample.sql
-- Purpose: Seed a deterministic QA sample for Phase 3 precision/coverage checks.
-- Notes:
--   - Uses current mock/news rows already present in NEWS_RAW.
--   - Precision-first sample: mostly relevant positives with one negative control.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.NEWS.NEWS_MAPPING_QA_SAMPLE t
using (
    -- Positive expectations (should map)
    select NEWS_ID, 'AAPL' as EXPECTED_SYMBOL, 'STOCK' as EXPECTED_MARKET_TYPE, true as IS_RELEVANT,
           'seeded deterministic qa sample' as NOTES
    from MIP.NEWS.NEWS_RAW
    where TITLE ilike 'Apple announces update%'

    union all
    select NEWS_ID, 'MSFT', 'STOCK', true, 'seeded deterministic qa sample'
    from MIP.NEWS.NEWS_RAW
    where TITLE ilike 'Apple announces update%'

    union all
    select NEWS_ID, 'NVDA', 'STOCK', true, 'seeded deterministic qa sample'
    from MIP.NEWS.NEWS_RAW
    where TITLE ilike 'NVIDIA and Tesla supplier note%'

    union all
    select NEWS_ID, 'TSLA', 'STOCK', true, 'seeded deterministic qa sample'
    from MIP.NEWS.NEWS_RAW
    where TITLE ilike 'NVIDIA and Tesla supplier note%'

    union all
    select NEWS_ID, 'SPY', 'ETF', true, 'seeded deterministic qa sample'
    from MIP.NEWS.NEWS_RAW
    where TITLE ilike '%market pulse for SPY and QQQ%'

    union all
    select NEWS_ID, 'QQQ', 'ETF', true, 'seeded deterministic qa sample'
    from MIP.NEWS.NEWS_RAW
    where TITLE ilike '%market pulse for SPY and QQQ%'

    union all
    select NEWS_ID, 'EURUSD', 'FX', true, 'seeded deterministic qa sample'
    from MIP.NEWS.NEWS_RAW
    where TITLE ilike 'Federal Reserve note: USD/JPY volatility rises%'

    union all
    select NEWS_ID, 'USDJPY', 'FX', true, 'seeded deterministic qa sample'
    from MIP.NEWS.NEWS_RAW
    where TITLE ilike 'Federal Reserve note: USD/JPY volatility rises%'

    union all
    select NEWS_ID, 'USDCAD', 'FX', true, 'seeded deterministic qa sample'
    from MIP.NEWS.NEWS_RAW
    where TITLE ilike 'Fed policy remarks keep USD/CAD and USD/CHF active%'

    union all
    select NEWS_ID, 'USDCHF', 'FX', true, 'seeded deterministic qa sample'
    from MIP.NEWS.NEWS_RAW
    where TITLE ilike 'Fed policy remarks keep USD/CAD and USD/CHF active%'

    union all
    -- Negative control (should not map for this symbol)
    select NEWS_ID, 'XOM', 'STOCK', false, 'negative control for precision'
    from MIP.NEWS.NEWS_RAW
    where TITLE ilike '%market pulse for SPY and QQQ%'
) s
on t.NEWS_ID = s.NEWS_ID
and upper(t.EXPECTED_SYMBOL) = upper(s.EXPECTED_SYMBOL)
and upper(t.EXPECTED_MARKET_TYPE) = upper(s.EXPECTED_MARKET_TYPE)
when matched then update set
    t.IS_RELEVANT = s.IS_RELEVANT,
    t.NOTES = s.NOTES,
    t.REVIEWED_BY = 'CURSOR_AGENT',
    t.REVIEWED_AT = current_timestamp()
when not matched then insert (
    NEWS_ID, EXPECTED_SYMBOL, EXPECTED_MARKET_TYPE, IS_RELEVANT, NOTES, CREATED_AT
) values (
    s.NEWS_ID, s.EXPECTED_SYMBOL, s.EXPECTED_MARKET_TYPE, s.IS_RELEVANT, s.NOTES, current_timestamp()
);

select
    count(*) as qa_rows,
    count_if(IS_RELEVANT) as relevant_rows,
    count_if(not IS_RELEVANT) as negative_rows
from MIP.NEWS.NEWS_MAPPING_QA_SAMPLE;
