-- Smoke test: portfolio trade idempotency
-- Replace the bind values before running.

set portfolio_id = 0;
set from_ts = '2024-01-01'::timestamp_ntz;
set to_ts = '2024-03-31'::timestamp_ntz;

-- Baseline trade count
set trade_count_before = (
    select count(*)
    from MIP.APP.PORTFOLIO_TRADES
    where PORTFOLIO_ID = $portfolio_id
      and TRADE_TS between $from_ts and $to_ts
);

call MIP.APP.SP_RUN_PORTFOLIO_SIMULATION($portfolio_id, $from_ts, $to_ts);

set trade_count_after_first = (
    select count(*)
    from MIP.APP.PORTFOLIO_TRADES
    where PORTFOLIO_ID = $portfolio_id
      and TRADE_TS between $from_ts and $to_ts
);

call MIP.APP.SP_RUN_PORTFOLIO_SIMULATION($portfolio_id, $from_ts, $to_ts);

set trade_count_after_second = (
    select count(*)
    from MIP.APP.PORTFOLIO_TRADES
    where PORTFOLIO_ID = $portfolio_id
      and TRADE_TS between $from_ts and $to_ts
);

select
    $trade_count_before as trade_count_before,
    $trade_count_after_first as trade_count_after_first,
    $trade_count_after_second as trade_count_after_second,
    ($trade_count_after_second - $trade_count_after_first) as delta_second_run_should_be_zero;

-- Proposal era duplicates: (portfolio_id, proposal_id)
select
    PORTFOLIO_ID,
    PROPOSAL_ID,
    count(*) as trade_rows
from MIP.APP.PORTFOLIO_TRADES
where PROPOSAL_ID is not null
group by PORTFOLIO_ID, PROPOSAL_ID
having count(*) > 1
order by trade_rows desc, PORTFOLIO_ID, PROPOSAL_ID;

-- Legacy duplicates: (portfolio_id, trade_day, symbol, side, price, quantity)
with legacy_dupes as (
    select
        PORTFOLIO_ID,
        date_trunc('day', TRADE_TS) as TRADE_DAY,
        SYMBOL,
        SIDE,
        PRICE,
        QUANTITY,
        count(*) as trade_rows
    from MIP.APP.PORTFOLIO_TRADES
    where PROPOSAL_ID is null
    group by PORTFOLIO_ID, date_trunc('day', TRADE_TS), SYMBOL, SIDE, PRICE, QUANTITY
    having count(*) > 1
)
select *
from legacy_dupes
order by trade_rows desc, PORTFOLIO_ID, TRADE_DAY, SYMBOL;
