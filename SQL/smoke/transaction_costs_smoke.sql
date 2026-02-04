-- Smoke test: deterministic trade costs (slippage, fee, optional spread)
-- Replace bind values if needed before running.

use role MIP_ADMIN_ROLE;
use database MIP;

set test_symbol = 'TEST_COSTS';
set test_portfolio_name = 'TEST_COSTS_PORTFOLIO';
set test_start_date = '2099-01-01'::date;
set test_end_date = '2099-01-02'::date;
set test_start_ts = '2099-01-01'::timestamp_ntz;
set test_end_ts = '2099-01-02'::timestamp_ntz;
set test_price = 100::number(18,8);
set test_cash = 1000::number(18,2);
set test_qty = 10::number(18,8);

-- Preserve current config
set prev_slippage_bps = (select CONFIG_VALUE from MIP.APP.APP_CONFIG where CONFIG_KEY = 'SLIPPAGE_BPS');
set prev_fee_bps = (select CONFIG_VALUE from MIP.APP.APP_CONFIG where CONFIG_KEY = 'FEE_BPS');
set prev_min_fee = (select CONFIG_VALUE from MIP.APP.APP_CONFIG where CONFIG_KEY = 'MIN_FEE');
set prev_spread_bps = (select CONFIG_VALUE from MIP.APP.APP_CONFIG where CONFIG_KEY = 'SPREAD_BPS');

-- Deterministic config values
update MIP.APP.APP_CONFIG set CONFIG_VALUE = '2' where CONFIG_KEY = 'SLIPPAGE_BPS';
update MIP.APP.APP_CONFIG set CONFIG_VALUE = '1' where CONFIG_KEY = 'FEE_BPS';
update MIP.APP.APP_CONFIG set CONFIG_VALUE = '0' where CONFIG_KEY = 'MIN_FEE';
update MIP.APP.APP_CONFIG set CONFIG_VALUE = '4' where CONFIG_KEY = 'SPREAD_BPS';

-- Cleanup prior test artifacts
delete from MIP.APP.PORTFOLIO_TRADES where SYMBOL = $test_symbol;
delete from MIP.APP.PORTFOLIO_POSITIONS where SYMBOL = $test_symbol;
delete from MIP.APP.PORTFOLIO_DAILY where PORTFOLIO_ID in (
    select PORTFOLIO_ID from MIP.APP.PORTFOLIO where NAME = $test_portfolio_name
);
delete from MIP.APP.RECOMMENDATION_LOG where SYMBOL = $test_symbol;
delete from MIP.MART.MARKET_BARS where SYMBOL = $test_symbol;
delete from MIP.APP.PORTFOLIO where NAME = $test_portfolio_name;

-- Create a test portfolio
insert into MIP.APP.PORTFOLIO (
    PROFILE_ID,
    NAME,
    BASE_CURRENCY,
    STARTING_CASH
)
select
    PROFILE_ID,
    $test_portfolio_name,
    'USD',
    $test_cash
from MIP.APP.PORTFOLIO_PROFILE
qualify row_number() over (order by PROFILE_ID) = 1;

set test_portfolio_id = (
    select PORTFOLIO_ID
    from MIP.APP.PORTFOLIO
    where NAME = $test_portfolio_name
    order by CREATED_AT desc
    limit 1
);

-- Seed two bars for the symbol (entry + exit)
insert into MIP.MART.MARKET_BARS (
    TS,
    SYMBOL,
    SOURCE,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME,
    INGESTED_AT
)
select $test_start_ts, $test_symbol, 'TEST', 'STOCK', 1440, $test_price, $test_price, $test_price, $test_price, 1000, current_timestamp()
union all
select $test_end_ts, $test_symbol, 'TEST', 'STOCK', 1440, $test_price, $test_price, $test_price, $test_price, 1000, current_timestamp();

-- Seed a single recommendation to trigger a buy
insert into MIP.APP.RECOMMENDATION_LOG (
    PATTERN_ID,
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    TS,
    SCORE,
    DETAILS
)
select
    (select min(PATTERN_ID) from MIP.APP.PATTERN_DEFINITION),
    $test_symbol,
    'STOCK',
    1440,
    $test_start_ts,
    0.9,
    object_construct('test', 'transaction_costs');

-- Run the simulation: 1-day hold, single position, full allocation
call MIP.APP.SP_SIMULATE_PORTFOLIO(
    $test_portfolio_id,
    $test_start_date,
    $test_end_date,
    1,
    1,
    1.0,
    0.0,
    'STOCK',
    true,
    null
);

-- Verify fee/slippage impact against deterministic expectations
with cfg as (
    select
        coalesce(try_to_number(max(case when CONFIG_KEY = 'SLIPPAGE_BPS' then CONFIG_VALUE end)), 2) as SLIPPAGE_BPS,
        coalesce(try_to_number(max(case when CONFIG_KEY = 'FEE_BPS' then CONFIG_VALUE end)), 1) as FEE_BPS,
        coalesce(try_to_number(max(case when CONFIG_KEY = 'MIN_FEE' then CONFIG_VALUE end)), 0) as MIN_FEE,
        coalesce(try_to_number(max(case when CONFIG_KEY = 'SPREAD_BPS' then CONFIG_VALUE end)), 0) as SPREAD_BPS
    from MIP.APP.APP_CONFIG
),
calc as (
    select
        $test_price as MID_PRICE,
        $test_qty as QTY,
        $test_cash as STARTING_CASH,
        SLIPPAGE_BPS,
        FEE_BPS,
        MIN_FEE,
        SPREAD_BPS
    from cfg
),
expected as (
    select
        MID_PRICE,
        QTY,
        STARTING_CASH,
        MID_PRICE * (1 + ((SLIPPAGE_BPS + (SPREAD_BPS / 2)) / 10000)) as BUY_EXEC_PRICE,
        MID_PRICE * (1 - ((SLIPPAGE_BPS + (SPREAD_BPS / 2)) / 10000)) as SELL_EXEC_PRICE,
        (MID_PRICE * (1 + ((SLIPPAGE_BPS + (SPREAD_BPS / 2)) / 10000))) * QTY as BUY_NOTIONAL,
        (MID_PRICE * (1 - ((SLIPPAGE_BPS + (SPREAD_BPS / 2)) / 10000))) * QTY as SELL_NOTIONAL,
        greatest(MIN_FEE, abs((MID_PRICE * (1 + ((SLIPPAGE_BPS + (SPREAD_BPS / 2)) / 10000))) * QTY) * FEE_BPS / 10000) as BUY_FEE,
        greatest(MIN_FEE, abs((MID_PRICE * (1 - ((SLIPPAGE_BPS + (SPREAD_BPS / 2)) / 10000))) * QTY) * FEE_BPS / 10000) as SELL_FEE
    from calc
),
expected_cash as (
    select
        BUY_EXEC_PRICE,
        SELL_EXEC_PRICE,
        BUY_NOTIONAL,
        SELL_NOTIONAL,
        BUY_FEE,
        SELL_FEE,
        STARTING_CASH - (BUY_NOTIONAL + BUY_FEE) as EXPECTED_CASH_AFTER_BUY,
        STARTING_CASH - (BUY_NOTIONAL + BUY_FEE) + (SELL_NOTIONAL - SELL_FEE) as EXPECTED_CASH_AFTER_SELL,
        (SELL_NOTIONAL - SELL_FEE) - (BUY_NOTIONAL + BUY_FEE) as EXPECTED_REALIZED_PNL
    from expected
),
trade_rows as (
    select
        t.*,
        row_number() over (order by t.TRADE_TS, t.TRADE_ID) as RN
    from MIP.APP.PORTFOLIO_TRADES t
    where t.PORTFOLIO_ID = $test_portfolio_id
      and t.SYMBOL = $test_symbol
)
select
    t.SIDE,
    t.PRICE as ACTUAL_PRICE,
    t.QUANTITY as ACTUAL_QTY,
    t.NOTIONAL as ACTUAL_NOTIONAL,
    t.CASH_AFTER as ACTUAL_CASH_AFTER,
    t.REALIZED_PNL as ACTUAL_REALIZED_PNL,
    case when t.SIDE = 'BUY' then e.BUY_EXEC_PRICE else e.SELL_EXEC_PRICE end as EXPECTED_PRICE,
    e.BUY_NOTIONAL as EXPECTED_BUY_NOTIONAL,
    e.SELL_NOTIONAL as EXPECTED_SELL_NOTIONAL,
    case when t.SIDE = 'BUY' then e.EXPECTED_CASH_AFTER_BUY else e.EXPECTED_CASH_AFTER_SELL end as EXPECTED_CASH_AFTER,
    e.EXPECTED_REALIZED_PNL as EXPECTED_REALIZED_PNL,
    case
        when t.SIDE = 'BUY' then abs(t.CASH_AFTER - e.EXPECTED_CASH_AFTER_BUY)
        else abs(t.CASH_AFTER - e.EXPECTED_CASH_AFTER_SELL)
    end as CASH_AFTER_DIFF,
    abs(coalesce(t.REALIZED_PNL, 0) - e.EXPECTED_REALIZED_PNL) as REALIZED_PNL_DIFF
from trade_rows t
cross join expected_cash e
order by t.TRADE_TS;

-- Cleanup test artifacts
delete from MIP.APP.PORTFOLIO_TRADES where PORTFOLIO_ID = $test_portfolio_id;
delete from MIP.APP.PORTFOLIO_POSITIONS where PORTFOLIO_ID = $test_portfolio_id;
delete from MIP.APP.PORTFOLIO_DAILY where PORTFOLIO_ID = $test_portfolio_id;
delete from MIP.APP.PORTFOLIO where PORTFOLIO_ID = $test_portfolio_id;
delete from MIP.APP.RECOMMENDATION_LOG where SYMBOL = $test_symbol;
delete from MIP.MART.MARKET_BARS where SYMBOL = $test_symbol;

-- Restore previous config
update MIP.APP.APP_CONFIG set CONFIG_VALUE = $prev_slippage_bps where CONFIG_KEY = 'SLIPPAGE_BPS';
update MIP.APP.APP_CONFIG set CONFIG_VALUE = $prev_fee_bps where CONFIG_KEY = 'FEE_BPS';
update MIP.APP.APP_CONFIG set CONFIG_VALUE = $prev_min_fee where CONFIG_KEY = 'MIN_FEE';
update MIP.APP.APP_CONFIG set CONFIG_VALUE = $prev_spread_bps where CONFIG_KEY = 'SPREAD_BPS';
