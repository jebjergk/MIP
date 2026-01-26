-- entry_gate_blocked_smoke.sql
-- Purpose: Smoke test for entry gate enforcement (exits-only mode)

use role MIP_ADMIN_ROLE;
use database MIP;

set run_id = to_number(to_varchar(current_timestamp(), 'YYYYMMDDHH24MISS'));
set run_id_string = to_varchar($run_id);
set test_portfolio_name = 'SMOKE_ENTRY_GATE_' || $run_id_string;

set profile_id = (
    select coalesce(
        (select PROFILE_ID from MIP.APP.PORTFOLIO_PROFILE where NAME = 'PRIVATE_SAVINGS' limit 1),
        (select min(PROFILE_ID) from MIP.APP.PORTFOLIO_PROFILE)
    )
);

insert into MIP.APP.PORTFOLIO (
    PROFILE_ID,
    NAME,
    BASE_CURRENCY,
    STARTING_CASH
)
select
    $profile_id,
    $test_portfolio_name,
    'USD',
    100000;

set portfolio_id = (
    select PORTFOLIO_ID
    from MIP.APP.PORTFOLIO
    where NAME = $test_portfolio_name
);

set pattern_id = (select min(PATTERN_ID) from MIP.APP.PATTERN_DEFINITION);
set signal_ts = (select max(TS) from MIP.MART.MARKET_BARS where INTERVAL_MINUTES = 1440);
set signal_symbol = (
    select SYMBOL
    from MIP.MART.MARKET_BARS
    where TS = $signal_ts
      and INTERVAL_MINUTES = 1440
    limit 1
);
set signal_market_type = (
    select MARKET_TYPE
    from MIP.MART.MARKET_BARS
    where TS = $signal_ts
      and INTERVAL_MINUTES = 1440
      and SYMBOL = $signal_symbol
    limit 1
);

insert into MIP.APP.RECOMMENDATION_LOG (
    PATTERN_ID,
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    TS,
    GENERATED_AT,
    SCORE,
    DETAILS
)
select
    $pattern_id,
    $signal_symbol,
    $signal_market_type,
    1440,
    $signal_ts,
    current_timestamp(),
    0.95,
    object_construct(
        'run_id', $run_id_string,
        'source', 'SMOKE_ENTRY_GATE_TEST'
    );

set recommendation_id = (
    select max(RECOMMENDATION_ID)
    from MIP.APP.RECOMMENDATION_LOG
    where DETAILS:run_id::string = $run_id_string
);

select
    PORTFOLIO_ID,
    RUN_ID,
    ENTRIES_BLOCKED,
    ALLOWED_ACTIONS,
    STOP_REASON
from MIP.MART.V_PORTFOLIO_RISK_STATE
where PORTFOLIO_ID = $portfolio_id;

call MIP.APP.SP_AGENT_PROPOSE_TRADES($portfolio_id, $run_id);

select
    'ENTRY_GATE_BUY_PROPOSALS' as test_name,
    count(*) as buy_proposals_inserted
from MIP.AGENT_OUT.ORDER_PROPOSALS
where RUN_ID = $run_id
  and PORTFOLIO_ID = $portfolio_id
  and SIDE = 'BUY';

insert into MIP.AGENT_OUT.ORDER_PROPOSALS (
    RUN_ID,
    PORTFOLIO_ID,
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    SIDE,
    TARGET_WEIGHT,
    RECOMMENDATION_ID,
    SIGNAL_TS,
    SIGNAL_PATTERN_ID,
    SIGNAL_INTERVAL_MINUTES,
    SIGNAL_RUN_ID,
    SOURCE_SIGNALS,
    RATIONALE
)
select
    $run_id,
    $portfolio_id,
    $signal_symbol,
    $signal_market_type,
    1440,
    'BUY',
    0.05,
    $recommendation_id,
    $signal_ts,
    $pattern_id,
    1440,
    $run_id_string,
    object_construct('score', 0.95),
    object_construct('source', 'SMOKE_ENTRY_GATE_TEST');

call MIP.APP.SP_VALIDATE_AND_EXECUTE_PROPOSALS($portfolio_id, $run_id);

select
    'ENTRY_GATE_BUY_EXECUTIONS' as test_name,
    count(*) as buy_executions
from MIP.APP.PORTFOLIO_TRADES
where RUN_ID = $run_id
  and PORTFOLIO_ID = $portfolio_id
  and SIDE = 'BUY';

select
    'ENTRY_GATE_BUY_STATUS' as test_name,
    count_if(STATUS = 'REJECTED') as rejected_count,
    count_if(STATUS = 'EXECUTED') as executed_count
from MIP.AGENT_OUT.ORDER_PROPOSALS
where RUN_ID = $run_id
  and PORTFOLIO_ID = $portfolio_id
  and SIDE = 'BUY';

delete from MIP.APP.PORTFOLIO_TRADES
where RUN_ID = $run_id
  and PORTFOLIO_ID = $portfolio_id;

delete from MIP.AGENT_OUT.ORDER_PROPOSALS
where RUN_ID = $run_id
  and PORTFOLIO_ID = $portfolio_id;

delete from MIP.APP.RECOMMENDATION_LOG
where DETAILS:run_id::string = $run_id_string;

delete from MIP.APP.PORTFOLIO
where PORTFOLIO_ID = $portfolio_id;
