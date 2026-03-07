use role MIP_ADMIN_ROLE;
use database MIP;

-- 12_learning_decision_ledger_smoke.sql
-- Purpose: verify canonical Learning-to-Decision ledger objects and append path.

-- 1) Object existence
show tables like 'LEARNING_DECISION_LEDGER' in schema MIP.AGENT_OUT;
show procedures like 'SP_LEDGER_APPEND_EVENT' in schema MIP.APP;

-- 2) Append a smoke event
call MIP.APP.SP_LEDGER_APPEND_EVENT(
    'TRAINING_EVENT',
    'SMOKE_LEDGER_EVENT',
    'SUCCESS',
    'SMOKE_RUN',
    null,
    null,
    null,
    null,
    null,
    'SMOKE',
    'STOCK',
    'SMOKE_TRAINING_V1',
    'SMOKE_POLICY_V1',
    object_construct('smoke', true),
    object_construct('trusted_before', 10),
    object_construct('trusted_after', 11),
    object_construct('trusted_delta', 1),
    object_construct('source', 'smoke_test'),
    object_construct('note', 'smoke'),
    null
);

-- 3) Verify inserted row
select
    LEDGER_ID,
    EVENT_TS,
    EVENT_TYPE,
    EVENT_NAME,
    STATUS,
    RUN_ID,
    SYMBOL,
    MARKET_TYPE,
    TRAINING_VERSION,
    POLICY_VERSION
from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
where RUN_ID = 'SMOKE_RUN'
order by LEDGER_ID desc
limit 5;

