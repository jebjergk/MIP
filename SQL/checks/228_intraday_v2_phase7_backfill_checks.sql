-- 228_intraday_v2_phase7_backfill_checks.sql
-- Purpose: Phase 7 validation for chunked/resumable/idempotent backfill runs.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Latest chunk statuses for most recent run window.
select
    RUN_ID,
    CHUNK_ID,
    START_TS,
    END_TS,
    STATUS,
    ROWS_STATE_SNAPSHOT,
    ROWS_STATE_TRANSITIONS,
    ROWS_SIGNALS,
    ROWS_OUTCOMES,
    ROWS_TRUST,
    ROWS_TERRAIN,
    ERROR_MESSAGE
from MIP.APP.INTRA_BACKFILL_RUN_LOG
where CREATED_AT >= dateadd(hour, -6, current_timestamp())
order by CREATED_AT desc, CHUNK_ID desc
limit 200;

-- 2) Chunk status summary.
select
    STATUS,
    count(*) as CHUNKS
from MIP.APP.INTRA_BACKFILL_RUN_LOG
where CREATED_AT >= dateadd(hour, -6, current_timestamp())
group by 1
order by 2 desc;

-- 3) Duplicate guard checks in key target tables for latest 6h.
select
    'INTRA_SIGNALS' as TABLE_NAME,
    count(*) as DUP_ROWS
from (
    select SIGNAL_NK_HASH, count(*) as C
    from MIP.APP.INTRA_SIGNALS
    where GENERATED_AT >= dateadd(hour, -6, current_timestamp())
    group by 1
    having count(*) > 1
)
union all
select
    'INTRA_OUTCOMES',
    count(*)
from (
    select SIGNAL_ID, HORIZON_BARS, count(*) as C
    from MIP.APP.INTRA_OUTCOMES
    where CALCULATED_AT >= dateadd(hour, -6, current_timestamp())
    group by 1,2
    having count(*) > 1
)
union all
select
    'OPPORTUNITY_TERRAIN_15M',
    count(*)
from (
    select PATTERN_ID, MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS, HORIZON_BARS, STATE_BUCKET_ID, count(*) as C
    from MIP.APP.OPPORTUNITY_TERRAIN_15M
    where CALCULATED_AT >= dateadd(hour, -6, current_timestamp())
    group by 1,2,3,4,5,6,7
    having count(*) > 1
);
