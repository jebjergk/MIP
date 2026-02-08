-- training_digest_smoke.sql
-- Purpose: Smoke tests for Training Journey Digest tables, views, and procedure.
-- Run after deploying 210/211/212 SQL files and running the pipeline at least once.

use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- 1. Table existence
-- =============================================================================

select 'TRAINING_SNAPSHOT_TABLE_EXISTS' as TEST,
       iff(count(*) = 1, 'PASS', 'FAIL') as RESULT
from MIP.INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'AGENT_OUT'
  and TABLE_NAME = 'TRAINING_DIGEST_SNAPSHOT';

select 'TRAINING_NARRATIVE_TABLE_EXISTS' as TEST,
       iff(count(*) = 1, 'PASS', 'FAIL') as RESULT
from MIP.INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'AGENT_OUT'
  and TABLE_NAME = 'TRAINING_DIGEST_NARRATIVE';

-- =============================================================================
-- 2. Views compile
-- =============================================================================

select 'GLOBAL_TRAINING_VIEW_COMPILES' as TEST,
       iff(count(*) >= 0, 'PASS', 'FAIL') as RESULT
from MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_GLOBAL
where 1 = 0;

select 'SYMBOL_TRAINING_VIEW_COMPILES' as TEST,
       iff(count(*) >= 0, 'PASS', 'FAIL') as RESULT
from MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL
where 1 = 0;

-- =============================================================================
-- 3. Procedure exists
-- =============================================================================

select 'TRAINING_DIGEST_PROCEDURE_EXISTS' as TEST,
       iff(count(*) >= 1, 'PASS', 'FAIL') as RESULT
from MIP.INFORMATION_SCHEMA.PROCEDURES
where PROCEDURE_SCHEMA = 'APP'
  and PROCEDURE_NAME = 'SP_AGENT_GENERATE_TRAINING_DIGEST';

-- =============================================================================
-- 4. Global training snapshot exists (after first run)
-- =============================================================================

select 'GLOBAL_TRAINING_SNAPSHOT_EXISTS' as TEST,
       iff(count(*) >= 1, 'PASS', 'PENDING') as RESULT,
       count(*) as ROW_COUNT
from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
where SCOPE = 'GLOBAL_TRAINING'
  and SYMBOL is null;

-- =============================================================================
-- 5. Global training narrative exists (after first run)
-- =============================================================================

select 'GLOBAL_TRAINING_NARRATIVE_EXISTS' as TEST,
       iff(count(*) >= 1, 'PASS', 'PENDING') as RESULT,
       count(*) as ROW_COUNT
from MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE
where SCOPE = 'GLOBAL_TRAINING'
  and SYMBOL is null;

-- =============================================================================
-- 6. At least one symbol training snapshot exists (after first run)
-- =============================================================================

select 'SYMBOL_TRAINING_SNAPSHOTS_EXIST' as TEST,
       iff(count(*) >= 1, 'PASS', 'PENDING') as RESULT,
       count(*) as ROW_COUNT,
       count(distinct SYMBOL) as DISTINCT_SYMBOLS
from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
where SCOPE = 'SYMBOL_TRAINING'
  and SYMBOL is not null;

-- =============================================================================
-- 7. CREATED_AT not null
-- =============================================================================

select 'TRAINING_SNAPSHOT_CREATED_AT_NOT_NULL' as TEST,
       iff(count(*) = 0, 'PASS', 'FAIL') as RESULT,
       count(*) as NULL_COUNT
from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
where CREATED_AT is null;

select 'TRAINING_NARRATIVE_CREATED_AT_NOT_NULL' as TEST,
       iff(count(*) = 0, 'PASS', 'FAIL') as RESULT,
       count(*) as NULL_COUNT
from MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE
where CREATED_AT is null;

-- =============================================================================
-- 8. SOURCE_FACTS_HASH populated
-- =============================================================================

select 'TRAINING_SNAPSHOT_HASH_POPULATED' as TEST,
       iff(count_if(SOURCE_FACTS_HASH is null) = 0 or count(*) = 0, 'PASS', 'FAIL') as RESULT,
       count(*) as TOTAL_ROWS,
       count_if(SOURCE_FACTS_HASH is null) as NULL_HASH_COUNT
from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT;

-- =============================================================================
-- 9. Idempotency
-- =============================================================================

select 'TRAINING_SNAPSHOT_IDEMPOTENCY' as TEST,
       iff(count(*) = 0, 'PASS', 'FAIL') as RESULT
from (
    select SCOPE, SYMBOL, MARKET_TYPE, AS_OF_TS, RUN_ID, count(*) as cnt
    from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
    group by 1, 2, 3, 4, 5
    having cnt > 1
);

select 'TRAINING_NARRATIVE_IDEMPOTENCY' as TEST,
       iff(count(*) = 0, 'PASS', 'FAIL') as RESULT
from (
    select SCOPE, SYMBOL, MARKET_TYPE, AS_OF_TS, RUN_ID, AGENT_NAME, count(*) as cnt
    from MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE
    group by 1, 2, 3, 4, 5, 6
    having cnt > 1
);

-- =============================================================================
-- 10. Global narrative has required JSON keys
-- =============================================================================

select 'GLOBAL_TRAINING_NARRATIVE_JSON_STRUCTURE' as TEST,
       case
           when count(*) = 0 then 'PENDING'
           when max(iff(
               n.NARRATIVE_JSON:headline is not null
               and n.NARRATIVE_JSON:what_changed is not null
               and n.NARRATIVE_JSON:what_matters is not null
               and n.NARRATIVE_JSON:waiting_for is not null,
               1, 0
           )) = 1 then 'PASS'
           else 'FAIL'
       end as RESULT,
       max(n.NARRATIVE_JSON:headline::string) as HEADLINE_PREVIEW
from (
    select *
    from MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE
    where SCOPE = 'GLOBAL_TRAINING' and SYMBOL is null
    qualify row_number() over (order by CREATED_AT desc) = 1
) n;

-- =============================================================================
-- 11. Global snapshot JSON has required top-level keys
-- =============================================================================

select 'GLOBAL_TRAINING_SNAPSHOT_JSON_STRUCTURE' as TEST,
       case
           when count(*) = 0 then 'PENDING'
           when max(iff(
               s.SNAPSHOT_JSON:stages is not null
               and s.SNAPSHOT_JSON:trust is not null
               and s.SNAPSHOT_JSON:thresholds is not null
               and s.SNAPSHOT_JSON:detectors is not null
               and s.SNAPSHOT_JSON:near_miss_symbols is not null,
               1, 0
           )) = 1 then 'PASS'
           else 'FAIL'
       end as RESULT
from (
    select *
    from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
    where SCOPE = 'GLOBAL_TRAINING' and SYMBOL is null
    qualify row_number() over (order by CREATED_AT desc) = 1
) s;

-- =============================================================================
-- 12. Hash grounding check — global
-- =============================================================================

with global_pair as (
    select s.SOURCE_FACTS_HASH as SNAPSHOT_HASH,
           n.SOURCE_FACTS_HASH as NARRATIVE_HASH
    from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT s
    join MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE n
        on  n.SCOPE = s.SCOPE
        and n.SYMBOL is null and s.SYMBOL is null
        and n.MARKET_TYPE is null and s.MARKET_TYPE is null
        and n.AS_OF_TS = s.AS_OF_TS
        and n.RUN_ID = s.RUN_ID
    where s.SCOPE = 'GLOBAL_TRAINING'
    qualify row_number() over (order by s.CREATED_AT desc) = 1
)
select 'TRAINING_HASH_GROUNDING_GLOBAL' as TEST,
       case
           when (select count(*) from global_pair) = 0 then 'PENDING'
           when (select count(*) from global_pair where SNAPSHOT_HASH = NARRATIVE_HASH) = 1 then 'PASS'
           else 'FAIL'
       end as RESULT;

-- =============================================================================
-- 13. Audit log has TRAINING_DIGEST entries
-- =============================================================================

select 'AUDIT_LOG_HAS_TRAINING_DIGEST' as TEST,
       iff(count(*) >= 1, 'PASS', 'PENDING') as RESULT,
       count(*) as TRAINING_DIGEST_ENTRIES
from MIP.APP.MIP_AUDIT_LOG
where EVENT_NAME = 'SP_AGENT_GENERATE_TRAINING_DIGEST';
