-- daily_digest_smoke.sql
-- Purpose: Smoke tests for Daily Intelligence Digest tables, view, and procedure.
-- Run after deploying 200/201/202 SQL files and running the pipeline at least once.

use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- 1. Table existence
-- =============================================================================

select 'SNAPSHOT_TABLE_EXISTS' as TEST,
       iff(count(*) = 1, 'PASS', 'FAIL') as RESULT
from MIP.INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'AGENT_OUT'
  and TABLE_NAME = 'DAILY_DIGEST_SNAPSHOT';

select 'NARRATIVE_TABLE_EXISTS' as TEST,
       iff(count(*) = 1, 'PASS', 'FAIL') as RESULT
from MIP.INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'AGENT_OUT'
  and TABLE_NAME = 'DAILY_DIGEST_NARRATIVE';

-- =============================================================================
-- 2. View compiles (select 0 rows)
-- =============================================================================

select 'SNAPSHOT_VIEW_COMPILES' as TEST,
       iff(count(*) >= 0, 'PASS', 'FAIL') as RESULT
from MIP.MART.V_DAILY_DIGEST_SNAPSHOT
where 1 = 0;

-- =============================================================================
-- 3. Procedure exists
-- =============================================================================

select 'PROCEDURE_EXISTS' as TEST,
       iff(count(*) >= 1, 'PASS', 'FAIL') as RESULT
from MIP.INFORMATION_SCHEMA.PROCEDURES
where PROCEDURE_SCHEMA = 'APP'
  and PROCEDURE_NAME = 'SP_AGENT_GENERATE_DAILY_DIGEST';

-- =============================================================================
-- 4. At least one snapshot per active portfolio (after first pipeline run)
-- =============================================================================

select 'SNAPSHOT_PER_PORTFOLIO' as TEST,
       p.PORTFOLIO_ID,
       iff(count(s.SNAPSHOT_ID) >= 1, 'PASS', 'PENDING') as RESULT
from MIP.APP.PORTFOLIO p
left join MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT s
    on s.PORTFOLIO_ID = p.PORTFOLIO_ID
where p.STATUS = 'ACTIVE'
group by p.PORTFOLIO_ID
order by p.PORTFOLIO_ID;

-- =============================================================================
-- 5. At least one narrative per active portfolio (after first pipeline run)
-- =============================================================================

select 'NARRATIVE_PER_PORTFOLIO' as TEST,
       p.PORTFOLIO_ID,
       iff(count(n.NARRATIVE_ID) >= 1, 'PASS', 'PENDING') as RESULT
from MIP.APP.PORTFOLIO p
left join MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE n
    on n.PORTFOLIO_ID = p.PORTFOLIO_ID
where p.STATUS = 'ACTIVE'
group by p.PORTFOLIO_ID
order by p.PORTFOLIO_ID;

-- =============================================================================
-- 6. CREATED_AT is never NULL (acceptance criteria)
-- =============================================================================

select 'SNAPSHOT_CREATED_AT_NOT_NULL' as TEST,
       iff(count(*) = 0, 'PASS', 'FAIL') as RESULT,
       count(*) as NULL_COUNT
from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
where CREATED_AT is null;

select 'NARRATIVE_CREATED_AT_NOT_NULL' as TEST,
       iff(count(*) = 0, 'PASS', 'FAIL') as RESULT,
       count(*) as NULL_COUNT
from MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE
where CREATED_AT is null;

-- =============================================================================
-- 7. SOURCE_FACTS_HASH is populated
-- =============================================================================

select 'SNAPSHOT_HASH_POPULATED' as TEST,
       iff(count_if(SOURCE_FACTS_HASH is null) = 0 or count(*) = 0, 'PASS', 'FAIL') as RESULT,
       count(*) as TOTAL_ROWS,
       count_if(SOURCE_FACTS_HASH is null) as NULL_HASH_COUNT
from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT;

-- =============================================================================
-- 8. Idempotency: no duplicate (PORTFOLIO_ID, AS_OF_TS, RUN_ID) rows
-- =============================================================================

select 'SNAPSHOT_IDEMPOTENCY' as TEST,
       iff(count(*) = 0, 'PASS', 'FAIL') as RESULT
from (
    select PORTFOLIO_ID, AS_OF_TS, RUN_ID, count(*) as cnt
    from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
    group by 1, 2, 3
    having cnt > 1
);

select 'NARRATIVE_IDEMPOTENCY' as TEST,
       iff(count(*) = 0, 'PASS', 'FAIL') as RESULT
from (
    select PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME, count(*) as cnt
    from MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE
    group by 1, 2, 3, 4
    having cnt > 1
);

-- =============================================================================
-- 9. Narrative has required JSON keys (headline, what_changed, what_matters, waiting_for)
-- =============================================================================

select 'NARRATIVE_JSON_STRUCTURE' as TEST,
       n.PORTFOLIO_ID,
       iff(
           n.NARRATIVE_JSON:headline is not null
           and n.NARRATIVE_JSON:what_changed is not null
           and n.NARRATIVE_JSON:what_matters is not null
           and n.NARRATIVE_JSON:waiting_for is not null,
           'PASS', 'FAIL'
       ) as RESULT,
       n.NARRATIVE_JSON:headline::string as HEADLINE_PREVIEW
from MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE n
qualify row_number() over (partition by n.PORTFOLIO_ID order by n.CREATED_AT desc) = 1;

-- =============================================================================
-- 10. Snapshot JSON has required top-level keys
-- =============================================================================

select 'SNAPSHOT_JSON_STRUCTURE' as TEST,
       s.PORTFOLIO_ID,
       iff(
           s.SNAPSHOT_JSON:gate is not null
           and s.SNAPSHOT_JSON:capacity is not null
           and s.SNAPSHOT_JSON:signals is not null
           and s.SNAPSHOT_JSON:proposals is not null
           and s.SNAPSHOT_JSON:detectors is not null
           and s.SNAPSHOT_JSON:kpis is not null,
           'PASS', 'FAIL'
       ) as RESULT
from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT s
qualify row_number() over (partition by s.PORTFOLIO_ID order by s.CREATED_AT desc) = 1;

-- =============================================================================
-- 11. Audit log has DAILY_DIGEST step entries
-- =============================================================================

select 'AUDIT_LOG_HAS_DIGEST_STEP' as TEST,
       iff(count(*) >= 1, 'PASS', 'PENDING') as RESULT,
       count(*) as DIGEST_AUDIT_ENTRIES
from MIP.APP.MIP_AUDIT_LOG
where EVENT_NAME = 'SP_AGENT_GENERATE_DAILY_DIGEST';

-- =============================================================================
-- 12. Facts hash matches between snapshot and narrative (grounding check)
-- =============================================================================

select 'HASH_GROUNDING_CHECK' as TEST,
       s.PORTFOLIO_ID,
       iff(
           s.SOURCE_FACTS_HASH = n.SOURCE_FACTS_HASH,
           'PASS', 'FAIL'
       ) as RESULT,
       s.SOURCE_FACTS_HASH as SNAPSHOT_HASH,
       n.SOURCE_FACTS_HASH as NARRATIVE_HASH
from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT s
join MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE n
    on  n.PORTFOLIO_ID = s.PORTFOLIO_ID
    and n.AS_OF_TS     = s.AS_OF_TS
    and n.RUN_ID       = s.RUN_ID
qualify row_number() over (partition by s.PORTFOLIO_ID order by s.CREATED_AT desc) = 1;
