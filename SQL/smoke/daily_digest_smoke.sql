-- daily_digest_smoke.sql
-- Purpose: Smoke tests for Daily Intelligence Digest tables, view, and procedure.
-- Run after deploying 200/201/202/200b SQL files and running the pipeline at least once.
-- Covers both PORTFOLIO and GLOBAL scopes.

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
-- 2. Views compile (select 0 rows)
-- =============================================================================

select 'SNAPSHOT_VIEW_COMPILES' as TEST,
       iff(count(*) >= 0, 'PASS', 'FAIL') as RESULT
from MIP.MART.V_DAILY_DIGEST_SNAPSHOT
where 1 = 0;

select 'GLOBAL_SNAPSHOT_VIEW_COMPILES' as TEST,
       iff(count(*) >= 0, 'PASS', 'FAIL') as RESULT
from MIP.MART.V_DAILY_DIGEST_SNAPSHOT_GLOBAL
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
-- 4. SCOPE column exists on both tables
-- =============================================================================

select 'SNAPSHOT_HAS_SCOPE' as TEST,
       iff(count(*) = 1, 'PASS', 'FAIL') as RESULT
from MIP.INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = 'AGENT_OUT'
  and TABLE_NAME = 'DAILY_DIGEST_SNAPSHOT'
  and COLUMN_NAME = 'SCOPE';

select 'NARRATIVE_HAS_SCOPE' as TEST,
       iff(count(*) = 1, 'PASS', 'FAIL') as RESULT
from MIP.INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = 'AGENT_OUT'
  and TABLE_NAME = 'DAILY_DIGEST_NARRATIVE'
  and COLUMN_NAME = 'SCOPE';

-- =============================================================================
-- 5. At least one PORTFOLIO snapshot per active portfolio (after first run)
-- =============================================================================

select 'SNAPSHOT_PER_PORTFOLIO' as TEST,
       p.PORTFOLIO_ID,
       iff(count(s.SNAPSHOT_ID) >= 1, 'PASS', 'PENDING') as RESULT
from MIP.APP.PORTFOLIO p
left join MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT s
    on s.PORTFOLIO_ID = p.PORTFOLIO_ID
   and s.SCOPE = 'PORTFOLIO'
where p.STATUS = 'ACTIVE'
group by p.PORTFOLIO_ID
order by p.PORTFOLIO_ID;

-- =============================================================================
-- 6. At least one PORTFOLIO narrative per active portfolio (after first run)
-- =============================================================================

select 'NARRATIVE_PER_PORTFOLIO' as TEST,
       p.PORTFOLIO_ID,
       iff(count(n.NARRATIVE_ID) >= 1, 'PASS', 'PENDING') as RESULT
from MIP.APP.PORTFOLIO p
left join MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE n
    on n.PORTFOLIO_ID = p.PORTFOLIO_ID
   and n.SCOPE = 'PORTFOLIO'
where p.STATUS = 'ACTIVE'
group by p.PORTFOLIO_ID
order by p.PORTFOLIO_ID;

-- =============================================================================
-- 7. At least one GLOBAL snapshot + narrative exists (after first run)
-- =============================================================================

select 'GLOBAL_SNAPSHOT_EXISTS' as TEST,
       iff(count(*) >= 1, 'PASS', 'PENDING') as RESULT,
       count(*) as ROW_COUNT
from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
where SCOPE = 'GLOBAL'
  and PORTFOLIO_ID is null;

select 'GLOBAL_NARRATIVE_EXISTS' as TEST,
       iff(count(*) >= 1, 'PASS', 'PENDING') as RESULT,
       count(*) as ROW_COUNT
from MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE
where SCOPE = 'GLOBAL'
  and PORTFOLIO_ID is null;

-- =============================================================================
-- 8. CREATED_AT is never NULL (acceptance criteria)
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
-- 9. SOURCE_FACTS_HASH is populated
-- =============================================================================

select 'SNAPSHOT_HASH_POPULATED' as TEST,
       iff(count_if(SOURCE_FACTS_HASH is null) = 0 or count(*) = 0, 'PASS', 'FAIL') as RESULT,
       count(*) as TOTAL_ROWS,
       count_if(SOURCE_FACTS_HASH is null) as NULL_HASH_COUNT
from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT;

-- =============================================================================
-- 10. Idempotency: no duplicate (SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID) rows
-- =============================================================================

select 'SNAPSHOT_IDEMPOTENCY' as TEST,
       iff(count(*) = 0, 'PASS', 'FAIL') as RESULT
from (
    select SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID, count(*) as cnt
    from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
    group by 1, 2, 3, 4
    having cnt > 1
);

select 'NARRATIVE_IDEMPOTENCY' as TEST,
       iff(count(*) = 0, 'PASS', 'FAIL') as RESULT
from (
    select SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME, count(*) as cnt
    from MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE
    group by 1, 2, 3, 4, 5
    having cnt > 1
);

-- =============================================================================
-- 11. PORTFOLIO narrative has required JSON keys
-- =============================================================================

select 'NARRATIVE_JSON_STRUCTURE_PORTFOLIO' as TEST,
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
where n.SCOPE = 'PORTFOLIO'
qualify row_number() over (partition by n.PORTFOLIO_ID order by n.CREATED_AT desc) = 1;

-- =============================================================================
-- 12. GLOBAL narrative has required JSON keys
-- =============================================================================

select 'NARRATIVE_JSON_STRUCTURE_GLOBAL' as TEST,
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
    from MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE
    where SCOPE = 'GLOBAL'
      and PORTFOLIO_ID is null
    qualify row_number() over (order by CREATED_AT desc) = 1
) n;

-- =============================================================================
-- 13. PORTFOLIO snapshot JSON has required top-level keys
-- =============================================================================

select 'SNAPSHOT_JSON_STRUCTURE_PORTFOLIO' as TEST,
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
where s.SCOPE = 'PORTFOLIO'
qualify row_number() over (partition by s.PORTFOLIO_ID order by s.CREATED_AT desc) = 1;

-- =============================================================================
-- 14. GLOBAL snapshot JSON has required top-level keys
-- =============================================================================

select 'SNAPSHOT_JSON_STRUCTURE_GLOBAL' as TEST,
       case
           when count(*) = 0 then 'PENDING'
           when max(iff(
               s.SNAPSHOT_JSON:scope::string = 'GLOBAL'
               and s.SNAPSHOT_JSON:system is not null
               and s.SNAPSHOT_JSON:gates is not null
               and s.SNAPSHOT_JSON:capacity is not null
               and s.SNAPSHOT_JSON:signals is not null
               and s.SNAPSHOT_JSON:proposals is not null
               and s.SNAPSHOT_JSON:training is not null
               and s.SNAPSHOT_JSON:detectors is not null,
               1, 0
           )) = 1 then 'PASS'
           else 'FAIL'
       end as RESULT
from (
    select *
    from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
    where SCOPE = 'GLOBAL'
      and PORTFOLIO_ID is null
    qualify row_number() over (order by CREATED_AT desc) = 1
) s;

-- =============================================================================
-- 15. Audit log has DAILY_DIGEST step entries
-- =============================================================================

select 'AUDIT_LOG_HAS_DIGEST_STEP' as TEST,
       iff(count(*) >= 1, 'PASS', 'PENDING') as RESULT,
       count(*) as DIGEST_AUDIT_ENTRIES
from MIP.APP.MIP_AUDIT_LOG
where EVENT_NAME = 'SP_AGENT_GENERATE_DAILY_DIGEST';

-- =============================================================================
-- 16. Facts hash matches between snapshot and narrative — PORTFOLIO scope
-- =============================================================================

select 'HASH_GROUNDING_PORTFOLIO' as TEST,
       s.PORTFOLIO_ID,
       iff(
           s.SOURCE_FACTS_HASH = n.SOURCE_FACTS_HASH,
           'PASS', 'FAIL'
       ) as RESULT,
       s.SOURCE_FACTS_HASH as SNAPSHOT_HASH,
       n.SOURCE_FACTS_HASH as NARRATIVE_HASH
from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT s
join MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE n
    on  n.SCOPE        = s.SCOPE
    and n.PORTFOLIO_ID = s.PORTFOLIO_ID
    and n.AS_OF_TS     = s.AS_OF_TS
    and n.RUN_ID       = s.RUN_ID
where s.SCOPE = 'PORTFOLIO'
qualify row_number() over (partition by s.PORTFOLIO_ID order by s.CREATED_AT desc) = 1;

-- =============================================================================
-- 17. Facts hash matches between snapshot and narrative — GLOBAL scope
-- =============================================================================

with global_pair as (
    select s.SOURCE_FACTS_HASH as SNAPSHOT_HASH,
           n.SOURCE_FACTS_HASH as NARRATIVE_HASH
    from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT s
    join MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE n
        on  n.SCOPE = s.SCOPE
        and n.PORTFOLIO_ID is null
        and s.PORTFOLIO_ID is null
        and n.AS_OF_TS     = s.AS_OF_TS
        and n.RUN_ID       = s.RUN_ID
    where s.SCOPE = 'GLOBAL'
    qualify row_number() over (order by s.CREATED_AT desc) = 1
)
select 'HASH_GROUNDING_GLOBAL' as TEST,
       case
           when (select count(*) from global_pair) = 0 then 'PENDING'
           when (select count(*) from global_pair where SNAPSHOT_HASH = NARRATIVE_HASH) = 1 then 'PASS'
           else 'FAIL'
       end as RESULT,
       (select SNAPSHOT_HASH from global_pair limit 1) as SNAPSHOT_HASH,
       (select NARRATIVE_HASH from global_pair limit 1) as NARRATIVE_HASH;
