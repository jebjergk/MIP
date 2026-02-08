-- 200b_alter_digest_add_scope.sql
-- Purpose: Add SCOPE column to digest tables for GLOBAL vs PORTFOLIO scoping.
-- SCOPE = 'PORTFOLIO' (existing rows) or 'GLOBAL' (new system-wide digest).
-- Also drops the old unique constraint and recreates it to include SCOPE.
-- PORTFOLIO_ID becomes nullable (GLOBAL rows have PORTFOLIO_ID = NULL).
--
-- Migration: safe to run on existing deployments; existing rows get SCOPE='PORTFOLIO'.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ── SNAPSHOT table ──────────────────────────────────────────
alter table MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
    add column if not exists SCOPE varchar(16) default 'PORTFOLIO';

-- Backfill existing rows
update MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
   set SCOPE = 'PORTFOLIO'
 where SCOPE is null;

-- Make PORTFOLIO_ID nullable for GLOBAL scope
alter table MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
    alter column PORTFOLIO_ID drop not null;

-- Drop old unique constraint and recreate with SCOPE
-- (Snowflake: drop by name, recreate)
alter table MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
    drop constraint UQ_DIGEST_SNAPSHOT;

alter table MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
    add constraint UQ_DIGEST_SNAPSHOT unique (SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID);

-- ── NARRATIVE table ─────────────────────────────────────────
alter table MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE
    add column if SCOPE varchar(16) default 'PORTFOLIO';

-- Backfill existing rows
update MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE
   set SCOPE = 'PORTFOLIO'
 where SCOPE is null;

-- Make PORTFOLIO_ID nullable for GLOBAL scope
alter table MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE
    alter column PORTFOLIO_ID drop not null;

-- Drop old unique constraint and recreate with SCOPE
alter table MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE
    drop constraint UQ_DIGEST_NARRATIVE;

alter table MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE
    add constraint UQ_DIGEST_NARRATIVE unique (SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME);
