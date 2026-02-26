-- 20260226_add_symbol_cohort_to_ingest_universe.sql
-- Purpose: Add additive cohort tagging for ingestion/training universes.

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.APP.INGEST_UNIVERSE
    add column if not exists SYMBOL_COHORT string default 'CORE';

update MIP.APP.INGEST_UNIVERSE
   set SYMBOL_COHORT = 'CORE'
 where SYMBOL_COHORT is null;

