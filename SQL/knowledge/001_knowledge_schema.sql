-- 001_knowledge_schema.sql
-- Purpose: Create the MIP.KNOWLEDGE schema for offline literature-derived concept staging.
--
-- IMPORTANT: This schema is an advisory staging/audit layer only.
--   - Literature concept cards are derived from trading books by the offline extractor tool.
--   - They are NOT market evidence, NOT trading signals, and NOT decision authority.
--   - No agent, procedure, or live execution path reads from this schema automatically.
--   - A separate approved task is required before any runtime component reads this data.

use role MIP_ADMIN_ROLE;
use database MIP;

create schema if not exists MIP.KNOWLEDGE
    comment = 'Advisory literature knowledge staging layer. Derived from offline concept extraction of trading books. NOT market evidence. NOT trading authority. NOT wired into any live agent or execution path. A separate approved task is required before runtime use.';
