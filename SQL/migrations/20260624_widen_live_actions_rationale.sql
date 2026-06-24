-- 20260624_widen_live_actions_rationale.sql
-- Fix: LIVE_ACTIONS.PROPOSAL_RATIONALE and SETUP_NARRATIVE were VARCHAR(500),
-- but the source STRUCTURAL_TRADE_PROPOSALS.RATIONALE_TEXT is VARCHAR(2000).
-- Phase 4 chair rationales routinely exceed 500 chars (e.g. ~900+), which made
-- import_structural_proposals fail with Snowflake error 100078 (string too long)
-- and silently prevented proposals from reaching LPA as pending decisions.
-- Widen both narrative columns to VARCHAR(2000) to match the source.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

ALTER TABLE MIP.LIVE.LIVE_ACTIONS
    ALTER COLUMN PROPOSAL_RATIONALE SET DATA TYPE VARCHAR(2000);

ALTER TABLE MIP.LIVE.LIVE_ACTIONS
    ALTER COLUMN SETUP_NARRATIVE SET DATA TYPE VARCHAR(2000);
