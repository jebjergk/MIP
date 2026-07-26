-- Additive advisory attribution only. This column has no authority semantics.
USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

ALTER TABLE MIP.APP.SHADOW_CHAIR_RULING
    ADD COLUMN IF NOT EXISTS METHODOLOGIST_EFFECT VARIANT;

COMMENT ON COLUMN MIP.APP.SHADOW_CHAIR_RULING.METHODOLOGIST_EFFECT IS
    'Advisory-only Price Action & Trading Methodologist usage/effect trace. Does not affect authority, materialization, Submit gating, or execution.';
