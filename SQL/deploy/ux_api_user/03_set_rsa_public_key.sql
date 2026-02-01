-- 03_set_rsa_public_key.sql
-- Purpose: Set RSA public key for the UX API user (keypair auth, MFA environments).
-- Idempotent: safe to run multiple times (overwrites with same key).
--
-- Placeholders: replace :var with literal before running.
--   :ux_user        default MIP_UI_API
--   :rsa_public_key public key body (see instructions below)
--
-- Run as SECURITYADMIN or role with MODIFY PROGRAMMATIC AUTHENTICATION METHODS on the user.
--
-- =============================================================================
-- HOW TO PASTE THE PUBLIC KEY BODY
-- =============================================================================
-- Snowflake RSA_PUBLIC_KEY expects the raw base64 content ONLY.
--
-- 1. Generate the public key file:
--      openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--
-- 2. Your rsa_key.pub looks like:
--      -----BEGIN PUBLIC KEY-----
--      MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...
--      ...more lines...
--      -----END PUBLIC KEY-----
--
-- 3. For Snowflake, use ONLY the base64 lines between the headers.
--    Remove "-----BEGIN PUBLIC KEY-----" and "-----END PUBLIC KEY-----".
--    Concatenate all base64 lines into a single string (no newlines).
--
--    Example: if the middle lines are
--      MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA
--      xYz123...
--    then paste: MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxYz123...
--
-- 4. Replace REPLACE_ME_WITH_PUBLIC_KEY_BODY below with that single string.
-- =============================================================================

use role SECURITYADMIN;

alter user MIP_UI_API set rsa_public_key = 'REPLACE_ME_WITH_PUBLIC_KEY_BODY';  -- :ux_user, :rsa_public_key
