/* ================================================================
   repair_agentic_authority_negative_session_age.sql

   One-shot repair for Stage 4a/4b/4c authority rows that recorded a
   negative SESSION_AGE_MINUTES due to the pre-fix Python staleness
   bug. Cause:

     - SHADOW_BOARD_SESSION.CREATED_AT is TIMESTAMP_NTZ in Snowflake.
     - Snowflake account TIMEZONE = 'Europe/Berlin', so CURRENT_TIMESTAMP()
       writes Berlin wall-clock into the NTZ column (no offset info).
     - The Python staleness check naïvely treated those values as UTC and
       compared against datetime.now(timezone.utc), producing a 1–2 hour
       negative skew.

   Going-forward fix: `_fetch_shadow_session_sync` now selects
   `DATEDIFF('minute', CREATED_AT, CURRENT_TIMESTAMP()) AS AGE_MINUTES_AT_FETCH`
   and `build_staleness_check` prefers that DB-computed value.

   This repair recomputes the correct historical "age at commit" for the
   affected rows in-Snowflake (the diff cancels any session-TIMEZONE
   offset). Idempotent — re-running affects only rows with negative ages.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

-- 1) Inspect: which rows are affected?
SELECT 'before_repair' AS phase,
       COUNT(*) AS bad_rows,
       MIN(SESSION_AGE_MINUTES) AS min_age,
       MAX(SESSION_AGE_MINUTES) AS max_age
FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY
WHERE SESSION_AGE_MINUTES < 0;

-- 2) Recompute the correct historical age using both NTZ timestamps from
-- the same database (both stored in Snowflake session TIMEZONE; the diff
-- cancels the offset and is timezone-agnostic).
UPDATE MIP.APP.AGENTIC_REVALIDATION_AUTHORITY ara
SET    SESSION_AGE_MINUTES = GREATEST(0,
           DATEDIFF('minute', sbs.CREATED_AT, ara.CREATED_AT)
       ),
       UPDATED_AT = CURRENT_TIMESTAMP()
FROM   MIP.APP.SHADOW_BOARD_SESSION sbs
WHERE  ara.SHADOW_SESSION_ID = sbs.SESSION_ID
  AND  ara.SESSION_AGE_MINUTES < 0;

-- 3) Verify
SELECT 'after_repair' AS phase,
       COUNT(*) AS bad_rows,
       MIN(SESSION_AGE_MINUTES) AS min_age,
       MAX(SESSION_AGE_MINUTES) AS max_age
FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY
WHERE SESSION_AGE_MINUTES < 0;

SELECT 'final_age_distribution' AS phase,
       COUNT(*) AS total_rows,
       MIN(SESSION_AGE_MINUTES) AS min_age,
       MAX(SESSION_AGE_MINUTES) AS max_age,
       AVG(SESSION_AGE_MINUTES) AS avg_age
FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY;
