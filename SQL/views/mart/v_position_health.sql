/* ================================================================
   v_position_health.sql
   Daily Position Health V1 - mart views.

   Six views, all keyed (or joined) on POSITION_EPISODE_KEY:
     V_DAILY_POSITION_VERDICT_LATEST
     V_DAILY_POSITION_VERDICT_HISTORY
     V_DAILY_POSITION_SHADOW_REVIEW_LATEST
     V_DAILY_POSITION_SHADOW_REVIEW_HISTORY
     V_SHADOW_POSITION_LIFECYCLE_STATUS
     V_POSITION_HEALTH_COMPARISON_LATEST   (real vs shadow side-by-side)

   Read-only by MIP_UI_API_ROLE.

   Phase 1 re-anchor: horizon-based scoring fields
   (EXPECTED_HORIZON_DAYS, HORIZON_SOURCE_CODE, TIME_EFFICIENCY) are
   intentionally NOT projected by these operator-facing views. The
   underlying physical columns on MIP.APP.DAILY_POSITION_VERDICT are
   still present for one validation cycle (and will hold NULL on new
   live-anchored rows produced by the re-pointed SP). They will be
   physically dropped in Phase 2 once parity is proven.

   Liveness gate: V_DAILY_POSITION_VERDICT_LATEST and
   V_POSITION_HEALTH_COMPARISON_LATEST inner-join MIP.MART.V_LIVE_OPEN_POSITIONS
   on POSITION_EPISODE_KEY so they only surface verdicts for positions
   currently open per broker truth. Stale sim-anchored verdict rows from
   pre-cutover runs remain in the underlying table for audit but do not
   leak into operator-facing surfaces.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA MART;

-- ----------------------------------------------------------------
-- V_DAILY_POSITION_VERDICT_LATEST
-- One row per POSITION_EPISODE_KEY: most recent AS_OF_DATE.
-- Explicit column list (no v.*) so operator-facing surfaces never
-- inherit deprecated horizon-based fields.
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.MART.V_DAILY_POSITION_VERDICT_LATEST AS
WITH live_keys AS (
    SELECT DISTINCT POSITION_EPISODE_KEY
    FROM MIP.MART.V_LIVE_OPEN_POSITIONS
),
latest AS (
    SELECT
        v.POSITION_EPISODE_KEY,
        MAX(v.AS_OF_DATE) AS LATEST_AS_OF_DATE
    FROM MIP.APP.DAILY_POSITION_VERDICT v
    INNER JOIN live_keys lk
      ON lk.POSITION_EPISODE_KEY = v.POSITION_EPISODE_KEY
    GROUP BY v.POSITION_EPISODE_KEY
)
SELECT
    v.AS_OF_DATE,
    v.POSITION_EPISODE_KEY,
    v.PORTFOLIO_ID,
    v.EPISODE_ID,
    v.SYMBOL,
    v.SIDE,
    v.ENTRY_DATE,
    v.ENTRY_TS,
    v.ENTRY_PRICE,
    v.DAYS_HELD,
    v.VERDICT,
    v.HEALTH_STATE,
    v.BASELINE_QUALITY,
    v.THESIS_INTEGRITY,
    v.PATH_QUALITY,
    v.REGIME_ALIGNMENT,
    v.FRAGILITY,
    v.SEVERITY,
    v.DISTANCE_TO_INVALIDATION_PCT,
    v.UNREALIZED_PNL_PCT,
    v.PRIMARY_REASON_CODE,
    v.PRIMARY_REASON_TEXT,
    v.OBSERVATION_SUMMARY,
    v.VERDICT_SUMMARY,
    v.WHY_SUMMARY,
    v.DETAIL_JSON,
    v.ENGINE_VERSION,
    v.SOURCE_RUN_TS,
    v.UPDATED_AT
FROM MIP.APP.DAILY_POSITION_VERDICT v
JOIN latest l
  ON l.POSITION_EPISODE_KEY = v.POSITION_EPISODE_KEY
 AND l.LATEST_AS_OF_DATE    = v.AS_OF_DATE;

-- ----------------------------------------------------------------
-- V_DAILY_POSITION_VERDICT_HISTORY
-- Full history, ordered for chart consumption. Explicit column list.
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.MART.V_DAILY_POSITION_VERDICT_HISTORY AS
SELECT
    v.AS_OF_DATE,
    v.POSITION_EPISODE_KEY,
    v.PORTFOLIO_ID,
    v.EPISODE_ID,
    v.SYMBOL,
    v.SIDE,
    v.ENTRY_DATE,
    v.ENTRY_TS,
    v.ENTRY_PRICE,
    v.DAYS_HELD,
    v.VERDICT,
    v.HEALTH_STATE,
    v.BASELINE_QUALITY,
    v.THESIS_INTEGRITY,
    v.PATH_QUALITY,
    v.REGIME_ALIGNMENT,
    v.FRAGILITY,
    v.SEVERITY,
    v.DISTANCE_TO_INVALIDATION_PCT,
    v.UNREALIZED_PNL_PCT,
    v.PRIMARY_REASON_CODE,
    v.PRIMARY_REASON_TEXT,
    v.OBSERVATION_SUMMARY,
    v.VERDICT_SUMMARY,
    v.WHY_SUMMARY,
    v.DETAIL_JSON,
    v.ENGINE_VERSION,
    v.SOURCE_RUN_TS,
    v.UPDATED_AT,
    ROW_NUMBER() OVER (
        PARTITION BY v.POSITION_EPISODE_KEY
        ORDER BY v.AS_OF_DATE DESC
    ) AS RN_DESC
FROM MIP.APP.DAILY_POSITION_VERDICT v;

-- ----------------------------------------------------------------
-- V_DAILY_POSITION_SHADOW_REVIEW_LATEST
-- One row per POSITION_EPISODE_KEY: most recent AS_OF_DATE.
-- Shadow review table has no horizon-based columns, so v.* is safe
-- here; we still pin to the explicit shape for symmetry.
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.MART.V_DAILY_POSITION_SHADOW_REVIEW_LATEST AS
WITH latest AS (
    SELECT
        POSITION_EPISODE_KEY,
        MAX(AS_OF_DATE) AS LATEST_AS_OF_DATE
    FROM MIP.APP.DAILY_POSITION_SHADOW_REVIEW
    GROUP BY POSITION_EPISODE_KEY
)
SELECT s.*
FROM MIP.APP.DAILY_POSITION_SHADOW_REVIEW s
JOIN latest l
  ON l.POSITION_EPISODE_KEY = s.POSITION_EPISODE_KEY
 AND l.LATEST_AS_OF_DATE    = s.AS_OF_DATE;

-- ----------------------------------------------------------------
-- V_DAILY_POSITION_SHADOW_REVIEW_HISTORY
-- Full history.
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.MART.V_DAILY_POSITION_SHADOW_REVIEW_HISTORY AS
SELECT
    s.*,
    ROW_NUMBER() OVER (
        PARTITION BY s.POSITION_EPISODE_KEY
        ORDER BY s.AS_OF_DATE DESC
    ) AS RN_DESC
FROM MIP.APP.DAILY_POSITION_SHADOW_REVIEW s;

-- ----------------------------------------------------------------
-- V_SHADOW_POSITION_LIFECYCLE_STATUS
-- One row per POSITION_EPISODE_KEY with derived fields for UI.
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.MART.V_SHADOW_POSITION_LIFECYCLE_STATUS AS
SELECT
    l.*,
    DATEDIFF('day', l.SHADOW_ENTRY_DATE, COALESCE(l.SHADOW_EXIT_DATE, CURRENT_DATE())) AS DAYS_SINCE_SHADOW_ENTRY,
    CASE
        WHEN l.SHADOW_STATUS = 'OPEN' THEN 'OPEN'
        WHEN l.SHADOW_STATUS = 'SIM_EXITED' AND l.REALIZED_RETURN_SHADOW_PCT IS NULL THEN 'EXITED_NO_PNL'
        WHEN l.SHADOW_STATUS = 'SIM_EXITED' AND l.REALIZED_RETURN_SHADOW_PCT >= 0 THEN 'EXITED_PROFIT'
        WHEN l.SHADOW_STATUS = 'SIM_EXITED' AND l.REALIZED_RETURN_SHADOW_PCT < 0 THEN 'EXITED_LOSS'
        ELSE 'UNKNOWN'
    END AS LIFECYCLE_STATE
FROM MIP.APP.SHADOW_POSITION_LIFECYCLE l;

-- ----------------------------------------------------------------
-- V_POSITION_HEALTH_COMPARISON_LATEST
-- Real verdict and shadow review side-by-side, joined on
-- (POSITION_EPISODE_KEY, AS_OF_DATE) for the latest AS_OF_DATE.
-- Includes lifecycle status and a normalized agreement label.
-- Horizon-based REAL_TIME_EFFICIENCY / EXPECTED_HORIZON_DAYS /
-- HORIZON_SOURCE_CODE are NOT projected (Phase 1 re-anchor).
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW MIP.MART.V_POSITION_HEALTH_COMPARISON_LATEST AS
WITH live_keys AS (
    SELECT DISTINCT POSITION_EPISODE_KEY
    FROM MIP.MART.V_LIVE_OPEN_POSITIONS
),
latest_verdict AS (
    SELECT
        v.POSITION_EPISODE_KEY,
        MAX(v.AS_OF_DATE) AS LATEST_AS_OF_DATE
    FROM MIP.APP.DAILY_POSITION_VERDICT v
    INNER JOIN live_keys lk
      ON lk.POSITION_EPISODE_KEY = v.POSITION_EPISODE_KEY
    GROUP BY v.POSITION_EPISODE_KEY
)
SELECT
    v.AS_OF_DATE,
    v.POSITION_EPISODE_KEY,
    v.PORTFOLIO_ID,
    v.EPISODE_ID,
    v.SYMBOL,
    v.SIDE,
    v.ENTRY_DATE,
    v.DAYS_HELD,

    /* Real */
    v.VERDICT                       AS REAL_VERDICT,
    v.HEALTH_STATE                  AS REAL_HEALTH_STATE,
    v.SEVERITY                      AS REAL_SEVERITY,
    v.BASELINE_QUALITY              AS REAL_BASELINE_QUALITY,
    v.THESIS_INTEGRITY              AS REAL_THESIS_INTEGRITY,
    v.PATH_QUALITY                  AS REAL_PATH_QUALITY,
    v.REGIME_ALIGNMENT              AS REAL_REGIME_ALIGNMENT,
    v.FRAGILITY                     AS REAL_FRAGILITY,
    v.PRIMARY_REASON_CODE           AS REAL_PRIMARY_REASON_CODE,
    v.OBSERVATION_SUMMARY           AS REAL_OBSERVATION_SUMMARY,
    v.VERDICT_SUMMARY               AS REAL_VERDICT_SUMMARY,
    v.WHY_SUMMARY                   AS REAL_WHY_SUMMARY,
    v.UNREALIZED_PNL_PCT,
    v.DISTANCE_TO_INVALIDATION_PCT,

    /* Shadow */
    s.SHADOW_VERDICT,
    s.SHADOW_THESIS_STATUS,
    s.SHADOW_ACTION_BIAS,
    s.SHADOW_SEVERITY,
    s.PRIMARY_REASON_CODE           AS SHADOW_PRIMARY_REASON_CODE,
    s.OBSERVATION_SUMMARY           AS SHADOW_OBSERVATION_SUMMARY,
    s.VERDICT_SUMMARY               AS SHADOW_VERDICT_SUMMARY,
    s.WHY_SUMMARY                   AS SHADOW_WHY_SUMMARY,
    s.RATIONALE_TEXT                AS SHADOW_RATIONALE_TEXT,
    s.SHADOW_RUN_STATUS,
    s.SHADOW_RUN_TS,
    s.SHADOW_RUN_ERROR,

    /* Agreement */
    CASE
        WHEN s.SHADOW_VERDICT IS NULL THEN 'NO_SHADOW'
        WHEN s.SHADOW_VERDICT = v.VERDICT THEN 'AGREE'
        WHEN s.SHADOW_VERDICT = 'EXIT_REVIEW' AND v.VERDICT IN ('KEEP', 'WATCH') THEN 'SHADOW_MORE_BEARISH'
        WHEN v.VERDICT = 'EXIT_REVIEW' AND s.SHADOW_VERDICT IN ('KEEP', 'WATCH') THEN 'REAL_MORE_BEARISH'
        ELSE 'DIFFERENT_BUT_BOTH_NON_EXIT'
    END                             AS AGREEMENT_LABEL,

    /* Lifecycle */
    l.SHADOW_STATUS,
    l.SHADOW_EXIT_DATE,
    l.SHADOW_EXIT_PRICE,
    l.SHADOW_EXIT_TRIGGER,
    l.REALIZED_RETURN_SHADOW_PCT,
    l.DAYS_HELD_SHADOW

FROM MIP.APP.DAILY_POSITION_VERDICT v
JOIN latest_verdict lv
       ON lv.POSITION_EPISODE_KEY = v.POSITION_EPISODE_KEY
      AND lv.LATEST_AS_OF_DATE    = v.AS_OF_DATE
LEFT JOIN MIP.APP.DAILY_POSITION_SHADOW_REVIEW s
       ON s.POSITION_EPISODE_KEY = v.POSITION_EPISODE_KEY
      AND s.AS_OF_DATE           = v.AS_OF_DATE
LEFT JOIN MIP.APP.SHADOW_POSITION_LIFECYCLE l
       ON l.POSITION_EPISODE_KEY = v.POSITION_EPISODE_KEY;

/* ================================================================
   Grants - read-only views for the UI API role and admin role.
   ================================================================ */
GRANT SELECT ON VIEW MIP.MART.V_DAILY_POSITION_VERDICT_LATEST          TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_DAILY_POSITION_VERDICT_HISTORY         TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_DAILY_POSITION_SHADOW_REVIEW_LATEST    TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_DAILY_POSITION_SHADOW_REVIEW_HISTORY   TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_SHADOW_POSITION_LIFECYCLE_STATUS       TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_POSITION_HEALTH_COMPARISON_LATEST      TO ROLE MIP_ADMIN_ROLE;

GRANT SELECT ON VIEW MIP.MART.V_DAILY_POSITION_VERDICT_LATEST          TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_DAILY_POSITION_VERDICT_HISTORY         TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_DAILY_POSITION_SHADOW_REVIEW_LATEST    TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_DAILY_POSITION_SHADOW_REVIEW_HISTORY   TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_SHADOW_POSITION_LIFECYCLE_STATUS       TO ROLE MIP_UI_API_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_POSITION_HEALTH_COMPARISON_LATEST      TO ROLE MIP_UI_API_ROLE;
