-- restore_live_order_filled_at_from_ib_executions_20260528.sql
--
-- Recovery for 2026-05-28 reconcile-stamping incident:
-- `cursorfiles/apply_execution_reconcile_p1.py` ran ~14:39 UTC and synced 7
-- LIVE_ORDERS rows that IBKR had filled days earlier but MIP still had as
-- PENDINGSUBMIT (null QTY_FILLED). The status / qty / price reconcile was
-- correct, but `update_live_order_status` stamped FILLED_AT with
-- CURRENT_TIMESTAMP(), so all fills now appear as 2026-05-28 in the UI.
--
-- This script restores FILLED_AT from the matching BROKER_SNAPSHOTS EXECUTION
-- row's PAYLOAD:time (the actual IB fill time), for the affected orders only.
--
-- Safe to re-run: only updates rows where FILLED_AT::date = '2026-05-28' AND
-- CREATED_AT::date < '2026-05-28' (the reconcile-stamped set), and only when
-- a valid IBKR execution payload time is found.
--
-- DO NOT re-run cursorfiles/apply_execution_reconcile_p1.py until the API
-- guard in update_live_order_status is in place (filled_at parameter +
-- coalesce). Re-running today would re-stamp dates back to today.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

------------------------------------------------------------------
-- 1) Preview: affected orders + proposed FILLED_AT from IB payload.
--    Run as a SELECT first; nothing changes until step 2.
------------------------------------------------------------------
WITH ib_exec_dedup AS (
    SELECT
        TRIM(COALESCE(
            OPEN_ORDER_ID::string,
            PAYLOAD:perm_id::string,
            PAYLOAD:orderId::string,
            PAYLOAD:order_id::string
        )) AS broker_key,
        TRY_TO_TIMESTAMP_NTZ(PAYLOAD:time::string) AS ib_exec_ts_ntz,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(COALESCE(
                OPEN_ORDER_ID::string,
                PAYLOAD:perm_id::string,
                PAYLOAD:orderId::string,
                PAYLOAD:order_id::string
            ))
            ORDER BY SNAPSHOT_TS DESC
        ) AS rn
    FROM MIP.LIVE.BROKER_SNAPSHOTS
    WHERE SNAPSHOT_TYPE = 'EXECUTION'
      AND SNAPSHOT_TS >= DATEADD(day, -30, CURRENT_TIMESTAMP())
)
SELECT
    o.ORDER_ID,
    o.SYMBOL,
    o.SIDE,
    o.STATUS,
    o.BROKER_ORDER_ID,
    o.CREATED_AT,
    o.FILLED_AT                       AS filled_at_before,
    x.ib_exec_ts_ntz                  AS filled_at_after_proposed
FROM MIP.LIVE.LIVE_ORDERS o
LEFT JOIN ib_exec_dedup x
       ON x.rn = 1
      AND TRIM(o.BROKER_ORDER_ID) = x.broker_key
WHERE o.STATUS IN ('FILLED', 'PARTIAL_FILL')
  AND o.FILLED_AT::date = '2026-05-28'
  AND o.CREATED_AT::date < '2026-05-28'
ORDER BY x.ib_exec_ts_ntz;

------------------------------------------------------------------
-- 2) Backfill FILLED_AT from IB execution payload time.
--    Conservative: only writes when PAYLOAD:time parses cleanly.
------------------------------------------------------------------
UPDATE MIP.LIVE.LIVE_ORDERS o
SET
    o.FILLED_AT       = x.ib_exec_ts_ntz,
    o.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
FROM (
    SELECT
        TRIM(COALESCE(
            OPEN_ORDER_ID::string,
            PAYLOAD:perm_id::string,
            PAYLOAD:orderId::string,
            PAYLOAD:order_id::string
        )) AS broker_key,
        TRY_TO_TIMESTAMP_NTZ(PAYLOAD:time::string) AS ib_exec_ts_ntz,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(COALESCE(
                OPEN_ORDER_ID::string,
                PAYLOAD:perm_id::string,
                PAYLOAD:orderId::string,
                PAYLOAD:order_id::string
            ))
            ORDER BY SNAPSHOT_TS DESC
        ) AS rn
    FROM MIP.LIVE.BROKER_SNAPSHOTS
    WHERE SNAPSHOT_TYPE = 'EXECUTION'
      AND SNAPSHOT_TS >= DATEADD(day, -30, CURRENT_TIMESTAMP())
) x
WHERE x.rn = 1
  AND TRIM(o.BROKER_ORDER_ID) = x.broker_key
  AND x.ib_exec_ts_ntz IS NOT NULL
  AND o.STATUS IN ('FILLED', 'PARTIAL_FILL')
  AND o.FILLED_AT::date = '2026-05-28'
  AND o.CREATED_AT::date < '2026-05-28';

------------------------------------------------------------------
-- 3) Audit ledger row (single roll-up entry; per-order detail is in
--    EXECUTION_RECONCILE_APPLY events from earlier today).
------------------------------------------------------------------
INSERT INTO MIP.LIVE.BROKER_EVENT_LEDGER (
    EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, PAYLOAD
)
SELECT
    UUID_STRING(),
    CURRENT_TIMESTAMP(),
    'FILLED_AT_BACKFILL',
    1,
    PARSE_JSON('{
        "actor": "cursor_agent",
        "reason": "Restore FILLED_AT after 2026-05-28 reconcile incident. update_live_order_status stamped FILLED_AT = current_timestamp() for orders that IBKR had filled days earlier. This script copied PAYLOAD:time from the latest matching BROKER_SNAPSHOTS EXECUTION row into FILLED_AT.",
        "scope": "LIVE_ORDERS where FILLED_AT::date = 2026-05-28 AND CREATED_AT::date < 2026-05-28",
        "follow_up": "Deploy update_live_order_status filled_at parameter + coalesce guard before re-running apply_execution_reconcile_p1.py."
    }');

------------------------------------------------------------------
-- 4) Verification: the 7+ affected orders should now show FILLED_AT
--    matching IB payload time, not 2026-05-28.
------------------------------------------------------------------
SELECT
    o.ORDER_ID,
    o.SYMBOL,
    o.SIDE,
    o.STATUS,
    o.BROKER_ORDER_ID,
    o.CREATED_AT,
    o.FILLED_AT                  AS filled_at_after,
    o.LAST_UPDATED_AT
FROM MIP.LIVE.LIVE_ORDERS o
WHERE o.BROKER_ORDER_ID IN (
        '679611670',   -- WMT BUY
        '679611674',   -- XOM BUY
        '1420141846',  -- NKE BUY
        '1420141957',  -- MSFT BUY
        '1420141898',  -- PFE BUY
        '1420141896',  -- PFE SELL
        '18456828',    -- JD SELL
        '37773617'     -- CLF SELL (PARTIAL_FILL)
)
ORDER BY o.FILLED_AT;
