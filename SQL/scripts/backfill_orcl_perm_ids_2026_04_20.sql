-- One-off backfill: ORCL bracket placed 2026-04-20.
-- IB returned perm_id=0 in the ack, so LIVE_ORDERS recorded the TWS local order_id (351/352/353).
-- The real perm_ids (1395266669/70/71) only appeared once IB acknowledged the orders.
-- This left _recent_unmapped_execution_summary unable to link the ORCL fill, blocking new submissions.
--
-- This script restores the linkage (BROKER_ORDER_ID = perm_id) and marks the parent FILLED.
-- Audit rows are written to BROKER_EVENT_LEDGER for traceability.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA LIVE;

-- 1) Parent BUY LMT (filled at 175.88 / 3 sh per BROKER_SNAPSHOTS exec_id 0000dc8f.6a35b374.01.01)
UPDATE MIP.LIVE.LIVE_ORDERS
   SET BROKER_ORDER_ID = '1395266669',
       STATUS          = 'FILLED',
       QTY_FILLED      = 3,
       AVG_FILL_PRICE  = 175.88,
       FILLED_AT       = COALESCE(FILLED_AT, '2026-04-20T14:15:39'::TIMESTAMP_NTZ),
       LAST_UPDATED_AT = CURRENT_TIMESTAMP()
 WHERE PORTFOLIO_ID    = 1
   AND ACTION_ID       = 'c58352d6-efe2-4bf5-8b10-e97d6d81a52b'
   AND ORDER_ROLE      = 'ENTRY'
   AND BROKER_ORDER_ID = '351';

-- 2) TAKE_PROFIT SELL LMT (working: Submitted)
UPDATE MIP.LIVE.LIVE_ORDERS
   SET BROKER_ORDER_ID = '1395266670',
       STATUS          = 'SUBMITTED',
       LAST_UPDATED_AT = CURRENT_TIMESTAMP()
 WHERE PORTFOLIO_ID    = 1
   AND ACTION_ID       = 'c58352d6-efe2-4bf5-8b10-e97d6d81a52b'
   AND ORDER_ROLE      = 'PROTECTIVE_TP'
   AND BROKER_ORDER_ID = '352';

-- 3) STOP_LOSS SELL STP (working: PreSubmitted)
UPDATE MIP.LIVE.LIVE_ORDERS
   SET BROKER_ORDER_ID = '1395266671',
       STATUS          = 'PRESUBMITTED',
       LAST_UPDATED_AT = CURRENT_TIMESTAMP()
 WHERE PORTFOLIO_ID    = 1
   AND ACTION_ID       = 'c58352d6-efe2-4bf5-8b10-e97d6d81a52b'
   AND ORDER_ROLE      = 'PROTECTIVE_STOP'
   AND BROKER_ORDER_ID = '353';

-- 4) Audit ledger entries (one per leg) so the backfill is traceable.
INSERT INTO MIP.LIVE.BROKER_EVENT_LEDGER (
  EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, ACTION_ID, BROKER_ORDER_ID, SYMBOL, PAYLOAD
)
SELECT UUID_STRING(), CURRENT_TIMESTAMP(), 'PERM_ID_BACKFILL', 1,
       'c58352d6-efe2-4bf5-8b10-e97d6d81a52b', '1395266669', 'ORCL',
       PARSE_JSON('{
         "actor": "cursor_agent",
         "reason": "IB returned perm_id=0 in ack so LIVE_ORDERS recorded TWS local order_id 351. Backfilled perm_id 1395266669 (parent BUY LMT) and marked FILLED from execution 0000dc8f.6a35b374.01.01.",
         "before": {"BROKER_ORDER_ID": "351", "STATUS": "PENDINGSUBMIT"},
         "after":  {"BROKER_ORDER_ID": "1395266669", "STATUS": "FILLED", "QTY_FILLED": 3, "AVG_FILL_PRICE": 175.88}
       }');

INSERT INTO MIP.LIVE.BROKER_EVENT_LEDGER (
  EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, ACTION_ID, BROKER_ORDER_ID, SYMBOL, PAYLOAD
)
SELECT UUID_STRING(), CURRENT_TIMESTAMP(), 'PERM_ID_BACKFILL', 1,
       'c58352d6-efe2-4bf5-8b10-e97d6d81a52b', '1395266670', 'ORCL',
       PARSE_JSON('{
         "actor": "cursor_agent",
         "reason": "Backfilling perm_id 1395266670 for TAKE_PROFIT leg (was TWS local 352).",
         "before": {"BROKER_ORDER_ID": "352", "STATUS": "PENDINGSUBMIT"},
         "after":  {"BROKER_ORDER_ID": "1395266670", "STATUS": "SUBMITTED"}
       }');

INSERT INTO MIP.LIVE.BROKER_EVENT_LEDGER (
  EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, ACTION_ID, BROKER_ORDER_ID, SYMBOL, PAYLOAD
)
SELECT UUID_STRING(), CURRENT_TIMESTAMP(), 'PERM_ID_BACKFILL', 1,
       'c58352d6-efe2-4bf5-8b10-e97d6d81a52b', '1395266671', 'ORCL',
       PARSE_JSON('{
         "actor": "cursor_agent",
         "reason": "Backfilling perm_id 1395266671 for STOP_LOSS leg (was TWS local 353).",
         "before": {"BROKER_ORDER_ID": "353", "STATUS": "PENDINGSUBMIT"},
         "after":  {"BROKER_ORDER_ID": "1395266671", "STATUS": "PRESUBMITTED"}
       }');

-- 5) Verify the rows now show the perm_ids.
SELECT ORDER_ID, BROKER_ORDER_ID, ORDER_ROLE, STATUS, QTY_FILLED, AVG_FILL_PRICE
  FROM MIP.LIVE.LIVE_ORDERS
 WHERE PORTFOLIO_ID = 1
   AND ACTION_ID    = 'c58352d6-efe2-4bf5-8b10-e97d6d81a52b'
 ORDER BY ORDER_ROLE;
