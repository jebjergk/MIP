-- Missing audit row for the parent leg (the original script's first INSERT was split by a ';' inside JSON).
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
