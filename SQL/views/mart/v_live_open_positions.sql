/* ================================================================
   v_live_open_positions.sql
   MIP.MART.V_LIVE_OPEN_POSITIONS

   Canonical "current open positions" source of truth for all
   operator-facing surfaces (Position Health, Cockpit, Live Portfolio
   Activity, Today, etc.).

   Anchored on MIP.LIVE.BROKER_SNAPSHOTS (POSITION rows) - i.e. real
   IBKR broker truth - NOT on the simulation/horizon-based research
   book (PORTFOLIO_POSITIONS / PORTFOLIO_TRADES, which carry
   HOLD_UNTIL_INDEX semantics that no longer apply to live trades).

   ENTRY_TS / ENTRY_PRICE / PROPOSAL_ID are derived from the live
   execution lineage: the earliest still-open ENTRY-leg fill in
   MIP.LIVE.LIVE_ORDERS (joined via MIP.LIVE.LIVE_ACTIONS), filtered
   to actions that have NOT been closed out in MIP.LIVE.TRADE_CLOSEOUT.
   When no LIVE_ORDERS lineage is available the SNAPSHOT_TS / AVG_COST
   are used as a defensive fallback (ENTRY_SOURCE = 'BROKER_SNAPSHOT_FALLBACK').

   Read-only by MIP_UI_API_ROLE.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

CREATE OR REPLACE VIEW MIP.MART.V_LIVE_OPEN_POSITIONS AS
WITH latest_snapshot AS (
    SELECT
        IBKR_ACCOUNT_ID,
        MAX(SNAPSHOT_TS) AS LATEST_TS
    FROM MIP.LIVE.BROKER_SNAPSHOTS
    WHERE SNAPSHOT_TYPE = 'POSITION'
    GROUP BY IBKR_ACCOUNT_ID
),
broker_open AS (
    SELECT
        bs.SNAPSHOT_TS,
        bs.IBKR_ACCOUNT_ID,
        UPPER(bs.SYMBOL)                         AS SYMBOL,
        UPPER(COALESCE(bs.SECURITY_TYPE, 'STK')) AS SECURITY_TYPE,
        UPPER(COALESCE(bs.CURRENCY, 'USD'))      AS CURRENCY,
        bs.POSITION_QTY,
        bs.AVG_COST,
        bs.MARKET_VALUE,
        bs.UNREALIZED_PNL
    FROM MIP.LIVE.BROKER_SNAPSHOTS bs
    INNER JOIN latest_snapshot ls
        ON ls.IBKR_ACCOUNT_ID = bs.IBKR_ACCOUNT_ID
       AND ls.LATEST_TS       = bs.SNAPSHOT_TS
    WHERE bs.SNAPSHOT_TYPE = 'POSITION'
      AND COALESCE(bs.POSITION_QTY, 0) <> 0
),
/*
  Fallback when no LIVE_ORDERS_FILL ENTRY lineage is available for an
  open broker position: anchor ENTRY_TS to the first broker snapshot
  where the position was non-zero in the current contiguous holding
  period (i.e., min(SNAPSHOT_TS) > most-recent zero snapshot, if any).
  Gives operator-meaningful DAYS_HELD without re-introducing sim or
  horizon semantics.
*/
broker_history AS (
    SELECT
        IBKR_ACCOUNT_ID,
        UPPER(SYMBOL)                  AS SYMBOL,
        SNAPSHOT_TS,
        COALESCE(POSITION_QTY, 0)      AS POSITION_QTY
    FROM MIP.LIVE.BROKER_SNAPSHOTS
    WHERE SNAPSHOT_TYPE = 'POSITION'
      AND SYMBOL IS NOT NULL
),
last_zero AS (
    SELECT
        IBKR_ACCOUNT_ID,
        SYMBOL,
        MAX(SNAPSHOT_TS) AS LAST_ZERO_TS
    FROM broker_history
    WHERE POSITION_QTY = 0
    GROUP BY IBKR_ACCOUNT_ID, SYMBOL
),
broker_first_seen AS (
    SELECT
        bh.IBKR_ACCOUNT_ID,
        bh.SYMBOL,
        MIN(bh.SNAPSHOT_TS) AS FIRST_NONZERO_TS
    FROM broker_history bh
    LEFT JOIN last_zero lz
        ON lz.IBKR_ACCOUNT_ID = bh.IBKR_ACCOUNT_ID
       AND lz.SYMBOL          = bh.SYMBOL
    WHERE bh.POSITION_QTY <> 0
      AND (lz.LAST_ZERO_TS IS NULL OR bh.SNAPSHOT_TS > lz.LAST_ZERO_TS)
    GROUP BY bh.IBKR_ACCOUNT_ID, bh.SYMBOL
),
portfolio_map AS (
    SELECT
        PORTFOLIO_ID,
        IBKR_ACCOUNT_ID,
        ADAPTER_MODE,
        BASE_CURRENCY
    FROM MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    WHERE IS_ACTIVE = TRUE
),
closed_actions AS (
    SELECT DISTINCT ENTRY_ACTION_ID
    FROM MIP.LIVE.TRADE_CLOSEOUT
    WHERE ENTRY_ACTION_ID IS NOT NULL
),
open_entry_fills AS (
    SELECT
        la.PORTFOLIO_ID,
        UPPER(la.SYMBOL)        AS SYMBOL,
        la.PROPOSAL_ID,
        lo.ACTION_ID,
        lo.FILLED_AT,
        lo.AVG_FILL_PRICE,
        lo.QTY_FILLED
    FROM MIP.LIVE.LIVE_ORDERS lo
    INNER JOIN MIP.LIVE.LIVE_ACTIONS la
        ON la.ACTION_ID = lo.ACTION_ID
    LEFT JOIN closed_actions ca
        ON ca.ENTRY_ACTION_ID = lo.ACTION_ID
    WHERE lo.FILLED_AT IS NOT NULL
      AND COALESCE(lo.QTY_FILLED, 0) > 0
      AND NOT REGEXP_LIKE(COALESCE(lo.IDEMPOTENCY_KEY, ''), ':(TP|SL|tp|sl)$')
      AND UPPER(
              COALESCE(
                  lo.ACTION_INTENT,
                  la.ACTION_INTENT,
                  IFF(UPPER(COALESCE(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY')
              )
          ) = 'ENTRY'
      AND ca.ENTRY_ACTION_ID IS NULL
),
earliest_open_entry AS (
    SELECT
        PORTFOLIO_ID,
        SYMBOL,
        MIN(FILLED_AT)                           AS ENTRY_TS,
        MIN_BY(AVG_FILL_PRICE, FILLED_AT)        AS ENTRY_FILL_PRICE,
        MIN_BY(PROPOSAL_ID, FILLED_AT)           AS PROPOSAL_ID
    FROM open_entry_fills
    GROUP BY PORTFOLIO_ID, SYMBOL
),
joined AS (
    SELECT
        pm.PORTFOLIO_ID,
        bo.IBKR_ACCOUNT_ID,
        bo.SYMBOL,
        'STOCK'::VARCHAR(10) AS MARKET_TYPE,
        1440                 AS INTERVAL_MINUTES,
        COALESCE(eo.ENTRY_TS, bfs.FIRST_NONZERO_TS, bo.SNAPSHOT_TS)             AS ENTRY_TS,
        COALESCE(eo.ENTRY_TS::DATE, bfs.FIRST_NONZERO_TS::DATE, bo.SNAPSHOT_TS::DATE) AS ENTRY_DATE,
        COALESCE(eo.ENTRY_FILL_PRICE, bo.AVG_COST)        AS ENTRY_PRICE,
        bo.POSITION_QTY      AS QUANTITY,
        bo.AVG_COST,
        bo.MARKET_VALUE,
        bo.UNREALIZED_PNL,
        eo.PROPOSAL_ID,
        bo.SNAPSHOT_TS       AS AS_OF_TS,
        bo.SECURITY_TYPE,
        bo.CURRENCY,
        pm.ADAPTER_MODE,
        CASE
            WHEN eo.ENTRY_TS IS NOT NULL          THEN 'LIVE_ORDERS_FILL'
            WHEN bfs.FIRST_NONZERO_TS IS NOT NULL THEN 'BROKER_SNAPSHOT_FIRST_SEEN'
            ELSE 'BROKER_SNAPSHOT_FALLBACK'
        END                  AS ENTRY_SOURCE,
        TRUE                 AS IS_OPEN
    FROM broker_open bo
    INNER JOIN portfolio_map pm
        ON pm.IBKR_ACCOUNT_ID = bo.IBKR_ACCOUNT_ID
    LEFT JOIN earliest_open_entry eo
        ON eo.PORTFOLIO_ID = pm.PORTFOLIO_ID
       AND eo.SYMBOL       = bo.SYMBOL
    LEFT JOIN broker_first_seen bfs
        ON bfs.IBKR_ACCOUNT_ID = bo.IBKR_ACCOUNT_ID
       AND bfs.SYMBOL          = bo.SYMBOL
)
SELECT
    PORTFOLIO_ID,
    IBKR_ACCOUNT_ID,
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    ENTRY_TS,
    ENTRY_DATE,
    ENTRY_PRICE,
    QUANTITY,
    AVG_COST,
    MARKET_VALUE,
    UNREALIZED_PNL,
    PROPOSAL_ID,
    AS_OF_TS,
    SECURITY_TYPE,
    CURRENCY,
    ADAPTER_MODE,
    ENTRY_SOURCE,
    IS_OPEN,
    /* Canonical operator-truth episode key. Matches the hash produced by
       SP_RUN_DAILY_POSITION_VERDICT so MART latest views can inner-join on
       liveness without surfacing stale sim-anchored verdict rows. */
    SHA2(
        PORTFOLIO_ID::STRING || '|'
        || COALESCE(IBKR_ACCOUNT_ID, 'NO_ACCOUNT') || '|'
        || SYMBOL || '|'
        || TO_CHAR(ENTRY_DATE, 'YYYY-MM-DD')
    )                                          AS POSITION_EPISODE_KEY,
    COUNT(*) OVER (PARTITION BY PORTFOLIO_ID) AS OPEN_POSITIONS
FROM joined;

/* ================================================================
   Grants - canonical live source for the UI API role and admin role.
   ================================================================ */
GRANT SELECT ON VIEW MIP.MART.V_LIVE_OPEN_POSITIONS TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT ON VIEW MIP.MART.V_LIVE_OPEN_POSITIONS TO ROLE MIP_UI_API_ROLE;
