/* ================================================================
   543_shadow_trade_linkage.sql
   Phase 1 dual-hearing — durable executed-vs-shadow linkage.

   One row is written by the REAL execute path the moment IBKR returns
   a pending-order ack. The shadow board never writes to this table.
   Purpose: enable later real-vs-shadow comparison and chart overlays.
   ================================================================ */

USE DATABASE MIP;
USE SCHEMA APP;

CREATE TABLE IF NOT EXISTS MIP.APP.SHADOW_TRADE_LINKAGE (
    LINK_ID                  NUMBER        AUTOINCREMENT START 1 INCREMENT 1
                                            ORDER PRIMARY KEY,
    HEARING_ID               VARCHAR(36)   NOT NULL,
    SNAPSHOT_ID              NUMBER,
    EVIDENCE_PACK_HASH       VARCHAR(64),
    PROPOSAL_ID              NUMBER,
    REAL_ACTION_ID           VARCHAR(64)   NOT NULL,
    REAL_COMMITTEE_RUN_ID    VARCHAR(64),
    BROKER_ORDER_IDS         VARIANT,
    REAL_TRADE_CONFIG_JSON   VARIANT,
    SHADOW_SESSION_ID        VARCHAR(36),
    SHADOW_STANCE            VARCHAR(20),
    SHADOW_CONFIDENCE        FLOAT,
    SHADOW_TRADE_CONFIG_JSON VARIANT,
    SHADOW_STATUS            VARCHAR(20),
    SHADOW_NOTES             VARCHAR(500),
    REAL_EXECUTION_TS        TIMESTAMP_NTZ,
    SHADOW_COMPLETED_AT      TIMESTAMP_NTZ,
    CREATED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE MIP.APP.SHADOW_TRADE_LINKAGE IS
    'Phase 1 dual-hearing: durable real-trade vs shadow-trade linkage. '
    'Written by the REAL execute path at the moment IBKR pending-order ack returns. '
    'Shadow code never writes here. SHADOW_SESSION_ID may be NULL if the shadow '
    'board had not completed by submit time (recorded in SHADOW_NOTES).';

-- Idempotent guard: at most one linkage per (REAL_ACTION_ID). The execute
-- path may be retried; we only keep the first successful write.
CREATE OR REPLACE PROCEDURE MIP.APP.SHADOW_TRADE_LINKAGE_DEDUPE()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    deleted NUMBER DEFAULT 0;
BEGIN
    DELETE FROM MIP.APP.SHADOW_TRADE_LINKAGE
     WHERE LINK_ID NOT IN (
         SELECT MIN(LINK_ID)
           FROM MIP.APP.SHADOW_TRADE_LINKAGE
          GROUP BY REAL_ACTION_ID
     );
    deleted := SQLROWCOUNT;
    RETURN 'deleted=' || deleted::STRING;
END;
$$;

/* ================================================================
   Grants
   ================================================================ */
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_TRADE_LINKAGE TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.SHADOW_TRADE_LINKAGE TO ROLE MIP_UI_API_ROLE;
GRANT USAGE ON PROCEDURE MIP.APP.SHADOW_TRADE_LINKAGE_DEDUPE() TO ROLE MIP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE MIP.APP.SHADOW_TRADE_LINKAGE_DEDUPE() TO ROLE MIP_UI_API_ROLE;
