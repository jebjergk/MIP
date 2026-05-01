/* ================================================================
   568_phase4_dossier_pack_cache.sql
   Phase 4 Cortex Agentic Proposal Board: per-(run, dossier)
   evidence pack cache used as the tool backing for all PHASE4_*
   agents.

   Mirrors the SHADOW_EVIDENCE_PACK_CACHE pattern. The orchestrator
   builds a pack per dossier from V_PROPOSAL_BOARD_SYMBOL_DOSSIER
   and writes it here BEFORE invoking any agent. Agents call
   GET_PHASE4_DOSSIER_SLICE which reads from this cache only.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

CREATE TABLE IF NOT EXISTS MIP.APP.PROPOSAL_BOARD_DOSSIER_PACK_CACHE (
    RUN_ID        VARCHAR(36)   NOT NULL,
    DOSSIER_ID    NUMBER        NOT NULL,
    SYMBOL        VARCHAR(20)   NOT NULL,
    MARKET_TYPE   VARCHAR(20),
    AS_OF_DATE    DATE          NOT NULL,
    PACK_JSON     VARIANT       NOT NULL,
    PACK_HASH     VARCHAR(64),
    EXPIRES_AT    TIMESTAMP_NTZ NOT NULL,
    CREATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (RUN_ID, DOSSIER_ID)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MIP.APP.PROPOSAL_BOARD_DOSSIER_PACK_CACHE TO ROLE MIP_ADMIN_ROLE;
