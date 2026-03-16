-- 392_live_execution_foundation.sql
-- Purpose: Phase 0 live-execution domain foundation.
-- Creates MIP.LIVE schema and additive tables for paper/live broker sync.
-- Does NOT modify simulation or research portfolio tables.

use role MIP_ADMIN_ROLE;
use database MIP;

create schema if not exists MIP.LIVE;

-----------------------------
-- 1) LIVE_PORTFOLIO_CONFIG
-- One row per live portfolio config. Contains optional soft link to
-- simulation portfolio context and operational thresholds.
-----------------------------
create table if not exists MIP.LIVE.LIVE_PORTFOLIO_CONFIG (
    PORTFOLIO_ID                    number        not null,
    SIM_PORTFOLIO_ID                number,
    IBKR_ACCOUNT_ID                 string        not null,
    ADAPTER_MODE                    string        not null default 'PAPER',
    BASE_CURRENCY                   string        not null default 'EUR',
    MAX_POSITIONS                   number,
    MAX_POSITION_PCT                number(18,6),
    CASH_BUFFER_PCT                 number(18,6),
    MAX_SLIPPAGE_PCT                number(18,6),
    VALIDITY_WINDOW_SEC             number        default 14400,
    QUOTE_FRESHNESS_THRESHOLD_SEC   number        default 60,
    SNAPSHOT_FRESHNESS_THRESHOLD_SEC number       default 300,
    DRAWDOWN_STOP_PCT               number(18,6),
    BUST_PCT                        number(18,6),
    COOLDOWN_BARS                   number        default 3,
    DRIFT_STATUS                    string        default 'OK',
    CONFIG_VERSION                  number        default 1,
    IS_ACTIVE                       boolean       default true,
    CREATED_AT                      timestamp_ntz default current_timestamp(),
    UPDATED_AT                      timestamp_ntz default current_timestamp(),
    constraint PK_LIVE_PORTFOLIO_CONFIG primary key (PORTFOLIO_ID)
);

-----------------------------
-- 2) BROKER_EVENT_LEDGER
-- Append-only broker event log (orders, fills, compliance, revalidation).
-----------------------------
create table if not exists MIP.LIVE.BROKER_EVENT_LEDGER (
    EVENT_ID                        string        not null,
    EVENT_TS                        timestamp_ntz not null,
    EVENT_TYPE                      string        not null,
    PORTFOLIO_ID                    number        not null,
    PROPOSAL_ID                     number,
    ACTION_ID                       string,
    IDEMPOTENCY_KEY                 string,
    BROKER_ORDER_ID                 string,
    BROKER_EXEC_ID                  string,
    SYMBOL                          string,
    SIDE                            string,
    QTY                             number(18,8),
    PRICE                           number(18,8),
    COMMISSION                      number(18,8),
    CURRENCY                        string,
    PAYLOAD                         variant,
    INGESTED_AT                     timestamp_ntz default current_timestamp(),
    constraint PK_BROKER_EVENT_LEDGER primary key (EVENT_ID)
);

-----------------------------
-- 3) BROKER_SNAPSHOTS
-- Append-only snapshots pulled from IBKR.
-- This is the operational source used for reconciliation/drift checks.
-----------------------------
create table if not exists MIP.LIVE.BROKER_SNAPSHOTS (
    SNAPSHOT_ROW_ID                 number        autoincrement,
    SNAPSHOT_ID                     string        not null,
    SNAPSHOT_TS                     timestamp_ntz not null,
    SNAPSHOT_TYPE                   string        not null, -- NAV | CASH | POSITION | OPEN_ORDER
    IBKR_ACCOUNT_ID                 string        not null,
    PORTFOLIO_ID                    number,
    SYMBOL                          string,
    BROKER_CON_ID                   number,
    SECURITY_TYPE                   string,
    EXCHANGE                        string,
    CURRENCY                        string,
    POSITION_QTY                    number(18,8),
    AVG_COST                        number(18,8),
    MARKET_VALUE                    number(18,8),
    UNREALIZED_PNL                  number(18,8),
    REALIZED_PNL                    number(18,8),
    CASH_BALANCE                    number(18,8),
    SETTLED_CASH                    number(18,8),
    NET_LIQUIDATION_EUR             number(18,8),
    TOTAL_CASH_EUR                  number(18,8),
    GROSS_POSITION_VALUE_EUR        number(18,8),
    OPEN_ORDER_ID                   string,
    OPEN_ORDER_STATUS               string,
    OPEN_ORDER_QTY                  number(18,8),
    OPEN_ORDER_FILLED               number(18,8),
    OPEN_ORDER_REMAINING            number(18,8),
    OPEN_ORDER_LIMIT_PRICE          number(18,8),
    OPEN_ORDER_AUX_PRICE            number(18,8),
    PAYLOAD                         variant,
    CREATED_AT                      timestamp_ntz default current_timestamp(),
    constraint PK_BROKER_SNAPSHOTS primary key (SNAPSHOT_ROW_ID)
);

-----------------------------
-- 4) SNAPSHOT_SYNC_RUN_LOG
-- Health and audit log for snapshot sync runs.
-----------------------------
create table if not exists MIP.LIVE.SNAPSHOT_SYNC_RUN_LOG (
    RUN_ID                          string        not null,
    STARTED_AT                      timestamp_ntz not null,
    COMPLETED_AT                    timestamp_ntz,
    STATUS                          string        not null, -- RUNNING | SUCCESS | FAILED
    HOST                            string,
    PORT                            number,
    CLIENT_ID                       number,
    IBKR_ACCOUNT_ID                 string,
    PORTFOLIO_ID                    number,
    SNAPSHOT_TS                     timestamp_ntz,
    NAV_ROWS                        number        default 0,
    CASH_ROWS                       number        default 0,
    POSITION_ROWS                   number        default 0,
    OPEN_ORDER_ROWS                 number        default 0,
    ERROR_MESSAGE                   string,
    DETAILS                         variant,
    CREATED_AT                      timestamp_ntz default current_timestamp(),
    UPDATED_AT                      timestamp_ntz default current_timestamp(),
    constraint PK_SNAPSHOT_SYNC_RUN_LOG primary key (RUN_ID)
);

-----------------------------
-- 5) DRIFT_LOG
-----------------------------
create table if not exists MIP.LIVE.DRIFT_LOG (
    DRIFT_ID                        string        not null,
    RECONCILIATION_TS               timestamp_ntz not null,
    PORTFOLIO_ID                    number,
    IBKR_ACCOUNT_ID                 string        not null,
    NAV_DRIFT_PCT                   number(18,8),
    CASH_DRIFT_EUR                  number(18,8),
    POSITION_DRIFT_COUNT            number        default 0,
    DRIFT_DETECTED                  boolean       default false,
    RESOLUTION_TS                   timestamp_ntz,
    RESOLUTION_METHOD               string,
    DETAILS                         variant,
    CREATED_AT                      timestamp_ntz default current_timestamp(),
    constraint PK_DRIFT_LOG primary key (DRIFT_ID)
);

-----------------------------
-- 6) DRAWDOWN_LOG
-----------------------------
create table if not exists MIP.LIVE.DRAWDOWN_LOG (
    LOG_TS                          timestamp_ntz not null,
    PORTFOLIO_ID                    number,
    IBKR_ACCOUNT_ID                 string        not null,
    NAV_EUR                         number(18,8)  not null,
    PEAK_NAV_EUR                    number(18,8),
    DRAWDOWN_PCT                    number(18,8),
    DRAWDOWN_LIMIT_PCT              number(18,8),
    EXECUTION_BLOCKED               boolean       default false,
    DETAILS                         variant,
    CREATED_AT                      timestamp_ntz default current_timestamp(),
    constraint PK_DRAWDOWN_LOG primary key (LOG_TS, IBKR_ACCOUNT_ID)
);

-----------------------------
-- 7) LIVE_ACTIONS (phase-0 shape only)
-----------------------------
create table if not exists MIP.LIVE.LIVE_ACTIONS (
    ACTION_ID                       string        not null,
    PROPOSAL_ID                     number,
    PORTFOLIO_ID                    number        not null,
    SYMBOL                          string,
    SIDE                            string,
    ACTION_INTENT                   string,
    EXIT_TYPE                       string,
    EXIT_REASON                     string,
    PROPOSED_QTY                    number(18,8),
    PROPOSED_PRICE                  number(18,8),
    ASSET_CLASS                     string,
    VALIDITY_WINDOW_END             timestamp_ntz,
    STATUS                          string        not null default 'PROPOSED',
    PM_APPROVED_BY                  string,
    PM_APPROVED_TS                  timestamp_ntz,
    COMPLIANCE_STATUS               string,
    COMPLIANCE_APPROVED_BY          string,
    COMPLIANCE_DECISION_TS          timestamp_ntz,
    COMPLIANCE_NOTES                string,
    COMPLIANCE_REFERENCE_ID         string,
    REVALIDATION_TS                 timestamp_ntz,
    QUOTE_TS                        timestamp_ntz,
    BID                             number(18,8),
    ASK                             number(18,8),
    LAST                            number(18,8),
    ONE_MIN_BAR_TS                  timestamp_ntz,
    ONE_MIN_BAR_CLOSE               number(18,8),
    EXECUTION_PRICE_SOURCE          string, -- ONE_MIN_BAR | QUOTE_FALLBACK
    REVALIDATION_PRICE              number(18,8),
    PRICE_DEVIATION_PCT             number(18,8),
    PRICE_GUARD_RESULT              string,
    EXPOSURE_CHECK_RESULT           string,
    MARKET_OPEN                     boolean,
    SYMBOL_HALTED                   boolean,
    REVALIDATION_STATUS             string,
    REASON_CODES                    variant,
    PARAM_SNAPSHOT                  variant,
    CREATED_AT                      timestamp_ntz default current_timestamp(),
    UPDATED_AT                      timestamp_ntz default current_timestamp(),
    constraint PK_LIVE_ACTIONS primary key (ACTION_ID)
);

-----------------------------
-- 8) LIVE_ORDERS (phase-0 shape only)
-----------------------------
create table if not exists MIP.LIVE.LIVE_ORDERS (
    ORDER_ID                        string        not null,
    ACTION_ID                       string        not null,
    PORTFOLIO_ID                    number        not null,
    IBKR_ACCOUNT_ID                 string        not null,
    IDEMPOTENCY_KEY                 string        not null,
    BROKER_ORDER_ID                 string,
    STATUS                          string        not null default 'SUBMITTED',
    SYMBOL                          string,
    SIDE                            string,
    ACTION_INTENT                   string,
    EXIT_TYPE                       string,
    ORDER_TYPE                      string,
    QTY_ORDERED                     number(18,8),
    LIMIT_PRICE                     number(18,8),
    QTY_FILLED                      number(18,8),
    AVG_FILL_PRICE                  number(18,8),
    TOTAL_COMMISSION                number(18,8),
    SUBMITTED_AT                    timestamp_ntz,
    ACKNOWLEDGED_AT                 timestamp_ntz,
    FILLED_AT                       timestamp_ntz,
    LAST_UPDATED_AT                 timestamp_ntz,
    CREATED_AT                      timestamp_ntz default current_timestamp(),
    constraint PK_LIVE_ORDERS primary key (ORDER_ID),
    constraint UQ_LIVE_ORDERS_IDEMPOTENCY unique (IDEMPOTENCY_KEY)
);

