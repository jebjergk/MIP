-- 362_vol_exp_bootstrap_run_log.sql
-- Purpose: Run logging for VOL_EXP backfill/bootstrap flows.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.VOL_EXP_BOOTSTRAP_RUN_LOG (
    RUN_ID                 string        not null,
    STEP_NAME              string        not null, -- BACKFILL_DAILY / TRAINING_REPLAY
    SYMBOL_COHORT          string        not null,
    MARKET_TYPE            string,
    START_DATE             date,
    END_DATE               date,
    STARTED_AT             timestamp_ntz default current_timestamp(),
    FINISHED_AT            timestamp_ntz,
    STATUS                 string,       -- RUNNING / SUCCESS / SUCCESS_WITH_SKIPS / FAIL
    SYMBOLS_PROCESSED      number,
    BARS_LOADED_COUNT      number,
    SIGNALS_CREATED_COUNT  number,
    OUTCOMES_COMPUTED_COUNT number,
    TRUST_ROWS_UPDATED_COUNT number,
    FAILURES               variant,
    DETAILS                variant,
    constraint PK_VOL_EXP_BOOTSTRAP_RUN_LOG primary key (RUN_ID, STEP_NAME)
);

create table if not exists MIP.APP.VOL_EXP_BOOTSTRAP_SYMBOL_LOG (
    RUN_ID                 string        not null,
    STEP_NAME              string        not null,
    SYMBOL                 string        not null,
    MARKET_TYPE            string        not null,
    STARTED_AT             timestamp_ntz default current_timestamp(),
    FINISHED_AT            timestamp_ntz,
    STATUS                 string,
    BARS_LOADED_COUNT      number,
    SIGNALS_CREATED_COUNT  number,
    OUTCOMES_COMPUTED_COUNT number,
    TRUST_ROWS_UPDATED_COUNT number,
    ERROR_MESSAGE          string,
    DETAILS                variant
);

