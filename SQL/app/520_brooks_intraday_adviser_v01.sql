-- Brooks INTRADAY Adviser V0.1 — session call log (POC)
use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.APP;

create table if not exists MIP.APP.BROOKS_INTRADAY_ADVISER_ATTEMPT (
    ADVISER_ATTEMPT_ID     string        not null,
    RUN_ID                 string        not null,
    SYMBOL                 string        not null,
    TRADING_DATE           date          not null,
    STATUS                 string        not null default 'RUNNING',
    CORPUS_VERSION         string,
    QUERY_TAG              string,
    CONFIG_JSON            variant,
    COST_SUMMARY_JSON      variant,
    CREATED_AT             timestamp_ntz default CURRENT_TIMESTAMP(),
    COMPLETED_AT           timestamp_ntz
);

create table if not exists MIP.APP.BROOKS_INTRADAY_ADVISER_CALL (
    CALL_ID                string        not null,
    ADVISER_ATTEMPT_ID     string        not null,
    CALL_NUMBER            number        not null,
    BAR_TS_NY              timestamp_ntz not null,
    BAR_TS_ET              string,
    WAKE_REASON            string,
    POSITION_STATE         string,
    RETRIEVAL_QUERY        string,
    RAG_CARD_IDS           variant,
    RETRIEVED_CONCEPTS     variant,
    CURRENT_THESIS         string,
    BROOKS_MARKET_STATE    string,
    ACTION                 string,
    WATCH_CONDITIONS       variant,
    CONFIRMATION_CONDITIONS variant,
    INVALIDATION_CONDITIONS variant,
    BROOKS_REASONING_SUMMARY string,
    OBSERVATION_PACKET     variant,
    LLM_MODEL              string,
    LLM_INPUT_TOKENS       number,
    LLM_OUTPUT_TOKENS      number,
    RAG_LATENCY_MS         number,
    LLM_LATENCY_MS         number,
    TOTAL_LATENCY_MS       number,
    CREATED_AT             timestamp_ntz default CURRENT_TIMESTAMP()
);

create table if not exists MIP.APP.BROOKS_INTRADAY_ADVISER_BAR (
    ADVISER_ATTEMPT_ID     string        not null,
    BAR_TS_NY              timestamp_ntz not null,
    BAR_TS_ET              string,
    ADVISER_CALLED         boolean       not null default false,
    CALL_ID                string,
    THESIS_SNAPSHOT        string,
    ACTION_SNAPSHOT        string,
    WATCH_SNAPSHOT         string,
    BAR_NOTE               string,
    SIM_POSITION_QTY       number,
    SIM_POSITION_AVG       float,
    primary key (ADVISER_ATTEMPT_ID, BAR_TS_NY)
);

create table if not exists MIP.APP.BROOKS_INTRADAY_ADVISER_SIM_TRADE (
    TRADE_ID               string        not null,
    ADVISER_ATTEMPT_ID     string        not null,
    SYMBOL                 string        not null,
    ENTRY_TS_NY            timestamp_ntz not null,
    EXIT_TS_NY             timestamp_ntz,
    ENTRY_PRICE            float,
    EXIT_PRICE             float,
    QUANTITY               number,
    STOP_PRICE             float,
    EXIT_REASON            string,
    REALIZED_PNL           float
);
