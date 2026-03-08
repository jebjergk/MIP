use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.LIVE.COMMITTEE_RUN (
    RUN_ID           string        not null,
    ACTION_ID        string        not null,
    PORTFOLIO_ID     number        not null,
    STATUS           string        not null, -- RUNNING | COMPLETED | FAILED
    MODEL_NAME       string,
    STARTED_AT       timestamp_ntz not null default current_timestamp(),
    COMPLETED_AT     timestamp_ntz,
    DETAILS          variant,
    constraint PK_COMMITTEE_RUN primary key (RUN_ID)
);

create table if not exists MIP.LIVE.COMMITTEE_ROLE_OUTPUT (
    RUN_ID           string        not null,
    ROLE_NAME        string        not null,
    STANCE           string        not null, -- SUPPORT | CONDITIONAL | BLOCK
    CONFIDENCE       number(9,6),
    SUMMARY          string,
    OUTPUT_JSON      variant,
    CREATED_AT       timestamp_ntz not null default current_timestamp(),
    constraint PK_COMMITTEE_ROLE_OUTPUT primary key (RUN_ID, ROLE_NAME)
);

create table if not exists MIP.LIVE.COMMITTEE_VERDICT (
    RUN_ID           string        not null,
    ACTION_ID        string        not null,
    PORTFOLIO_ID     number        not null,
    RECOMMENDATION   string        not null, -- PROCEED | PROCEED_REDUCED | BLOCK
    SIZE_FACTOR      number(9,6),
    CONFIDENCE       number(9,6),
    IS_BLOCKED       boolean,
    REASON_CODES     variant,
    VERDICT_JSON     variant,
    CREATED_AT       timestamp_ntz not null default current_timestamp(),
    constraint PK_COMMITTEE_VERDICT primary key (RUN_ID)
);

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists COMMITTEE_REQUIRED boolean default true;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists COMMITTEE_STATUS string;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists COMMITTEE_RUN_ID string;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists COMMITTEE_COMPLETED_TS timestamp_ntz;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists COMMITTEE_VERDICT string;

