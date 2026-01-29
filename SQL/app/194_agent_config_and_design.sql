-- 194_agent_config_and_design.sql
-- [A0] Agent v0 design constants + idempotent plumbing.
-- Conventions: agent name AGENT_V0_MORNING_BRIEF; determinism key (as_of_ts, signal_run_id, agent_name);
-- output schema MIP.AGENT_OUT; only writes: MIP.AGENT_OUT.* and MIP.APP.MIP_AUDIT_LOG.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ------------------------------------------------------------------------------
-- AGENT_CONFIG: per-agent config (min_n_signals, top_n, ranking_formula, enabled)
-- ------------------------------------------------------------------------------
create table if not exists MIP.APP.AGENT_CONFIG (
    AGENT_NAME       varchar(128)  not null,
    MIN_N_SIGNALS    number        not null default 20,
    TOP_N_PATTERNS   number        not null default 5,
    TOP_N_CANDIDATES number        not null default 5,
    RANKING_FORMULA  varchar(512)  not null default 'HIT_RATE_SUCCESS * AVG_RETURN_SUCCESS',
    ENABLED          boolean       not null default true,
    UPDATED_AT       timestamp_ntz default current_timestamp(),
    constraint PK_AGENT_CONFIG primary key (AGENT_NAME)
);

-- Defaults for AGENT_V0_MORNING_BRIEF
merge into MIP.APP.AGENT_CONFIG t
using (
    select
        'AGENT_V0_MORNING_BRIEF' as AGENT_NAME,
        20 as MIN_N_SIGNALS,
        5 as TOP_N_PATTERNS,
        5 as TOP_N_CANDIDATES,
        'HIT_RATE_SUCCESS * AVG_RETURN_SUCCESS' as RANKING_FORMULA,
        true as ENABLED
) s
on t.AGENT_NAME = s.AGENT_NAME
when not matched then
    insert (AGENT_NAME, MIN_N_SIGNALS, TOP_N_PATTERNS, TOP_N_CANDIDATES, RANKING_FORMULA, ENABLED)
    values (s.AGENT_NAME, s.MIN_N_SIGNALS, s.TOP_N_PATTERNS, s.TOP_N_CANDIDATES, s.RANKING_FORMULA, s.ENABLED)
when matched then
    update set
        MIN_N_SIGNALS = s.MIN_N_SIGNALS,
        TOP_N_PATTERNS = s.TOP_N_PATTERNS,
        TOP_N_CANDIDATES = s.TOP_N_CANDIDATES,
        RANKING_FORMULA = s.RANKING_FORMULA,
        ENABLED = s.ENABLED,
        UPDATED_AT = current_timestamp();
