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
    AGENT_NAME              varchar(128)  not null,
    MIN_N_SIGNALS           number        not null default 20,
    MIN_N_SIGNALS_BOOTSTRAP number        null default 5,   -- bootstrap: allow LOW-confidence candidates with N_SIGNALS >= this
    TOP_N_PATTERNS          number        not null default 5,
    TOP_N_CANDIDATES        number        not null default 5,
    RANKING_FORMULA         varchar(512)  not null default 'HIT_RATE_SUCCESS * AVG_RETURN_SUCCESS',
    RANKING_FORMULA_TYPE    varchar(64)   not null default 'HIT_RATE_AVG_RETURN',  -- enum: HIT_RATE_AVG_RETURN | SHARPE_LIKE
    ENABLED                 boolean       not null default true,
    UPDATED_AT              timestamp_ntz default current_timestamp(),
    constraint PK_AGENT_CONFIG primary key (AGENT_NAME)
);
-- Backward compat: add columns if missing (existing seed unchanged)
alter table MIP.APP.AGENT_CONFIG add column if not exists RANKING_FORMULA_TYPE varchar(64) default 'HIT_RATE_AVG_RETURN';
alter table MIP.APP.AGENT_CONFIG add column if not exists MIN_N_SIGNALS_BOOTSTRAP number default 5;
update MIP.APP.AGENT_CONFIG set RANKING_FORMULA_TYPE = 'HIT_RATE_AVG_RETURN' where RANKING_FORMULA_TYPE is null;
update MIP.APP.AGENT_CONFIG set MIN_N_SIGNALS_BOOTSTRAP = 5 where MIN_N_SIGNALS_BOOTSTRAP is null;

-- Defaults for AGENT_V0_MORNING_BRIEF (RANKING_FORMULA kept for display; RANKING_FORMULA_TYPE for branch; bootstrap 5)
merge into MIP.APP.AGENT_CONFIG t
using (
    select
        'AGENT_V0_MORNING_BRIEF' as AGENT_NAME,
        20 as MIN_N_SIGNALS,
        5 as MIN_N_SIGNALS_BOOTSTRAP,
        5 as TOP_N_PATTERNS,
        5 as TOP_N_CANDIDATES,
        'HIT_RATE_SUCCESS * AVG_RETURN_SUCCESS' as RANKING_FORMULA,
        'HIT_RATE_AVG_RETURN' as RANKING_FORMULA_TYPE,
        true as ENABLED
) s
on t.AGENT_NAME = s.AGENT_NAME
when not matched then
    insert (AGENT_NAME, MIN_N_SIGNALS, MIN_N_SIGNALS_BOOTSTRAP, TOP_N_PATTERNS, TOP_N_CANDIDATES, RANKING_FORMULA, RANKING_FORMULA_TYPE, ENABLED)
    values (s.AGENT_NAME, s.MIN_N_SIGNALS, s.MIN_N_SIGNALS_BOOTSTRAP, s.TOP_N_PATTERNS, s.TOP_N_CANDIDATES, s.RANKING_FORMULA, s.RANKING_FORMULA_TYPE, s.ENABLED)
when matched then
    update set
        MIN_N_SIGNALS = s.MIN_N_SIGNALS,
        MIN_N_SIGNALS_BOOTSTRAP = coalesce(t.MIN_N_SIGNALS_BOOTSTRAP, s.MIN_N_SIGNALS_BOOTSTRAP),
        TOP_N_PATTERNS = s.TOP_N_PATTERNS,
        TOP_N_CANDIDATES = s.TOP_N_CANDIDATES,
        RANKING_FORMULA = s.RANKING_FORMULA,
        RANKING_FORMULA_TYPE = coalesce(t.RANKING_FORMULA_TYPE, s.RANKING_FORMULA_TYPE),
        ENABLED = s.ENABLED,
        UPDATED_AT = current_timestamp();
