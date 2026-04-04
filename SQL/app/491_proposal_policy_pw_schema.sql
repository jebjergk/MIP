-- 491_proposal_policy_pw_schema.sql
-- Versioned proposal policy manifest + rules, ORDER_PROPOSALS audit columns for
-- autonomous proposals + Parallel Worlds soft overlay (see SP_AGENT_PROPOSE_TRADES).

use role MIP_ADMIN_ROLE;
use database MIP;

-- ---------------------------------------------------------------------------
-- Policy manifest (one active default row drives SP_AGENT_PROPOSE_TRADES)
-- ---------------------------------------------------------------------------
create table if not exists MIP.APP.PROPOSAL_POLICY_MANIFEST (
    POLICY_VERSION varchar(32) not null,
    EFFECTIVE_FROM timestamp_ntz not null default current_timestamp(),
    IS_ACTIVE      boolean       not null default true,
    IS_DEFAULT     boolean       not null default false,
    DESCRIPTION    varchar(4096),
    constraint PK_PROPOSAL_POLICY_MANIFEST primary key (POLICY_VERSION)
);

-- Rules: PATTERN_FAMILY matches PATTERN_DEFINITION.PATTERN_TYPE.
-- MR_DIRECTION: '*' for non-MEAN_REVERSION patterns; BULLISH / BEARISH for MEAN_REVERSION only.
create table if not exists MIP.APP.PROPOSAL_POLICY_RULE (
    POLICY_VERSION  varchar(32) not null,
    PATTERN_FAMILY  varchar(64) not null,
    MR_DIRECTION    varchar(16) not null,
    IS_ELIGIBLE     boolean     not null,
    NOTES           varchar(1024),
    constraint PK_PROPOSAL_POLICY_RULE primary key (POLICY_VERSION, PATTERN_FAMILY, MR_DIRECTION)
);

-- Seed v1: matches legacy 188 behavior (MOMENTUM + MR BULLISH only; other families explicit OFF)
merge into MIP.APP.PROPOSAL_POLICY_MANIFEST t
using (
    select
        '2026_04_03_V1' as POLICY_VERSION,
        current_timestamp() as EFFECTIVE_FROM,
        true as IS_ACTIVE,
        true as IS_DEFAULT,
        'Initial explicit policy: same eligibility as pre-policy 188 (MOMENTUM all, MEAN_REVERSION BULLISH only). ORB PULLBACK BEARISH_MOM off.' as DESCRIPTION
) s
on t.POLICY_VERSION = s.POLICY_VERSION
when matched then update set
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.IS_DEFAULT = s.IS_DEFAULT,
    t.DESCRIPTION = s.DESCRIPTION
when not matched then insert (POLICY_VERSION, EFFECTIVE_FROM, IS_ACTIVE, IS_DEFAULT, DESCRIPTION)
values (s.POLICY_VERSION, s.EFFECTIVE_FROM, s.IS_ACTIVE, s.IS_DEFAULT, s.DESCRIPTION);

-- Only one default: clear others then set seed default
update MIP.APP.PROPOSAL_POLICY_MANIFEST set IS_DEFAULT = false where POLICY_VERSION <> '2026_04_03_V1';
update MIP.APP.PROPOSAL_POLICY_MANIFEST set IS_DEFAULT = true where POLICY_VERSION = '2026_04_03_V1';

merge into MIP.APP.PROPOSAL_POLICY_RULE t
using (
    select column1 as pv, column2 as fam, column3 as mrd, column4 as ok, column5 as notes
    from values
        ('2026_04_03_V1', 'MOMENTUM', '*', true, 'All momentum directions per pattern definition'),
        ('2026_04_03_V1', 'MEAN_REVERSION', 'BULLISH', true, 'Legacy: bullish MR only'),
        ('2026_04_03_V1', 'MEAN_REVERSION', 'BEARISH', false, 'Explicitly off in v1 (legacy)'),
        ('2026_04_03_V1', 'MEAN_REVERSION', 'UNKNOWN', false, 'Unknown direction excluded'),
        ('2026_04_03_V1', 'BEARISH_MOMENTUM', '*', false, 'Off until explicitly enabled'),
        ('2026_04_03_V1', 'ORB', '*', false, 'Off until explicitly enabled'),
        ('2026_04_03_V1', 'PULLBACK_CONTINUATION', '*', false, 'Off until explicitly enabled')
) s(pv, fam, mrd, ok, notes)
on t.POLICY_VERSION = s.pv and t.PATTERN_FAMILY = s.fam and t.MR_DIRECTION = s.mrd
when matched then update set
    t.IS_ELIGIBLE = s.ok,
    t.NOTES = s.notes
when not matched then insert (POLICY_VERSION, PATTERN_FAMILY, MR_DIRECTION, IS_ELIGIBLE, NOTES)
values (s.pv, s.fam, s.mrd, s.ok, s.notes);

-- ORDER_PROPOSALS additive columns
alter table if exists MIP.AGENT_OUT.ORDER_PROPOSALS
    add column if not exists PROPOSAL_POLICY_VERSION varchar(32);

alter table if exists MIP.AGENT_OUT.ORDER_PROPOSALS
    add column if not exists PROPOSAL_DIAGNOSTICS variant;

alter table if exists MIP.AGENT_OUT.ORDER_PROPOSALS
    add column if not exists PW_ENRICHMENT variant;
