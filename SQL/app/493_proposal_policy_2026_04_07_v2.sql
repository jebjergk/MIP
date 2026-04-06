-- 493_proposal_policy_2026_04_07_v2.sql
-- Autonomous proposal policy v2: MOMENTUM primary; MEAN_REVERSION support-only (no autonomous origination);
-- BEARISH_MOMENTUM off until explicitly enabled. See MIP/docs/proposal_trust_correction_spec.md.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.PROPOSAL_POLICY_MANIFEST t
using (
    select
        '2026_04_07_V2' as POLICY_VERSION,
        current_timestamp() as EFFECTIVE_FROM,
        true as IS_ACTIVE,
        true as IS_DEFAULT,
        'Momentum primary for autonomous proposals, MR support-only (not autonomous), bearish momentum off.' as DESCRIPTION
) s
on t.POLICY_VERSION = s.POLICY_VERSION
when matched then update set
    t.EFFECTIVE_FROM = s.EFFECTIVE_FROM,
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.IS_DEFAULT = s.IS_DEFAULT,
    t.DESCRIPTION = s.DESCRIPTION
when not matched then insert (POLICY_VERSION, EFFECTIVE_FROM, IS_ACTIVE, IS_DEFAULT, DESCRIPTION)
values (s.POLICY_VERSION, s.EFFECTIVE_FROM, s.IS_ACTIVE, s.IS_DEFAULT, s.DESCRIPTION);

update MIP.APP.PROPOSAL_POLICY_MANIFEST set IS_DEFAULT = false where POLICY_VERSION <> '2026_04_07_V2';
update MIP.APP.PROPOSAL_POLICY_MANIFEST set IS_DEFAULT = true where POLICY_VERSION = '2026_04_07_V2';

merge into MIP.APP.PROPOSAL_POLICY_RULE t
using (
    select column1 as pv, column2 as fam, column3 as mrd, column4 as ok, column5 as notes
    from values
        ('2026_04_07_V2', 'MOMENTUM', '*', true, 'Primary long entry family for autonomous proposals'),
        ('2026_04_07_V2', 'MEAN_REVERSION', 'BULLISH', false, 'Support/context only — not autonomous proposal-originating'),
        ('2026_04_07_V2', 'MEAN_REVERSION', 'BEARISH', false, 'Support/context only'),
        ('2026_04_07_V2', 'MEAN_REVERSION', 'UNKNOWN', false, 'Unknown direction excluded'),
        ('2026_04_07_V2', 'BEARISH_MOMENTUM', '*', false, 'Off until explicitly enabled'),
        ('2026_04_07_V2', 'ORB', '*', false, 'Off until explicitly enabled'),
        ('2026_04_07_V2', 'PULLBACK_CONTINUATION', '*', false, 'Off until explicitly enabled')
) s(pv, fam, mrd, ok, notes)
on t.POLICY_VERSION = s.pv and t.PATTERN_FAMILY = s.fam and t.MR_DIRECTION = s.mrd
when matched then update set
    t.IS_ELIGIBLE = s.ok,
    t.NOTES = s.notes
when not matched then insert (POLICY_VERSION, PATTERN_FAMILY, MR_DIRECTION, IS_ELIGIBLE, NOTES)
values (s.pv, s.fam, s.mrd, s.ok, s.notes);
