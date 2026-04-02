-- Lifecycle reconstruction for closed trades with entry intelligence (Phase 4).
-- Deploy with migrations/phase4_trade_closeout_hardening.sql or run this file after TRADE_CLOSEOUT columns exist.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.LIVE.V_ENTRY_INTEL_LIFECYCLE_RECONSTRUCTION as
select
    tc.CLOSEOUT_ID,
    tc.ENTRY_ACTION_ID,
    tc.EXIT_ACTION_ID,
    tc.SNAPSHOT_ID,
    tc.PROPOSAL_ID,
    tc.SYMBOL,
    tc.EXIT_TYPE,
    tc.ENTRY_TS,
    tc.EXIT_TS,
    tc.HOLDING_PERIOD_SEC,
    tc.REALIZED_RETURN_PCT,
    tc.REALIZED_SIZE,
    tc.REALIZED_PNL,
    tc.FROZEN_ENTRY_EXPECTATION,
    tc.ALIGNMENT_JSON,
    la_in.SIDE as ENTRY_SIDE,
    la_in.COMMITTEE_RUN_ID as ENTRY_COMMITTEE_RUN_ID,
    la_in.STATUS as ENTRY_STATUS,
    la_ex.ACTION_INTENT as EXIT_ACTION_INTENT,
    la_ex.STATUS as EXIT_STATUS,
    e.ALPHA_SPEC as SNAPSHOT_ALPHA_SPEC,
    e.WORLDS_SPEC as SNAPSHOT_WORLDS_SPEC,
    e.SOURCE_VERSION as SNAPSHOT_SOURCE_VERSION,
    e.EIS_VERSION as SNAPSHOT_EIS_VERSION,
    cv.VERDICT_JSON as COMMITTEE_VERDICT_JSON,
    cv.RECOMMENDATION as COMMITTEE_RECOMMENDATION
from MIP.LIVE.TRADE_CLOSEOUT tc
left join MIP.LIVE.LIVE_ACTIONS la_in
    on la_in.ACTION_ID = tc.ENTRY_ACTION_ID
left join MIP.LIVE.LIVE_ACTIONS la_ex
    on la_ex.ACTION_ID = tc.EXIT_ACTION_ID
left join MIP.LIVE.ENTRY_INTEL_SNAPSHOT e
    on e.SNAPSHOT_ID = tc.SNAPSHOT_ID
left join MIP.LIVE.COMMITTEE_VERDICT cv
    on cv.RUN_ID = la_in.COMMITTEE_RUN_ID;
