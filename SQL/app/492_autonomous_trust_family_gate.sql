-- 492_autonomous_trust_family_gate.sql
-- Config-driven participation + thresholds for the AUTONOMOUS proposal trust slice only.
-- Training/UI may still use V_TRUSTED_PATTERN_HORIZONS (stricter global bar).
-- See MIP/docs/proposal_trust_correction_spec.md — family-level gate is structural participation,
-- not the sole statistical veto for long MOMENTUM; symbol-local gates in SP_AGENT_PROPOSE_TRADES remain decisive.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.AUTONOMOUS_TRUST_FAMILY_GATE (
    PATTERN_TYPE     varchar(64) not null,
    MARKET_TYPE      varchar(32) not null,
    INTERVAL_MINUTES number(38,0) not null,
    MIN_HIT_RATE     float        not null,
    MIN_AVG_RETURN   float        not null,
    NOTES            varchar(2048),
    constraint PK_AUTONOMOUS_TRUST_FAMILY_GATE primary key (PATTERN_TYPE, MARKET_TYPE, INTERVAL_MINUTES)
);

-- Primary long momentum: relaxed hit-rate vs global 0.55 so patterns 2/3 can participate.
-- MEAN_REVERSION and BEARISH_MOMENTUM intentionally omitted = excluded from autonomous trust slice for these keys.
merge into MIP.APP.AUTONOMOUS_TRUST_FAMILY_GATE t
using (
    select column1 as pt, column2 as mt, column3 as im, column4 as mhr, column5 as mar, column6 as notes
    from values
        ('MOMENTUM', 'STOCK', 1440, 0.52, 0.0005, 'Primary long entry, symbol-local gate in SP_AGENT_PROPOSE_TRADES'),
        ('MOMENTUM', 'ETF', 1440, 0.52, 0.0005, 'Parallel daily momentum path'),
        ('MOMENTUM', 'FX', 1440, 0.52, 0.0005, 'Parallel daily momentum path')
) s(pt, mt, im, mhr, mar, notes)
on t.PATTERN_TYPE = s.pt and t.MARKET_TYPE = s.mt and t.INTERVAL_MINUTES = s.im
when matched then update set
    t.MIN_HIT_RATE = s.mhr,
    t.MIN_AVG_RETURN = s.mar,
    t.NOTES = s.notes
when not matched then insert (PATTERN_TYPE, MARKET_TYPE, INTERVAL_MINUTES, MIN_HIT_RATE, MIN_AVG_RETURN, NOTES)
values (s.pt, s.mt, s.im, s.mhr, s.mar, s.notes);
