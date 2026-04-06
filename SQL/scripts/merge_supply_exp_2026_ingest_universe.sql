-- merge_supply_exp_2026_ingest_universe.sql
-- SUPPLY_EXP_2026 — STOCK / 1440 expansion (IB onboarding cohort).
-- Approved tranche 1: 13 core + FCX, NET, CRWD (16 total). Optional later: LUV, FSLR, MRNA, DE (not in this merge).

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.INGEST_UNIVERSE t
using (
    select
        column1 as SYMBOL,
        'STOCK' as MARKET_TYPE,
        1440 as INTERVAL_MINUTES,
        true as IS_ENABLED,
        55 as PRIORITY,
        'SUPPLY_EXP_2026' as SYMBOL_COHORT,
        column2 as NOTES
    from values
        ('MRK',  'SUPPLY_EXP_2026 — Merck, approved 2026-04-05'),
        ('ABBV', 'SUPPLY_EXP_2026 — AbbVie, approved 2026-04-05'),
        ('COP',  'SUPPLY_EXP_2026 — ConocoPhillips, approved 2026-04-05'),
        ('NEE',  'SUPPLY_EXP_2026 — NextEra Energy, approved 2026-04-05'),
        ('QCOM', 'SUPPLY_EXP_2026 — Qualcomm, approved 2026-04-05'),
        ('ORCL', 'SUPPLY_EXP_2026 — Oracle, approved 2026-04-05'),
        ('PANW', 'SUPPLY_EXP_2026 — Palo Alto Networks, approved 2026-04-05'),
        ('SBUX', 'SUPPLY_EXP_2026 — Starbucks, approved 2026-04-05'),
        ('MCD',  'SUPPLY_EXP_2026 — McDonald''s, approved 2026-04-05'),
        ('TGT',  'SUPPLY_EXP_2026 — Target, approved 2026-04-05'),
        ('DAL',  'SUPPLY_EXP_2026 — Delta Air Lines, approved 2026-04-05'),
        ('NUE',  'SUPPLY_EXP_2026 — Nucor, approved 2026-04-05'),
        ('SLB',  'SUPPLY_EXP_2026 — SLB, approved 2026-04-05'),
        ('FCX',  'SUPPLY_EXP_2026 — Freeport-McMoRan, tranche-1 promoted 2026-04-05'),
        ('NET',  'SUPPLY_EXP_2026 — Cloudflare, tranche-1 promoted 2026-04-05'),
        ('CRWD', 'SUPPLY_EXP_2026 — CrowdStrike, tranche-1 promoted 2026-04-05')
) s
on upper(t.SYMBOL) = upper(s.SYMBOL)
and upper(t.MARKET_TYPE) = upper(s.MARKET_TYPE)
and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
when matched then update set
    t.IS_ENABLED = true,
    t.PRIORITY = greatest(coalesce(t.PRIORITY, 0), s.PRIORITY),
    t.SYMBOL_COHORT = s.SYMBOL_COHORT,
    t.NOTES = s.NOTES
when not matched then insert (
    SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, IS_ENABLED, PRIORITY, SYMBOL_COHORT, NOTES
) values (
    s.SYMBOL, s.MARKET_TYPE, s.INTERVAL_MINUTES, s.IS_ENABLED, s.PRIORITY, s.SYMBOL_COHORT, s.NOTES
);
