# SUPPLY_EXP_2026 — IB contract reference and onboarding payload

## Phase intent (approved 2026-04-05)

Universe expansion supports **LONG opportunity density**, **SHORT research supply**, and **overall training evidence**—not SHORT-only optimization.

## 1) IB contract / exchange confirmation

Daily ingest uses `ib_insync` **`Stock(symbol, 'SMART', 'USD')`** in [`cursorfiles/ingest_ibkr_bars.py`](../../cursorfiles/ingest_ibkr_bars.py). IB resolves **SMART** to the primary listing. For operator verification (TWS contract details), use:

| Symbol | Primary listing (US) | Notes |
|--------|----------------------|--------|
| MRK | NYSE | Large pharma |
| ABBV | NYSE | |
| COP | NYSE | |
| NEE | NYSE | |
| QCOM | Nasdaq | |
| ORCL | NYSE | |
| PANW | Nasdaq | |
| SBUX | Nasdaq | |
| MCD | NYSE | |
| TGT | NYSE | |
| DAL | NYSE | |
| NUE | NYSE | |
| SLB | NYSE | |
| FCX | NYSE | |
| NET | NYSE | |
| CRWD | Nasdaq | |

**Action:** Before first ingest, spot-check one symbol in TWS **Contract Details** (SMART/USD) matches expected routing. If IB returns a different primary, document in run notes; no code change required unless contract is wrong.

**Optional-later symbols** (not in first MERGE): LUV (NYSE), FSLR (Nasdaq), MRNA (Nasdaq), DE (NYSE).

## 2) Ingest universe MERGE (Snowflake)

Run [`MIP/SQL/scripts/merge_supply_exp_2026_ingest_universe.sql`](../SQL/scripts/merge_supply_exp_2026_ingest_universe.sql) (same SQL as **Appendix A**), or use the API body below (which also MERGEs `INGEST_UNIVERSE`). Cohort **`SUPPLY_EXP_2026`**, **16** symbols (13 approved + **FCX**, **NET**, **CRWD** promoted; **LUV**, **FSLR**, **MRNA**, **DE** remain optional-later and are **not** in this MERGE).

**Onboarding SQL:** [`MIP/SQL/scripts/call_supply_exp_2026_ib_onboarding.sql`](../SQL/scripts/call_supply_exp_2026_ib_onboarding.sql) — includes `USE SCHEMA APP` (required for `SP_RUN_IB_SYMBOL_ONBOARDING` temp tables).

## 3) Recommended API onboarding body

Call **`POST /manage/ib/onboarding/run`** on the MIP UI API with a live **IB Gateway** session (see [`ib_symbol_onboarding_runbook.md`](ib_symbol_onboarding_runbook.md)):

```json
{
  "symbols": [
    "MRK", "ABBV", "COP", "NEE", "QCOM", "ORCL", "PANW", "SBUX", "MCD", "TGT",
    "DAL", "NUE", "SLB", "FCX", "NET", "CRWD"
  ],
  "market_type": "STOCK",
  "symbol_cohort": "SUPPLY_EXP_2026",
  "start_date": "2025-08-01",
  "end_date": "2026-04-05",
  "auto_activate_if_trusted": true,
  "priority": 55,
  "use_rth": true
}
```

Set **`end_date`** to **current date** on the day you run. **`duration_str`** is derived from the date range for historical bars.

The API then runs **`ingest_ibkr_bars.py`** for historical daily bars and **`SP_RUN_IB_SYMBOL_ONBOARDING`**, which calls **`SP_BOOTSTRAP_DAILY_TRAINING_FOR_NEW_SYMBOLS_IB`** over **`start_date`…`end_date`** for the cohort (returns refresh + generation + evaluation for that scope).

## 4) Alias / display-name fill

Deploy [`MIP/SQL/migrations/20260406_supply_exp_2026_symbol_aliases.sql`](../SQL/migrations/20260406_supply_exp_2026_symbol_aliases.sql) (same as **Appendix B**): `MIP.NEWS.SYMBOL_ALIAS_DICT`, `ALIAS_TYPE = 'COMPANY_NAME'`, so [`GET /reference/symbols`](../apps/mip_ui_api/app/routers/reference.py) and **`formatSymbolLabel`** show names. Includes optional-later four symbols for a future MERGE.

## 5) Bootstrap / back-training (after IB validation)

1. Confirm **Adequate IB history** for each enabled **SUPPLY_EXP_2026** symbol; scoped **`IS_ENABLED = false`** only on failing expansion rows.
2. If onboarding did not complete replay for the full window, **`CALL SP_BOOTSTRAP_DAILY_TRAINING_FOR_NEW_SYMBOLS_IB`** with **`P_SYMBOL_COHORT = 'SUPPLY_EXP_2026'`**, **`P_START_DATE = 2025-08-01`**, **`P_END_DATE = current_date`**.
3. **SHORT research:** when relaxed patterns are ready, **`SP_BACKFILL_SHORT_MOMENTUM`** over **`STOCK`**, **1440**, date range aligned to the phase (see execution plan WS5).

## 6) Training Status cleanup (WS6)

UI workstream: narrow primary grid columns; move per-horizon **`avg_outcome_*`** into expanded row; sticky header + ellipsis (see execution plan WS6 — [`TrainingStatus.jsx`](../apps/mip_ui_web/src/pages/TrainingStatus.jsx)).

## 7) SQL fallback (bars already ingested)

See [`ib_symbol_onboarding_runbook.md`](ib_symbol_onboarding_runbook.md) — `CALL MIP.APP.SP_RUN_IB_SYMBOL_ONBOARDING(...)` with the same symbol array and cohort.

## 8) Post-run validation

- `MIP.APP.IB_SYMBOL_ONBOARDING_SYMBOL_LOG` — per-symbol `FIRST_BAR_DATE` / `LAST_BAR_DATE` / `BAR_COUNT`.
- Phase **Adequate IB history** rules in the execution plan (NYSE session set **`U`**, **`D_HISTORY_FLOOR`**, scoped disable on failing **SUPPLY_EXP_2026** row only).
- Checks: [`MIP/SQL/checks/236_ib_symbol_onboarding_smoke_checks.sql`](../SQL/checks/236_ib_symbol_onboarding_smoke_checks.sql).

---

## Appendix A — `INGEST_UNIVERSE` MERGE (`SUPPLY_EXP_2026`)

```sql
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
```

## Appendix B — `SYMBOL_ALIAS_DICT` COMPANY_NAME merge

```sql
use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.NEWS.SYMBOL_ALIAS_DICT t
using (
    select column1 as SYMBOL, 'STOCK' as MARKET_TYPE, column2 as ALIAS, 'COMPANY_NAME' as ALIAS_TYPE, true as IS_ACTIVE
    from values
        ('MRK',  'MERCK'),
        ('ABBV', 'ABBVIE'),
        ('COP',  'CONOCOPHILLIPS'),
        ('NEE',  'NEXTERA ENERGY'),
        ('QCOM', 'QUALCOMM'),
        ('ORCL', 'ORACLE'),
        ('PANW', 'PALO ALTO NETWORKS'),
        ('SBUX', 'STARBUCKS'),
        ('MCD',  'MCDONALDS'),
        ('TGT',  'TARGET'),
        ('DAL',  'DELTA AIR LINES'),
        ('NUE',  'NUCOR'),
        ('SLB',  'SLB'),
        ('FCX',  'FREEPORT-MCMORAN'),
        ('NET',  'CLOUDFLARE'),
        ('CRWD', 'CROWDSTRIKE'),
        ('LUV',  'SOUTHWEST AIRLINES'),
        ('FSLR', 'FIRST SOLAR'),
        ('MRNA', 'MODERNA'),
        ('DE',   'DEERE')
) s
on t.SYMBOL = s.SYMBOL
and t.MARKET_TYPE = s.MARKET_TYPE
and t.ALIAS = s.ALIAS
when matched then update set
    t.ALIAS_TYPE = s.ALIAS_TYPE,
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (
    SYMBOL, MARKET_TYPE, ALIAS, ALIAS_TYPE, IS_ACTIVE, CREATED_AT, UPDATED_AT
) values (
    s.SYMBOL, s.MARKET_TYPE, s.ALIAS, s.ALIAS_TYPE, s.IS_ACTIVE, current_timestamp(), current_timestamp()
);
```
