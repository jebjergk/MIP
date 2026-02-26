# VOL_EXP Bootstrap Runbook

This runbook adds volatile symbols as an additive cohort and bootstraps daily training without modifying existing daily/intraday pipeline procedures.

## 1) Deploy SQL objects

Run in this order:

1. `MIP/SQL/migrations/20260226_add_symbol_cohort_to_ingest_universe.sql`
2. `MIP/SQL/app/361_vol_exp_cohort_seed.sql`
3. `MIP/SQL/app/362_vol_exp_bootstrap_run_log.sql`
4. `MIP/SQL/app/363_sp_backfill_daily_cohort_alphavantage.sql`
5. `MIP/SQL/app/364_sp_bootstrap_generate_recommendations_cohort.sql`
6. `MIP/SQL/app/365_sp_bootstrap_evaluate_recommendations_cohort.sql`
7. `MIP/SQL/app/366_sp_bootstrap_daily_training_for_new_symbols.sql`
8. `MIP/SQL/views/mart/v_symbol_training_readiness.sql`
9. `MIP/SQL/views/mart/v_vol_exp_bootstrap_diagnostics.sql`

## 2) Run bootstrap

### Stocks only (default market type)

```sql
call MIP.APP.SP_BOOTSTRAP_DAILY_TRAINING_FOR_NEW_SYMBOLS(
    null,
    '2025-09-01'::date,
    current_date(),
    'VOL_EXP',
    'STOCK',
    null
);
```

### ETF / FX passes

```sql
call MIP.APP.SP_BOOTSTRAP_DAILY_TRAINING_FOR_NEW_SYMBOLS(
    null,
    '2025-09-01'::date,
    current_date(),
    'VOL_EXP',
    'ETF',
    null
);

call MIP.APP.SP_BOOTSTRAP_DAILY_TRAINING_FOR_NEW_SYMBOLS(
    null,
    '2025-09-01'::date,
    current_date(),
    'VOL_EXP',
    'FX',
    null
);
```

## 3) Verify readiness and diagnostics

```sql
select * from MIP.MART.V_SYMBOL_TRAINING_READINESS where COHORT = 'VOL_EXP' order by SYMBOL;
select * from MIP.MART.V_VOL_EXP_BOOTSTRAP_DIAGNOSTICS order by MARKET_TYPE, SYMBOL;
select * from MIP.APP.VOL_EXP_BOOTSTRAP_RUN_LOG order by STARTED_AT desc;
```

## 4) Smoke checks

Run `MIP/SQL/checks/235_vol_exp_bootstrap_smoke_checks.sql`.

