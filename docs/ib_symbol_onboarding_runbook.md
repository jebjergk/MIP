# IB Symbol Onboarding Runbook

This runbook onboards a batch of IB symbols, ingests daily bars, runs day-by-day bootstrap training from `2025-08-01`, and auto-activates symbols for trade eligibility when trust/readiness gates pass.

## 1) Deploy SQL object

Run:

1. `MIP/SQL/app/410_sp_run_ib_symbol_onboarding.sql`

## 2) API flow (recommended)

Call:

`POST /manage/ib/onboarding/run`

Example body:

```json
{
  "symbols": ["SMCI", "PLTR", "COIN"],
  "market_type": "STOCK",
  "start_date": "2025-08-01",
  "end_date": "2026-03-17",
  "auto_activate_if_trusted": true,
  "priority": 50,
  "use_rth": true
}
```

What it does:

1. Upserts symbols into `MIP.APP.INGEST_UNIVERSE` (daily interval, cohort tag).
2. Ingests daily bars from IB via `cursorfiles/ingest_ibkr_bars.py`.
3. Calls `MIP.APP.SP_RUN_IB_SYMBOL_ONBOARDING(...)` to replay training and persist readiness/activation outcomes.

## 3) SQL fallback (operator)

If you need to run SQL directly after bars are already ingested:

```sql
call MIP.APP.SP_RUN_IB_SYMBOL_ONBOARDING(
    parse_json('["SMCI","PLTR","COIN"]'),
    'STOCK',
    '2025-08-01'::date,
    current_date(),
    true,
    null,
    null,
    50
);
```

## 4) Validate run outputs

```sql
select * from MIP.APP.IB_SYMBOL_ONBOARDING_RUN_LOG order by STARTED_AT desc;
select * from MIP.APP.IB_SYMBOL_ONBOARDING_SYMBOL_LOG order by UPDATED_AT desc;
select * from MIP.APP.IB_SYMBOL_TRADE_ACTIVATION order by UPDATED_AT desc;
```

Or execute smoke checks:

- `MIP/SQL/checks/236_ib_symbol_onboarding_smoke_checks.sql`
