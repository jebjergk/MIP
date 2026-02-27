# Daily Symbol Calibration Rollout (Mode C, No Shadow)

## Scope
- Daily-only (`INTERVAL_MINUTES = 1440`) policy personalization.
- No changes to signal generation logic or core pipeline orchestration.
- Additive versioned artifacts with single-switch activation and rollback.

## Objects Added
- `MIP.APP.DAILY_SYMBOL_CALIBRATION_TRAINED`
- `MIP.APP.DAILY_POLICY_EFFECTIVE_TRAINED`
- `MIP.APP.DAILY_CALIBRATION_EVAL_RUNS`
- `MIP.APP.V_TRAINING_VERSION_CURRENT`
- `MIP.MART.V_DAILY_POLICY_EFFECTIVE_ACTIVE`
- `MIP.APP.SP_RETRAIN_DAILY_POLICY_SYMBOL_CAL(...)`

## Config Keys
- `ENABLE_DAILY_SYMBOL_CALIBRATION`
- `DAILY_POLICY_ACTIVE_TRAINING_VERSION`
- `DAILY_POLICY_BASELINE_VERSION`
- `DAILY_POLICY_CAL_MIN_N`
- `DAILY_POLICY_CAL_MAX_CI_WIDTH`
- `DAILY_POLICY_CAL_MULT_CAP_LO`
- `DAILY_POLICY_CAL_MULT_CAP_HI`
- `DAILY_POLICY_CAL_SHRINK_K`
- `DAILY_POLICY_CAL_MIN_N_HORIZON`
- `DAILY_POLICY_CAL_MAX_CI_WIDTH_HORIZON`

## Run Retrain
```sql
call MIP.APP.SP_RETRAIN_DAILY_POLICY_SYMBOL_CAL(
  'CAL_RUN_20260226_01',
  'DAILY_CAL_V1',
  '2025-09-01'::date,
  current_date(),
  'STOCK',
  null
);
```

## Lite Acceptance Checks
- Run: `MIP/SQL/checks/236_daily_symbol_calibration_lite_checks.sql`
- Focus:
  - Signal invariance
  - Target cap bounds
  - Horizon consistency
  - Multiplier distribution sanity
  - Anti-inflation regression

## Activate / Rollback
Activate:
```sql
update MIP.APP.APP_CONFIG
set CONFIG_VALUE = 'DAILY_CAL_V1', UPDATED_AT = current_timestamp()
where CONFIG_KEY = 'DAILY_POLICY_ACTIVE_TRAINING_VERSION';
```

Rollback:
```sql
update MIP.APP.APP_CONFIG
set CONFIG_VALUE = 'CURRENT', UPDATED_AT = current_timestamp()
where CONFIG_KEY = 'DAILY_POLICY_ACTIVE_TRAINING_VERSION';
```

## Notes
- Decision Console uses active version from `V_DAILY_POLICY_EFFECTIVE_ACTIVE` with safe fallback to baseline trusted pattern target when no symbol row exists.
- Proposal generation reads the active policy context through `V_TRUSTED_SIGNALS_LATEST_TS`.
