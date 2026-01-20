# Timestamp policy (system vs market)

## System timestamps (Berlin-local)
Use Berlin-local `TIMESTAMP_NTZ` **only** for system/audit timestamps that record when MIP processes ran.
This assumes the account/session timezone is set to `Europe/Berlin`.
These should default to `CURRENT_TIMESTAMP()`.

**Examples**
- `CREATED_AT`, `UPDATED_AT`, `GENERATED_AT`, `CALCULATED_AT`
- `RUN_TS`, `STARTED_AT`, `ENDED_AT`
- `EVENT_TS` in audit logging tables

**Example DDL**
```sql
CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
```

## Market/business timestamps (as-is)
Market timestamps represent exchange or bar times and must remain **as-is**. Do **not** apply Berlin defaults
or normalization.

**Examples**
- `TS`, `BAR_TS`, `TRADE_TS`, `QUOTE_TS`
- `FROM_TS`, `TO_TS` window parameters
- any timestamp used to join to `MIP.MART.MARKET_BARS.TS`

**Example DDL**
```sql
TS TIMESTAMP_NTZ NOT NULL
```

**Example procedure default**
```sql
v_to_ts TIMESTAMP_NTZ := COALESCE(:P_TO_DATE, CURRENT_TIMESTAMP())
```
