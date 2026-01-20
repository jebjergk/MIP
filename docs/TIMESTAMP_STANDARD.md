# Timestamp standard for MIP (system vs market)

## Standard
Only **system timestamps** in MIP tables must be stored as:

- **Type:** `TIMESTAMP_NTZ`
- **Semantic:** Berlin local wall-clock time (DST-aware; CET/CEST)
- **Expression:**
  ```sql
  CONVERT_TIMEZONE('UTC', 'Europe/Berlin', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
  ```

## Required helper UDF
Use the canonical helper to avoid relying on session timezone:

```sql
CREATE OR REPLACE FUNCTION MIP.APP.F_NOW_BERLIN_NTZ()
RETURNS TIMESTAMP_NTZ
AS
$$
  CONVERT_TIMEZONE('UTC','Europe/Berlin', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
$$;
```

## Where to apply
Use `MIP.APP.F_NOW_BERLIN_NTZ()` for **system timestamps** such as:

- `CREATED_AT`, `UPDATED_AT`, `GENERATED_AT`,
  `CALCULATED_AT`, `RUN_TS`, `STARTED_AT`, `ENDED_AT`, `EVENT_TS`
- Table defaults
- Inserts/updates in stored procedures
- Audit logging

Do **not** apply Berlin normalization to market/business timestamps such as:

- `TS`, `BAR_TS`, `TRADE_TS`, `QUOTE_TS`
- window parameters like `FROM_TS` / `TO_TS`
- any timestamp used to join to `MIP.MART.MARKET_BARS.TS`

## Correct patterns
```sql
CREATED_AT TIMESTAMP_NTZ DEFAULT MIP.APP.F_NOW_BERLIN_NTZ()
```

```sql
INSERT INTO MIP.APP.MIP_AUDIT_LOG (EVENT_TS, ...)
VALUES (MIP.APP.F_NOW_BERLIN_NTZ(), ...)
```

```sql
UPDATE MIP.APP.PATTERN_DEFINITION
SET UPDATED_AT = MIP.APP.F_NOW_BERLIN_NTZ()
```

## Incorrect patterns
```sql
-- Market timestamps should not be defaulted to Berlin
TS TIMESTAMP_NTZ DEFAULT MIP.APP.F_NOW_BERLIN_NTZ()
```

```sql
-- Relies on session timezone (not allowed)
DEFAULT CURRENT_TIMESTAMP()
```

```sql
-- CET is not DST-aware and is incorrect
CONVERT_TIMEZONE('UTC','CET', CURRENT_TIMESTAMP())
```

```sql
-- Storing UTC in NTZ while intending Berlin time
CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
```
