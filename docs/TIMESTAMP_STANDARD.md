# Timestamp standard for MIP (system vs market)

## Standard
Only **system timestamps** in MIP tables must be stored as:

- **Type:** `TIMESTAMP_NTZ`
- **Semantic:** Berlin local wall-clock time (DST-aware; CET/CEST)
- **Expression:**
  ```sql
  CURRENT_TIMESTAMP()
  ```

## Timezone configuration
This relies on the account/session timezone being set to Berlin. Use:

```sql
ALTER ACCOUNT SET TIMEZONE = 'Europe/Berlin';
```

```sql
ALTER SESSION SET TIMEZONE = 'Europe/Berlin';
```

## Where to apply
Use `CURRENT_TIMESTAMP()` for **system timestamps** such as:

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
CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
```

```sql
INSERT INTO MIP.APP.MIP_AUDIT_LOG (EVENT_TS, ...)
VALUES (CURRENT_TIMESTAMP(), ...)
```

```sql
UPDATE MIP.APP.PATTERN_DEFINITION
SET UPDATED_AT = CURRENT_TIMESTAMP()
```

## Incorrect patterns
```sql
-- Market timestamps should not be defaulted to Berlin
TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
```

```sql
-- Hard-codes a fixed offset (not DST-aware)
CONVERT_TIMEZONE('UTC','CET', CURRENT_TIMESTAMP())
```

```sql
-- Forces a different timezone
CONVERT_TIMEZONE('UTC','America/Los_Angeles', CURRENT_TIMESTAMP())
```
