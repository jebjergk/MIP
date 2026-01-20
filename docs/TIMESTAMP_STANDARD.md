# Timestamp standard for MIP (Berlin local NTZ)

## Standard
All **persisted** timestamps in MIP tables must be stored as:

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
Use `MIP.APP.F_NOW_BERLIN_NTZ()` for any **persisted** timestamp columns, including (but not limited to):

- `*_TS`, `*_AT`, `EVENT_TS`, `CREATED_AT`, `UPDATED_AT`, `GENERATED_AT`,
  `CALCULATED_AT`, `RUN_TS`
- Table defaults
- Inserts/updates in stored procedures
- Audit logging

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
