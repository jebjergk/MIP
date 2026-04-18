# Committee 2.0 — Risks & validation

## Risks

- **Missing snapshot** for old proposals — backfill or re-run propose; API returns `NO_SNAPSHOT`.
- **UI API role** must have DML on new tables (see grants migration).
- **Concurrent commit** — unique constraint + catch + return existing row.

## Validation

- **Unit:** `tests/test_committee2_engine.py`
- **SQL smoke:** [`33_committee2_hearing_smoke.sql`](../../SQL/smoke/33_committee2_hearing_smoke.sql)
- **Manual:** open → refresh → commit twice (second idempotent) from UI or curl
