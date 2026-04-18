# Committee 2.0 — Flags

| Flag | Stage |
|------|-------|
| `COMMITTEE2_ENABLED` | 1 — master (in `APP_CONFIG`) |
| `COMMITTEE2_CONTEXT_DISCLOSURE_ENABLED` | 2 — future |
| `COMMITTEE2_LLM_ROLES_ENABLED` | 3 — future |
| `COMMITTEE2_EVAL_MART_ENABLED` | 4 — future |

Migration [`20260418_committee2_config_grants.sql`](../../SQL/migrations/20260418_committee2_config_grants.sql) seeds `COMMITTEE2_ENABLED=true`.
