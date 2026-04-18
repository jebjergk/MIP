# Committee 2.0 — Flags

| Flag | Stage |
|------|-------|
| `COMMITTEE2_ENABLED` | 1 — master (in `APP_CONFIG`) |
| `COMMITTEE2_CONTEXT_DISCLOSURE_ENABLED` | 2 — politician trade disclosure card (stored Snowflake rows) |
| `COMMITTEE2_LIVE_POLITICIAN_DISCLOSURE_ENABLED` | 2 — optional silent live JSON fetch (requires env template; see below) |
| `COMMITTEE2_LLM_ROLES_ENABLED` | 3 — future |
| `COMMITTEE2_EVAL_MART_ENABLED` | 4 — future |

Migration [`20260418_committee2_config_grants.sql`](../../SQL/migrations/20260418_committee2_config_grants.sql) seeds `COMMITTEE2_ENABLED=true`.

**Phase 2 live enrichment (optional):** When both `COMMITTEE2_CONTEXT_DISCLOSURE_ENABLED` and `COMMITTEE2_LIVE_POLITICIAN_DISCLOSURE_ENABLED` are true:

- **Real JSON bridge (preferred when set):** `MIP_POLITICIAN_DISCLOSURE_LIVE_URL_TEMPLATE` — URL containing `{symbol}`. Response JSON must include non-empty `summary_lines`. Optional: `link_url` (https), `source_label`. Errors/timeouts → **no** live exhibit.
- **Local demo (no HTTP):** when **no** template is configured, set `MIP_POLITICIAN_DISCLOSURE_LIVE_DEMO=true` to return a deterministic stub and verify the **Live disclosure context** card. If both template and demo are set, the template wins.
