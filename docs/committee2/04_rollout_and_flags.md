# Committee 2.0 — Flags

| Flag | Stage |
|------|-------|
| `COMMITTEE2_ENABLED` | 1 — master (in `APP_CONFIG`) |
| `COMMITTEE2_CONTEXT_DISCLOSURE_ENABLED` | 2 — politician trade disclosure card (stored Snowflake rows) |
| `COMMITTEE2_LIVE_POLITICIAN_DISCLOSURE_ENABLED` | 2 — optional silent live JSON fetch (requires env template; see below) |
| `COMMITTEE2_LLM_ROLES_ENABLED` | 3 — future |
| `COMMITTEE2_EVAL_MART_ENABLED` | 4 — future |

Migration [`20260418_committee2_config_grants.sql`](../../SQL/migrations/20260418_committee2_config_grants.sql) seeds `COMMITTEE2_ENABLED=true`.

**Phase 2 live enrichment (optional):** When both `COMMITTEE2_CONTEXT_DISCLOSURE_ENABLED` and `COMMITTEE2_LIVE_POLITICIAN_DISCLOSURE_ENABLED` are true, the API may call a **trusted** HTTP JSON endpoint if the host sets:

`MIP_POLITICIAN_DISCLOSURE_LIVE_URL_TEMPLATE` — URL string containing `{symbol}` (e.g. `https://internal.example.com/disclosures/{symbol}`). Response must be JSON with non-empty `summary_lines` (array of strings). Optional: `link_url` (https), `source_label`. Any error, timeout, or invalid body → **no** live exhibit (same UX as no data).
