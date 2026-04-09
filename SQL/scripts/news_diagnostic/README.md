# MIP news diagnostic SQL pack

Reusable queries for the news stack measurement pass. Run with the agent Snowflake runner, for example:

```
cursorfiles\.venv\Scripts\python.exe cursorfiles\query_snowflake.py -f MIP/SQL/scripts/news_diagnostic/01_freshness_by_source.sql
```

These files are **local diagnostics only** — they do not create or alter Snowflake objects.

Files: `00_config_and_tasks.sql` … `07_audit_log_news_pipeline.sql`.

Evidence in [news_diagnostic_memo.md](../../../docs/news_diagnostic_memo.md) was captured from Snowflake on **2026-04-08** (agent `CURSOR_AGENT`).
