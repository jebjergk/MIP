# 19. Runs (Audit Viewer)

Monitor every pipeline run — when it happened, whether it succeeded, how long each step took, and what errors occurred. This is your pipeline health dashboard.

## Daily / Intraday Toggle

The page has a **Daily Pipeline / Intraday Pipeline** toggle at the top. Switch to "Intraday Pipeline" to see intraday-specific pipeline runs with their own metrics (bars ingested, signals generated, outcomes evaluated, compute time). The run list, filters, and detail panel all adapt to the selected pipeline type.

## Left Panel: Run List

A list of recent pipeline runs, showing status, time, and run ID. Click a run to see its details.

- **Status Badge** — Green (SUCCESS) = pipeline completed normally. Yellow (SUCCESS WITH SKIPS) = completed but some steps were skipped (e.g., no new data). Red (FAILED) = one or more steps had errors. Blue (RUNNING) = pipeline is currently in progress.
- **Started Time** — When the run started, formatted as "Feb 07, 14:30".
- **Run ID** — Unique identifier (first 8 characters shown). Useful for debugging.

## Filters

Filter runs by **Status** (All, Failed, Success, Success with Skips, Running), **From date**, and **To date**.

## Right Panel: Run Detail

When you click a run, the detail panel shows:

- **Summary Cards** — Status, Duration (total time in seconds), As-of (market data date), Portfolios (how many portfolios were processed), Errors (error count if any).

- **Run Summary Narrative** — An AI-generated or deterministic summary: headline, what happened, why, impact, and next check time.

- **Error Panel (if errors exist)** — Lists each error with event name, timestamp, error message, SQLSTATE code, and query ID. Useful for Snowflake debugging.

- **Step Timeline** — A chronological list of every step the pipeline executed. Each shows: step name, status (pass/fail/skip), duration in seconds, and portfolio ID. Example: "SP_GENERATE_SIGNALS — 2.3s — Portfolio 1" means signal generation took 2.3 seconds and succeeded.

- **Step Detail Panel** — Click a step to see: duration (seconds + ms), rows affected, portfolio ID, timestamps, and error details if it failed.
