# 19. Runs (Audit Viewer)

Runs is MIP's operational truth source for both daily and intraday pipelines.

If a page looks stale, a trade did not execute, or a metric changed unexpectedly, verify here first.

## Daily / Intraday toggle

Use the top toggle to switch between:

- **Daily Pipeline**: end-of-day style learning and decision flow.
- **Intraday Pipeline**: shorter-cycle updates with intraday bar/signal/evaluation stats.

The run list, filters, summary cards, and step detail all follow the selected mode.

## Left panel: run list

Each row summarizes one run.

- **Status badge**:
  - `SUCCESS`: completed without blocking errors.
  - `SUCCESS_WITH_SKIPS`: completed, but one or more steps were intentionally skipped (for example `NO_NEW_BARS`).
  - `FAILED`: one or more steps failed.
  - `RUNNING`: currently in progress.
- **Started time**: run start timestamp in local UI format.
- **Run ID**: unique run identifier (shortened in list, full ID in detail).

Tip: `SUCCESS_WITH_SKIPS` is often normal during no-data windows and should be interpreted with step-level detail, not as an automatic incident.

## Filters

Filter by:

- **Status** (All / Failed / Success / Success with skips / Running),
- **From** date,
- **To** date.

Use date filtering when comparing behavior before/after a config or policy change.

## Right panel: run detail

Selecting a run opens full diagnostics:

- **Summary cards**: status, total duration, market as-of date, portfolio count, and error count.
- **Run summary narrative**: deterministic/AI summary describing what changed, impact, and next checks.
- **Error panel** (if present): event name, timestamp, SQLSTATE, query ID, and message.
- **Step timeline**: every executed step in order, including status and duration.
- **Step detail panel**: row counts, portfolio ID, timestamps, and full error context.

## Fast triage workflow

1. Confirm latest run status for the selected pipeline mode.
2. Open the newest run and check summary cards + narrative.
3. Inspect skipped or failed steps in timeline.
4. Copy run ID and correlate with AI Agent Decisions / Live Portfolio Activity when needed.
5. Escalate only after confirming whether behavior was expected (`NO_NEW_BARS`, policy block, freshness guard, etc.).
